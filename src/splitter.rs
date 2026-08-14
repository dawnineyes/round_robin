use crate::frame::{
    self, FLAG_DATA, FLAG_FIN, FLAG_RST, FLAG_SYN, Frame, FrameDecoder, PROTO_UDP, SynTarget,
};
use crate::reorder::ReorderBuf;
use crate::socks5;
use crate::tunnel::{TUNNEL_CHANNEL_CAP, TunnelLink, TunnelPool, drain_frames};
use anyhow::{Result, bail};
use bytes::Bytes;
use dashmap::DashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream, UdpSocket};
use tokio::sync::mpsc;
use tracing::{error, info, warn};

// ── Config ────────────────────────────────────────────────────────────

pub struct SplitterConfig {
    pub listen_addr: SocketAddr,
    pub tunnels: Vec<TunnelEndpoint>,
    pub chunk_size: usize,
    /// Heartbeat / connection-sweep interval (B21: configurable so tests
    /// can exercise sweep races without waiting a full minute).
    pub heartbeat_interval: Duration,
}

#[derive(Clone)]
pub struct TunnelEndpoint {
    /// SOCKS5 proxy to connect through (Windows sing-box SOCKS5 inbound)
    pub proxy: SocketAddr,
    /// Address the Debian Rust listens on (flows through TUIC → Debian sing-box → Debian Rust)
    pub target: String,
    /// Port the Debian Rust listens on for this tunnel
    pub port: u16,
}

// ── Virtual connection (splitter side) ────────────────────────────────

/// Client-facing channel capacity. ~32 MB worst-case backlog per TCP
/// connection before the connection is reset (bounded — no OOM).
const CLIENT_CHANNEL_CAP: usize = 512;
/// UDP relay channel capacity (datagrams are usually small).
const UDP_CHANNEL_CAP: usize = 1024;
/// A client/egress write stalled for this long is a dead peer — give up.
const CLIENT_WRITE_TIMEOUT: Duration = Duration::from_secs(60);
/// DATA send timeout: no live tunnel can take the frame within this
/// window → the connection cannot proceed.
const DATA_SEND_TIMEOUT: Duration = Duration::from_secs(30);
/// SOCKS5 handshake timeout (BUG-6): a peer that stalls mid-handshake
/// must not pin a task and a socket forever.
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(15);
/// Max wait after our FIN for in-flight DATA (remote fin_seq) before
/// force-closing (BUG-2). A dead tunnel can never deliver the gap, so
/// the wait must be bounded.
const CLOSE_GRACE_MAX: Duration = Duration::from_secs(15);
/// When no remote FIN arrives at all, tear down after this much quiet
/// time (no inbound DATA).  Generous enough for slowly-streaming
/// targets, short enough to reclaim conns from a dead reassembler.
const CLOSE_QUIET_TIMEOUT: Duration = Duration::from_secs(60);
/// B21: after the remote FIN and a complete response stream, reclaim the
/// conn only after this much silence — never while the client is still
/// actively sending (D3 half-close).
const FIN_COMPLETE_IDLE_SECS: u64 = 30;

struct VirtConn {
    to_client_tx: mpsc::Sender<Bytes>,
    reorder: Mutex<ReorderBuf>,
    /// Woken on FIN/RST so the client read loop can exit.
    notify: tokio::sync::Notify,
    /// Teardown requested: RST / overflow / idle sweep.  A remote FIN is
    /// NOT a teardown (D3 — it's a half-close).
    closed: AtomicBool,
    /// FIN received from reassembler (close initiated remotely).
    fin_received: AtomicBool,
    /// BUG-2: FIN's seq = next_seq — all frames below it must be
    /// delivered before the connection may be torn down.
    fin_seq: AtomicU64,
    /// The client loop has exited and the handler is inside the FIN
    /// grace wait — the heartbeat must not sweep this conn (D3).
    grace_waiting: AtomicBool,
    /// Reset received or triggered locally (reorder/channel overflow).
    rst: AtomicBool,
    /// UDP relay connections bypass the reorder buffer entirely
    /// (datagrams have no ordering semantics) — BUG-3/B19.
    is_udp: bool,
    created_at: Instant,
    last_active: Mutex<Instant>,
    bytes_sent: AtomicU64,
    bytes_recv: AtomicU64,
    frames_sent: AtomicU64,
    frames_recv: AtomicU64,
}

impl VirtConn {
    /// Returns true when the connection must be reset: either the
    /// reorder window overflowed (sequence permanently broken) or the
    /// client channel is full/closed (client can't keep up).
    fn on_frame(&self, seq: u64, payload: Bytes) -> bool {
        let plen = payload.len() as u64;
        // Hold the reorder lock across push + channel writes: concurrent
        // deliveries from other tunnels must not interleave their ready
        // chunks (mpsc try_send from two tasks has no total order).
        let mut reorder = self.reorder.lock().unwrap();
        let result = reorder.push(seq, payload);
        if !result.accepted {
            // Duplicate or window-overflow drop — don't update stats.
            return result.overflow;
        }
        let mut overflow = false;
        for chunk in result.ready {
            if self.to_client_tx.try_send(chunk).is_err() {
                overflow = true;
            }
        }
        self.bytes_recv.fetch_add(plen, Ordering::Relaxed);
        self.frames_recv.fetch_add(1, Ordering::Relaxed);
        *self.last_active.lock().unwrap() = Instant::now();
        overflow
    }
}

/// B21: FIN-received connection sweep decision (pure function, unit
/// tested).  A complete response stream alone is NOT enough to reclaim
/// the conn — the client may legitimately keep sending after the remote
/// FIN (D3 half-close).  Require a quiet window after completion, or the
/// usual idle limit when nothing is flowing at all.
fn fin_sweep_decision(complete: bool, fin_idle_secs: u64, handler_alive: bool) -> bool {
    let limit = if handler_alive {
        TCP_IDLE_TIMEOUT.as_secs()
    } else {
        30
    };
    if fin_idle_secs > limit {
        return true; // silent too long (or handler gone) — reclaim
    }
    // Complete response + 30s of silence: reclaim idle clients quickly
    // without cutting off active uploads.
    complete && fin_idle_secs > FIN_COMPLETE_IDLE_SECS
}

/// Idle timeout constants for automatic connection cleanup.
const TCP_IDLE_TIMEOUT: Duration = Duration::from_secs(300);
const UDP_IDLE_TIMEOUT: Duration = Duration::from_secs(60);

type ConnMap = Arc<DashMap<u32, Arc<VirtConn>>>;

// ── Main entry ────────────────────────────────────────────────────────

pub async fn run_splitter(cfg: SplitterConfig) -> Result<()> {
    let conns: ConnMap = Arc::new(DashMap::new());
    let pool = Arc::new(TunnelPool::new());
    // TIME_WAIT: recently closed conn_ids held to prevent stale-frame misrouting.
    let time_wait: Arc<DashMap<u32, Instant>> = Arc::new(DashMap::new());
    const TIME_WAIT_TTL: Duration = Duration::from_secs(60);

    // Graceful shutdown signal (SIGINT + SIGTERM on unix — BUG-13).
    let shutdown: Arc<AtomicBool> = Arc::new(AtomicBool::new(false));
    let ctrl_c_shutdown = shutdown.clone();
    tokio::spawn(async move {
        crate::shutdown_signal().await;
        info!("shutdown signal received, shutting down");
        ctrl_c_shutdown.store(true, Ordering::Release);
    });

    // Connection reset counter (observability: logged by the heartbeat).
    let resets: Arc<AtomicU64> = Arc::new(AtomicU64::new(0));
    // BUG-6: count handshakes in flight — conns only holds entries after
    // the SOCKS5 handshake completes, so the connection-limit check would
    // otherwise be bypassed by an unbounded number of stalled handshakes.
    let half_open: Arc<AtomicUsize> = Arc::new(AtomicUsize::new(0));
    // B32: wakes the accept loop when a connection slot frees up —
    // replaces the old 100ms busy-poll at the concurrency limit.
    let conn_slot: Arc<tokio::sync::Notify> = Arc::new(tokio::sync::Notify::new());

    // 1. Establish persistent tunnel connections (with reconnect)
    for (i, ep) in cfg.tunnels.iter().enumerate() {
        let ep = ep.clone();
        let pool = pool.clone();
        let conns = conns.clone();
        let time_wait = time_wait.clone();
        let shutdown = shutdown.clone();
        let resets = resets.clone();
        tokio::spawn(async move {
            let mut retry_count: u32 = 0;
            loop {
                match establish_tunnel(&ep).await {
                    Ok(stream) => {
                        retry_count = 0;
                        info!(tunnel = i, proxy = %ep.proxy, target = %ep.target, port = ep.port, "connected");
                        let (rd, wr) = stream.into_split();
                        let (tx, rx) = mpsc::channel::<Frame>(TUNNEL_CHANNEL_CAP);
                        let link = Arc::new(TunnelLink {
                            tx,
                            alive: AtomicBool::new(true),
                            bytes_sent: AtomicU64::new(0),
                            bytes_recv: AtomicU64::new(0),
                            frames_sent: AtomicU64::new(0),
                            frames_recv: AtomicU64::new(0),
                            stop: Arc::new(tokio::sync::Notify::new()),
                            lost_frames: Mutex::new(Vec::new()),
                        });
                        pool.add(link.clone());

                        let wr_task = tokio::spawn(drain_frames(rx, wr, link.clone()));

                        if let Err(e) =
                            tunnel_read_loop(rd, &conns, &pool, &link, &time_wait, &resets).await
                        {
                            warn!(tunnel = i, error = %e, "read loop ended");
                        }
                        link.alive.store(false, Ordering::Release);
                        // Tell the drain task to stop and drain its queue.
                        link.stop.notify_one();
                        let _ = wr_task.await;
                        // D1 fast recovery: frames that were queued but
                        // never written are lost forever.  Resend control
                        // frames; reset connections that lost DATA.
                        let lost = std::mem::take(&mut *link.lost_frames.lock().unwrap());
                        for f in lost {
                            if f.flags & (FLAG_SYN | FLAG_FIN | FLAG_RST) != 0 {
                                pool.send(f);
                            } else if conns.contains_key(&f.conn_id) {
                                warn!(
                                    conn_id = f.conn_id,
                                    tunnel = i,
                                    "tunnel died with queued DATA, resetting connection"
                                );
                                reset_conn(&conns, &time_wait, &pool, &resets, f.conn_id);
                            }
                        }
                        // Log disconnect summary
                        info!(
                            tunnel = i,
                            bytes_sent = link.bytes_sent.load(Ordering::Relaxed),
                            bytes_recv = link.bytes_recv.load(Ordering::Relaxed),
                            frames_sent = link.frames_sent.load(Ordering::Relaxed),
                            frames_recv = link.frames_recv.load(Ordering::Relaxed),
                            "disconnected"
                        );
                    }
                    Err(e) => {
                        retry_count += 1;
                        error!(tunnel = i, retry = retry_count, error = %e, "connect failed, retrying");
                    }
                }
                if shutdown.load(Ordering::Acquire) {
                    info!(tunnel = i, "shutting down tunnel reconnect loop");
                    return;
                }
                // O4: 3s cadence after a session that ran and ended,
                // exponential backoff (3→6→12→24s) on repeated connect
                // failures so a dead peer isn't hammered.
                // BUG-15: the old formula produced 3,3,6,12,24.
                let delay_secs = if retry_count == 0 {
                    3
                } else {
                    std::cmp::min(24u64, 3u64 << retry_count.min(3))
                };
                // B32: interruptible backoff — shutdown must not wait out
                // the full backoff (up to 24s).
                let deadline = tokio::time::Instant::now() + Duration::from_secs(delay_secs);
                loop {
                    if shutdown.load(Ordering::Acquire) {
                        info!(tunnel = i, "shutting down tunnel reconnect loop");
                        return;
                    }
                    let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
                    if remaining.is_zero() {
                        break;
                    }
                    tokio::time::sleep(remaining.min(Duration::from_secs(1))).await;
                }
            }
        });
    }

    // Bind the SOCKS listener BEFORE waiting for the first tunnel:
    // clients then get a deterministic failure reply (BUG-10) instead of
    // ECONNREFUSED while tunnels are still connecting (CI e2e race).
    let listener = TcpListener::bind(cfg.listen_addr).await?;

    // Wait for at least one tunnel. BUG-9: honor shutdown while waiting —
    // a bad proxy config used to make Ctrl+C impossible to exit.
    while pool.link_count() == 0 {
        if shutdown.load(Ordering::Acquire) {
            info!("shutting down before first tunnel connected");
            return Ok(());
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    info!(listen = %cfg.listen_addr, tunnels = pool.link_count(), "splitter ready");

    // UDP datagram counters (shared with heartbeat and UDP relay)
    let udp_sent = Arc::new(AtomicU64::new(0));
    let udp_recv = Arc::new(AtomicU64::new(0));

    // Periodic heartbeat
    let start_time = Instant::now();
    let hb_interval = cfg.heartbeat_interval;
    let hb_pool = pool.clone();
    let hb_conns = conns.clone();
    let hb_udp_sent = udp_sent.clone();
    let hb_udp_recv = udp_recv.clone();
    let hb_time_wait = time_wait.clone();
    let hb_shutdown = shutdown.clone();
    let hb_resets = resets.clone();
    let hb_half_open = half_open.clone();
    let hb_conn_slot = conn_slot.clone();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(hb_interval).await;
            if hb_shutdown.load(Ordering::Acquire) {
                break;
            }
            let (alive, total) = hb_pool.stats();
            let queue_depth = hb_pool.queue_depth();
            // Sweep dead links that accumulated from tunnel reconnects
            hb_pool.compact();
            // Sweep expired TIME_WAIT entries
            let now = Instant::now();
            hb_time_wait.retain(|_, &mut since| now.duration_since(since) < TIME_WAIT_TTL);
            // Sweep idle and orphaned connections
            let now = Instant::now();
            hb_conns.retain(|&cid, vc| {
                // Orphaned: Arc only held by DashMap
                if Arc::strong_count(vc) <= 1 {
                    warn!(conn_id = cid, "sweeping orphaned connection");
                    // B25: tombstone before removal so alloc_conn_id can't
                    // reuse the id while stale frames are in flight.
                    hb_time_wait.insert(cid, Instant::now());
                    return false;
                }
                // FIN-received connections: the response stream is over.
                // - grace wait (handler active, post-EOF): handler
                //   manages its own 15s cap — never sweep here.
                // - D3 wait (client still connected) or orphaned handler:
                //   reclaim when complete or silent too long.
                if vc.fin_received.load(Ordering::Acquire) {
                    if vc.grace_waiting.load(Ordering::Acquire) {
                        return true;
                    }
                    let fin_seq = vc.fin_seq.load(Ordering::Acquire);
                    let complete = vc.reorder.lock().unwrap().is_complete_through(fin_seq);
                    let fin_idle = now
                        .duration_since(*vc.last_active.lock().unwrap())
                        .as_secs();
                    let handler_alive = Arc::strong_count(vc) > 1;
                    // B21: only sweep when complete AND quiet — never while
                    // the client is still actively sending (D3).
                    if fin_sweep_decision(complete, fin_idle, handler_alive) {
                        if !complete {
                            warn!(
                                conn_id = cid,
                                idle_secs = fin_idle,
                                fin_seq,
                                "FIN-received connection swept with in-flight frames"
                            );
                        }
                        vc.closed.store(true, Ordering::Release);
                        vc.notify.notify_one();
                        // B25: tombstone before removal.
                        hb_time_wait.insert(cid, Instant::now());
                        return false;
                    }
                    return true; // keep alive during grace period
                }
                // Idle timeout (TCP) or UDP timeout
                let idle = now
                    .duration_since(*vc.last_active.lock().unwrap())
                    .as_secs();
                let timeout = if vc.is_udp {
                    UDP_IDLE_TIMEOUT.as_secs()
                } else {
                    TCP_IDLE_TIMEOUT.as_secs()
                };
                if idle > timeout {
                    warn!(conn_id = cid, idle_secs = idle, "connection idle timeout");
                    vc.closed.store(true, Ordering::Release);
                    vc.notify.notify_one();
                    // B25: tombstone before removal.
                    hb_time_wait.insert(cid, Instant::now());
                    return false;
                }
                true
            });
            // B32: sweeps may have freed slots — wake accept-loop waiters.
            hb_conn_slot.notify_waiters();
            let uptime = start_time.elapsed().as_secs();
            info!(
                uptime,
                alive,
                total,
                queue_depth,
                active_conns = hb_conns.len(),
                half_open = hb_half_open.load(Ordering::Acquire),
                time_wait = hb_time_wait.len(),
                resets = hb_resets.swap(0, Ordering::Relaxed),
                udp_sent = hb_udp_sent.swap(0, Ordering::Relaxed),
                udp_recv = hb_udp_recv.swap(0, Ordering::Relaxed),
                "heartbeat"
            );
        }
    });

    // 2. Accept SOCKS5 clients (listener already bound above)
    // Max concurrent connections — prevent resource exhaustion.
    const MAX_CONCURRENT_CONNS: usize = 4096;

    loop {
        if shutdown.load(Ordering::Acquire) {
            info!("shutting down accept loop");
            return Ok(());
        }
        // Check connection limit before accepting (coarse but fast check).
        // B32: wait for a slot notification instead of busy-polling.
        if conns.len() + half_open.load(Ordering::Acquire) >= MAX_CONCURRENT_CONNS {
            conn_slot.notified().await;
            continue;
        }
        let (stream, peer) = loop {
            match listener.accept().await {
                Ok(v) => break v,
                Err(e) => {
                    warn!(error = %e, "accept failed, retrying in 100ms");
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
            }
        };
        let _ = stream.set_nodelay(true);
        // B40: conn_id allocation moved into handle_client — an id drawn
        // here would sit unoccupied across the (up to 15s) SOCKS5
        // handshake, letting a second accept draw the same id and later
        // overwrite the first conn's conns entry.
        let pool = pool.clone();
        let conns = conns.clone();
        let time_wait = time_wait.clone();
        let us = udp_sent.clone();
        let ur = udp_recv.clone();
        let half_open = half_open.clone();
        let conn_slot = conn_slot.clone();

        tokio::spawn(async move {
            let ctx = ClientCtx {
                peer,
                pool: pool.clone(),
                conns: conns.clone(),
                time_wait: time_wait.clone(),
                chunk_size: cfg.chunk_size,
                udp_sent: us,
                udp_recv: ur,
                half_open,
                conn_slot,
            };
            if let Err(e) = handle_client(stream, ctx).await {
                warn!(peer = %peer, error = %e, "client handler failed");
            }
        });
    }
}

/// Allocate a random conn_id that is neither in use nor in TIME_WAIT.
/// Shared by the TCP accept path and per-association UDP relays (BUG-19).
fn alloc_conn_id(
    conns: &DashMap<u32, Arc<VirtConn>>,
    time_wait: &DashMap<u32, Instant>,
) -> Option<u32> {
    for _ in 0..1024 {
        let id: u32 = rand::random();
        if id == 0 {
            continue; // 0 historically reserved (legacy UDP relay)
        }
        if !conns.contains_key(&id) && !time_wait.contains_key(&id) {
            return Some(id);
        }
    }
    None
}

// ── Tunnel management ─────────────────────────────────────────────────

async fn establish_tunnel(ep: &TunnelEndpoint) -> Result<TcpStream> {
    let stream = tokio::time::timeout(
        Duration::from_secs(10),
        socks5::socks5_client_connect(ep.proxy, &ep.target, ep.port),
    )
    .await??;
    let _ = stream.set_nodelay(true);
    Ok(stream)
}

async fn tunnel_read_loop(
    mut rd: tokio::net::tcp::OwnedReadHalf,
    conns: &ConnMap,
    pool: &TunnelPool,
    link: &TunnelLink,
    time_wait: &DashMap<u32, Instant>,
    resets: &AtomicU64,
) -> Result<()> {
    let mut decoder = FrameDecoder::new();
    loop {
        let frame = match decoder.try_next(&mut rd).await? {
            Some(f) => f,
            None => return Ok(()),
        };
        let plen = frame.payload.len() as u64;
        handle_inbound_frame(frame, conns, pool, time_wait, resets);
        link.bytes_recv.fetch_add(plen, Ordering::Relaxed);
        link.frames_recv.fetch_add(1, Ordering::Relaxed);
    }
}

// ── Inbound frame dispatch ────────────────────────────────────────────

fn handle_inbound_frame(
    frame: Frame,
    conns: &ConnMap,
    pool: &TunnelPool,
    time_wait: &DashMap<u32, Instant>,
    resets: &AtomicU64,
) {
    if frame.flags & FLAG_DATA != 0 {
        if let Some(conn) = conns.get(&frame.conn_id) {
            // BUG-3/B19: UDP relay DATA bypasses the reorder buffer —
            // datagrams have no ordering semantics, and a single dropped
            // response used to leave a permanent seq gap that eventually
            // overflowed the window and killed the whole relay.
            if conn.is_udp {
                let plen = frame.payload.len() as u64;
                if conn.to_client_tx.try_send(frame.payload).is_err() {
                    // Client not draining (relay channel full) — drop the
                    // datagram, best effort; never reset a UDP relay.
                    warn!(
                        conn_id = frame.conn_id,
                        "UDP relay client channel full, dropping datagram"
                    );
                } else {
                    conn.bytes_recv.fetch_add(plen, Ordering::Relaxed);
                    conn.frames_recv.fetch_add(1, Ordering::Relaxed);
                }
                *conn.last_active.lock().unwrap() = Instant::now();
                return;
            }
            let overflow = conn.on_frame(frame.seq, frame.payload);
            drop(conn); // release the shard lock before remove()
            if overflow {
                // Reorder window overflow or client channel full: the
                // sequence is broken — reset instead of stalling.
                warn!(
                    conn_id = frame.conn_id,
                    seq = frame.seq,
                    "reorder/channel overflow, resetting connection"
                );
                reset_conn(conns, time_wait, pool, resets, frame.conn_id);
            }
        } else if time_wait.contains_key(&frame.conn_id) {
            // Late DATA arrived after FIN but before TIME_WAIT expiry.
            // This means the FIN_GRACE period was too short for this path.
            warn!(
                conn_id = frame.conn_id,
                seq = frame.seq,
                "late DATA frame on TIME_WAIT conn_id — possible data loss"
            );
        } else {
            // Unknown conn_id: stale/dangling. Send RST so the
            // reassembler cleans up and stops flooding the tunnel.
            pool.send(Frame::rst(frame.conn_id));
        }
        return;
    }

    if frame.flags & FLAG_FIN != 0 {
        // Don't remove from conns immediately — late DATA frames may still
        // be in-flight on other tunnels.  Record the FIN's next_seq so the
        // client handler can wait for every in-flight frame before removal
        // (BUG-2: the old fixed 3s grace dropped late DATA on slow tunnels).
        if let Some(conn) = conns.get(&frame.conn_id) {
            if conn.is_udp {
                return; // UDP has no FIN semantics
            }
            conn.fin_received.store(true, Ordering::Release);
            conn.fin_seq.store(frame.seq, Ordering::Release);
            // D3: FIN is a half-close — do NOT mark the conn closed.
            conn.notify.notify_one();
        }
        return;
    }

    if frame.flags & FLAG_RST != 0 {
        // RST = force-close, no grace period needed.
        reset_conn(conns, time_wait, pool, resets, frame.conn_id);
    }
}

/// Tear down a virtual connection and tell the peer to do the same.
/// Used for reorder overflow, remote RST and tunnel-loss recovery (D1).
fn reset_conn(
    conns: &ConnMap,
    time_wait: &DashMap<u32, Instant>,
    pool: &TunnelPool,
    resets: &AtomicU64,
    conn_id: u32,
) {
    // B25: tombstone first so alloc_conn_id can't reuse the id in the
    // window between removal and the previous tombstone insert.
    time_wait.insert(conn_id, Instant::now());
    if let Some((_, conn)) = conns.remove(&conn_id) {
        conn.rst.store(true, Ordering::Release);
        conn.closed.store(true, Ordering::Release);
        conn.notify.notify_one();
        drop(conn);
    }
    pool.send(Frame::rst(conn_id));
    resets.fetch_add(1, Ordering::Relaxed);
}

// ── Client handler ────────────────────────────────────────────────────

struct ClientCtx {
    peer: SocketAddr,
    pool: Arc<TunnelPool>,
    conns: ConnMap,
    time_wait: Arc<DashMap<u32, Instant>>,
    chunk_size: usize,
    udp_sent: Arc<AtomicU64>,
    udp_recv: Arc<AtomicU64>,
    half_open: Arc<AtomicUsize>,
    /// B32: notified when a handler exits — wakes the accept loop.
    conn_slot: Arc<tokio::sync::Notify>,
}

async fn handle_client(stream: TcpStream, ctx: ClientCtx) -> Result<()> {
    // BUG-6: bound the SOCKS5 handshake — a stalled peer must not pin the
    // task/socket forever — and count it against the connection limit.
    ctx.half_open.fetch_add(1, Ordering::AcqRel);
    let accepted =
        match tokio::time::timeout(HANDSHAKE_TIMEOUT, socks5::socks5_server_accept(stream)).await {
            Ok(v) => v,
            Err(_) => {
                ctx.half_open.fetch_sub(1, Ordering::AcqRel);
                // B32: the handshake slot just freed — wake the accept loop.
                ctx.conn_slot.notify_one();
                return Err(anyhow::anyhow!(
                    "SOCKS5 handshake timed out after {HANDSHAKE_TIMEOUT:?}"
                ));
            }
        };
    ctx.half_open.fetch_sub(1, Ordering::AcqRel);
    ctx.conn_slot.notify_one();
    let (accepted, reply) = accepted?;
    // B40: allocate the conn_id only after the handshake completes — an
    // id allocated earlier would sit unoccupied for up to HANDSHAKE_TIMEOUT
    // and a second accept could draw the same id, so the two conns would
    // later overwrite each other's conns entry.  Post-handshake allocation
    // leaves only the instruction-wide alloc→insert window (the inherent
    // 2⁻³² random bound).
    let conn_id = match alloc_conn_id(&ctx.conns, &ctx.time_wait) {
        Some(id) => id,
        None => {
            // Practically unreachable (2³² ids), but never loop forever.
            warn!(peer = %ctx.peer, "conn_id space exhausted, dropping connection");
            return Ok(());
        }
    };
    // B32: the handler is about to finish — a connection slot frees up.
    // Wake the accept loop so it re-checks the limit promptly.
    let result = match accepted {
        socks5::Socks5Result::Connect(accepted) => {
            handle_tcp_client(conn_id, accepted, reply, &ctx).await
        }
        socks5::Socks5Result::UdpAssociate {
            stream: keepalive,
            relay,
        } => handle_udp_client(relay, keepalive, reply, &ctx).await,
    };
    ctx.conn_slot.notify_one();
    result
}

/// B35: write a SOCKS5 reply with a bounded timeout.  The reply moved out
/// of socks5_server_accept's handshake timeout (deferred until the tunnel
/// SYN is queued), so a peer that stops reading must not pin the handler.
async fn send_socks5_reply(stream: &mut TcpStream, rep: &[u8]) {
    let _ = tokio::time::timeout(Duration::from_secs(5), stream.write_all(rep)).await;
}

async fn handle_tcp_client(
    conn_id: u32,
    mut accepted: socks5::Socks5Accept,
    reply: Vec<u8>,
    ctx: &ClientCtx,
) -> Result<()> {
    // Bundled context (B35: the reply param grew the arg list past
    // clippy's limit — mirror EgressReaderCtx's style).
    let ClientCtx {
        peer,
        pool,
        conns,
        time_wait,
        ..
    } = ctx;
    let chunk_size = ctx.chunk_size;
    info!(conn_id, peer = %peer, target = %accepted.target.address, port = accepted.target.port, "accepted");

    let syn_target = SynTarget {
        proto: frame::PROTO_TCP,
        address: accepted.target.address.clone(),
        port: accepted.target.port,
    };
    let syn_frame = Frame::syn(conn_id, syn_target.encode()?);

    let (to_client_tx, to_client_rx) = mpsc::channel::<Bytes>(CLIENT_CHANNEL_CAP);
    let vconn = Arc::new(VirtConn {
        to_client_tx,
        reorder: Mutex::new(ReorderBuf::new()),
        notify: tokio::sync::Notify::new(),
        closed: AtomicBool::new(false),
        fin_received: AtomicBool::new(false),
        fin_seq: AtomicU64::new(0),
        grace_waiting: AtomicBool::new(false),
        rst: AtomicBool::new(false),
        is_udp: false,
        created_at: Instant::now(),
        last_active: Mutex::new(Instant::now()),
        bytes_sent: AtomicU64::new(0),
        bytes_recv: AtomicU64::new(0),
        frames_sent: AtomicU64::new(0),
        frames_recv: AtomicU64::new(0),
    });
    // BUG-6: insert BEFORE sending the SYN so an early RST (egress
    // connect failure on the reassembler) can't slip through the gap
    // and leave the client hanging on a dead connection.
    conns.insert(conn_id, vconn.clone());

    if !pool.send(syn_frame) {
        // B25: only remove OUR entry.
        conns.remove_if(&conn_id, |_, v| Arc::ptr_eq(v, &vconn));
        // BUG-10/B35: the success reply is deferred until the SYN is
        // queued, so on failure the client gets a deterministic
        // REP_GENERAL_FAILURE — never success-then-garbage-then-EOF.
        let mut client_stream = accepted.stream;
        send_socks5_reply(&mut client_stream, &socks5::REPLY_GENERAL_FAILURE).await;
        bail!("no live tunnels to send SYN");
    }
    // B35: the SYN is queued — the client may learn of success now.
    send_socks5_reply(&mut accepted.stream, &reply).await;

    let (mut client_reader, mut client_writer) = accepted.stream.into_split();

    let writer_task = tokio::spawn(async move {
        let mut rx = to_client_rx;
        while let Some(chunk) = rx.recv().await {
            // BUG-9: a peer that reads nothing must not block this task
            // forever — give up after CLIENT_WRITE_TIMEOUT.
            match tokio::time::timeout(CLIENT_WRITE_TIMEOUT, client_writer.write_all(&chunk)).await
            {
                Ok(Ok(())) => {}
                _ => break,
            }
        }
        let _ = client_writer.shutdown().await;
    });

    let mut seq: u64 = 1;
    let close_reason: &str;
    // O1: one reusable read buffer per connection; each frame copies
    // exactly n bytes into a fresh Bytes (no 64 KB backing per frame).
    let mut buf = vec![0u8; chunk_size];
    loop {
        // Race client read against close notification.
        tokio::select! {
            result = client_reader.read(&mut buf) => {
                match result {
                    Ok(0) => {
                        close_reason = "eof";
                        break;
                    }
                    Ok(n) => {
                        let frame = Frame::data(conn_id, seq, Bytes::copy_from_slice(&buf[..n]));
                        // BUG-5: real backpressure — wait for a tunnel to
                        // take the frame instead of killing the connection
                        // after a few microsecond yields.
                        let sent = tokio::time::timeout(DATA_SEND_TIMEOUT, pool.send_async(frame))
                            .await
                            .unwrap_or(false);
                        if !sent {
                            warn!(conn_id, "no live tunnels after timeout, aborting");
                            close_reason = "no_tunnel";
                            break;
                        }
                        vconn.bytes_sent.fetch_add(n as u64, Ordering::Relaxed);
                        vconn.frames_sent.fetch_add(1, Ordering::Relaxed);
                        *vconn.last_active.lock().unwrap() = Instant::now();
                        seq = seq.wrapping_add(1);
                    }
                    Err(e) => {
                        warn!(conn_id, error = %e, "client read error");
                        close_reason = "read_error";
                        break;
                    }
                }
            }
            _ = vconn.notify.notified() => {
                // D3: a remote FIN is a half-close, not a teardown —
                // keep forwarding client data until the client closes its
                // write half.  Only RST / overflow / idle sweep stop us.
                if vconn.closed.load(Ordering::Acquire) {
                    close_reason = if vconn.rst.load(Ordering::Acquire) {
                        "rst"
                    } else {
                        "timeout"
                    };
                    break;
                }
                // Remote FIN or spurious notify — loop back to reading.
            }
        }
    }

    // FIN carries next_seq so the reassembler can half-close its egress
    // write side exactly when every in-flight frame has been delivered.
    // On RST the peer is already tearing down — no FIN needed.
    let fin_sent = if close_reason == "rst" {
        false
    } else {
        tokio::time::timeout(DATA_SEND_TIMEOUT, pool.send_async(Frame::fin(conn_id, seq)))
            .await
            .unwrap_or(false)
    };
    if !fin_sent && close_reason != "rst" {
        // BUG-11: without a FIN the reassembler's egress would linger for
        // up to the 300s idle sweep — send a best-effort RST instead so
        // it tears down promptly.
        warn!(conn_id, "FIN send failed, sending RST fallback");
        pool.send(Frame::rst(conn_id));
    }
    // Grace period (BUG-2): wait for in-flight DATA on other tunnels
    // before removing from conns.  Only meaningful for client-EOF closes:
    // on RST/timeout/no_tunnel the connection is already broken and the
    // wait would just pin the entry.  The reassembler's FIN carries
    // next_seq — wait until the reorder buffer is complete through it,
    // bounded by CLOSE_GRACE_MAX so a dead tunnel can't hang teardown.
    // If no FIN arrives at all, keep the conn alive while responses keep
    // flowing and tear down after CLOSE_QUIET_TIMEOUT of silence.
    // grace_waiting tells the heartbeat to keep its hands off (D3).
    if close_reason == "eof" {
        vconn.grace_waiting.store(true, Ordering::Release);
        let mut fin_seen_at: Option<Instant> = None;
        loop {
            // B26: RST / overflow / idle sweep during the wait — stop
            // immediately instead of lingering up to 60s.
            if vconn.closed.load(Ordering::Acquire) {
                break;
            }
            if vconn.fin_received.load(Ordering::Acquire) {
                let fin_seq = vconn.fin_seq.load(Ordering::Acquire);
                if vconn.reorder.lock().unwrap().is_complete_through(fin_seq) {
                    break; // every in-flight frame below fin_seq delivered
                }
                let seen = *fin_seen_at.get_or_insert_with(Instant::now);
                if seen.elapsed() >= CLOSE_GRACE_MAX {
                    warn!(conn_id, "close grace expired with in-flight frames");
                    break;
                }
            } else {
                let idle = Instant::now().duration_since(*vconn.last_active.lock().unwrap());
                if idle >= CLOSE_QUIET_TIMEOUT {
                    warn!(
                        conn_id,
                        idle_secs = idle.as_secs(),
                        "close wait expired without FIN"
                    );
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
    // Move to TIME_WAIT before removing from conns so a new random
    // conn_id won't collide before the grace period expires.
    // B25: remove only OUR entry — a newer conn may have reused the id
    // in the tiny window after a heartbeat sweep.
    time_wait.insert(conn_id, Instant::now());
    conns.remove_if(&conn_id, |_, v| Arc::ptr_eq(v, &vconn));
    // Snapshot stats before dropping vconn (last Arc → drops to_client_tx → writer_task exits)
    let duration_ms = vconn.created_at.elapsed().as_millis() as u64;
    let bs = vconn.bytes_sent.load(Ordering::Relaxed);
    let br = vconn.bytes_recv.load(Ordering::Relaxed);
    let fs = vconn.frames_sent.load(Ordering::Relaxed);
    let fr = vconn.frames_recv.load(Ordering::Relaxed);
    drop(vconn);
    let _ = writer_task.await;
    info!(
        conn_id,
        bytes_sent = bs,
        bytes_recv = br,
        frames_sent = fs,
        frames_recv = fr,
        duration_ms,
        reason = close_reason,
        "closed"
    );
    Ok(())
}

/// UDP relay: read SOCKS5-wrapped datagrams → DATA frames → pool.
/// BUG-19: each association gets its own conn_id (allocated like a TCP
/// conn and announced via SYN proto=UDP) — multiple clients can relay
/// concurrently instead of fighting over UDP_CONN_ID.
async fn handle_udp_client(
    relay: UdpSocket,
    keepalive: TcpStream,
    reply: Vec<u8>,
    ctx: &ClientCtx,
) -> Result<()> {
    // Bundled context (see handle_tcp_client).
    let ClientCtx {
        pool,
        conns,
        time_wait,
        udp_sent,
        udp_recv,
        ..
    } = ctx;
    let relay = Arc::new(relay);
    let relay_addr = relay.local_addr()?;
    let conn_id = alloc_conn_id(conns, time_wait)
        .ok_or_else(|| anyhow::anyhow!("conn_id space exhausted, cannot start UDP relay"))?;
    info!(conn_id, addr = %relay_addr, "UDP relay started");

    let (to_udp_tx, mut to_udp_rx) = mpsc::channel::<Bytes>(UDP_CHANNEL_CAP);
    let vconn = Arc::new(VirtConn {
        to_client_tx: to_udp_tx,
        reorder: Mutex::new(ReorderBuf::new()),
        notify: tokio::sync::Notify::new(),
        closed: AtomicBool::new(false),
        fin_received: AtomicBool::new(false),
        fin_seq: AtomicU64::new(0),
        grace_waiting: AtomicBool::new(false),
        rst: AtomicBool::new(false),
        is_udp: true,
        created_at: Instant::now(),
        last_active: Mutex::new(Instant::now()),
        bytes_sent: AtomicU64::new(0),
        bytes_recv: AtomicU64::new(0),
        frames_sent: AtomicU64::new(0),
        frames_recv: AtomicU64::new(0),
    });
    // Insert BEFORE the SYN (same BUG-6 rule as TCP): the reassembler
    // creates its UDP vconn from the SYN, so early responses race safe.
    conns.insert(conn_id, vconn.clone());

    let syn = Frame::syn(
        conn_id,
        SynTarget {
            proto: PROTO_UDP,
            address: "0.0.0.0".into(),
            port: 0,
        }
        .encode()?,
    );
    if !pool.send(syn) {
        conns.remove(&conn_id);
        // B35: success reply deferred until the SYN is queued — send a
        // deterministic REP_GENERAL_FAILURE instead of success-then-EOF.
        let mut ka = keepalive;
        send_socks5_reply(&mut ka, &socks5::REPLY_GENERAL_FAILURE).await;
        bail!("no live tunnels to send UDP SYN");
    }
    // B35: the SYN is queued — the client may learn of success now.
    let mut ka = keepalive;
    send_socks5_reply(&mut ka, &reply).await;

    // Track SOCKS5 client address so we can send_to (socket is unconnected).
    let client_addr: Arc<Mutex<Option<SocketAddr>>> = Arc::new(Mutex::new(None));

    let relay2 = relay.clone();
    let ca = client_addr.clone();
    let recv_ctr = Arc::clone(udp_recv);
    tokio::spawn(async move {
        while let Some(dgram) = to_udp_rx.recv().await {
            recv_ctr.fetch_add(1, Ordering::Relaxed);
            let addr = *ca.lock().unwrap();
            if let Some(addr) = addr
                && relay2.send_to(&dgram, addr).await.is_err()
            {
                break;
            }
        }
    });

    // RFC 1928: UDP association is tied to the TCP control connection.
    // When the client closes it, tear down the relay.
    let (ka_tx, mut ka_rx) = tokio::sync::oneshot::channel::<()>();
    tokio::spawn(async move {
        // B31: RFC 1928 — the association is tied to the TCP control
        // connection's lifetime (EOF), not to stray bytes arriving on
        // it.  Loop until EOF or error.
        let mut ka = ka;
        let mut buf = [0u8; 1];
        loop {
            match ka.read(&mut buf).await {
                Ok(0) | Err(_) => break,
                Ok(_) => {}
            }
        }
        let _ = ka_tx.send(());
    });

    let mut buf = vec![0u8; 65535];
    let mut seq: u64 = 1;
    loop {
        tokio::select! {
            result = relay.recv_from(&mut buf) => {
                let (n, client) = match result {
                    Ok(v) => v,
                    Err(e) => {
                        warn!(error = %e, "UDP relay recv error");
                        break;
                    }
                };
                // B39: only the associating client may inject datagrams —
                // responses go to the first sender, so an unverified
                // source could both inject traffic and steal responses.
                {
                    let mut ca = client_addr.lock().unwrap();
                    match *ca {
                        None => *ca = Some(client),
                        Some(known) if known == client => {}
                        Some(known) => {
                            warn!(
                                conn_id,
                                from = %client,
                                expected = %known,
                                "UDP relay: dropping datagram from unexpected source"
                            );
                            continue;
                        }
                    }
                }
                udp_sent.fetch_add(1, Ordering::Relaxed);
                let frame = Frame::data(conn_id, seq, Bytes::copy_from_slice(&buf[..n]));
                let mut sent = false;
                for _ in 0..3 {
                    if pool.send(frame.clone()) {
                        sent = true;
                        break;
                    }
                    tokio::task::yield_now().await;
                }
                if !sent {
                    warn!("UDP relay: no live tunnels, dropping datagram");
                    continue;
                }
                // B29: count UDP bytes/frames like the TCP path does.
                vconn.bytes_sent.fetch_add(n as u64, Ordering::Relaxed);
                vconn.frames_sent.fetch_add(1, Ordering::Relaxed);
                // BUG-3: outbound traffic must also count as activity,
                // otherwise a send-only client (e.g. unanswered DNS) is
                // swept by the 60s idle timeout.
                *vconn.last_active.lock().unwrap() = Instant::now();
                seq = seq.wrapping_add(1);
            }
            _ = &mut ka_rx => {
                info!(conn_id, "UDP keepalive closed, ending relay");
                break;
            }
            _ = vconn.notify.notified() => {
                // Swept by heartbeat / reset — stop the relay loop.
                if vconn.closed.load(Ordering::Acquire) {
                    info!(conn_id, "UDP relay closed by heartbeat/reset, ending relay");
                    break;
                }
            }
        }
    }
    // Only remove our own entry — never a newer relay's. Best-effort RST
    // so the reassembler drops the UDP vconn instead of idle-sweeping it.
    // B25: tombstone the id so stale datagrams can't hit a reused id.
    time_wait.insert(conn_id, Instant::now());
    conns.remove_if(&conn_id, |_, v| Arc::ptr_eq(v, &vconn));
    pool.send(Frame::rst(conn_id));
    Ok(())
}

// ── tests ─────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fin_sweep_keeps_active_upload_alive() {
        // B21 regression: complete response + active client (recent
        // activity) must NOT be swept — D3 half-close semantics.
        assert!(!fin_sweep_decision(true, 0, true));
        assert!(!fin_sweep_decision(true, 30, true));
        assert!(!fin_sweep_decision(false, 0, true));
    }

    #[test]
    fn fin_sweep_reclaims_idle_and_incomplete() {
        // Complete + 30s+ of silence → reclaim (idle client after response).
        assert!(fin_sweep_decision(true, 31, true));
        // Incomplete but silent beyond the idle limit → reclaim.
        assert!(fin_sweep_decision(false, 301, true));
        // Orphaned handler (no clone) → short 30s limit applies.
        assert!(fin_sweep_decision(false, 31, false));
        // Orphaned but recently active → keep (complete path needs 30s too).
        assert!(!fin_sweep_decision(true, 10, false));
    }
}
