use crate::frame::{
    self, FLAG_ACK, FLAG_DATA, FLAG_FIN, FLAG_RST, FLAG_SYN, Frame, FrameDecoder, SynTarget,
    UDP_CONN_ID,
};
use crate::reorder::ReorderBuf;
use crate::socks5;
use crate::tunnel::{TUNNEL_CHANNEL_CAP, TunnelLink, TunnelPool, drain_frames};
use anyhow::{Result, bail};
use bytes::{Bytes, BytesMut};
use dashmap::DashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
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

struct VirtConn {
    to_client_tx: mpsc::UnboundedSender<Bytes>,
    reorder: Mutex<ReorderBuf>,
    /// Woken on FIN/RST so the client read loop can exit.
    notify: tokio::sync::Notify,
    closed: AtomicBool,
    /// FIN received from reassembler (close initiated remotely).
    fin_received: AtomicBool,
    created_at: Instant,
    last_active: Mutex<Instant>,
    bytes_sent: AtomicU64,
    bytes_recv: AtomicU64,
    frames_sent: AtomicU64,
    frames_recv: AtomicU64,
}

impl VirtConn {
    fn on_frame(&self, seq: u64, payload: Bytes) {
        let plen = payload.len() as u64;
        let result = self.reorder.lock().unwrap().push(seq, payload);
        if !result.accepted {
            // Duplicate or buffer-full drop — don't update stats.
            return;
        }
        for chunk in result.ready {
            let _ = self.to_client_tx.send(chunk);
        }
        self.bytes_recv.fetch_add(plen, Ordering::Relaxed);
        self.frames_recv.fetch_add(1, Ordering::Relaxed);
        *self.last_active.lock().unwrap() = Instant::now();
    }
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

    // Graceful shutdown signal.
    let shutdown: Arc<AtomicBool> = Arc::new(AtomicBool::new(false));
    let ctrl_c_shutdown = shutdown.clone();
    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        info!("ctrl+c received, shutting down");
        ctrl_c_shutdown.store(true, Ordering::Release);
    });

    // 1. Establish persistent tunnel connections (with reconnect)
    for (i, ep) in cfg.tunnels.iter().enumerate() {
        let ep = ep.clone();
        let pool = pool.clone();
        let conns = conns.clone();
        let time_wait = time_wait.clone();
        let shutdown = shutdown.clone();
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
                        });
                        pool.add(link.clone());

                        let wr_task = tokio::spawn(drain_frames(rx, wr, link.clone()));

                        if let Err(e) =
                            tunnel_read_loop(rd, i, &conns, &pool, &link, &time_wait).await
                        {
                            warn!(tunnel = i, error = %e, "read loop ended");
                        }
                        link.alive.store(false, Ordering::Release);
                        wr_task.abort();
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
                tokio::time::sleep(std::time::Duration::from_secs(3)).await;
            }
        });
    }

    // Wait for at least one tunnel
    while pool.link_count() == 0 {
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    info!(listen = %cfg.listen_addr, tunnels = pool.link_count(), "splitter ready");

    // UDP datagram counters (shared with heartbeat and UDP relay)
    let udp_sent = Arc::new(AtomicU64::new(0));
    let udp_recv = Arc::new(AtomicU64::new(0));

    // Periodic heartbeat
    let start_time = Instant::now();
    let hb_pool = pool.clone();
    let hb_conns = conns.clone();
    let hb_udp_sent = udp_sent.clone();
    let hb_udp_recv = udp_recv.clone();
    let hb_time_wait = time_wait.clone();
    let hb_shutdown = shutdown.clone();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(60)).await;
            if hb_shutdown.load(Ordering::Acquire) {
                break;
            }
            let (alive, total) = hb_pool.stats();
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
                    return false;
                }
                // FIN-received connections: use short grace timeout (10s)
                // to prevent lingering after remote close.
                if vc.fin_received.load(Ordering::Acquire) {
                    let fin_idle = now
                        .duration_since(*vc.last_active.lock().unwrap())
                        .as_secs();
                    if fin_idle > 10 {
                        warn!(
                            conn_id = cid,
                            idle_secs = fin_idle,
                            "FIN-received connection timeout"
                        );
                        return false;
                    }
                    return true; // keep alive during grace period
                }
                // Idle timeout (TCP) or UDP timeout
                let idle = now
                    .duration_since(*vc.last_active.lock().unwrap())
                    .as_secs();
                let timeout = if cid == UDP_CONN_ID {
                    UDP_IDLE_TIMEOUT.as_secs()
                } else {
                    TCP_IDLE_TIMEOUT.as_secs()
                };
                if idle > timeout {
                    warn!(conn_id = cid, idle_secs = idle, "connection idle timeout");
                    vc.closed.store(true, Ordering::Release);
                    vc.notify.notify_one();
                    return false;
                }
                true
            });
            let uptime = start_time.elapsed().as_secs();
            info!(
                uptime,
                alive,
                total,
                active_conns = hb_conns.len(),
                udp_sent = hb_udp_sent.swap(0, Ordering::Relaxed),
                udp_recv = hb_udp_recv.swap(0, Ordering::Relaxed),
                "heartbeat"
            );
        }
    });

    // 2. Accept SOCKS5 clients
    let listener = TcpListener::bind(cfg.listen_addr).await?;

    // Max concurrent connections — prevent resource exhaustion.
    const MAX_CONCURRENT_CONNS: usize = 4096;

    loop {
        if shutdown.load(Ordering::Acquire) {
            info!("shutting down accept loop");
            return Ok(());
        }
        // Check connection limit before accepting (coarse but fast check)
        if conns.len() >= MAX_CONCURRENT_CONNS {
            tokio::time::sleep(Duration::from_millis(100)).await;
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
        // Random conn_id — collision probability ~ N²/2³² (< 0.01% at 1000 conns)
        let conn_id = loop {
            let id: u32 = rand::random();
            if id == 0 {
                continue; // reserved for UDP
            }
            if !conns.contains_key(&id) && !time_wait.contains_key(&id) {
                break id;
            }
        };
        let pool = pool.clone();
        let conns = conns.clone();
        let time_wait = time_wait.clone();
        let us = udp_sent.clone();
        let ur = udp_recv.clone();

        tokio::spawn(async move {
            let ctx = ClientCtx {
                conn_id,
                peer,
                pool: pool.clone(),
                conns: conns.clone(),
                time_wait: time_wait.clone(),
                chunk_size: cfg.chunk_size,
                udp_sent: us,
                udp_recv: ur,
            };
            if let Err(e) = handle_client(stream, ctx).await {
                warn!(conn_id, peer = %peer, error = %e, "client handler failed");
            }
        });
    }
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
    tunnel_idx: usize,
    conns: &ConnMap,
    pool: &TunnelPool,
    link: &TunnelLink,
    time_wait: &DashMap<u32, Instant>,
) -> Result<()> {
    let mut decoder = FrameDecoder::new();
    loop {
        let frame = match decoder.try_next(&mut rd).await? {
            Some(f) => f,
            None => return Ok(()),
        };
        let plen = frame.payload.len() as u64;
        handle_inbound_frame(frame, tunnel_idx, conns, pool, time_wait);
        link.bytes_recv.fetch_add(plen, Ordering::Relaxed);
        link.frames_recv.fetch_add(1, Ordering::Relaxed);
    }
}

// ── Inbound frame dispatch ────────────────────────────────────────────

fn handle_inbound_frame(
    frame: Frame,
    _tunnel_idx: usize,
    conns: &ConnMap,
    pool: &TunnelPool,
    time_wait: &DashMap<u32, Instant>,
) {
    if frame.flags & FLAG_SYN != 0 && frame.flags & FLAG_ACK != 0 {
        // SYN+ACK: handshake complete — handled by the pending oneshot in handle_client
        // Frame just arrives; the oneshot is triggered elsewhere after the initial SYN is sent.
        // ponytail: SYN+ACK frames are no-ops here; handle_client manages the handshake directly.
        return;
    }

    // Ignore control frames (FIN/RST/SYN) for UDP relay; only DATA is valid.
    if frame.conn_id == UDP_CONN_ID && frame.flags & FLAG_DATA == 0 {
        return;
    }

    if frame.flags & FLAG_DATA != 0 {
        if let Some(conn) = conns.get(&frame.conn_id) {
            conn.on_frame(frame.seq, frame.payload);
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
        // be in-flight on other tunnels.  Just signal the client loop and
        // let handle_tcp_client perform the actual removal after a grace
        // period so in-flight DATA is not dropped with RST.
        if let Some(conn) = conns.get(&frame.conn_id) {
            conn.fin_received.store(true, Ordering::Release);
            conn.closed.store(true, Ordering::Release);
            conn.notify.notify_one();
        }
        return;
    }

    if frame.flags & FLAG_RST != 0 {
        // RST = force-close, no grace period needed.
        if let Some((_, conn)) = conns.remove(&frame.conn_id) {
            time_wait.insert(frame.conn_id, Instant::now());
            conn.closed.store(true, Ordering::Release);
            conn.notify.notify_one();
            drop(conn);
        }
        return;
    }

    // ACK frames are ignored — TCP backpressure replaces application flow control.
    let _ = frame.flags & FLAG_ACK;
}

// ── Client handler ────────────────────────────────────────────────────

struct ClientCtx {
    conn_id: u32,
    peer: SocketAddr,
    pool: Arc<TunnelPool>,
    conns: ConnMap,
    time_wait: Arc<DashMap<u32, Instant>>,
    chunk_size: usize,
    udp_sent: Arc<AtomicU64>,
    udp_recv: Arc<AtomicU64>,
}

async fn handle_client(stream: TcpStream, ctx: ClientCtx) -> Result<()> {
    let accepted = socks5::socks5_server_accept(stream).await?;
    match accepted {
        socks5::Socks5Result::Connect(accepted) => {
            handle_tcp_client(
                ctx.conn_id,
                accepted,
                ctx.peer,
                &ctx.pool,
                &ctx.conns,
                &ctx.time_wait,
                ctx.chunk_size,
            )
            .await
        }
        socks5::Socks5Result::UdpAssociate {
            stream: keepalive,
            relay,
        } => {
            handle_udp_client(
                &ctx.pool,
                &ctx.conns,
                relay,
                keepalive,
                ctx.udp_sent.clone(),
                ctx.udp_recv.clone(),
            )
            .await
        }
    }
}

async fn handle_tcp_client(
    conn_id: u32,
    accepted: socks5::Socks5Accept,
    peer: SocketAddr,
    pool: &TunnelPool,
    conns: &ConnMap,
    time_wait: &DashMap<u32, Instant>,
    chunk_size: usize,
) -> Result<()> {
    info!(conn_id, peer = %peer, target = %accepted.target.address, port = accepted.target.port, "accepted");

    let syn_target = SynTarget {
        proto: frame::PROTO_TCP,
        address: accepted.target.address.clone(),
        port: accepted.target.port,
    };
    let syn_frame = Frame::syn(conn_id, syn_target.encode());

    if !pool.send(syn_frame) {
        bail!("no live tunnels to send SYN");
    }

    let (to_client_tx, to_client_rx) = mpsc::unbounded_channel();
    let vconn = Arc::new(VirtConn {
        to_client_tx,
        reorder: Mutex::new(ReorderBuf::new()),
        notify: tokio::sync::Notify::new(),
        closed: AtomicBool::new(false),
        fin_received: AtomicBool::new(false),
        created_at: Instant::now(),
        last_active: Mutex::new(Instant::now()),
        bytes_sent: AtomicU64::new(0),
        bytes_recv: AtomicU64::new(0),
        frames_sent: AtomicU64::new(0),
        frames_recv: AtomicU64::new(0),
    });
    let vconn2 = vconn.clone();
    conns.insert(conn_id, vconn2);

    let (mut client_reader, mut client_writer) = accepted.stream.into_split();

    let writer_task = tokio::spawn(async move {
        let mut rx = to_client_rx;
        while let Some(chunk) = rx.recv().await {
            if client_writer.write_all(&chunk).await.is_err() {
                break;
            }
        }
        let _ = client_writer.shutdown().await;
    });

    let mut seq: u64 = 1;
    let close_reason: &str;
    loop {
        // read_buf reads directly into BytesMut, allocating only what
        // the TCP stream delivers.  freeze() converts to Bytes with zero
        // copy — no separate allocation and no copy_from_slice.
        let mut buf = BytesMut::with_capacity(chunk_size);
        // Race client read against close notification.
        tokio::select! {
            result = client_reader.read_buf(&mut buf) => {
                match result {
                    Ok(0) => {
                        close_reason = "eof";
                        break;
                    }
                    Ok(n) => {
                        let frame = Frame::data(conn_id, seq, buf.freeze());
                        // Backpressure: if all tunnel channels are full, yield
                        // briefly to let drain_frames catch up instead of
                        // immediately killing the connection.
                        let mut sent = false;
                        for _ in 0..10 {
                            if pool.send(frame.clone()) {
                                sent = true;
                                break;
                            }
                            tokio::task::yield_now().await;
                        }
                        if !sent {
                            warn!(conn_id, "no live tunnels after retries, aborting");
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
                if vconn.closed.load(Ordering::Acquire) {
                    close_reason = if vconn.fin_received.load(Ordering::Acquire) {
                        "remote_fin"
                    } else {
                        "timeout"
                    };
                    break;
                }
                // FIN/RST notification — loop back to check closed flag
            }
        }
    }

    pool.send(Frame::fin(conn_id, seq));
    // Grace period: wait for late DATA frames on other tunnels before
    // removing from conns.  Without this, a FIN arriving on tunnel A
    // would cause DATA frames still in-flight on tunnel B to trigger
    // an RST response (data loss).
    const FIN_GRACE_MS: u64 = 3000;
    tokio::time::sleep(Duration::from_millis(FIN_GRACE_MS)).await;
    // Move to TIME_WAIT before removing from conns so a new random
    // conn_id won't collide before the grace period expires.
    time_wait.insert(conn_id, Instant::now());
    conns.remove(&conn_id);
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
/// Responses from reassembler arrive via handle_inbound_frame → conns[0] → relay socket.
async fn handle_udp_client(
    pool: &TunnelPool,
    conns: &ConnMap,
    relay: UdpSocket,
    keepalive: TcpStream,
    udp_sent: Arc<AtomicU64>,
    udp_recv: Arc<AtomicU64>,
) -> Result<()> {
    let relay = Arc::new(relay);
    let relay_addr = relay.local_addr()?;
    info!(addr = %relay_addr, "UDP relay started");

    let (to_udp_tx, mut to_udp_rx) = mpsc::unbounded_channel::<Bytes>();
    let vconn = Arc::new(VirtConn {
        to_client_tx: to_udp_tx,
        reorder: Mutex::new(ReorderBuf::new()),
        notify: tokio::sync::Notify::new(),
        closed: AtomicBool::new(false),
        fin_received: AtomicBool::new(false),
        created_at: Instant::now(),
        last_active: Mutex::new(Instant::now()),
        bytes_sent: AtomicU64::new(0),
        bytes_recv: AtomicU64::new(0),
        frames_sent: AtomicU64::new(0),
        frames_recv: AtomicU64::new(0),
    });
    conns.insert(UDP_CONN_ID, vconn);

    // Track SOCKS5 client address so we can send_to (socket is unconnected).
    let client_addr: Arc<Mutex<Option<SocketAddr>>> = Arc::new(Mutex::new(None));

    let relay2 = relay.clone();
    let ca = client_addr.clone();
    let recv_ctr = udp_recv.clone();
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
        let mut ka = keepalive;
        let mut buf = [0u8; 1];
        let _ = ka.read(&mut buf).await;
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
                *client_addr.lock().unwrap() = Some(client);
                udp_sent.fetch_add(1, Ordering::Relaxed);
                let frame = Frame::data(UDP_CONN_ID, seq, Bytes::copy_from_slice(&buf[..n]));
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
                seq = seq.wrapping_add(1);
            }
            _ = &mut ka_rx => {
                info!("UDP keepalive closed, ending relay");
                break;
            }
        }
    }
    conns.remove(&UDP_CONN_ID);
    Ok(())
}
