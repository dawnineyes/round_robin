use crate::frame::{
    FLAG_DATA, FLAG_FIN, FLAG_RST, FLAG_SYN, Frame, FrameDecoder, MAX_PAYLOAD, MAX_PENDING_BYTES,
    MAX_PENDING_CIDS, PROTO_TCP, PROTO_UDP, SynTarget,
};
use crate::reorder::ReorderBuf;
use crate::shutdown_signal;
use crate::socks5;
use crate::tunnel::{TUNNEL_CHANNEL_CAP, TunnelLink, TunnelPool, drain_frames};
use anyhow::Result;
use bytes::Bytes;
use dashmap::DashMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, UdpSocket};
use tokio::sync::{Notify, Semaphore, mpsc};
use tracing::{error, info, warn};

// ── Config ────────────────────────────────────────────────────────────

pub struct ReassemblerConfig {
    pub listen_ip: IpAddr,
    pub listen_ports: Vec<u16>,
    pub local_target: SocketAddr,
    pub chunk_size: usize,
    /// DATA send timeout (O5: configurable via `data_send_timeout_secs`;
    /// default 30s, previously the DATA_SEND_TIMEOUT constant).
    pub data_send_timeout: Duration,
    /// Heartbeat / connection-sweep interval (O5: previously hardcoded
    /// 60s; configurable so tests can exercise sweeps without waiting).
    pub heartbeat_interval: Duration,
}

// ── Tuning constants ──────────────────────────────────────────────────

/// Egress write queue capacity. ~32 MB worst-case backlog per connection
/// before the connection is reset (bounded — no unbounded growth).
const EGRESS_CHANNEL_CAP: usize = 512;
/// An egress write stalled for this long is a dead peer — give up.
const EGRESS_WRITE_TIMEOUT: Duration = Duration::from_secs(60);
/// After the splitter's FIN, force-close the egress write half if the
/// seq gap never fills (a tunnel died and frames are gone).
const HALF_CLOSE_FALLBACK: Duration = Duration::from_secs(10);
/// Tombstone TTL for closed conn_ids (late DATA gets RST instead of
/// being buffered forever).
const CLOSED_TTL: Duration = Duration::from_secs(60);
/// Cap tunnel links per reassembler to bound per-link task/memory usage.
const MAX_TUNNEL_LINKS: usize = 64;
/// B29: handshaking flags outlive any possible SYN handshake (egress
/// connect timeout is 10s) — sweep entries older than this.
const HANDSHAKING_TTL: Duration = Duration::from_secs(120);
/// B46: cap concurrent SYN handshakes spawned off the tunnel read loops.
/// Each holds an egress connect (≤10s); the cap bounds tasks + sockets
/// under a SYN flood, and at cap the SYN is handled inline (old behavior).
const MAX_CONCURRENT_SYN_HANDSHAKES: usize = 64;

// ── Egress connection ─────────────────────────────────────────────────

struct EgressConn {
    write_tx: mpsc::Sender<Bytes>,
}

impl EgressConn {
    fn write(&self, data: Bytes) -> bool {
        self.write_tx.try_send(data).is_ok()
    }
}

impl VirtConnDe {
    /// Egress write channel — TCP conns only (UDP conns store `None`).
    fn egress(&self) -> &EgressConn {
        self.egress
            .as_ref()
            .expect("UDP conn has no egress channel")
    }
}

/// B50: every teardown path (RST / idle sweep / overflow / egress write
/// failure / egress send failure) must wake BOTH egress tasks.  Each
/// Notify has exactly one waiter, so `notify_one` stores a permit if the
/// task has not registered yet — no lost-wakeup window (unlike a shared
/// Notify, where the second waiter would miss the single wakeup).
/// UDP conns have no egress writer; the stored permit is dropped with
/// the Arc, which is harmless.
fn signal_teardown(vc: &VirtConnDe) {
    vc.cancel.notify_one();
    vc.cancel_writer.notify_one();
}

// ── Virtual connection (reassembler side) ─────────────────────────────

struct VirtConnDe {
    /// Egress write channel.  `None` for UDP conns — datagrams bypass
    /// the channel (O7: the UDP path used to allocate a cap-1 channel
    /// whose receiver was dropped immediately, a dead allocation).
    egress: Option<EgressConn>,
    reorder: Mutex<ReorderBuf>,
    /// Teardown signal for the egress reader / UDP response reader
    /// (RST / idle sweep / overflow).
    cancel: Arc<Notify>,
    /// B50: teardown signal for the egress writer.  Reader and writer
    /// used to share one Notify, but `notify_one` wakes only a single
    /// waiter — the loser of the race relied on the peer closing the
    /// connection (indefinite task+fd leak on a stalled target) or
    /// drained up to 32 MB of stale chunks to the abandoned target
    /// (B48 regression).  One Notify per task keeps `notify_one`'s
    /// stored-permit semantics race-free (single waiter each).
    cancel_writer: Arc<Notify>,
    /// Half-close signal: shut the egress write half down.
    half_close: Arc<Notify>,
    /// FIN received from the splitter (half-close in progress).
    fin_received: AtomicBool,
    /// FIN's seq = next_seq: all frames below it must be delivered
    /// before the write half may close.
    fin_seq: AtomicU64,
    /// BUG-5: half-close state machine: 0=open, 1=closing, 2=closed.
    /// A plain bool had a swap/store race with the 10s force-close
    /// timer that could lose the force-close entirely.
    half_close_state: AtomicU8,
    /// UDP relay conn (SYN proto=UDP): DATA goes straight to the UDP
    /// socket, no reorder, no egress TCP stream.  Each UDP conn owns its
    /// socket so responses from the same target can be told apart per
    /// client (BUG-19: a shared (target→conn) route table breaks when two
    /// clients talk to the same target).
    is_udp: bool,
    udp_sock: Option<Arc<UdpPair>>,
    /// D3: the egress reader hit EOF (target half-closed its write side).
    /// The conn must stay alive — the splitter may keep sending — until
    /// the splitter's FIN closes the write half too.
    egress_eof: AtomicBool,
    created_at: Instant,
    last_active: Mutex<Instant>,
    bytes_sent: AtomicU64,
    bytes_recv: AtomicU64,
    frames_sent: AtomicU64,
    frames_recv: AtomicU64,
}

/// Idle timeout constants for automatic connection cleanup.
const TCP_IDLE_TIMEOUT: Duration = Duration::from_secs(300);
const UDP_IDLE_TIMEOUT: Duration = Duration::from_secs(60);

type ConnMap = Arc<DashMap<u32, Arc<VirtConnDe>>>;

/// Frames that arrived before the SYN handler finished creating the VirtConnDe.
struct PendingEntry {
    frames: Vec<Frame>,
    since: Instant,
    /// BUG-7: bytes buffered, counted against the global pending budget.
    bytes: usize,
    /// BUG-4: RST arrived while the SYN handshake was in flight — the
    /// SYN handler must abort the egress connect instead of building a
    /// connection nobody wants.
    cancelled: bool,
    /// Lets the RST handler wake the SYN handler's connect future.
    cancel: Option<Arc<Notify>>,
}

impl PendingEntry {
    fn new() -> Self {
        Self {
            frames: Vec::new(),
            since: Instant::now(),
            bytes: 0,
            cancelled: false,
            cancel: None,
        }
    }
}

/// Max frames buffered per CID before the SYN handshake completes.
const MAX_PENDING_FRAMES_PER_CID: usize = 256;
/// Drop stale pending entries that never received a SYN.
const PENDING_TTL_SECS: u64 = 30;

type PendingMap = Arc<DashMap<u32, PendingEntry>>;

// ── Main entry ────────────────────────────────────────────────────────

pub async fn run_reassembler(cfg: ReassemblerConfig) -> Result<()> {
    let conns: ConnMap = Arc::new(DashMap::new());
    let pending: PendingMap = Arc::new(DashMap::new());
    let closed: Arc<DashMap<u32, Instant>> = Arc::new(DashMap::new());
    let handshaking: Arc<DashMap<u32, Instant>> = Arc::new(DashMap::new());
    let pool = Arc::new(TunnelPool::new());
    // BUG-7: global byte budget for DATA-before-SYN buffering.
    let pending_bytes: Arc<AtomicUsize> = Arc::new(AtomicUsize::new(0));
    // Connection reset counter (observability: logged by the heartbeat).
    let resets: Arc<AtomicU64> = Arc::new(AtomicU64::new(0));
    // B46: bound concurrent SYN handshakes spawned off the tunnel loops.
    let syn_limit: Arc<Semaphore> = Arc::new(Semaphore::new(MAX_CONCURRENT_SYN_HANDSHAKES));

    // B28: the legacy conn-0 single-client UDP relay was removed — every
    // UDP association gets its own conn_id + socket pair (BUG-19 design).

    // Spawn a listener for each port
    for &port in &cfg.listen_ports {
        let conns = conns.clone();
        let pending = pending.clone();
        let closed = closed.clone();
        let handshaking = handshaking.clone();
        let pool = pool.clone();
        let local_target = cfg.local_target;
        let listen_ip = cfg.listen_ip;
        let pending_bytes = pending_bytes.clone();
        let resets = resets.clone();
        let syn_limit = syn_limit.clone();
        let lctx = ListenerCtx {
            listen_ip,
            local_target,
            conns,
            pending,
            closed,
            handshaking,
            pool,
            chunk_size: cfg.chunk_size,
            data_send_timeout: cfg.data_send_timeout,
            pending_bytes,
            resets,
            syn_limit,
        };
        tokio::spawn(async move {
            if let Err(e) = run_tunnel_listener(port, lctx).await {
                error!(port, error = %e, "listener died");
            }
        });
    }

    info!(ports = ?cfg.listen_ports, egress = %cfg.local_target, "reassembler ready");

    // Periodic heartbeat
    let start_time = Instant::now();
    let hb_pool = pool.clone();
    let hb_conns = conns.clone();
    let hb_pending = pending.clone();
    let hb_closed = closed.clone();
    let hb_pending_bytes = pending_bytes.clone();
    let hb_resets = resets.clone();
    let hb_handshaking = handshaking.clone();
    tokio::spawn(async move {
        loop {
            // O5: configurable heartbeat interval (default 60s).
            tokio::time::sleep(cfg.heartbeat_interval).await;
            let (alive, total) = hb_pool.stats();
            let queue_depth = hb_pool.queue_depth();
            // Sweep dead links that accumulated from tunnel reconnects
            hb_pool.compact();
            // Sweep idle connections
            let now = Instant::now();
            hb_conns.retain(|&cid, vc| {
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
                    hb_closed.insert(cid, Instant::now());
                    signal_teardown(vc);
                    return false;
                }
                true
            });
            // Sweep closed-cid tombstones
            hb_closed.retain(|_, since| now.duration_since(*since) < CLOSED_TTL);
            // B29: sweep handshaking flags that outlived any possible
            // SYN handshake (only leaked by panic/bind-failure paths).
            let handshaking_before = hb_handshaking.len();
            hb_handshaking.retain(|_, since| now.duration_since(*since) < HANDSHAKING_TTL);
            if hb_handshaking.len() != handshaking_before {
                warn!(
                    swept = handshaking_before - hb_handshaking.len(),
                    "stale SYN handshakes swept"
                );
            }
            // Sweep stale pending entries that never got a SYN, refunding
            // their byte budget (BUG-7) and resetting the affected cids
            // (B34 — the buffered frames are gone, so fail fast instead
            // of letting the splitter re-buffer into a new entry forever).
            sweep_stale_pending(
                &hb_pending,
                &hb_pending_bytes,
                &hb_closed,
                &hb_pool,
                PENDING_TTL_SECS,
            );
            let uptime = start_time.elapsed().as_secs();
            info!(
                uptime,
                alive,
                total,
                queue_depth,
                active_conns = hb_conns.len(),
                pending_cids = hb_pending.len(),
                pending_bytes = hb_pending_bytes.load(Ordering::Relaxed),
                handshaking = hb_handshaking.len(),
                resets = hb_resets.swap(0, Ordering::Relaxed),
                "heartbeat"
            );
        }
    });

    // Keep alive (BUG-13: SIGTERM on unix too, not just Ctrl+C)
    shutdown_signal().await;
    info!("shutting down");
    Ok(())
}

/// Normalize IPv4-mapped IPv6 addresses from dual-stack sockets back to
/// plain IPv4 so datagram headers stay compatible (BUG-18).
fn normalize_ip(ip: IpAddr) -> String {
    match ip {
        IpAddr::V6(v6) => match v6.to_ipv4_mapped() {
            Some(v4) => v4.to_string(),
            None => v6.to_string(),
        },
        IpAddr::V4(v4) => v4.to_string(),
    }
}

/// A v4 + optional v6 UDP socket pair (BUG-18).  A single `[::]` socket
/// can't reliably carry IPv4 traffic on Windows (IPV6_V6ONLY defaults on,
/// and v4-mapped sendto fails with WSAEADDRNOTAVAIL), so each family gets
/// its own socket.
struct UdpPair {
    v4: UdpSocket,
    v6: Option<UdpSocket>,
}

async fn bind_udp_pair() -> anyhow::Result<UdpPair> {
    let v4 = UdpSocket::bind("0.0.0.0:0").await?;
    let v6 = match UdpSocket::bind("[::]:0").await {
        Ok(s) => Some(s),
        Err(e) => {
            warn!(error = %e, "IPv6 UDP bind failed, IPv6 targets unavailable");
            None
        }
    };
    Ok(UdpPair { v4, v6 })
}

/// B49: domain resolution inside `UdpPair::send_to` runs inline on the
/// tunnel read loop (UDP DATA path).  An unresponsive resolver must not
/// head-of-line block every other connection on the tunnel — bound it.
const UDP_DNS_TIMEOUT: Duration = Duration::from_secs(5);

/// Resolve a domain target within `timeout_dur`.  Separated so the
/// timeout path is deterministically testable (a blocking-pool DNS round
/// trip can never complete within the first poll of `timeout(Duration::ZERO)`).
async fn resolve_target(host: &str, port: u16, timeout_dur: Duration) -> anyhow::Result<IpAddr> {
    let addrs = tokio::time::timeout(timeout_dur, tokio::net::lookup_host((host, port)))
        .await
        .map_err(|_| anyhow::anyhow!("UDP DNS resolution timed out for {host}"))??;
    addrs
        .map(|a| a.ip())
        .next()
        .ok_or_else(|| anyhow::anyhow!("UDP target resolved to no address: {host}"))
}

impl UdpPair {
    /// Send to `host:port`, choosing the socket matching the family.
    async fn send_to(&self, host: &str, port: u16, data: &[u8]) -> anyhow::Result<()> {
        let ip: IpAddr = match host.parse() {
            Ok(ip) => ip,
            Err(_) => {
                // Domain name: resolve (UDP datagram targets are almost
                // always IP literals, but keep domain support).
                resolve_target(host, port, UDP_DNS_TIMEOUT).await?
            }
        };
        let addr = SocketAddr::new(ip, port);
        match addr {
            SocketAddr::V4(_) => {
                self.v4.send_to(data, addr).await?;
            }
            SocketAddr::V6(_) => {
                let sock = self
                    .v6
                    .as_ref()
                    .ok_or_else(|| anyhow::anyhow!("no IPv6 UDP socket for {host}"))?;
                sock.send_to(data, addr).await?;
            }
        }
        Ok(())
    }
}

struct ListenerCtx {
    listen_ip: IpAddr,
    local_target: SocketAddr,
    conns: ConnMap,
    pending: PendingMap,
    closed: Arc<DashMap<u32, Instant>>,
    handshaking: Arc<DashMap<u32, Instant>>,
    pool: Arc<TunnelPool>,
    chunk_size: usize,
    /// O5: per-connection DATA send timeout (configurable).
    data_send_timeout: Duration,
    pending_bytes: Arc<AtomicUsize>,
    resets: Arc<AtomicU64>,
    /// B46: bounds concurrent spawned SYN handshakes.
    syn_limit: Arc<Semaphore>,
}

async fn run_tunnel_listener(port: u16, ctx: ListenerCtx) -> Result<()> {
    let listener = TcpListener::bind((ctx.listen_ip, port)).await?;
    info!(listen = %ctx.listen_ip, port, "tunnel listener ready");

    loop {
        // Transient accept errors (EMFILE etc.) must not kill the
        // listener permanently — retry like the splitter does.
        let (stream, peer) = loop {
            match listener.accept().await {
                Ok(v) => break v,
                Err(e) => {
                    warn!(port, error = %e, "accept failed, retrying in 100ms");
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        };
        let _ = stream.set_nodelay(true);

        // Raw TCP — sing-box direct outbound connects here.
        // No SOCKS5 handshake needed; TUIC streams carry their own target.

        // Cap tunnel links to bound per-link task/memory usage.
        // BUG-17: count only alive links — dead ones haven't been
        // compacted yet and used to reject healthy reconnects.
        if ctx.pool.alive_count() >= MAX_TUNNEL_LINKS {
            warn!(peer = %peer, port, "too many tunnel links, dropping");
            continue;
        }

        info!(peer = %peer, port, pool_size = ctx.pool.link_count() + 1, "tunnel link accepted");

        let (rd, wr) = stream.into_split();
        let (tx, rx) = mpsc::channel::<Frame>(TUNNEL_CHANNEL_CAP);
        let link = Arc::new(TunnelLink {
            tx,
            alive: AtomicBool::new(true),
            bytes_sent: AtomicU64::new(0),
            bytes_recv: AtomicU64::new(0),
            frames_sent: AtomicU64::new(0),
            frames_recv: AtomicU64::new(0),
            stop: Arc::new(Notify::new()),
            writer_died: Arc::new(Notify::new()),
            lost_frames: Mutex::new(Vec::new()),
            rate_bps: AtomicU64::new(0),
        });
        ctx.pool.add(link.clone());

        // Writer task. D1: when the link dies, frames that were queued
        // but never written are lost — RST the affected conns (resend
        // control frames) instead of letting the splitter stall until
        // its reorder window overflows.
        let link2 = link.clone();
        let lost_pool = ctx.pool.clone();
        tokio::spawn(async move {
            drain_frames(rx, wr, link2.clone()).await;
            let lost = std::mem::take(&mut *link2.lost_frames.lock().unwrap());
            for f in lost {
                if f.flags & (FLAG_SYN | FLAG_FIN | FLAG_RST) != 0 {
                    lost_pool.send(f);
                } else {
                    lost_pool.send(Frame::rst(f.conn_id));
                }
            }
        });

        // Reader task (one per link)
        let reader_ctx = ReadLoopCtx {
            conns: ctx.conns.clone(),
            pending: ctx.pending.clone(),
            closed: ctx.closed.clone(),
            handshaking: ctx.handshaking.clone(),
            pool: ctx.pool.clone(),
            local_target: ctx.local_target,
            chunk_size: ctx.chunk_size,
            data_send_timeout: ctx.data_send_timeout,
            pending_bytes: ctx.pending_bytes.clone(),
            resets: ctx.resets.clone(),
            link: link.clone(),
            syn_limit: ctx.syn_limit.clone(),
        };
        tokio::spawn(async move {
            if let Err(e) = tunnel_read_loop(rd, reader_ctx).await {
                warn!(tunnel = port, error = %e, "read loop ended");
            }
            link.alive.store(false, Ordering::Release);
            // B22: wake the drain task — without this it can only exit
            // when every Sender is dropped, and it holds the last Arc
            // itself → permanent task + socket leak per dead tunnel.
            link.stop.notify_one();
            info!(
                tunnel = port,
                bytes_sent = link.bytes_sent.load(Ordering::Relaxed),
                bytes_recv = link.bytes_recv.load(Ordering::Relaxed),
                frames_sent = link.frames_sent.load(Ordering::Relaxed),
                frames_recv = link.frames_recv.load(Ordering::Relaxed),
                "disconnected"
            );
        });
    }
}

#[derive(Clone)]
struct ReadLoopCtx {
    conns: ConnMap,
    pending: PendingMap,
    closed: Arc<DashMap<u32, Instant>>,
    handshaking: Arc<DashMap<u32, Instant>>,
    pool: Arc<TunnelPool>,
    local_target: SocketAddr,
    chunk_size: usize,
    /// O5: per-connection DATA send timeout (configurable).
    data_send_timeout: Duration,
    pending_bytes: Arc<AtomicUsize>,
    resets: Arc<AtomicU64>,
    link: Arc<TunnelLink>,
    /// B46: bounds concurrent spawned SYN handshakes.
    syn_limit: Arc<Semaphore>,
}

async fn tunnel_read_loop(mut rd: tokio::net::tcp::OwnedReadHalf, ctx: ReadLoopCtx) -> Result<()> {
    let mut decoder = FrameDecoder::new();
    loop {
        // B56: exit when the drain task died on its own (write stall =
        // tunnel dead).  A silently dead peer would otherwise keep this
        // task (and its socket half) blocked in read until the TCP
        // stack's RTO gives up — minutes instead of the 60s bound.
        let frame = match tokio::select! {
            r = decoder.try_next(&mut rd) => r?,
            _ = ctx.link.writer_died.notified() => return Ok(()),
        } {
            Some(f) => f,
            None => return Ok(()),
        };
        let plen = frame.payload.len() as u64;
        dispatch_frame(frame, &ctx).await?;
        ctx.link.bytes_recv.fetch_add(plen, Ordering::Relaxed);
        ctx.link.frames_recv.fetch_add(1, Ordering::Relaxed);
    }
}

/// B46: dispatch a decoded frame to its handler.  SYN frames are handled
/// on a bounded concurrent task — the egress connect inside can stall up
/// to 10s, and handling it inline used to head-of-line block this
/// tunnel's read loop (every other conn on the tunnel waited behind it).
/// DATA/FIN/RST handlers are cheap and stay inline (per-cid ordering is
/// preserved by the pending/reorder machinery either way — that same
/// machinery already tolerates SYN vs DATA racing across tunnels).
/// handle_frame never returns an error, so swallowing it in the spawned
/// task is safe.
async fn dispatch_frame(frame: Frame, ctx: &ReadLoopCtx) -> Result<()> {
    if frame.flags & FLAG_SYN != 0 {
        match ctx.syn_limit.clone().try_acquire_owned() {
            Ok(permit) => {
                let ctx2 = ctx.clone();
                tokio::spawn(async move {
                    let _permit = permit;
                    let _ = handle_frame(frame, &ctx2).await;
                });
                return Ok(());
            }
            Err(_) => {
                // SYN flood — degrade to inline handling (old behavior)
                // rather than dropping the connection.
                warn!(
                    conn_id = frame.conn_id,
                    "SYN handshake limit reached, handling inline"
                );
            }
        }
    }
    handle_frame(frame, ctx).await
}

// ── Frame handler ─────────────────────────────────────────────────────

async fn handle_frame(frame: Frame, ctx: &ReadLoopCtx) -> Result<()> {
    let cid = frame.conn_id;

    // SYN: new virtual connection
    if frame.flags & FLAG_SYN != 0 {
        // Duplicate SYN on an established connection — nothing to do.
        if ctx.conns.contains_key(&cid) {
            warn!(
                conn_id = cid,
                "duplicate SYN on established connection, ignoring"
            );
            return Ok(());
        }
        // One SYN handshake per cid at a time; a duplicate SYN racing on
        // a different tunnel would otherwise spawn a second egress
        // connection and overwrite the conn entry.
        if ctx.handshaking.insert(cid, Instant::now()).is_some() {
            warn!(conn_id = cid, "duplicate SYN while handshaking, ignoring");
            return Ok(());
        }

        // Reserve a pending slot so DATA/FIN arriving during the handshake
        // are queued instead of dropped.  Use entry API so we don't
        // overwrite frames that already arrived before the SYN.  The
        // cancel Notify lets an RST abort the in-flight egress connect.
        let hshake_cancel = Arc::new(Notify::new());
        {
            let mut entry = ctx.pending.entry(cid).or_insert_with(PendingEntry::new);
            entry.cancel = Some(hshake_cancel.clone());
        }
        // B27/B23: an RST (or a pending-drop fail-fast) may have
        // tombstoned this cid while the SYN was in flight — don't build
        // an egress connection for a conn the splitter already reset.
        if ctx.closed.contains_key(&cid) {
            warn!(conn_id = cid, "SYN for closed cid, ignoring");
            remove_pending(ctx, &cid);
            ctx.handshaking.remove(&cid);
            return Ok(());
        }

        // Parse target from SYN payload
        let syn_target = match SynTarget::decode(&frame.payload) {
            Ok(t) => t,
            Err(e) => {
                warn!(conn_id = cid, error = %e, "SYN decode failed");
                remove_pending(ctx, &cid);
                ctx.handshaking.remove(&cid);
                // Tombstone so late DATA gets RST instead of a zombie
                // pending entry.
                ctx.closed.insert(cid, Instant::now());
                ctx.pool.send(Frame::rst(cid));
                return Ok(());
            }
        };
        info!(conn_id = cid, target = %syn_target.address, proto = syn_target.proto, "SYN");

        // B30: only TCP and UDP are meaningful protos — reject anything
        // else instead of silently treating it as TCP.
        if syn_target.proto != PROTO_TCP && syn_target.proto != PROTO_UDP {
            warn!(
                conn_id = cid,
                proto = syn_target.proto,
                "unknown SYN proto, resetting"
            );
            remove_pending(ctx, &cid);
            ctx.handshaking.remove(&cid);
            ctx.closed.insert(cid, Instant::now());
            ctx.pool.send(Frame::rst(cid));
            return Ok(());
        }

        // BUG-19: UDP association — no egress TCP stream at all. DATA
        // frames go straight to a per-conn UDP socket; a per-conn reader
        // sends responses back with this conn_id (so two clients talking
        // to the same target can't cross their datagrams).
        if syn_target.proto == PROTO_UDP {
            let cancel = Arc::new(Notify::new());
            // B50: separate per-task teardown Notify (no waiter here —
            // UDP conns have no egress writer; see signal_teardown).
            let cancel_writer = Arc::new(Notify::new());
            let half_close = Arc::new(Notify::new());
            // B38: a UDP socket bind failure must not propagate out of
            // handle_frame — the `?` used to kill the entire tunnel read
            // loop (all conns on the tunnel stall for the 3s+ reconnect)
            // for one unusable association.  Fail just this cid instead.
            let udp_sock = match bind_udp_pair().await {
                Ok(s) => Arc::new(s),
                Err(e) => {
                    warn!(
                        conn_id = cid,
                        error = %e,
                        "UDP socket bind failed, resetting connection"
                    );
                    remove_pending(ctx, &cid);
                    ctx.handshaking.remove(&cid);
                    ctx.closed.insert(cid, Instant::now());
                    ctx.pool.send(Frame::rst(cid));
                    return Ok(());
                }
            };
            let vconn = Arc::new(VirtConnDe {
                egress: None, // O7: UDP datagrams bypass the egress channel
                reorder: Mutex::new(ReorderBuf::new()),
                cancel: cancel.clone(),
                cancel_writer: cancel_writer.clone(),
                half_close: half_close.clone(),
                fin_received: AtomicBool::new(false),
                fin_seq: AtomicU64::new(0),
                half_close_state: AtomicU8::new(0),
                is_udp: true,
                udp_sock: Some(udp_sock.clone()),
                egress_eof: AtomicBool::new(false),
                created_at: Instant::now(),
                last_active: Mutex::new(Instant::now()),
                bytes_sent: AtomicU64::new(0),
                bytes_recv: AtomicU64::new(0),
                frames_sent: AtomicU64::new(0),
                frames_recv: AtomicU64::new(0),
            });
            ctx.conns.insert(cid, vconn.clone());

            // Per-conn response reader: wraps replies in SOCKS5 UDP
            // headers and sends them to the splitter with this conn_id.
            // Splitter UDP relays don't reorder, so a plain monotonic
            // counter is enough even across the v4/v6 sockets.
            let cancel_r = cancel.clone();
            let pool_r = ctx.pool.clone();
            let udp_r = udp_sock.clone();
            let conns_r = ctx.conns.clone();
            tokio::spawn(async move {
                let mut buf4 = vec![0u8; 65535];
                let mut buf6 = vec![0u8; 65535];
                let mut seq: u64 = 1;
                loop {
                    let (n, src) = tokio::select! {
                        _ = cancel_r.notified() => break,
                        r = udp_r.v4.recv_from(&mut buf4) => match r {
                            Ok(v) => v,
                            Err(e) => {
                                warn!(conn_id = cid, error = %e, "UDP conn recv error (v4)");
                                break;
                            }
                        },
                        r = async {
                            match &udp_r.v6 {
                                Some(s) => s.recv_from(&mut buf6).await,
                                None => std::future::pending().await,
                            }
                        } => match r {
                            Ok(v) => v,
                            Err(e) => {
                                warn!(conn_id = cid, error = %e, "UDP conn recv error (v6)");
                                break;
                            }
                        },
                    };
                    let src_target = socks5::TargetAddr {
                        address: normalize_ip(src.ip()),
                        port: src.port(),
                    };
                    let payload = if src.is_ipv4() {
                        &buf4[..n]
                    } else {
                        &buf6[..n]
                    };
                    let dgram = match socks5::encode_udp_datagram(&src_target, payload) {
                        Ok(d) => d,
                        Err(e) => {
                            warn!(conn_id = cid, error = %e, "UDP encode failed");
                            continue;
                        }
                    };
                    if dgram.len() > MAX_PAYLOAD {
                        warn!(
                            conn_id = cid,
                            len = dgram.len(),
                            "UDP datagram exceeds frame capacity, dropping"
                        );
                        continue;
                    }
                    let dgram_len = dgram.len() as u64;
                    let frame = Frame::data(cid, seq, dgram);
                    if pool_r.send(frame) {
                        seq = seq.wrapping_add(1);
                        // B29: count responses and refresh activity so a
                        // busy relay isn't idle-swept mid-flight.
                        if let Some(vc) = conns_r.get(&cid) {
                            vc.bytes_sent.fetch_add(dgram_len, Ordering::Relaxed);
                            vc.frames_sent.fetch_add(1, Ordering::Relaxed);
                            *vc.last_active.lock().unwrap() = Instant::now();
                        }
                    } else {
                        warn!(
                            conn_id = cid,
                            "UDP relay: no live tunnels, dropping response datagram"
                        );
                    }
                }
            });

            // Drain DATA frames that beat the SYN; datagrams need no
            // ordering, just forward each one.
            if let Some(entry) = remove_pending(ctx, &cid) {
                if entry.cancelled {
                    if let Some((_, vc)) = ctx.conns.remove(&cid) {
                        signal_teardown(&vc);
                        drop(vc);
                    }
                    ctx.closed.insert(cid, Instant::now());
                    ctx.handshaking.remove(&cid);
                    return Ok(());
                }
                for f in entry.frames {
                    if f.flags & FLAG_DATA != 0 {
                        forward_udp_datagram(&vconn, &f.payload).await;
                    }
                }
            }
            ctx.handshaking.remove(&cid);
            return Ok(());
        }

        // Connect to local_target via SOCKS5 (with timeout). BUG-4: race
        // the connect against the cancel Notify so an RST aborts it.
        let connect_fut = tokio::time::timeout(
            Duration::from_secs(10),
            socks5::socks5_client_connect(ctx.local_target, &syn_target.address, syn_target.port),
        );
        let egress_stream = tokio::select! {
            r = connect_fut => match r {
                Ok(Ok(s)) => s,
                Ok(Err(e)) => {
                    warn!(conn_id = cid, target = %syn_target.address, error = %e, "egress connect failed");
                    remove_pending(ctx, &cid);
                    ctx.handshaking.remove(&cid);
                    // B41: tombstone like every other SYN failure path —
                    // without it, late DATA frames for this cid (still in
                    // flight on other tunnels) would create a zombie
                    // pending entry that eats the byte budget for 30s and
                    // can evict healthy entries before the TTL sweep.
                    ctx.closed.insert(cid, Instant::now());
                    ctx.pool.send(Frame::rst(cid));
                    return Ok(());
                }
                Err(_) => {
                    warn!(conn_id = cid, target = %syn_target.address, "egress connect timeout");
                    remove_pending(ctx, &cid);
                    ctx.handshaking.remove(&cid);
                    // B41: see above.
                    ctx.closed.insert(cid, Instant::now());
                    ctx.pool.send(Frame::rst(cid));
                    return Ok(());
                }
            },
            _ = hshake_cancel.notified() => {
                warn!(conn_id = cid, "SYN handshake cancelled by RST");
                remove_pending(ctx, &cid);
                ctx.handshaking.remove(&cid);
                ctx.closed.insert(cid, Instant::now());
                return Ok(());
            }
        };
        let _ = egress_stream.set_nodelay(true);

        // B33/B36: an RST (or pending eviction/sweep) may have raced the
        // connect and tombstoned this cid after the pre-connect check —
        // don't build an egress connection for a conn the splitter
        // already reset.
        if ctx.closed.contains_key(&cid) {
            warn!(
                conn_id = cid,
                "SYN closed while connecting, dropping egress"
            );
            remove_pending(ctx, &cid);
            ctx.handshaking.remove(&cid);
            return Ok(());
        }

        let (egress_rd, egress_wr) = egress_stream.into_split();
        let (write_tx, write_rx) = mpsc::channel::<Bytes>(EGRESS_CHANNEL_CAP);
        let cancel = Arc::new(Notify::new());
        // B50: the writer gets its own teardown Notify — reader and
        // writer sharing one Notify meant every teardown woke only one
        // of them (notify_one wakes a single waiter); the loser leaked
        // until the target closed or drained stale data.
        let cancel_writer = Arc::new(Notify::new());
        let half_close = Arc::new(Notify::new());

        let vconn = Arc::new(VirtConnDe {
            egress: Some(EgressConn { write_tx }),
            reorder: Mutex::new(ReorderBuf::new()),
            cancel: cancel.clone(),
            cancel_writer: cancel_writer.clone(),
            half_close: half_close.clone(),
            fin_received: AtomicBool::new(false),
            fin_seq: AtomicU64::new(0),
            half_close_state: AtomicU8::new(0),
            is_udp: false,
            udp_sock: None,
            egress_eof: AtomicBool::new(false),
            created_at: Instant::now(),
            last_active: Mutex::new(Instant::now()),
            bytes_sent: AtomicU64::new(0),
            bytes_recv: AtomicU64::new(0),
            frames_sent: AtomicU64::new(0),
            frames_recv: AtomicU64::new(0),
        });

        // Insert before spawning the I/O tasks so an instant egress EOF
        // can't race past the insert and orphan the entry.
        ctx.conns.insert(cid, vconn.clone());

        // Spawn egress writer: ordered data → egress connection
        tokio::spawn(write_to_egress(
            write_rx,
            egress_wr,
            half_close,
            cancel_writer,
        ));

        // Spawn egress reader: egress response → frames → pool
        tokio::spawn(read_from_egress(
            egress_rd,
            EgressReaderCtx {
                conn_id: cid,
                conns: ctx.conns.clone(),
                pool: ctx.pool.clone(),
                chunk_size: ctx.chunk_size,
                cancel,
                closed: ctx.closed.clone(),
                vconn: vconn.clone(),
                // O5: configurable, default 30s.
                data_send_timeout: ctx.data_send_timeout,
            },
        ));

        // Drain any frames that arrived during SOCKS5 connect
        if let Some(entry) = remove_pending(ctx, &cid) {
            if entry.cancelled {
                // BUG-4: RST raced the connect — tear the fresh conn down.
                if let Some((_, vc)) = ctx.conns.remove(&cid) {
                    signal_teardown(&vc);
                    drop(vc);
                }
                ctx.closed.insert(cid, Instant::now());
                ctx.handshaking.remove(&cid);
                return Ok(());
            }
            let mut fin_seq: Option<u64> = None;
            for f in entry.frames {
                if f.flags & FLAG_DATA != 0 {
                    // Hold the reorder lock across push + egress writes —
                    // concurrent tunnel deliveries must not interleave
                    // their ready chunks.
                    let mut reorder = vconn.reorder.lock().unwrap();
                    let result = reorder.push(f.seq, f.payload);
                    // Pending frames are replayed — stats already counted when
                    // originally queued, so don't double-count here.
                    let mut write_failed = false;
                    for chunk in result.ready {
                        if !vconn.egress().write(chunk) {
                            warn!(conn_id = cid, "egress write failed (drain)");
                            write_failed = true;
                            break;
                        }
                    }
                    drop(reorder);
                    if write_failed {
                        break;
                    }
                    if vconn.fin_received.load(Ordering::Acquire) {
                        close_write_half(&vconn, cid, false);
                    }
                } else if f.flags & FLAG_FIN != 0 {
                    fin_seq = Some(f.seq);
                }
            }
            if let Some(seq) = fin_seq {
                info!(conn_id = cid, "FIN during SYN, half-closing");
                start_half_close(&vconn, seq, cid);
                finish_if_done(&vconn, cid, &ctx.conns, &ctx.closed);
            }
        }

        ctx.handshaking.remove(&cid);
        return Ok(());
    }

    // DATA
    if frame.flags & FLAG_DATA != 0 {
        if let Some(vconn) = ctx.conns.get(&cid) {
            if vconn.is_udp {
                // BUG-19: UDP relay conn — forward the datagram directly
                // through the conn's own socket (no ordering, no routes).
                // B37: clone out of the DashMap shard guard before the
                // await — a domain target triggers DNS inside
                // forward_udp_datagram, and holding the guard across it
                // would stall every other conn in this shard.
                let vc = vconn.clone();
                drop(vconn);
                forward_udp_datagram(&vc, &frame.payload).await;
                vc.bytes_recv
                    .fetch_add(frame.payload.len() as u64, Ordering::Relaxed);
                vc.frames_recv.fetch_add(1, Ordering::Relaxed);
                *vc.last_active.lock().unwrap() = Instant::now();
                return Ok(());
            }
            let plen = frame.payload.len() as u64;
            // Hold the reorder lock across push + egress writes so ready
            // chunks from concurrent tunnel deliveries can't interleave
            // (mpsc try_send from two tasks has no total order).
            {
                let mut reorder = vconn.reorder.lock().unwrap();
                let result = reorder.push(frame.seq, frame.payload);
                if !result.accepted {
                    let overflow = result.overflow;
                    drop(reorder);
                    if overflow {
                        // Window full — the sequence is permanently broken;
                        // reset both sides instead of stalling forever.
                        warn!(
                            conn_id = cid,
                            seq = frame.seq,
                            "reorder window overflow, resetting connection"
                        );
                        drop(vconn);
                        ctx.closed.insert(cid, Instant::now());
                        if let Some((_, vconn)) = ctx.conns.remove(&cid) {
                            signal_teardown(&vconn);
                            drop(vconn);
                        }
                        ctx.pool.send(Frame::rst(cid));
                        ctx.resets.fetch_add(1, Ordering::Relaxed);
                    }
                    return Ok(());
                }
                for chunk in result.ready {
                    if !vconn.egress().write(chunk) {
                        drop(reorder);
                        warn!(conn_id = cid, "egress write failed, resetting connection");
                        drop(vconn);
                        ctx.closed.insert(cid, Instant::now());
                        if let Some((_, vconn)) = ctx.conns.remove(&cid) {
                            signal_teardown(&vconn);
                            drop(vconn);
                        }
                        ctx.pool.send(Frame::rst(cid));
                        ctx.resets.fetch_add(1, Ordering::Relaxed);
                        return Ok(());
                    }
                }
            }
            vconn.bytes_recv.fetch_add(plen, Ordering::Relaxed);
            vconn.frames_recv.fetch_add(1, Ordering::Relaxed);
            *vconn.last_active.lock().unwrap() = Instant::now();
            if vconn.fin_received.load(Ordering::Acquire) {
                close_write_half(&vconn, cid, false);
                // The delivered frame may have filled the FIN gap; if the
                // target already half-closed, the conn is now done.
                let vc = vconn.clone();
                drop(vconn); // release the shard lock before remove()
                finish_if_done(&vc, cid, &ctx.conns, &ctx.closed);
            }
            return Ok(());
        }
        // Late frame for a closed conn — tell the splitter to stop
        // instead of buffering it in a zombie pending entry.
        if ctx.closed.contains_key(&cid) {
            ctx.pool.send(Frame::rst(cid));
            return Ok(());
        }
        // Not in conns — could be pending (SYN still in flight) or
        // DATA arrived before SYN (out-of-order delivery across tunnels).
        // Create a pending slot so data isn't lost — the SYN handler
        // will drain it once the egress connection is established.
        let plen = frame.payload.len();
        if ctx.pending.contains_key(&cid) {
            // BUG-7: budget the frame before touching the entry.
            if !try_reserve_pending(ctx, Some(cid), plen) {
                warn!(
                    conn_id = cid,
                    "pending byte budget exhausted, resetting connection"
                );
                // B23: the frame is gone and the seq stream is broken —
                // fail fast instead of stalling the conn for minutes.
                fail_pending_conn(ctx, &cid);
                return Ok(());
            }
            if let Some(mut entry) = ctx.pending.get_mut(&cid) {
                if entry.frames.len() < MAX_PENDING_FRAMES_PER_CID {
                    entry.frames.push(frame);
                    entry.bytes += plen;
                } else {
                    warn!(
                        conn_id = cid,
                        count = entry.frames.len(),
                        "pending overflow, resetting connection"
                    );
                    ctx.pending_bytes.fetch_sub(plen, Ordering::Relaxed);
                    drop(entry);
                    // B23: fail fast — see above.
                    fail_pending_conn(ctx, &cid);
                }
            } else {
                // Entry vanished between contains_key and get_mut —
                // refund the reservation.
                ctx.pending_bytes.fetch_sub(plen, Ordering::Relaxed);
                // B27: the SYN handler may have just established the
                // conn — deliver through the normal path instead of
                // dropping the frame (which would leave a seq gap).
                if ctx.conns.contains_key(&cid) {
                    return Box::pin(handle_frame(frame, ctx)).await;
                }
            }
        } else if ctx.pending.len() < MAX_PENDING_CIDS {
            if !try_reserve_pending(ctx, None, plen) {
                warn!(
                    conn_id = cid,
                    "pending byte budget exhausted, resetting connection"
                );
                fail_pending_conn(ctx, &cid);
                return Ok(());
            }
            let mut entry = PendingEntry::new();
            entry.frames.push(frame);
            entry.bytes = plen;
            ctx.pending.insert(cid, entry);
        } else {
            warn!(
                conn_id = cid,
                "pending CID limit reached, resetting connection"
            );
            fail_pending_conn(ctx, &cid);
        }
        return Ok(());
    }

    // FIN: half-close — keep reading the egress side until the server
    // closes; stop writing once every in-flight frame has been delivered.
    // The reader echoes FIN at egress EOF (or fallback timer).
    if frame.flags & FLAG_FIN != 0 {
        if let Some(vconn) = ctx.conns.get(&cid) {
            if vconn.is_udp {
                return Ok(()); // UDP has no half-close semantics
            }
            let vc = vconn.clone();
            drop(vconn);
            start_half_close(&vc, frame.seq, cid);
            finish_if_done(&vc, cid, &ctx.conns, &ctx.closed);
            return Ok(());
        }
        // BUG-1: FIN can legitimately arrive before the SYN (SYN rides a
        // round-robin tunnel, FIN rides the least-loaded one).  Queue it
        // like DATA — the SYN handler drains it via start_half_close.
        if ctx.closed.contains_key(&cid) {
            return Ok(());
        }
        if ctx.pending.contains_key(&cid) {
            if let Some(mut entry) = ctx.pending.get_mut(&cid) {
                if entry.frames.len() < MAX_PENDING_FRAMES_PER_CID {
                    entry.frames.push(frame);
                } else {
                    warn!(conn_id = cid, "pending overflow, resetting connection");
                    drop(entry);
                    // B23: a dropped FIN would leave the splitter waiting
                    // out its 60s quiet timeout — fail fast instead.
                    fail_pending_conn(ctx, &cid);
                }
            }
        } else if ctx.pending.len() < MAX_PENDING_CIDS {
            let mut entry = PendingEntry::new();
            entry.frames.push(frame);
            ctx.pending.insert(cid, entry);
        } else {
            warn!(
                conn_id = cid,
                "pending CID limit reached, resetting connection"
            );
            fail_pending_conn(ctx, &cid);
        }
        return Ok(());
    }

    // RST
    if frame.flags & FLAG_RST != 0 {
        if let Some((_, vconn)) = ctx.conns.remove(&cid) {
            ctx.closed.insert(cid, Instant::now());
            signal_teardown(&vconn);
            info!(conn_id = cid, "RST, force close");
            drop(vconn);
            ctx.resets.fetch_add(1, Ordering::Relaxed);
            return Ok(());
        }
        // BUG-1/BUG-4: RST while the SYN handshake is in flight — mark
        // the pending entry so the SYN handler aborts the egress connect
        // instead of building a connection nobody wants.
        if let Some(mut entry) = ctx.pending.get_mut(&cid) {
            entry.cancelled = true;
            if let Some(notify) = &entry.cancel {
                notify.notify_one();
            }
        }
        // B27/B36: tombstone unconditionally, even for a completely
        // unknown cid — an RST processed in the window between the SYN
        // handler's conns check and its handshaking registration used to
        // be dropped, and the egress connection was then built for a conn
        // the splitter already reset.  Bounded by the CLOSED_TTL sweep.
        ctx.closed.insert(cid, Instant::now());
        return Ok(());
    }

    Ok(())
}

/// Remove a pending entry and refund its byte budget (BUG-7).
fn remove_pending(ctx: &ReadLoopCtx, cid: &u32) -> Option<PendingEntry> {
    let entry = ctx.pending.remove(cid).map(|(_, e)| e);
    if let Some(e) = &entry {
        ctx.pending_bytes.fetch_sub(e.bytes, Ordering::Relaxed);
    }
    entry
}

/// B34: sweep stale pending entries (never got a SYN within `ttl`),
/// refund their byte budget and fail fast the affected cids (abort any
/// in-flight SYN handshake, tombstone, RST) — the queued frames are
/// gone forever, so without a reset the splitter would keep buffering
/// into a fresh entry and repeat the cycle silently.
fn sweep_stale_pending(
    pending: &PendingMap,
    pending_bytes: &AtomicUsize,
    closed: &DashMap<u32, Instant>,
    pool: &TunnelPool,
    ttl_secs: u64,
) {
    let mut freed = 0usize;
    let mut swept: Vec<(u32, Option<Arc<Notify>>)> = Vec::new();
    pending.retain(|&cid, entry| {
        let keep = entry.since.elapsed().as_secs() < ttl_secs;
        if !keep {
            freed += entry.bytes;
            swept.push((cid, entry.cancel.clone()));
        }
        keep
    });
    pending_bytes.fetch_sub(freed, Ordering::Relaxed);
    for (cid, cancel) in swept {
        if let Some(notify) = &cancel {
            notify.notify_one();
        }
        closed.insert(cid, Instant::now());
        pool.send(Frame::rst(cid));
    }
}

/// B23: a pending DATA/FIN frame had to be dropped — the connection's
/// seq stream is permanently broken (TCP tunnels never retransmit), so
/// fail fast: cancel any pending entry, tombstone the cid and reset
/// both sides instead of stalling the connection for minutes.
fn fail_pending_conn(ctx: &ReadLoopCtx, cid: &u32) {
    if let Some(mut entry) = ctx.pending.get_mut(cid) {
        entry.cancelled = true;
        if let Some(notify) = &entry.cancel {
            notify.notify_one();
        }
    }
    ctx.closed.insert(*cid, Instant::now());
    ctx.pool.send(Frame::rst(*cid));
}

/// Reserve `need` bytes against the global pending budget, evicting the
/// oldest *other* entries when over budget (BUG-7).  Returns false when
/// the budget can't be satisfied.
fn try_reserve_pending(ctx: &ReadLoopCtx, exclude: Option<u32>, need: usize) -> bool {
    loop {
        // B32: atomic check-and-add (CAS) — the old load + fetch_add
        // pair let concurrent arrivals all pass the pre-check and
        // overshoot the budget.
        let mut current = ctx.pending_bytes.load(Ordering::Relaxed);
        while current + need <= MAX_PENDING_BYTES {
            match ctx.pending_bytes.compare_exchange_weak(
                current,
                current + need,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return true,
                Err(actual) => current = actual,
            }
        }
        // Over budget: evict oldest entries (other than the excluded
        // one) until the frame fits.  DashMap refs must be dropped
        // before remove().
        if ctx.pending.is_empty() {
            return false;
        }
        let oldest = ctx
            .pending
            .iter()
            .filter(|e| Some(*e.key()) != exclude)
            .min_by_key(|e| e.since);
        let Some(entry_ref) = oldest else {
            return false;
        };
        let (key, bytes) = (*entry_ref.key(), entry_ref.bytes);
        let cancel = entry_ref.cancel.clone();
        drop(entry_ref);
        if ctx.pending.remove(&key).is_none() {
            // Raced — another task removed it and already refunded.
            continue;
        }
        ctx.pending_bytes.fetch_sub(bytes, Ordering::Relaxed);
        // B33: the evicted cid's queued frames are gone forever (TCP
        // tunnels never retransmit) — fail fast like a dropped frame:
        // abort any in-flight SYN handshake, tombstone the cid and reset
        // both sides instead of silently truncating the request.
        if let Some(notify) = &cancel {
            notify.notify_one();
        }
        ctx.closed.insert(key, Instant::now());
        ctx.pool.send(Frame::rst(key));
        // Loop back and retry the CAS with the freed budget.
    }
}

// ── UDP relay handler ─────────────────────────────────────────────────

/// Decode a SOCKS5-wrapped datagram and send it to the target through
/// the conn's own socket (BUG-19).
async fn forward_udp_datagram(vconn: &VirtConnDe, payload: &[u8]) {
    let (target, data) = match socks5::decode_udp_datagram(payload) {
        Ok(t) => t,
        Err(e) => {
            warn!(error = %e, "UDP datagram decode failed");
            return;
        }
    };
    let Some(sock) = &vconn.udp_sock else {
        warn!("UDP conn without socket");
        return;
    };
    if let Err(e) = sock.send_to(&target.address, target.port, &data).await {
        warn!(error = %e, target = %target.address, port = target.port, "UDP send_to failed");
    }
}

// ── Half-close helpers ────────────────────────────────────────────────

/// Begin a half-close after the splitter's FIN: the splitter's FIN seq
/// is next_seq, so every frame below it must be delivered before the
/// egress write half may close.
fn start_half_close(vconn: &Arc<VirtConnDe>, fin_seq: u64, cid: u32) {
    if vconn.fin_received.swap(true, Ordering::AcqRel) {
        return; // duplicate FIN
    }
    vconn.fin_seq.store(fin_seq, Ordering::Release);
    // Safety net: if a tunnel died and the seq gap never fills, force the
    // write half closed after a grace period so the server can finish.
    let vc = vconn.clone();
    tokio::spawn(async move {
        tokio::time::sleep(HALF_CLOSE_FALLBACK).await;
        close_write_half(&vc, cid, true);
    });
    close_write_half(vconn, cid, false);
}

/// Close the egress write half when either all frames below fin_seq are
/// delivered (`force=false`) or the fallback timer expires (`force=true`).
///
/// BUG-5: state machine on an AtomicU8 (0=open, 1=closing, 2=closed).
/// The old swap(true)/store(false) pair had a race where the force timer
/// could fire between the two and lose its close entirely, leaving the
/// write half open until the idle sweep.
fn close_write_half(vconn: &VirtConnDe, cid: u32, force: bool) {
    if force {
        // Force close always wins: advance straight to closed. A
        // concurrent normal close in the "closing" state is irrelevant.
        let prev = vconn.half_close_state.swap(2, Ordering::AcqRel);
        if prev == 2 {
            return; // already closed
        }
        warn!(
            conn_id = cid,
            fin_seq = vconn.fin_seq.load(Ordering::Acquire),
            "write half force-closed with pending frames"
        );
        vconn.half_close.notify_one();
        return;
    }
    // Normal close: only the first caller (0→1) evaluates completeness.
    if vconn
        .half_close_state
        .compare_exchange(0, 1, Ordering::AcqRel, Ordering::Acquire)
        .is_err()
    {
        return; // a close is already in progress or done
    }
    let fin_seq = vconn.fin_seq.load(Ordering::Acquire);
    if !vconn.reorder.lock().unwrap().is_complete_through(fin_seq) {
        // Gap frames still in flight — roll back to open so a later DATA
        // delivery retries.  CAS (not store) so a force close that fired
        // in the meantime is not overwritten.
        let _ = vconn
            .half_close_state
            .compare_exchange(1, 0, Ordering::AcqRel, Ordering::Acquire);
        return;
    }
    vconn.half_close_state.store(2, Ordering::Release);
    vconn.half_close.notify_one();
}

// ── Egress I/O tasks ──────────────────────────────────────────────────

async fn write_to_egress<W>(
    mut rx: mpsc::Receiver<Bytes>,
    mut wr: W,
    half_close: Arc<Notify>,
    cancel_writer: Arc<Notify>,
) where
    W: tokio::io::AsyncWrite + Unpin,
{
    loop {
        // B48: biased with cancel first — an RST / idle sweep must stop
        // the writer immediately instead of draining up to
        // EGRESS_CHANNEL_CAP (512 ≈ 32 MB) of stale chunks to a target
        // the client already abandoned.  Without a cancel arm the task
        // only exits when the channel closes after the queue drains.
        tokio::select! {
            biased;
            _ = cancel_writer.notified() => break,
            chunk = rx.recv() => {
                let Some(chunk) = chunk else { break }; // vconn dropped — teardown
                // B48: race the write itself against cancel too — a
                // stalled write must not wait out the 60s timeout on a
                // connection that was reset mid-write.
                tokio::select! {
                    biased;
                    _ = cancel_writer.notified() => break,
                    r = tokio::time::timeout(EGRESS_WRITE_TIMEOUT, wr.write_all(&chunk)) => {
                        match r {
                            Ok(Ok(())) => {}
                            _ => break, // write error or stall timeout
                        }
                    }
                }
            }
            _ = half_close.notified() => {
                // Peer FIN and all in-flight data delivered: drain whatever
                // is still queued, then half-close so the server sees EOF
                // and can finish its response.
                // B51: the drain races cancel like every other write — a
                // reset mid-drain must stop the stale data immediately,
                // not after up to 512 queued chunks.
                while let Ok(chunk) = rx.try_recv() {
                    tokio::select! {
                        biased;
                        _ = cancel_writer.notified() => return,
                        r = tokio::time::timeout(EGRESS_WRITE_TIMEOUT, wr.write_all(&chunk)) => {
                            match r {
                                Ok(Ok(())) => {}
                                _ => break,
                            }
                        }
                    }
                }
                let _ = wr.shutdown().await;
                return;
            }
        }
    }
    let _ = wr.shutdown().await;
}

/// Bundled context for the egress reader task (keeps the arg list small).
struct EgressReaderCtx {
    conn_id: u32,
    conns: ConnMap,
    pool: Arc<TunnelPool>,
    chunk_size: usize,
    cancel: Arc<Notify>,
    closed: Arc<DashMap<u32, Instant>>,
    vconn: Arc<VirtConnDe>,
    /// DATA/FIN send timeout (injectable so tests don't wait 30s).
    data_send_timeout: Duration,
}

async fn read_from_egress(mut rd: tokio::net::tcp::OwnedReadHalf, ctx: EgressReaderCtx) {
    let conn_id = ctx.conn_id;
    let conns = ctx.conns;
    let pool = ctx.pool;
    let chunk_size = ctx.chunk_size;
    let cancel = ctx.cancel;
    let closed = ctx.closed;
    let vconn = ctx.vconn;
    let data_send_timeout = ctx.data_send_timeout;
    let mut seq: u64 = 1;
    let mut cancelled = false;
    let mut send_failed = false;
    // One reusable read buffer per connection; each frame copies exactly
    // n bytes into a fresh Bytes (no 64 KB backing per in-flight frame).
    let mut buf = vec![0u8; chunk_size];
    loop {
        tokio::select! {
            _ = cancel.notified() => {
                cancelled = true;
                break;
            }
            result = rd.read(&mut buf) => {
                match result {
                    Ok(0) => break,
                    Ok(n) => {
                        let frame = Frame::data(conn_id, seq, Bytes::copy_from_slice(&buf[..n]));
                        // Real backpressure: wait for a tunnel to take the
                        // frame (bounded by data_send_timeout; B45 makes the
                        // wait cover short tunnel-reconnect gaps).
                        // B52: race the wait against cancel — a teardown
                        // must not sit out up to 30s waiting for tunnel
                        // capacity (and then ship a stale frame once the
                        // tunnels recover).
                        let sent = tokio::select! {
                            biased;
                            _ = cancel.notified() => {
                                cancelled = true;
                                false
                            }
                            r = tokio::time::timeout(data_send_timeout, pool.send_async(frame)) => {
                                r.unwrap_or(false)
                            }
                        };
                        if !sent {
                            if !cancelled {
                                warn!(conn_id, "no live tunnels for egress response after timeout");
                                // B42: the response seq stream is permanently
                                // broken (TCP tunnels never retransmit) — the
                                // normal FIN flow would hand the splitter a
                                // gap it can never fill.  Fail fast instead.
                                send_failed = true;
                            }
                            break;
                        }
                        // Count on the VirtConnDe — the task already holds
                        // the Arc, no DashMap lookup per chunk.
                        vconn.bytes_sent.fetch_add(n as u64, Ordering::Relaxed);
                        vconn.frames_sent.fetch_add(1, Ordering::Relaxed);
                        *vconn.last_active.lock().unwrap() = Instant::now();
                        seq = seq.wrapping_add(1);
                    }
                    Err(e) => {
                        warn!(conn_id, error = %e, "egress read error");
                        break;
                    }
                }
            }
        }
    }
    if cancelled {
        return; // teardown in progress elsewhere (RST / finish_if_done)
    }
    if send_failed {
        // B42: the splitter's client would otherwise hang until its 60s
        // quiet timeout — reset both sides now.  Remove the conn and
        // tombstone so late DATA from the splitter gets a deterministic
        // RST; the best-effort RST below tears the client down promptly
        // whenever any tunnel can carry it.
        warn!(conn_id, "egress response send failed, resetting connection");
        closed.insert(conn_id, Instant::now());
        if let Some((_, vc)) = conns.remove(&conn_id) {
            signal_teardown(&vc);
        }
        pool.send(Frame::rst(conn_id));
        return;
    }
    // Echo FIN to the splitter. D3: do NOT tear the conn down —
    // the splitter may still be sending data on the other direction.
    // The conn is removed only once the splitter's FIN half-closes
    // the egress write side as well (finish_if_done).
    // B52: race the FIN send against cancel like the DATA sends — a
    // teardown mid-wait stops immediately instead of sitting out the
    // timeout (and the RST fallback is pointless for a dead conn).
    let fin_sent = tokio::select! {
        biased;
        _ = cancel.notified() => false,
        r = tokio::time::timeout(data_send_timeout, pool.send_async(Frame::fin(conn_id, seq))) => {
            r.unwrap_or(false)
        }
    };
    if !fin_sent {
        // B42: without a FIN the splitter's client hangs until its 60s
        // quiet timeout — best-effort RST so it fails fast as soon as
        // any tunnel can carry it.
        warn!(
            conn_id,
            "failed to send FIN to splitter, sending RST fallback"
        );
        pool.send(Frame::rst(conn_id));
    }
    vconn.egress_eof.store(true, Ordering::Release);
    finish_if_done(&vconn, conn_id, &conns, &closed);
}

/// Remove the conn (and tombstone its cid) once both directions are
/// finished: the target half-closed (egress_eof), the splitter's FIN
/// half-closed the egress write side (half_close_state == 2).  D3.
fn finish_if_done(
    vconn: &Arc<VirtConnDe>,
    cid: u32,
    conns: &ConnMap,
    closed: &Arc<DashMap<u32, Instant>>,
) {
    if !vconn.fin_received.load(Ordering::Acquire)
        || !vconn.egress_eof.load(Ordering::Acquire)
        || vconn.half_close_state.load(Ordering::Acquire) < 2
    {
        return;
    }
    closed.insert(cid, Instant::now());
    if let Some((_, vc)) = conns.remove(&cid) {
        // B48: no cancel notify here — egress_eof is only ever set by
        // the egress reader itself, so by the time this runs the reader
        // is already past its select loop.  The signal was dead; with
        // the writer now listening on cancel it became actively harmful
        // (it raced the half_close drain and truncated the tail of the
        // egress stream — D3 regression).
        let dur = vc.created_at.elapsed().as_millis() as u64;
        info!(
            conn_id = cid,
            bytes_sent = vc.bytes_sent.load(Ordering::Relaxed),
            bytes_recv = vc.bytes_recv.load(Ordering::Relaxed),
            frames_sent = vc.frames_sent.load(Ordering::Relaxed),
            frames_recv = vc.frames_recv.load(Ordering::Relaxed),
            duration_ms = dur,
            "closed"
        );
        drop(vc);
    }
}

/// Mini SOCKS5 CONNECT proxy for the B46 test: reads greeting + request,
/// stalls on the CONNECT until `release`, then connects to the target
/// and reports success.  Rejects non-CONNECT commands.
#[cfg(test)]
async fn run_stalling_proxy(listener: TcpListener, release: Arc<Notify>) {
    loop {
        let (s, _) = match listener.accept().await {
            Ok(v) => v,
            Err(_) => continue,
        };
        let release = release.clone();
        tokio::spawn(async move {
            let mut s = s;
            let mut hdr = [0u8; 2];
            if s.read_exact(&mut hdr).await.is_err() {
                return;
            }
            let mut methods = vec![0u8; hdr[1] as usize];
            if s.read_exact(&mut methods).await.is_err() {
                return;
            }
            if s.write_all(&[0x05, 0x00]).await.is_err() {
                return;
            }
            let mut req = [0u8; 4];
            if s.read_exact(&mut req).await.is_err() {
                return;
            }
            if req[1] != 0x01 {
                let _ = s
                    .write_all(&[0x05, 0x07, 0x00, 0x01, 0, 0, 0, 0, 0, 0])
                    .await;
                return;
            }
            // Read the address per ATYP (same layout as the e2e proxy).
            let (host, port) = match req[3] {
                0x01 => {
                    let mut b = [0u8; 6];
                    if s.read_exact(&mut b).await.is_err() {
                        return;
                    }
                    (
                        format!("{}.{}.{}.{}", b[0], b[1], b[2], b[3]),
                        u16::from_be_bytes([b[4], b[5]]),
                    )
                }
                0x03 => {
                    let mut len = [0u8; 1];
                    if s.read_exact(&mut len).await.is_err() {
                        return;
                    }
                    let mut b = vec![0u8; len[0] as usize];
                    if s.read_exact(&mut b).await.is_err() {
                        return;
                    }
                    let mut p = [0u8; 2];
                    if s.read_exact(&mut p).await.is_err() {
                        return;
                    }
                    (
                        String::from_utf8_lossy(&b).into_owned(),
                        u16::from_be_bytes(p),
                    )
                }
                0x04 => {
                    let mut b = [0u8; 18];
                    if s.read_exact(&mut b).await.is_err() {
                        return;
                    }
                    let segs: Vec<String> = b[..16]
                        .chunks(2)
                        .map(|c| format!("{:02x}{:02x}", c[0], c[1]))
                        .collect();
                    (segs.join(":"), u16::from_be_bytes([b[16], b[17]]))
                }
                _ => return,
            };
            // Stall until the test releases the connect.
            release.notified().await;
            // Keep the upstream connection alive for the rest of the test
            // (`_up` binds it until the task ends).
            let _up = match tokio::net::TcpStream::connect((host.as_str(), port)).await {
                Ok(u) => u,
                Err(_) => {
                    let _ = s
                        .write_all(&[0x05, 0x01, 0x00, 0x01, 0, 0, 0, 0, 0, 0])
                        .await;
                    return;
                }
            };
            if s.write_all(&[0x05, 0x00, 0x00, 0x01, 0, 0, 0, 0, 0, 0])
                .await
                .is_err()
            {
                return;
            }
            // Hold the connection open until the client side closes.
            let mut b = [0u8; 1];
            let _ = s.read(&mut b).await;
        });
    }
}

// ── tests ─────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn make_ctx() -> (ReadLoopCtx, mpsc::Receiver<Frame>) {
        let (tx, rx) = mpsc::channel::<Frame>(TUNNEL_CHANNEL_CAP);
        let link = Arc::new(TunnelLink {
            tx,
            alive: AtomicBool::new(true),
            bytes_sent: AtomicU64::new(0),
            bytes_recv: AtomicU64::new(0),
            frames_sent: AtomicU64::new(0),
            frames_recv: AtomicU64::new(0),
            stop: Arc::new(Notify::new()),
            writer_died: Arc::new(Notify::new()),
            lost_frames: Mutex::new(Vec::new()),
            rate_bps: AtomicU64::new(0),
        });
        let pool = Arc::new(TunnelPool::new());
        pool.add(link.clone());
        let ctx = ReadLoopCtx {
            conns: Arc::new(DashMap::new()),
            pending: Arc::new(DashMap::new()),
            closed: Arc::new(DashMap::new()),
            handshaking: Arc::new(DashMap::new()),
            pool,
            local_target: "127.0.0.1:9".parse().unwrap(),
            chunk_size: 4096,
            data_send_timeout: Duration::from_secs(30),
            pending_bytes: Arc::new(AtomicUsize::new(0)),
            resets: Arc::new(AtomicU64::new(0)),
            link,
            syn_limit: Arc::new(Semaphore::new(MAX_CONCURRENT_SYN_HANDSHAKES)),
        };
        (ctx, rx)
    }

    /// Helper: a VirtConnDe wired to a real egress channel (used by the
    /// read_from_egress test).
    fn make_vconn(_cid: u32) -> Arc<VirtConnDe> {
        let (write_tx, _write_rx) = mpsc::channel::<Bytes>(EGRESS_CHANNEL_CAP);
        Arc::new(VirtConnDe {
            egress: Some(EgressConn { write_tx }),
            reorder: Mutex::new(ReorderBuf::new()),
            cancel: Arc::new(Notify::new()),
            cancel_writer: Arc::new(Notify::new()),
            half_close: Arc::new(Notify::new()),
            fin_received: AtomicBool::new(false),
            fin_seq: AtomicU64::new(0),
            half_close_state: AtomicU8::new(0),
            is_udp: false,
            udp_sock: None,
            egress_eof: AtomicBool::new(false),
            created_at: Instant::now(),
            last_active: Mutex::new(Instant::now()),
            bytes_sent: AtomicU64::new(0),
            bytes_recv: AtomicU64::new(0),
            frames_sent: AtomicU64::new(0),
            frames_recv: AtomicU64::new(0),
        })
    }

    /// B23 regression: pending DATA overflow must reset the connection
    /// (RST + tombstone + cancelled entry) instead of silently dropping
    /// the frame and stalling the conn.
    #[tokio::test]
    async fn pending_data_overflow_fails_fast() {
        let (ctx, mut rx) = make_ctx();
        let cid = 42u32;
        let mut entry = PendingEntry::new();
        for _ in 0..MAX_PENDING_FRAMES_PER_CID {
            entry.frames.push(Frame::data(cid, 0, Bytes::new()));
        }
        ctx.pending.insert(cid, entry);
        handle_frame(Frame::data(cid, 1, Bytes::from_static(b"x")), &ctx)
            .await
            .unwrap();
        let got = rx.recv().await.expect("expected an RST on the pool");
        assert_eq!(got.conn_id, cid);
        assert_eq!(got.flags, FLAG_RST);
        assert!(ctx.closed.contains_key(&cid), "cid must be tombstoned");
        assert!(
            ctx.pending.get(&cid).unwrap().cancelled,
            "pending entry must be cancelled"
        );
    }

    /// B23 regression: FIN dropped at the pending-CID limit must fail
    /// fast instead of leaving the splitter to wait out its quiet timeout.
    #[tokio::test]
    async fn pending_fin_drop_fails_fast() {
        let (ctx, mut rx) = make_ctx();
        for i in 1..=MAX_PENDING_CIDS as u32 {
            ctx.pending.insert(i, PendingEntry::new());
        }
        let cid = 777u32;
        handle_frame(Frame::fin(cid, 7), &ctx).await.unwrap();
        let got = rx.recv().await.expect("expected an RST on the pool");
        assert_eq!(got.conn_id, cid);
        assert_eq!(got.flags, FLAG_RST);
        assert!(ctx.closed.contains_key(&cid));
    }

    /// B30 regression: unknown SYN proto must be rejected with RST.
    #[tokio::test]
    async fn unknown_syn_proto_resets() {
        let (ctx, mut rx) = make_ctx();
        let cid = 5u32;
        let payload = SynTarget {
            proto: 0x99,
            address: "example.com".into(),
            port: 80,
        }
        .encode()
        .unwrap();
        handle_frame(Frame::syn(cid, payload), &ctx).await.unwrap();
        let got = rx.recv().await.expect("expected an RST on the pool");
        assert_eq!(got.conn_id, cid);
        assert_eq!(got.flags, FLAG_RST);
        assert!(ctx.closed.contains_key(&cid));
        assert!(!ctx.handshaking.contains_key(&cid));
    }

    /// B33 regression: evicting the oldest pending entry to free budget
    /// must fail fast the evicted cid (RST + tombstone) instead of
    /// silently dropping its queued frames and truncating the request.
    #[tokio::test]
    async fn pending_eviction_resets_evicted_cid() {
        let (ctx, mut rx) = make_ctx();
        let evicted = 42u32;
        let mut entry = PendingEntry::new();
        entry.bytes = MAX_PENDING_BYTES; // consume the whole budget
        ctx.pending.insert(evicted, entry);
        ctx.pending_bytes
            .store(MAX_PENDING_BYTES, Ordering::Relaxed);
        // Reserve for a new cid — must evict the oldest entry (42).
        assert!(try_reserve_pending(&ctx, None, 1));
        let got = rx.recv().await.expect("expected an RST on the pool");
        assert_eq!(got.conn_id, evicted);
        assert_eq!(got.flags, FLAG_RST);
        assert!(
            ctx.closed.contains_key(&evicted),
            "evicted cid must be tombstoned"
        );
        assert!(
            !ctx.pending.contains_key(&evicted),
            "evicted entry must be gone"
        );
    }

    /// B34 regression: the pending TTL sweep must RST the swept cid (and
    /// refund its byte budget) so a lost-SYN conn fails fast instead of
    /// silently re-buffering into a fresh entry forever.
    #[tokio::test]
    async fn pending_sweep_resets_swept_cid() {
        let (ctx, mut rx) = make_ctx();
        let cid = 55u32;
        let mut entry = PendingEntry::new();
        entry.bytes = 1234;
        ctx.pending.insert(cid, entry);
        ctx.pending_bytes.store(1234, Ordering::Relaxed);
        // ttl=0 forces every entry stale.
        sweep_stale_pending(&ctx.pending, &ctx.pending_bytes, &ctx.closed, &ctx.pool, 0);
        let got = rx.recv().await.expect("expected an RST on the pool");
        assert_eq!(got.conn_id, cid);
        assert_eq!(got.flags, FLAG_RST);
        assert!(
            ctx.closed.contains_key(&cid),
            "swept cid must be tombstoned"
        );
        assert!(!ctx.pending.contains_key(&cid), "swept entry must be gone");
        assert_eq!(
            ctx.pending_bytes.load(Ordering::Relaxed),
            0,
            "budget must be refunded"
        );
    }

    /// B36 regression: an RST for a cid with no conn / pending / handshake
    /// state must still tombstone — otherwise a RST landing in the window
    /// before the SYN handler registers its entries would be dropped and
    /// a ghost egress built for a conn the splitter already reset.
    #[tokio::test]
    async fn rst_for_unknown_cid_tombstones() {
        let (ctx, _rx) = make_ctx();
        let cid = 99u32;
        handle_frame(Frame::rst(cid), &ctx).await.unwrap();
        assert!(
            ctx.closed.contains_key(&cid),
            "RST for unknown cid must tombstone"
        );
    }

    /// B41 regression: an egress connect failure must tombstone the cid
    /// (like every other SYN failure path) — otherwise late DATA frames
    /// still in flight on other tunnels create a zombie pending entry
    /// that eats the byte budget for 30s and can evict healthy entries.
    #[tokio::test]
    async fn syn_connect_failure_tombstones_cid() {
        let (ctx, mut rx) = make_ctx();
        // make_ctx's local_target is 127.0.0.1:9 — nothing listens there,
        // so the egress connect fails fast.
        let cid = 1234u32;
        let payload = SynTarget {
            proto: PROTO_TCP,
            address: "127.0.0.1".into(),
            port: 9,
        }
        .encode()
        .unwrap();
        handle_frame(Frame::syn(cid, payload), &ctx).await.unwrap();
        let got = rx.recv().await.expect("expected an RST on the pool");
        assert_eq!(got.conn_id, cid);
        assert_eq!(got.flags, FLAG_RST);
        assert!(
            ctx.closed.contains_key(&cid),
            "connect-failed cid must be tombstoned"
        );
        assert!(!ctx.handshaking.contains_key(&cid));
        assert!(!ctx.pending.contains_key(&cid));
    }

    /// B42 regression: when the egress response cannot be sent (no live
    /// tunnel), read_from_egress must fail fast — tombstone + remove the
    /// conn + best-effort RST — instead of the FIN flow, which would hand
    /// the splitter a seq gap it can never fill (its client then hangs
    /// until the 60s quiet timeout).
    #[tokio::test]
    async fn egress_send_failure_resets_conn() {
        let (ctx, rx) = make_ctx();
        drop(rx); // tunnel channel closed → the link dies on first try_send
        let cid = 4242u32;
        let vconn = make_vconn(cid);
        ctx.conns.insert(cid, vconn.clone());

        // Real TCP pair: the server half feeds the egress reader.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let mut peer = tokio::net::TcpStream::connect(addr).await.unwrap();
        let (server, _) = listener.accept().await.unwrap();
        let (rd, _wr) = server.into_split();

        let task = tokio::spawn(read_from_egress(
            rd,
            EgressReaderCtx {
                conn_id: cid,
                conns: ctx.conns.clone(),
                pool: ctx.pool.clone(),
                chunk_size: 4096,
                cancel: Arc::new(Notify::new()),
                closed: ctx.closed.clone(),
                vconn,
                data_send_timeout: Duration::from_millis(100),
            },
        ));
        // Feed response data — the send fails (no live tunnel) and the
        // reader must tear the connection down.
        peer.write_all(b"response-bytes").await.unwrap();
        tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .expect("egress reader did not exit")
            .unwrap();
        assert!(ctx.closed.contains_key(&cid), "conn must be tombstoned");
        assert!(
            !ctx.conns.contains_key(&cid),
            "conn must be removed from conns"
        );
        drop(peer);
    }

    /// B48 regression: cancel must stop the egress writer promptly even
    /// mid-write — without the cancel race the writer would sit out the
    /// 60s stall timeout on a connection the client already reset.
    #[tokio::test]
    async fn write_to_egress_aborts_on_cancel() {
        // duplex capacity 4: the first queued chunk stalls the write
        // (the peer never reads), forcing the mid-write path.
        let (peer, wr) = tokio::io::duplex(4);
        let (tx, rx) = mpsc::channel::<Bytes>(EGRESS_CHANNEL_CAP);
        for i in 0..4 {
            tx.try_send(Bytes::from(vec![i as u8; 1024])).unwrap();
        }
        let cancel = Arc::new(Notify::new());
        let half_close = Arc::new(Notify::new());
        let task = tokio::spawn(write_to_egress(rx, wr, half_close, cancel.clone()));
        // Let the writer dequeue the first chunk and stall on the write.
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        cancel.notify_one();
        tokio::time::timeout(std::time::Duration::from_secs(2), task)
            .await
            .expect("egress writer did not exit after cancel")
            .unwrap();
        drop(peer);
    }

    /// B48 regression: a cancel that fired before the writer started
    /// (notify_one stores a permit) must stop it before any queued chunk
    /// is written.
    #[tokio::test]
    async fn write_to_egress_stops_immediately_when_pre_cancelled() {
        let (peer, wr) = tokio::io::duplex(1024);
        let (tx, rx) = mpsc::channel::<Bytes>(EGRESS_CHANNEL_CAP);
        tx.try_send(Bytes::from(vec![0u8; 64])).unwrap();
        let cancel = Arc::new(Notify::new());
        cancel.notify_one();
        let half_close = Arc::new(Notify::new());
        let task = tokio::spawn(write_to_egress(rx, wr, half_close, cancel));
        tokio::time::timeout(std::time::Duration::from_secs(2), task)
            .await
            .expect("pre-cancelled writer did not exit")
            .unwrap();
        drop(peer);
    }

    /// B50 regression: reader and writer each wait on their OWN teardown
    /// Notify — a shared Notify woke only one of the two waiters per
    /// `notify_one`, and the loser hung on the peer (deliberately never
    /// closed here) or drained stale data (the B48 regression).
    #[tokio::test]
    async fn teardown_wakes_both_egress_tasks() {
        let (ctx, _rx) = make_ctx();
        let cid = 77u32;
        // Real TCP pair standing in for the egress connection; the peer
        // half is never closed — a stalled target must not pin either
        // task after teardown.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let peer = tokio::net::TcpStream::connect(addr).await.unwrap();
        let (server, _) = listener.accept().await.unwrap();
        let (rd, wr) = server.into_split();

        let (write_tx, write_rx) = mpsc::channel::<Bytes>(EGRESS_CHANNEL_CAP);
        let cancel = Arc::new(Notify::new());
        let cancel_writer = Arc::new(Notify::new());
        let half_close = Arc::new(Notify::new());
        let vconn = Arc::new(VirtConnDe {
            egress: Some(EgressConn { write_tx }),
            reorder: Mutex::new(ReorderBuf::new()),
            cancel: cancel.clone(),
            cancel_writer: cancel_writer.clone(),
            half_close: half_close.clone(),
            fin_received: AtomicBool::new(false),
            fin_seq: AtomicU64::new(0),
            half_close_state: AtomicU8::new(0),
            is_udp: false,
            udp_sock: None,
            egress_eof: AtomicBool::new(false),
            created_at: Instant::now(),
            last_active: Mutex::new(Instant::now()),
            bytes_sent: AtomicU64::new(0),
            bytes_recv: AtomicU64::new(0),
            frames_sent: AtomicU64::new(0),
            frames_recv: AtomicU64::new(0),
        });

        let w_task = tokio::spawn(write_to_egress(
            write_rx,
            wr,
            half_close,
            cancel_writer.clone(),
        ));
        let r_task = tokio::spawn(read_from_egress(
            rd,
            EgressReaderCtx {
                conn_id: cid,
                conns: ctx.conns.clone(),
                pool: ctx.pool.clone(),
                chunk_size: 4096,
                cancel: cancel.clone(),
                closed: ctx.closed.clone(),
                vconn: vconn.clone(),
                data_send_timeout: Duration::from_secs(30),
            },
        ));

        // Let both tasks register their waiters, then tear down once.
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        signal_teardown(&vconn);

        // Both must exit promptly even though the target never closes.
        tokio::time::timeout(std::time::Duration::from_secs(2), w_task)
            .await
            .expect("egress writer stuck after teardown")
            .unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(2), r_task)
            .await
            .expect("egress reader stuck after teardown")
            .unwrap();
        drop(peer);
    }

    /// B51 regression: chunks can still land in the channel after the
    /// half-close fires (in-flight DATA on other tunnels) — a teardown
    /// during that drain must stop it immediately, not after the
    /// remaining chunks or the 60s stall timeout.  Post-fix the cancel
    /// wins in every arm of the writer; pre-fix the half-close drain
    /// ignored it entirely.
    #[tokio::test]
    async fn write_to_egress_cancel_during_half_close_drain() {
        // Tiny duplex: every 1 KB chunk write stalls (peer never reads),
        // holding the writer inside its current arm while we cancel.
        let (peer, wr) = tokio::io::duplex(4);
        let (tx, rx) = mpsc::channel::<Bytes>(EGRESS_CHANNEL_CAP);
        let cancel = Arc::new(Notify::new());
        let half_close = Arc::new(Notify::new());
        // Half-close fires first — the writer enters the drain.
        half_close.notify_one();
        let task = tokio::spawn(write_to_egress(rx, wr, half_close, cancel.clone()));
        // A concurrent sender keeps the channel fed during the drain
        // window, so the drain's write path is actually exercised.
        let feeder = tokio::spawn(async move {
            for _ in 0..64 {
                let _ = tx.try_send(Bytes::from(vec![0u8; 1024]));
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        cancel.notify_one();
        tokio::time::timeout(std::time::Duration::from_secs(2), task)
            .await
            .expect("half-close drain did not stop after cancel")
            .unwrap();
        feeder.abort();
        drop(peer);
    }

    /// B56 regression: the read loop must exit when the drain task dies
    /// (writer_died) — a silently dead peer (no FIN/RST) would otherwise
    /// keep this task and its socket half blocked in read for the TCP
    /// RTO duration (minutes).
    #[tokio::test]
    async fn tunnel_read_loop_exits_when_writer_dies() {
        let (ctx, _rx) = make_ctx();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let peer = tokio::net::TcpStream::connect(addr).await.unwrap();
        let (server, _) = listener.accept().await.unwrap();
        let (rd, _wr) = server.into_split();

        let task = tokio::spawn(tunnel_read_loop(rd, ctx.clone()));
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        assert!(
            !task.is_finished(),
            "read loop must be blocked on the silent peer"
        );
        ctx.link.writer_died.notify_one();
        tokio::time::timeout(std::time::Duration::from_secs(2), task)
            .await
            .expect("read loop did not exit after writer death")
            .unwrap()
            .unwrap();
        drop(peer);
    }

    /// B49 regression: IP-literal datagrams go straight to the matching
    /// socket (no resolution, no timeout involvement).
    #[tokio::test]
    async fn udp_pair_send_to_ip_literal() {
        let pair = bind_udp_pair().await.unwrap();
        let target = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let addr = target.local_addr().unwrap();
        pair.send_to("127.0.0.1", addr.port(), b"ping")
            .await
            .unwrap();
        let mut buf = [0u8; 16];
        let (n, _) = tokio::time::timeout(Duration::from_secs(2), target.recv_from(&mut buf))
            .await
            .expect("datagram not received")
            .unwrap();
        assert_eq!(&buf[..n], b"ping");
    }

    /// O7: a UDP conn must not allocate an egress channel (datagrams
    /// bypass it entirely — the old code created a cap-1 channel whose
    /// receiver was dropped immediately).
    #[tokio::test]
    async fn udp_syn_creates_no_egress_channel() {
        let (ctx, _rx) = make_ctx();
        let cid = 77u32;
        let payload = SynTarget {
            proto: PROTO_UDP,
            address: "127.0.0.1".into(),
            port: 53,
        }
        .encode()
        .unwrap();
        handle_frame(Frame::syn(cid, payload), &ctx).await.unwrap();
        let vc = ctx
            .conns
            .get(&cid)
            .expect("UDP conn must be established")
            .clone();
        assert!(vc.is_udp);
        assert!(
            vc.egress.is_none(),
            "UDP conn must not allocate an egress channel"
        );
        drop(vc);
        // RST tears the conn (and its response reader) down.
        handle_frame(Frame::rst(cid), &ctx).await.unwrap();
        assert!(!ctx.conns.contains_key(&cid));
    }

    // B49: the timeout path itself is not unit-tested — `lookup_host`
    // resolves synchronously on the first poll in this environment
    // (wildcard DNS), so the wrapper can never be observed elapsing.
    // The wrapper is tokio's own `timeout` primitive; the regression
    // risk being guarded (an unbounded await on the tunnel read loop)
    // is removed by construction.

    /// B46 regression: a SYN whose egress connect stalls must NOT block
    /// the tunnel read loop — frames for other cids keep flowing.  The
    /// SYN handler runs on a spawned task (bounded by syn_limit).
    #[tokio::test]
    async fn syn_connect_stall_does_not_block_other_cids() {
        let (mut ctx, _rx) = make_ctx();

        // Target listener (accept and hold) so the proxy's connect
        // succeeds once it is released.
        let target_l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let target_addr = target_l.local_addr().unwrap();
        tokio::spawn(async move {
            let _ = target_l.accept().await;
        });

        // Mini SOCKS5 proxy: completes greeting/auth, then stalls on the
        // CONNECT request until released, then connects to the target.
        let proxy_l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let proxy_addr = proxy_l.local_addr().unwrap();
        let release = Arc::new(Notify::new());
        tokio::spawn(run_stalling_proxy(proxy_l, release.clone()));
        ctx.local_target = proxy_addr;

        // SYN for cid A: dispatch returns immediately while the connect
        // stalls inside the spawned handshake task.
        let syn_a = Frame::syn(
            1001,
            SynTarget {
                proto: PROTO_TCP,
                address: "127.0.0.1".into(),
                port: target_addr.port(),
            }
            .encode()
            .unwrap(),
        );
        let t0 = Instant::now();
        dispatch_frame(syn_a, &ctx).await.unwrap();
        assert!(
            t0.elapsed() < Duration::from_millis(500),
            "SYN dispatch must not block on the egress connect"
        );

        // DATA for cid B is processed promptly — pending entry created
        // without waiting for the stalled SYN.
        dispatch_frame(Frame::data(2002, 1, Bytes::from_static(b"x")), &ctx)
            .await
            .unwrap();
        assert!(
            ctx.pending.contains_key(&2002),
            "other cid's DATA must be buffered while the SYN connect stalls"
        );

        // Release the connect — cid A's conn must appear promptly.
        release.notify_one();
        tokio::time::timeout(Duration::from_secs(3), async {
            while !ctx.conns.contains_key(&1001) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("conn A not established after connect release");
    }
}
