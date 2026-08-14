use crate::frame::{
    FLAG_DATA, FLAG_FIN, FLAG_RST, FLAG_SYN, MAX_PENDING_BYTES, MAX_PENDING_CIDS, PROTO_UDP,
    Frame, FrameDecoder, MAX_PAYLOAD, SynTarget, UDP_CONN_ID,
};
use crate::reorder::ReorderBuf;
use crate::shutdown_signal;
use crate::socks5;
use crate::tunnel::{TUNNEL_CHANNEL_CAP, TunnelLink, TunnelPool, drain_frames};
use anyhow::Result;
use bytes::Bytes;
use dashmap::DashMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, UdpSocket};
use tokio::sync::{Notify, mpsc};
use tracing::{error, info, warn};

// ── Config ────────────────────────────────────────────────────────────

pub struct ReassemblerConfig {
    pub listen_ip: IpAddr,
    pub listen_ports: Vec<u16>,
    pub local_target: SocketAddr,
    pub chunk_size: usize,
}

// ── Tuning constants ──────────────────────────────────────────────────

/// Egress write queue capacity. ~32 MB worst-case backlog per connection
/// before the connection is reset (bounded — no unbounded growth).
const EGRESS_CHANNEL_CAP: usize = 512;
/// An egress write stalled for this long is a dead peer — give up.
const EGRESS_WRITE_TIMEOUT: Duration = Duration::from_secs(60);
/// DATA send timeout: no live tunnel can take the frame within this
/// window → the connection cannot proceed.
const DATA_SEND_TIMEOUT: Duration = Duration::from_secs(30);
/// After the splitter's FIN, force-close the egress write half if the
/// seq gap never fills (a tunnel died and frames are gone).
const HALF_CLOSE_FALLBACK: Duration = Duration::from_secs(10);
/// Tombstone TTL for closed conn_ids (late DATA gets RST instead of
/// being buffered forever).
const CLOSED_TTL: Duration = Duration::from_secs(60);
/// Cap tunnel links per reassembler to bound per-link task/memory usage.
const MAX_TUNNEL_LINKS: usize = 64;

// ── Egress connection ─────────────────────────────────────────────────

struct EgressConn {
    write_tx: mpsc::Sender<Bytes>,
}

impl EgressConn {
    fn write(&self, data: Bytes) -> bool {
        self.write_tx.try_send(data).is_ok()
    }
}

// ── Virtual connection (reassembler side) ─────────────────────────────

struct VirtConnDe {
    egress: EgressConn,
    reorder: Mutex<ReorderBuf>,
    /// Teardown signal (RST / idle sweep / duplicate).
    cancel: Arc<Notify>,
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
    let handshaking: Arc<DashMap<u32, ()>> = Arc::new(DashMap::new());
    let pool = Arc::new(TunnelPool::new());
    // BUG-7: global byte budget for DATA-before-SYN buffering.
    let pending_bytes: Arc<AtomicUsize> = Arc::new(AtomicUsize::new(0));
    // Connection reset counter (observability: logged by the heartbeat).
    let resets: Arc<AtomicU64> = Arc::new(AtomicU64::new(0));

    // Global UDP socket pair for the LEGACY single-client relay (conn 0).
    // BUG-18: v4 + optional v6, so IPv6 targets work too.
    let udp_pair = Arc::new(bind_udp_pair().await?);
    info!(
        v4 = %udp_pair.v4.local_addr()?,
        v6 = udp_pair.v6.as_ref().map(|s| s.local_addr()).transpose()?.map(|a| a.to_string()).unwrap_or_default(),
        "UDP relay ready"
    );

    // Background: read UDP responses from targets → DATA frames → pool
    {
        let udp = udp_pair.clone();
        let pool = pool.clone();
        tokio::spawn(async move {
            let mut buf4 = vec![0u8; 65535];
            let mut buf6 = vec![0u8; 65535];
            let mut udp_seq: u64 = 1;
            loop {
                let (n, src) = tokio::select! {
                    r = udp.v4.recv_from(&mut buf4) => match r { Ok(v) => v, Err(e) => {
                        warn!(error = %e, "UDP relay recv error (v4)");
                        break;
                    }},
                    r = async {
                        match &udp.v6 {
                            Some(s) => s.recv_from(&mut buf6).await,
                            None => std::future::pending().await,
                        }
                    } => match r { Ok(v) => v, Err(e) => {
                        warn!(error = %e, "UDP relay recv error (v6)");
                        break;
                    }},
                };
                // Wrap in SOCKS5 UDP response header
                let src_target = socks5::TargetAddr {
                    address: normalize_ip(src.ip()),
                    port: src.port(),
                };
                let payload = if src.is_ipv4() { &buf4[..n] } else { &buf6[..n] };
                let dgram = match socks5::encode_udp_datagram(&src_target, payload) {
                    Ok(d) => d,
                    Err(e) => {
                        warn!(error = %e, "UDP encode failed");
                        continue;
                    }
                };
                // A wrapped max-size IPv6 datagram can exceed the
                // u16 frame length field — drop instead of
                // truncating and corrupting the stream.
                if dgram.len() > MAX_PAYLOAD {
                    warn!(
                        len = dgram.len(),
                        "UDP datagram exceeds frame capacity, dropping"
                    );
                    continue;
                }
                // BUG-3: only consume the seq when the frame is actually
                // handed to a tunnel — a dropped response used to leave a
                // permanent gap in the splitter's reorder buffer and
                // eventually kill the relay.
                let frame = Frame::data(UDP_CONN_ID, udp_seq, dgram);
                if pool.send(frame) {
                    udp_seq = udp_seq.wrapping_add(1);
                } else {
                    warn!("UDP relay: no live tunnels, dropping response datagram");
                }
            }
        });
    }

    // Spawn a listener for each port
    for &port in &cfg.listen_ports {
        let conns = conns.clone();
        let pending = pending.clone();
        let closed = closed.clone();
        let handshaking = handshaking.clone();
        let pool = pool.clone();
        let local_target = cfg.local_target;
        let listen_ip = cfg.listen_ip;
        let udp = udp_pair.clone();
        let pending_bytes = pending_bytes.clone();
        let resets = resets.clone();
        let lctx = ListenerCtx {
            listen_ip,
            local_target,
            conns,
            pending,
            closed,
            handshaking,
            pool,
            chunk_size: cfg.chunk_size,
            udp_sock: udp,
            pending_bytes,
            resets,
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
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(60)).await;
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
                    vc.cancel.notify_one();
                    return false;
                }
                true
            });
            // Sweep closed-cid tombstones
            hb_closed.retain(|_, since| now.duration_since(*since) < CLOSED_TTL);
            // Sweep stale pending entries that never got a SYN, refunding
            // their byte budget (BUG-7).
            let mut freed = 0usize;
            hb_pending.retain(|_, entry| {
                let keep = entry.since.elapsed().as_secs() < PENDING_TTL_SECS;
                if !keep {
                    freed += entry.bytes;
                }
                keep
            });
            hb_pending_bytes.fetch_sub(freed, Ordering::Relaxed);
            let uptime = start_time.elapsed().as_secs();
            info!(
                uptime,
                alive,
                total,
                queue_depth,
                active_conns = hb_conns.len(),
                pending_cids = hb_pending.len(),
                pending_bytes = hb_pending_bytes.load(Ordering::Relaxed),
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

impl UdpPair {
    /// Send to `host:port`, choosing the socket matching the family.
    async fn send_to(&self, host: &str, port: u16, data: &[u8]) -> anyhow::Result<()> {
        let ip: IpAddr = match host.parse() {
            Ok(ip) => ip,
            Err(_) => {
                // Domain name: resolve (UDP datagram targets are almost
                // always IP literals, but keep domain support).
                let addrs = tokio::net::lookup_host((host, port)).await?;
                addrs.map(|a| a.ip()).next().ok_or_else(|| {
                    anyhow::anyhow!("UDP target resolved to no address: {host}")
                })?
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
    handshaking: Arc<DashMap<u32, ()>>,
    pool: Arc<TunnelPool>,
    chunk_size: usize,
    udp_sock: Arc<UdpPair>,
    pending_bytes: Arc<AtomicUsize>,
    resets: Arc<AtomicU64>,
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
            lost_frames: Mutex::new(Vec::new()),
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
            udp_sock: ctx.udp_sock.clone(),
            pending_bytes: ctx.pending_bytes.clone(),
            resets: ctx.resets.clone(),
            link: link.clone(),
        };
        tokio::spawn(async move {
            if let Err(e) = tunnel_read_loop(rd, reader_ctx).await {
                warn!(tunnel = port, error = %e, "read loop ended");
            }
            link.alive.store(false, Ordering::Release);
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

struct ReadLoopCtx {
    conns: ConnMap,
    pending: PendingMap,
    closed: Arc<DashMap<u32, Instant>>,
    handshaking: Arc<DashMap<u32, ()>>,
    pool: Arc<TunnelPool>,
    local_target: SocketAddr,
    chunk_size: usize,
    udp_sock: Arc<UdpPair>,
    pending_bytes: Arc<AtomicUsize>,
    resets: Arc<AtomicU64>,
    link: Arc<TunnelLink>,
}

async fn tunnel_read_loop(mut rd: tokio::net::tcp::OwnedReadHalf, ctx: ReadLoopCtx) -> Result<()> {
    let mut decoder = FrameDecoder::new();
    loop {
        let frame = match decoder.try_next(&mut rd).await? {
            Some(f) => f,
            None => return Ok(()),
        };
        let plen = frame.payload.len() as u64;
        handle_frame(frame, &ctx).await?;
        ctx.link.bytes_recv.fetch_add(plen, Ordering::Relaxed);
        ctx.link.frames_recv.fetch_add(1, Ordering::Relaxed);
    }
}

// ── Frame handler ─────────────────────────────────────────────────────

async fn handle_frame(frame: Frame, ctx: &ReadLoopCtx) -> Result<()> {
    let cid = frame.conn_id;

    // UDP relay: conn_id 0, DATA → send to target
    if cid == UDP_CONN_ID && frame.flags & FLAG_DATA != 0 {
        handle_udp_frame(frame, &ctx.udp_sock).await;
        return Ok(());
    }
    // Ignore any non-DATA frames for UDP_CONN_ID (SYN/FIN/RST not applicable)
    if cid == UDP_CONN_ID {
        return Ok(());
    }

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
        if ctx.handshaking.insert(cid, ()).is_some() {
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

        // Parse target from SYN payload
        let syn_target = match SynTarget::decode(&frame.payload) {
            Ok(t) => t,
            Err(e) => {
                warn!(conn_id = cid, error = %e, "SYN decode failed");
                remove_pending(ctx, &cid);
                ctx.handshaking.remove(&cid);
                ctx.pool.send(Frame::rst(cid));
                return Ok(());
            }
        };
        info!(conn_id = cid, target = %syn_target.address, proto = syn_target.proto, "SYN");

        // BUG-4: an RST may have arrived while this SYN was in flight —
        // don't build a connection nobody wants.
        if ctx.pending.get(&cid).map(|e| e.cancelled).unwrap_or(false) {
            warn!(conn_id = cid, "SYN cancelled by RST before handshake");
            remove_pending(ctx, &cid);
            ctx.handshaking.remove(&cid);
            ctx.closed.insert(cid, Instant::now());
            return Ok(());
        }

        // BUG-19: UDP association — no egress TCP stream at all. DATA
        // frames go straight to a per-conn UDP socket; a per-conn reader
        // sends responses back with this conn_id (so two clients talking
        // to the same target can't cross their datagrams).
        if syn_target.proto == PROTO_UDP {
            let (write_tx, _write_rx) = mpsc::channel::<Bytes>(1);
            let cancel = Arc::new(Notify::new());
            let half_close = Arc::new(Notify::new());
            let udp_sock = Arc::new(bind_udp_pair().await?);
            let vconn = Arc::new(VirtConnDe {
                egress: EgressConn { write_tx },
                reorder: Mutex::new(ReorderBuf::new()),
                cancel: cancel.clone(),
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
                    let payload = if src.is_ipv4() { &buf4[..n] } else { &buf6[..n] };
                    let dgram = match socks5::encode_udp_datagram(&src_target, payload) {
                        Ok(d) => d,
                        Err(e) => {
                            warn!(conn_id = cid, error = %e, "UDP encode failed");
                            continue;
                        }
                    };
                    if dgram.len() > MAX_PAYLOAD {
                        warn!(conn_id = cid, len = dgram.len(), "UDP datagram exceeds frame capacity, dropping");
                        continue;
                    }
                    let frame = Frame::data(cid, seq, dgram);
                    if pool_r.send(frame) {
                        seq = seq.wrapping_add(1);
                    } else {
                        warn!(conn_id = cid, "UDP relay: no live tunnels, dropping response datagram");
                    }
                }
            });

            // Drain DATA frames that beat the SYN; datagrams need no
            // ordering, just forward each one.
            if let Some(entry) = remove_pending(ctx, &cid) {
                if entry.cancelled {
                    if let Some((_, vc)) = ctx.conns.remove(&cid) {
                        vc.cancel.notify_one();
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
                    ctx.pool.send(Frame::rst(cid));
                    return Ok(());
                }
                Err(_) => {
                    warn!(conn_id = cid, target = %syn_target.address, "egress connect timeout");
                    remove_pending(ctx, &cid);
                    ctx.handshaking.remove(&cid);
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

        let (egress_rd, egress_wr) = egress_stream.into_split();
        let (write_tx, write_rx) = mpsc::channel::<Bytes>(EGRESS_CHANNEL_CAP);
        let cancel = Arc::new(Notify::new());
        let half_close = Arc::new(Notify::new());

        let vconn = Arc::new(VirtConnDe {
            egress: EgressConn { write_tx },
            reorder: Mutex::new(ReorderBuf::new()),
            cancel: cancel.clone(),
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
        tokio::spawn(write_to_egress(write_rx, egress_wr, half_close));

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
            },
        ));

        // Drain any frames that arrived during SOCKS5 connect
        if let Some(entry) = remove_pending(ctx, &cid) {
            if entry.cancelled {
                // BUG-4: RST raced the connect — tear the fresh conn down.
                if let Some((_, vc)) = ctx.conns.remove(&cid) {
                    vc.cancel.notify_one();
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
                        if !vconn.egress.write(chunk) {
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
                forward_udp_datagram(&vconn, &frame.payload).await;
                vconn.bytes_recv.fetch_add(frame.payload.len() as u64, Ordering::Relaxed);
                vconn.frames_recv.fetch_add(1, Ordering::Relaxed);
                *vconn.last_active.lock().unwrap() = Instant::now();
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
                            vconn.cancel.notify_one();
                            drop(vconn);
                        }
                        ctx.pool.send(Frame::rst(cid));
                        ctx.resets.fetch_add(1, Ordering::Relaxed);
                    }
                    return Ok(());
                }
                for chunk in result.ready {
                    if !vconn.egress.write(chunk) {
                        drop(reorder);
                        warn!(conn_id = cid, "egress write failed, resetting connection");
                        drop(vconn);
                        ctx.closed.insert(cid, Instant::now());
                        if let Some((_, vconn)) = ctx.conns.remove(&cid) {
                            vconn.cancel.notify_one();
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
                warn!(conn_id = cid, "pending byte budget exhausted, dropping DATA");
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
                        "pending overflow, dropping DATA"
                    );
                    ctx.pending_bytes.fetch_sub(plen, Ordering::Relaxed);
                }
            } else {
                // Entry vanished between contains_key and get_mut —
                // refund the reservation.
                ctx.pending_bytes.fetch_sub(plen, Ordering::Relaxed);
            }
        } else if ctx.pending.len() < MAX_PENDING_CIDS {
            if !try_reserve_pending(ctx, None, plen) {
                warn!(conn_id = cid, "pending byte budget exhausted, dropping DATA");
                return Ok(());
            }
            let mut entry = PendingEntry::new();
            entry.frames.push(frame);
            entry.bytes = plen;
            ctx.pending.insert(cid, entry);
        } else {
            warn!(conn_id = cid, "pending CID limit reached, dropping DATA");
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
                    warn!(conn_id = cid, "pending overflow, dropping FIN");
                }
            }
        } else if ctx.pending.len() < MAX_PENDING_CIDS {
            let mut entry = PendingEntry::new();
            entry.frames.push(frame);
            ctx.pending.insert(cid, entry);
        } else {
            warn!(conn_id = cid, "pending CID limit reached, dropping FIN");
        }
        return Ok(());
    }

    // RST
    if frame.flags & FLAG_RST != 0 {
        if let Some((_, vconn)) = ctx.conns.remove(&cid) {
            ctx.closed.insert(cid, Instant::now());
            vconn.cancel.notify_one();
            info!(conn_id = cid, "RST, force close");
            drop(vconn);
            ctx.resets.fetch_add(1, Ordering::Relaxed);
            return Ok(());
        }
        // BUG-1/BUG-4: RST while the SYN handshake is in flight — mark
        // the pending entry so the SYN handler aborts the egress connect
        // instead of building a connection nobody wants.
        if ctx.handshaking.contains_key(&cid) || ctx.pending.contains_key(&cid) {
            if let Some(mut entry) = ctx.pending.get_mut(&cid) {
                entry.cancelled = true;
                if let Some(notify) = &entry.cancel {
                    notify.notify_one();
                }
            } else if ctx.pending.len() < MAX_PENDING_CIDS {
                let mut entry = PendingEntry::new();
                entry.cancelled = true;
                ctx.pending.insert(cid, entry);
            }
            ctx.closed.insert(cid, Instant::now());
        }
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

/// Reserve `need` bytes against the global pending budget, evicting the
/// oldest *other* entries when over budget (BUG-7).  Returns false when
/// the budget can't be satisfied.
fn try_reserve_pending(ctx: &ReadLoopCtx, exclude: Option<u32>, need: usize) -> bool {
    if ctx.pending_bytes.load(Ordering::Relaxed) + need <= MAX_PENDING_BYTES {
        ctx.pending_bytes.fetch_add(need, Ordering::Relaxed);
        return true;
    }
    // Over budget: evict oldest entries (other than `exclude`) until the
    // frame fits.  DashMap refs must be dropped before remove().
    loop {
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
        drop(entry_ref);
        if ctx.pending.remove(&key).is_none() {
            // Raced — another task removed it and already refunded.
            continue;
        }
        ctx.pending_bytes.fetch_sub(bytes, Ordering::Relaxed);
        if ctx.pending_bytes.load(Ordering::Relaxed) + need <= MAX_PENDING_BYTES {
            ctx.pending_bytes.fetch_add(need, Ordering::Relaxed);
            return true;
        }
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

async fn handle_udp_frame(frame: Frame, udp_sock: &UdpPair) {
    let (target, data) = match socks5::decode_udp_datagram(&frame.payload) {
        Ok(t) => t,
        Err(e) => {
            warn!(error = %e, "UDP datagram decode failed");
            return;
        }
    };
    if let Err(e) = udp_sock.send_to(&target.address, target.port, &data).await {
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

async fn write_to_egress(
    mut rx: mpsc::Receiver<Bytes>,
    mut wr: tokio::net::tcp::OwnedWriteHalf,
    half_close: Arc<Notify>,
) {
    loop {
        tokio::select! {
            chunk = rx.recv() => {
                match chunk {
                    Some(chunk) => {
                        match tokio::time::timeout(EGRESS_WRITE_TIMEOUT, wr.write_all(&chunk)).await {
                            Ok(Ok(())) => {}
                            _ => break, // write error or stall timeout
                        }
                    }
                    None => break, // vconn dropped — teardown
                }
            }
            _ = half_close.notified() => {
                // Peer FIN and all in-flight data delivered: drain whatever
                // is still queued, then half-close so the server sees EOF
                // and can finish its response.
                while let Ok(chunk) = rx.try_recv() {
                    match tokio::time::timeout(EGRESS_WRITE_TIMEOUT, wr.write_all(&chunk)).await {
                        Ok(Ok(())) => {}
                        _ => break,
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
}

async fn read_from_egress(mut rd: tokio::net::tcp::OwnedReadHalf, ctx: EgressReaderCtx) {
    let conn_id = ctx.conn_id;
    let conns = ctx.conns;
    let pool = ctx.pool;
    let chunk_size = ctx.chunk_size;
    let cancel = ctx.cancel;
    let closed = ctx.closed;
    let vconn = ctx.vconn;
    let mut seq: u64 = 1;
    let mut cancelled = false;
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
                        // frame (bounded by DATA_SEND_TIMEOUT).
                        let sent = tokio::time::timeout(DATA_SEND_TIMEOUT, pool.send_async(frame))
                            .await
                            .unwrap_or(false);
                        if !sent {
                            warn!(conn_id, "no live tunnels for egress response after timeout");
                            break;
                        }
                        // Count on the VirtConnDe
                        if let Some(vconn) = conns.get(&conn_id) {
                            vconn.bytes_sent.fetch_add(n as u64, Ordering::Relaxed);
                            vconn.frames_sent.fetch_add(1, Ordering::Relaxed);
                            *vconn.last_active.lock().unwrap() = Instant::now();
                        }
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
    if !cancelled {
        // Echo FIN to the splitter. D3: do NOT tear the conn down —
        // the splitter may still be sending data on the other direction.
        // The conn is removed only once the splitter's FIN half-closes
        // the egress write side as well (finish_if_done).
        let fin_sent =
            tokio::time::timeout(DATA_SEND_TIMEOUT, pool.send_async(Frame::fin(conn_id, seq)))
                .await
                .unwrap_or(false);
        if !fin_sent {
            warn!(conn_id, "failed to send FIN to splitter");
        }
        vconn.egress_eof.store(true, Ordering::Release);
        finish_if_done(&vconn, conn_id, &conns, &closed);
    }
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
        vc.cancel.notify_one();
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
