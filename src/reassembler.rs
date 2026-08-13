use crate::frame::{
    FLAG_DATA, FLAG_FIN, FLAG_RST, FLAG_SYN, Frame, FrameDecoder, MAX_PAYLOAD, MAX_PENDING_CIDS,
    SynTarget, UDP_CONN_ID,
};
use crate::reorder::ReorderBuf;
use crate::socks5;
use crate::tunnel::{TUNNEL_CHANNEL_CAP, TunnelLink, TunnelPool, drain_frames};
use anyhow::Result;
use bytes::Bytes;
use dashmap::DashMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
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
    half_closed: AtomicBool,
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

    // Global UDP socket for relay (responses from targets come back here)
    let udp_sock = Arc::new(UdpSocket::bind("0.0.0.0:0").await?);
    info!(addr = %udp_sock.local_addr()?, "UDP relay ready");

    // Background: read UDP responses from targets → DATA frames → pool
    {
        let udp = udp_sock.clone();
        let pool = pool.clone();
        tokio::spawn(async move {
            let mut buf = vec![0u8; 65535];
            let mut udp_seq: u64 = 1;
            loop {
                match udp.recv_from(&mut buf).await {
                    Ok((n, src)) => {
                        // Wrap in SOCKS5 UDP response header
                        let src_target = socks5::TargetAddr {
                            address: src.ip().to_string(),
                            port: src.port(),
                        };
                        let dgram = match socks5::encode_udp_datagram(&src_target, &buf[..n]) {
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
                        let frame = Frame::data(UDP_CONN_ID, udp_seq, dgram);
                        udp_seq = udp_seq.wrapping_add(1);
                        if !pool.send(frame) {
                            warn!("UDP relay: no live tunnels, dropping response datagram");
                        }
                    }
                    Err(e) => {
                        warn!(error = %e, "UDP relay recv error");
                    }
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
        let udp = udp_sock.clone();
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
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(60)).await;
            let (alive, total) = hb_pool.stats();
            // Sweep dead links that accumulated from tunnel reconnects
            hb_pool.compact();
            // Sweep idle connections
            let now = Instant::now();
            hb_conns.retain(|&cid, vc| {
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
                    hb_closed.insert(cid, Instant::now());
                    vc.cancel.notify_one();
                    return false;
                }
                true
            });
            // Sweep closed-cid tombstones
            hb_closed.retain(|_, since| now.duration_since(*since) < CLOSED_TTL);
            // Sweep stale pending entries that never got a SYN
            hb_pending.retain(|_, entry| entry.since.elapsed().as_secs() < PENDING_TTL_SECS);
            let uptime = start_time.elapsed().as_secs();
            info!(
                uptime,
                alive,
                total,
                active_conns = hb_conns.len(),
                "heartbeat"
            );
        }
    });

    // Keep alive
    tokio::signal::ctrl_c().await?;
    info!("shutting down");
    Ok(())
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
    udp_sock: Arc<UdpSocket>,
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
        if ctx.pool.link_count() >= MAX_TUNNEL_LINKS {
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
        });
        ctx.pool.add(link.clone());

        // Writer task
        tokio::spawn(drain_frames(rx, wr, link.clone()));

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
    udp_sock: Arc<UdpSocket>,
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

        // Reserve a pending slot so DATA/FIN arriving during SOCKS5 connect
        // are queued instead of dropped.  Use entry API so we don't
        // overwrite DATA frames that already arrived before the SYN.
        ctx.pending.entry(cid).or_insert_with(|| PendingEntry {
            frames: Vec::new(),
            since: Instant::now(),
        });

        // Parse target from SYN payload
        let syn_target = match SynTarget::decode(&frame.payload) {
            Ok(t) => t,
            Err(e) => {
                warn!(conn_id = cid, error = %e, "SYN decode failed");
                ctx.pending.remove(&cid);
                ctx.handshaking.remove(&cid);
                ctx.pool.send(Frame::rst(cid));
                return Ok(());
            }
        };
        info!(conn_id = cid, target = %syn_target.address, proto = syn_target.proto, "SYN");

        // Connect to local_target via SOCKS5 (with timeout)
        let egress_stream = match tokio::time::timeout(
            Duration::from_secs(10),
            socks5::socks5_client_connect(ctx.local_target, &syn_target.address, syn_target.port),
        )
        .await
        {
            Ok(Ok(s)) => s,
            Ok(Err(e)) => {
                warn!(conn_id = cid, target = %syn_target.address, error = %e, "egress connect failed");
                ctx.pending.remove(&cid);
                ctx.handshaking.remove(&cid);
                ctx.pool.send(Frame::rst(cid));
                return Ok(());
            }
            Err(_) => {
                warn!(conn_id = cid, target = %syn_target.address, "egress connect timeout");
                ctx.pending.remove(&cid);
                ctx.handshaking.remove(&cid);
                ctx.pool.send(Frame::rst(cid));
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
            half_closed: AtomicBool::new(false),
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
            cid,
            egress_rd,
            ctx.conns.clone(),
            ctx.pool.clone(),
            ctx.chunk_size,
            cancel,
            ctx.closed.clone(),
        ));

        // Drain any frames that arrived during SOCKS5 connect
        if let Some((_, entry)) = ctx.pending.remove(&cid) {
            let mut fin_seq: Option<u64> = None;
            for f in entry.frames {
                if f.flags & FLAG_DATA != 0 {
                    let result = vconn.reorder.lock().unwrap().push(f.seq, f.payload);
                    // Pending frames are replayed — stats already counted when
                    // originally queued, so don't double-count here.
                    for chunk in result.ready {
                        if !vconn.egress.write(chunk) {
                            warn!(conn_id = cid, "egress write failed (drain)");
                            break;
                        }
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
            }
        }

        ctx.handshaking.remove(&cid);
        return Ok(());
    }

    // DATA
    if frame.flags & FLAG_DATA != 0 {
        if let Some(vconn) = ctx.conns.get(&cid) {
            let plen = frame.payload.len() as u64;
            let result = vconn.reorder.lock().unwrap().push(frame.seq, frame.payload);
            if !result.accepted {
                if result.overflow {
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
                }
                return Ok(());
            }
            for chunk in result.ready {
                if !vconn.egress.write(chunk) {
                    warn!(conn_id = cid, "egress write failed, resetting connection");
                    drop(vconn);
                    ctx.closed.insert(cid, Instant::now());
                    if let Some((_, vconn)) = ctx.conns.remove(&cid) {
                        vconn.cancel.notify_one();
                        drop(vconn);
                    }
                    ctx.pool.send(Frame::rst(cid));
                    return Ok(());
                }
            }
            if vconn.fin_received.load(Ordering::Acquire) {
                close_write_half(&vconn, cid, false);
            }
            vconn.bytes_recv.fetch_add(plen, Ordering::Relaxed);
            vconn.frames_recv.fetch_add(1, Ordering::Relaxed);
            *vconn.last_active.lock().unwrap() = Instant::now();
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
        if let Some(mut entry) = ctx.pending.get_mut(&cid) {
            if entry.frames.len() < MAX_PENDING_FRAMES_PER_CID {
                entry.frames.push(frame);
            } else {
                warn!(
                    conn_id = cid,
                    count = entry.frames.len(),
                    "pending overflow, dropping DATA"
                );
            }
        } else if ctx.pending.len() < MAX_PENDING_CIDS {
            ctx.pending.insert(
                cid,
                PendingEntry {
                    frames: vec![frame],
                    since: Instant::now(),
                },
            );
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
            start_half_close(&vconn, frame.seq, cid);
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
        }
        return Ok(());
    }

    Ok(())
}

// ── UDP relay handler ─────────────────────────────────────────────────

async fn handle_udp_frame(frame: Frame, udp_sock: &UdpSocket) {
    let (target, data) = match socks5::decode_udp_datagram(&frame.payload) {
        Ok(t) => t,
        Err(e) => {
            warn!(error = %e, "UDP datagram decode failed");
            return;
        }
    };
    if let Err(e) = udp_sock
        .send_to(&data, (target.address.as_str(), target.port))
        .await
    {
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
fn close_write_half(vconn: &VirtConnDe, cid: u32, force: bool) {
    if vconn.half_closed.swap(true, Ordering::AcqRel) {
        return; // already handled
    }
    let fin_seq = vconn.fin_seq.load(Ordering::Acquire);
    if !force && !vconn.reorder.lock().unwrap().is_complete_through(fin_seq) {
        // Gap frames still in flight — retry when they arrive.
        vconn.half_closed.store(false, Ordering::Release);
        return;
    }
    if force {
        warn!(
            conn_id = cid,
            fin_seq, "write half force-closed with pending frames"
        );
    }
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

async fn read_from_egress(
    conn_id: u32,
    mut rd: tokio::net::tcp::OwnedReadHalf,
    conns: ConnMap,
    pool: Arc<TunnelPool>,
    chunk_size: usize,
    cancel: Arc<Notify>,
    closed: Arc<DashMap<u32, Instant>>,
) {
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
        // Echo FIN to the splitter; then the conn is done and late DATA
        // must be answered with RST, not buffered.
        let fin_sent =
            tokio::time::timeout(DATA_SEND_TIMEOUT, pool.send_async(Frame::fin(conn_id, seq)))
                .await
                .unwrap_or(false);
        if !fin_sent {
            warn!(conn_id, "failed to send FIN to splitter");
        }
        closed.insert(conn_id, Instant::now());
        if let Some((_, vconn)) = conns.remove(&conn_id) {
            let dur = vconn.created_at.elapsed().as_millis() as u64;
            info!(
                conn_id,
                bytes_sent = vconn.bytes_sent.load(Ordering::Relaxed),
                bytes_recv = vconn.bytes_recv.load(Ordering::Relaxed),
                frames_sent = vconn.frames_sent.load(Ordering::Relaxed),
                frames_recv = vconn.frames_recv.load(Ordering::Relaxed),
                duration_ms = dur,
                "closed"
            );
        }
    }
}
