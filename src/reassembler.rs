use crate::frame::{Frame, FrameDecoder, SynTarget, FLAG_ACK, FLAG_DATA, FLAG_FIN, FLAG_RST, FLAG_SYN};
use crate::socks5;
use anyhow::{bail, Result};
use bytes::Bytes;
use dashmap::DashMap;
use std::collections::BTreeMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, UdpSocket};
use tokio::sync::mpsc;
use tracing::{error, info, warn};

const UDP_CONN_ID: u32 = 0;
/// If a tunnel receives no frames for this long, it is considered dead and reconnected.
const TUNNEL_READ_TIMEOUT_SECS: u64 = 25;

// ── Config ────────────────────────────────────────────────────────────

pub struct ReassemblerConfig {
    pub listen_ip: IpAddr,
    pub listen_ports: Vec<u16>,
    pub local_target: SocketAddr,
    pub chunk_size: usize,
}

// ── Tunnel pool (same pattern as splitter) ────────────────────────────

struct TunnelLink {
    tx: mpsc::UnboundedSender<Frame>,
    alive: AtomicBool,
    bytes_sent: AtomicU64,
    bytes_recv: AtomicU64,
    frames_sent: AtomicU64,
    frames_recv: AtomicU64,
}

struct TunnelPool {
    links: Mutex<Vec<Arc<TunnelLink>>>,
    rr: AtomicUsize,
}

impl TunnelPool {
    fn new() -> Self {
        Self { links: Mutex::new(Vec::new()), rr: AtomicUsize::new(0) }
    }

    fn add(&self, link: Arc<TunnelLink>) {
        self.links.lock().unwrap().push(link);
    }

    /// Remove dead links from the pool. Called periodically from heartbeat.
    fn compact(&self) {
        let mut links = self.links.lock().unwrap();
        let before = links.len();
        links.retain(|l| l.alive.load(Ordering::Acquire));
        if links.len() != before {
            self.rr.store(0, Ordering::Release);
        }
    }

    fn send(&self, frame: Frame) -> bool {
        let links = self.links.lock().unwrap();
        if links.is_empty() {
            return false;
        }
        let start = self.rr.fetch_add(1, Ordering::Relaxed) % links.len();
        for i in 0..links.len() {
            let link = &links[(start + i) % links.len()];
            if !link.alive.load(Ordering::Acquire) {
                continue;
            }
            if link.tx.send(frame.clone()).is_ok() {
                return true;
            }
            link.alive.store(false, Ordering::Release);
        }
        false
    }

    fn send_via(&self, frame: Frame, src_link: usize) -> bool {
        let links = self.links.lock().unwrap();
        if src_link < links.len() {
            let link = &links[src_link];
            if link.alive.load(Ordering::Acquire) && link.tx.send(frame.clone()).is_ok() {
                return true;
            }
        }
        drop(links);
        self.send(frame)
    }
}

// ── Reorder buffer ────────────────────────────────────────────────────

/// Max out-of-order entries before we drop new arrivals.
const MAX_PENDING_ENTRIES: usize = 256;
/// If a gap persists this long, skip it and RST the connection.
const GAP_TIMEOUT_SECS: u64 = 30;
/// Max number of pending cids with DATA-before-SYN buffered.
const MAX_PENDING_CIDS: usize = 256;

struct ReorderBuf {
    expected: u64,
    pending: BTreeMap<u64, Bytes>,
    gap_since: Option<Instant>,
}

impl ReorderBuf {
    fn new() -> Self {
        Self { expected: 1, pending: BTreeMap::new(), gap_since: None }
    }

    /// Returns (in_order_chunks, gap_timeout).
    /// `gap_timeout` means a persistent gap was skipped — callers should
    /// RST the connection to force cleanup on both ends.
    fn push(&mut self, seq: u64, payload: Bytes) -> (Vec<Bytes>, bool) {
        let mut out = Vec::new();
        let mut gap_timeout = false;

        if seq < self.expected {
            return (out, false);
        }
        if seq == self.expected {
            out.push(payload);
            self.expected = self.expected.wrapping_add(1);
            while let Some(chunk) = self.pending.remove(&self.expected) {
                out.push(chunk);
                self.expected = self.expected.wrapping_add(1);
            }
            self.gap_since = None;
        } else if self.pending.len() < MAX_PENDING_ENTRIES {
            self.pending.insert(seq, payload);
            if self.gap_since.is_none() {
                self.gap_since = Some(Instant::now());
            }
        }

        // Gap timeout: skip the gap, signal RST — don't deliver dirty data
        if let Some(start) = self.gap_since {
            if start.elapsed().as_secs() >= GAP_TIMEOUT_SECS && !self.pending.is_empty() {
                // Advance expected past the gap so the connection can recover
                // if the caller chooses not to RST.
                if let Some(&max_seq) = self.pending.keys().last() {
                    self.expected = max_seq.wrapping_add(1);
                }
                self.pending.clear();
                self.gap_since = None;
                gap_timeout = true;
            }
        }

        (out, gap_timeout)
    }
}

// ── Egress connection ─────────────────────────────────────────────────

struct EgressConn {
    write_tx: mpsc::UnboundedSender<Bytes>,
}

impl EgressConn {
    fn write(&self, data: &[u8]) -> bool {
        self.write_tx.send(Bytes::copy_from_slice(data)).is_ok()
    }
}

// ── Virtual connection (reassembler side) ─────────────────────────────

struct VirtConnDe {
    egress: EgressConn,
    reorder: Mutex<ReorderBuf>,
    created_at: Instant,
    bytes_sent: AtomicU64,
    bytes_recv: AtomicU64,
    frames_sent: AtomicU64,
    frames_recv: AtomicU64,
}

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
                        let dgram = socks5::encode_udp_datagram(&src_target, &buf[..n]);
                        let frame = Frame::data(UDP_CONN_ID, udp_seq, dgram);
                        udp_seq = udp_seq.wrapping_add(1);
                        pool.send(frame);
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
        let pool = pool.clone();
        let local_target = cfg.local_target;
        let listen_ip = cfg.listen_ip;
        let udp = udp_sock.clone();
        tokio::spawn(async move {
            if let Err(e) = run_tunnel_listener(listen_ip, port, local_target, conns, pending, pool, cfg.chunk_size, udp).await {
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
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(60)).await;
            let links = hb_pool.links.lock().unwrap();
            let total = links.len();
            let alive = links.iter().filter(|l| l.alive.load(Ordering::Acquire)).count();
            drop(links);
            // Sweep dead links that accumulated from tunnel reconnects
            hb_pool.compact();
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

async fn run_tunnel_listener(
    listen_ip: IpAddr,
    port: u16,
    local_target: SocketAddr,
    conns: ConnMap,
    pending: PendingMap,
    pool: Arc<TunnelPool>,
    chunk_size: usize,
    udp_sock: Arc<UdpSocket>,
) -> Result<()> {
    let listener = TcpListener::bind((listen_ip, port)).await?;
    info!(listen = %listen_ip, port, "tunnel listener ready");

    loop {
        let (stream, peer) = listener.accept().await?;
        let _ = stream.set_nodelay(true);

        // SOCKS5 handshake: sing-box SOCKS5 outbound CONNECTs here.
        // Accept any no-auth client, ignore the CONNECT target.
        let stream = match socks5::socks5_accept_tunnel(stream).await {
            Ok(s) => s,
            Err(e) => {
                warn!(peer = %peer, error = %e, "SOCKS5 handshake failed");
                continue;
            }
        };

        info!(peer = %peer, port, pool_size = pool.links.lock().unwrap().len() + 1, "tunnel link accepted");

        let (rd, wr) = stream.into_split();
        let (tx, rx) = mpsc::unbounded_channel::<Frame>();
        let link = Arc::new(TunnelLink {
            tx,
            alive: AtomicBool::new(true),
            bytes_sent: AtomicU64::new(0),
            bytes_recv: AtomicU64::new(0),
            frames_sent: AtomicU64::new(0),
            frames_recv: AtomicU64::new(0),
        });
        pool.add(link.clone());

        // Writer task
        tokio::spawn(drain_frames(rx, wr, link.clone()));

        // Reader task (one per link)
        let conns = conns.clone();
        let pending = pending.clone();
        let pool = pool.clone();
        let udp = udp_sock.clone();
        let link2 = link.clone();
        tokio::spawn(async move {
            if let Err(e) = tunnel_read_loop(rd, conns, pending, pool, local_target, port as usize, chunk_size, udp, &link2).await {
                warn!(tunnel = port, error = %e, "read loop ended");
            }
            link2.alive.store(false, Ordering::Release);
            info!(tunnel = port,
                bytes_sent = link2.bytes_sent.load(Ordering::Relaxed),
                bytes_recv = link2.bytes_recv.load(Ordering::Relaxed),
                frames_sent = link2.frames_sent.load(Ordering::Relaxed),
                frames_recv = link2.frames_recv.load(Ordering::Relaxed),
                "disconnected");
        });
    }
}

/// Time between keepalive frames when the tunnel is idle.
/// Must be well below sing-box's 25s SOCKS5 idle timeout.
/// 12s gives 13s margin against transient TCP congestion delays.
const KEEPALIVE_INTERVAL_SECS: u64 = 12;

async fn drain_frames(
    mut rx: mpsc::UnboundedReceiver<Frame>,
    mut wr: tokio::net::tcp::OwnedWriteHalf,
    link: Arc<TunnelLink>,
) {
    let ka = Frame { conn_id: 0, seq: 0, flags: 0, payload: Bytes::new() };
    let ka_bytes = ka.encode();
    loop {
        tokio::select! {
            frame = rx.recv() => {
                match frame {
                    Some(frame) => {
                        let n = frame.payload.len() as u64;
                        if wr.write_all(&frame.encode()).await.is_err() {
                            break;
                        }
                        link.bytes_sent.fetch_add(n, Ordering::Relaxed);
                        link.frames_sent.fetch_add(1, Ordering::Relaxed);
                    }
                    None => break,
                }
            }
            _ = tokio::time::sleep(Duration::from_secs(KEEPALIVE_INTERVAL_SECS)) => {
                if wr.write_all(&ka_bytes).await.is_err() {
                    break;
                }
            }
        }
    }
    let _ = wr.shutdown().await;
}

async fn tunnel_read_loop(
    mut rd: tokio::net::tcp::OwnedReadHalf,
    conns: ConnMap,
    pending: PendingMap,
    pool: Arc<TunnelPool>,
    local_target: SocketAddr,
    src_port: usize,
    chunk_size: usize,
    udp_sock: Arc<UdpSocket>,
    link: &TunnelLink,
) -> Result<()> {
    let mut decoder = FrameDecoder::new();
    loop {
        // Adaptive timeout: short when connections are active, long when idle.
        let timeout = if conns.is_empty() { 300 } else { TUNNEL_READ_TIMEOUT_SECS };
        let frame = match tokio::time::timeout(
            Duration::from_secs(timeout),
            decoder.try_next(&mut rd),
        )
        .await
        {
            Ok(Ok(Some(f))) => f,
            Ok(Ok(None)) => return Ok(()),
            Ok(Err(e)) => return Err(e),
            Err(_elapsed) => {
                bail!("tunnel read idle timeout after {timeout}s");
            }
        };
        let plen = frame.payload.len() as u64;
        handle_frame(frame, &conns, &pending, &pool, local_target, src_port, chunk_size, &udp_sock).await?;
        link.bytes_recv.fetch_add(plen, Ordering::Relaxed);
        link.frames_recv.fetch_add(1, Ordering::Relaxed);
    }
}

// ── Frame handler ─────────────────────────────────────────────────────

async fn handle_frame(
    frame: Frame,
    conns: &ConnMap,
    pending: &PendingMap,
    pool: &Arc<TunnelPool>,
    local_target: SocketAddr,
    src_port: usize,
    chunk_size: usize,
    udp_sock: &Arc<UdpSocket>,
) -> Result<()> {
    let cid = frame.conn_id;

    // UDP relay: conn_id 0, DATA → send to target
    if cid == UDP_CONN_ID && frame.flags & FLAG_DATA != 0 {
        return handle_udp_frame(frame, udp_sock).await;
    }

    // SYN: new virtual connection
    if frame.flags & FLAG_SYN != 0 {
        // Reserve a pending slot so DATA/FIN arriving during SOCKS5 connect
        // are queued instead of dropped.  Use entry API so we don't
        // overwrite DATA frames that already arrived before the SYN.
        pending.entry(cid).or_insert_with(|| PendingEntry {
            frames: Vec::new(),
            since: Instant::now(),
        });

        // Parse target from SYN payload
        let syn_target = SynTarget::decode(&frame.payload)?;
        info!(conn_id = cid, target = %syn_target.address, proto = syn_target.proto, "SYN");

        // Connect to local_target via SOCKS5 (with timeout)
        let egress_stream = match tokio::time::timeout(
            Duration::from_secs(10),
            socks5::socks5_client_connect(local_target, &syn_target.address, syn_target.port),
        )
        .await
        {
            Ok(Ok(s)) => s,
            Ok(Err(e)) => {
                warn!(conn_id = cid, target = %syn_target.address, error = %e, "egress connect failed");
                pending.remove(&cid);
                pool.send_via(Frame::rst(cid), src_port);
                return Ok(());
            }
            Err(_) => {
                warn!(conn_id = cid, target = %syn_target.address, "egress connect timeout");
                pending.remove(&cid);
                pool.send_via(Frame::rst(cid), src_port);
                return Ok(());
            }
        };
        let _ = egress_stream.set_nodelay(true);

        let (egress_rd, egress_wr) = egress_stream.into_split();
        let (write_tx, write_rx) = mpsc::unbounded_channel::<Bytes>();

        let vconn = Arc::new(VirtConnDe {
            egress: EgressConn { write_tx },
            reorder: Mutex::new(ReorderBuf::new()),
            created_at: Instant::now(),
            bytes_sent: AtomicU64::new(0),
            bytes_recv: AtomicU64::new(0),
            frames_sent: AtomicU64::new(0),
            frames_recv: AtomicU64::new(0),
        });

        // Spawn egress writer: ordered data → egress connection
        tokio::spawn(write_to_egress(write_rx, egress_wr));

        // Spawn egress reader: egress response → frames → pool
        let conns_clone = conns.clone();
        let pool_clone = Arc::clone(pool);
        tokio::spawn(read_from_egress(cid, egress_rd, conns_clone, pool_clone, chunk_size));

        conns.insert(cid, vconn.clone());

        // Drain any frames that arrived during SOCKS5 connect
        if let Some((_, entry)) = pending.remove(&cid) {
            for f in entry.frames {
                if f.flags & FLAG_DATA != 0 {
                    let (ready, gap_timeout) = vconn.reorder.lock().unwrap().push(f.seq, f.payload);
                    if gap_timeout {
                        conns.remove(&cid);
                        break;
                    }
                    for chunk in ready {
                        if !vconn.egress.write(&chunk) {
                            warn!(conn_id = cid, "egress write failed (drain)");
                            break;
                        }
                    }
                } else if f.flags & FLAG_FIN != 0 {
                    info!(conn_id = cid, "FIN during SYN, cleaning up");
                    conns.remove(&cid);
                }
            }
        }

        // Reply SYN+ACK
        let syn_ack = Frame::syn_ack(cid);
        pool.send_via(syn_ack, src_port);

        return Ok(());
    }

    // DATA
    if frame.flags & FLAG_DATA != 0 {
        if let Some(vconn) = conns.get(&cid) {
            let plen = frame.payload.len() as u64;
            let (ready, gap_timeout) = vconn.reorder.lock().unwrap().push(frame.seq, frame.payload);
            if gap_timeout {
                warn!(conn_id = cid, "reorder gap timeout → RST");
                pool.send(Frame::rst(cid));
                conns.remove(&cid);
                return Ok(());
            }
            for chunk in ready {
                if !vconn.egress.write(&chunk) {
                    warn!(conn_id = cid, "egress write failed");
                    break;
                }
            }
            vconn.bytes_recv.fetch_add(plen, Ordering::Relaxed);
            vconn.frames_recv.fetch_add(1, Ordering::Relaxed);
            return Ok(());
        }
        // Not in conns — could be pending (SYN still in flight) or
        // DATA arrived before SYN (out-of-order delivery across tunnels).
        // Create a pending slot so data isn't lost — the SYN handler
        // will drain it once the egress connection is established.
        if let Some(mut entry) = pending.get_mut(&cid) {
            if entry.frames.len() < MAX_PENDING_FRAMES_PER_CID {
                entry.frames.push(frame);
            } else {
                warn!(conn_id = cid, count = entry.frames.len(), "pending overflow, dropping DATA");
            }
        } else if pending.len() < MAX_PENDING_CIDS {
            pending.insert(cid, PendingEntry {
                frames: vec![frame],
                since: Instant::now(),
            });
        }
        return Ok(());
    }

    // FIN
    if frame.flags & FLAG_FIN != 0 {
        if let Some((_, vconn)) = conns.remove(&cid) {
            let dur = vconn.created_at.elapsed().as_millis() as u64;
            info!(conn_id = cid,
                bytes_sent = vconn.bytes_sent.load(Ordering::Relaxed),
                bytes_recv = vconn.bytes_recv.load(Ordering::Relaxed),
                frames_sent = vconn.frames_sent.load(Ordering::Relaxed),
                frames_recv = vconn.frames_recv.load(Ordering::Relaxed),
                duration_ms = dur,
                "FIN, closed");
            drop(vconn);
        }
        return Ok(());
    }

    // RST
    if frame.flags & FLAG_RST != 0 {
        if let Some((_, vconn)) = conns.remove(&cid) {
            info!(conn_id = cid, "RST, force close");
            drop(vconn);
        }
        return Ok(());
    }

    // ACK frames are ignored — TCP backpressure replaces application flow control.
    let _ = frame.flags & FLAG_ACK;

    Ok(())
}

// ── UDP relay handler ─────────────────────────────────────────────────

async fn handle_udp_frame(frame: Frame, udp_sock: &UdpSocket) -> Result<()> {
    let (target, data) = socks5::decode_udp_datagram(&frame.payload)?;
    udp_sock.send_to(&data, (target.address.as_str(), target.port)).await?;
    Ok(())
}

// ── Egress I/O tasks ──────────────────────────────────────────────────

async fn write_to_egress(
    mut rx: mpsc::UnboundedReceiver<Bytes>,
    mut wr: tokio::net::tcp::OwnedWriteHalf,
) {
    while let Some(chunk) = rx.recv().await {
        if wr.write_all(&chunk).await.is_err() {
            break;
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
) {
    let mut buf = vec![0u8; chunk_size];
    let mut seq: u64 = 1;
    loop {
        match rd.read(&mut buf).await {
            Ok(0) => break,
            Ok(n) => {
                let frame = Frame::data(conn_id, seq, Bytes::copy_from_slice(&buf[..n]));
                // Backpressure: if all tunnel channels are momentarily full,
                // yield and retry instead of killing the connection.
                let mut sent = false;
                for retry in 0..5 {
                    if pool.send(frame.clone()) {
                        sent = true;
                        break;
                    }
                    if retry == 0 {
                        warn!(conn_id, "egress backpressure, retrying");
                    }
                    tokio::task::yield_now().await;
                }
                if !sent {
                    warn!(conn_id, "no live tunnels for egress response after retries");
                    break;
                }
                // Count on the VirtConnDe
                if let Some(vconn) = conns.get(&conn_id) {
                    vconn.bytes_sent.fetch_add(n as u64, Ordering::Relaxed);
                    vconn.frames_sent.fetch_add(1, Ordering::Relaxed);
                }
                seq += 1;
            }
            Err(e) => {
                warn!(conn_id, error = %e, "egress read error");
                break;
            }
        }
    }
    pool.send(Frame::fin(conn_id, seq));
    if let Some((_, vconn)) = conns.remove(&conn_id) {
        let dur = vconn.created_at.elapsed().as_millis() as u64;
        info!(conn_id,
            bytes_sent = vconn.bytes_sent.load(Ordering::Relaxed),
            bytes_recv = vconn.bytes_recv.load(Ordering::Relaxed),
            frames_sent = vconn.frames_sent.load(Ordering::Relaxed),
            frames_recv = vconn.frames_recv.load(Ordering::Relaxed),
            duration_ms = dur,
            "closed");
    }
}
