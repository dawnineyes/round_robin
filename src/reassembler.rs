use crate::frame::{
    FLAG_ACK, FLAG_DATA, FLAG_FIN, FLAG_RST, FLAG_SYN, Frame, FrameDecoder, SynTarget,
};
use crate::socks5;
use anyhow::Result;
use bytes::Bytes;
use dashmap::DashMap;
use std::collections::BTreeMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, UdpSocket};
use tokio::sync::{Notify, mpsc};
use tracing::{error, info, warn};

const UDP_CONN_ID: u32 = 0;

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
        Self {
            links: Mutex::new(Vec::new()),
            rr: AtomicUsize::new(0),
        }
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
const MAX_PENDING_ENTRIES: usize = 512;
/// Max number of pending cids with DATA-before-SYN buffered.
const MAX_PENDING_CIDS: usize = 256;

struct ReorderBuf {
    expected: u64,
    pending: BTreeMap<u64, Bytes>,
}

impl ReorderBuf {
    fn new() -> Self {
        Self {
            expected: 1,
            pending: BTreeMap::new(),
        }
    }

    /// Returns in-order chunks. Out-of-order frames are buffered until the gap fills.
    /// TUIC TCP guarantees delivery — we just wait.
    fn push(&mut self, seq: u64, payload: Bytes) -> Vec<Bytes> {
        let mut out = Vec::new();

        if seq < self.expected {
            return out;
        }
        if seq == self.expected {
            out.push(payload);
            self.expected = self.expected.wrapping_add(1);
            while let Some(chunk) = self.pending.remove(&self.expected) {
                out.push(chunk);
                self.expected = self.expected.wrapping_add(1);
            }
        } else if self.pending.len() < MAX_PENDING_ENTRIES {
            self.pending.insert(seq, payload);
        }

        out
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
    cancel: Arc<Notify>,
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
                        let dgram = match socks5::encode_udp_datagram(&src_target, &buf[..n]) {
                            Ok(d) => d,
                            Err(e) => {
                                warn!(error = %e, "UDP encode failed");
                                continue;
                            }
                        };
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
        let pool = pool.clone();
        let local_target = cfg.local_target;
        let listen_ip = cfg.listen_ip;
        let udp = udp_sock.clone();
        tokio::spawn(async move {
            if let Err(e) = run_tunnel_listener(
                listen_ip,
                port,
                local_target,
                conns,
                pending,
                pool,
                cfg.chunk_size,
                udp,
            )
            .await
            {
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
            let alive = links
                .iter()
                .filter(|l| l.alive.load(Ordering::Acquire))
                .count();
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
            if let Err(e) = tunnel_read_loop(
                rd,
                conns,
                pending,
                pool,
                local_target,
                port as usize,
                chunk_size,
                udp,
                &link2,
            )
            .await
            {
                warn!(tunnel = port, error = %e, "read loop ended");
            }
            link2.alive.store(false, Ordering::Release);
            info!(
                tunnel = port,
                bytes_sent = link2.bytes_sent.load(Ordering::Relaxed),
                bytes_recv = link2.bytes_recv.load(Ordering::Relaxed),
                frames_sent = link2.frames_sent.load(Ordering::Relaxed),
                frames_recv = link2.frames_recv.load(Ordering::Relaxed),
                "disconnected"
            );
        });
    }
}

async fn drain_frames(
    mut rx: mpsc::UnboundedReceiver<Frame>,
    mut wr: tokio::net::tcp::OwnedWriteHalf,
    link: Arc<TunnelLink>,
) {
    while let Some(frame) = rx.recv().await {
        let n = frame.payload.len() as u64;
        if wr.write_all(&frame.encode()).await.is_err() {
            break;
        }
        link.bytes_sent.fetch_add(n, Ordering::Relaxed);
        link.frames_sent.fetch_add(1, Ordering::Relaxed);
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
        let frame = match decoder.try_next(&mut rd).await? {
            Some(f) => f,
            None => return Ok(()),
        };
        let plen = frame.payload.len() as u64;
        handle_frame(
            frame,
            &conns,
            &pending,
            &pool,
            local_target,
            src_port,
            chunk_size,
            &udp_sock,
        )
        .await?;
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
        handle_udp_frame(frame, udp_sock).await;
        return Ok(());
    }
    // Ignore any non-DATA frames for UDP_CONN_ID (SYN/FIN/RST not applicable)
    if cid == UDP_CONN_ID {
        return Ok(());
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
        let syn_target = match SynTarget::decode(&frame.payload) {
            Ok(t) => t,
            Err(e) => {
                warn!(conn_id = cid, error = %e, "SYN decode failed");
                pending.remove(&cid);
                pool.send_via(Frame::rst(cid), src_port);
                return Ok(());
            }
        };
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
        let cancel = Arc::new(Notify::new());

        let vconn = Arc::new(VirtConnDe {
            egress: EgressConn { write_tx },
            reorder: Mutex::new(ReorderBuf::new()),
            cancel: cancel.clone(),
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
        tokio::spawn(read_from_egress(
            cid,
            egress_rd,
            conns_clone,
            pool_clone,
            chunk_size,
            cancel,
        ));

        conns.insert(cid, vconn.clone());

        // Drain any frames that arrived during SOCKS5 connect
        if let Some((_, entry)) = pending.remove(&cid) {
            for f in entry.frames {
                if f.flags & FLAG_DATA != 0 {
                    let ready = vconn.reorder.lock().unwrap().push(f.seq, f.payload);
                    for chunk in ready {
                        if !vconn.egress.write(&chunk) {
                            warn!(conn_id = cid, "egress write failed (drain)");
                            break;
                        }
                    }
                } else if f.flags & FLAG_FIN != 0 {
                    info!(conn_id = cid, "FIN during SYN, cleaning up");
                    vconn.cancel.notify_one();
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
            let ready = vconn.reorder.lock().unwrap().push(frame.seq, frame.payload);
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
                warn!(
                    conn_id = cid,
                    count = entry.frames.len(),
                    "pending overflow, dropping DATA"
                );
            }
        } else if pending.len() < MAX_PENDING_CIDS {
            pending.insert(
                cid,
                PendingEntry {
                    frames: vec![frame],
                    since: Instant::now(),
                },
            );
        }
        return Ok(());
    }

    // FIN
    if frame.flags & FLAG_FIN != 0 {
        if let Some((_, vconn)) = conns.remove(&cid) {
            vconn.cancel.notify_one();
            let dur = vconn.created_at.elapsed().as_millis() as u64;
            info!(
                conn_id = cid,
                bytes_sent = vconn.bytes_sent.load(Ordering::Relaxed),
                bytes_recv = vconn.bytes_recv.load(Ordering::Relaxed),
                frames_sent = vconn.frames_sent.load(Ordering::Relaxed),
                frames_recv = vconn.frames_recv.load(Ordering::Relaxed),
                duration_ms = dur,
                "FIN, closed"
            );
            drop(vconn);
        }
        return Ok(());
    }

    // RST
    if frame.flags & FLAG_RST != 0 {
        if let Some((_, vconn)) = conns.remove(&cid) {
            vconn.cancel.notify_one();
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
    cancel: Arc<Notify>,
) {
    let mut buf = vec![0u8; chunk_size];
    let mut seq: u64 = 1;
    let mut cancelled = false;
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
        }
    }
    if !cancelled {
        pool.send(Frame::fin(conn_id, seq));
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
