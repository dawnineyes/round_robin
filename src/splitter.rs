use crate::frame::{self, AckInfo, Frame, FrameDecoder, SynTarget, FLAG_ACK, FLAG_DATA, FLAG_FIN, FLAG_RST, FLAG_SYN};
use crate::socks5;
use anyhow::{bail, Result};
use bytes::Bytes;
use dashmap::DashMap;
use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream, UdpSocket};
use tokio::sync::mpsc;
use tracing::{error, info, warn};

// ponytail: channel cap per tunnel; bump if 9× saturated links drop frames
const TUNNEL_CHAN_CAP: usize = 64;
/// Consecutive channel-full results before cooling down a tunnel.
const STRIKE_THRESHOLD: u32 = 3;
/// Round-robin rounds to skip a degraded tunnel.
const COOLDOWN_ROUNDS: u32 = 16;
/// Initial send credit (bytes) before first ACK arrives.
const INITIAL_CREDIT: i64 = 262144;
/// If a tunnel receives no frames for this long, it is considered dead and reconnected.
const TUNNEL_READ_TIMEOUT_SECS: u64 = 25;
/// Max time to wait for credit (ACK) before closing connection as dead.
const CREDIT_TIMEOUT_SECS: u64 = 15;
/// Reserved conn_id for UDP relay traffic.
const UDP_CONN_ID: u32 = 0;

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

// ── Tunnel pool ───────────────────────────────────────────────────────

struct TunnelLink {
    tx: mpsc::Sender<Frame>,
    alive: AtomicBool,
    strikes: AtomicU32,
    skip_rounds: AtomicU32,
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

    fn link_count(&self) -> usize {
        self.links.lock().unwrap().len()
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

    /// Round-robin send. Skips full channels and degraded tunnels.
    fn send(&self, frame: Frame) -> bool {
        let links = self.links.lock().unwrap();
        if links.is_empty() {
            return false;
        }
        let start = self.rr.fetch_add(1, Ordering::Relaxed) % links.len();
        let n = links.len();
        for i in 0..n {
            let idx = (start + i) % n;
            let link = &links[idx];
            if !link.alive.load(Ordering::Acquire) {
                continue;
            }
            // Cool-down: skip degraded tunnels, decrement counter
            let skip = link.skip_rounds.load(Ordering::Acquire);
            if skip > 0 {
                link.skip_rounds.store(skip - 1, Ordering::Release);
                continue;
            }
            match link.tx.try_send(frame.clone()) {
                Ok(()) => {
                    link.strikes.store(0, Ordering::Release);
                    return true;
                }
                Err(mpsc::error::TrySendError::Full(_)) => {
                    let s = link.strikes.fetch_add(1, Ordering::AcqRel) + 1;
                    if s >= STRIKE_THRESHOLD {
                        link.strikes.store(0, Ordering::Release);
                        link.skip_rounds.store(COOLDOWN_ROUNDS, Ordering::Release);
                        warn!(tunnel = idx, cooldown = COOLDOWN_ROUNDS, "degraded");
                    }
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    link.alive.store(false, Ordering::Release);
                }
            }
        }
        false
    }

}

// ── Reorder buffer ────────────────────────────────────────────────────

/// Max out-of-order entries before we drop new arrivals.
const MAX_PENDING_ENTRIES: usize = 256;
/// If a gap persists this long, skip it and RST the connection.
const GAP_TIMEOUT_SECS: u64 = 30;

struct ReorderBuf {
    expected: u64,
    pending: BTreeMap<u64, Bytes>,
    gap_since: Option<Instant>,
}

impl ReorderBuf {
    fn new() -> Self {
        Self { expected: 1, pending: BTreeMap::new(), gap_since: None } // DATA seq starts at 1
    }

    /// Returns (in_order_chunks, gap_timeout).
    fn push(&mut self, seq: u64, payload: Bytes) -> (Vec<Bytes>, bool) {
        let mut out = Vec::new();
        let mut gap_timeout = false;

        if seq < self.expected {
            return (out, false); // duplicate
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

        // Gap timeout: skip the gap, deliver pending data to unblock
        if let Some(start) = self.gap_since {
            if start.elapsed().as_secs() >= GAP_TIMEOUT_SECS && !self.pending.is_empty() {
                let keys: Vec<u64> = self.pending.keys().cloned().collect();
                if let Some(&max_seq) = keys.last() {
                    for k in keys {
                        if let Some(chunk) = self.pending.remove(&k) {
                            out.push(chunk);
                        }
                    }
                    self.expected = max_seq.wrapping_add(1);
                }
                self.gap_since = None;
                gap_timeout = true;
            }
        }

        (out, gap_timeout)
    }
}

// ── Virtual connection (splitter side) ────────────────────────────────

struct VirtConn {
    to_client_tx: mpsc::UnboundedSender<Bytes>,
    reorder: Mutex<ReorderBuf>,
    send_credit: AtomicI64,
    ack_notify: tokio::sync::Notify,
    closed: AtomicBool,
    created_at: Instant,
    bytes_sent: AtomicU64,
    bytes_recv: AtomicU64,
    frames_sent: AtomicU64,
    frames_recv: AtomicU64,
}

impl VirtConn {
    /// Returns true if a persistent gap just timed out — caller should RST.
    fn on_frame(&self, seq: u64, payload: Bytes) -> bool {
        let plen = payload.len() as u64;
        let (ready, gap_timeout) = self.reorder.lock().unwrap().push(seq, payload);
        for chunk in ready {
            let _ = self.to_client_tx.send(chunk);
        }
        self.bytes_recv.fetch_add(plen, Ordering::Relaxed);
        self.frames_recv.fetch_add(1, Ordering::Relaxed);
        gap_timeout
    }

    fn on_ack(&self, ack_bytes: u64, window: u32) {
        // ponytail: simple byte-count credit; seq-based tracking in v3
        let credit = ack_bytes as i64 + window as i64;
        self.send_credit.store(credit, Ordering::Release);
        self.ack_notify.notify_one();
    }

    fn consume_credit(&self, n: usize) {
        self.send_credit.fetch_sub(n as i64, Ordering::Release);
    }

    fn has_credit(&self) -> bool {
        self.send_credit.load(Ordering::Acquire) > 0
    }
}

type ConnMap = Arc<DashMap<u32, Arc<VirtConn>>>;

// ── Main entry ────────────────────────────────────────────────────────

pub async fn run_splitter(cfg: SplitterConfig) -> Result<()> {
    let conns: ConnMap = Arc::new(DashMap::new());
    let pool = Arc::new(TunnelPool::new());

    // 1. Establish persistent tunnel connections (with reconnect)
    for (i, ep) in cfg.tunnels.iter().enumerate() {
        let ep = ep.clone();
        let pool = pool.clone();
        let conns = conns.clone();
        tokio::spawn(async move {
            let mut retry_count: u32 = 0;
            loop {
                match establish_tunnel(&ep).await {
                    Ok(stream) => {
                        retry_count = 0;
                        info!(tunnel = i, proxy = %ep.proxy, target = %ep.target, port = ep.port, "connected");
                        let (rd, wr) = stream.into_split();
                        let (tx, rx) = mpsc::channel::<Frame>(TUNNEL_CHAN_CAP);
                        let link = Arc::new(TunnelLink {
                            tx,
                            alive: AtomicBool::new(true),
                            strikes: AtomicU32::new(0),
                            skip_rounds: AtomicU32::new(0),
                            bytes_sent: AtomicU64::new(0),
                            bytes_recv: AtomicU64::new(0),
                            frames_sent: AtomicU64::new(0),
                            frames_recv: AtomicU64::new(0),
                        });
                        pool.add(link.clone());

                        let wr_task = tokio::spawn(drain_frames(rx, wr, link.clone()));

                        if let Err(e) = tunnel_read_loop(rd, i, &conns, &pool, &link).await {
                            warn!(tunnel = i, error = %e, "read loop ended");
                        }
                        link.alive.store(false, Ordering::Release);
                        wr_task.abort();
                        // Log disconnect summary
                        info!(tunnel = i,
                            bytes_sent = link.bytes_sent.load(Ordering::Relaxed),
                            bytes_recv = link.bytes_recv.load(Ordering::Relaxed),
                            frames_sent = link.frames_sent.load(Ordering::Relaxed),
                            frames_recv = link.frames_recv.load(Ordering::Relaxed),
                            "disconnected");
                    }
                    Err(e) => {
                        retry_count += 1;
                        error!(tunnel = i, retry = retry_count, error = %e, "connect failed, retrying");
                    }
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
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(60)).await;
            let links = hb_pool.links.lock().unwrap();
            let total = links.len();
            let alive = links.iter().filter(|l| l.alive.load(Ordering::Acquire)).count();
            let degraded = links.iter().filter(|l| l.skip_rounds.load(Ordering::Acquire) > 0).count();
            drop(links);
            // Sweep dead links that accumulated from tunnel reconnects
            hb_pool.compact();
            let uptime = start_time.elapsed().as_secs();
            info!(
                uptime,
                alive,
                total,
                degraded,
                active_conns = hb_conns.len(),
                udp_sent = hb_udp_sent.swap(0, Ordering::Relaxed),
                udp_recv = hb_udp_recv.swap(0, Ordering::Relaxed),
                "heartbeat"
            );
        }
    });

    // 2. Accept SOCKS5 clients
    let listener = TcpListener::bind(cfg.listen_addr).await?;
    let next_conn_id = AtomicU64::new(1); // ponytail: u64 atomic, truncate to u32 for ConnID

    loop {
        let (stream, peer) = listener.accept().await?;
        let _ = stream.set_nodelay(true);
        let conn_id = next_conn_id.fetch_add(1, Ordering::Relaxed) as u32;
        let pool = pool.clone();
        let conns = conns.clone();
        let us = udp_sent.clone();
        let ur = udp_recv.clone();

        tokio::spawn(async move {
            if let Err(e) = handle_client(conn_id, stream, peer, &pool, &conns, cfg.chunk_size, us, ur).await {
                warn!(conn_id, peer = %peer, error = %e, "client handler failed");
            }
        });
    }
}

// ── Tunnel management ─────────────────────────────────────────────────

async fn establish_tunnel(ep: &TunnelEndpoint) -> Result<TcpStream> {
    let stream = socks5::socks5_client_connect(ep.proxy, &ep.target, ep.port).await?;
    let _ = stream.set_nodelay(true);
    Ok(stream)
}

async fn drain_frames(
    mut rx: mpsc::Receiver<Frame>,
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
    tunnel_idx: usize,
    conns: &ConnMap,
    pool: &TunnelPool,
    link: &TunnelLink,
) -> Result<()> {
    let mut decoder = FrameDecoder::new();
    loop {
        // Adaptive timeout: short when connections are active (need to
        // detect dead tunnels), long when idle (avoid reconnect cycling).
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
        handle_inbound_frame(frame, tunnel_idx, conns, pool);
        link.bytes_recv.fetch_add(plen, Ordering::Relaxed);
        link.frames_recv.fetch_add(1, Ordering::Relaxed);
    }
}

// ── Inbound frame dispatch ────────────────────────────────────────────

fn handle_inbound_frame(frame: Frame, _tunnel_idx: usize, conns: &ConnMap, pool: &TunnelPool) {
    if frame.flags & FLAG_SYN != 0 && frame.flags & FLAG_ACK != 0 {
        // SYN+ACK: handshake complete — handled by the pending oneshot in handle_client
        // Frame just arrives; the oneshot is triggered elsewhere after the initial SYN is sent.
        // ponytail: SYN+ACK frames are no-ops here; handle_client manages the handshake directly.
        return;
    }

    if frame.flags & FLAG_DATA != 0 {
        if let Some(conn) = conns.get(&frame.conn_id) {
            if conn.on_frame(frame.seq, frame.payload) {
                // Reorder gap timeout — tear down the connection
                conn.closed.store(true, Ordering::Release);
                conn.ack_notify.notify_one();
                conns.remove(&frame.conn_id);
                pool.send(Frame::rst(frame.conn_id));
            }
        }
        // Unknown conn_id: stale/dangling. Send RST so the
        // reassembler cleans up and stops flooding the tunnel.
        pool.send(Frame::rst(frame.conn_id));
        return;
    }

    if frame.flags & FLAG_FIN != 0 {
        if let Some((_, conn)) = conns.remove(&frame.conn_id) {
            conn.closed.store(true, Ordering::Release);
            conn.ack_notify.notify_one();
            drop(conn);
        }
        return;
    }

    if frame.flags & FLAG_RST != 0 {
        if let Some((_, conn)) = conns.remove(&frame.conn_id) {
            conn.closed.store(true, Ordering::Release);
            conn.ack_notify.notify_one();
            drop(conn);
        }
        return;
    }

    if frame.flags & FLAG_ACK != 0 {
        if let Ok(ack) = AckInfo::decode(&frame.payload) {
            if let Some(conn) = conns.get(&frame.conn_id) {
                conn.on_ack(ack.ack_seq, ack.window);
            }
        }
    }
}

// ── Client handler ────────────────────────────────────────────────────

async fn handle_client(
    conn_id: u32,
    stream: TcpStream,
    peer: SocketAddr,
    pool: &TunnelPool,
    conns: &ConnMap,
    chunk_size: usize,
    udp_sent: Arc<AtomicU64>,
    udp_recv: Arc<AtomicU64>,
) -> Result<()> {
    let accepted = socks5::socks5_server_accept(stream).await?;
    match accepted {
        socks5::Socks5Result::Connect(accepted) => {
            handle_tcp_client(conn_id, accepted, peer, pool, conns, chunk_size).await
        }
        socks5::Socks5Result::UdpAssociate { stream: _keepalive, relay } => {
            handle_udp_client(pool, conns, relay, udp_sent.clone(), udp_recv.clone()).await
        }
    }
}

async fn handle_tcp_client(
    conn_id: u32,
    accepted: socks5::Socks5Accept,
    peer: SocketAddr,
    pool: &TunnelPool,
    conns: &ConnMap,
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
        send_credit: AtomicI64::new(INITIAL_CREDIT),
        ack_notify: tokio::sync::Notify::new(),
        closed: AtomicBool::new(false),
        created_at: Instant::now(),
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

    let mut buf = vec![0u8; chunk_size];
    let mut seq: u64 = 1;
    let mut credit_exhausted = false;
    loop {
        while !vconn.has_credit() {
            if vconn.closed.load(Ordering::Acquire) {
                break;
            }
            if !credit_exhausted {
                info!(conn_id, "flow: credit exhausted, pausing");
                credit_exhausted = true;
            }
            {
                let notified = vconn.ack_notify.notified();
                // Double-check: ACK may have arrived between has_credit()
                // and notified() creation — Notify does not capture past events.
                if vconn.has_credit() {
                    break; // credit restored, no need to wait
                }
                match tokio::time::timeout(
                    Duration::from_secs(CREDIT_TIMEOUT_SECS),
                    notified,
                )
                .await
                {
                    Ok(()) => {}
                    Err(_) => {
                        warn!(conn_id, timeout = CREDIT_TIMEOUT_SECS, "credit timeout, no ACK — closing");
                        vconn.closed.store(true, Ordering::Release);
                        break;
                    }
                }
            }
        }
        if vconn.closed.load(Ordering::Acquire) {
            break;
        }
        if credit_exhausted && vconn.has_credit() {
            info!(conn_id, credit = vconn.send_credit.load(Ordering::Acquire), "flow: credit restored, resuming");
            credit_exhausted = false;
        }

        match client_reader.read(&mut buf).await {
            Ok(0) => break,
            Ok(n) => {
                vconn.consume_credit(n);
                let frame = Frame::data(conn_id, seq, Bytes::copy_from_slice(&buf[..n]));
                if !pool.send(frame) {
                    warn!(conn_id, "no live tunnels, aborting");
                    break;
                }
                vconn.bytes_sent.fetch_add(n as u64, Ordering::Relaxed);
                vconn.frames_sent.fetch_add(1, Ordering::Relaxed);
                seq += 1;
            }
            Err(e) => {
                warn!(conn_id, error = %e, "client read error");
                break;
            }
        }
    }

    pool.send(Frame::fin(conn_id, seq));
    conns.remove(&conn_id);
    // Snapshot stats before dropping vconn (last Arc → drops to_client_tx → writer_task exits)
    let duration_ms = vconn.created_at.elapsed().as_millis() as u64;
    let bs = vconn.bytes_sent.load(Ordering::Relaxed);
    let br = vconn.bytes_recv.load(Ordering::Relaxed);
    let fs = vconn.frames_sent.load(Ordering::Relaxed);
    let fr = vconn.frames_recv.load(Ordering::Relaxed);
    drop(vconn);
    let _ = writer_task.await;
    info!(conn_id,
        bytes_sent = bs,
        bytes_recv = br,
        frames_sent = fs,
        frames_recv = fr,
        duration_ms,
        "closed");
    Ok(())
}

/// UDP relay: read SOCKS5-wrapped datagrams → DATA frames → pool.
/// Responses from reassembler arrive via handle_inbound_frame → conns[0] → relay socket.
async fn handle_udp_client(
    pool: &TunnelPool,
    conns: &ConnMap,
    relay: UdpSocket,
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
        send_credit: AtomicI64::new(i64::MAX),
        ack_notify: tokio::sync::Notify::new(),
        closed: AtomicBool::new(false),
        created_at: Instant::now(),
        bytes_sent: AtomicU64::new(0),
        bytes_recv: AtomicU64::new(0),
        frames_sent: AtomicU64::new(0),
        frames_recv: AtomicU64::new(0),
    });
    conns.insert(UDP_CONN_ID, vconn);

    let relay2 = relay.clone();
    let recv_ctr = udp_recv.clone();
    tokio::spawn(async move {
        while let Some(dgram) = to_udp_rx.recv().await {
            recv_ctr.fetch_add(1, Ordering::Relaxed);
            if relay2.send(&dgram).await.is_err() {
                break;
            }
        }
    });

    let mut buf = vec![0u8; 65535];
    let mut seq: u64 = 1;
    loop {
        let (n, _client) = relay.recv_from(&mut buf).await?;
        udp_sent.fetch_add(1, Ordering::Relaxed);
        let frame = Frame::data(UDP_CONN_ID, seq, Bytes::copy_from_slice(&buf[..n]));
        if !pool.send(frame) {
            warn!("UDP relay: no live tunnels");
            break;
        }
        seq += 1;
    }
    conns.remove(&UDP_CONN_ID);
    Ok(())
}
