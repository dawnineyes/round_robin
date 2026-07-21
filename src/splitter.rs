use crate::frame::{self, Frame, FrameDecoder, SynTarget, FLAG_ACK, FLAG_DATA, FLAG_FIN, FLAG_RST, FLAG_SYN};
use crate::socks5;
use anyhow::{bail, Result};
use bytes::Bytes;
use dashmap::DashMap;
use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream, UdpSocket};
use tokio::sync::mpsc;
use tracing::{error, info, warn};

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

    /// Round-robin send. Unbounded sender — only fails if link is dead.
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

}

// ── Reorder buffer ────────────────────────────────────────────────────

/// Max out-of-order entries before we drop new arrivals.
/// At 65535 byte chunks, 512 entries = 32 MB reorder window.
/// Overflow with a warning — the gap will eventually fill when
/// the slow tunnel catches up.
const MAX_PENDING_ENTRIES: usize = 512;

struct ReorderBuf {
    expected: u64,
    pending: BTreeMap<u64, Bytes>,
}

impl ReorderBuf {
    fn new() -> Self {
        Self { expected: 1, pending: BTreeMap::new() }
    }

    /// Returns in-order chunks. Out-of-order frames are buffered until the gap fills.
    /// TUIC TCP guarantees delivery — we just wait.
    fn push(&mut self, seq: u64, payload: Bytes) -> Vec<Bytes> {
        let mut out = Vec::new();

        if seq < self.expected {
            return out; // duplicate
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

// ── Virtual connection (splitter side) ────────────────────────────────

struct VirtConn {
    to_client_tx: mpsc::UnboundedSender<Bytes>,
    reorder: Mutex<ReorderBuf>,
    /// Woken on FIN/RST so the client read loop can exit.
    notify: tokio::sync::Notify,
    closed: AtomicBool,
    created_at: Instant,
    bytes_sent: AtomicU64,
    bytes_recv: AtomicU64,
    frames_sent: AtomicU64,
    frames_recv: AtomicU64,
}

impl VirtConn {
    fn on_frame(&self, seq: u64, payload: Bytes) {
        let plen = payload.len() as u64;
        let ready = self.reorder.lock().unwrap().push(seq, payload);
        for chunk in ready {
            let _ = self.to_client_tx.send(chunk);
        }
        self.bytes_recv.fetch_add(plen, Ordering::Relaxed);
        self.frames_recv.fetch_add(1, Ordering::Relaxed);
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
            drop(links);
            // Sweep dead links that accumulated from tunnel reconnects
            hb_pool.compact();
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
    let mut next_conn_id: u64 = 1;

    loop {
        let (stream, peer) = listener.accept().await?;
        let _ = stream.set_nodelay(true);
        let conn_id = next_conn_id as u32;
        next_conn_id += 1;
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
    let stream = tokio::time::timeout(
        Duration::from_secs(10),
        socks5::socks5_client_connect(ep.proxy, &ep.target, ep.port),
    )
    .await??;
    let _ = stream.set_nodelay(true);
    Ok(stream)
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
    tunnel_idx: usize,
    conns: &ConnMap,
    pool: &TunnelPool,
    link: &TunnelLink,
) -> Result<()> {
    let mut decoder = FrameDecoder::new();
    loop {
        let frame = match decoder.try_next(&mut rd).await? {
            Some(f) => f,
            None => return Ok(()),
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
            conn.on_frame(frame.seq, frame.payload);
        } else {
            // Unknown conn_id: stale/dangling. Send RST so the
            // reassembler cleans up and stops flooding the tunnel.
            pool.send(Frame::rst(frame.conn_id));
        }
        return;
    }

    if frame.flags & FLAG_FIN != 0 {
        if let Some((_, conn)) = conns.remove(&frame.conn_id) {
            conn.closed.store(true, Ordering::Release);
            conn.notify.notify_one();
            drop(conn);
        }
        return;
    }

    if frame.flags & FLAG_RST != 0 {
        if let Some((_, conn)) = conns.remove(&frame.conn_id) {
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
        socks5::Socks5Result::UdpAssociate { stream: keepalive, relay } => {
            handle_udp_client(pool, conns, relay, keepalive, udp_sent.clone(), udp_recv.clone()).await
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
        notify: tokio::sync::Notify::new(),
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
    loop {
        // Race client read against close notification.
        // Without this, a FIN from the reassembler would leave us
        // stuck in read() while the browser holds the connection open
        // (HTTP keep-alive), and the hung connection keeps tunnels in
        // short-timeout mode, causing tunnel cycling.
        tokio::select! {
            result = client_reader.read(&mut buf) => {
                match result {
                    Ok(0) => break,
                    Ok(n) => {
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
            _ = vconn.notify.notified() => {
                if vconn.closed.load(Ordering::Acquire) {
                    break;
                }
                // FIN/RST notification — loop back to check closed flag
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
        created_at: Instant::now(),
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
            let addr = ca.lock().unwrap().clone();
            if let Some(addr) = addr {
                if relay2.send_to(&dgram, addr).await.is_err() {
                    break;
                }
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
                let (n, client) = result?;
                *client_addr.lock().unwrap() = Some(client);
                udp_sent.fetch_add(1, Ordering::Relaxed);
                let frame = Frame::data(UDP_CONN_ID, seq, Bytes::copy_from_slice(&buf[..n]));
                if !pool.send(frame) {
                    warn!("UDP relay: no live tunnels");
                    break;
                }
                seq += 1;
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
