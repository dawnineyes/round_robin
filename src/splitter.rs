use crate::frame::{self, AckInfo, Frame, FrameDecoder, SynTarget, FLAG_ACK, FLAG_DATA, FLAG_FIN, FLAG_RST, FLAG_SYN};
use crate::socks5;
use anyhow::{bail, Result};
use bytes::Bytes;
use dashmap::DashMap;
use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
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

// ── Tunnel pool ───────────────────────────────────────────────────────

struct TunnelLink {
    tx: mpsc::UnboundedSender<Frame>,
    alive: AtomicBool,
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

    /// Round-robin send. Returns false if no live links.
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
            if link.alive.load(Ordering::Acquire) {
                if link.tx.send(frame.clone()).is_ok() {
                    return true;
                }
                // Send failed → mark dead, try next
                link.alive.store(false, Ordering::Release);
            }
        }
        false
    }

}

// ── Reorder buffer ────────────────────────────────────────────────────

struct ReorderBuf {
    expected: u64,
    pending: BTreeMap<u64, Bytes>,
}

impl ReorderBuf {
    fn new() -> Self {
        Self { expected: 1, pending: BTreeMap::new() } // DATA seq starts at 1
    }

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
        } else {
            self.pending.insert(seq, payload);
        }
        out
    }
}

// ── Virtual connection (splitter side) ────────────────────────────────

struct VirtConn {
    to_client_tx: mpsc::UnboundedSender<Bytes>,
    reorder: Mutex<ReorderBuf>,
}

impl VirtConn {
    fn on_frame(&self, seq: u64, payload: Bytes) {
        let ready = self.reorder.lock().unwrap().push(seq, payload);
        for chunk in ready {
            let _ = self.to_client_tx.send(chunk);
        }
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
            loop {
                match establish_tunnel(&ep).await {
                    Ok(stream) => {
                        info!("tunnel {i}: connected via {} → {}:{}", ep.proxy, ep.target, ep.port);
                        let (rd, wr) = stream.into_split();
                        let (tx, rx) = mpsc::unbounded_channel();
                        let link = Arc::new(TunnelLink { tx, alive: AtomicBool::new(true) });
                        pool.add(link.clone());

                        // Writer task: serialize frames onto the tunnel
                        let wr_task = tokio::spawn(drain_frames(rx, wr));

                        // Reader task: demux inbound frames
                        if let Err(e) = tunnel_read_loop(rd, i, &conns, &pool).await {
                            warn!("tunnel {i}: read loop ended: {e}");
                        }
                        link.alive.store(false, Ordering::Release);
                        wr_task.abort();
                    }
                    Err(e) => {
                        error!("tunnel {i}: failed to establish: {e}, retrying in 3s");
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
    info!("splitter: {} tunnels ready, listening on {}", pool.link_count(), cfg.listen_addr);

    // 2. Accept SOCKS5 clients
    let listener = TcpListener::bind(cfg.listen_addr).await?;
    let next_conn_id = AtomicU64::new(1); // ponytail: u64 atomic, truncate to u32 for ConnID

    loop {
        let (stream, peer) = listener.accept().await?;
        let _ = stream.set_nodelay(true);
        let conn_id = next_conn_id.fetch_add(1, Ordering::Relaxed) as u32;
        let pool = pool.clone();
        let conns = conns.clone();

        tokio::spawn(async move {
            if let Err(e) = handle_client(conn_id, stream, peer, &pool, &conns, cfg.chunk_size).await {
                warn!("conn {conn_id} from {peer}: {e}");
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

async fn drain_frames(mut rx: mpsc::UnboundedReceiver<Frame>, mut wr: tokio::net::tcp::OwnedWriteHalf) {
    while let Some(frame) = rx.recv().await {
        if wr.write_all(&frame.encode()).await.is_err() {
            break;
        }
    }
    let _ = wr.shutdown().await;
}

async fn tunnel_read_loop(
    mut rd: tokio::net::tcp::OwnedReadHalf,
    tunnel_idx: usize,
    conns: &ConnMap,
    pool: &TunnelPool,
) -> Result<()> {
    let mut decoder = FrameDecoder::new();
    loop {
        let frame = match decoder.try_next(&mut rd).await? {
            Some(f) => f,
            None => return Ok(()), // clean EOF
        };
        handle_inbound_frame(frame, tunnel_idx, conns, pool);
    }
}

// ── Inbound frame dispatch ────────────────────────────────────────────

fn handle_inbound_frame(frame: Frame, _tunnel_idx: usize, conns: &ConnMap, _pool: &TunnelPool) {
    if frame.flags & FLAG_SYN != 0 && frame.flags & FLAG_ACK != 0 {
        // SYN+ACK: handshake complete — handled by the pending oneshot in handle_client
        // Frame just arrives; the oneshot is triggered elsewhere after the initial SYN is sent.
        // ponytail: SYN+ACK frames are no-ops here; handle_client manages the handshake directly.
        return;
    }

    if frame.flags & FLAG_DATA != 0 {
        if let Some(conn) = conns.get(&frame.conn_id) {
            conn.on_frame(frame.seq, frame.payload);
        }
        // Unknown conn_id: stale/dangling, drop
        return;
    }

    if frame.flags & FLAG_FIN != 0 {
        if let Some((_, conn)) = conns.remove(&frame.conn_id) {
            drop(conn); // closes to_client_tx → client write task ends
        }
        return;
    }

    if frame.flags & FLAG_RST != 0 {
        if let Some((_, conn)) = conns.remove(&frame.conn_id) {
            drop(conn);
        }
        return;
    }

    if frame.flags & FLAG_ACK != 0 {
        if let Ok(ack) = AckInfo::decode(&frame.payload) {
            // ponytail: update ack tracking for slow-path detection (v2)
            let _ = (ack, frame.conn_id);
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
) -> Result<()> {
    // SOCKS5 handshake
    let accepted = socks5::socks5_server_accept(stream).await?;
    info!("conn {conn_id}: {peer} → {}:{}", accepted.target.address, accepted.target.port);

    // Build SYN payload — address is host only, port is separate
    let syn_target = SynTarget {
        proto: frame::PROTO_TCP,
        address: accepted.target.address.clone(),
        port: accepted.target.port,
    };
    let syn_frame = Frame::syn(conn_id, syn_target.encode());

    // Send SYN via round-robin
    if !pool.send(syn_frame) {
        bail!("no live tunnels to send SYN");
    }

    // Wait for SYN+ACK. ponytail: poll the conn map; SYN+ACK handling in
    // handle_inbound_frame will insert the VirtConn when created. For now,
    // we create the VirtConn immediately and the SYN+ACK is implicit:
    // the first DATA frame from the reassembler confirms the connection.
    let (to_client_tx, to_client_rx) = mpsc::unbounded_channel();
    let vconn = Arc::new(VirtConn {
        to_client_tx,
        reorder: Mutex::new(ReorderBuf::new()),
    });
    conns.insert(conn_id, vconn);

    let (mut client_reader, mut client_writer) = accepted.stream.into_split();

    // Spawn writer task: responses from tunnels → client
    let writer_task = tokio::spawn(async move {
        let mut rx = to_client_rx;
        while let Some(chunk) = rx.recv().await {
            if client_writer.write_all(&chunk).await.is_err() {
                break;
            }
        }
        let _ = client_writer.shutdown().await;
    });

    // Read loop: client → chunks → DATA frames → pool
    let mut buf = vec![0u8; chunk_size];
    let mut seq: u64 = 1;
    loop {
        match client_reader.read(&mut buf).await {
            Ok(0) => break, // client EOF
            Ok(n) => {
                let frame = Frame::data(conn_id, seq, Bytes::copy_from_slice(&buf[..n]));
                if !pool.send(frame) {
                    warn!("conn {conn_id}: no live tunnels, aborting");
                    break;
                }
                seq += 1;
            }
            Err(e) => {
                warn!("conn {conn_id}: client read error: {e}");
                break;
            }
        }
    }

    // Send FIN, cleanup
    pool.send(Frame::fin(conn_id, seq));
    // Remove from map → drops VirtConn → drops to_client_tx → writer task exits
    conns.remove(&conn_id);
    // Wait for writer to drain its last chunk, then drop
    let _ = writer_task.await;
    info!("conn {conn_id}: closed");
    Ok(())
}
