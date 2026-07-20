use crate::frame::{AckInfo, Frame, FrameDecoder, SynTarget, FLAG_ACK, FLAG_DATA, FLAG_FIN, FLAG_RST, FLAG_SYN};
use crate::socks5;
use anyhow::Result;
use bytes::Bytes;
use dashmap::DashMap;
use std::collections::BTreeMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tracing::{error, info, warn};

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
                link.alive.store(false, Ordering::Release);
            }
        }
        false
    }

    fn send_via(&self, frame: Frame, src_link: usize) -> bool {
        // Prefer the source link; fall back to round-robin
        let links = self.links.lock().unwrap();
        if src_link < links.len() {
            let link = &links[src_link];
            if link.alive.load(Ordering::Acquire) && link.tx.send(frame.clone()).is_ok() {
                return true;
            }
            link.alive.store(false, Ordering::Release);
        }
        drop(links);
        self.send(frame)
    }
}

// ── Reorder buffer ────────────────────────────────────────────────────

struct ReorderBuf {
    expected: u64,
    pending: BTreeMap<u64, Bytes>,
}

impl ReorderBuf {
    fn new() -> Self {
        Self { expected: 1, pending: BTreeMap::new() }
    }

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
        } else {
            self.pending.insert(seq, payload);
            // ponytail: BTreeMap unbounded; add MAX_REORDER_WINDOW in v2
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
}

type ConnMap = Arc<DashMap<u32, Arc<VirtConnDe>>>;
/// Frames that arrived before the SYN handler finished creating the VirtConnDe.
type PendingMap = Arc<DashMap<u32, Vec<Frame>>>;

// ── Main entry ────────────────────────────────────────────────────────

pub async fn run_reassembler(cfg: ReassemblerConfig) -> Result<()> {
    let conns: ConnMap = Arc::new(DashMap::new());
    let pending: PendingMap = Arc::new(DashMap::new());
    let pool = Arc::new(TunnelPool::new());

    // Spawn a listener for each port
    for &port in &cfg.listen_ports {
        let conns = conns.clone();
        let pending = pending.clone();
        let pool = pool.clone();
        let local_target = cfg.local_target;
        let listen_ip = cfg.listen_ip;
        tokio::spawn(async move {
            if let Err(e) = run_tunnel_listener(listen_ip, port, local_target, conns, pending, pool, cfg.chunk_size).await {
                error!("listener on port {port} died: {e}");
            }
        });
    }

    info!("reassembler: listening on ports {:?}, egress → {}", cfg.listen_ports, cfg.local_target);

    // Keep alive
    tokio::signal::ctrl_c().await?;
    info!("reassembler: shutting down");
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
) -> Result<()> {
    let listener = TcpListener::bind((listen_ip, port)).await?;
    info!("reassembler: tunnel listener on {listen_ip}:{port}");

    loop {
        let (stream, peer) = listener.accept().await?;
        let _ = stream.set_nodelay(true);

        // SOCKS5 handshake: sing-box SOCKS5 outbound CONNECTs here.
        // Accept any no-auth client, ignore the CONNECT target.
        let stream = match socks5::socks5_accept_tunnel(stream).await {
            Ok(s) => s,
            Err(e) => {
                warn!("tunnel SOCKS5 handshake failed from {peer}: {e}");
                continue;
            }
        };

        info!("tunnel link from {peer} on port {port} (pool size {})", pool.links.lock().unwrap().len() + 1);

        let (rd, wr) = stream.into_split();
        let (tx, rx) = mpsc::unbounded_channel();
        let link = Arc::new(TunnelLink { tx, alive: AtomicBool::new(true) });
        pool.add(link.clone());

        // Writer task
        tokio::spawn(drain_frames(rx, wr));

        // Reader task (one per link)
        let conns = conns.clone();
        let pending = pending.clone();
        let pool = pool.clone();
        tokio::spawn(async move {
            if let Err(e) = tunnel_read_loop(rd, conns, pending, pool, local_target, port as usize, chunk_size).await {
                warn!("tunnel link from {peer}: {e}");
            }
            link.alive.store(false, Ordering::Release);
        });
    }
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
    conns: ConnMap,
    pending: PendingMap,
    pool: Arc<TunnelPool>,
    local_target: SocketAddr,
    src_port: usize,
    chunk_size: usize,
) -> Result<()> {
    let mut decoder = FrameDecoder::new();
    loop {
        let frame = match decoder.try_next(&mut rd).await? {
            Some(f) => f,
            None => return Ok(()),
        };
        handle_frame(frame, &conns, &pending, &pool, local_target, src_port, chunk_size).await?;
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
) -> Result<()> {
    let cid = frame.conn_id;

    // SYN: new virtual connection
    if frame.flags & FLAG_SYN != 0 {
        // Reserve a pending slot so DATA/FIN arriving during SOCKS5 connect
        // are queued instead of dropped
        pending.insert(cid, Vec::new());

        // Parse target from SYN payload
        let syn_target = SynTarget::decode(&frame.payload)?;
        info!("conn {cid}: SYN → {} (proto={})", syn_target.address, syn_target.proto);

        // Connect to local_target via SOCKS5
        let egress_stream = match socks5::socks5_client_connect(
            local_target,
            &syn_target.address,
            syn_target.port,
        )
        .await
        {
            Ok(s) => s,
            Err(e) => {
                warn!("conn {cid}: failed to connect egress to {local_target} for {}: {e}", syn_target.address);
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
        });

        // Spawn egress writer: ordered data → egress connection
        tokio::spawn(write_to_egress(write_rx, egress_wr));

        // Spawn egress reader: egress response → frames → pool
        let conns_clone = conns.clone();
        let pool_clone = Arc::clone(pool);
        tokio::spawn(read_from_egress(cid, egress_rd, conns_clone, pool_clone, chunk_size));

        conns.insert(cid, vconn.clone());

        // Drain any frames that arrived during SOCKS5 connect
        if let Some((_, queued)) = pending.remove(&cid) {
            for f in queued {
                if f.flags & FLAG_DATA != 0 {
                    let ready = vconn.reorder.lock().unwrap().push(f.seq, f.payload);
                    for chunk in ready {
                        if !vconn.egress.write(&chunk) {
                            warn!("conn {cid}: egress write failed (drain)");
                            break;
                        }
                    }
                } else if f.flags & FLAG_FIN != 0 {
                    info!("conn {cid}: FIN during SYN, cleaning up");
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
            let ready = vconn.reorder.lock().unwrap().push(frame.seq, frame.payload);
            for chunk in ready {
                if !vconn.egress.write(&chunk) {
                    warn!("conn {cid}: egress write failed");
                    break;
                }
            }
        } else if let Some(mut entry) = pending.get_mut(&cid) {
            // SYN is still in progress, queue this frame
            entry.push(frame);
        }
        // else: conn already cleaned up, drop frame
        return Ok(());
    }

    // FIN
    if frame.flags & FLAG_FIN != 0 {
        if let Some((_, vconn)) = conns.remove(&cid) {
            info!("conn {cid}: FIN received, cleaning up");
            drop(vconn); // drops write_tx → egress write task exits → egress wr shutdown
        }
        return Ok(());
    }

    // RST
    if frame.flags & FLAG_RST != 0 {
        if let Some((_, vconn)) = conns.remove(&cid) {
            info!("conn {cid}: RST received, force close");
            drop(vconn);
        }
        return Ok(());
    }

    // ACK (splitter → reassembler)
    if frame.flags & FLAG_ACK != 0 {
        if let Ok(ack) = AckInfo::decode(&frame.payload) {
            // ponytail: ack tracking for egress send window (v2)
            let _ = ack;
        }
    }

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
            Ok(0) => break, // egress EOF
            Ok(n) => {
                let frame = Frame::data(conn_id, seq, Bytes::copy_from_slice(&buf[..n]));
                if !pool.send(frame) {
                    warn!("conn {conn_id}: no live tunnels for egress response");
                    break;
                }
                seq += 1;
            }
            Err(e) => {
                warn!("conn {conn_id}: egress read error: {e}");
                break;
            }
        }
    }
    // Send FIN + cleanup
    pool.send(Frame::fin(conn_id, seq));
    conns.remove(&conn_id);
    info!("conn {conn_id}: egress closed");
}
