use crate::frame::Frame;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc;

// ── Tunnel link ────────────────────────────────────────────────────────

pub struct TunnelLink {
    pub tx: mpsc::UnboundedSender<Frame>,
    pub alive: AtomicBool,
    pub bytes_sent: AtomicU64,
    pub bytes_recv: AtomicU64,
    pub frames_sent: AtomicU64,
    pub frames_recv: AtomicU64,
}

// ── Tunnel pool ────────────────────────────────────────────────────────

pub struct TunnelPool {
    links: Mutex<Vec<Arc<TunnelLink>>>,
    rr: AtomicUsize,
}

impl TunnelPool {
    pub fn new() -> Self {
        Self {
            links: Mutex::new(Vec::new()),
            rr: AtomicUsize::new(0),
        }
    }

    pub fn add(&self, link: Arc<TunnelLink>) {
        self.links.lock().unwrap().push(link);
    }

    pub fn link_count(&self) -> usize {
        self.links.lock().unwrap().len()
    }

    /// Remove dead links from the pool. Called periodically from heartbeat.
    pub fn compact(&self) {
        let mut links = self.links.lock().unwrap();
        let before = links.len();
        links.retain(|l| l.alive.load(Ordering::Acquire));
        if links.len() != before {
            self.rr.store(0, Ordering::Release);
        }
    }

    /// Return (alive_count, total_count) for monitoring / heartbeat.
    pub fn stats(&self) -> (usize, usize) {
        let links = self.links.lock().unwrap();
        let total = links.len();
        let alive = links
            .iter()
            .filter(|l| l.alive.load(Ordering::Acquire))
            .count();
        (alive, total)
    }

    /// Round-robin send. Unbounded sender — only fails if link is dead.
    pub fn send(&self, frame: Frame) -> bool {
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

// ── Drain frames ───────────────────────────────────────────────────────

pub async fn drain_frames(
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
