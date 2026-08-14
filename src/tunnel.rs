use crate::frame::{Frame, MAX_PAYLOAD};
use bytes::BytesMut;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tokio::io::AsyncWriteExt;
use tokio::sync::{Notify, mpsc};

/// Per-tunnel send queue capacity. At 65535 bytes max frame, 128
/// entries = ~8 MB max backlog per tunnel before backpressure kicks in.
pub const TUNNEL_CHANNEL_CAP: usize = 128;

// ── Tunnel link ────────────────────────────────────────────────────────

pub struct TunnelLink {
    pub tx: mpsc::Sender<Frame>,
    pub alive: AtomicBool,
    pub bytes_sent: AtomicU64,
    pub bytes_recv: AtomicU64,
    pub frames_sent: AtomicU64,
    pub frames_recv: AtomicU64,
    /// Fires when the link dies: the drain task stops writing, drains the
    /// queue and reports frames that were never written (D1 fast recovery).
    pub stop: Arc<Notify>,
    /// Frames that were queued but never written when the link died.
    pub lost_frames: Mutex<Vec<Frame>>,
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

    /// Count only alive links (BUG-17: link caps must ignore dead links
    /// that haven't been compacted by the heartbeat yet).
    pub fn alive_count(&self) -> usize {
        let links = self.links.lock().unwrap();
        links
            .iter()
            .filter(|l| l.alive.load(Ordering::Acquire))
            .count()
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

    /// Sum of queued-but-unwritten frames across all links — a backlog
    /// proxy for monitoring (heartbeat).
    pub fn queue_depth(&self) -> usize {
        let links = self.links.lock().unwrap();
        links
            .iter()
            .map(|l| TUNNEL_CHANNEL_CAP - l.tx.capacity())
            .sum()
    }

    /// Round-robin send with backpressure.  Uses `try_send` so that a
    /// full channel skips to the next link instead of blocking.
    /// Returns false only when no link can accept the frame.
    /// Control frames (SYN/FIN/RST) use this path; DATA uses `send_async`.
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
            match link.tx.try_send(frame.clone()) {
                Ok(()) => return true,
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    link.alive.store(false, Ordering::Release);
                }
                Err(mpsc::error::TrySendError::Full(_)) => {
                    // Link is alive but its queue is full — try next link.
                    continue;
                }
            }
        }
        false
    }

    /// Real backpressure send for DATA frames: waits until some live
    /// tunnel can take the frame (callers wrap this in a timeout).
    /// Picks the link with the most spare queue capacity so a slow
    /// tunnel doesn't stall every connection through it.
    pub async fn send_async(&self, frame: Frame) -> bool {
        loop {
            let best = {
                let links = self.links.lock().unwrap();
                let mut best: Option<Arc<TunnelLink>> = None;
                let mut best_cap = 0usize;
                for link in links.iter() {
                    if !link.alive.load(Ordering::Acquire) {
                        continue;
                    }
                    let cap = link.tx.capacity();
                    if best.is_none() || cap > best_cap {
                        best = Some(link.clone());
                        best_cap = cap;
                    }
                }
                best
            };
            match best {
                Some(link) => {
                    if link.tx.send(frame.clone()).await.is_ok() {
                        return true;
                    }
                    // Channel closed (tunnel died) — mark it dead so the
                    // next iteration skips it instead of spinning.
                    link.alive.store(false, Ordering::Release);
                }
                None => return false,
            }
        }
    }
}

// ── Drain frames ───────────────────────────────────────────────────────

impl Default for TunnelPool {
    fn default() -> Self {
        Self::new()
    }
}

/// A tunnel write stalled for this long is a dead link — drop it so the
/// channel closes and senders fail over to other tunnels.
const TUNNEL_WRITE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60);

pub async fn drain_frames(
    mut rx: mpsc::Receiver<Frame>,
    mut wr: tokio::net::tcp::OwnedWriteHalf,
    link: Arc<TunnelLink>,
) {
    // O2: one reusable encode buffer per tunnel (no per-frame alloc).
    let mut enc = BytesMut::with_capacity(15 + MAX_PAYLOAD);
    let mut dead = false;
    while !dead {
        tokio::select! {
            _ = link.stop.notified() => dead = true,
            frame = rx.recv() => {
                let Some(frame) = frame else { dead = true; continue; };
                let n = frame.payload.len() as u64;
                enc.clear();
                // BUG-12: encode can fail (oversized payload) instead of
                // truncating and corrupting the byte stream.
                if let Err(e) = frame.encode_into(&mut enc) {
                    tracing::warn!(error = %e, conn_id = frame.conn_id, "encode failed, dropping frame");
                    dead = true;
                    continue;
                }
                tokio::select! {
                    _ = link.stop.notified() => dead = true,
                    r = tokio::time::timeout(TUNNEL_WRITE_TIMEOUT, wr.write_all(&enc)) => {
                        match r {
                            Ok(Ok(())) => {
                                link.bytes_sent.fetch_add(n, Ordering::Relaxed);
                                link.frames_sent.fetch_add(1, Ordering::Relaxed);
                            }
                            _ => dead = true, // write error or stall timeout
                        }
                    }
                }
            }
        }
    }
    // D1: frames still queued were never written — report them so the
    // owner can reset the affected connections / resend control frames.
    while let Ok(frame) = rx.try_recv() {
        link.lost_frames.lock().unwrap().push(frame);
    }
    let _ = wr.shutdown().await;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn send_async_without_links_fails() {
        let pool = TunnelPool::new();
        assert!(!pool.send_async(Frame::rst(1)).await);
    }

    #[tokio::test]
    async fn alive_count_ignores_dead_links() {
        let pool = TunnelPool::new();
        let (tx1, rx1) = mpsc::channel::<Frame>(4);
        let (tx2, rx2) = mpsc::channel::<Frame>(4);
        let mk = |tx| {
            Arc::new(TunnelLink {
                tx,
                alive: AtomicBool::new(true),
                bytes_sent: AtomicU64::new(0),
                bytes_recv: AtomicU64::new(0),
                frames_sent: AtomicU64::new(0),
                frames_recv: AtomicU64::new(0),
                stop: Arc::new(Notify::new()),
                lost_frames: Mutex::new(Vec::new()),
            })
        };
        let live = mk(tx1);
        let dead = mk(tx2);
        dead.alive.store(false, Ordering::Release);
        pool.add(live);
        pool.add(dead);
        drop((rx1, rx2));
        assert_eq!(pool.link_count(), 2);
        assert_eq!(pool.alive_count(), 1); // BUG-17
    }

    #[tokio::test]
    async fn send_async_fails_over_closed_channel() {
        let pool = TunnelPool::new();
        let (tx, rx) = mpsc::channel::<Frame>(4);
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
        pool.add(link);
        drop(rx); // receiver gone → channel closed
        assert!(!pool.send_async(Frame::rst(1)).await);
    }

    #[tokio::test]
    async fn send_async_delivers_to_live_link() {
        let pool = TunnelPool::new();
        let (tx, mut rx) = mpsc::channel::<Frame>(4);
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
        pool.add(link);
        assert!(pool.send_async(Frame::rst(1)).await);
        let got = rx.recv().await.unwrap();
        assert_eq!(got.conn_id, 1);
        assert_eq!(got.flags, crate::frame::FLAG_RST);
    }
}
