use crate::frame::{Frame, MAX_PAYLOAD};
use bytes::BytesMut;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tokio::io::AsyncWriteExt;
use tokio::sync::{Notify, mpsc};

/// Per-tunnel send queue capacity. At 65535 bytes max frame, 128
/// entries = ~8 MB max backlog per tunnel before backpressure kicks in.
pub const TUNNEL_CHANNEL_CAP: usize = 128;

/// Time constant of the drain-rate EWMA used for weighted DATA
/// scheduling (Phase 14).  Larger = smoother but slower to react to a
/// tunnel being rate-limited or recovering.
const RATE_EWMA_TAU_SECS: f64 = 2.5;
/// Per-link floor share of total weight: every live link is guaranteed
/// this fraction of picks regardless of its measured rate, so a
/// throttled-but-alive tunnel can never be starved into congestion-
/// window collapse (and recovers automatically when it speeds up).
const FLOOR_SHARE: f64 = 0.05;
/// Anchor grid for the deterministic weighted round-robin cursor.
const WEIGHT_GRID: usize = 1024;

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
    /// EWMA of drain throughput in bytes/sec (f64 bits).  Written only by
    /// the drain task; read by the scheduler for weighted selection.
    /// 0.0 means "never measured" — the scheduler treats it optimistically
    /// (mean of measured links) so a new tunnel is probed at load.
    pub rate_bps: AtomicU64,
}

/// Time-decayed EWMA step: converges toward `inst` with time constant
/// `RATE_EWMA_TAU_SECS`.  Pure function — unit tested.
fn ewma_rate(prev: f64, inst: f64, dt_secs: f64) -> f64 {
    let alpha = 1.0 - (-dt_secs / RATE_EWMA_TAU_SECS).exp();
    prev * (1.0 - alpha) + inst * alpha
}

// ── Tunnel pool ────────────────────────────────────────────────────────

pub struct TunnelPool {
    links: Mutex<Vec<Arc<TunnelLink>>>,
    rr: AtomicUsize,
    /// Fires when a link is added: `send_async` waits on this when no
    /// live link exists instead of failing instantly (B45 — a reconnect
    /// usually lands within seconds; the caller's DATA_SEND_TIMEOUT
    /// bounds the wait).
    added: Notify,
}

impl TunnelPool {
    pub fn new() -> Self {
        Self {
            links: Mutex::new(Vec::new()),
            rr: AtomicUsize::new(0),
            added: Notify::new(),
        }
    }

    pub fn add(&self, link: Arc<TunnelLink>) {
        self.links.lock().unwrap().push(link);
        // Wake every send_async waiter blocked on "no live link".
        // B47: notify_waiters stores no permit when no waiter is
        // registered — send_async creates its Notified future BEFORE the
        // pick, which tokio guarantees will observe this call.
        self.added.notify_waiters();
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

    /// Sum of queued-but-unwritten frames across all live links — a
    /// backlog proxy for monitoring (heartbeat).  O6: dead links are
    /// excluded — their drain task is gone and the closed channel reads
    /// as `capacity() == 0`, which inflated the metric by the full
    /// channel depth per dead link between death and the next compact.
    pub fn queue_depth(&self) -> usize {
        let links = self.links.lock().unwrap();
        links
            .iter()
            .filter(|l| l.alive.load(Ordering::Acquire))
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
    ///
    /// Phase 14: weighted scheduling instead of least-loaded.  Each link's
    /// weight is the EWMA of its drain throughput (`rate_bps`), floored at
    /// a minimum share so no live tunnel starves.  A full queue means the
    /// link is already saturated — skip it and re-pick instead of blocking
    /// (a slow tunnel never stalls a frame another tunnel can take); only
    /// when every live link is full do we block on the best one.
    pub async fn send_async(&self, frame: Frame) -> bool {
        let mut full: Vec<Arc<TunnelLink>> = Vec::new();
        loop {
            // B47: create the Notified future BEFORE the pick.  tokio's
            // `notify_waiters` guarantees a wakeup for every `notified()`
            // future created before the call — but a `notify_waiters`
            // landing between the pick and the future creation would be
            // missed (the B45 comment was wrong: notify_waiters stores no
            // permit).  Creating it first makes the wait race-free.
            let added = self.added.notified();
            match self.weighted_pick(&full) {
                Some(link) => match link.tx.try_send(frame.clone()) {
                    Ok(()) => return true,
                    Err(mpsc::error::TrySendError::Closed(_)) => {
                        // Tunnel died — mark it dead and re-pick.
                        link.alive.store(false, Ordering::Release);
                    }
                    Err(mpsc::error::TrySendError::Full(_)) => {
                        // Saturated: redistribute its weight this round.
                        full.push(link);
                    }
                },
                None => {
                    // No live link, or every live link is saturated —
                    // block on the highest-weight one (real backpressure,
                    // bounded by callers' DATA_SEND_TIMEOUT).
                    let best = {
                        let links = self.links.lock().unwrap();
                        links
                            .iter()
                            .filter(|l| l.alive.load(Ordering::Acquire))
                            .max_by(|a, b| {
                                let ra = f64::from_bits(a.rate_bps.load(Ordering::Relaxed));
                                let rb = f64::from_bits(b.rate_bps.load(Ordering::Relaxed));
                                ra.partial_cmp(&rb).unwrap_or(std::cmp::Ordering::Equal)
                            })
                            .cloned()
                    };
                    match best {
                        Some(link) => {
                            if link.tx.send(frame.clone()).await.is_ok() {
                                return true;
                            }
                            link.alive.store(false, Ordering::Release);
                            full.retain(|l| !Arc::ptr_eq(l, &link));
                        }
                        None => {
                            // B45: no live link at all right now — wait for
                            // one to be added (tunnel reconnect) instead of
                            // failing instantly, which truncated every
                            // active connection's transfer on a short total
                            // outage.  Callers wrap this in DATA_SEND_TIMEOUT
                            // (30s), so the wait stays bounded.  The future
                            // was created before the pick (B47), so a
                            // concurrent add() can never be missed.
                            added.await;
                        }
                    }
                }
            }
        }
    }

    /// Proportional pick over live, non-saturated links.  Deterministic
    /// weighted round-robin: the cursor rotates over a fixed anchor grid,
    /// so over a full cycle each link is picked in proportion to its
    /// weight.  Weights:
    /// - measured rate (`rate_bps`) when > 0;
    /// - the mean of measured links when never measured (optimistic — a
    ///   new tunnel must be probed at load to discover its capacity);
    /// - floored at `FLOOR_SHARE` of total weight (guaranteed share);
    /// - uniform when nothing has been measured yet (cold start).
    fn weighted_pick(&self, skip: &[Arc<TunnelLink>]) -> Option<Arc<TunnelLink>> {
        let links = self.links.lock().unwrap();
        let alive: Vec<Arc<TunnelLink>> = links
            .iter()
            .filter(|l| l.alive.load(Ordering::Acquire))
            .filter(|l| !skip.iter().any(|s| Arc::ptr_eq(s, l)))
            .cloned()
            .collect();
        if alive.is_empty() {
            return None;
        }
        let n = alive.len() as f64;
        let raw: Vec<f64> = alive
            .iter()
            .map(|l| f64::from_bits(l.rate_bps.load(Ordering::Relaxed)).max(0.0))
            .collect();
        let total: f64 = raw.iter().sum();
        let weights: Vec<f64> = if total > 0.0 {
            let mean = total / n;
            let floor = FLOOR_SHARE * total;
            raw.iter()
                .map(|&r| {
                    let w = if r > 0.0 { r } else { mean };
                    w.max(floor)
                })
                .collect()
        } else {
            vec![1.0; alive.len()] // cold start: uniform
        };
        let total: f64 = weights.iter().sum();
        let anchor = (self.rr.fetch_add(1, Ordering::Relaxed) % WEIGHT_GRID) as f64
            / WEIGHT_GRID as f64
            * total;
        let mut acc = 0.0;
        for (link, w) in alive.iter().zip(&weights) {
            acc += w;
            if anchor < acc {
                return Some(link.clone());
            }
        }
        Some(alive.last().unwrap().clone()) // rounding fallback
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

pub async fn drain_frames<W>(mut rx: mpsc::Receiver<Frame>, mut wr: W, link: Arc<TunnelLink>)
where
    W: tokio::io::AsyncWrite + Unpin,
{
    // O2: one reusable encode buffer per tunnel (no per-frame alloc).
    let mut enc = BytesMut::with_capacity(15 + MAX_PAYLOAD);
    let mut dead = false;
    // Phase 14: EWMA drain-rate state, published to `link.rate_bps` for
    // the weighted scheduler.  The interval between writes includes idle
    // gaps, so a saturated tunnel (writes stall on transport
    // backpressure) measures its real capacity while a fed one measures
    // its arrival rate — exactly what the scheduler needs.
    let mut rate: f64 = 0.0;
    let mut last_sample = std::time::Instant::now();
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
                    // B24: report it as lost so D1 recovery can reset the
                    // affected connection instead of stalling it.
                    link.lost_frames.lock().unwrap().push(frame);
                    dead = true;
                    continue;
                }
                tokio::select! {
                    _ = link.stop.notified() => {
                        // B24: the frame was dequeued but not written —
                        // report it as lost instead of dropping it
                        // silently (the old code let D1 recovery miss
                        // exactly this in-flight frame).
                        link.lost_frames.lock().unwrap().push(frame);
                        dead = true;
                    }
                    r = tokio::time::timeout(TUNNEL_WRITE_TIMEOUT, wr.write_all(&enc)) => {
                        match r {
                            Ok(Ok(())) => {
                                link.bytes_sent.fetch_add(n, Ordering::Relaxed);
                                link.frames_sent.fetch_add(1, Ordering::Relaxed);
                                // Phase 14: update the drain-rate EWMA.
                                let now = std::time::Instant::now();
                                let dt = now.duration_since(last_sample).as_secs_f64();
                                last_sample = now;
                                if dt > 0.0 {
                                    rate = ewma_rate(rate, n as f64 / dt, dt);
                                    link.rate_bps.store(rate.to_bits(), Ordering::Relaxed);
                                }
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
        // B45: no live link → wait for a link to be added, bounded by the
        // caller's timeout (as in production, DATA_SEND_TIMEOUT).
        let ok = tokio::time::timeout(
            std::time::Duration::from_millis(100),
            pool.send_async(Frame::rst(1)),
        )
        .await
        .unwrap_or(false);
        assert!(!ok);
    }

    /// O6: queue_depth must ignore dead links — a dead link's closed
    /// channel reads as full depth and used to inflate the backlog
    /// metric (and the heartbeat log) until the next compact.
    #[test]
    fn queue_depth_ignores_dead_links() {
        let pool = TunnelPool::new();
        // Production-capacity channels: the metric subtracts from
        // TUNNEL_CHANNEL_CAP, so smaller test channels would skew it.
        let (tx1, rx1) = mpsc::channel::<Frame>(TUNNEL_CHANNEL_CAP);
        let (tx2, rx2) = mpsc::channel::<Frame>(TUNNEL_CHANNEL_CAP);
        let live = mk_link(tx1, 0.0);
        let dead = mk_link(tx2, 0.0);
        pool.add(live.clone());
        pool.add(dead.clone());
        // One queued frame on the live link.
        live.tx.try_send(Frame::rst(1)).unwrap();
        // Dead link with the receiver dropped (closed channel).
        dead.alive.store(false, Ordering::Release);
        drop(rx2);
        assert_eq!(pool.queue_depth(), 1, "only live links count");
        drop(live);
        drop(rx1);
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
                rate_bps: AtomicU64::new(0),
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
            rate_bps: AtomicU64::new(0),
        });
        pool.add(link);
        drop(rx); // receiver gone → channel closed
        // B45: the dead link is marked down on first try_send; with no
        // live link left the send waits for a reconnect — the caller's
        // timeout is what makes it fail.
        let ok = tokio::time::timeout(
            std::time::Duration::from_millis(100),
            pool.send_async(Frame::rst(1)),
        )
        .await
        .unwrap_or(false);
        assert!(!ok);
    }

    /// B45 regression: with no live link, send_async must WAIT for a
    /// reconnect instead of failing instantly — a short total outage
    /// (all tunnels reconnecting) used to truncate every active
    /// connection's transfer.
    #[tokio::test]
    async fn send_async_waits_for_new_link() {
        let pool = Arc::new(TunnelPool::new());
        let (tx, mut rx) = mpsc::channel::<Frame>(4);
        let link = mk_link(tx, 0.0);
        let pool2 = pool.clone();
        let task = tokio::spawn(async move { pool2.send_async(Frame::rst(7)).await });
        // No link yet — the send must NOT have returned after 100ms.
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        assert!(!task.is_finished(), "send must wait while no link is live");
        // A reconnect lands — the send must complete promptly.
        pool.add(link);
        let sent = tokio::time::timeout(std::time::Duration::from_secs(1), task)
            .await
            .expect("send did not complete after a link was added")
            .unwrap();
        assert!(sent);
        assert_eq!(rx.recv().await.unwrap().conn_id, 7);
    }

    /// B47 regression: the reconnect add may land anywhere relative to
    /// send_async's first poll (spawn → pick → wait).  The Notified
    /// future is created BEFORE the pick, so every interleaving must
    /// end with the frame sent — never a lost wakeup stalling the send
    /// until the caller's 30s timeout.
    #[tokio::test]
    async fn send_async_sees_link_added_during_first_poll() {
        let pool = Arc::new(TunnelPool::new());
        let (tx, mut rx) = mpsc::channel::<Frame>(4);
        let link = mk_link(tx, 0.0);
        let pool2 = pool.clone();
        let task = tokio::spawn(async move { pool2.send_async(Frame::rst(7)).await });
        // No sleep: add immediately so the link may land before the
        // pick, between the pick and the wait, or during the wait.
        pool.add(link);
        let sent = tokio::time::timeout(std::time::Duration::from_secs(2), task)
            .await
            .expect("send did not complete despite the link being added")
            .unwrap();
        assert!(sent);
        assert_eq!(rx.recv().await.unwrap().conn_id, 7);
    }

    // B22/B24 regression: stop must make drain_frames exit promptly and
    // report the in-flight frame as lost (covers both the still-queued
    // path and the dequeued-but-stalled write path).
    #[tokio::test]
    async fn stop_exits_and_reports_lost_frames() {
        // duplex capacity 1024 per direction: a frame larger than the
        // buffer makes the drain task's write stall (nobody reads the
        // peer end), which forces the in-flight select path.
        let (peer, wr) = tokio::io::duplex(1024);

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
            rate_bps: AtomicU64::new(0),
        });
        let f = Frame::data(7, 3, bytes::Bytes::from(vec![0u8; 8192]));
        link.tx.send(f.clone()).await.unwrap();
        let task = tokio::spawn(drain_frames(rx, wr, link.clone()));
        // Give the drain task a chance to dequeue the frame and stall on
        // the full duplex buffer, then stop it.
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        link.stop.notify_one();
        tokio::time::timeout(std::time::Duration::from_secs(2), task)
            .await
            .expect("drain_frames did not exit after stop")
            .unwrap();
        let lost = link.lost_frames.lock().unwrap().clone();
        assert_eq!(lost.len(), 1, "in-flight frame must be reported as lost");
        assert_eq!(lost[0].conn_id, 7);
        assert_eq!(lost[0].seq, 3);
        drop(peer);
    }

    #[tokio::test]
    async fn stop_exits_with_empty_queue() {
        let (peer, wr) = tokio::io::duplex(1024);
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
            rate_bps: AtomicU64::new(0),
        });
        let task = tokio::spawn(drain_frames(rx, wr, link.clone()));
        link.stop.notify_one();
        tokio::time::timeout(std::time::Duration::from_secs(2), task)
            .await
            .expect("drain_frames did not exit after stop")
            .unwrap();
        assert!(link.lost_frames.lock().unwrap().is_empty());
        drop(peer);
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
            rate_bps: AtomicU64::new(0),
        });
        pool.add(link);
        assert!(pool.send_async(Frame::rst(1)).await);
        let got = rx.recv().await.unwrap();
        assert_eq!(got.conn_id, 1);
        assert_eq!(got.flags, crate::frame::FLAG_RST);
    }

    // ── Phase 14: weighted scheduler ────────────────────────────────────

    fn mk_link(tx: mpsc::Sender<Frame>, rate: f64) -> Arc<TunnelLink> {
        Arc::new(TunnelLink {
            tx,
            alive: AtomicBool::new(true),
            bytes_sent: AtomicU64::new(0),
            bytes_recv: AtomicU64::new(0),
            frames_sent: AtomicU64::new(0),
            frames_recv: AtomicU64::new(0),
            stop: Arc::new(Notify::new()),
            lost_frames: Mutex::new(Vec::new()),
            rate_bps: AtomicU64::new(rate.to_bits()),
        })
    }

    #[test]
    fn ewma_decays_and_converges() {
        // Steady state: prev == inst holds the value.
        let steady = ewma_rate(100.0, 100.0, 2.5);
        assert!((steady - 100.0).abs() < 1e-9);
        // After one time constant, the estimate moves 63% toward inst.
        let moved = ewma_rate(100.0, 20.0, 2.5);
        assert!((moved - 49.4304).abs() < 1e-3);
        // dt = 0 → no decay at all.
        assert_eq!(ewma_rate(100.0, 20.0, 0.0), 100.0);
    }

    /// The cursor rotates over a fixed anchor grid, so over one full
    /// cycle pick counts are deterministic — assert exact shares.
    #[test]
    fn weighted_pick_distributes_by_rate() {
        let pool = TunnelPool::new();
        let (tx1, _rx1) = mpsc::channel::<Frame>(4);
        let (tx2, _rx2) = mpsc::channel::<Frame>(4);
        let (tx3, _rx3) = mpsc::channel::<Frame>(4);
        let a = mk_link(tx1, 100.0);
        let b = mk_link(tx2, 50.0);
        let c = mk_link(tx3, 25.0);
        pool.add(a.clone());
        pool.add(b.clone());
        pool.add(c.clone());
        let mut counts = [0usize; 3];
        for _ in 0..WEIGHT_GRID {
            let picked = pool.weighted_pick(&[]).unwrap();
            if Arc::ptr_eq(&picked, &a) {
                counts[0] += 1;
            } else if Arc::ptr_eq(&picked, &b) {
                counts[1] += 1;
            } else if Arc::ptr_eq(&picked, &c) {
                counts[2] += 1;
            } else {
                panic!("unknown link picked");
            }
        }
        // 1024 × {100, 50, 25} / 175 → exact anchor boundaries.
        assert_eq!(counts, [586, 292, 146]);
    }

    /// Low-rate links are floored at FLOOR_SHARE of total weight — they
    /// keep a guaranteed share instead of being starved to zero.
    #[test]
    fn weighted_pick_floor_guarantees_share() {
        let pool = TunnelPool::new();
        let (tx1, _rx1) = mpsc::channel::<Frame>(4);
        let (tx2, _rx2) = mpsc::channel::<Frame>(4);
        let (tx3, _rx3) = mpsc::channel::<Frame>(4);
        let a = mk_link(tx1, 100.0);
        let b = mk_link(tx2, 1.0);
        let c = mk_link(tx3, 1.0);
        pool.add(a.clone());
        pool.add(b.clone());
        pool.add(c.clone());
        let mut counts = [0usize; 3];
        for _ in 0..WEIGHT_GRID {
            let picked = pool.weighted_pick(&[]).unwrap();
            if Arc::ptr_eq(&picked, &a) {
                counts[0] += 1;
            } else if Arc::ptr_eq(&picked, &b) {
                counts[1] += 1;
            } else {
                counts[2] += 1;
            }
        }
        // Floor = 5% of 102 = 5.1 → low links get 47 picks each, not
        // ~9 (their raw 1/102 share would have been).
        assert_eq!(counts, [930, 47, 47]);
    }

    /// Never-measured links (rate 0) get the mean weight — optimistic
    /// probing so a new tunnel is fed at load and can be measured.
    #[test]
    fn weighted_pick_optimistic_for_unmeasured() {
        let pool = TunnelPool::new();
        let (tx1, _rx1) = mpsc::channel::<Frame>(4);
        let (tx2, _rx2) = mpsc::channel::<Frame>(4);
        let (tx3, _rx3) = mpsc::channel::<Frame>(4);
        let a = mk_link(tx1, 100.0);
        let b = mk_link(tx2, 0.0);
        let c = mk_link(tx3, 0.0);
        pool.add(a.clone());
        pool.add(b.clone());
        pool.add(c.clone());
        let mut counts = [0usize; 3];
        for _ in 0..WEIGHT_GRID {
            let picked = pool.weighted_pick(&[]).unwrap();
            if Arc::ptr_eq(&picked, &a) {
                counts[0] += 1;
            } else if Arc::ptr_eq(&picked, &b) {
                counts[1] += 1;
            } else {
                counts[2] += 1;
            }
        }
        // Mean = 100/3 ≈ 33.3 → the two new links split ~40% of picks.
        assert_eq!(counts, [615, 205, 204]);
    }

    /// Cold start (nothing measured): uniform rotation.
    #[test]
    fn weighted_pick_cold_start_uniform() {
        let pool = TunnelPool::new();
        let (tx1, _rx1) = mpsc::channel::<Frame>(4);
        let (tx2, _rx2) = mpsc::channel::<Frame>(4);
        let (tx3, _rx3) = mpsc::channel::<Frame>(4);
        let a = mk_link(tx1, 0.0);
        let b = mk_link(tx2, 0.0);
        let c = mk_link(tx3, 0.0);
        pool.add(a.clone());
        pool.add(b.clone());
        pool.add(c.clone());
        let mut counts = [0usize; 3];
        for _ in 0..WEIGHT_GRID {
            let picked = pool.weighted_pick(&[]).unwrap();
            if Arc::ptr_eq(&picked, &a) {
                counts[0] += 1;
            } else if Arc::ptr_eq(&picked, &b) {
                counts[1] += 1;
            } else {
                counts[2] += 1;
            }
        }
        assert_eq!(counts, [342, 341, 341]);
    }

    /// A saturated link's weight is redistributed: the frame goes to the
    /// healthy link instead of blocking on the full queue.
    #[tokio::test]
    async fn send_async_skips_full_link() {
        let pool = TunnelPool::new();
        let (tx_a, mut rx_a) = mpsc::channel::<Frame>(2);
        let (tx_b, mut rx_b) = mpsc::channel::<Frame>(2);
        let a = mk_link(tx_a, 100.0);
        let b = mk_link(tx_b, 50.0);
        // Saturate A (receiver kept alive so the channel is not closed).
        assert!(a.tx.try_send(Frame::rst(9)).is_ok());
        assert!(a.tx.try_send(Frame::rst(10)).is_ok());
        pool.add(a);
        pool.add(b);

        assert!(pool.send_async(Frame::rst(7)).await);
        // The frame landed on B; A still holds exactly its two fillers.
        let got = rx_b.recv().await.unwrap();
        assert_eq!(got.conn_id, 7);
        assert_eq!(rx_a.try_recv().unwrap().conn_id, 9);
        assert_eq!(rx_a.try_recv().unwrap().conn_id, 10);
        assert!(rx_a.try_recv().is_err());
    }

    /// When every live link is saturated, block on the best one until it
    /// drains (real backpressure) instead of dropping the frame.
    #[tokio::test]
    async fn send_async_blocks_when_all_full() {
        let pool = TunnelPool::new();
        let (tx_a, mut rx_a) = mpsc::channel::<Frame>(2);
        let (tx_b, _rx_b) = mpsc::channel::<Frame>(2);
        let a = mk_link(tx_a, 100.0);
        let b = mk_link(tx_b, 50.0);
        assert!(a.tx.try_send(Frame::rst(9)).is_ok());
        assert!(a.tx.try_send(Frame::rst(10)).is_ok());
        assert!(b.tx.try_send(Frame::rst(11)).is_ok());
        assert!(b.tx.try_send(Frame::rst(12)).is_ok());
        pool.add(a.clone());
        pool.add(b);

        let task = tokio::spawn({
            let pool = Arc::new(pool);
            async move { pool.send_async(Frame::rst(7)).await }
        });
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        assert!(!task.is_finished(), "must block while all links are full");
        // Drain one slot on A (the best link) — the send must complete.
        let _ = rx_a.try_recv().unwrap();
        let sent = tokio::time::timeout(std::time::Duration::from_secs(2), task)
            .await
            .expect("send did not complete after drain")
            .unwrap();
        assert!(sent);
        // The test frame is queued on A after the remaining filler.
        let _ = rx_a.try_recv().unwrap(); // filler (conn 10)
        assert_eq!(rx_a.try_recv().unwrap().conn_id, 7);
    }
}
