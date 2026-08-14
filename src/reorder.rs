use crate::frame::{MAX_PAYLOAD, MAX_REORDER_BYTES, MAX_REORDER_WINDOW};
use bytes::Bytes;
use std::collections::BTreeMap;

/// Result of a push into the reorder buffer.
pub struct PushResult {
    /// In-order chunks that are ready to be delivered.
    pub ready: Vec<Bytes>,
    /// Whether the frame was accepted (queued or delivered).
    /// false means the frame was either a duplicate or dropped
    /// because the buffer is full.
    pub accepted: bool,
    /// Set when the frame was dropped because the window is full. The
    /// sequence is permanently broken then (TCP tunnels deliver exactly
    /// once, no retransmit) — callers must reset the connection.
    pub overflow: bool,
}

/// Reorder buffer: buffers out-of-order chunks and delivers them in
/// sequence order once gaps are filled.
///
/// The window is bounded both by entry count and by total buffered
/// bytes.  The byte budget is configurable (B58: it must cover the
/// sender's in-flight window — tunnels × 128 frames × chunk_size — or
/// latency skew between tunnels overflows the window and resets the
/// connection mid-transfer).
pub struct ReorderBuf {
    expected: u64,
    pending: BTreeMap<u64, Bytes>,
    pending_bytes: usize,
    /// Per-connection byte budget (B58, default `MAX_REORDER_BYTES`).
    max_bytes: usize,
    /// Entry cap: `max_bytes / MAX_PAYLOAD`, floored at
    /// `MAX_REORDER_WINDOW` — bounds BTreeMap node count for pathological
    /// tiny frames while never binding before the byte budget for
    /// full-size frames.
    max_entries: usize,
}

impl ReorderBuf {
    /// Default window: `MAX_REORDER_BYTES` (64 MB — covers 8 tunnels of
    /// full in-flight skew at the default chunk size).
    pub fn new() -> Self {
        Self::with_limit(MAX_REORDER_BYTES)
    }

    /// B58: explicit window budget (config `reorder_window_bytes`).
    pub fn with_limit(max_bytes: usize) -> Self {
        Self {
            expected: 1,
            pending: BTreeMap::new(),
            pending_bytes: 0,
            max_bytes,
            max_entries: (max_bytes / MAX_PAYLOAD).max(MAX_REORDER_WINDOW),
        }
    }

    /// Returns in-order chunks and whether this frame was accepted.
    /// Out-of-order frames are buffered until the gap fills.
    /// Frames are dropped (accepted=false) when:
    /// - seq < expected (duplicate)
    /// - pending buffer is full (≥ MAX_REORDER_WINDOW entries or
    ///   ≥ MAX_REORDER_BYTES bytes)
    pub fn push(&mut self, seq: u64, payload: Bytes) -> PushResult {
        let mut ready = Vec::new();

        if seq < self.expected {
            return PushResult {
                ready,
                accepted: false,
                overflow: false,
            }; // duplicate
        }
        if seq == self.expected {
            ready.push(payload);
            self.expected = self.expected.wrapping_add(1);
            while let Some(chunk) = self.pending.remove(&self.expected) {
                self.pending_bytes -= chunk.len();
                ready.push(chunk);
                self.expected = self.expected.wrapping_add(1);
            }
            PushResult {
                ready,
                accepted: true,
                overflow: false,
            }
        } else {
            // B54: check the capacity before `entry` (which borrows the
            // map mutably) and dedup pending duplicates via the entry
            // API.  The old `insert` replaced a buffered entry and added
            // the new payload's bytes without subtracting the old ones —
            // every duplicate leaked byte budget and could trip the
            // overflow reset early.
            let can_buffer = self.pending.len() < self.max_entries
                && self.pending_bytes + payload.len() <= self.max_bytes;
            match self.pending.entry(seq) {
                std::collections::btree_map::Entry::Vacant(e) if can_buffer => {
                    self.pending_bytes += payload.len();
                    e.insert(payload);
                    PushResult {
                        ready,
                        accepted: true,
                        overflow: false,
                    }
                }
                // Buffer full — drop the frame. The caller must reset the
                // connection: the missing seq will never be retransmitted.
                std::collections::btree_map::Entry::Vacant(_) => PushResult {
                    ready,
                    accepted: false,
                    overflow: true,
                },
                // Duplicate of a frame still buffered in the window —
                // drop it like any other duplicate.
                std::collections::btree_map::Entry::Occupied(_) => PushResult {
                    ready,
                    accepted: false,
                    overflow: false,
                },
            }
        }
    }

    /// Number of bytes currently buffered (out-of-order).
    #[cfg(test)]
    fn pending_bytes(&self) -> usize {
        self.pending_bytes
    }

    /// True when every frame below `seq` has been delivered (used to
    /// decide when an egress write half can be closed after FIN).
    pub fn is_complete_through(&self, seq: u64) -> bool {
        self.expected >= seq
    }
}

impl Default for ReorderBuf {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn overflow_and_completion() {
        // 8 MB limit → entry cap stays at the 512 floor (8 MB / 64 KB =
        // 128 < 512), preserving the classic window-full behavior.
        let mut buf = ReorderBuf::with_limit(8 * 1024 * 1024);
        // 512 pending frames fill the window; the next one overflows.
        for s in 2..(MAX_REORDER_WINDOW as u64 + 3) {
            buf.push(s, Bytes::from_static(b"x"));
        }
        let overflowed = buf.push(1_000_000, Bytes::from_static(b"y"));
        assert!(overflowed.overflow && !overflowed.accepted);

        // Nothing delivered yet — not complete through any real seq.
        assert!(!buf.is_complete_through(2));

        // Delivering seq 1 drains the whole window in order.
        let ready = buf.push(1, Bytes::from_static(b"z"));
        assert!(ready.accepted && !ready.overflow);
        assert_eq!(ready.ready.len(), MAX_REORDER_WINDOW + 1);
        assert!(buf.is_complete_through(MAX_REORDER_WINDOW as u64 + 2));
        assert!(!buf.is_complete_through(MAX_REORDER_WINDOW as u64 + 3));

        // A late duplicate is rejected without signalling overflow.
        let dup = buf.push(1, Bytes::from_static(b"z"));
        assert!(!dup.accepted && !dup.overflow);
    }

    #[test]
    fn byte_budget_bounds_window() {
        // BUG-8 semantics with an explicit 8 MB limit: with 64 KB chunks
        // the byte budget kicks in long before the 512-entry cap.
        let mut buf = ReorderBuf::with_limit(8 * 1024 * 1024);
        let chunk = Bytes::from(vec![0u8; 64 * 1024]);
        let mut accepted = 0;
        for s in 2..(MAX_REORDER_WINDOW as u64 + 2) {
            if buf.push(s, chunk.clone()).accepted {
                accepted += 1;
            } else {
                break;
            }
        }
        // 8 MB / 64 KB = 128 frames; far below the 512-entry cap.
        assert_eq!(accepted, (8 * 1024 * 1024) / (64 * 1024));
        assert_eq!(buf.pending_bytes(), 8 * 1024 * 1024);
        // Next frame overflows (signals reset) instead of being queued.
        let overflow = buf.push(1_000_000, chunk.clone());
        assert!(overflow.overflow && !overflow.accepted);

        // Delivering seq 1 drains everything and resets the byte counter.
        let ready = buf.push(1, Bytes::from_static(b"z"));
        assert!(ready.accepted);
        assert_eq!(ready.ready.len(), accepted + 1);
        assert_eq!(buf.pending_bytes(), 0);
    }

    /// B58 regression: the DEFAULT window must tolerate the full
    /// in-flight skew of 4 tunnels (4 × 128 frames × 64 KB = 32 MB).
    /// The old fixed 8 MB cap overflowed at ~1/4 of that and reset every
    /// large download on latency-skewed tunnels.
    #[test]
    fn default_window_tolerates_four_tunnel_skew() {
        let mut buf = ReorderBuf::new();
        let chunk = Bytes::from(vec![0u8; 64 * 1024]);
        // 512 out-of-order frames = 32 MB = 4 tunnels' full in-flight.
        let mut accepted = 0;
        for s in 2..(MAX_REORDER_WINDOW as u64 + 2) {
            assert!(
                buf.push(s, chunk.clone()).accepted,
                "frame {s} must be accepted by the default window"
            );
            accepted += 1;
        }
        assert_eq!(accepted, MAX_REORDER_WINDOW);
        // Delivering the gap frame drains all 32 MB in order.
        let ready = buf.push(1, Bytes::from_static(b"z"));
        assert_eq!(ready.ready.len(), MAX_REORDER_WINDOW + 1);
        assert_eq!(buf.pending_bytes(), 0);
    }

    #[test]
    fn duplicate_of_pending_frame_does_not_leak_bytes() {
        // B54: a duplicate of a still-buffered frame used to replace the
        // pending entry and add the new payload's bytes again — the byte
        // accounting leaked on every duplicate and could trip the
        // overflow reset early.  A dup must be dropped like any other.
        let mut buf = ReorderBuf::new();
        assert!(buf.push(2, Bytes::from(vec![0u8; 10])).accepted);
        assert_eq!(buf.pending_bytes(), 10);
        // Same seq, different payload — duplicate, not a replacement.
        let dup = buf.push(2, Bytes::from(vec![0u8; 1000]));
        assert!(!dup.accepted && !dup.overflow);
        assert_eq!(buf.pending_bytes(), 10, "duplicate must not leak bytes");
        // The original payload must still be delivered untouched.
        let ready = buf.push(1, Bytes::from_static(b"z"));
        assert_eq!(ready.ready.len(), 2);
        assert_eq!(ready.ready[1].len(), 10);
        assert_eq!(buf.pending_bytes(), 0);
    }
}
