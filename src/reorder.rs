use crate::frame::{MAX_REORDER_BYTES, MAX_REORDER_WINDOW};
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
/// The window is bounded both by entry count (`MAX_REORDER_WINDOW`) and
/// by total buffered bytes (`MAX_REORDER_BYTES`, BUG-8 fix: 512 × 64 KB
/// = 32 MB per connection was too much).
pub struct ReorderBuf {
    expected: u64,
    pending: BTreeMap<u64, Bytes>,
    pending_bytes: usize,
}

impl ReorderBuf {
    pub fn new() -> Self {
        Self {
            expected: 1,
            pending: BTreeMap::new(),
            pending_bytes: 0,
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
        } else if self.pending.len() < MAX_REORDER_WINDOW
            && self.pending_bytes + payload.len() <= MAX_REORDER_BYTES
        {
            self.pending_bytes += payload.len();
            self.pending.insert(seq, payload);
            PushResult {
                ready,
                accepted: true,
                overflow: false,
            }
        } else {
            // Buffer full — drop the frame. The caller must reset the
            // connection: the missing seq will never be retransmitted.
            PushResult {
                ready,
                accepted: false,
                overflow: true,
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
        let mut buf = ReorderBuf::new();
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
        // BUG-8: with 64 KB chunks the byte budget (8 MB) must kick in
        // long before the 512-entry cap.
        let mut buf = ReorderBuf::new();
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
        assert_eq!(accepted, MAX_REORDER_BYTES / (64 * 1024));
        assert_eq!(buf.pending_bytes(), MAX_REORDER_BYTES);
        // Next frame overflows (signals reset) instead of being queued.
        let overflow = buf.push(1_000_000, chunk.clone());
        assert!(overflow.overflow && !overflow.accepted);

        // Delivering seq 1 drains everything and resets the byte counter.
        let ready = buf.push(1, Bytes::from_static(b"z"));
        assert!(ready.accepted);
        assert_eq!(ready.ready.len(), accepted + 1);
        assert_eq!(buf.pending_bytes(), 0);
    }
}
