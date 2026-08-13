use crate::frame::MAX_REORDER_WINDOW;
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
pub struct ReorderBuf {
    expected: u64,
    pending: BTreeMap<u64, Bytes>,
}

impl ReorderBuf {
    pub fn new() -> Self {
        Self {
            expected: 1,
            pending: BTreeMap::new(),
        }
    }

    /// Returns in-order chunks and whether this frame was accepted.
    /// Out-of-order frames are buffered until the gap fills.
    /// Frames are dropped (accepted=false) when:
    /// - seq < expected (duplicate)
    /// - pending buffer is full (≥ MAX_REORDER_WINDOW)
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
                ready.push(chunk);
                self.expected = self.expected.wrapping_add(1);
            }
            PushResult {
                ready,
                accepted: true,
                overflow: false,
            }
        } else if self.pending.len() < MAX_REORDER_WINDOW {
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

    /// True when every frame below `seq` has been delivered (used to
    /// decide when an egress write half can be closed after FIN).
    pub fn is_complete_through(&self, seq: u64) -> bool {
        self.expected >= seq
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
}
