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
            }
        } else if self.pending.len() < MAX_REORDER_WINDOW {
            self.pending.insert(seq, payload);
            PushResult {
                ready,
                accepted: true,
            }
        } else {
            // Buffer full — drop the frame.
            PushResult {
                ready,
                accepted: false,
            }
        }
    }
}
