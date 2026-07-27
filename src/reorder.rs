use crate::frame::MAX_REORDER_WINDOW;
use bytes::Bytes;
use std::collections::BTreeMap;

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

    /// Returns in-order chunks. Out-of-order frames are buffered until the gap fills.
    /// TUIC TCP guarantees delivery — we just wait.
    pub fn push(&mut self, seq: u64, payload: Bytes) -> Vec<Bytes> {
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
        } else if self.pending.len() < MAX_REORDER_WINDOW {
            self.pending.insert(seq, payload);
        }

        out
    }
}
