use anyhow::{Result, bail};
use bytes::{BufMut, Bytes, BytesMut};

// ── Wire format (big-endian) ──────────────────────────────────────────
// ConnID    u32   4 bytes
// Sequence  u64   8 bytes
// Flags     u8    1 byte
// Length    u16   2 bytes   (payload length, 0–65535)
// Payload   [u8]  Length bytes
// Total header: 15 bytes

pub const FLAG_SYN: u8 = 0x01;
pub const FLAG_DATA: u8 = 0x02;
pub const FLAG_FIN: u8 = 0x04;
pub const FLAG_RST: u8 = 0x08;
// ACK (0x10) is reserved in the wire format but unused — TUIC TCP
// guarantees delivery, so no application-level flow control is needed.

pub const HEADER_LEN: usize = 4 + 8 + 1 + 2; // 15
pub const MAX_PAYLOAD: usize = 65535;
pub const MIN_CHUNK: usize = 512;
pub const MAX_CHUNK: usize = 65535;

/// Max out-of-order entries before new arrivals are dropped.
pub const MAX_REORDER_WINDOW: usize = 512;
/// BUG-8 fix: byte budget for the reorder window, independent of frame
/// count. 512 frames × 64 KB = 32 MB/connection was too much; 8 MB caps
/// memory while keeping the window large enough for high-BDP tunnels.
pub const MAX_REORDER_BYTES: usize = 8 * 1024 * 1024;
/// Max number of pending CIDs with DATA-before-SYN buffered (reassembler).
pub const MAX_PENDING_CIDS: usize = 256;
/// BUG-7 fix: global byte budget for pending DATA-before-SYN frames.
/// 256 CIDs × 256 frames × 64 KB ≈ 4.3 GB theoretical; cap at 64 MB.
pub const MAX_PENDING_BYTES: usize = 64 * 1024 * 1024;

// ── Frame ─────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct Frame {
    pub conn_id: u32,
    pub seq: u64,
    pub flags: u8,
    pub payload: Bytes,
}

impl Frame {
    pub fn data(conn_id: u32, seq: u64, payload: Bytes) -> Self {
        Self {
            conn_id,
            seq,
            flags: FLAG_DATA,
            payload,
        }
    }

    pub fn syn(conn_id: u32, payload: Bytes) -> Self {
        Self {
            conn_id,
            seq: 0,
            flags: FLAG_SYN,
            payload,
        }
    }

    pub fn fin(conn_id: u32, seq: u64) -> Self {
        Self {
            conn_id,
            seq,
            flags: FLAG_FIN,
            payload: Bytes::new(),
        }
    }

    pub fn rst(conn_id: u32) -> Self {
        Self {
            conn_id,
            seq: 0,
            flags: FLAG_RST,
            payload: Bytes::new(),
        }
    }

    /// Encode the frame to wire format.  Returns an error instead of
    /// truncating when the payload exceeds the u16 length field —
    /// truncation would corrupt the stream (the tail would be parsed as
    /// the next frame header).
    pub fn encode(&self) -> Result<Bytes> {
        let mut buf = BytesMut::with_capacity(HEADER_LEN + self.payload.len());
        self.encode_into(&mut buf)?;
        Ok(buf.freeze())
    }

    /// Encode into a reusable buffer (O2: avoids one allocation per
    /// frame on the tunnel write path).
    pub fn encode_into(&self, buf: &mut BytesMut) -> Result<()> {
        if self.payload.len() > MAX_PAYLOAD {
            bail!(
                "frame payload too large: {} > {MAX_PAYLOAD}",
                self.payload.len()
            );
        }
        buf.reserve(HEADER_LEN + self.payload.len());
        buf.put_u32(self.conn_id);
        buf.put_u64(self.seq);
        buf.put_u8(self.flags);
        buf.put_u16(self.payload.len() as u16);
        buf.put_slice(&self.payload);
        Ok(())
    }
}

// ── SYN payload helpers ───────────────────────────────────────────────

pub const PROTO_TCP: u8 = 0x06;
pub const PROTO_UDP: u8 = 0x11;

#[derive(Debug, Clone)]
pub struct SynTarget {
    pub proto: u8,
    pub address: String,
    pub port: u16,
}

impl SynTarget {
    /// B30: returns an error instead of truncating addresses that exceed
    /// the u16 length field (truncation would corrupt the SYN payload).
    pub fn encode(&self) -> Result<Bytes> {
        let addr = self.address.as_bytes();
        if addr.len() > u16::MAX as usize {
            bail!("SYN address too long: {} bytes", addr.len());
        }
        let mut buf = BytesMut::with_capacity(1 + 2 + addr.len() + 2);
        buf.put_u8(self.proto);
        buf.put_u16(addr.len() as u16);
        buf.put_slice(addr);
        buf.put_u16(self.port);
        Ok(buf.freeze())
    }

    pub fn decode(payload: &[u8]) -> Result<Self> {
        if payload.len() < 5 {
            bail!("SYN payload too short");
        }
        let proto = payload[0];
        let addr_len = u16::from_be_bytes([payload[1], payload[2]]) as usize;
        if payload.len() < 5 + addr_len {
            bail!("SYN payload truncated");
        }
        let address = String::from_utf8(payload[3..3 + addr_len].to_vec())?;
        let port = u16::from_be_bytes([payload[3 + addr_len], payload[4 + addr_len]]);
        Ok(SynTarget {
            proto,
            address,
            port,
        })
    }
}

// ── Streaming frame decoder ───────────────────────────────────────────

/// Max decoder buffer before we bail out.  One full max frame plus two
/// read blocks of headroom — anything larger indicates a malformed or
/// malicious peer that never completes a frame.
const MAX_DECODER_BUF: usize = HEADER_LEN + MAX_PAYLOAD + 16384; // ~81 KB

/// Stateful decoder that reads from a TCP byte stream and yields complete
/// frames one at a time. Handles frames up to 64 KiB payload.
pub struct FrameDecoder {
    buf: BytesMut,
}

impl FrameDecoder {
    pub fn new() -> Self {
        Self {
            buf: BytesMut::with_capacity(16384),
        }
    }

    /// Read from `rd` until a complete frame is available. Returns `None`
    /// on clean EOF with no partial data.
    pub async fn try_next(
        &mut self,
        rd: &mut (impl tokio::io::AsyncReadExt + Unpin),
    ) -> Result<Option<Frame>> {
        loop {
            // Try to parse a complete frame from the buffer
            if self.buf.len() >= HEADER_LEN {
                let payload_len = u16::from_be_bytes([self.buf[13], self.buf[14]]) as usize;
                if payload_len > MAX_PAYLOAD {
                    bail!("frame payload too large: {payload_len}");
                }
                if self.buf.len() >= HEADER_LEN + payload_len {
                    let conn_id =
                        u32::from_be_bytes([self.buf[0], self.buf[1], self.buf[2], self.buf[3]]);
                    let seq = u64::from_be_bytes(self.buf[4..12].try_into().unwrap());
                    let flags = self.buf[12];
                    let _ = self.buf.split_to(HEADER_LEN);
                    let payload = self.buf.split_to(payload_len).freeze();
                    return Ok(Some(Frame {
                        conn_id,
                        seq,
                        flags,
                        payload,
                    }));
                }
            }

            // Need more data: read directly into the decoder buffer
            // (O1: no 8 KB stack buffer + copy).  Bound the read so a
            // malformed / malicious peer can never blow past the limit.
            let space = MAX_DECODER_BUF.saturating_sub(self.buf.len());
            if space == 0 {
                bail!("frame decoder buffer overflow (> {MAX_DECODER_BUF} bytes)");
            }
            self.buf.reserve(space.min(8192));
            let n = rd.read_buf(&mut self.buf).await?;
            if n == 0 {
                return if self.buf.is_empty() {
                    Ok(None)
                } else {
                    bail!("EOF mid-frame ({} buffered bytes)", self.buf.len())
                };
            }
        }
    }
}

impl Default for FrameDecoder {
    fn default() -> Self {
        Self::new()
    }
}

// ── tests ─────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncRead;

    // Helper: wrap encoded bytes in a reader
    struct BufReader(Vec<u8>, usize);

    impl AsyncRead for BufReader {
        fn poll_read(
            mut self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
            buf: &mut tokio::io::ReadBuf<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            let remaining = &self.0[self.1..];
            let n = remaining.len().min(buf.remaining());
            buf.put_slice(&remaining[..n]);
            self.1 += n;
            std::task::Poll::Ready(Ok(()))
        }
    }

    #[tokio::test]
    async fn decoder_single_frame() {
        let f = Frame::data(1, 42, Bytes::from_static(b"payload"));
        let encoded = f.encode().unwrap().to_vec();
        let mut decoder = FrameDecoder::new();
        let mut reader = BufReader(encoded, 0);
        let got = decoder.try_next(&mut reader).await.unwrap().unwrap();
        assert_eq!(got.conn_id, 1);
        assert_eq!(got.seq, 42);
        assert_eq!(got.flags, FLAG_DATA);
        assert_eq!(&got.payload[..], b"payload");
    }

    #[tokio::test]
    async fn decoder_multiple_frames() {
        let f1 = Frame::data(1, 1, Bytes::from_static(b"aaa"));
        let f2 = Frame::data(1, 2, Bytes::from_static(b"bb"));
        let mut data = Vec::new();
        data.extend_from_slice(&f1.encode().unwrap());
        data.extend_from_slice(&f2.encode().unwrap());

        let mut decoder = FrameDecoder::new();
        let mut reader = BufReader(data, 0);
        let g1 = decoder.try_next(&mut reader).await.unwrap().unwrap();
        let g2 = decoder.try_next(&mut reader).await.unwrap().unwrap();
        assert_eq!(&g1.payload[..], b"aaa");
        assert_eq!(&g2.payload[..], b"bb");
    }

    #[tokio::test]
    async fn decoder_tiny_reads() {
        // Simulate byte-by-byte reads to stress the buffer logic
        let f = Frame::syn(
            7,
            SynTarget {
                proto: PROTO_TCP,
                address: "example.com".into(),
                port: 443,
            }
            .encode()
            .unwrap(),
        );
        let encoded = f.encode().unwrap();
        let mut decoder = FrameDecoder::new();

        // ponytail: feed one byte at a time via a manual reader
        let mut pos = 0;
        loop {
            let mut tmp = [0u8; 1];
            if pos >= encoded.len() {
                break;
            }
            tmp[0] = encoded[pos];
            pos += 1;
            let mut reader = BufReader(tmp.to_vec(), 0);
            if let Ok(Some(frame)) = decoder.try_next(&mut reader).await {
                assert_eq!(frame.conn_id, 7);
                assert_eq!(frame.flags, FLAG_SYN);
                let parsed = SynTarget::decode(&frame.payload).unwrap();
                assert_eq!(parsed.address, "example.com");
                return; // success
            }
        }
        panic!("decoder never returned a frame");
    }

    #[test]
    fn frame_roundtrip() {
        let f = Frame::data(42, 7, Bytes::from_static(b"hello"));
        let encoded = f.encode().unwrap();
        assert_eq!(encoded.len(), HEADER_LEN + 5);

        // Decode manually
        let conn_id = u32::from_be_bytes([encoded[0], encoded[1], encoded[2], encoded[3]]);
        let seq = u64::from_be_bytes(encoded[4..12].try_into().unwrap());
        let flags = encoded[12];
        let len = u16::from_be_bytes([encoded[13], encoded[14]]) as usize;
        assert_eq!(conn_id, 42);
        assert_eq!(seq, 7);
        assert_eq!(flags, FLAG_DATA);
        assert_eq!(len, 5);
        assert_eq!(&encoded[15..], b"hello");
    }

    #[test]
    fn encode_rejects_oversized_payload() {
        // BUG-12: release builds must not silently truncate via `as u16`.
        let f = Frame::data(1, 1, Bytes::from(vec![0u8; MAX_PAYLOAD + 1]));
        assert!(f.encode().is_err());
        let ok = Frame::data(1, 1, Bytes::from(vec![0u8; MAX_PAYLOAD]));
        assert!(ok.encode().is_ok());
    }

    #[test]
    fn syn_target_roundtrip() {
        let t = SynTarget {
            proto: PROTO_TCP,
            address: "example.com".into(),
            port: 443,
        };
        let encoded = t.encode().unwrap();
        let decoded = SynTarget::decode(&encoded).unwrap();
        assert_eq!(decoded.proto, PROTO_TCP);
        assert_eq!(decoded.address, "example.com");
        assert_eq!(decoded.port, 443);
    }
}
