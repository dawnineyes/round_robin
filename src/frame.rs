use anyhow::{bail, Result};
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
pub const FLAG_ACK: u8 = 0x10;

pub const HEADER_LEN: usize = 4 + 8 + 1 + 2; // 15
pub const MAX_PAYLOAD: usize = 65535;
pub const MIN_CHUNK: usize = 512;
pub const MAX_CHUNK: usize = 65535;

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
        Self { conn_id, seq, flags: FLAG_DATA, payload }
    }

    pub fn syn(conn_id: u32, payload: Bytes) -> Self {
        Self { conn_id, seq: 0, flags: FLAG_SYN, payload }
    }

    pub fn syn_ack(conn_id: u32) -> Self {
        Self { conn_id, seq: 0, flags: FLAG_SYN | FLAG_ACK, payload: Bytes::new() }
    }

    pub fn fin(conn_id: u32, seq: u64) -> Self {
        Self { conn_id, seq, flags: FLAG_FIN, payload: Bytes::new() }
    }

    pub fn rst(conn_id: u32) -> Self {
        Self { conn_id, seq: 0, flags: FLAG_RST, payload: Bytes::new() }
    }

    #[allow(dead_code)]
    pub fn ack(conn_id: u32, ack_seq: u64, window: u32) -> Self {
        let mut payload = BytesMut::with_capacity(12);
        payload.put_u64(ack_seq);
        payload.put_u32(window);
        Self { conn_id, seq: 0, flags: FLAG_ACK, payload: payload.freeze() }
    }

    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(HEADER_LEN + self.payload.len());
        buf.put_u32(self.conn_id);
        buf.put_u64(self.seq);
        buf.put_u8(self.flags);
        buf.put_u16(self.payload.len() as u16);
        buf.put_slice(&self.payload);
        buf.freeze()
    }
}

// ── SYN payload helpers ───────────────────────────────────────────────

pub const PROTO_TCP: u8 = 0x06;
#[allow(dead_code)]
pub const PROTO_UDP: u8 = 0x11;

#[derive(Debug, Clone)]
pub struct SynTarget {
    pub proto: u8,
    pub address: String,
    pub port: u16,
}

impl SynTarget {
    pub fn encode(&self) -> Bytes {
        let addr = self.address.as_bytes();
        let mut buf = BytesMut::with_capacity(1 + 2 + addr.len() + 2);
        buf.put_u8(self.proto);
        buf.put_u16(addr.len() as u16);
        buf.put_slice(addr);
        buf.put_u16(self.port);
        buf.freeze()
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
        Ok(SynTarget { proto, address, port })
    }
}

// ── ACK payload helpers ───────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct AckInfo {
    #[allow(dead_code)]
    pub ack_seq: u64,
    #[allow(dead_code)]
    pub window: u32,
}

impl AckInfo {
    pub fn decode(payload: &[u8]) -> Result<Self> {
        if payload.len() < 12 {
            bail!("ACK payload too short");
        }
        let ack_seq = u64::from_be_bytes(payload[0..8].try_into().unwrap());
        let window = u32::from_be_bytes(payload[8..12].try_into().unwrap());
        Ok(AckInfo { ack_seq, window })
    }
}

// ── Streaming frame decoder ───────────────────────────────────────────

/// Stateful decoder that reads from a TCP byte stream and yields complete
/// frames one at a time. Handles frames up to 64 KiB payload.
pub struct FrameDecoder {
    buf: BytesMut,
}

impl FrameDecoder {
    pub fn new() -> Self {
        Self { buf: BytesMut::with_capacity(16384) }
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
                let payload_len =
                    u16::from_be_bytes([self.buf[13], self.buf[14]]) as usize;
                if payload_len > MAX_PAYLOAD {
                    bail!("frame payload too large: {payload_len}");
                }
                if self.buf.len() >= HEADER_LEN + payload_len {
                    let conn_id = u32::from_be_bytes(
                        [self.buf[0], self.buf[1], self.buf[2], self.buf[3]],
                    );
                    let seq = u64::from_be_bytes(
                        self.buf[4..12].try_into().unwrap(),
                    );
                    let flags = self.buf[12];
                    let _ = self.buf.split_to(HEADER_LEN);
                    let payload = self.buf.split_to(payload_len).freeze();
                    return Ok(Some(Frame { conn_id, seq, flags, payload }));
                }
            }

            // Need more data: read into a temp buffer, append
            let mut tmp = [0u8; 8192];
            let n = rd.read(&mut tmp).await?;
            if n == 0 {
                return if self.buf.is_empty() {
                    Ok(None)
                } else {
                    bail!("EOF mid-frame ({} buffered bytes)", self.buf.len())
                };
            }
            self.buf.extend_from_slice(&tmp[..n]);
        }
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
        let encoded = f.encode().to_vec();
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
        data.extend_from_slice(&f1.encode());
        data.extend_from_slice(&f2.encode());

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
        let f = Frame::syn(7, SynTarget { proto: PROTO_TCP, address: "example.com".into(), port: 443 }.encode());
        let encoded = f.encode();
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
        let encoded = f.encode();
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
    fn syn_target_roundtrip() {
        let t = SynTarget { proto: PROTO_TCP, address: "example.com".into(), port: 443 };
        let encoded = t.encode();
        let decoded = SynTarget::decode(&encoded).unwrap();
        assert_eq!(decoded.proto, PROTO_TCP);
        assert_eq!(decoded.address, "example.com");
        assert_eq!(decoded.port, 443);
    }

    #[test]
    fn ack_roundtrip() {
        let f = Frame::ack(1, 100, 64);
        let info = AckInfo::decode(&f.payload).unwrap();
        assert_eq!(info.ack_seq, 100);
        assert_eq!(info.window, 64);
    }

    #[test]
    fn flags_composition() {
        let syn_ack = FLAG_SYN | FLAG_ACK;
        assert_eq!(syn_ack, 0x11);
        assert_ne!(syn_ack & FLAG_SYN, 0);
        assert_ne!(syn_ack & FLAG_ACK, 0);
        assert_eq!(syn_ack & FLAG_DATA, 0);
    }
}
