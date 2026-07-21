use anyhow::{bail, Result};
use bytes::{BufMut, Bytes, BytesMut};
use std::net::SocketAddr;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpStream, UdpSocket};

// ── SOCKS5 constants ──────────────────────────────────────────────────

const SOCKS_VERSION: u8 = 0x05;
const AUTH_NONE: u8 = 0x00;
const CMD_CONNECT: u8 = 0x01;
const CMD_UDP_ASSOCIATE: u8 = 0x03;
const ATYP_IPV4: u8 = 0x01;
const ATYP_DOMAIN: u8 = 0x03;
const ATYP_IPV6: u8 = 0x04;
const REP_SUCCESS: u8 = 0x00;
const REP_CMD_NOT_SUPPORTED: u8 = 0x07;

// ── Server-side: accept a SOCKS5 client ───────────────────────────────

/// Result of a SOCKS5 server-side handshake.
pub enum Socks5Result {
    Connect(Socks5Accept),
    UdpAssociate { stream: TcpStream, relay: UdpSocket },
}

/// Result of a SOCKS5 CONNECT: the target the client wants and the stream.
pub struct Socks5Accept {
    pub target: TargetAddr,
    pub stream: TcpStream,
}

#[derive(Debug, Clone)]
pub struct TargetAddr {
    pub address: String,
    pub port: u16,
}

/// Perform SOCKS5 server-side handshake on `stream`:
/// 1. Read greeting → negotiate no-auth
/// 2. Read request → if CONNECT, parse target; if UDP ASSOCIATE, create relay
/// 3. Reply success
pub async fn socks5_server_accept(mut stream: TcpStream) -> Result<Socks5Result> {
    // 1. Greeting
    let mut hdr = [0u8; 2];
    stream.read_exact(&mut hdr).await?;
    if hdr[0] != SOCKS_VERSION {
        bail!("not SOCKS5 (version {})", hdr[0]);
    }
    let nmethods = hdr[1] as usize;
    let mut methods = [0u8; 256];
    stream.read_exact(&mut methods[..nmethods]).await?;
    if !methods[..nmethods].contains(&AUTH_NONE) {
        stream.write_all(&[SOCKS_VERSION, 0xFF]).await?;
        bail!("client requires auth, only no-auth supported");
    }
    stream.write_all(&[SOCKS_VERSION, AUTH_NONE]).await?;

    // 2. Request
    let mut req = [0u8; 4];
    stream.read_exact(&mut req).await?;
    if req[0] != SOCKS_VERSION {
        bail!("bad SOCKS5 request version");
    }

    match req[1] {
        CMD_CONNECT => {
            let target = read_address(&mut stream, req[3]).await?;
            let rep = [SOCKS_VERSION, REP_SUCCESS, 0x00, ATYP_IPV4, 0, 0, 0, 0, 0, 0];
            stream.write_all(&rep).await?;
            Ok(Socks5Result::Connect(Socks5Accept { target, stream }))
        }
        CMD_UDP_ASSOCIATE => {
            let client_addr = read_address(&mut stream, req[3]).await?;
            // Bind a UDP relay on the same IP the TCP listener uses
            let local = stream.local_addr()?;
            let relay = UdpSocket::bind((local.ip(), 0u16)).await?;
            let relay_addr = relay.local_addr()?;
            let rep = encode_reply(REP_SUCCESS, relay_addr);
            stream.write_all(&rep).await?;
            let _ = client_addr; // ponytail: client_addr unused for single-client setup
            Ok(Socks5Result::UdpAssociate { stream, relay })
        }
        other => {
            let rep = [SOCKS_VERSION, REP_CMD_NOT_SUPPORTED, 0x00, ATYP_IPV4, 0, 0, 0, 0, 0, 0];
            stream.write_all(&rep).await?;
            bail!("unsupported CMD={other}, only CONNECT/UDP_ASSOCIATE supported");
        }
    }
}

/// Encode a SOCKS5 reply with the given bind address.
fn encode_reply(rep: u8, addr: SocketAddr) -> Vec<u8> {
    let mut v = vec![SOCKS_VERSION, rep, 0x00];
    match addr {
        SocketAddr::V4(a) => {
            v.push(ATYP_IPV4);
            v.extend_from_slice(&a.ip().octets());
        }
        SocketAddr::V6(a) => {
            v.push(ATYP_IPV6);
            v.extend_from_slice(&a.ip().octets());
        }
    }
    v.extend_from_slice(&addr.port().to_be_bytes());
    v
}

/// SOCKS5 server handshake for tunnel links: accept no-auth, accept any
/// CONNECT target, reply success. Returns the stream for frame I/O.
pub async fn socks5_accept_tunnel(mut stream: TcpStream) -> Result<TcpStream> {
    // Greeting
    let mut hdr = [0u8; 2];
    stream.read_exact(&mut hdr).await?;
    if hdr[0] != SOCKS_VERSION {
        bail!("not SOCKS5 (version {})", hdr[0]);
    }
    let nmethods = hdr[1] as usize;
    let mut methods = [0u8; 256];
    stream.read_exact(&mut methods[..nmethods]).await?;
    if !methods[..nmethods].contains(&AUTH_NONE) {
        stream.write_all(&[SOCKS_VERSION, 0xFF]).await?;
        bail!("tunnel requires auth, only no-auth supported");
    }
    stream.write_all(&[SOCKS_VERSION, AUTH_NONE]).await?;

    // Request — read it but ignore the target
    let mut req = [0u8; 4];
    stream.read_exact(&mut req).await?;
    if req[0] != SOCKS_VERSION {
        bail!("bad SOCKS5 request version");
    }
    drain_address(&mut stream, req[3]).await?;

    // Reply success
    let rep = [SOCKS_VERSION, REP_SUCCESS, 0x00, ATYP_IPV4, 0, 0, 0, 0, 0, 0];
    stream.write_all(&rep).await?;

    Ok(stream)
}

// ── Client-side: connect through a SOCKS5 proxy ───────────────────────

/// SOCKS5 CONNECT through `proxy` to `target`. Returns the stream ready
/// for raw data transfer.
pub async fn socks5_client_connect(
    proxy: SocketAddr,
    target: &str,
    port: u16,
) -> Result<TcpStream> {
    let mut stream = TcpStream::connect(proxy).await?;

    // Greeting
    stream.write_all(&[SOCKS_VERSION, 0x01, AUTH_NONE]).await?;
    let mut resp = [0u8; 2];
    stream.read_exact(&mut resp).await?;
    if resp[0] != SOCKS_VERSION {
        bail!("proxy not SOCKS5 (version {})", resp[0]);
    }
    if resp[1] != AUTH_NONE {
        bail!("proxy requires auth method 0x{:02x}", resp[1]);
    }

    // Request: CONNECT to target
    let addr_bytes = encode_address(target, port);
    let mut req = vec![SOCKS_VERSION, CMD_CONNECT, 0x00];
    req.extend_from_slice(&addr_bytes);
    stream.write_all(&req).await?;

    // Reply
    let mut head = [0u8; 4];
    stream.read_exact(&mut head).await?;
    if head[0] != SOCKS_VERSION {
        bail!("bad SOCKS5 reply version");
    }
    if head[1] != REP_SUCCESS {
        bail!("SOCKS5 CONNECT to {target}:{port} failed, rep=0x{:02x}", head[1]);
    }
    // Drain bind address
    drain_address(&mut stream, head[3]).await?;

    Ok(stream)
}

// ── Address helpers ───────────────────────────────────────────────────

async fn read_address(stream: &mut TcpStream, atyp: u8) -> Result<TargetAddr> {
    match atyp {
        ATYP_IPV4 => {
            let mut buf = [0u8; 6];
            stream.read_exact(&mut buf).await?;
            let addr = format!("{}.{}.{}.{}", buf[0], buf[1], buf[2], buf[3]);
            let port = u16::from_be_bytes([buf[4], buf[5]]);
            Ok(TargetAddr { address: addr, port })
        }
        ATYP_DOMAIN => {
            let mut len = [0u8; 1];
            stream.read_exact(&mut len).await?;
            let mut addr = vec![0u8; len[0] as usize];
            stream.read_exact(&mut addr).await?;
            let mut port_buf = [0u8; 2];
            stream.read_exact(&mut port_buf).await?;
            let address = String::from_utf8(addr)?;
            let port = u16::from_be_bytes(port_buf);
            Ok(TargetAddr { address, port })
        }
        ATYP_IPV6 => {
            let mut buf = [0u8; 18];
            stream.read_exact(&mut buf).await?;
            // Format as IPv6 string
            let segments: Vec<String> = buf[..16]
                .chunks(2)
                .map(|c| format!("{:02x}{:02x}", c[0], c[1]))
                .collect();
            let addr = segments.join(":");
            let port = u16::from_be_bytes([buf[16], buf[17]]);
            Ok(TargetAddr { address: addr, port })
        }
        other => bail!("unsupported address type: {other}"),
    }
}

async fn drain_address(stream: &mut TcpStream, atyp: u8) -> Result<()> {
    match atyp {
        ATYP_IPV4 => {
            let mut buf = [0u8; 6];
            stream.read_exact(&mut buf).await?;
        }
        ATYP_DOMAIN => {
            let mut len = [0u8; 1];
            stream.read_exact(&mut len).await?;
            let mut buf = vec![0u8; len[0] as usize + 2];
            stream.read_exact(&mut buf).await?;
        }
        ATYP_IPV6 => {
            let mut buf = [0u8; 18];
            stream.read_exact(&mut buf).await?;
        }
        _ => bail!("unsupported address type: {atyp}"),
    }
    Ok(())
}

fn encode_address(host: &str, port: u16) -> Vec<u8> {
    // Try parse as IPv4
    if let Ok(ip) = host.parse::<std::net::Ipv4Addr>() {
        let mut v = Vec::with_capacity(7);
        v.push(ATYP_IPV4);
        v.extend_from_slice(&ip.octets());
        v.extend_from_slice(&port.to_be_bytes());
        return v;
    }
    // Try parse as IPv6
    if let Ok(ip) = host.parse::<std::net::Ipv6Addr>() {
        let mut v = Vec::with_capacity(19);
        v.push(ATYP_IPV6);
        v.extend_from_slice(&ip.octets());
        v.extend_from_slice(&port.to_be_bytes());
        return v;
    }
    // Domain name
    let name = host.as_bytes();
    let mut v = Vec::with_capacity(1 + 1 + name.len() + 2);
    v.push(ATYP_DOMAIN);
    v.push(name.len() as u8);
    v.extend_from_slice(name);
    v.extend_from_slice(&port.to_be_bytes());
    v
}

// ── UDP datagram helpers (RFC 1928 §7) ────────────────────────────────

/// Encode a SOCKS5 UDP datagram: RSV(2) + FRAG(1) + ATYP(1) + ADDR(var) + PORT(2) + DATA(var)
pub fn encode_udp_datagram(target: &TargetAddr, data: &[u8]) -> Bytes {
    let addr = encode_address(&target.address, target.port);
    let mut buf = BytesMut::with_capacity(3 + addr.len() + data.len());
    buf.put_u16(0); // RSV
    buf.put_u8(0);  // FRAG (fragmentation not supported)
    buf.put_slice(&addr);
    buf.put_slice(data);
    buf.freeze()
}

/// Decode a SOCKS5 UDP datagram. Returns (target, data).
pub fn decode_udp_datagram(payload: &[u8]) -> Result<(TargetAddr, Bytes)> {
    if payload.len() < 4 {
        bail!("UDP datagram too short");
    }
    let frag = payload[2];
    if frag != 0 {
        bail!("UDP fragmentation not supported");
    }
    let atyp = payload[3];
    // Reuse address parser on a cursor-like slice
    let addr_data = &payload[3..];
    let (addr, port, consumed) = parse_udp_address(atyp, addr_data)?;
    let data = Bytes::copy_from_slice(&payload[3 + consumed..]);
    Ok((TargetAddr { address: addr, port }, data))
}

fn parse_udp_address(atyp: u8, data: &[u8]) -> Result<(String, u16, usize)> {
    match atyp {
        ATYP_IPV4 => {
            if data.len() < 7 {
                bail!("truncated IPv4 in UDP datagram");
            }
            let addr = format!("{}.{}.{}.{}", data[1], data[2], data[3], data[4]);
            let port = u16::from_be_bytes([data[5], data[6]]);
            Ok((addr, port, 7))
        }
        ATYP_DOMAIN => {
            if data.len() < 2 {
                bail!("truncated domain in UDP datagram");
            }
            let len = data[1] as usize;
            if data.len() < 4 + len {
                bail!("truncated domain data");
            }
            let addr = String::from_utf8(data[2..2 + len].to_vec())?;
            let port = u16::from_be_bytes([data[2 + len], data[3 + len]]);
            Ok((addr, port, 4 + len))
        }
        ATYP_IPV6 => {
            if data.len() < 19 {
                bail!("truncated IPv6 in UDP datagram");
            }
            let segs: Vec<String> = data[1..17]
                .chunks(2)
                .map(|c| format!("{:02x}{:02x}", c[0], c[1]))
                .collect();
            let addr = segs.join(":");
            let port = u16::from_be_bytes([data[17], data[18]]);
            Ok((addr, port, 19))
        }
        _ => bail!("unsupported ATYP in UDP datagram: {atyp}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn udp_datagram_ipv4_roundtrip() {
        let target = TargetAddr { address: "1.2.3.4".into(), port: 53 };
        let data = b"hello udp";
        let encoded = encode_udp_datagram(&target, data);
        let (decoded_target, decoded_data) = decode_udp_datagram(&encoded).unwrap();
        assert_eq!(decoded_target.address, "1.2.3.4");
        assert_eq!(decoded_target.port, 53);
        assert_eq!(&decoded_data[..], b"hello udp");
    }

    #[test]
    fn udp_datagram_domain_roundtrip() {
        let target = TargetAddr { address: "dns.google".into(), port: 53 };
        let data = vec![0u8; 32];
        let encoded = encode_udp_datagram(&target, &data);
        let (decoded_target, decoded_data) = decode_udp_datagram(&encoded).unwrap();
        assert_eq!(decoded_target.address, "dns.google");
        assert_eq!(decoded_target.port, 53);
        assert_eq!(decoded_data.len(), 32);
    }
}
