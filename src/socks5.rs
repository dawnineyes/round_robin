use anyhow::{bail, Result};
use std::net::SocketAddr;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

// ── SOCKS5 constants ──────────────────────────────────────────────────

const SOCKS_VERSION: u8 = 0x05;
const AUTH_NONE: u8 = 0x00;
const CMD_CONNECT: u8 = 0x01;
const ATYP_IPV4: u8 = 0x01;
const ATYP_DOMAIN: u8 = 0x03;
const ATYP_IPV6: u8 = 0x04;
const REP_SUCCESS: u8 = 0x00;
const REP_CMD_NOT_SUPPORTED: u8 = 0x07;

// ── Server-side: accept a SOCKS5 client ───────────────────────────────

/// Result of a SOCKS5 server-side handshake: the target the client wants
/// to CONNECT to, and the stream positioned after the reply.
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
/// 2. Read CONNECT request → parse target
/// 3. Reply success
pub async fn socks5_server_accept(mut stream: TcpStream) -> Result<Socks5Accept> {
    // 1. Greeting
    let mut hdr = [0u8; 2];
    stream.read_exact(&mut hdr).await?;
    if hdr[0] != SOCKS_VERSION {
        bail!("not SOCKS5 (version {})", hdr[0]);
    }
    let nmethods = hdr[1] as usize;
    let mut methods = vec![0u8; nmethods.min(16)];
    stream.read_exact(&mut methods).await?;
    // Drain excess methods
    if nmethods > 16 {
        let mut drain = vec![0u8; nmethods - 16];
        stream.read_exact(&mut drain).await?;
    }
    if !methods.contains(&AUTH_NONE) {
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
    if req[1] != CMD_CONNECT {
        // Reply: command not supported
        let rep = [SOCKS_VERSION, REP_CMD_NOT_SUPPORTED, 0x00, ATYP_IPV4, 0, 0, 0, 0, 0, 0];
        stream.write_all(&rep).await?;
        bail!("only CONNECT supported, got cmd={}", req[1]);
    }
    let target = read_address(&mut stream, req[3]).await?;

    // 3. Reply success (bind addr = 0.0.0.0:0)
    let rep = [SOCKS_VERSION, REP_SUCCESS, 0x00, ATYP_IPV4, 0, 0, 0, 0, 0, 0];
    stream.write_all(&rep).await?;

    Ok(Socks5Accept { target, stream })
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
