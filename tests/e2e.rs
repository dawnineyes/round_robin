//! End-to-end integration tests: real splitter ↔ reassembler over local
//! TCP, with a mini SOCKS5 proxy standing in for sing-box on both sides.
//!
//! Coverage targets (BUG_REVIEW F9):
//! - ordered delivery through 3 tunnels with one deliberately slow
//!   (forces DATA-before-SYN and FIN-before-SYN races — B1/B2)
//! - multi-client UDP relay with per-association conn_ids (B3/B19)

use round_robin::reassembler::{self, ReassemblerConfig};
use round_robin::splitter::{self, SplitterConfig, TunnelEndpoint};
use std::net::SocketAddr;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream, UdpSocket};

const CHUNK: usize = 4096;

fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .try_init();
}

// ── mini SOCKS5 CONNECT proxy ─────────────────────────────────────────

async fn run_socks5_proxy(listener: TcpListener, connect_delay: Duration) {
    loop {
        let (s, _) = match listener.accept().await {
            Ok(v) => v,
            Err(_) => continue,
        };
        tokio::spawn(async move {
            if let Err(e) = proxy_conn(s, connect_delay).await {
                eprintln!("proxy conn error: {e}");
            }
        });
    }
}

async fn proxy_conn(mut s: TcpStream, connect_delay: Duration) -> std::io::Result<()> {
    // greeting
    let mut hdr = [0u8; 2];
    s.read_exact(&mut hdr).await?;
    let mut methods = vec![0u8; hdr[1] as usize];
    s.read_exact(&mut methods).await?;
    s.write_all(&[0x05, 0x00]).await?;
    // request
    let mut req = [0u8; 4];
    s.read_exact(&mut req).await?;
    if req[1] != 0x01 {
        s.write_all(&[0x05, 0x07, 0x00, 0x01, 0, 0, 0, 0, 0, 0])
            .await?;
        return Ok(());
    }
    let (host, port) = read_socks_addr(&mut s, req[3]).await?;
    s.write_all(&[0x05, 0x00, 0x00, 0x01, 0, 0, 0, 0, 0, 0])
        .await?;
    if !connect_delay.is_zero() {
        tokio::time::sleep(connect_delay).await;
    }
    let mut up = TcpStream::connect((host.as_str(), port)).await?;
    tokio::io::copy_bidirectional(&mut s, &mut up).await?;
    Ok(())
}

async fn read_socks_addr(s: &mut TcpStream, atyp: u8) -> std::io::Result<(String, u16)> {
    let (host, mut port_buf) = match atyp {
        0x01 => {
            let mut b = [0u8; 4];
            s.read_exact(&mut b).await?;
            (format!("{}.{}.{}.{}", b[0], b[1], b[2], b[3]), [0u8; 2])
        }
        0x03 => {
            let mut len = [0u8; 1];
            s.read_exact(&mut len).await?;
            let mut b = vec![0u8; len[0] as usize];
            s.read_exact(&mut b).await?;
            (String::from_utf8_lossy(&b).into_owned(), [0u8; 2])
        }
        0x04 => {
            let mut b = [0u8; 16];
            s.read_exact(&mut b).await?;
            let mut segs = Vec::with_capacity(8);
            for c in b.chunks(2) {
                segs.push(format!("{:02x}{:02x}", c[0], c[1]));
            }
            (segs.join(":"), [0u8; 2])
        }
        _ => return Err(std::io::Error::other("bad atyp")),
    };
    s.read_exact(&mut port_buf).await?;
    Ok((host, u16::from_be_bytes(port_buf)))
}

// ── echo targets ──────────────────────────────────────────────────────

async fn run_tcp_target(listener: TcpListener) {
    loop {
        let (mut s, _) = match listener.accept().await {
            Ok(v) => v,
            Err(_) => continue,
        };
        tokio::spawn(async move {
            let mut buf = vec![0u8; 65536];
            let mut got = Vec::new();
            loop {
                match s.read(&mut buf).await {
                    Ok(0) | Err(_) => break,
                    Ok(n) => got.extend_from_slice(&buf[..n]),
                }
            }
            // Response: echo with markers; only sent after EOF (FIN path).
            let mut resp = Vec::with_capacity(got.len() + 12);
            resp.extend_from_slice(b"PONG:");
            resp.extend_from_slice(&got);
            resp.extend_from_slice(b":DONE");
            let _ = s.write_all(&resp).await;
        });
    }
}

async fn run_udp_target(sock: UdpSocket) {
    let mut buf = vec![0u8; 65535];
    loop {
        let (n, src) = match sock.recv_from(&mut buf).await {
            Ok(v) => v,
            Err(_) => continue,
        };
        let mut resp = Vec::with_capacity(n + 8);
        resp.extend_from_slice(b"PONGUDP:");
        resp.extend_from_slice(&buf[..n]);
        let _ = sock.send_to(&resp, src).await;
    }
}

// ── SOCKS5 client helpers ─────────────────────────────────────────────

/// CI-scheduling guard: the splitter binds its SOCKS listener (and
/// registers its first tunnel) only after its tasks get scheduled — on
/// busy runners the client can race ahead.  Retry the whole handshake
/// for up to 10s: both ECONNREFUSED (listener not bound yet) and a
/// SOCKS failure reply (no live tunnels yet) are retryable.
async fn socks5_connect(proxy: SocketAddr, host: &str, port: u16) -> std::io::Result<TcpStream> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        match socks5_handshake(proxy, host, port).await {
            Ok(s) => return Ok(s),
            Err(e) => {
                if tokio::time::Instant::now() >= deadline {
                    return Err(e);
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }
}

async fn socks5_handshake(proxy: SocketAddr, host: &str, port: u16) -> std::io::Result<TcpStream> {
    let mut s = TcpStream::connect(proxy).await?;
    s.write_all(&[0x05, 0x01, 0x00]).await?;
    let mut resp = [0u8; 2];
    s.read_exact(&mut resp).await?;
    if resp != [0x05, 0x00] {
        return Err(std::io::Error::other(format!(
            "SOCKS5 auth failed: {:02x} {:02x}",
            resp[0], resp[1]
        )));
    }
    // CONNECT with domain address
    let hb = host.as_bytes();
    let mut req = vec![0x05, 0x01, 0x00, 0x03, hb.len() as u8];
    req.extend_from_slice(hb);
    req.extend_from_slice(&port.to_be_bytes());
    s.write_all(&req).await?;
    let mut head = [0u8; 4];
    s.read_exact(&mut head).await?;
    if head[1] != 0x00 {
        return Err(std::io::Error::other(format!(
            "SOCKS5 CONNECT failed: rep=0x{:02x}",
            head[1]
        )));
    }
    match head[3] {
        0x01 => {
            let mut rest = [0u8; 6];
            s.read_exact(&mut rest).await?;
        }
        0x04 => {
            let mut rest = [0u8; 18];
            s.read_exact(&mut rest).await?;
        }
        0x03 => {
            let mut len = [0u8; 1];
            s.read_exact(&mut len).await?;
            let mut rest = vec![0u8; len[0] as usize + 2];
            s.read_exact(&mut rest).await?;
        }
        other => return Err(std::io::Error::other(format!("bad reply atyp {other}"))),
    }
    Ok(s)
}

/// UDP ASSOCIATE with retry, then send one datagram; returns the
/// response payload.
async fn udp_associate_exchange(proxy: SocketAddr, target: SocketAddr, payload: &[u8]) -> Vec<u8> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let (ctrl, relay) = loop {
        match udp_associate(proxy).await {
            Ok(v) => break v,
            Err(e) => {
                if tokio::time::Instant::now() >= deadline {
                    panic!("UDP ASSOCIATE failed after retries: {e}");
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    };
    // Keep the control connection alive until the exchange is done.
    let _keepalive = ctrl;

    let sock = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    // SOCKS5 UDP datagram header: RSV(2) FRAG(1) ATYP(1) ADDR(4) PORT(2)
    let mut dgram = vec![0u8, 0, 0, 0x01];
    let ip: std::net::Ipv4Addr = match target.ip() {
        std::net::IpAddr::V4(v4) => v4,
        v6 => panic!("expected IPv4 target, got {v6}"),
    };
    dgram.extend_from_slice(&ip.octets());
    dgram.extend_from_slice(&target.port().to_be_bytes());
    dgram.extend_from_slice(payload);
    sock.send_to(&dgram, relay).await.unwrap();

    let mut buf = vec![0u8; 65535];
    let (n, _) = tokio::time::timeout(Duration::from_secs(10), sock.recv_from(&mut buf))
        .await
        .expect("UDP response timed out")
        .unwrap();
    // strip SOCKS5 UDP header (4 + 4 + 2 = 10 bytes for IPv4)
    buf[10..n].to_vec()
}

/// One UDP ASSOCIATE handshake; Err on any failure (retryable).
async fn udp_associate(proxy: SocketAddr) -> std::io::Result<(TcpStream, SocketAddr)> {
    let mut ctrl = TcpStream::connect(proxy).await?;
    ctrl.write_all(&[0x05, 0x01, 0x00]).await?;
    let mut resp = [0u8; 2];
    ctrl.read_exact(&mut resp).await?;
    if resp != [0x05, 0x00] {
        return Err(std::io::Error::other("SOCKS5 auth failed"));
    }
    // UDP ASSOCIATE, addr 0.0.0.0:0
    ctrl.write_all(&[0x05, 0x03, 0x00, 0x01, 0, 0, 0, 0, 0, 0])
        .await?;
    let mut head = [0u8; 4];
    ctrl.read_exact(&mut head).await?;
    if head[1] != 0x00 {
        return Err(std::io::Error::other(format!(
            "UDP ASSOCIATE failed: rep=0x{:02x}",
            head[1]
        )));
    }
    if head[3] != 0x01 {
        return Err(std::io::Error::other("unexpected bind atyp"));
    }
    let mut bind = [0u8; 6];
    ctrl.read_exact(&mut bind).await?;
    let relay = SocketAddr::from(([127, 0, 0, 1], u16::from_be_bytes([bind[4], bind[5]])));
    Ok((ctrl, relay))
}

// ── killable proxy (D1 test) ──────────────────────────────────────────

/// SOCKS5 CONNECT proxy that silently kills the tunnel after forwarding
/// `kill_after` upstream bytes (plus a short grace delay so the splitter
/// has frames queued on the link when it dies).
async fn run_kill_proxy(listener: TcpListener, kill_after: usize) {
    loop {
        let (s, _) = match listener.accept().await {
            Ok(v) => v,
            Err(_) => continue,
        };
        tokio::spawn(async move {
            if let Err(e) = kill_proxy_conn(s, kill_after).await {
                eprintln!("kill proxy conn error: {e}");
            }
        });
    }
}

async fn kill_proxy_conn(mut s: TcpStream, kill_after: usize) -> std::io::Result<()> {
    let mut hdr = [0u8; 2];
    s.read_exact(&mut hdr).await?;
    let mut methods = vec![0u8; hdr[1] as usize];
    s.read_exact(&mut methods).await?;
    s.write_all(&[0x05, 0x00]).await?;
    let mut req = [0u8; 4];
    s.read_exact(&mut req).await?;
    let (host, port) = read_socks_addr(&mut s, req[3]).await?;
    s.write_all(&[0x05, 0x00, 0x00, 0x01, 0, 0, 0, 0, 0, 0])
        .await?;
    let mut up = TcpStream::connect((host.as_str(), port)).await?;
    // Relay upstream until kill_after bytes, then die with frames queued.
    let mut buf = vec![0u8; 65536];
    let mut forwarded = 0usize;
    while forwarded < kill_after {
        let n = match s.read(&mut buf).await {
            Ok(0) | Err(_) => return Ok(()),
            Ok(n) => n,
        };
        up.write_all(&buf[..n]).await?;
        forwarded += n;
    }
    // Stop reading so the splitter's queue backs up, then drop everything.
    tokio::time::sleep(Duration::from_millis(500)).await;
    drop(s);
    drop(up);
    Ok(())
}

// ── infra helpers ─────────────────────────────────────────────────────

async fn reserve_ports(n: usize) -> Vec<u16> {
    let mut listeners = Vec::new();
    let mut ports = Vec::new();
    for _ in 0..n {
        let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        ports.push(l.local_addr().unwrap().port());
        listeners.push(l);
    }
    drop(listeners);
    ports
}

// ── tests ─────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tcp_e2e_ordered_delivery_with_slow_tunnel() {
    init_tracing();
    tokio::time::timeout(Duration::from_secs(40), async {
        // 3 tunnel ports on the reassembler
        let tports = reserve_ports(3).await;
        // fast + slow SOCKS5 proxies
        let fast_proxy = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let fast_proxy_addr = fast_proxy.local_addr().unwrap();
        let slow_proxy = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let slow_proxy_addr = slow_proxy.local_addr().unwrap();
        tokio::spawn(run_socks5_proxy(fast_proxy, Duration::ZERO));
        tokio::spawn(run_socks5_proxy(slow_proxy, Duration::from_millis(400)));
        // echo target
        let target_l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let target_addr = target_l.local_addr().unwrap();
        tokio::spawn(run_tcp_target(target_l));

        // splitter SOCKS5 listen port
        let splitter_port = reserve_ports(1).await[0];
        let splitter_addr: SocketAddr = format!("127.0.0.1:{splitter_port}").parse().unwrap();

        // reassembler (egress through the fast proxy)
        let reassembler_cfg = ReassemblerConfig {
            listen_ip: "127.0.0.1".parse().unwrap(),
            listen_ports: tports.clone(),
            local_target: fast_proxy_addr,
            chunk_size: CHUNK,
            data_send_timeout: Duration::from_secs(30),
            heartbeat_interval: Duration::from_secs(60),
        };
        tokio::spawn(async move {
            let _ = reassembler::run_reassembler(reassembler_cfg).await;
        });

        // splitter: tunnel 1 rides the SLOW proxy (forces DATA/FIN to
        // arrive before the SYN on the fast tunnels)
        let splitter_cfg = SplitterConfig {
            listen_addr: splitter_addr,
            tunnels: vec![
                TunnelEndpoint {
                    proxy: slow_proxy_addr,
                    target: "127.0.0.1".into(),
                    port: tports[0],
                },
                TunnelEndpoint {
                    proxy: fast_proxy_addr,
                    target: "127.0.0.1".into(),
                    port: tports[1],
                },
                TunnelEndpoint {
                    proxy: fast_proxy_addr,
                    target: "127.0.0.1".into(),
                    port: tports[2],
                },
            ],
            chunk_size: CHUNK,
            data_send_timeout: Duration::from_secs(30),
            heartbeat_interval: Duration::from_secs(60),
        };
        tokio::spawn(async move {
            let _ = splitter::run_splitter(splitter_cfg).await;
        });

        // Client: CONNECT through the splitter, write a large request,
        // half-close, then expect the full echoed response.
        let mut s = socks5_connect(splitter_addr, "127.0.0.1", target_addr.port())
            .await
            .unwrap();
        let request: Vec<u8> = (0..CHUNK * 16).map(|i| (i % 251) as u8).collect();
        s.write_all(&request).await.unwrap();
        s.shutdown().await.unwrap();

        let mut got = Vec::new();
        let mut buf = vec![0u8; 65536];
        loop {
            match s.read(&mut buf).await {
                Ok(0) | Err(_) => break,
                Ok(n) => got.extend_from_slice(&buf[..n]),
            }
        }
        let mut expected = Vec::new();
        expected.extend_from_slice(b"PONG:");
        expected.extend_from_slice(&request);
        expected.extend_from_slice(b":DONE");
        assert_eq!(
            got.len(),
            expected.len(),
            "response length mismatch (truncation?)"
        );
        assert_eq!(got, expected, "response corrupted (reorder bug?)");
    })
    .await
    .expect("TCP e2e test timed out");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tunnel_death_resets_affected_connection_fast() {
    init_tracing();
    tokio::time::timeout(Duration::from_secs(60), async {
        let tports = reserve_ports(2).await;
        let proxy_a = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let proxy_a_addr = proxy_a.local_addr().unwrap();
        let proxy_b = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let proxy_b_addr = proxy_b.local_addr().unwrap();
        tokio::spawn(run_socks5_proxy(proxy_a, Duration::ZERO));
        // Tunnel B dies after 16 KB upstream — mid-transfer.
        tokio::spawn(run_kill_proxy(proxy_b, 16 * 1024));

        let target_l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let target_addr = target_l.local_addr().unwrap();
        tokio::spawn(run_tcp_target(target_l));

        let splitter_port = reserve_ports(1).await[0];
        let splitter_addr: SocketAddr = format!("127.0.0.1:{splitter_port}").parse().unwrap();

        let reassembler_cfg = ReassemblerConfig {
            listen_ip: "127.0.0.1".parse().unwrap(),
            listen_ports: tports.clone(),
            local_target: proxy_a_addr,
            chunk_size: CHUNK,
            data_send_timeout: Duration::from_secs(30),
            heartbeat_interval: Duration::from_secs(60),
        };
        tokio::spawn(async move {
            let _ = reassembler::run_reassembler(reassembler_cfg).await;
        });

        let splitter_cfg = SplitterConfig {
            listen_addr: splitter_addr,
            tunnels: vec![
                TunnelEndpoint {
                    proxy: proxy_a_addr,
                    target: "127.0.0.1".into(),
                    port: tports[0],
                },
                TunnelEndpoint {
                    proxy: proxy_b_addr,
                    target: "127.0.0.1".into(),
                    port: tports[1],
                },
            ],
            chunk_size: CHUNK,
            data_send_timeout: Duration::from_secs(30),
            heartbeat_interval: Duration::from_secs(60),
        };
        tokio::spawn(async move {
            let _ = splitter::run_splitter(splitter_cfg).await;
        });

        let s = socks5_connect(splitter_addr, "127.0.0.1", target_addr.port())
            .await
            .unwrap();
        // Pump data through both tunnels until tunnel B dies; D1 must
        // reset the connection instead of stalling it forever.
        let chunk: Vec<u8> = (0..CHUNK).map(|i| (i % 251) as u8).collect();
        let (mut read_half, mut write_half) = s.into_split();
        let writer = tokio::spawn(async move {
            loop {
                if write_half.write_all(&chunk).await.is_err() {
                    break;
                }
            }
        });
        let reader = tokio::spawn(async move {
            let mut buf = vec![0u8; 65536];
            loop {
                match read_half.read(&mut buf).await {
                    Ok(0) | Err(_) => break,
                    Ok(_) => {}
                }
            }
        });
        // The connection must terminate (not hang) within 20s.
        let _ = tokio::time::timeout(Duration::from_secs(20), writer).await;
        let _ = tokio::time::timeout(Duration::from_secs(20), reader).await;
    })
    .await
    .expect("D1 tunnel-death test timed out (connection stalled instead of resetting)");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn client_keeps_sending_after_remote_fin() {
    init_tracing();
    tokio::time::timeout(Duration::from_secs(60), async {
        let tports = reserve_ports(2).await;
        let proxy = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let proxy_addr = proxy.local_addr().unwrap();
        tokio::spawn(run_socks5_proxy(proxy, Duration::ZERO));

        // D3 target: send PART1, half-close the write side, then keep
        // reading.  Whatever arrives after the half-close is reported
        // back to the test through a channel (TCP can't send more after
        // shutdown(write), so the channel is the observable).
        let (report_tx, mut report_rx) = tokio::sync::mpsc::channel::<Vec<u8>>(1);
        let target_l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let target_addr = target_l.local_addr().unwrap();
        tokio::spawn(async move {
            loop {
                let (s, _) = match target_l.accept().await {
                    Ok(v) => v,
                    Err(_) => continue,
                };
                let tx = report_tx.clone();
                tokio::spawn(async move {
                    let (mut rd, mut wr) = s.into_split();
                    let mut head = [0u8; 5];
                    if rd.read_exact(&mut head).await.is_err() {
                        return;
                    }
                    let mut resp = Vec::new();
                    resp.extend_from_slice(b"PART1:");
                    resp.extend_from_slice(&head);
                    if wr.write_all(&resp).await.is_err() {
                        return;
                    }
                    // Half-close the write side → remote FIN path.
                    let _ = wr.shutdown().await;
                    // Keep reading: the client's post-FIN data must still
                    // arrive (D3), followed by EOF when the write half of
                    // the egress closes.
                    let mut tail = Vec::new();
                    let mut buf = vec![0u8; 65536];
                    loop {
                        match rd.read(&mut buf).await {
                            Ok(0) | Err(_) => break,
                            Ok(n) => tail.extend_from_slice(&buf[..n]),
                        }
                    }
                    let _ = tx.send(tail).await;
                });
            }
        });

        let splitter_port = reserve_ports(1).await[0];
        let splitter_addr: SocketAddr = format!("127.0.0.1:{splitter_port}").parse().unwrap();

        let reassembler_cfg = ReassemblerConfig {
            listen_ip: "127.0.0.1".parse().unwrap(),
            listen_ports: tports.clone(),
            local_target: proxy_addr,
            chunk_size: CHUNK,
            data_send_timeout: Duration::from_secs(30),
            heartbeat_interval: Duration::from_secs(60),
        };
        tokio::spawn(async move {
            let _ = reassembler::run_reassembler(reassembler_cfg).await;
        });

        let splitter_cfg = SplitterConfig {
            listen_addr: splitter_addr,
            tunnels: tports
                .iter()
                .map(|p| TunnelEndpoint {
                    proxy: proxy_addr,
                    target: "127.0.0.1".into(),
                    port: *p,
                })
                .collect(),
            chunk_size: CHUNK,
            data_send_timeout: Duration::from_secs(30),
            // B21 regression: 2s heartbeat so the sweep would fire while
            // the client is still sending — pre-fix, the first sweep
            // after the FIN killed the conn and truncated the tail.
            heartbeat_interval: Duration::from_secs(2),
        };
        tokio::spawn(async move {
            let _ = splitter::run_splitter(splitter_cfg).await;
        });

        let mut s = socks5_connect(splitter_addr, "127.0.0.1", target_addr.port())
            .await
            .unwrap();
        s.write_all(b"hello").await.unwrap();
        // Read exactly PART1:hello — the remote FIN follows it through
        // the chain.
        let mut part1 = vec![0u8; 11];
        s.read_exact(&mut part1).await.unwrap();
        assert_eq!(&part1[..], b"PART1:hello");
        // Give the FIN time to propagate to the splitter, then keep
        // sending ACROSS MULTIPLE heartbeat cycles (B21 regression:
        // pre-fix, the first heartbeat after the FIN swept the conn and
        // truncated this tail; heartbeat_interval here is 2s).
        let mut expected_tail = Vec::new();
        for _ in 0..6 {
            tokio::time::sleep(Duration::from_millis(900)).await;
            s.write_all(b"world").await.unwrap();
            expected_tail.extend_from_slice(b"world");
        }
        s.shutdown().await.unwrap();
        let mut buf = vec![0u8; 65536];
        loop {
            match s.read(&mut buf).await {
                Ok(0) | Err(_) => break,
                Ok(_) => {}
            }
        }
        // The target must have received the post-FIN data.
        let tail = tokio::time::timeout(Duration::from_secs(10), report_rx.recv())
            .await
            .expect("target never reported its received tail")
            .expect("target task died without reporting");
        assert_eq!(
            &tail[..],
            &expected_tail[..],
            "client data after remote FIN was lost (B21 sweep?)"
        );
    })
    .await
    .expect("D3 half-close test timed out");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn udp_e2e_two_clients_relay_concurrently() {
    init_tracing();
    tokio::time::timeout(Duration::from_secs(40), async {
        let tports = reserve_ports(2).await;
        let fast_proxy = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let fast_proxy_addr = fast_proxy.local_addr().unwrap();
        tokio::spawn(run_socks5_proxy(fast_proxy, Duration::ZERO));

        // UDP echo target
        let udp_target = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let udp_target_addr = udp_target.local_addr().unwrap();
        tokio::spawn(run_udp_target(udp_target));

        let splitter_port = reserve_ports(1).await[0];
        let splitter_addr: SocketAddr = format!("127.0.0.1:{splitter_port}").parse().unwrap();

        let reassembler_cfg = ReassemblerConfig {
            listen_ip: "127.0.0.1".parse().unwrap(),
            listen_ports: tports.clone(),
            local_target: fast_proxy_addr,
            chunk_size: CHUNK,
            data_send_timeout: Duration::from_secs(30),
            heartbeat_interval: Duration::from_secs(60),
        };
        tokio::spawn(async move {
            let _ = reassembler::run_reassembler(reassembler_cfg).await;
        });

        let splitter_cfg = SplitterConfig {
            listen_addr: splitter_addr,
            tunnels: tports
                .iter()
                .map(|p| TunnelEndpoint {
                    proxy: fast_proxy_addr,
                    target: "127.0.0.1".into(),
                    port: *p,
                })
                .collect(),
            chunk_size: CHUNK,
            data_send_timeout: Duration::from_secs(30),
            heartbeat_interval: Duration::from_secs(60),
        };
        tokio::spawn(async move {
            let _ = splitter::run_splitter(splitter_cfg).await;
        });

        // Two concurrent clients — the single-UDP_CONN_ID design would
        // reject the second association (BUG-19 regression test).
        let (a, b) = tokio::join!(
            udp_associate_exchange(splitter_addr, udp_target_addr, b"hello-from-A"),
            udp_associate_exchange(splitter_addr, udp_target_addr, b"hello-from-B"),
        );
        let mut ea = Vec::new();
        ea.extend_from_slice(b"PONGUDP:");
        ea.extend_from_slice(b"hello-from-A");
        let mut eb = Vec::new();
        eb.extend_from_slice(b"PONGUDP:");
        eb.extend_from_slice(b"hello-from-B");
        assert_eq!(a, ea);
        assert_eq!(b, eb);
    })
    .await
    .expect("UDP e2e test timed out");
}
