#![windows_subsystem = "windows"]

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{self, AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Semaphore;

const LISTEN: &str = "127.0.0.1:52030";
const BACKEND_BASE: u16 = 52031;
const BACKEND_COUNT: usize = 9;

// 生产级硬防护参数
const MAX_CONNECTIONS: usize = 5000;       
const CLIENT_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10); // 仅限握手与后端连接建立超时
const BACKEND_CONNECT_TIMEOUT: Duration = Duration::from_secs(3);
const HEALTH_CHECK_TIMEOUT: Duration = Duration::from_millis(1500);   

struct BackendManager {
    counter: AtomicUsize,
    health_status: [AtomicBool; BACKEND_COUNT],
}

impl BackendManager {
    fn new() -> Self {
        let health_status = std::array::from_fn(|_| AtomicBool::new(true));
        Self {
            counter: AtomicUsize::new(0),
            health_status,
        }
    }

    fn select_backend(&self) -> Option<u16> {
        let idx = self.counter.fetch_add(1, Ordering::Relaxed);

        for i in 0..BACKEND_COUNT {
            let target_idx = (idx.wrapping_add(i)) % BACKEND_COUNT;
            if self.health_status[target_idx].load(Ordering::Acquire) {
                return Some(BACKEND_BASE + target_idx as u16);
            }
        }
        None
    }

    fn set_health(&self, port: u16, alive: bool) {
        if port >= BACKEND_BASE && (port as usize) < (BACKEND_BASE as usize + BACKEND_COUNT) {
            let idx = (port - BACKEND_BASE) as usize;
            self.health_status[idx].store(alive, Ordering::Release);
        }
    }
}

#[tokio::main]
async fn main() -> io::Result<()> {
    let listener = TcpListener::bind(LISTEN).await?;
    let manager = Arc::new(BackendManager::new());
    let semaphore = Arc::new(Semaphore::new(MAX_CONNECTIONS));

    // --- 1. SOCKS5 健康检查 ---
    let manager_hc = manager.clone();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(5)).await;
            for i in 0..BACKEND_COUNT {
                let port = BACKEND_BASE + i as u16;

                let is_alive = match tokio::time::timeout(
                    HEALTH_CHECK_TIMEOUT,
                    check_backend_socks5_handshake(port)
                ).await {
                    Ok(Ok(_)) => true,
                    _ => false,
                };

                let current_state = manager_hc.health_status[i].load(Ordering::Acquire);
                if current_state != is_alive {
                    manager_hc.set_health(port, is_alive);
                }
            }
        }
    });

    // --- 2. 主 Accept 循环 ---
    while let Ok((stream, client_addr)) = listener.accept().await {
        let _ = stream.set_nodelay(true);

        // 限制最大并发数
        let permit = match semaphore.clone().acquire_owned().await {
            Ok(p) => p,
            Err(_) => continue,
        };

        let manager_clone = manager.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_socks5_client(stream, manager_clone).await {
                if e.kind() != io::ErrorKind::UnexpectedEof {
                    eprintln!("ERROR client={} error={} action=disconnect", client_addr, e);
                }
            }
            // 确保只有当 handle_socks5_client 彻底退出（包括数据传输完毕）后，才释放信号量
            drop(permit);
        });
    }

    Ok(())
}

async fn check_backend_socks5_handshake(port: u16) -> io::Result<()> {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).await?;
    let _ = stream.set_nodelay(true);
    stream.write_all(&[0x05, 0x01, 0x00]).await?;
    let mut resp = [0u8; 2];
    stream.read_exact(&mut resp).await?;
    if resp[0] != 0x05 || resp[1] != 0x00 {
        return Err(io::Error::new(io::ErrorKind::ConnectionRefused, "SOCKS5 握手失败"));
    }
    let _ = stream.shutdown().await;
    Ok(())
}

async fn handle_socks5_client(mut client: TcpStream, manager: Arc<BackendManager>) -> io::Result<()> {
    // --- 阶段 1：握手与建立后端连接（受限时保护，防止慢速连接攻击和挂死） ---
    let backend = tokio::time::timeout(CLIENT_HANDSHAKE_TIMEOUT, async {
        // 1.1 SOCKS5 认证协商
        let mut header = [0u8; 2];
        client.read_exact(&mut header).await?;
        if header[0] != 0x05 {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "非 SOCKS5 协议"));
        }
        
        let nmethods = header[1] as usize;
        if nmethods > 16 {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "nmethods 过大"));
        }
        let mut methods = vec![0u8; nmethods];
        client.read_exact(&mut methods).await?;

        if !methods.contains(&0x00) {
            client.write_all(&[0x05, 0xFF]).await?;
            return Err(io::Error::new(io::ErrorKind::PermissionDenied, "客户端不支持无认证模式"));
        }
        client.write_all(&[0x05, 0x00]).await?;

        // 1.2 读取并解析 SOCKS5 CONNECT 请求
        let mut req_header = [0u8; 4];
        client.read_exact(&mut req_header).await?;
        if req_header[0] != 0x05 {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "损坏的 SOCKS5 请求包"));
        }
        if req_header[1] != 0x01 { 
            let _ = client.write_all(&[0x05, 0x07, 0x00, 0x01, 0, 0, 0, 0, 0, 0]).await;
            return Err(io::Error::new(io::ErrorKind::Unsupported, "仅支持 CONNECT 命令"));
        }

        let atyp = req_header[3];
        let bnd_addr_payload = read_socks5_address_payload(&mut client, atyp).await?;

        // 1.3 路由与高可用容错机制
        let mut retry_count = 0;
        let mut backend_stream = None;

        while retry_count < 2 {
            if let Some(port) = manager.select_backend() {
                match tokio::time::timeout(
                    BACKEND_CONNECT_TIMEOUT,
                    TcpStream::connect(("127.0.0.1", port))
                ).await {
                    Ok(Ok(stream)) => {
                        backend_stream = Some(stream);
                        break;
                    }
                    _ => {
                        manager.set_health(port, false);
                        retry_count += 1;
                    }
                }
            } else {
                break;
            }
        }

        let mut backend = match backend_stream {
            Some(s) => s,
            None => {
                let _ = client.write_all(&[0x05, 0x03, 0x00, 0x01, 0, 0, 0, 0, 0, 0]).await;
                return Err(io::Error::new(io::ErrorKind::NotConnected, "所有上游后端均不可用或连接超时"));
            }
        };
        let _ = backend.set_nodelay(true);

        // 1.4 与后端握手并发送 CONNECT 请求
        backend.write_all(&[0x05, 0x01, 0x00]).await?;
        let mut backend_auth_resp = [0u8; 2];
        backend.read_exact(&mut backend_auth_resp).await?;
        if backend_auth_resp[0] != 0x05 || backend_auth_resp[1] != 0x00 {
            let _ = client.write_all(&[0x05, 0x05, 0x00, 0x01, 0, 0, 0, 0, 0, 0]).await;
            return Err(io::Error::new(io::ErrorKind::ConnectionRefused, "后端代理拒绝无认证"));
        }

        backend.write_all(&req_header).await?;
        backend.write_all(&bnd_addr_payload).await?;

        let mut backend_conn_resp = [0u8; 4];
        backend.read_exact(&mut backend_conn_resp).await?;
        let resp_atyp = backend_conn_resp[3];
        let resp_addr_payload = read_socks5_address_payload(&mut backend, resp_atyp).await?;

        client.write_all(&backend_conn_resp).await?;
        client.write_all(&resp_addr_payload).await?;

        if backend_conn_resp[1] != 0x00 {
            return Err(io::Error::new(io::ErrorKind::ConnectionRefused, format!("后端连接目标失败码: {}", backend_conn_resp[1])));
        }

        Ok::<TcpStream, io::Error>(backend)
    }).await
    .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "SOCKS5 握手或后端建立连接超时"))??;

    // --- 阶段 2：双向透明传输（解耦，不限时，支持长连接） ---
    let (mut client_reader, mut client_writer) = client.into_split();
    let (mut backend_reader, mut backend_writer) = backend.into_split();

    // 直接在当前协程运行，无需 tokio::spawn 产生额外调度，确保生命周期与信号量精准挂钩
    let client_to_backend = async {
        let _ = io::copy(&mut client_reader, &mut backend_writer).await;
        let _ = backend_writer.shutdown().await;
    };

    let backend_to_client = async {
        let _ = io::copy(&mut backend_reader, &mut client_writer).await;
        let _ = client_writer.shutdown().await;
    };

    // 等待双向传输都结束
    tokio::join!(client_to_backend, backend_to_client);
    
    Ok(())
}

async fn read_socks5_address_payload(stream: &mut TcpStream, atyp: u8) -> io::Result<Vec<u8>> {
    match atyp {
        0x01 => { 
            let mut buf = vec![0u8; 6];
            stream.read_exact(&mut buf).await?;
            Ok(buf)
        }
        0x03 => { 
            let mut len_buf = [0u8; 1];
            stream.read_exact(&mut len_buf).await?;
            let domain_len = len_buf[0] as usize;
            if domain_len == 0 {
                return Err(io::Error::new(io::ErrorKind::InvalidData, "零长度的无效域名"));
            }
            let mut buf = vec![0u8; 1 + domain_len + 2];
            buf[0] = len_buf[0];
            stream.read_exact(&mut buf[1..]).await?;
            Ok(buf)
        }
        0x04 => { 
            let mut buf = vec![0u8; 18];
            stream.read_exact(&mut buf).await?;
            Ok(buf)
        }
        _ => Err(io::Error::new(io::ErrorKind::InvalidData, "未知的 ATYP 地址类型")),
    }
}