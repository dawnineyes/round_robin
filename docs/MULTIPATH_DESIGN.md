# 多路径 SOCKS5 负载均衡 — 技术设计文档

> **⚠️ 本文档为 v1.6 时期设计，v1.8.1 已大幅简化。**
> 已移除：ACK/credit 流控、strike/cooldown 降级、gap timeout、tunnel timeout、keepalive、重传、拥塞控制。
> 当前设计原则：TUIC TCP 保证送达 → 只做 seq 重排。详见 git log v1.7.0 ~ v1.8.1。
>
> 版本: v1.6 (历史)  
> 日期: 2026-07-21

---

## 1. 问题定义

### 1.1 当前架构

```
Win11 App ──→ sing-box TUN ──→ SOCKS5(52030) ──→ Rust 轮询 ──→ SOCKS5(52031) ──→ TUIC1 ──→ Debian TUIC1 ──→ 目标
                                                                    SOCKS5(52032) ──→ TUIC2 ──→ Debian TUIC2 ──→ 目标
                                                                    ...
                                                                    SOCKS5(52039) ──→ TUIC9 ──→ Debian TUIC9 ──→ 目标
```

**瓶颈**: Rust 每条 SOCKS5 CONNECT 只分配到一个后端端口，单连接 → 单 TUIC 隧道 → 单路径速度。

### 1.2 目标

单条虚拟连接（一个 TCP 流 / 一个 UDP 会话）的数据分片后**同时**走 9 条 TUIC 隧道，在服务端重组后发往目标，实现单连接速度 ≈ 9 × 单隧道速度。

---

## 2. 整体架构

### 端口映射

```
              Windows 11                        Debian 13
              ──────────                        ─────────
Rust listen   127.0.0.1:52030 (SOCKS5 入)       127.0.0.1:52031-52039 (SOCKS5 入 ×9)
Rust → sing   sing-box:52031-52039 (SOCKS5 ×9)  sing-box:52030 (SOCKS5 出)
sing-box WAN  ─                                :54431-54439 (TUIC 入 ×9)
```

两端 **52030-52039** 端口角色镜像对称，TUIC 使用独立范围 **54431-54439** 避免冲突。

### 架构图

```
┌──────────────────────────── Windows 11 ────────────────────────────────┐
│                                                                         │
│  sing-box TUN ──→ SOCKS5(52030) ──→ Rust [Splitter]                    │
│                                           │                            │
│                             9 条持久 SOCKS5 连接                        │
│                            ┌─→ sing-box(:52031) ──→ TUIC 隧道 1 ──┐    │
│                            ├─→ sing-box(:52032) ──→ TUIC 隧道 2 ──┤    │
│       Round-robin 分发     ┊         ⋮                         ⋮  ┊    │
│                            └─→ sing-box(:52039) ──→ TUIC 隧道 9 ──┘    │
│                                                                         │
└──────────────────────────────┬──────────────────────────────────────────┘
                               │
                    ═══════════╧═══════════  Internet  ═══════════════════
                               │
┌──────────────────────────────┴──────────────────────────────────────────┐
│                            Debian 13                                    │
│                                                                         │
│  TUIC 隧道 1 ──→ sing-box TUIC(:54431) ──→ SOCKS5 ──→ Rust(:52031)    │
│  TUIC 隧道 2 ──→ sing-box TUIC(:54432) ──→ SOCKS5 ──→ Rust(:52032)    │
│       ⋮                                        ⋮              ⋮        │
│  TUIC 隧道 9 ──→ sing-box TUIC(:54439) ──→ SOCKS5 ──→ Rust(:52039)    │
│                                                                         │
│  Rust [Reassembler]:                                                    │
│    9 个 SOCKS5 server 收帧 → ConnID 重组 → SOCKS5 client 出站           │
│                                                      │                  │
│                                          sing-box SOCKS5(:52030)        │
│                                                      │                  │
│                                                   direct               │
│                                                      │                  │
│                                                      ↓                  │
│                                                 Internet               │
└─────────────────────────────────────────────────────────────────────────┘
```

**核心**: Rust 是纯中间件。Windows 端分片，Debian 端重组，sing-box 负责传输和最终出站路由。两端均通过 SOCKS5 与 sing-box 通信。

---

## 3. 传输协议设计

### 3.1 帧格式

每条隧道连接上传输的帧（大端序）：

```
 0                   1                   2                   3
 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                         ConnID (32 bits)                      |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                        Sequence (64 bits)                     |
|                                                               |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|  Flags (8)    |            Length (16 bits)                   |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                                                               |
|                    Payload (0 ~ 65535 bytes)                  |
|                                                               |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
```

| 字段 | 大小 | 说明 |
|---|---|---|
| ConnID | 4 bytes | 虚拟连接标识，全局唯一（随机生成） |
| Sequence | 8 bytes | 该 ConnID 内的单调递增序号，从 0 开始 |
| Flags | 1 byte | 见下表 |
| Length | 2 bytes | Payload 长度，0~65535 |
| Payload | 可变 | 数据负载 |

### 3.2 Flags 位定义

```
Bit 0 (0x01) — SYN    : 建立虚拟连接，Payload 为目标地址
Bit 1 (0x02) — DATA   : 数据帧
Bit 2 (0x04) — FIN    : 关闭连接
Bit 3 (0x08) — RST    : 异常重置
Bit 4 (0x10) — ACK    : 重组窗口确认 (流量控制)
Bit 5-7                : 保留
```

**组合**: SYN+ACK（握手响应）、DATA+FIN（带最后数据的关闭）等允许组合。

### 3.3 SYN 帧 Payload 格式

```
 0                   1
 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
| Proto (8)     |  Addr Len (16)  |   Address ...  |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|             Port (16)            |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+

Proto: 0x06 = TCP, 0x11 = UDP
Address: "example.com" (域名) 或 "1.2.3.4" (IPv4) 或 IPv6 字面量
```

### 3.4 ACK 帧 Payload 格式

```
 0                   1                   2                   3
 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                   Ack Sequence (64 bits)                      |
|                                                               |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                  Window Size (32 bits)                        |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
```

由重组器周期性发送，告知分片器已连续收到的最大 Seq。用于流量控制和慢路径检测。

---

## 4. 连接生命周期

### 4.1 TCP 流程

```
 Win Rust (Splitter)                           Debian Rust (Reassembler)
 ──────────────────                           ──────────────────────────
                                       
 [收到 SOCKS5 CONNECT 到 "example.com:443"]
 ConnID = rand_u32()
                                       
 SYN(conn=42, seq=0, proto=TCP,        ──→ 收到 SYN
      addr="example.com:443")               │
          │                                  ├─ SOCKS5 CONNECT 到
          │  (走任意一条隧道)                 │  sing-box:52030
          │                                  │  目标="example.com:443"
          │                                  ├─ 成功 → SYN+ACK(conn=42, seq=0)
          │                                  │
 SYN+ACK(conn=42, seq=0)               ←──   │
          │                                  
 [向 Windows sing-box 客户端返回成功]              
          │                                  
 [客户端数据到达]                            
 DATA(conn=42, seq=1, payload[...])    ──→  重组后写入 sing-box:52030 连接
 DATA(conn=42, seq=2, payload[...])    ──→  (乱序到达 → BTreeMap 缓冲 → 按序交付)
 DATA(conn=42, seq=3, payload[...])    ──→  
          │                                  
          │                            ←──  DATA(conn=42, seq=1, payload[...])  (目标响应,来自 sing-box:52030)
          │                            ←──  DATA(conn=42, seq=2, payload[...])
          │                                  
 [客户端关闭]                                
 FIN(conn=42, seq=4)                   ──→  收到 FIN
          │                                  ├─ shutdown 到 sing-box:52030 连接
          │                                  ├─ 等待残余响应数据
          │                            ←──  FIN(conn=42, seq=3)
          │                                  
 [清理 ConnID 42 的状态]               [清理 ConnID 42 + SOCKS5 连接]
```

### 4.2 UDP 流程

```
 Win Rust (Splitter)                           Debian Rust (Reassembler)
 ──────────────────                           ──────────────────────────

 [收到 SOCKS5 UDP ASSOCIATE]
 ConnID = rand_u32()

 SYN(conn=43, seq=0, proto=UDP,        ──→  收到 SYN
      addr="0.0.0.0:0")                     │  (UDP 无连接，addr 字段保留)
          │                                  ├─ 创建 UDP socket
          │                            ←──  SYN+ACK(conn=43, seq=0)
          │
 [收到 SOCKS5 UDP 数据报]                   
 每个 UDP 数据报作为一个完整 DATA 帧，
 包含 SOCKS5 UDP 头（ATYP + DST.ADDR + DST.PORT + DATA）

 DATA(conn=43, seq=1,                  ──→  解析 SOCKS5 UDP 头
      payload[udp_header + data])            ├─ sendto(dst_addr, dst_port, data)
                                             │
          │                            ←──  DATA(conn=43, seq=1, payload[...])  (响应)
          

 [超时无数据]  RST(conn=43)            ──→  清理 UDP socket
```

**UDP 关键差异**:
- 每个 UDP 数据报 = 一个完整的 DATA 帧（不跨帧分包）
- Seq 按数据报递增，但不会用于排序——重组器直接转发，乱序即乱序（UDP 本就不保证顺序）
- ConnID 超时（如 60s 无数据）自动 RST

---

## 5. Windows 端 Rust — Splitter 设计

### 5.1 职责

1. SOCKS5 服务器: 监听 `127.0.0.1:52030`
2. 隧道管理: 维护 9 条到 `127.0.0.1:52031~52039` 的持久 SOCKS5 连接
3. 分片: 按 chunk 大小（默认 16384 bytes）切分 TCP 数据流
4. 轮询分发: 每个 chunk 封装成帧，轮询发到 9 条隧道
5. 流量控制: 按 ConnID 跟踪发送窗口，接收 ACK，处理慢路径

### 5.2 数据结构

```rust
// 虚拟连接
struct VirtualConn {
    conn_id: u32,
    proto: Proto,           // TCP or UDP
    state: ConnState,       // Connecting | Established | Closing | Closed
    next_seq: u64,          // 下一个发出帧的 Seq
    recv_seq: u64,          // 收到的最大 Seq（从 Debian 方向）
    send_window: u64,       // 飞行中的最大 Seq
    ack_seq: u64,           // Debian 已确认的最大 Seq
    client_handle: ClientHandle, // SOCKS5 客户端读写句柄
}

// 隧道连接
struct Tunnel {
    stream: TcpStream,      // 到 sing-box SOCKS5 的连接
    write_lock: Mutex<()>,  // 写端锁
    alive: AtomicBool,
}

// 分片器全局状态
struct Splitter {
    tunnels: [Tunnel; 9],
    conns: DashMap<u32, VirtualConn>,
    next_tunnel: AtomicUsize,   // 轮询计数器
}
```

### 5.3 核心流程

```
主循环:
  accept SOCKS5 客户端连接
  ↓
  认证协商 (0x05, 0x00)
  ↓
  读取 CONNECT / UDP ASSOCIATE 请求
  ↓
  生成 ConnID
  选择隧道 (轮询) 发送 SYN 帧
  ↓
  等待 SYN+ACK (带超时)
  ↓
  向 SOCKS5 客户端返回成功
  ↓
  spawn 双向转发协程:
    C→P: 读取客户端数据 → 分片 → 封装帧 → 轮询发送到隧道
    P→C: 从隧道收帧 → 按 Seq 写入客户端

隧道读取协程 (每个隧道一个):
  loop { 读取帧 → 根据 ConnID 路由到对应 VirtualConn 的接收缓冲区 }
```

### 5.4 分片策略

```
chunk_size = 16384  // 16 KB

对于 DATA 帧:
  while 客户端有数据:
    chunk = read(client, chunk_size)
    seq = conn.next_seq++
    frame = Frame { conn_id, seq, flags=DATA, payload=chunk }
    tunnel_idx = counter.fetch_add(1) % 9
    tunnels[tunnel_idx].write(frame)
```

### 5.5 慢路径降级

```
检测: Ack Seq 远小于 Send Seq 的隧道 → 该路径拥塞
策略: 连续 3 次检测到拥塞 → 临时排除该隧道 (N 秒后恢复)
      被排除隧道的数据重新分配到其他隧道
      全部隧道拥塞 → 暂停客户端读取 (背压传播)
```

---

## 6. Debian 端 Rust — Reassembler 设计

### 6.1 职责

```
                     Rust Reassembler
                     ═══════════════
 sing-box ──SOCKS5──→ :52031 ─┐
 sing-box ──SOCKS5──→ :52032 ─┤
    ⋮                    ⋮    ├──→ 帧解析 → ConnID 重组 → SOCKS5 client → sing-box:52030 → direct → Internet
 sing-box ──SOCKS5──→ :52039 ─┘
```

1. **9 个 SOCKS5 服务器**: 监听 `127.0.0.1:52031~52039`，只接受无认证 (0x00)，接受任意 CONNECT 目标（数据是帧流，目标地址不关心）
2. **帧解析**: 每个隧道连接独立维护 Decoder，从字节流切帧
3. **重组**: 按 ConnID + Seq 排序 DATA 帧，按序交付
4. **出站**: 收到 SYN → 以 SOCKS5 客户端身份连接 `127.0.0.1:52030`（sing-box SOCKS5 入站），CONNECT 到真实目标地址；重组后的数据写入此连接
5. **周期 ACK**: 告知 Splitter 已连续接收的进度，同时根据隧道写入队列深度做背压

### 6.2 数据结构

```rust
struct Reassembler {
    tunnels: [TunnelRx; 9],              // 9 条来自 sing-box 的持久连接
    conns: DashMap<u32, VirtualConnDe>,  // ConnID → 虚拟连接
    egress: EgressPool,                  // 到 sing-box:52030 的 SOCKS5 连接池
}

struct TunnelRx {
    stream: TcpStream,                   // 来自 sing-box SOCKS5 的持久 TCP 连接
    decoder: FrameDecoder,               // 字节流 → 帧
    write_half: OwnedWriteHalf,          // 用于回复 ACK / 响应数据回 Splitter
    alive: AtomicBool,
}

struct VirtualConnDe {
    conn_id: u32,
    proto: Proto,
    egress: Option<EgressConn>,          // 到 sing-box:52030 的 SOCKS5 连接 (TCP only)
    recv_buffer: BTreeMap<u64, Vec<u8>>, // Seq → chunk 缓存 (乱序)
    next_deliver: u64,                   // 下一个要交付的 Seq
    last_ack_sent: Instant,
    udp_socket: Option<UdpSocket>,       // UDP only: 本地 UDP relay
}

struct EgressConn {
    stream: TcpStream,                   // 到 sing-box:52030 的 TCP 连接 (已完成 SOCKS5 握手)
    target_addr: String,                 // 真实目标 "example.com:443"
}
```

### 6.3 帧读取

每个隧道连接 1 个读取协程：

```rust
// 每个隧道独立运行
async fn tunnel_read_loop(mut tunnel: TunnelRx, conns: Arc<DashMap<u32, VirtualConnDe>>) {
    loop {
        let frame = tunnel.decoder.read_frame(&mut tunnel.stream).await?;
        handle_frame(frame, tunnel.id, &conns).await;
    }
}
```

Decoder 状态机：

```
NeedHeader (15 bytes)
  → 读完 15 字节 → 解析 ConnID / Seq / Flags / Length → 进入 NeedPayload
NeedPayload (Length bytes)
  → 读完 Payload → 触发 handle_frame() → 回到 NeedHeader
```

### 6.4 重组与出站

```rust
async fn handle_frame(frame: Frame, src_tunnel: usize, conns: &DashMap<u32, VirtualConnDe>) {
    match frame.flags & 0x0F {  // 低 4 位为帧类型
        SYN => {
            let target = parse_syn_payload(&frame.payload);
            // 通过 SOCKS5 连接 sing-box:52030，CONNECT 到真实目标
            let egress = socks5_connect("127.0.0.1:52030", &target.addr, target.port).await?;
            
            conns.insert(frame.conn_id, VirtualConnDe {
                conn_id: frame.conn_id,
                proto: target.proto,
                egress: Some(egress),
                recv_buffer: BTreeMap::new(),
                next_deliver: 1,
                last_ack_sent: Instant::now(),
            });
            // 回复 SYN+ACK 到 Splitter
            send_frame_via(frame.conn_id, 0, SYN | ACK, &[], src_tunnel);
        }
        DATA => {
            let Some(mut conn) = conns.get_mut(&frame.conn_id) else { return };
            if frame.seq == conn.next_deliver {
                // 顺序到达 → 直接写入 sing-box:52030 的出站连接
                conn.write_to_egress(&frame.payload);
                conn.next_deliver += 1;
                // 清缓冲区
                while let Some(data) = conn.recv_buffer.remove(&conn.next_deliver) {
                    conn.write_to_egress(&data);
                    conn.next_deliver += 1;
                }
            } else if frame.seq > conn.next_deliver {
                // 乱序到达 → 缓存
                conn.recv_buffer.insert(frame.seq, frame.payload);
                if conn.recv_buffer.len() > MAX_REORDER_WINDOW {
                    send_ack(conn, src_tunnel);  // 告知期望的 Seq
                }
            }
            // seq < next_deliver: 重复帧，忽略
        }
        FIN => {
            // shutdown egress 连接的写端
            // 等待 egress 读端返回 FIN 后回复 FIN 到 Splitter
        }
        ACK => {
            // ACK 帧是 Debian→Windows 方向的，此处处理的是 Windows 发给 Debian 的 ACK
            // 更新对应 ConnID 的发送窗口
        }
    }

    if should_send_ack(&conn) {
        send_ack(conn, src_tunnel);
    }
}
```

### 6.5 响应数据回传 (Debian → Windows)

目标服务器 → sing-box direct → sing-box SOCKS5(:52030) → Rust egress 连接收到数据 → 分帧 → 轮询走 9 条隧道写回 Splitter。

```rust
// 每个 VirtualConnDe 的 egress 连接读取协程
async fn egress_read_loop(conn_id: u32, mut egress: EgressConn, 
                           tunnels: Arc<[TunnelTx; 9]>, next_tnl: Arc<AtomicUsize>) {
    let mut buf = vec![0u8; CHUNK_SIZE];
    let mut seq: u64 = 1;
    loop {
        let n = egress.stream.read(&mut buf).await?;
        if n == 0 { /* egress FIN */ break; }
        let tnl = next_tnl.fetch_add(1, Relaxed) % 9;
        send_frame(conn_id, seq, DATA, &buf[..n], tnl, &tunnels).await;
        seq += 1;
    }
    let tnl = next_tnl.fetch_add(1, Relaxed) % 9;
    send_frame(conn_id, seq, FIN, &[], tnl, &tunnels).await;
}
```

### 6.6 关键参数

| 参数 | 建议值 | 说明 |
|---|---|---|
| Chunk 大小 | 16384 bytes (16 KB) | 头开销 15/16384 ≈ 0.09% |
| 重组窗口 | 64 chunks ≈ 1 MB | 缓存开销 ~1MB/连接 |
| ACK 间隔 | 100ms 或每 64 帧 | 平衡流量控制精度和开销 |
| egress 连接超时 | TCP: 300s 无数据; UDP: 60s | 回收 sing-box:52030 连接 |

---

## 7. sing-box 配置要点

### 7.1 Windows 端

**核心约束**: 每个 SOCKS5 入站端口绑定固定的 TUIC 出站，确保 Rust 的持久连接对应固定隧道。

```json
// 9 个 SOCKS5 入站 + 9 个 TUIC 出站，一一绑定
{
  "inbounds": [
    { "type": "socks5", "tag": "socks-in-1", "listen": "127.0.0.1", "listen_port": 52031 },
    { "type": "socks5", "tag": "socks-in-2", "listen": "127.0.0.1", "listen_port": 52032 },
    // ... 直到 52039
  ],
  "outbounds": [
    { "type": "tuic", "tag": "tuic-out-1", "server": "<debian-ip>", "server_port": 54431, /* ... */ },
    { "type": "tuic", "tag": "tuic-out-2", "server": "<debian-ip>", "server_port": 54432, /* ... */ },
    // ... 直到 tuic-out-9
  ],
  "route": {
    "rules": [
      { "inbound": "socks-in-1", "outbound": "tuic-out-1" },
      { "inbound": "socks-in-2", "outbound": "tuic-out-2" },
      // ... 一一对应
    ]
  }
}
```

### 7.2 Debian 端

**三层结构**:

```
Layer 1: 9 个 TUIC 入站 (WAN-facing)
         :54431-54439

Layer 2: 9 个 SOCKS5 出站 (每个 TUIC 入站 → 固定 SOCKS5 出站 → Rust 固定端口)
         127.0.0.1:52031-52039

Layer 3: 1 个 SOCKS5 入站 (接收 Rust 重组后的出站流量) + 1 个 direct 出站
         127.0.0.1:52030
```

```json
{
  "inbounds": [
    { "type": "tuic", "tag": "tuic-in-1", "listen": "0.0.0.0", "listen_port": 54431, /* users: [...] */ },
    { "type": "tuic", "tag": "tuic-in-2", "listen": "0.0.0.0", "listen_port": 54432, /* users: [...] */ },
    { "type": "tuic", "tag": "tuic-in-3", "listen": "0.0.0.0", "listen_port": 54433, /* users: [...] */ },
    { "type": "tuic", "tag": "tuic-in-4", "listen": "0.0.0.0", "listen_port": 54434, /* users: [...] */ },
    { "type": "tuic", "tag": "tuic-in-5", "listen": "0.0.0.0", "listen_port": 54435, /* users: [...] */ },
    { "type": "tuic", "tag": "tuic-in-6", "listen": "0.0.0.0", "listen_port": 54436, /* users: [...] */ },
    { "type": "tuic", "tag": "tuic-in-7", "listen": "0.0.0.0", "listen_port": 54437, /* users: [...] */ },
    { "type": "tuic", "tag": "tuic-in-8", "listen": "0.0.0.0", "listen_port": 54438, /* users: [...] */ },
    { "type": "tuic", "tag": "tuic-in-9", "listen": "0.0.0.0", "listen_port": 54439, /* users: [...] */ },

    { "type": "socks5", "tag": "socks-from-rust", "listen": "127.0.0.1", "listen_port": 52030 }
  ],

  "outbounds": [
    { "type": "socks5", "tag": "to-rust-1", "server": "127.0.0.1", "server_port": 52031 },
    { "type": "socks5", "tag": "to-rust-2", "server": "127.0.0.1", "server_port": 52032 },
    { "type": "socks5", "tag": "to-rust-3", "server": "127.0.0.1", "server_port": 52033 },
    { "type": "socks5", "tag": "to-rust-4", "server": "127.0.0.1", "server_port": 52034 },
    { "type": "socks5", "tag": "to-rust-5", "server": "127.0.0.1", "server_port": 52035 },
    { "type": "socks5", "tag": "to-rust-6", "server": "127.0.0.1", "server_port": 52036 },
    { "type": "socks5", "tag": "to-rust-7", "server": "127.0.0.1", "server_port": 52037 },
    { "type": "socks5", "tag": "to-rust-8", "server": "127.0.0.1", "server_port": 52038 },
    { "type": "socks5", "tag": "to-rust-9", "server": "127.0.0.1", "server_port": 52039 },

    { "type": "direct", "tag": "direct-out" }
  ],

  "route": {
    "rules": [
      { "inbound": "tuic-in-1", "outbound": "to-rust-1" },
      { "inbound": "tuic-in-2", "outbound": "to-rust-2" },
      { "inbound": "tuic-in-3", "outbound": "to-rust-3" },
      { "inbound": "tuic-in-4", "outbound": "to-rust-4" },
      { "inbound": "tuic-in-5", "outbound": "to-rust-5" },
      { "inbound": "tuic-in-6", "outbound": "to-rust-6" },
      { "inbound": "tuic-in-7", "outbound": "to-rust-7" },
      { "inbound": "tuic-in-8", "outbound": "to-rust-8" },
      { "inbound": "tuic-in-9", "outbound": "to-rust-9" },

      { "inbound": "socks-from-rust", "outbound": "direct-out" }
    ]
  }
}
```

**行为预期**:

| 方向 | 连接语义 |
|---|---|
| TUIC 入站 → SOCKS5 → Rust | 每条 TUIC 隧道维持 1 条持久流，sing-box 为此流创建 1 条到对应 Rust 端口的 SOCKS5 连接。Rust 的 SOCKS5 server 忽略 CONNECT 目标，只取原始 TCP 流进行帧解析 |
| Rust → SOCKS5 → sing-box:52030 → direct | Rust 为每个虚拟连接 (ConnID) 发起 SOCKS5 CONNECT（携带真实目标地址），sing-box direct 出站到 Internet |

**⚠️ 待验证**: sing-box TUIC 入站内部的流→SOCKS5 出站连接的生命周期。如果每个 TUIC 子流都创建新 SOCKS5 连接（而非复用），需评估连接创建开销。降级方案见 10.1。

---

## 8. 错误处理

| 场景 | Windows Splitter | Debian Reassembler |
|---|---|---|
| 隧道断开 | 标记隧道不可用，重连（指数退避），数据改走其他隧道 | 标记对应连接断开，等待 sing-box 重连 |
| SYN 超时 (3s) | 重试下一隧道 | N/A |
| ConnID 重组窗口溢出 | ACK 反压 | 丢弃超出窗口的数据帧，回复 ACK |
| 目标连接失败 | 向 SOCKS5 客户端返回错误 (0x03) | sing-box:52030 SOCKS5 返回失败 → 向 Splitter 回复 RST |
| chunk MD5 校验失败 (可选) | N/A | 请求重发 (Data Seq) |
| 内存超限 | 限制活跃 ConnID 数量，拒绝新连接 | 同上 + LRU 淘汰最久未活动的连接 |

---

## 9. 性能预估

### 9.1 开销分析

| 开销来源 | 每帧成本 |
|---|---|
| 帧头 | 15 bytes |
| SOCKS5 层 | 无额外开销（持久连接，握手已完成） |
| TUIC 加密 | 由 TUIC 处理，透明 |
| 内存复制 | 分片/重组各 1 次 copy |

**带宽利用率**: 16KB / (16KB + 15B) ≈ 99.91%

### 9.2 延迟

| 项目 | 增加延迟 |
|---|---|
| 分片 | < 1ms |
| 轮询分发 | < 0.1ms |
| 重组 | < 1ms (顺序到达时) / 取决于路径差异 (乱序时) |
| SYN 握手额外 RTT | 1 RTT (Win Rust → Debian Rust SYN+ACK) + SOCKS5 握手 (Debian Rust → sing-box:52030，本地 < 1ms) |

### 9.3 设计上限

| 指标 | 值 |
|---|---|
| 并发虚拟连接 | ~10000 (内存 ~10MB + 1MB × 活跃窗口) |
| 单连接吞吐 | 受限于 9 条路径总带宽 × 0.999 |
| 单机吞吐 | 受限于 CPU（内存 copy + 帧序列化） |

---

## 10. 备选方案与降级

### 10.1 如果 TUIC→SOCKS5 的连接复用不符合预期

**症状**: Debian 端 Rust 收到大量短连接（每个 chunk 一个新 SOCKS5 连接），而非 9 条长连接。

**降级方案**: 绕过 SOCKS5，改用裸 TCP：

sing-box 用 `direct` 出站 + destination override 到 Rust（如果 sing-box 支持），或 Rust 直接监听 9 个原始 TCP 端口。去掉中间的 SOCKS5 握手，sing-box 直接转发 TUIC 流字节到 Rust。

```json
// 降级: 如果 sing-box 支持 destination_override
{
  "outbounds": [
    { 
      "type": "direct", "tag": "to-rust-1",
      "destination_override": "127.0.0.1:52031"
    }
  ]
}
```

如果 sing-box 不支持此特性，Rust 端改为实现最简 HTTP CONNECT 或无协议裸 TCP 接收。额外工作量约 100 行。

### 10.2 最小化方案 (YAGNI)

v1 可暂不实现：
- **payload 校验**: TUIC 层已有完整性保证，在应用层校验是冗余
- **流量加密**: 数据已在 TUIC 层加密
- **UDP 拥塞控制**: 依赖 TUIC 本身的 QUIC 拥塞控制
- **自适应 Chunk 大小**: 固定 16KB 先跑，有数据再调

---

## 11. 实施计划

### Phase 1: 协议核心 (约 400 行)

- [ ] 帧编码/解码 (serialize/deserialize)
- [ ] Decoder（从 TCP 字节流切帧）
- [ ] ConnID 管理

### Phase 2: Windows Splitter (约 500 行)

- [ ] 隧道连接建立与维护（重连）
- [ ] TCP CONNECT 处理（握手→分片→双向转发）
- [ ] UDP ASSOCIATE 处理
- [ ] ACK 接收与慢路径降级

### Phase 3: Debian Reassembler (约 500 行)

- [ ] 接收隧道连接，帧解析循环
- [ ] 重组（BTreeMap 乱序缓冲 + 按序交付）
- [ ] 目标连接建立与双向转发
- [ ] 周期 ACK 发送

### Phase 4: 集成与验证

- [ ] sing-box 配置（两端）
- [ ] 压测: 单连接大文件下载，验证速度是否接近 9× 单隧道
- [ ] 故障注入: 随机关闭隧道，验证容错

---

## 12. 开放问题

| # | 问题 | 决策 / 状态 | 优先级 |
|---|---|---|---|
| 1 | sing-box 1.14 TUIC 入站→SOCKS5 出站的连接复用行为 | 待实测，降级方案见 10.1 | P0 |
| 2 | SYN 握手模式 | ✅ SYN+ACK（可靠交付） | 已决 |
| 3 | UDP 支持 | v1 实现，Phase 2/3 包含 UDP ASSOCIATE 处理 | P1 |
| 4 | 是否需要压缩？ | TUIC 流量可能已被应用层压缩，暂不加 | P3 |
| 5 | IPv6 目标地址 | SYN payload 已预留地址类型字段，支持时只需扩展 parse | P2 |
| 6 | sing-box direct 出站的 destination_override 可用性 | 影响 10.1 降级方案复杂度 | P2 |

---

*本文档随设计迭代更新。Phase 1 开始前请评审第 3 节（协议设计）和第 7 节（sing-box 配置要点）。*
