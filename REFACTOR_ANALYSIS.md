# REFACTOR_ANALYSIS.md — Round Robin v1.9.1

> **分析日期**: 2026-07-27  
> **分析范围**: 全部源码 (main.rs, frame.rs, socks5.rs, splitter.rs, reassembler.rs, logging.rs)  
> **基线状态**: cargo test 9/9 通过, cargo build --release 成功, 7 clippy 警告

---

## 1. 项目概览

### 1.1 项目结构

```
round_robin/                    ← 单一 Cargo package (非 workspace)
├── Cargo.toml                  ← edition 2024, 依赖: tokio/bytes/dashmap/anyhow/serde/toml/tracing/rand
├── src/
│   ├── main.rs                 ← 入口: TOML 配置解析, 模式分发 (splitter/reassembler)
│   ├── frame.rs                ← 线协议: 帧编解码, 流式解码器, SYN/ACK 载荷
│   ├── socks5.rs               ← SOCKS5 完整实现: 服务端/客户端/UDP 数据报
│   ├── splitter.rs             ← Windows 端: SOCKS5 接入, 分片, 多路隧道, UDP 中继
│   ├── reassembler.rs          ← Debian 端: 隧道监听, 重组, 出站连接, UDP 中继
│   └── logging.rs              ← 日志: 按日滚动, Howard Hinnant 日期算法
├── config.toml                 ← splitter 配置示例
├── config.reassembler.example.toml
├── config.example.toml
├── docs/MULTIPATH_DESIGN.md    ← v1.6 设计文档 (已过时, v1.8.1 移除 ACK/流控)
└── backup/
    ├── 0.2.rs                  ← v0.2: 简单 SOCKS5 轮询负载均衡
    └── udp.rs                  ← 独立 UDP 中继实现 (已整合进 v1.x)
```

### 1.2 依赖图谱

```
                 ┌──────────┐
                 │  main.rs │  ← Config parse, mode dispatch
                 └────┬─────┘
          ┌───────────┼───────────┐
          ▼           ▼           ▼
   ┌──────────┐ ┌──────────┐ ┌──────────┐
   │splitter.rs│ │reassembler│ │logging.rs│
   └─────┬─────┘ │  .rs     │ └──────────┘
         │       └─────┬─────┘
         └───────┬─────┘
                 ▼
          ┌──────────┐    ┌──────────┐
          │ frame.rs │◄───│ socks5.rs│
          └──────────┘    └──────────┘
```

**关键依赖关系**:
- `splitter.rs` 和 `reassembler.rs` 彼此完全独立（无交叉引用）
- 两者都依赖 `frame.rs` 和 `socks5.rs`
- `socks5.rs` 独立于 `frame.rs`（不直接依赖）
- `logging.rs` 仅被 `main.rs` 使用

---

## 2. 模块职责分析

### 2.1 frame.rs (315 行)

| 职责 | 实现 |
|------|------|
| 线协议定义 | ConnID(u32) + Seq(u64) + Flags(u8) + Len(u16) + Payload |
| Frame 构造器 | `data()`, `syn()`, `syn_ack()`, `fin()`, `rst()`, `ack()` |
| Frame 编解码 | `encode()` → `Bytes`, `FrameDecoder` 流式解码 |
| SYN 载荷 | `SynTarget` (proto + address + port) 编解码 |
| ACK 载荷 | `AckInfo` 解码 (当前未使用) |

**状态**: 协议层稳定，代码质量高。`AckInfo` 和相关方法为死代码（ACK 流控已在 v1.8.1 移除）。

### 2.2 socks5.rs (433 行)

| 职责 | 实现 |
|------|------|
| 服务端握手 | `socks5_server_accept()` — CONNECT + UDP ASSOCIATE |
| 隧道握手 | `socks5_accept_tunnel()` — 接受任意 CONNECT，忽略目标 |
| 客户端连接 | `socks5_client_connect()` — 通过代理连接目标 |
| UDP 数据报 | `encode_udp_datagram()` / `decode_udp_datagram()` |
| 地址解析 | IPv4/IPv6/Domain 三种 ATYP |

**状态**: 实现完整，有单元测试覆盖。`TargetAddr` 同时用于 TCP 和 UDP 场景。

### 2.3 splitter.rs (640 行)

| 职责 | 实现 |
|------|------|
| `TunnelPool` | 隧道池 + 轮询发送 (RR atomic) + 死链压缩 |
| `TunnelLink` | 单隧道: tx channel + 统计计数器 + alive 标志 |
| `ReorderBuf` | seq 排序缓冲 (BTreeMap)，乱序等待，max 512 entries |
| `VirtConn` | 虚拟连接: to_client_tx + reorder + notify + 统计 |
| 连接管理 | `next_conn_id` (u32 wrapping)，conns DashMap |
| TCP 客户端 | `handle_tcp_client()`: SYN → 双向 select! 转发 → FIN |
| UDP 客户端 | `handle_udp_client()`: recv_from → DATA frame → pool |
| 隧道生命周期 | establish → read_loop / drain_frames → reconnect (3s 间隔) |
| 心跳 | 60s 间隔: compact + orphan sweep + 统计输出 |

### 2.4 reassembler.rs (711 行)

| 职责 | 实现 |
|------|------|
| `TunnelPool` | 与 splitter 完全相同的实现 |
| `ReorderBuf` | 与 splitter 完全相同的实现 |
| `VirtConnDe` | 虚拟连接: egress + reorder + cancel + 统计 |
| `PendingEntry` | DATA-before-SYN 缓冲 (max 256 frames/CID, 30s TTL) |
| 隧道监听 | 每端口一个 listener，accept → SOCKS5 handshake → read/write task |
| 帧处理 | SYN → egress SOCKS5 connect → SYN+ACK; DATA → reorder → write; FIN/RST → cleanup |
| UDP 中继 | 全局 UDP socket → 接收目标响应 → DATA frame → pool |
| `read_from_egress` | 响应数据: egress read → frame → pool，带 backpressure retry (5次) |
| 心跳 | 60s: compact + pending sweep + 统计输出 |

### 2.5 logging.rs (133 行)

| 职责 | 实现 |
|------|------|
| 按日滚动 | `DailyLogWriter` + `DailyWriter` 实现 `MakeWriter` trait |
| 日期算法 | Howard Hinnant `days_to_civil` (无外部依赖) |
| 旧日志清理 | `purge_old_logs()` — 按 `keep_days` 删除 |
| 线程安全 | `Mutex<Inner>` 保护文件句柄和日期 |

### 2.6 main.rs (204 行)

| 职责 | 实现 |
|------|------|
| TOML 配置 | 扁平结构: mode + [splitter] / [reassembler] 互斥 section |
| 端口解析 | 支持范围 `"52311-52319"` 和列表 `[52311, 52312]` |
| 配置发现 | exe 同目录 > 当前目录，文件名: config.toml / round_robin.toml |
| 日志初始化 | tracing-subscriber + DailyLogWriter |
| 参数校验 | chunk_size 范围 [512, 65535], 端口范围校验 |

---

## 3. 数据流分析

### 3.1 TCP 数据流 (splitter → reassembler)

```
[Browser] → SOCKS5(52310) → splitter:
  1. SOCKS5 handshake → 获取目标地址
  2. SYN frame (conn_id=N, target=addr) → pool.send() → 隧道
  3. 等待 SYN+ACK (隐式: 通过 conns 注册完成)
  4. 客户端数据 → read() chunk → Frame::data(conn_id, seq, payload) → pool.send()
  5. 响应数据 ← tunnel_read_loop → handle_inbound_frame → VirtConn.on_frame() → ReorderBuf.push() → to_client_tx → writer_task

[隧道] → TUIC → [隧道] → reassembler:
  6. tunnel_read_loop → FrameDecoder → handle_frame()
  7. SYN: socks5_client_connect(local_target, ...) → VirtConnDe → SYN+ACK
  8. DATA: ReorderBuf.push() → egress.write() → write_to_egress → [目标]
  9. 响应: egress.read() → read_from_egress → Frame::data → pool.send()
  10. FIN: splitter 发 FIN → reassembler: cancel egress, remove conn
           reassembler 发 FIN → splitter: remove conn, notify client
```

### 3.2 UDP 数据流

```
splitter UDP relay:
  [sing-box] → SOCKS5 UDP ASSOCIATE → UdpSocket.bind(0)
  → recv_from → Frame::data(conn_id=0, seq, payload) → pool.send()
  → 响应 ← VirtConn(0).on_frame → to_udp_tx → send_to(client_addr)

reassembler UDP relay:
  全局 UdpSocket → recv_from → encode_udp_datagram → Frame::data(conn_id=0) → pool.send()
  ← handle_udp_frame: decode_udp_datagram → send_to(目标)
```

### 3.3 Channel 通信拓扑

```
splitter 侧:
  TunnelLink.tx ────────► drain_frames ──► TcpStream (隧道写入)
  VirtConn.to_client_tx ─► writer_task ──► TcpStream (客户端写入)
  oneshot (UDP keepalive)

reassembler 侧:
  TunnelLink.tx ────────► drain_frames ──► TcpStream (隧道写入)
  EgressConn.write_tx ───► write_to_egress ─► TcpStream (出站写入)
  cancel (Notify) ───────► read_from_egress (取消信号)
```

---

## 4. 异步任务模型

### 4.1 splitter 任务树

```
main
├── [spawn] tunnel_manager × N (per tunnel)
│   ├── establish_tunnel → read_loop + drain_frames (spawn)
│   └── reconnect loop (3s 间隔)
├── [spawn] heartbeat (60s 间隔)
├── [spawn] handle_client × M (per SOCKS5 accept)
│   ├── [spawn] writer_task (to_client_rx → client_writer)
│   ├── [spawn] UDP keepalive watcher (oneshot)
│   └── [spawn] UDP sender (to_udp_rx → relay.send_to)
└── accept loop (阻塞)
```

### 4.2 reassembler 任务树

```
main
├── [spawn] UDP reader (全局 socket → pool)
├── [spawn] tunnel_listener × N (per port)
│   ├── accept loop
│   ├── [spawn] drain_frames × 1 per link
│   └── [spawn] tunnel_read_loop × 1 per link
│       └── handle_frame (inline, async)
│           ├── [spawn] write_to_egress (per SYN)
│           └── [spawn] read_from_egress (per SYN)
├── [spawn] heartbeat (60s 间隔)
└── ctrl_c wait
```

### 4.3 潜在问题

| 问题 | 位置 | 风险 |
|------|------|------|
| 无 spawn 失败处理 | 所有 `tokio::spawn` 调用 | 静默丢失任务，连接泄漏 |
| wr_task.abort() 后无 join | splitter.rs:211 | write half 可能未完全 flush |
| 无限重连无退避 | splitter.rs:188-229 | 固定 3s，无指数退避 |
| 无全局任务追踪 | 全局 | 无法实现优雅关闭 |

---

## 5. 内存管理分析

### 5.1 主要内存分配点

| 位置 | 分配 | 大小 | 生命周期 |
|------|------|------|----------|
| FrameDecoder::buf | BytesMut | 初始 16KB, 最大 ~64KB+15B | 隧道连接级别 |
| ReorderBuf::pending | BTreeMap<u64, Bytes> | 最多 512 entries × 65535B = 32MB | 虚拟连接级别 |
| read buffer | vec![0u8; chunk_size] | 最多 65535B | task 本地 |
| PendingEntry::frames | Vec<Frame> | 最多 256 × 65535B = 16MB | 短期 (max 30s or SYN arrival) |
| UDP buf | vec![0u8; 65535] | 65535B | task 本地 |

### 5.2 内存风险

| 风险 | 位置 | 详情 |
|------|------|------|
| **ReorderBuf 无限增长** | splitter/reassembler | MAX_PENDING_ENTRIES=512 限制单连接，但无全局限制。100 个活跃连接 × 32MB = 3.2GB |
| **PendingEntry 泄漏** | reassembler | 30s TTL 清理在 heartbeat 中执行，但 heartbeat panic 会导致永久泄漏 |
| **BytesMut 增长** | FrameDecoder | 无上限检查，畸形帧可能使其无限增长（已有 `payload_len > MAX_PAYLOAD` 检查但不够） |
| **无连接数限制** | splitter/reassembler | 无 max connections cap，可能被 DoS |

### 5.3 Arc 使用审计

| 位置 | Arc 目标 | 必要性 | 建议 |
|------|----------|--------|------|
| ConnMap (DashMap) | Arc<DashMap> | ✅ 必要 | 多 task 共享 |
| TunnelPool | Arc<TunnelPool> | ✅ 必要 | 多 task 共享 |
| TunnelLink | Arc<TunnelLink> | ✅ 必要 | 统计计数器跨 task |
| VirtConn / VirtConnDe | Arc<VirtConn> | ✅ 必要 | 多 task 共享 + 生命周期检测 |
| UdpSocket | Arc<UdpSocket> | ⚠️ 可简化 | 单 owner + clone 即可，无需 Arc |

---

## 6. 错误处理分析

### 6.1 错误传播路径

```
隧道读取错误 → tunnel_read_loop → warn! + return → 外层 loop reconnect
帧解析错误   → FrameDecoder → bail! → 同上
SOCKS5 错误  → socks5_* → Result → 调用者 warn! + 清理
客户端错误   → handle_client → warn! + return → task 结束
```

### 6.2 问题

| 问题 | 位置 | 影响 |
|------|------|------|
| `tunnel_read_loop` 返回 Err 后，link.alive 和 wr_task.abort() 的顺序 | splitter.rs:207-211 | wr_task 可能还未 flush 就被 abort |
| `read_from_egress` backpressure retry 隐藏真实错误 | reassembler.rs:666-674 | 5 次 yield_now 后静默断开 |
| 多处 `let _ =` 忽略错误 | 多处 | `shutdown()`, `write_all()` 等 |
| `unwrap()` 使用 | 数处 `lock().unwrap()` | Mutex poison 会导致 panic |
| heartbeat panic 防护 | heartbeat 中的 retain 操作 | 如果 DashMap::retain panic，心跳永久停止 |

---

## 7. 并发安全分析

### 7.1 锁使用

| 锁 | 位置 | 保护数据 | 持有时间 |
|----|------|----------|----------|
| TunnelPool.links: Mutex<Vec<Arc<TunnelLink>>> | splitter/reassembler | 隧道列表 | 短 (send/compact/add) |
| ReorderBuf pending: Mutex<BTreeMap> | 两者 | 乱序缓冲 | 短 (insert + drain) |
| logging Inner: Mutex | logging.rs | 文件句柄 | 短 (write + rotate) |
| UDP client_addr: Mutex<Option<SocketAddr>> | splitter.rs | 客户端地址 | 极短 |

### 7.2 原子操作

| 原子变量 | 位置 | Ordering | 用途 |
|----------|------|----------|------|
| AtomicBool (alive) | TunnelLink | Acquire/Release | 隧道存活标志 |
| AtomicUsize (rr) | TunnelPool | Relaxed | 轮询计数器 |
| AtomicU64 (bytes_sent/recv 等) | TunnelLink, VirtConn | Relaxed | 统计（非关键路径） |
| AtomicBool (closed) | VirtConn | Release/Acquire | 关闭标志 |

### 7.3 并发问题

| 问题 | 位置 | 详情 |
|------|------|------|
| **rr 计数器溢出回绕不精确** | TunnelPool | `fetch_add % len` 在 len 变化时可能跳跃，但不造成正确性问题 |
| **compact 与 send 的 TOCTOU** | TunnelPool | compact 可能移除刚被 send 选中的 link，但 send 有重试循环 |
| **conns.remove 与并发 on_frame** | splitter.rs:392 | FIN/RST 处理 remove conn 后，可能仍有 DATA frame 在飞行中 |
| **heartbeat retain 与并发 insert** | splitter.rs:261 | `retain` 与 `insert` 并发安全（DashMap 保证），但逻辑正确性需审查 |

---

## 8. 技术债清单

### 8.1 代码重复 (DRY 违反)

| 项目 | 重复位置 | 行数 | 优先级 |
|------|----------|------|--------|
| `TunnelPool` struct + impl | splitter.rs + reassembler.rs | ~60 行 × 2 | **高** |
| `TunnelLink` struct | splitter.rs + reassembler.rs | ~10 行 × 2 | **高** |
| `ReorderBuf` struct + impl | splitter.rs + reassembler.rs | ~30 行 × 2 | **高** |
| `drain_frames` 函数 | splitter.rs + reassembler.rs | ~14 行 × 2 | **中** |
| `MAX_PENDING_ENTRIES` 常量 | splitter.rs + reassembler.rs | 1 行 × 2 | **低** |
| `UDP_CONN_ID` 常量 | splitter.rs + reassembler.rs | 1 行 × 2 | **低** |

### 8.2 死代码

| 项目 | 位置 | 详情 |
|------|------|------|
| `AckInfo` struct | frame.rs:114-120 | v1.8.1 移除 ACK 流控后未清理 |
| `AckInfo::decode()` | frame.rs:122-131 | 同上 |
| `Frame::ack()` | frame.rs:54-60 | `#[allow(dead_code)]` 标注 |
| `PROTO_UDP` | frame.rs:77 | `#[allow(dead_code)]` 标注 |

### 8.3 设计债务

| 项目 | 详情 | 优先级 |
|------|------|--------|
| `VirtConn` vs `VirtConnDe` 命名不一致 | splitter 侧叫 VirtConn，reassembler 侧叫 VirtConnDe | **低** |
| Config struct 过度扁平 | main.rs 中所有 config 定义混在一起，无子模块 | **中** |
| 无连接超时 | TCP 连接可无限挂起，无 idle timeout | **高** |
| 无出站连接池 | reassembler 每次 SYN 都新建 SOCKS5 连接到 local_target | **中** |
| unbounded channel | 无 backpressure，内存压力下可能 OOM | **中** |
| 无优雅关闭 | Ctrl+C 后直接退出，无资源清理 | **中** |
| 缺少集成测试 | 只有 frame.rs 和 socks5.rs 有单元测试 | **高** |

---

## 9. 高风险区域

### 9.1 CRITICAL: conn_id 溢出与冲突 (splitter.rs:291-301)

**代码**:
```rust
let mut next_conn_id: u32 = 1;
// ...
let conn_id = loop {
    let id = next_conn_id;
    next_conn_id = next_conn_id.wrapping_add(1);
    if next_conn_id == 0 {
        next_conn_id = 1;
    }
    if !conns.contains_key(&id) {
        break id;
    }
};
```

**问题**: 
1. `conns.contains_key(&id)` 检查只防止了与**当前活跃** conn_id 的冲突
2. 但如果旧连接的 FIN 帧还在隧道中传输，新连接可能收到旧连接的残留数据
3. wrapping 后从 1 重新开始，但注释说 "only matters at u32 wrap" — 这是对问题严重性的低估
4. 无 TIME_WAIT 状态 — TCP 有，但应用层没有

**影响**: u32 可表示 40 亿连接，长期运行的服务器可能触发。更严重的是，高并发场景下短时间内大量短连接可能快速消耗 ID 空间。

### 9.2 HIGH: FIN frame 竞态条件 (splitter.rs:391-398, reassembler.rs:575-591)

**场景**: 多隧道环境下，FIN 帧通过隧道 A 到达，但隧道 B 上仍有该 conn_id 的 DATA 帧在传输中。

**splitter 行为**: FIN → `conns.remove()` → `conn.closed = true` → `notify.notify_one()` → client read loop 退出 → 发送 FIN → 清理

**问题**: 如果在 FIN 之后到达的 DATA 帧会被 `handle_inbound_frame` 的 `conns.get()` 返回 None → 发送 RST。这是正确的清理行为，但 RST 会导致 reassembler 侧的 egress 连接被强制关闭，可能丢失数据。

### 9.3 HIGH: 无连接超时

TCP 连接可以在不发送任何数据的情况下无限期保持打开。如果客户端崩溃而没有发送 FIN/RST：
- splitter 侧: VirtConn 永久占用内存
- reassembler 侧: VirtConnDe + egress SOCKS5 连接永久占用资源
- 心跳中的 `Arc::strong_count(vc) > 1` 检查无法检测此情况

### 9.4 MEDIUM: ReorderBuf 内存无上限

每个 VirtConn 一个 ReorderBuf，每个 ReorderBuf 最多 512 entries × 65535 bytes = 32 MB。
- 100 个并发连接 = 3.2 GB 潜在内存使用
- 如果某条隧道长时间不通，该连接的所有数据都积累在 ReorderBuf 中
- 无超时丢弃机制

### 9.5 MEDIUM: unbounded channel 无背压

`mpsc::unbounded_channel()` 在所有地方使用：
- 内存压力下可能无限增长
- 发送方永远不阻塞，无法将背压传播到上游

### 9.6 MEDIUM: heartbeat 中的 retain 逻辑 (splitter.rs:261)

```rust
hb_conns.retain(|_, vc| Arc::strong_count(vc) > 1);
```

**问题**: 依赖 `Arc::strong_count` 判断是否孤立是不精确的。`strong_count > 1` 意味着除了 DashMap 本身还有至少一个引用。但如果 writer_task 恰好在 `retain` 和下一次检查之间退出，合法的活跃连接可能被误清理。

---

## 10. Rust 专项检查

### 10.1 所有权与 Clone

| 位置 | 操作 | 评价 |
|------|------|------|
| `frame.clone()` in pool.send() | splitter.rs:93, reassembler.rs:78 | **必要** — unbounded channel 需要 ownership |
| `Bytes::copy_from_slice()` | 多处 | **必要** — 从临时 buffer 到 frame payload |
| `ep.clone()` in tunnel spawn | splitter.rs:183 | **可优化** — TunnelEndpoint 可改为 Arc 避免 clone |
| `Bytes::copy_from_slice()` in handle_udp_frame | reassembler.rs | **可优化** — 可直接使用 payload slice |

### 10.2 生命周期

- 无显式生命周期标注 — 全部依赖 Arc + 'static bounds
- 这是此项目规模的合理选择

### 10.3 异步正确性

| 检查项 | 状态 | 说明 |
|--------|------|------|
| blocking 调用 | ✅ 无 | 所有 I/O 都是 async |
| spawn 失败处理 | ❌ 缺失 | 所有 spawn 使用 `let _ =` 忽略 JoinHandle |
| channel 死锁 | ⚠️ 可能有 | unbounded channel 不会死锁，但 select! 可能永久等待 |
| task 泄漏 | ⚠️ 可能有 | abort 的 task 无 join，资源清理不完整 |
| cancellation safety | ⚠️ 部分 | `tokio::select!` 中取消的 future 可能丢失数据 |

### 10.4 网络正确性

| 检查项 | 状态 | 说明 |
|--------|------|------|
| TCP half-close | ✅ 已处理 | shutdown() 正确调用 |
| FIN/RST 处理 | ⚠️ 见 9.2 | 多隧道竞态条件 |
| timeout | ⚠️ 部分 | 有 connect timeout，无 idle timeout |
| connection cleanup | ⚠️ 见 9.3 | 依赖正常 FIN/RST 路径 |
| buffer 增长 | ⚠️ 见 9.4 | ReorderBuf 有上限但无全局限制 |
| Nagle 算法 | ✅ 已禁用 | set_nodelay(true) 到处使用 |

### 10.5 协议安全

| 检查项 | 状态 | 说明 |
|--------|------|------|
| frame 解析安全 | ✅ 较好 | 长度检查 + 边界验证 |
| ID 溢出 | ❌ 见 9.1 | conn_id u32 wrapping |
| 状态机一致性 | ⚠️ | 隐式状态 (conns 存在 = 活跃)，无显式状态机 |
| 数据乱序 | ✅ | BTreeMap 重排 |

---

## 11. 基线测试结果

```
cargo test:  9 passed, 0 failed
cargo build --release: 成功
cargo clippy: 7 warnings (0 errors)

Warning breakdown:
  - dead_code: 2 (AckInfo, AckInfo::decode)
  - too_many_arguments: 3 (8 args each in splitter/reassembler)
  - clone_on_copy: 1 (splitter.rs:591)
  - collapsible_if: 1 (splitter.rs:592)
```

---

## 12. 重构建议优先级

### P0 — 立即修复 (安全/正确性)
1. **conn_id TIME_WAIT**: 添加 ID 回收延迟，防止残留帧误路由
2. **FIN 竞态**: 添加 FIN 后的 grace period，排空残留 DATA 帧
3. **连接超时**: 添加 idle timeout，防止资源泄漏

### P1 — 结构改进 (可维护性)
4. **提取共享模块**: 消除 TunnelPool / ReorderBuf / TunnelLink / drain_frames 重复
5. **清理死代码**: 移除 AckInfo 和相关方法
6. **Config 模块化**: 将 config 类型移入独立模块

### P2 — 健壮性提升
7. **spawn 失败处理**: 处理 JoinHandle，添加任务监控
8. **全局内存限制**: 添加并发连接数上限和每连接内存预算
9. **优雅关闭**: 实现信号驱动的 graceful shutdown

### P3 — 性能优化
10. **bounded channel**: 用 bounded mpsc 替代 unbounded，实现背压
11. **出站连接池**: reassembler 复用 SOCKS5 连接
12. **Bytes 零拷贝优化**: 减少 `copy_from_slice` 调用

---

*本文档基于代码阅读和静态分析生成。实际重构前建议在测试环境验证高风险区域的假设。*
