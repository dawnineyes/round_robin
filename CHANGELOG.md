# CHANGELOG.md — Round Robin Refactoring

> **重构日期**: 2026-07-27  
> **基线**: v1.9.1  
> **原则**: 每次单阶段修改，cargo fmt + check + test + clippy 全部通过

---

## Phase 9: Bug 审查修复 + 背压重构 (v1.10.4)

> **基线**: v1.10.3  
> **来源**: 全量代码审查（12 个 bug + 优化项），对应审查报告中的 BUG-1~12 / O1~O4 / O8

### 修改文件

| 文件 | 修改内容 | 原因 |
|------|----------|------|
| `src/reorder.rs` | `PushResult` 新增 `overflow` 标记；新增 `is_complete_through()`；新增单元测试 | BUG-2: 窗口满丢帧后序列永久断裂，需重置而非静默 |
| `src/tunnel.rs` | 新增 `send_async()`（least-loaded 真实背压发送）；`drain_frames` 写超时 60s；新增 3 个测试 | BUG-5/O2: yield 重试是伪背压；BUG-9: 写阻塞无超时 |
| `src/frame.rs` | `encode()` 长度 `debug_assert`；删除 `Frame::syn_ack()` 与 `FLAG_ACK` | BUG-8: u16 长度字段截断会毁流；O3: SYN+ACK 死代码 |
| `src/reassembler.rs` | FIN 改为半关闭（`start_half_close`/`close_write_half`，10s 强制兜底）；重复 SYN 防护（`handshaking` 集合）；`closed` 墓碑集合（晚到 DATA 回 RST）；egress 通道改有界 512 + 写超时；accept 错误重试；隧道链路上限 64；UDP 超大报文丢弃；读缓冲复用 + `send_async` 背压；先 insert 后 spawn 消除竞态 | BUG-1/2/5/7/8/9/10/11/12 + O1/O3 |
| `src/splitter.rs` | FIN/RST 半关闭配合（`rst` 标记 + close_reason=rst）；溢出即重置并回 RST；UDP 出站刷新 `last_active`；单 UDP 客户端守卫 + `remove_if`；UDP 循环监听 notify；客户端通道改有界 512/1024；`writer_task` 写超时；先 insert 后发 SYN；`send_async` 背压 + 读缓冲复用；重连指数退避 3→24s；删除 SYN+ACK 忽略分支 | BUG-2/3/4/5/6/9 + O1/O3/O4 |
| `Cargo.toml` | 版本 1.10.3 → 1.10.4 | 发布 |
| `README.md` | reassembler `chunk_size` 默认值修正为 65535；协议节补充 FIN 半关闭语义 | O8 |

### 行为变化

- **修复**: 客户端半关闭（发完请求即关写端）不再导致服务器尾响应丢失——FIN 触发 egress 写端精确半关闭，读端持续到服务器 EOF 才回 FIN
- **修复**: 重排窗口溢出不再让连接静默挂死——立即重置并回 RST（原来只是统计不虚高，连接仍永久阻塞）
- **修复**: 纯单向 UDP 客户端不再被 60s idle 误杀；并发 UDP ASSOCIATE 不再互相覆盖
- **修复**: 重复 SYN 不再泄漏 egress 连接；SYN 发出前 conn 已注册，早期 RST 不再丢失
- **修复**: 隧道/egress/客户端写均带 60s 超时，对端卡死不阻塞任务；reassembler accept 瞬态错误不再杀死监听
- **改进**: DATA 发送改为真实背压（等待隧道容量，30s 超时兜底）并按队列余量选隧道；客户端/egress 通道有界（512），慢客户端触发重置而非 OOM
- **改进**: 晚到 DATA 对已关闭 conn 回 RST（closed 墓碑 60s），不再积压 30s 僵尸 pending 条目
- **改进**: 每帧 payload 精确分配（复用读缓冲），在途帧不再携带 64KB 底座
- **移除**: SYN+ACK 帧（splitter 从不使用）

### 测试结果

- `cargo test`: 15/15 passed（新增 reorder 1 个、tunnel 3 个，移除 flags_composition 1 个）
- `cargo clippy --all-targets`: 0 warnings
- `cargo build --release`: 成功

---

## Phase 8: 线上分析 Bug 修复 (2026-07-28)

> **基线**: v1.10.0  
> **参考**: `ONLINE_ANALYSIS_REPORT.md` (10h splitter 运行日志分析)

### 修改文件

| 文件 | 修改内容 | 原因 |
|------|----------|------|
| `src/reorder.rs` | `push()` 返回值从 `Vec<Bytes>` 改为 `PushResult { ready, accepted }` | ISSUE-003: 满缓冲时静默丢弃帧，调用者无感知 |
| `src/splitter.rs` | `VirtConn::on_frame()`: 检查 `accepted` 后更新统计，丢弃帧不计数 | ISSUE-003/005 |
| `src/reassembler.rs` | DATA handler: 检查 `accepted` 后更新统计 | ISSUE-003/005 |
| `src/reassembler.rs` | 待处理帧 drain: 适配新 API | ISSUE-003 |
| `src/splitter.rs` | `FIN_GRACE_MS`: 500 → 3000 | ISSUE-004: 慢隧道下 FIN/DATA 乱序风险 |
| `src/splitter.rs` | `handle_inbound_frame`: TIME_WAIT 中的 DATA 帧发出 WARN | ISSUE-004: 数据丢失可观测 |
| `src/splitter.rs` | `handle_tcp_client`: 关闭日志新增 `reason` 字段 (eof/remote_fin/timeout/read_error/no_tunnel) | ISSUE-006: 连接关闭原因追踪 |

### 行为变化
- **修复**: `ReorderBuf` 满后返回 `accepted: false`，调用者不再错误更新统计
- **修复**: 丢弃帧的 `bytes_recv`/`frames_recv`/`last_active` 不再被错误更新
- **改进**: FIN_GRACE 从 500ms 增加到 3000ms，降低慢隧道数据丢失风险
- **新增**: TIME_WAIT 内到达的 DATA 帧产生 WARN 日志（监控用）
- **新增**: 关闭日志带 `reason` 字段，便于区分关闭路径

### 测试结果
- `cargo test`: 12/12 passed
- `cargo clippy -- -D warnings`: 0 warnings
- `cargo build --release`: 成功

---

## Phase 1: 低风险清理

### 修改文件

| 文件 | 修改内容 | 原因 |
|------|----------|------|
| `src/frame.rs` | 移除 `AckInfo` struct 及其 `decode()` 方法 | v1.8.1 已废弃 ACK 流控，死代码 |
| `src/frame.rs` | 移除 `Frame::ack()` 构造器 | 同上 |
| `src/frame.rs` | 移除 `PROTO_UDP` 常量 | 未使用 |
| `src/frame.rs` | 移除 `ack_roundtrip` 测试 | 依赖已删除代码 |
| `src/frame.rs` | 新增 `UDP_CONN_ID`, `MAX_REORDER_WINDOW`, `MAX_PENDING_CIDS` 公共常量 | 统一常量定义 |
| `src/splitter.rs` | 移除本地 `UDP_CONN_ID`, `MAX_PENDING_ENTRIES` | 改用 frame.rs 共享常量 |
| `src/splitter.rs` | 修复 `clone_on_copy` (L591) | clippy 警告 |
| `src/splitter.rs` | 修复 `collapsible_if` (L592) | clippy 警告 |
| `src/reassembler.rs` | 移除本地 `UDP_CONN_ID`, `MAX_PENDING_ENTRIES`, `MAX_PENDING_CIDS` | 改用 frame.rs 共享常量 |
| `src/reassembler.rs` | 更新导入，移除未使用项 | 编译清理 |

### 行为变化
- **无** — 纯代码清理，不影响运行时行为

### 测试结果
- `cargo test`: 8/8 passed
- `cargo clippy`: 3 warnings (too_many_arguments, 将在后续 Phase 解决)

---

## Phase 2: 提取共享模块

### 修改文件

| 文件 | 修改内容 | 原因 |
|------|----------|------|
| `src/tunnel.rs` | **新建** — `TunnelLink`, `TunnelPool` (+ `stats()` 方法), `drain_frames()` | 消除 splitter/reassembler 中 60+ 行重复代码 |
| `src/reorder.rs` | **新建** — `ReorderBuf` | 消除 splitter/reassembler 中 30+ 行重复代码 |
| `src/main.rs` | 添加 `mod tunnel;` `mod reorder;` | 模块声明 |
| `src/splitter.rs` | 移除本地 TunnelLink/TunnelPool/ReorderBuf/drain_frames，用 imports 替代 | DRY |
| `src/reassembler.rs` | 同上 | DRY |
| `src/tunnel.rs` | `TunnelPool` 新增 `stats()` → `(alive, total)` | 封装，消除心跳中的私有字段访问 |

### 行为变化
- **无** — 纯代码移动，逻辑不变

### 测试结果
- `cargo test`: 8/8 passed
- `cargo clippy`: 3 warnings (too_many_arguments)

---

## Phase 3: Config 模块化

### 修改文件

| 文件 | 修改内容 | 原因 |
|------|----------|------|
| `src/config.rs` | **新建** — `Config`, `SplitterConfig`, `ReassemblerConfig`, `Tunnel`, `Ports`, 所有 default 函数, `find_config()`, `exe_dir()`, `parse_ports()` | 从 main.rs (204→110 行) 分离配置逻辑 |
| `src/config.rs` | 新增 4 个 `parse_ports` 单元测试 | 覆盖范围/列表/单端口/非法范围 |
| `src/main.rs` | 移除迁移到 config.rs 的所有类型和函数，添加 `mod config;` | main.rs 精简为入口逻辑 |

### 行为变化
- **无** — 纯代码移动

### 测试结果
- `cargo test`: 12/12 passed (+4 config tests)
- `cargo clippy`: 3 warnings (too_many_arguments)

---

## Phase 4: 连接生命周期修复

### 修改文件

| 文件 | 修改内容 | 原因 |
|------|----------|------|
| `src/splitter.rs` | `VirtConn` 新增 `last_active: Mutex<Instant>` | 追踪最后活跃时间 |
| `src/splitter.rs` | 新增 `TCP_IDLE_TIMEOUT` (300s) 和 `UDP_IDLE_TIMEOUT` (60s) | 自动清理僵尸连接 |
| `src/splitter.rs` | 新增 `MAX_CONCURRENT_CONNS` (4096) | 防止 DoS / OOM |
| `src/splitter.rs` | heartbeat 用 `last_active` 替代 `Arc::strong_count` 检测 | 原来依赖 Arc 引用计数不可靠 |
| `src/splitter.rs` | `on_frame()` / client read loop 更新 `last_active` | 活跃追踪 |
| `src/reassembler.rs` | `VirtConnDe` 新增 `last_active: Mutex<Instant>` | 同上 |
| `src/reassembler.rs` | 新增 `TCP_IDLE_TIMEOUT` / `UDP_IDLE_TIMEOUT` | 同上 |
| `src/reassembler.rs` | heartbeat 新增 idle timeout sweep | 同上 |
| `src/reassembler.rs` | DATA handler / egress reader 更新 `last_active` | 活跃追踪 |

### 行为变化
- **新增**: 300s 无数据 TCP 连接自动关闭 (RST)
- **新增**: 60s 无数据 UDP relay 自动关闭
- **新增**: 并发连接上限 4096，超限时拒绝新连接

### 测试结果
- `cargo test`: 12/12 passed
- `cargo clippy`: 3 warnings (too_many_arguments)

---

## Phase 5: conn_id 冲突防护 + Clippy 清零

### 修改文件

| 文件 | 修改内容 | 原因 |
|------|----------|------|
| `src/splitter.rs` | `next_conn_id` 顺序递增 → `rand::random::<u32>()` | 从根本上避免 u32 wrapping 冲突 |
| `src/splitter.rs` | 新增 `time_wait: DashMap<u32, Instant>` (TTL 60s) | 防止短时间内 conn_id 重用导致残留帧误路由 |
| `src/splitter.rs` | heartbeat 新增 TIME_WAIT 过期清理 | TIME_WAIT 条目自动回收 |
| `src/splitter.rs` | FIN/RST handler 向 time_wait 插入已关闭 conn_id | 完整的 TIME_WAIT 保护 |
| `src/splitter.rs` | `handle_inbound_frame()` 新增 `time_wait` 参数 | 传入 TIME_WAIT map |
| `src/splitter.rs` | `tunnel_read_loop()` 新增 `time_wait` 参数 | 同上 |
| `src/splitter.rs` | `handle_tcp_client()` 新增 `time_wait` 参数 | 同上 |
| `src/splitter.rs` | 新增 `ClientCtx` struct → `handle_client` 从 8 args 减为 2 args | 消除 clippy too_many_arguments |
| `src/reassembler.rs` | 新增 `ListenerCtx`, `ReadLoopCtx` struct | 消除 clippy too_many_arguments |

### 行为变化
- **新增**: conn_id 随机分配（碰撞概率 < 0.01% @1000 连接）
- **新增**: 60s TIME_WAIT 保护，防止残留帧误路由
- **新增**: TIME_WAIT 条目在心跳中自动清理

### 测试结果
- `cargo test`: 12/12 passed
- `cargo clippy`: **0 warnings**

### 重构前后对比

| 指标 | 重构前 | 重构后 |
|------|--------|--------|
| 总文件数 | 6 (.rs) | 9 (.rs) |
| main.rs 行数 | 233 | 110 |
| 模块依赖 | 扁平 | 分层 (frame/socks5 → tunnel/reorder → splitter/reassembler) |
| 代码重复 | TunnelPool ×2, ReorderBuf ×2, drain_frames ×2 | 0 |
| 常量重复 | UDP_CONN_ID ×2, MAX_PENDING ×2 | 0 |
| clippy warnings | 7 | 0 |
| 单元测试 | 8 | 12 |
| 死代码 | AckInfo + ack() + PROTO_UDP | 0 |
| conn_id 策略 | 顺序递增 + wrapping | 随机 + TIME_WAIT |
| 连接超时 | 无 | TCP 300s / UDP 60s |
| 并发限制 | 无 | 4096 |
| 孤儿检测 | Arc::strong_count (不精确) | last_active + idle timeout |

---

## Phase 6: FIN 竞态修复

### 修改文件

| 文件 | 修改内容 | 原因 |
|------|----------|------|
| `src/splitter.rs` | `VirtConn` 新增 `fin_received: AtomicBool` | 标记远程 FIN 到达 |
| `src/splitter.rs` | FIN handler: 不再立即 `conns.remove()`，只设置 `closed=true` + `notify` | 允许晚到的 DATA 帧继续被处理 |
| `src/splitter.rs` | RST handler: 保持立即移除（强制关闭） | RST = 不需要 grace period |
| `src/splitter.rs` | `handle_tcp_client`: 发送 FIN 后等 500ms grace period 再移除 conn | 给其他隧道上的 in-flight DATA 时间到达 |
| `src/splitter.rs` | heartbeat: FIN-received 连接使用 10s 短超时 | 防止远程关闭后连接永久挂起 |

### 行为变化
- **修复**: 多隧道下 FIN 先于 DATA 到达不再导致 RST（数据丢失）
- **新增**: FIN 后 500ms grace period 处理残留帧
- **新增**: FIN-received 连接 10s 自动超时清理

### 测试结果
- `cargo test`: 12/12 passed
- `cargo clippy`: 0 warnings

---

## Phase 7: 健壮性加固

### 修改文件

| 文件 | 修改内容 | 原因 |
|------|----------|------|
| `src/splitter.rs` | 新增 `shutdown: Arc<AtomicBool>` 优雅关闭信号 | 支持 Ctrl+C 安全退出 |
| `src/splitter.rs` | 隧道重连循环: 关闭信号时退出 | 防止持续重连 |
| `src/splitter.rs` | accept 循环: 关闭信号时退出 | 停止接受新连接 |
| `src/splitter.rs` | heartbeat: 关闭信号时退出 | 停止心跳 |

### 行为变化
- **新增**: Ctrl+C 触发优雅关闭（停止 accept + 停止重连 + 停止心跳）
- 注意: 活跃连接不会立即断开，需等待自然关闭或 idle timeout

### 测试结果
- `cargo test`: 12/12 passed
- `cargo clippy`: 0 warnings
- `cargo build --release`: 成功

---

## 待后续处理

以下项目需要多隧道测试环境或压测验证：

| 项目 | 风险 | 说明 |
|------|------|------|
| bounded channel 替换 unbounded | 中 | 背压机制，需压测确定 buffer 大小 |
| 出站连接池 (reassembler) | 中 | SOCKS5 连接复用 |
| spawn 失败 JoinHandle 追踪 | 低 | 任务崩溃检测 |
