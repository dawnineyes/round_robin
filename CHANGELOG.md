# CHANGELOG.md — Round Robin Refactoring

> **重构日期**: 2026-07-27  
> **基线**: v1.9.1  
> **原则**: 每次单阶段修改，cargo fmt + check + test + clippy 全部通过

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
