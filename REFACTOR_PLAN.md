# REFACTOR_PLAN.md — Round Robin v1.9.1

> **计划日期**: 2026-07-27  
> **原则**: 每次只进行一个阶段，先低风险后高风险，每阶段后必须 cargo fmt + check + test + clippy  
> **约束**: 不改外部行为，不改线协议，不改配置格式

---

## 阶段总览

```
Phase 1: 清理低风险死代码与 clippy 警告        ← 风险最低，纯删除/重排
Phase 2: 提取共享模块，消除 DRY 重复             ← 结构性改动，但不改变逻辑
Phase 3: Config 模块化                           ← 代码组织，不影响行为
Phase 4: 连接生命周期修复 (timeout + cleanup)    ← 行为改变，但属于安全修复
Phase 5: conn_id 冲突防护                         ← 行为改变，高风险
Phase 6: FIN 竞态修复                             ← 行为改变，高风险，多隧道场景
Phase 7: 健壮性加固 (spawn/bounded/graceful)      ← 综合提升
```

---

## Phase 1: 低风险清理

### 1.1 移除死代码 `AckInfo`

- **文件**: `src/frame.rs`
- **修改**: 删除 `AckInfo` struct (L114-120), `AckInfo::decode()` (L122-131), `Frame::ack()` (L54-60), `PROTO_UDP` 常量 (L77)
- **原因**: v1.8.1 已移除 ACK 流控机制，这些代码不再使用。保留死代码误导维护者。
- **风险**: 极低 — 编译期即可验证无引用
- **验证**: `cargo check` + `cargo test`

### 1.2 修复 clippy warnings

- **文件**: `src/splitter.rs`
- **修改**:
  - L591: `ca.lock().unwrap().clone()` → `*ca.lock().unwrap()`
  - L592-596: 合并嵌套 `if let` 为 `if let Some(addr) = addr && relay2.send_to(&dgram, addr).await.is_err()`
- **原因**: 清理编译警告，保持代码整洁
- **风险**: 极低 — 等价语义变换
- **验证**: `cargo clippy` 确认 0 warning

### 1.3 统一常量定义

- **文件**: `src/frame.rs` (新增), `src/splitter.rs`, `src/reassembler.rs`
- **修改**:
  - 将 `UDP_CONN_ID` 移入 `frame.rs` 作为 `pub const UDP_CONN_ID: u32 = 0`
  - 将 `MAX_PENDING_ENTRIES` 移入 `frame.rs` 作为 `pub const MAX_REORDER_WINDOW: usize = 512`
  - splitter.rs / reassembler.rs 中通过 `frame::UDP_CONN_ID` 引用
- **原因**: 消除跨文件常量重复
- **风险**: 低 — 值不变，仅改变引用路径
- **验证**: `cargo check` + `cargo test`

---

## Phase 2: 提取共享模块

### 2.1 新建 `src/tunnel.rs` — 隧道池与链路

- **文件**: 新建 `src/tunnel.rs`
- **修改**:
  - 从 splitter.rs 和 reassembler.rs 抽取:
    - `TunnelLink` struct
    - `TunnelPool` struct + impl (new, add, compact, send, link_count)
    - `drain_frames` async fn
  - 添加 `pub` 可见性
- **原因**: 消除 60+ 行完全重复代码
- **风险**: 低 — 纯代码移动，逻辑不变
- **验证**: `cargo check` + `cargo test` + `cargo build --release`

### 2.2 新建 `src/reorder.rs` — 乱序缓冲

- **文件**: 新建 `src/reorder.rs`
- **修改**:
  - 从 splitter.rs 和 reassembler.rs 抽取 `ReorderBuf` struct + impl
  - 使用 `frame::MAX_REORDER_WINDOW`
- **原因**: 消除 30+ 行完全重复代码
- **风险**: 低 — 纯代码移动
- **验证**: `cargo test` (ReorderBuf 通过 VirtConn 的间接测试)

### 2.3 更新 splitter.rs 和 reassembler.rs

- **文件**: `src/splitter.rs`, `src/reassembler.rs`
- **修改**:
  - 移除重复定义，添加 `use crate::tunnel::{TunnelLink, TunnelPool, drain_frames};`
  - 添加 `use crate::reorder::ReorderBuf;`
- **风险**: 低 — 依赖替换
- **验证**: `cargo check` + `cargo test` + `cargo clippy`

### 2.4 更新 main.rs 模块声明

- **文件**: `src/main.rs`
- **修改**: 添加 `mod tunnel;` 和 `mod reorder;`
- **风险**: 低
- **验证**: `cargo check`

---

## Phase 3: Config 模块化

### 3.1 新建 `src/config.rs`

- **文件**: 新建 `src/config.rs`
- **修改**:
  - 从 main.rs 迁移所有 config 类型:
    - `Config`, `SplitterConfig`, `ReassemblerConfig`, `Tunnel`, `Ports`
    - 所有 `default_*()` 函数
    - `parse_ports()` 函数
    - `find_config()` 函数
  - `Tunnel` 重命名为 `TunnelConfig` 避免与 splitter 内部类型混淆
  - 添加单元测试: `parse_ports("52311-52319")`, `parse_ports("[52311, 52312]")`
- **原因**: main.rs 204 行中 ~100 行是 config 逻辑，分离后 main.rs 更清晰
- **风险**: 低 — 代码移动，无逻辑改变
- **验证**: `cargo test` + 确认 bin 运行正常

### 3.2 精简 main.rs

- **文件**: `src/main.rs`
- **修改**:
  - 移除迁移到 config.rs 的类型和函数
  - 添加 `mod config;` 和 `use config::...;`
  - main() 函数保持模式分发逻辑
- **风险**: 低
- **验证**: `cargo check` + `cargo test`

---

## Phase 4: 连接生命周期修复

### 4.1 添加连接 idle timeout

- **文件**: `src/splitter.rs`, `src/reassembler.rs`
- **修改**:
  - 在 `VirtConn` 和 `VirtConnDe` 中添加 `last_active: Instant` 字段
  - 在 `on_frame()` / DATA 处理中更新 `last_active`
  - 在 heartbeat 中添加 idle 超时清理:
    - TCP 连接: 300s 无数据 → 清理
    - UDP relay: 60s 无数据 → 清理
  - 清理前发送 RST 通知对端
  - 添加配置项 `tcp_idle_timeout_secs` 和 `udp_idle_timeout_secs`（可选，有默认值）
- **原因**: 防止客户端崩溃后的资源泄漏
- **风险**: 中 — 改变行为（新增超时断开），但属于安全修复
- **验证**:
  - 单元测试：验证 idle timeout 触发
  - 手动测试：建立连接后不发送数据，等待超时

### 4.2 添加最大并发连接数限制

- **文件**: `src/splitter.rs`
- **修改**:
  - 添加 `MAX_CONCURRENT_CONNS: usize = 4096` 常量
  - 在 `handle_client` 前检查 `conns.len() >= MAX_CONCURRENT_CONNS`
  - 超限时向客户端返回 SOCKS5 错误
- **原因**: 防止 DoS 和 OOM
- **风险**: 低 — 仅添加保护性上限
- **验证**: 单元测试验证限流逻辑

### 4.3 修复 heartbeat 孤儿检测

- **文件**: `src/splitter.rs`
- **修改**:
  - 替换 `Arc::strong_count(vc) > 1` 检测（不可靠）
  - 改用显式 `ref_count: AtomicUsize` 字段
  - 在 `VirtConn` 创建时设为 1（DashMap 持有），writer_task 启动时 +1，结束时 -1
  - heartbeat 检查 `ref_count == 1` → 孤儿
- **原因**: Arc::strong_count 不精确，可能误清理活跃连接
- **风险**: 中 — 引入显式引用计数，需确保无遗漏
- **验证**: 手动测试多连接场景下的 heartbeat 输出

---

## Phase 5: conn_id 冲突防护

### 5.1 添加 TIME_WAIT 状态

- **文件**: `src/splitter.rs`
- **修改**:
  - 在 `conns` DashMap 之外添加 `time_wait: DashMap<u32, Instant>` 
  - FIN/RST 处理时: conn_id 从 `conns` 移除 → 插入 `time_wait`（TTL 60s）
  - `next_conn_id` 分配循环: 同时检查 `conns` 和 `time_wait`
  - heartbeat 清理过期 `time_wait` entries
- **原因**: 防止 conn_id 重用后残留帧误路由到新连接
- **风险**: 中高 — 引入新状态管理，需仔细测试
- **验证**:
  - 单元测试: 模拟 wrapping 场景
  - 手动测试: 高频短连接压测

### 5.2 conn_id 分配改进

- **文件**: `src/splitter.rs`
- **修改**:
  - 从顺序递增改为 `rand::random::<u32>()` 随机生成
  - 跳过 0 (UDP_CONN_ID)
  - 随机 ID 碰撞概率 ~N²/2³²（N=1000 时为 0.01%），比 wrapping 更安全
  - 保留碰撞重试循环
- **原因**: 随机 ID 从根本上避免 wrapping 问题
- **风险**: 中 — 改变 ID 分配策略
- **验证**: `cargo test` + 冲突测试

---

## Phase 6: FIN 帧竞态修复

### 6.1 FIN 后 grace period

- **文件**: `src/splitter.rs`
- **修改**:
  - FIN 到达时不立即 `remove` conn
  - 设置 `closing` 标志，启动 5s grace timer
  - grace period 内的 DATA 帧正常处理
  - grace period 结束后或收到 RST → `remove`
  - 如果所有 DATA 都已送达，grace period 可提前结束
- **原因**: 多隧道环境下 FIN 可能比最后的数据帧先到达
- **风险**: 高 — 改变连接关闭时序
- **验证**:
  - 需要多隧道环境测试（至少 3 条隧道）
  - 模拟隧道间延迟差异
  - 验证无数据丢失

### 6.2 reassembler 侧 FIN 处理改进

- **文件**: `src/reassembler.rs`
- **修改**:
  - FIN 到达时: cancel egress 读端 → 等待 egress 写端排空 → 移除 conn
  - 当前实现已大致正确，确认 `read_from_egress` 中的 cancel 通知不会丢失数据
- **原因**: 确保 egress 连接的响应数据全部发回 splitter 后再清理
- **风险**: 中
- **验证**: 测试 FIN 后仍有响应数据的场景

---

## Phase 7: 健壮性加固

### 7.1 spawn 失败处理

- **文件**: `src/splitter.rs`, `src/reassembler.rs`
- **修改**:
  - 所有 `tokio::spawn` 保存 `JoinHandle`
  - 添加 `tokio::spawn` 失败时的错误日志和资源清理
  - 关键任务（如 tunnel read_loop）失败时触发重连
- **原因**: spawn 失败（runtime shutdown）会导致静默状态泄漏
- **风险**: 低 — 仅添加错误处理
- **验证**: `cargo clippy` + 手动测试 Ctrl+C 行为

### 7.2 用 bounded channel 替换关键路径

- **文件**: `src/tunnel.rs`, `src/splitter.rs`, `src/reassembler.rs`
- **修改**:
  - `TunnelLink.tx`: unbounded → bounded(256)
  - `VirtConn.to_client_tx`: unbounded → bounded(256)
  - `EgressConn.write_tx`: unbounded → bounded(256)
  - 满时行为: 记录 warning 并丢弃（或短暂 yield）
- **原因**: 提供背压，防止 OOM
- **风险**: 中 — 改变 channel 语义，可能影响吞吐
- **验证**: 压测确认无性能退化

### 7.3 优雅关闭

- **文件**: `src/main.rs`, `src/splitter.rs`, `src/reassembler.rs`
- **修改**:
  - 添加 `tokio::signal::ctrl_c()` 监听
  - 收到信号后: 停止 accept → 发送 RST 到所有活跃连接 → 等待任务完成（30s timeout）→ 退出
  - splitter 侧额外: 关闭所有隧道连接
- **原因**: 当前 Ctrl+C 后直接退出，无任何清理
- **风险**: 中 — 涉及 shutdown 流程
- **验证**: 启用日志后 Ctrl+C，确认 `"shutting down"` 日志 + 无 panic

### 7.4 出站连接池 (reassembler)

- **文件**: `src/reassembler.rs` 或新建 `src/pool.rs`
- **修改**:
  - 维护到 `local_target` 的 SOCKS5 连接池
  - SYN 到达时从池中获取，FIN 后归还
  - 池大小: min(tunnel_count, 32)
  - 空闲连接超过 60s 关闭
- **原因**: 减少 SOCKS5 握手开销
- **风险**: 中 — 引入连接复用，需确保状态隔离
- **验证**: 压测对比应用前后的首次字节延迟

---

## 不纳入本次重构的项目

| 项目 | 原因 |
|------|------|
| 添加 ACK 流控 | 已在 v1.8.1 移除，TUIC TCP 保证可靠传输 |
| chunksum/MD5 校验 | TUIC 层已有完整性保证 |
| 压缩 | 应用层数据通常已压缩 |
| 自适应 chunk size | 缺乏数据支撑，固定值已足够 |
| sing-box 配置重构 | 不属于 Rust 项目范围 |

---

## 验证策略

每个 Phase 完成后执行:

```bash
cargo fmt --check        # 格式检查
cargo check              # 编译检查（全模块）
cargo test               # 单元测试
cargo clippy -- -W clippy::all   # Lint 检查
cargo build --release    # 发布构建
```

Phase 4-7 额外需要:
- 手动功能测试（TCP 代理 + UDP 中继）
- 异常场景测试（隧道断连、超时、高并发）

---

## 预期成果

完成所有 Phase 后:
- `REFACTOR_ANALYSIS.md` ✅ (已生成)
- `REFACTOR_PLAN.md` ✅ (本文档)
- `CHANGELOG.md` (Phase 完成后生成)
- `TEST_REPORT.md` (每 Phase 后累积)
- `cargo build --release` 成功
- `cargo clippy` 0 warning
- `cargo test` 全部通过
