# OPTIMIZATION_PLAN.md — round_robin v1.10.4 项目优化方案

> 配套文档：`BUG_REVIEW.md`（20 个 bug 详单）。本方案给出分优先级、可执行的优化路线，标注每项对应 bug 编号与预期收益。
> 基线验证：`cargo build` 通过、`cargo clippy --all-targets` 0 警告、`cargo test` 15/15 通过。
>
> ## 执行状态
>
> - **v1.10.5（已实施）**：P0（F1/F2/F3）✅；P1（F4–F9）✅；P2 的 B9/B11–B18/B20 ✅
> - **v1.10.6（已实施）**：D1 隧道故障快恢复 ✅（drain_frames 上报丢失帧 → splitter 重置连接/重发控制帧、reassembler 回 RST）；D3 splitter 侧半关闭 ✅（远端 FIN 后继续转发，egress 保留至双方 FIN，`finish_if_done` 三条件拆除）；O1 ✅（read_buf 零拷贝解码）；O2 ✅（drain_frames 复用编码缓冲）；O4 ✅（codegen-units=1 + strip）；E1 ✅（CI clippy -D warnings + 全量测试 + release 门槛）；E5 ✅（示例端口统一 52030-52040 段）；可观测性 ✅（心跳 resets/queue_depth/half_open/time_wait）；新增 D1/D3 端到端回归测试（e2e 共 4 个）
> - **仍开放**：O3（send_async 选择器优化，当前实现正确、收益有限）、O5（超时可配置化）、E2/E3（常量集中/统计字段抽取，纯重构）、E4（启动自检）、Prometheus 端点、D4（UDP ICMP 透传）

## 优先级总览

| 阶段 | 内容 | 目标 |
|------|------|------|
| **P0（1-2 天）** | 修复 B1、B2、B3（数据丢失/挂起类） | 正确性 |
| **P1（1 周）** | 修复 B4-B8、B10；补端到端集成测试 | 健壮性 + 防回归 |
| **P2（1-2 周）** | 修复 B9、B11-B20；性能优化 O1-O4 | 工程完备 |
| **P3（可选）** | 可观测性、协议增强（UDP 多客户端、故障快恢复） | 长期演进 |

---

## P0 — 正确性修复（对应 BUG_REVIEW 高严重度）

### F1. reassembler：FIN/RST 在 SYN 握手期间排队（B1、B4）

把 `handle_frame` 的 FIN/RST 分支改为与 DATA 分支一致：conn 不存在且不在 `closed` 时写入 `pending`（控制帧）。SYN 完成后 drain 时：
- queued FIN → `start_half_close(vconn, seq, cid)`（drain 逻辑 `reassembler.rs:489-496` 已就绪，仅差入队侧）；
- queued RST → 标记取消：egress connect 放进 `tokio::select!` 与 cancel Notify 竞争，命中即中止并回 RST，避免为已死连接建 egress。

**收益**：消除多隧道异构延迟下的连接挂起（最长 300s）与 egress 泄漏；HTTP/SMTP 等依赖 EOF 的协议不再假死。

### F2. splitter：按 FIN.next_seq 精确半关闭（B2）

`handle_inbound_frame` FIN 分支（splitter.rs:412-423）记录 `fin_seq`；客户端循环退出后不按固定 3s 移除，而是：
1. 发 FIN 回执；
2. 每次 DATA 送达后检查 `reorder.is_complete_through(fin_seq)`；
3. 完成后（或 15s 兜底超时）再移除 conn → TIME_WAIT。
heartbeat 的 FIN 清理路径（:221-236）同步改为依赖该状态。

**收益**：慢/抖动隧道下响应不再被静默截断；`TIME_WAIT` 的 "possible data loss" 警告从常态变为异常。

### F3. UDP：旁路重排 + seq 只在成功发送后递增（B3、B8-UDP）

1. splitter 侧 `handle_inbound_frame` 对 `UDP_CONN_ID` 的 DATA 直接 `to_client_tx.try_send`，不进 `ReorderBuf`（数据报无顺序语义）；
2. reassembler 侧 `udp_seq` 移到 `pool.send()` 成功之后递增；
3. 溢出重置路径对 UDP 失效，不再有"一个丢包毁掉整个中继"。

**收益**：UDP 中继在丢包/隧道抖动下持续可用；DNS 等流量稳定性显著提升。

---

## P1 — 健壮性修复

### F4. 握手超时 + 半开连接计数（B6）

- splitter：`socks5_server_accept` 整体包 `timeout(15s)`；
- accept 循环的并发上限改为"conns.len() + 半开计数"（独立 AtomicUsize，握手开始 +1、结束 -1）。

### F5. pending 字节预算（B7）

全局 `AtomicUsize` 计 pending 总字节（上限建议 64MB），超预算时丢弃最旧 CID 条目并回 RST；`MAX_PENDING_FRAMES_PER_CID` 保留为第二道闸。

### F6. close_write_half 状态机（B5）

`half_closed: AtomicBool` → `AtomicU8`（0=open / 1=closing / 2=closed）或 `Mutex<bool>`：force 兜底在 closing 状态下同样推进到 closed，消除竞态窗口。

### F7. 重排窗口按字节计（B8）

`ReorderBuf` 增加 `max_bytes`（建议 8MB/连接，`chunk_size` 相关配置项），`pending` 总字节超限即 `overflow=true`（复用现有重置路径）。收益：内存峰值从 32MB/连接降至 8MB/连接，且隧道故障后的停滞期缩短 4 倍。

### F8. SOCKS 应答与 SYN 时序（B10）

SYN 发送失败时向客户端回 `REP_GENERAL_FAILURE`（0x01）再关闭；或延后成功应答至 SYN 发出后（增加 ~1RTT 延迟，作为可配置项）。

### F9. 端到端集成测试（防回归，覆盖 B1/B2/B3 类竞态）

在 `tests/` 增加本地双端集成测试（无需真实 sing-box，tunnel 直连）：
1. **乱序注入**：tunnel 写入器按乱序帧喂给 reassembler，断言 egress 收到的字节流严格有序；
2. **FIN 竞态**：SYN 走慢隧道、FIN/DATA 走快隧道（手工控制写入顺序），断言 egress 收到完整数据 + EOF；
3. **FIN 截断回归**：splitter 侧 FIN 先于尾部 DATA 到达，断言客户端收到完整响应（对应 F2）；
4. **UDP 丢包恢复**：丢弃一个响应帧后继续发送，断言中继仍工作（对应 F3）；
5. **隧道断连**：杀一条隧道，断言连接在窗口溢出后重置且无 panic/泄漏。

## P2 — 工程与性能

### 性能优化

| 编号 | 位置 | 做法 | 预期收益 |
|------|------|------|----------|
| O1 | frame.rs:160-209 | 用 `AsyncReadExt::read_buf` 直接读入 `BytesMut`，删除 8KB 栈缓冲 + `extend_from_slice` 拷贝 | 热路径少一次全量拷贝 |
| O2 | tunnel.rs:138-153 | `drain_frames` 复用一块 `BytesMut` 编码缓冲（每帧 reset） | 消除每帧一次堆分配 |
| O3 | tunnel.rs:99-129 | `send_async` 的 least-loaded 选择改为 `RwLock<Vec>` 或每次只锁一次快照；选出的链路满时降级为"任意 alive 且未满"避免单点排队 | 高并发下减少锁竞争与队头阻塞 |
| O4 | Cargo.toml | release 增加 `codegen-units = 1`；服务场景可选 `panic = "abort"`（配合重启策略） | 体积/冷启动小幅优化 |
| O5 | 多处 | TCP 读写超时（60s/30s/10s…）与心跳 60s 收敛为可配置常量/TOML 项 | 运维可调 |

### 工程化

- **E1** CI 补 `clippy -D warnings` + `cargo test`（现 workflow 仅 release 构建）；
- **E2** 常量集中（`TUNNEL_CHANNEL_CAP`、各类 timeout 分散在 4 个文件）→ `src/constants.rs` 或 config 化；
- **E3** splitter/reassembler 的 VirtConn 统计字段组（bytes_sent/recv、frames_sent/recv、created_at、last_active）抽取为共享结构体，消除重复与漂移；
- **E4** 配置校验集中：端口去重（B14）、tunnel 目标 DNS 预解析告警、启动自检（本地 egress SOCKS 探测）；
- **E5** 文档同步：README（52035）与 config.example（52030-52039）、config.toml（52310-52314）端口族不一致，统一并注明默认值来源（config.rs:70-80）；
- **E6** 优雅关闭：unix 增加 SIGTERM 处理（B13）；splitter 等待首隧道循环加 shutdown 检查（B9）；install.sh 设 `KillSignal=SIGINT` 或 TimeoutStopSec；
- **E7** 日志：`DailyWriter` 完整写（B16）；心跳改为 `tracing` filter 可控级别。

## P3 — 可观测性与协议演进（可选）

### 可观测性

1. **指标**（日志结构化输出即可，无需引入新依赖）：
   - 每隧道：queue depth（`tx.capacity()` 差值）、bytes/frames、重连次数；
   - 每连接：reorder pending 数/字节、gap 大小、reset 原因（overflow/egress_full/client_full/timeout）；
   - 全局：pending CID 数、TIME_WAIT 数、UDP 丢报计数。
2. 心跳日志增加 `reorder_backlog`、`reset_count` 字段，把"possible data loss"从 warn 升级为可计数指标。
3. 可选：Prometheus textformat HTTP 端点（复用现有 DashMap 统计）。

### 协议/架构演进（按收益排序）

1. **隧道故障快恢复**（D1）：TunnelLink 死亡时对 splitter 侧受影响连接主动 RST，而不是等 512 帧窗口溢出。实现：splitter 记录每连接"最后发送帧 → 隧道"映射（DashMap<u32, Vec<(seq, tunnel_id)>> 或每帧携带 tunnel 标记），隧道死亡时扫描受影响连接，gap 命中即重置。可选简化版：隧道死亡时重置所有"正在等待该隧道数据"的连接。
2. **UDP 多客户端**（B19）：按客户端地址分配独立 conn_id，去掉单客户端守卫。
3. **半关闭语义补全**（D3）：splitter 收到远端 FIN 后仅停止"读隧道→写客户端"方向，客户端写侧继续转发（需协议区分 FIN 方向，当前 FIN 帧语义为"发送方不再发"已够用，改动在 splitter 客户端循环的关闭条件）。
4. **帧级校验**（可选）：CRC/哈希字段（Flags 扩展位），防 TUIC 上层误码（收益低、成本低，视需要）。

---

## 建议实施顺序与验收标准

1. **P0**：F1+F2+F3 合入 → 跑 F9 集成测试全绿 + 手工压测（双隧道人工延迟差 5s，验证 HTTP 请求不挂死、响应不截断）。
2. **P1**：F4-F8 合入 → clippy/test 全绿；`cargo test --release` 通过。
3. **P2**：O1-O5 + E1-E7 → `cargo bloat`/简单 bench 对比吞吐；发布 v1.10.5 打 tag 走现有 Actions 流程。
4. **P3** 按线上观察数据（心跳指标上线后）决定优先级。
