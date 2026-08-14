# BUG_REVIEW_v11010.md — round_robin v1.10.10 Bug 审查报告（第五轮）

> 审查日期：2026-08-14
> 审查对象：v1.10.10（Phase 15 B41–B46 修复之后）
> 审查基准：对照 Nowhere 项目（portal/conn 架构参考实现）的连接生命周期、错误传播、背压、并发原语与任务管理模式逐项复核。
> 审查方式：逐文件静态审查（config / frame / reorder / socks5 / tunnel / splitter / reassembler / main / logging / tests/e2e）+ tokio 1.52 `Notify` 源码语义核对 + 全量构建验证。
> 结论：共发现 **3 个新问题（全低）**，编号 B47–B49，**已全部修复**；另实施 3 项优化（O5/O6/O7，Phase 16）。

## 修复状态（2026-08-14 更新）

**B47–B49 已全部修复。** 验证：`cargo build --all-targets` 通过、`cargo clippy --all-targets -- -D warnings` 0 警告、`cargo fmt` 通过、48/48 单元测试 + 4/4 e2e 集成测试通过（新增 7 个测试：B47 回归 `send_async_sees_link_added_during_first_poll`、B48 回归 `write_to_egress_aborts_on_cancel`/`write_to_egress_stops_immediately_when_pre_cancelled`、B49 回归 `udp_pair_send_to_ip_literal`、优化 `queue_depth_ignores_dead_links`/`udp_syn_creates_no_egress_channel`、配置 `timeout_fields_default_and_parse`）。版本 1.10.10 → 1.10.11（CHANGELOG Phase 16）。

修复要点：

- **B47**：`send_async` 在 **pick 之前**创建 `added.notified()` future。tokio 文档保证"创建于 `notify_waiters` 调用之前的 Notified future 必被唤醒"，因此 pick 与等待注册之间的漏唤醒窗口被消除（B45 时注释声称"`notify_waiters` 无等待者时存 permit、无竞态窗口"与 tokio 实际语义不符——`notify_waiters` 不存 permit，只对已创建的 future 有效）。
- **B48**：`write_to_egress` 增加 `cancel: Arc<Notify>` 参数，biased select 取消优先、且与写操作本身竞争——RST/清扫后立即停止（含停滞中的写），不再向已被放弃的目标排空最多 512 块（~32MB）陈旧数据；同时移除 `finish_if_done` 中的 `cancel.notify_one()`（对 egress reader 的死信号，修复后反而会抢在 half-close 排空前截断 egress 流——D3 回归测试当场捕获）。
- **B49**：`UdpPair::send_to` 的域名解析包 `UDP_DNS_TIMEOUT`（5s）——DNS 卡住不再头部阻塞所在隧道的全部帧处理（提取 `resolve_target` 便于测试）。

## 与上一轮的关系

上一轮 BUG_REVIEW_v1109.md（v1.10.9）的 B41–B46 已在 v1.10.10 全部修复，本轮复核无回归。B33–B40、B21–B32、B1–B20、D1/D3 状态不变。本报告针对 v1.10.10 当前代码做第五轮审查，编号从 B47 继续。

## 结论摘要

| 编号 | 严重度 | 位置 | 一句话 |
|------|--------|------|--------|
| B47 | 低 | tunnel.rs `send_async`/`add` | `notify_waiters` 不存 permit——pick 与 Notified future 创建之间的窗口内发生的重连被漏掉，帧等待至调用方 30s 超时后重置连接（尽管隧道已恢复） |
| B48 | 低 | reassembler.rs `write_to_egress` | 写任务不监听 cancel——RST/清扫后仍向 egress 排空最多 512 块（~32MB）陈旧数据，任务与 socket 存活延长 |
| B49 | 低 | reassembler.rs `UdpPair::send_to` | 域名解析（lookup_host）无超时且内联于隧道读循环——DNS 卡住即头部阻塞同隧道所有帧 |

---

## 低严重度

### B47 — `send_async` 等待注册窗口内的重连被漏掉：帧拖满 30s 超时后连接被重置

- 位置：src/tunnel.rs `TunnelPool::add`（`notify_waiters`）+ `send_async` 的 None 分支（`self.added.notified().await`）。
- 触发条件：所有隧道链路死亡期间，`send_async` 执行 `weighted_pick`（返回 None）后、`added.notified()` future 创建之前（两者之间无 await，但多线程运行时上 `add()` 可在任意时刻穿插），恰好有隧道重连完成并调用 `add()` → `notify_waiters()`。
- 影响：tokio `Notify::notify_waiters` 在无已注册等待者时**不存储 permit**（仅 `notify_one` 存），只保证"调用前已创建的 Notified future 必被唤醒"（tokio 1.52 源码：future 创建时快照 `notify_waiters_calls` 计数，poll 时比对）。因此该次唤醒被漏掉，帧继续等待到下一次 `add()`（下一个重连）或调用方 30s 超时——超时即返回 false，连接被重置，尽管隧道早已恢复。B45 时注释声称"Notify 在无等待者时存储一个 permit，add() 与 notified() 之间无竞态窗口"与 tokio 实际语义不符（对照 splitter accept 循环的 `conn_slot` 用法：handler 退出用 `notify_one`，permits 累积，才是真正无竞态的用法）。
- 修复：`send_async` 在**每轮循环开头、pick 之前**创建 `added` future（`let added = self.added.notified();`），None 分支 await 该 future。三种时序全部闭合：① add 先于 pick → pick 直接看到新链路，无需唤醒；② add 落在 pick 与等待之间 → future 创建于 add 之前，tokio 保证其 poll 时观察到该次 `notify_waiters`（计数比对）；③ add 在等待中 → 正常唤醒。future 未 poll 即 drop（Some 分支）不注册 waiter，无 churn。
- 回归测试：`send_async_sees_link_added_during_first_poll`（spawn 后立即 add，覆盖 add 落在 pick 前/中/后的全部交错）；`send_async_waits_for_new_link` 完成时限收紧为 1s。

### B48 — `write_to_egress` 不监听 cancel：RST/清扫后仍排空最多 32MB 陈旧数据

- 位置：src/reassembler.rs `write_to_egress`（原仅监听 `rx.recv()` 与 `half_close`）。
- 触发条件：splitter 发 RST、心跳 300s/60s 清扫、或 `finish_if_done` 拆除连接时，均只 `vconn.cancel.notify_one()`——该信号只被 egress **reader** 监听；writer 依赖"vconn 最后一个 Arc drop → write_tx 关闭 → rx.recv() 返回 None"退出。而 mpsc 通道关闭后 `recv()` 仍会**先排空队列中已入队的 chunk**。
- 影响：连接被重置后，writer 继续向已被客户端放弃的 egress 目标写最多 `EGRESS_CHANNEL_CAP`（512 块 × 64KB ≈ 32MB）陈旧数据（目标服务器继续处理已死连接），每个停滞写还可各拖 60s 超时；任务与写半 socket 存活延长。对照 Nowhere 基准（CLOSE 帧触发 remove+cancel 立即停写）与 splitter 侧等价路径（writer_task 依赖通道关闭，但客户端通道是客户端自己不再读才关，语义正确），reassembler 侧缺失 cancel 感知。
- 修复：`write_to_egress` 增加 `cancel: Arc<Notify>`，biased select 取消优先；写操作本身也 select 竞争 cancel（停滞中的写不必等 60s 超时）。同时发现并移除 `finish_if_done` 中的 `cancel.notify_one()`——`egress_eof` 只由 egress reader 自己置位，该信号到达时 reader 必然已过 select 循环，属死信号；writer 开始监听 cancel 后它反而抢先 half-close 排空截断 egress 流（D3 e2e 回归当场捕获，验证了审查的必要性）。
- 回归测试：`write_to_egress_aborts_on_cancel`（写停滞中 cancel 即退）；`write_to_egress_stops_immediately_when_pre_cancelled`（预先 cancel，队列分毫未动即退）。

### B49 — UDP 域名解析无超时：DNS 卡住头部阻塞同隧道所有帧

- 位置：src/reassembler.rs `UdpPair::send_to` 的 `lookup_host` 调用。
- 触发条件：UDP DATA 帧在隧道读循环内联处理（B46 只把 SYN spawn 化）；客户端数据报目标是域名（SOCKS5 允许域名 ATYP）时，`send_to` 内 DNS 解析直接 await。解析器无响应（DNS 服务器故障、被劫持）时 `lookup_host` 无界阻塞。
- 影响：该隧道 read loop 卡死——同隧道上所有 cid 的 DATA/FIN/RST 处理全部停滞，直至重连（恶意/异常客户端发随机域名即可复现）。splitter 侧 UDP 路径无此问题（无 DNS）。
- 修复：`lookup_host` 包 `UDP_DNS_TIMEOUT = 5s`，超时该数据报失败（UDP 本尽力语义）；解析逻辑提取为 `resolve_target(host, port, timeout_dur)`。IP 字面量路径不受影响。
- 回归测试：`udp_pair_send_to_ip_literal`（IP 路径直发）。超时路径本身未单测：本机 DNS 对任意域名返回通配结果（劫持解析器），且 `lookup_host` 首次 poll 内联完成解析，wrapper 无法被观察到超时——wrapper 是 tokio 自带已测原语，风险点（无界 await）已按构造消除，见测试内注释。

---

## 复核确认无问题的候选（分析后不改）

对照 Nowhere 基准逐项复核，以下疑点确认现有实现已覆盖或有界，记录备查：

1. **其余 8 处 `Notify` 用法**：splitter `conn_slot`（`notify_one` 存 permit，permits 累积无竞态）、`VirtConn.notify`（notify_one 语义同上）、reassembler `cancel`/`half_close`/`hshake_cancel`/`stop`——全部 `notify_one`，仅 `TunnelPool::added` 一处用 `notify_waiters`（即 B47）。逐一核对 tokio 1.52 源码确认。
2. **splitter UDP 中继响应写任务失败（`send_to` Err → break）后主循环无感知**：触发条件实际不可达——写任务与主循环共享同一 relay socket，socket 故障时主循环的 `recv_from` 先失败并拆除中继；`client_addr` 为 None 的静默丢弃分支要求"先有响应后有请求"，协议上不可能。60s idle 清扫兜底。**无需改**。
3. **splitter `handle_tcp_client` 尾部 `writer_task.await` 最长 60s**：conn 已移除、连接槽已释放；writer 阻塞仅当客户端停止读（`CLIENT_WRITE_TIMEOUT` 60s 兜底），有界非泄漏。保持 join 语义（日志顺序）。**无需改**。
4. **`start_half_close` 的 10s 兜底任务在连接拆除后存活**：持有 vconn Arc 至多 10s，close_write_half 幂等。**无需改**。
5. **`dispatch_frame` SYN spawn 后的同 cid 竞态**：pending 条目在 SYN 处理器内创建、DATA 先到自建条目后由 SYN 处理器附着 cancel（entry API）；RST 无条件墓碑（B36）覆盖"SYN 任务尚未运行"窗口。**无需改**。
6. **`finish_if_done` 三条件与拆除幂等性**：remove 幂等、tombstone 幂等；`close_write_half` 状态机（0→1→2）本轮复核仍无竞态。**无需改**。
7. **锁序**：全库一致的"DashMap shard → reorder → last_active"顺序，B48 修复未引入新锁。**无死锁**。
8. **Nowhere 基准对照**：exactly-once 关闭（`closed` AtomicBool 模式）、显式重置（RST）、"先置状态再发信号"（splitter `reset_conn`：墓碑→移除→置位→notify，中间无 await）、UDP 空闲超时+Notify 活动重置（splitter/reassembler 心跳 + last_active）——round_robin 均已对齐；任务追踪方面所有 spawn 均有生命周期上界（B44 已修 keepalive 泄漏；B48 修复后 egress writer 亦然）。

## 优化建议（本轮已实施，见 OPTIMIZATION_PLAN_v11010.md）

1. **O5 超时可配置化**：`data_send_timeout_secs`（splitter + reassembler，默认 30）、`heartbeat_secs`（splitter + reassembler，默认 60）——原 OPTIMIZATION_PLAN 待办。
2. **O6 `queue_depth` 只计活链路**：死链的关闭通道 `capacity()==0`，曾把每死链按满深（128）计入指标直至下一次 compact。
3. **O7 UDP vconn 死通道清理**：`egress` 改 `Option<EgressConn>`，UDP 关联不再分配一个接收端立即被丢弃的 mpsc 通道。
4. B48 修复本身即优化：拆除路径即时停写，陈旧数据不再送达已放弃的目标。

仍开放（记录待排期）：UDP DATA 转发 spawn 化（B37 备注 4，引入数据报重排，收益有限）；`HANDSHAKE_TIMEOUT`/`CLOSE_GRACE_MAX` 等其余常量可配置化；O3 send_async 选择器再优化（当前实现正确，收益有限）。

## 验证基线

- `cargo build --all-targets`：通过
- `cargo clippy --all-targets -- -D warnings`：0 警告
- `cargo fmt`：通过
- `cargo test`：48/48 单元测试（新增 7 个）+ 4/4 e2e 集成测试通过
- 改动文件：`src/tunnel.rs`、`src/reassembler.rs`、`src/splitter.rs`、`src/main.rs`、`src/config.rs`、`tests/e2e.rs`、`config.example.toml`、`config.reassembler.example.toml`、`README.md`、`Cargo.toml`（1.10.11）、`CHANGELOG.md`（Phase 16）、本报告
