# BUG_REVIEW_v11012.md — round_robin v1.10.11 Bug 审查报告（第六轮）

> 审查日期：2026-08-14
> 审查对象：v1.10.11（Phase 16 B47–B49 修复之后）
> 审查基准：对照 Nowhere 项目（portal/conn 架构参考实现）的连接生命周期、错误传播、背压、并发原语与任务管理模式逐项复核；另对照 tokio 1.52.3 本地 registry 源码（notify.rs / macros/select.rs）逐行核对了 2 个 Notify/select 语义疑点。
> 审查方式：双审查员并行逐文件静态审查（splitter/tunnel/socks5/logging/main 与 reassembler/reorder/frame/config/e2e 两组）+ 主审查员独立复读全库 + 全量构建验证。
> 结论：共发现 **7 个新问题（1 中 / 6 低）**，编号 B50–B56，**已全部修复**；另驳回 1 个候选（经 tokio 源码核实不成立）；实施 2 项优化（O8/O9，Phase 17）。

## 修复状态（2026-08-14 更新）

**B50–B56 已全部修复。** 验证：`cargo build --all-targets` 通过、`cargo clippy --all-targets -- -D warnings` 0 警告、`cargo fmt` 通过、55/55 单元测试 + 4/4 e2e 集成测试通过（新增 7 个测试：`teardown_wakes_both_egress_tasks`、`write_to_egress_cancel_during_half_close_drain`、`tunnel_read_loop_exits_when_writer_dies`（splitter + reassembler 各一）、`duplicate_of_pending_frame_does_not_leak_bytes`、`parse_ports_rejects_huge_list`、`validate_rejects_zero_timeouts`）。版本 1.10.11 → 1.10.12（CHANGELOG Phase 17）。

修复要点：

- **B50**：egress 读/写两个任务原共享一个 `cancel: Notify`，而 `notify_one` 只唤醒一个等待者——每次 RST/清扫拆除只有其一被唤醒，另一个要么在对端 EOF 上无限期挂起（静默对端 = 任务 + fd + vconn 永久泄漏），要么退回 B48 修复前的 32MB 陈旧数据排空。修复：拆分为每任务独立 Notify（`cancel` + `cancel_writer`），7 处拆除点收敛为 `signal_teardown` helper（对两个 Notify 各 `notify_one`；单等待者 + permit 存储语义无启动竞态）。
- **B51**：`write_to_egress` 的 half_close 排空臂是 B48 修复的漏网之鱼——排空循环不竞争 cancel，拆除信号要等排空结束才被观察到。修复：排空循环每块写与 cancel 竞争（biased）。
- **B52**：`read_from_egress` 的 DATA/FIN 发送等待（`send_async`，B45 语义下可达 30s）不竞争 cancel——拆除后读任务最长多存活 30s 且可能在隧道恢复后发出一帧陈旧数据。修复：两处发送均与 cancel 竞争（biased），命中即按拆除语义退出。
- **B53**：`parse_ports` 的 `MAX_PORTS=256` 上限只作用于 Range，`Ports::List` 无上限——超大列表会 spawn 数千监听任务并刷 bind 失败日志。修复：List 与 Range 共享同一上限。
- **B54**：`ReorderBuf::push` 对"仍在窗口内的重复帧"用 `insert` 覆盖旧条目、新 payload 字节全额计入 `pending_bytes` 而不减旧值——每次重复泄漏字节预算，可提前触发溢出重置。修复：Entry API 判重，重复帧按普通重复丢弃，字节计数不变。
- **B55**：O5 新增的 `data_send_timeout_secs`/`heartbeat_secs` 允许 0 值——`heartbeat_secs=0` 使心跳任务 sleep(0) 忙等烧核并每轮全量清扫；`data_send_timeout_secs=0` 使所有 DATA/FIN 发送立即超时（连接雪崩）。修复：`SplitterConfig::validate()`/`ReassemblerConfig::validate()` 收敛 chunk_size 与零值校验（原 main.rs 内联 chunk_size 检查移入），可单测。
- **B56**：隧道读循环无存活上界——对端静默消失（无 FIN/RST）时 `decoder.try_next` 阻塞至 TCP RTO（分钟级）：splitter 侧重连循环卡在读循环 await 上、隧道槽位延迟恢复；reassembler 侧读任务与 socket 半部泄漏至 RTO。修复：`TunnelLink.writer_died: Notify`，`drain_frames` 退出时触发（写侧 60s 停滞超时即隧道死亡探针），两侧 `tunnel_read_loop` select 监听，把读循环退出上界收敛到写侧 60s。

## 与上一轮的关系

上一轮 BUG_REVIEW_v11010.md（v1.10.10）的 B47–B49 已在 v1.10.11 全部修复，本轮复核无回归。B41–B46、B33–B40、B21–B32、B1–B20、D1/D3 状态不变。本报告针对 v1.10.11 当前代码做第六轮审查，编号从 B50 继续。

## 结论摘要

| 编号 | 严重度 | 位置 | 一句话 |
|------|--------|------|--------|
| B50 | **中** | reassembler.rs `VirtConnDe.cancel` + 7 处拆除点 | egress 读/写任务共享一个 cancel Notify——`notify_one` 只唤醒一个等待者，拆除后另一个在对端 EOF 上无限期挂起或排空 32MB 陈旧数据（B48 回归） |
| B51 | 低 | reassembler.rs `write_to_egress` half_close 排空臂 | 排空循环不竞争 cancel——拆除信号要等排空结束才被观察到（B48 修复的漏网路径） |
| B52 | 低 | reassembler.rs `read_from_egress` | DATA/FIN 发送等待（可达 30s）不竞争 cancel——拆除后任务多存活 30s 且可能发出一帧陈旧数据 |
| B53 | 低 | config.rs `parse_ports` | MAX_PORTS=256 上限只作用于 Range——List 超大列表 spawn 数千监听任务 |
| B54 | 低 | reorder.rs `push` | 窗口内重复帧用 insert 覆盖旧条目、字节计数不减旧值——每次重复泄漏预算，可提前溢出重置 |
| B55 | 低 | config.rs / main.rs | O5 新增超时字段允许 0 值——heartbeat=0 心跳忙等烧核，send_timeout=0 连接雪崩，无启动报错 |
| B56 | 低 | splitter.rs / reassembler.rs `tunnel_read_loop` | 读循环无存活上界——静默消失的对端让读阻塞至 TCP RTO（分钟级），重连被延迟、任务泄漏 |

---

## 中严重度

### B50 — egress 读/写任务共享一个 cancel Notify：`notify_one` 只唤醒一个等待者

- **位置**：`src/reassembler.rs` `VirtConnDe.cancel` + 拆除点（心跳清扫 `hb_conns.retain`、RST 处理、reorder 溢出、egress 写失败、B42 发送失败、两处 pending-cancelled drain）。
- **触发条件**：B48 给 egress **写**任务加 cancel 监听后，同一个 `cancel` Notify 上出现两个等待者：写任务（外层 select + 内层写竞争，任一时刻注册其一）与读任务（`read_from_egress` 的 select）。稳态（写任务阻塞在 `rx.recv()`、读任务阻塞在 `rd.read()`）下两者同时注册。任一拆除路径（RST / 心跳清扫 / 溢出 / egress 写失败 / B42）只调一次 `cancel.notify_one()`——tokio 1.52.3 语义：从等待队列唤醒**一个** waiter（FIFO），不重复存储 permit。
- **影响**：每次拆除有一半概率（FIFO 首等待者）只唤醒其一：
  - 读任务输掉 → 写任务收不到取消，`rx.recv()` 在通道关闭前**先排空**已入队的最多 512 块陈旧数据写向已放弃的目标——正是 B48 修复前被判定为 bug 的行为，原样复现；
  - 写任务输掉 → 读任务继续阻塞在 `rd.read`，退出完全依赖目标关闭连接。目标若对 FIN 无反应（静默对端、忽略 EOF 的服务器、长轮询），读任务 + egress socket fd + vconn Arc（含最多 8MB reorder 缓冲与 512 容量通道）**无限期泄漏**（conn 已从 conns 移除，心跳清扫够不到）。
- **修复**：`VirtConnDe` 拆分为 `cancel`（读任务/UDP 响应读任务）与 `cancel_writer`（写任务）两个 Notify；新增 `signal_teardown(vc)` 对两者各 `notify_one`，7 处拆除点统一调用。每个 Notify 恒为单等待者：`notify_one` 无等待者时存 permit，任务尚未注册的启动窗口也闭合（与 `conn_slot`/`VirtConn.notify` 同模式，v11010 已核对该语义）。UDP conn 的 `cancel_writer` 无等待者，permit 随 Arc 丢弃，无害。
- 回归测试：`teardown_wakes_both_egress_tasks`（真实 TCP 对、目标永不关闭，`signal_teardown` 一次后读/写任务均须在 2s 内退出）。

---

## 低严重度

### B51 — `write_to_egress` 的 half_close 排空臂不竞争 cancel

- **位置**：`src/reassembler.rs` `write_to_egress` 的 `half_close` 分支。
- **触发条件**：FIN 半关闭通知触发、写任务进入排空臂后（该臂只在队列空时进入，排空的是**排空窗口内**由其他隧道在途 DATA 新入队的 chunk——合法常态路径），RST/心跳清扫的 cancel 到达。排空循环逐块写、每块最多等 `EGRESS_WRITE_TIMEOUT`(60s)，期间不观察 cancel。
- **影响**：已拆除连接的陈旧数据继续送达目标（有界：首个停滞写 60s 内必然 break，远小于 B48 原 32MB 排空，但同属"拆除即停写"语义的残留缺口）。
- **修复**：排空循环的每块写与 `cancel_writer` 竞争（biased，取消优先），命中即 return（wr 随任务 drop 关闭）。
- 回归测试：`write_to_egress_cancel_during_half_close_drain`（half_close 先触发 + 并发喂块任务维持排空窗口，cancel 后 2s 内必须退出）。

### B52 — `read_from_egress` 的发送等待不竞争 cancel

- **位置**：`src/reassembler.rs` `read_from_egress` 的 DATA `send_async`（B45 语义下无活隧道时等待重连，最长 `data_send_timeout` 30s）与尾部 FIN 发送。
- **触发条件**：连接被拆除时读任务恰好在发送等待中——cancel 只在 select 的 cancel 臂被轮询，而任务此刻在 read 臂内部等待隧道容量。
- **影响**：拆除后读任务与 egress fd 存活最长 30s；若隧道在等待期内恢复，还会把一帧**死 cid 的陈旧 DATA** 发给 splitter（白走一轮 RST）。
- **修复**：两处发送均改为 `select! { biased; _ = cancel.notified() => …, r = timeout(send_async) => … }`；DATA 等待命中 cancel 按 `cancelled` 拆除语义退出（不发 FIN、不置 egress_eof），FIN 等待命中则跳过 RST 兜底逻辑（拆除进行中，RST 无意义）。

### B53 — `Ports::List` 无长度上限（与 Range 上限不一致）

- **位置**：`src/config.rs:parse_ports`。
- **触发条件**：配置文件 `ports = [1, 2, …, 10000]`（手误或生成器产物）——Range 分支有 `MAX_PORTS=256` 封顶（BUG-14 的本意），List 分支直接 `v.clone()`。
- **影响**：启动期 spawn 上万监听任务、大量 bind 失败刷日志；能绑定的端口成为意外暴露的隧道监听面。
- **修复**：dedup 前对 `out.len() > MAX_PORTS` 统一 bail（Range 分支保留原有的精确错误信息）。
- 回归测试：`parse_ports_rejects_huge_list`。

### B54 — ReorderBuf 窗口内重复帧的字节计数泄漏

- **位置**：`src/reorder.rs:push`。
- **触发条件**：seq 相同的帧第二次到达且首帧仍在 pending（正常对端不产生——每帧恰好一次投递；但防御纵深上不能以"对端永远正确"为前提，且该函数无调用方约束保证）。
- **影响**：`insert` 覆盖旧条目后 `pending_bytes += payload.len()` 不减旧值，每次重复泄漏字节预算——重复帧可把计数推到 `MAX_REORDER_BYTES` 之上，后续正常帧被误判 overflow 触发整连接重置。
- **修复**：Entry API——`Occupied` 按普通重复帧丢弃（accepted=false, overflow=false），字节计数不变；`Vacant` 才做窗口/字节双预算检查。
- 回归测试：`duplicate_of_pending_frame_does_not_leak_bytes`（重复帧后 `pending_bytes` 不变、原 payload 完好交付）。

### B55 — O5 新增超时字段允许 0 值

- **位置**：`src/config.rs`（仅 serde default，无区间校验）、`src/main.rs`（原仅校验 chunk_size）。
- **触发条件**：`heartbeat_secs = 0` → 心跳任务 `sleep(Duration::ZERO)` 立即完成，忙等烧满一个核并每轮全量 retain 清扫；`data_send_timeout_secs = 0` → 隧道稍有竞争/中断时每次 DATA/FIN 发送立即超时，全部活动连接以 no_tunnel 批量重置。二者均无启动期报错（对比 chunk_size 有 512..65535 校验）。
- **修复**：`SplitterConfig::validate()`/`ReassemblerConfig::validate()` 收敛 chunk_size 校验与零值超时校验（`validate_secs`），main.rs 的两处内联 chunk_size 检查移入并可单测（O9）。
- 回归测试：`validate_rejects_zero_timeouts`（含默认值正例）。

### B56 — 隧道读循环无存活上界（静默对端僵尸至 TCP RTO）

- **位置**：`src/splitter.rs:tunnel_read_loop`、`src/reassembler.rs:tunnel_read_loop`（两者同构）。
- **触发条件**：隧道对端异常消失（断电 / kill -9 / 防火墙静默丢包）且未发 FIN/RST。写侧有 `TUNNEL_WRITE_TIMEOUT`(60s) 兜底（drain 任务写停滞即死），读侧无对应上界——`decoder.try_next` 阻塞到 TCP 栈重传耗尽（RTO 约 1–2 分钟，取决于 OS 参数）。
- **影响**：与写侧 60s 不对称：splitter 侧重连循环 await 在读循环上，隧道槽位在 RTO 期间无法重连（即使分区已恢复）；reassembler 侧读任务 + socket 读半部泄漏至 RTO。对照 Nowhere 基准（relay 显式 liveness 竞争），读侧缺少等价有界性。
- **修复**：`TunnelLink` 增加 `writer_died: Notify`，`drain_frames` 任一退出路径触发（写侧 60s 停滞超时即隧道死亡探针）；两侧 `tunnel_read_loop` 的 `decoder.try_next` 与 `writer_died.notified()` select 竞争。正常拆除顺序（读先退出 → stop → drain 退出）不受影响——writer_died 的 permit 落空随 Arc 丢弃。读循环退出上界由此收敛到写侧 60s。
- 回归测试：`tunnel_read_loop_exits_when_writer_dies`（splitter + reassembler 各一；真实 TCP 对、对端永不发送，触发 writer_died 后 2s 内必须退出）。

---

## 复核确认无问题的候选（分析后不改）

1. **`drain_frames` 的 stop 唤醒被外层 select 已注册、未 drop 的 Notified future 消费 → 重连循环永久卡死（审查员候选，疑似中）**：对照 tokio 1.52.3 本地源码逐行核实后**驳回**，三条证据闭合：(a) `macros/select.rs:633` 明确注释 "Create a scope to separate polling from handling the output"——所有分支 future 的作用域在 handler 执行**之前**结束，分支体运行时外层 stop future 已 drop；(b) `sync/notify.rs:1345-1358` `drop_notified` 在 `State::Waiting` 下把 waiter 从链表中摘除（`waiters.remove`），drop 的 future 不可能继续消费通知；(c) 更强：`notify.rs:1384-1392`——`notify_one` 的通知即使随一个未 poll 的 future 被 drop，也会**转发给下一个等待者**（`notify_locked`），无等待者时 `notify_locked` 的 EMPTY|NOTIFIED 分支把 permit 存回状态计数。故"唤醒被消费后丢失"的链条在 tokio 中不存在；内层写与 stop 竞争的既有测试（`stop_exits_and_reports_lost_frames`）也覆盖该路径。**无需改**。
2. **accept 循环 `conn_slot.notify_waiters` 不存 permit 的丢失窗口**：limit-check 与 `notified()` 创建之间仅指令级窗口，且丢失后有界恢复（下一次 handler 退出的 `notify_one` permit 或下一心跳周期的 `notify_waiters`），不构成实质问题。**无需改**。
3. **splitter `writer_task` 在 conn 拆除后排空 ≤512 块**：与 B48 结论一致——splitter 侧通道关闭对应客户端已不可读，排空是"该给客户端但仍可送达"的正常语义，且首个停滞写 60s 即 break，有界。**无需改**。
4. **`alloc_conn_id` 双 map check-then-act**：B40 已记录的固有 2⁻³² 随机界（并发 N 时生日界 N²/2³³，4096 并发约 0.2%），与 B40 评估一致。**无需改**。
5. **reassembler SYN drain 路径 egress 写失败不 fail-fast**：论证后确认不可达——drain 时通道为空且容量 512 ≥ 上限 256 帧，唯一失败模式（写任务已死）要求 cancel 已发/半个关闭已完成，而这两者分别被 `entry.cancelled` 检查与 drain 后处理覆盖。**无需改**。
6. **`close_write_half` 状态机（0→1→2 + force swap + 回滚 CAS）**：逐个交错推演（双 DATA 并发交付、force 与 rollback 竞速）均闭合。**无需改**。
7. **锁序**：全库一致 DashMap shard → reorder → last_active，本轮修改（signal_teardown、B52 内层 select）未引入新锁。**无死锁**。
8. **其余 Notify 用法**（`VirtConn.notify`、`conn_slot.notify_one`、`TunnelPool::added`、`stop`、`half_close`）：均为单消费者或 permit 累积语义，逐一核对无竞态；本轮修复后 `cancel`/`cancel_writer` 亦然。**无需改**。
9. **B47/B48/B49 修复复核**：`send_async` 的 added future 创建先于 pick、`write_to_egress` 的 cancel 竞争与 `finish_if_done` 死信号移除、`resolve_target` 5s 超时——本轮复核均仍正确。**无需改**。
10. **心跳 `resets`/`udp_sent` 的 `swap(0)` 并发丢增量**：仅影响监控精度，非功能问题（观察项，不动）。

## 优化建议（本轮已实施，见 OPTIMIZATION_PLAN_v11012.md）

1. **O8 拆除路径统一**（随 B50）：7 处 `cancel.notify_one()` 收敛为 `signal_teardown` 单一 helper，每任务独立 Notify 的不变式由 helper 保证，消除"未来新增拆除点只唤醒其一"的复发面。
2. **O9 配置校验集中**（随 B55，E4 部分落地）：`SplitterConfig::validate()`/`ReassemblerConfig::validate()` 收敛 chunk_size 与零值超时校验，main.rs 由内联检查改为调用，校验逻辑可单测。

仍开放（记录待排期）：O3 send_async 选择器（当前实现正确，收益有限）；E2/E3 常量集中/统计字段抽取（纯重构）；E4 剩余——端口 bind 失败可见性（评估：当前"部分降级 + error 日志"比 fail-fast 更抗 systemd crash-loop，保持）；Prometheus 端点；D4 UDP ICMP 透传；`HANDSHAKE_TIMEOUT`/`CLOSE_GRACE_MAX` 等常量可配置化（O5 模式已铺好）；UDP 域名解析缓存（5s 超时逐报解析已可接受）。

## 验证基线

- `cargo build --all-targets`：通过
- `cargo clippy --all-targets -- -D warnings`：0 警告
- `cargo fmt`：通过
- `cargo test`：55/55 单元测试（新增 7 个）+ 4/4 e2e 集成测试通过
- 改动文件：`src/reassembler.rs`、`src/splitter.rs`、`src/tunnel.rs`、`src/reorder.rs`、`src/config.rs`、`src/main.rs`、`Cargo.toml`（1.10.12）、`CHANGELOG.md`（Phase 17）、`OPTIMIZATION_PLAN.md`、本报告
