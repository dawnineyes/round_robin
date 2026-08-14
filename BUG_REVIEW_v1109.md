# BUG_REVIEW_v1109.md — round_robin v1.10.9 Bug 审查报告（第四轮）

> 审查日期：2026-08-14
> 审查对象：v1.10.9（Phase 14 加权 DATA 调度器之后）
> 审查方式：逐文件静态审查（config / frame / reorder / socks5 / tunnel / splitter / reassembler / main / logging / lib / tests/e2e）+ 全量构建验证。
> 结论：共发现 **6 个新问题（3 中 / 3 低）**，编号 B41–B46，**已全部修复**。

## 修复状态（2026-08-14 更新）

**B41–B46 已全部修复。** 验证：`cargo build --all-targets` 通过、`cargo clippy --all-targets -- -D warnings` 0 警告、`cargo fmt` 通过、41/41 单元测试 + 4/4 e2e 集成测试通过（新增 4 个回归测试：`syn_connect_failure_tombstones_cid`、`egress_send_failure_resets_conn`、`send_async_waits_for_new_link`、`syn_connect_stall_does_not_block_other_cids`）。版本 1.10.9 → 1.10.10（CHANGELOG Phase 15）。

修复要点：

- **B45**：`TunnelPool` 增加 `added: Notify`（`add()` 时 `notify_waiters`）；`send_async` 在"无任何活链路"分支等待新链路加入（调用方 `DATA_SEND_TIMEOUT` 30s 兜底），不再瞬间返回 false。
- **B42**：`read_from_egress` 增加 `send_failed` 状态——响应帧发送失败即 fail-fast（closed 墓碑 + 移除 conn + 尽力 RST），不再走"FIN 送入永远填不上的 seq 空洞"流程；FIN 发送失败补 RST 兜底；发送超时经 `EgressReaderCtx.data_send_timeout` 注入（默认 30s，测试 100ms）；统计改用任务持有的 `vconn` Arc（顺带去掉每块一次 DashMap 查找）。
- **B41**：SYN 处理器的 egress 连接失败/超时两分支补 `closed` 墓碑，与 SYN decode 失败 / proto 非法 / UDP bind 失败路径对齐。
- **B43**：`handle_client` 分配的 conn_id 直接传入 `handle_udp_client`，删除 UDP 路径的第二次随机分配。
- **B44**：UDP keepalive 监听任务保存 `JoinHandle`，中继循环先结束时 `abort()`——心跳清扫/RST 后任务与控制 TCP 连接不再泄漏至客户端关闭。
- **B46**：新增 `dispatch_frame`——SYN 帧在带 `Semaphore` 上限（64）的并发任务中处理，egress connect（≤10s）不再头部阻塞隧道读循环；`handle_frame` 无任何错误传播（`?`），spawn 化安全；SYN 洪泛时自动降级为内联处理（旧行为）。

## 与上一轮的关系

上一轮 BUG_REVIEW_v1107.md（v1.10.7）的 B33–B40 已在 v1.10.8 全部修复，本轮复核无回归（pending 逐出/清扫的 RST 语义、未知 cid 墓碑、推迟成功应答、conn_id 分配时机均按修复后语义工作）。B21–B32、B1–B20、D1/D3 状态不变。本报告针对 v1.10.9 当前代码做第四轮审查，编号从 B41 继续。

## 结论摘要

| 编号 | 严重度 | 位置 | 一句话 |
|------|--------|------|--------|
| B41 | 低 | reassembler.rs SYN egress connect 两分支 | 连接失败/超时不写 closed 墓碑 → 迟到的 DATA 帧生成僵尸 pending 条目（30s 内占用字节预算、可逐出健康条目）后才被清扫+RST |
| B42 | 中 | reassembler.rs read_from_egress | 响应帧发送失败仍走 FIN 流程 → splitter 侧 reorder 出现永远填不上的 seq 空洞，客户端挂起至 60s 静默超时；FIN 发送失败无 RST 兜底 |
| B43 | 低 | splitter.rs handle_client | UDP 路径丢弃已分配的 conn_id 再分配一次——冗余随机分配（无功能影响，纯清理） |
| B44 | 低 | splitter.rs handle_udp_client | 中继先于客户端结束（心跳清扫/RST）时 keepalive 监听任务泄漏：任务 + 控制 TCP socket 存活至客户端主动关闭 |
| B45 | 中 | tunnel.rs send_async | 无任何活链路时瞬间返回 false → 全部隧道短暂同时中断（重连 3s 窗口）即截断所有在途连接传输，违背调用方 30s 超时语义 |
| B46 | 中 | reassembler.rs tunnel_read_loop | SYN 的 egress connect（≤10s）内联处理 → 同隧道所有连接的帧处理被头部阻塞（每连接多 10s 级延迟抖动，慢目标/不可达目标时常态化） |

---

## 中严重度

### B46 — SYN 处理头部阻塞隧道读循环：一次慢连接拖累同隧道所有连接

- 位置：src/reassembler.rs `tunnel_read_loop`（原 `handle_frame(frame, &ctx).await?` 内联调用）。
- 触发条件：任何 SYN 的 egress connect 变慢——目标不可达（连接超时 10s）、目标端 SYN backlog 满、SOCKS 代理慢。`handle_frame` 的 SYN 分支在隧道读循环内**同步 await** 整个 connect（`tokio::select!` 与 10s timeout）。
- 影响：该隧道上的所有其他 cid 的 DATA/FIN/RST 处理全部排队等待，每连接注入最长 10s 的延迟抖动。多隧道轮询只能缓解不能消除（每帧最多 64KB，同一隧道上的相邻帧属于不同连接是常态）。UDP SYN 的 pending 排空（含 DNS 解析）同样阻塞。
- 修复：新增 `dispatch_frame`：SYN 帧经 `try_acquire_owned`（`Semaphore`，上限 `MAX_CONCURRENT_SYN_HANDSHAKES = 64`）后在独立任务中执行 `handle_frame`；上限耗尽（SYN 洪泛）时降级为内联处理。安全性论证：① `handle_frame` 无 `?` 错误传播（所有失败路径内部消化并 `Ok(())`），spawn 吞错误无影响；② 每 cid 的 SYN 幂等由 `handshaking` DashMap 保证（duplicate SYN 直接忽略），不因并发重复建连；③ SYN 与 DATA/FIN 的竞态本就是多隧道常态——pending 缓冲 + reorder + vanished-entry 重派发（B27）与 RST-cancel Notify（B4）即为此设计；④ 帧统计在 dispatch 返回后照常累计。
- 回归测试：`syn_connect_stall_does_not_block_other_cids`（stalling SOCKS 代理：SYN A 的 connect 挂起时，dispatch 即时返回、cid B 的 DATA 即时入 pending；释放后 A 的 conn 如期建立）。

### B45 — `send_async` 在无活链路时瞬间失败：隧道短暂全断截断所有在途传输

- 位置：src/tunnel.rs `TunnelPool::send_async` 的 None 分支（原 `None => return false`）。
- 触发条件：所有隧道链路的 `alive` 同时为 false。这是常态事件而非极端事件——网络抖动打断所有隧道时，splitter 的每条隧道重连需 3s+（`retry_count == 0 → 3s`），期间 pool 里只剩死链路（尚未被 60s 心跳 compact）。此时任何 `send_async(DATA)` 调用经 `weighted_pick` → None → `best` 为 None → **立即返回 false**。
- 影响：调用方（splitter 客户端读循环、reassembler egress 读循环）收到 false 后立即 `break`/abort——3 秒的抖动就重置**全部**在途连接（splitter 侧 close_reason="no_tunnel" → FIN 失败 → RST；reassembler 侧响应流断裂）。`DATA_SEND_TIMEOUT=30s` 的"等待隧道恢复"语义形同虚设：超时只约束阻塞在满队列上的情况，无活链路时永远等不到超时。
- 修复：`TunnelPool` 增加 `added: Notify`，`add()` 时 `notify_waiters()`；`send_async` 的 None 分支改为 `self.added.notified().await` 后重选。重连（≤3s 正常路径）落在等待窗口内即恢复发送；真故障由调用方 30s 超时兜底。`Notify` 在无等待者时存储一个 permit，`add()` 与 `notified()` 之间无竞态窗口。
- 回归测试：`send_async_waits_for_new_link`（无链路时挂起、加入链路后立即完成）；`send_async_without_links_fails` / `send_async_fails_over_closed_channel` 改为超时包裹（语义不变：无链路时等待，超时后 false）。

### B42 — egress 响应发送失败仍走 FIN 流程：seq 空洞 + 客户端 60s 挂起

- 位置：src/reassembler.rs `read_from_egress` 的 `!sent` 分支与结尾 FIN 流程。
- 触发条件：egress 读循环中任一响应帧 `send_async` 超时失败（30s 无活隧道）。旧代码仅 `warn!` + `break`，随后照常走 FIN 流程：`pool.send_async(Frame::fin(conn_id, seq))`（`seq` 已跳过被丢帧）→ 失败时仅告警，无 RST 兜底。
- 影响：被丢弃的响应帧在 splitter 侧 reorder 留下**永远填不上的空洞**（TCP 隧道不重传）。splitter 收到 FIN 后 grace 循环等 15s（CLOSE_GRACE_MAX）才放弃；若 FIN 也没送达（同为无隧道），客户端挂满 60s（CLOSE_QUIET_TIMEOUT）才拆除。两个方向都无错误信号，客户端看到的是"截断响应 + 延迟 EOF"而非连接重置。对照 splitter 侧等价路径（`handle_tcp_client` FIN 失败 → RST 兜底），reassembler 侧缺失对称处理。
- 修复：新增 `send_failed` 状态。发送失败时：`closed.insert` 墓碑 + `conns.remove` + `vconn.cancel.notify_one()` + 尽力 `pool.send(Frame::rst(cid))`，直接返回——不再发送注定制造空洞的 FIN。FIN 发送失败补 RST 兜底（隧道在超时后恢复的场景下客户端即时感知连接已断）。顺带把发送超时经 `EgressReaderCtx.data_send_timeout` 注入以便测试（默认 30s）。
- 回归测试：`egress_send_failure_resets_conn`（真实 TCP 对 + 死链路池 + 100ms 超时：喂入响应数据 → 断言 cid 被墓碑化且 conn 被移除）。

---

## 低严重度

### B41 — SYN egress 连接失败/超时路径不写 closed 墓碑：迟到 DATA 制造僵尸 pending 条目

- 位置：src/reassembler.rs SYN 处理器的 `connect_fut` select 两分支（`Ok(Err(e))` / `Err(_)`）。
- 触发条件：egress 连接失败（目标拒绝/不可达/10s 超时）→ 该 cid 发 RST 给 splitter。但这两分支只做 `remove_pending` + `handshaking.remove` + RST，**不写 `closed` 墓碑**——与同函数内其余失败路径（SYN decode 失败、proto 非法、UDP bind 失败，均写墓碑）不一致。
- 影响：RST 到达 splitter 前，其他隧道上仍有该 cid 的 DATA 帧在途。这些帧到达时：conns 无、pending 无、closed 无 → 走"DATA-before-SYN"路径**新建 pending 条目**，缓冲最多 256 帧/字节预算 30s（`PENDING_TTL_SECS`）后才被 `sweep_stale_pending` 清扫+RST。僵尸条目期间占用全局 64MB 预算，`try_reserve_pending` 为满足新连接预算还可能**逐出健康连接的 pending 条目**（B33 语义：逐出即 RST 无辜连接）。
- 修复：两分支补 `ctx.closed.insert(cid, Instant::now())`。迟到 DATA 立即走 `closed` 分支收到确定性 RST，不再产生僵尸条目。
- 回归测试：`syn_connect_failure_tombstones_cid`（local_target 指向无监听端口，连接即拒；断言 RST + 墓碑 + 无 handshaking/pending 残留）。

### B44 — UDP keepalive 监听任务泄漏：中继结束后任务与控制连接存活至客户端关闭

- 位置：src/splitter.rs `handle_udp_client` 的 keepalive 监听任务（B31 引入）。
- 触发条件：UDP 中继先于客户端结束——心跳 60s 空闲清扫或远端 RST 使主循环 break 并清理 conn。此时 keepalive 监听任务仍持有控制 TCP `TcpStream` 持续 `read()`，直到**客户端**主动关闭连接才退出。
- 影响：每关联泄漏一个任务 + 一个 TCP socket（客户端可无限保持控制连接打开）。清理路径（time_wait 墓碑 + conn 移除 + RST）已执行，唯独忘了这个旁观任务。
- 修复：保存 `JoinHandle`（`ka_task`），中继循环结束时 `abort()`。正常路径（keepalive EOF → 主循环 break）任务已退出，abort 为无害 no-op；abort 同时关闭控制 TCP 连接，客户端立即感知中继已死。

### B43 — UDP 路径 conn_id 双重分配

- 位置：src/splitter.rs `handle_client` / `handle_udp_client`。
- 说明：B40 修复后 conn_id 在握手完成后由 `handle_client` 统一分配，但 `handle_udp_client` 旧签名不接收该 id，内部又调用 `alloc_conn_id` 再分配一次——`handle_client` 分配的那个被静默丢弃。无功能影响（检查 conns/time_wait 的分配逻辑正确），属冗余随机分配与重复检查。
- 修复：`handle_udp_client` 增加 `conn_id` 参数，`handle_client` 传入已分配 id；删除内部 `alloc_conn_id` 调用。

---

## 复核确认无问题的候选（分析后不改）

本轮审查中还逐一排查了以下疑点，确认现有实现已覆盖或有界，记录备查：

1. **egress 写停滞（60s 超时）后的连接处理**：`write_to_egress` 退出 → `rx` 落盘 → `EgressConn.write_tx` 所在通道关闭 → 下一个 DATA 帧 `egress.write()` 立即失败 → 现有 fail-fast 路径（墓碑 + 移除 + RST）接管。无数据续到时由 egress EOF/300s 空闲清扫兜底。**无需改**。
2. **`write_to_egress` half_close 排空与 DATA 并发**：`close_write_half` 在持有 reorder 锁时判定 completeness，全部 ready 块在锁内已入通道，排空循环不会漏帧。**无需改**。
3. **RST 与 `conns.insert` 竞态下的 pending 条目**：SYN 处理器随后仍会 `remove_pending`（drain 路径）退还预算，已取消连接上的排空写入被通道关闭阻断。语义正确。**无需改**。
4. **`try_reserve_pending` / `sweep_stale_pending` 的预算退还竞态**：逐出与清扫都在 DashMap shard 锁内取条目，`remove_pending` 对同 shard 的并发删除被串行化，无双重退还。**无需改**。
5. **锁序**：全库一致的"DashMap shard → reorder → last_active"顺序，无反向嵌套。**无死锁**。
6. **`FrameDecoder` 缓冲上界**：`reserve` 的几何增长使单次读最多把 `buf.len()` 推过 `MAX_DECODER_BUF` 约一个增长步长（≤~16KB），随后立即 bail；容量随最大帧收敛于 ~65KB。内存有界。**无需改**。
7. **splitter FIN 清扫与 reassembler 的半关闭联动**：清扫后 handler 仍会补发 FIN，reassembler `start_half_close` → `finish_if_done` 正常收敛。**无需改**。

## 优化建议（未实施，记录待排期）

1. **UDP vconn 的 `EgressConn` 死通道**：UDP 关联构造 cap=1 的 mpsc 通道但从不使用（每关联浪费一次小分配）。可改为 `Option<EgressConn>` 或拆类型，纯清理，收益极微。
2. **超时可配置化**（原 OPTIMIZATION_PLAN O5）：`DATA_SEND_TIMEOUT` 等常量经 B42 已可在任务上下文注入，进一步下沉到 config.toml 属顺手之事。
3. **`queue_depth` 指标含死链路队列**：心跳监控值在链路死亡未被 compact 前偏高，仅观测问题。
4. **UDP DATA 转发的 DNS 解析仍内联于隧道读循环**（B37 只解除了 DashMap shard 锁）：域名目标的 `send_to` 内 DNS 解析会短暂阻塞同隧道帧处理。可 spawn 化（UDP 无顺序语义），但引入数据报重排，收益有限，暂不实施。

## 验证基线

- `cargo build --all-targets`：通过
- `cargo clippy --all-targets -- -D warnings`：0 警告
- `cargo fmt`：通过
- `cargo test`：41/41 单元测试（新增 4 个回归）+ 4/4 e2e 集成测试通过
- 改动文件：`src/tunnel.rs`、`src/reassembler.rs`、`src/splitter.rs`、`Cargo.toml`（1.10.10）、`CHANGELOG.md`（Phase 15）、本报告
