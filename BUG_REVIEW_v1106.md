# BUG_REVIEW_v1106.md — round_robin v1.10.6 Bug 审查报告（第二轮）

> 审查日期：2026-08-14
> 审查对象：当前工作副本 v1.10.6（Cargo.toml version = 1.10.6，CHANGELOG Phase 11 之后）
> 审查方式：逐文件静态审查（config / frame / reorder / socks5 / tunnel / splitter / reassembler / main / logging / tests）+ 全量构建验证。
> 验证基线：cargo build --all-targets 通过、cargo clippy --all-targets 0 警告、cargo test 20/20 单元 + 4/4 e2e 通过。

> ## 修复状态（2026-08-14 更新）
>
> **B21–B32 已全部修复并落地 v1.10.7**，详见 CHANGELOG.md Phase 12。
> 验证：cargo build 通过、clippy -D warnings 0 警告、27/27 单元测试 + 4/4 e2e 集成测试通过（新增 7 个回归测试：fin_sweep_decision×2、pending DATA/FIN 失败快、未知 proto 拒绝、drain stop 退出/丢帧上报×2、D3 跨心跳 e2e）。

## 与上一轮的关系

上一轮 BUG_REVIEW.md（v1.10.4）的 B1–B20 已在 v1.10.5 修复，D1/D3 已在 v1.10.6 修复（见 CHANGELOG Phase 10/11）。本报告针对 v1.10.6 当前代码做第二轮审查，共发现 12 个新问题（1 高 / 3 中 / 8 低）。编号从 B21 继续，避免与既有文档混淆。

## 结论摘要

| 编号 | 严重度 | 位置 | 一句话 |
|------|--------|------|--------|
| B21 | 高 | splitter.rs:295-322 | 心跳在“响应流完整”后立即清扫 FIN 连接，即使客户端仍在持续发送 → D3 半关闭在 >60s 的持续上传下失效 |
| B22 | 中 | reassembler.rs:477-490 | 隧道读循环结束后未调用 link.stop.notify_one() → drain 任务 + 写半部 socket 永久泄漏（每条死隧道 1 task + 1 fd） |
| B23 | 中 | reassembler.rs:912-944 | pending 缓冲丢弃 DATA 帧时不重置连接 → 该连接永久 seq 空洞，长时间停摆 + 目标端静默截断 |
| B24 | 低 | tunnel.rs:192-203 | stop 竞速丢弃在途帧且不记入 lost_frames → D1 快恢复漏掉恰好 1 帧，连接退回慢速窗口溢出恢复 |
| B25 | 低 | splitter.rs:285-339,812-813,967 | conn_id 复用竞态：idle sweep 不写 TIME_WAIT + 拆除路径无条件 remove → 理论可误删/误 FIN 新连接 |
| B26 | 低 | splitter.rs:781-808 | 关闭等待循环不感知 RST → RST 后 handler 与客户端 socket 延迟最多 60s 才拆除 |
| B27 | 低 | reassembler.rs:999-1011 | RST 与 SYN drain 的窄竞态 → 遗留 cancelled 幽灵 pending 条目，egress 未被拆除 |
| B28 | 低 | reassembler.rs:150-218 | conn 0 遗留 UDP 中继路径已是死代码（splitter 不再分配 conn 0），浪费 socket 对 + 常驻任务 |
| B29 | 低 | 多处 | 统计与观测缺口：UDP 连接不计数 bytes/frames、handshaking 无 TTL 清扫、_tunnel_idx 未用 |
| B30 | 低 | frame.rs:78-86; reassembler.rs:592 | 协议防御性校验缺口：SYN 地址 as u16 静默截断、未知 proto 按 TCP 处理 |
| B31 | 低 | socks5.rs:51; splitter.rs:911-917 | SOCKS5 兼容性：nmethods 上限 16（RFC 允许 255）；UDP 关联在 keepalive 收到 1 字节即终止 |
| B32 | 低 | splitter.rs:229-234,368-371; reassembler.rs:1030-1033 | 细节：shutdown 最长再睡 24s、连接上限忙轮询、pending 字节预算 check-then-add 非原子 |

---

## 高严重度

### B21 — 心跳在响应流完成后清扫 FIN 连接，D3 半关闭对持续上传失效

- 位置：src/splitter.rs:295-322（heartbeat retain 的 FIN 分支），特别是 :309 的 if complete || fin_idle > limit。
- 触发条件：目标发完响应后关闭写半部 → reassembler 回 FIN(seq=next_seq) → splitter 记录 fin_received。一旦响应帧全部送达（reorder.is_complete_through(fin_seq) 为 true），下一次心跳（≤60s）无条件把连接清扫：closed=true + notify + 从 conns 移除。此时客户端 handler 可能仍在读循环里持续转发数据（last_active 持续刷新，但 complete 短路使其无济于事）。
- 影响：v1.10.6 的 D3 承诺“远端 FIN 后 splitter 继续转发客户端数据”只在 FIN 后 <60s 内成立。凡是服务端提前结束响应（如拒绝大体积上传、流式场景）而客户端仍在写、且写持续超过一个心跳周期的连接，都会被中途切断：客户端收到 EOF，上传被静默截断，对端收到残缺请求。这正是 D3 想支持的半关闭场景。
- 为什么现有测试没抓到：tests/e2e.rs::client_keeps_sending_after_remote_fin 只在 FIN 后 sleep 1s 就断言，而心跳周期是 60s——测试永远跑不到清扫点。
- 修复建议：清扫条件改为“完整 + 一段静默期”：if (complete && fin_idle > 30) || fin_idle > limit。这样既快速回收响应完毕后客户端已空闲的连接（30s），又不误杀仍在活跃上传的连接。同时把该判定抽成纯函数（如 should_sweep_fin(vc, now) -> bool）配单元测试；心跳周期可配置化便于 e2e 覆盖。

---

## 中严重度

### B22 — reassembler 隧道死亡后 drain 任务与写半部永久泄漏

- 位置：src/reassembler.rs:477-490（reader 任务结束逻辑），对比 src/splitter.rs:186-189（正确示范）。
- 触发条件：隧道读侧 EOF/出错 → reassembler 的 reader 任务执行 link.alive.store(false) 后直接 return，没有调用 link.stop.notify_one()。drain_frames（tunnel.rs:170-213）只剩两条退出路径：stop notify，或 rx.recv() 返回 None。而 rx.recv() 返回 None 要求唯一的 Sender 被 drop——Sender 就放在 TunnelLink 里，而 drain 任务自己持有 link2: Arc<TunnelLink>。心跳 compact()（≤60s）清掉 pool 的 Arc 后，只剩下 drain 任务手里的 Arc → Sender 永不被 drop → rx.recv() 永久阻塞。
- 影响：只要对端是“读侧半关闭而写侧仍可用”的关闭方式（TUIC/sing-box 优雅关闭常见），每条死隧道就在 reassembler 上永久泄漏 1 个任务 + 1 个 OwnedWriteHalf（socket fd）。长稳运行的 reassembler 在周期性隧道重建下任务数与 fd 无界增长，直至资源耗尽。splitter 侧因为有 stop notify 不受影响——明显的两侧不对称缺陷。
- 修复建议：reader 任务在 link.alive.store(false) 后补一行 link.stop.notify_one()，与 splitter 完全对齐。drain 醒来后会把剩余队列帧记入 lost_frames（wrapper 任务随后重发控制帧/回 RST），并 shutdown 写半部。

### B23 — pending 缓冲丢弃 DATA 帧不重置连接：长时间停摆 + 静默截断

- 位置：src/reassembler.rs:912-914（字节预算耗尽）、:920-927（每 CID 帧数超限）、:942-944（CID 总数超限）。
- 触发条件：DATA-before-SYN 是快隧道的正常路径；当 pending 因预算/帧数/CID 上限丢弃某个 DATA 帧后，该连接在 reassembler 的重排流出现永远无法填补的 seq 空洞（TCP 隧道不重传）。后续帧全部卡在 ReorderBuf 里不交付。
- 影响：（1）连接停摆：splitter 毫无感知地继续发送，直到 8MB 窗口溢出触发重置（大上传可能停摆数分钟）或客户端 FIN 触发 10s 兜底强关；（2）目标端只收到被丢弃帧之前的残缺前缀，然后 EOF——对依赖完整请求的协议等于静默数据截断。
- 修复建议：任何 pending DATA 丢弃都立即失败快：标记该 pending 条目 cancelled + ctx.closed.insert + pool.send(Frame::rst(cid))（与 closed 墓碑路径一致），让 splitter 立即重置、客户端快速失败，而不是默默吞帧。FIN 被 pending 丢弃时同理（当前 splitter 会等 60s 静默超时才拆除）。

---

## 低严重度

- B24 — src/tunnel.rs:192-203：内层 select 中 link.stop.notified() 胜出时，已经出队的在途帧既不写出也不记入 lost_frames → D1 快恢复每次隧道死亡恰好漏掉 1 帧，受影响连接退回“等 8MB 窗口溢出再重置”的慢速恢复。建议：stop 分支把当前帧 push 进 lost_frames。
- B25 — src/splitter.rs:285-289, 333-338, 812-813, 967：心跳清扫（孤儿/空闲/FIN）只删 conns 不写 TIME_WAIT 墓碑；handle_tcp_client 拆除用无条件 conns.remove，UDP 拆除（:967）同样不写 TIME_WAIT。若新连接恰好在窗口期随机分配到同一 conn_id，旧 handler 会误删新连接的 map 条目并向其对端发旧 seq 的 FIN。概率极低（随机 u32），但修复廉价：sweep 路径先 time_wait.insert，拆除路径改用 remove_if(&cid, |_, v| Arc::ptr_eq(v, &vconn))。
- B26 — src/splitter.rs:781-808：close grace 循环只轮询 fin_received/last_active，不检查 closed/RST。远端 RST 在等待期间到达时，handler（及客户端 socket）仍会挂最多 CLOSE_QUIET_TIMEOUT=60s。建议循环内加 if vconn.closed.load(Acquire) { break; }。
- B27 — src/reassembler.rs:999-1011 vs 778-823：RST 恰在 SYN handler 的 remove_pending 之后、handshaking.remove 之前到达时，RST 分支会插入一个全新的 cancelled 幽灵条目；SYN handler 继续把 egress 建完 → 该 RST 语义丢失，egress 空转到 300s idle sweep，幽灵条目再占 30s。建议 RST 分支先检查 ctx.conns.contains_key，SYN handler 在 spawn I/O 前复核 ctx.closed。
- B28 — src/reassembler.rs:150-218 + src/frame.rs UDP_CONN_ID：conn 0 遗留单客户端 UDP 中继（全局 UdpPair + 常驻响应读取任务）在 v1.10.5 引入每关联 conn_id 后已无任何 splitter 会使用，纯死代码；任何误入 conn-0 的 DATA 还会让 splitter 回 RST(0)。建议删除或加 config 开关。
- B29 — 统计缺口：splitter UDP 中继的 bytes_sent/frames_sent 从不递增（TCP 路径才有）；handle_inbound_frame 的 _tunnel_idx 未使用；reassembler 的 handshaking 映射无 TTL 清扫（仅当 UDP SYN 的 bind_udp_pair 失败使 ? 传播、读循环死亡时残留，属边缘路径）。
- B30 — src/frame.rs:78-86 SynTarget::encode 地址长度 as u16 静默截断（当前 SOCKS5 侧受限 ≤255 字节、不可达，属埋雷）；src/reassembler.rs:592 SYN proto 非 TCP/UDP 时按 TCP 处理，建议未知 proto 回 RST。
- B31 — src/socks5.rs:51 nmethods>16 拒绝（RFC 1928 上限是 255，个别客户端会多报方法）；src/splitter.rs:911-917 UDP 关联的 keepalive TCP 收到任意 1 字节就结束关联（RFC 语义是随 TCP 连接 EOF 结束）。
- B32 — src/splitter.rs:229-234 shutdown 时重连循环最长还要睡满 24s 退避才退出；:368-371 连接上限忙轮询（100ms sleep）而非 Notify 唤醒；src/reassembler.rs:1030-1033 pending 字节预算 check-then-add 非原子，并发到达可软超预算（预算本身是软上限，影响小）。

---

## 已确认良好 / 不再复现的旧问题

- B1/B2/B3/B4/B5/B6/B7/B8/B10（v1.10.5 修复）与 D1/D3（v1.10.6 修复）在当前代码中均已正确落地，未发现回归。
- 帧解码器缓冲上限（frame.rs MAX_DECODER_BUF）、半关闭三态状态机、pending 字节预算、UDP 旁路重排等实现经复核无误。

## 本轮验证

- cargo build --all-targets：通过
- cargo clippy --all-targets：0 警告
- cargo test：20/20 单元 + 4/4 e2e 通过
- 测试覆盖缺口：心跳清扫判定（B21）、隧道死亡后的 drain 任务退出（B22）、pending 丢弃后的重置行为（B23）、stop 竞速帧上报（B24）均无测试覆盖。