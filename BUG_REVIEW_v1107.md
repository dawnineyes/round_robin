# BUG_REVIEW_v1107.md — round_robin v1.10.7 Bug 审查报告（第三轮）

> 审查日期：2026-08-14
> 审查对象：当前工作副本 v1.10.7（Cargo.toml version = 1.10.7，CHANGELOG Phase 12 之后）
> 审查方式：逐文件静态审查（config / frame / reorder / socks5 / tunnel / splitter / reassembler / main / logging / lib）+ 全量构建验证。
> 验证基线：cargo build --all-targets 通过、cargo clippy --all-targets 0 警告、cargo test 27/27 单元 + 4/4 e2e 通过。

> ## 修复状态（2026-08-14 更新）
>
> **B33–B40 已全部修复。** 验证：cargo build --all-targets 通过、clippy -D warnings 0 警告、30/30 单元测试 + 4/4 e2e 集成测试通过（新增 3 个回归测试：pending 逐出 RST、pending 清扫 RST、未知 cid RST 墓碑）。
>
> 修复要点：
> - **B33**：`try_reserve_pending` 逐出最旧条目时对被逐出 cid 执行 fail-fast（取消在途握手 + closed 墓碑 + RST）。
> - **B34**：pending 清扫提取为 `sweep_stale_pending()`，清扫时对每个超龄 cid 取消在途握手 + 墓碑 + RST（心跳调用）。
> - **B35**：`socks5_server_accept` 不再发送成功应答，改为返回应答字节；splitter 在 SYN 入队成功后写成功应答，失败时写 `REPLY_GENERAL_FAILURE`（新增带 5s 超时的 `send_socks5_reply`）。TCP 与 UDP ASSOCIATE 路径均覆盖。
> - **B36**：RST 分支对完全未知的 cid 也写 closed 墓碑；SYN 建连后补 closed 复核（顺带封堵 B33/B34 逐出/清扫导致的幽灵 egress 窗口）。
> - **B37**：UDP DATA 分支先克隆 vconn 并释放 DashMap shard 引用，再 await `forward_udp_datagram`。
> - **B38**：`bind_udp_pair` 失败只重置该 cid（清理 pending/handshaking + 墓碑 + RST），不再经 `?` 杀死整条隧道读循环。
> - **B39**：UDP 中继记录首个客户端地址，只接受该来源的数据报，其余丢弃并告警。
> - **B40**：conn_id 分配移到 SOCKS5 握手完成后（`handle_client` 内），消除"握手期间 id 无人占用可被重复分配"的窗口。

## 与上一轮的关系

上一轮 BUG_REVIEW_v1106.md（v1.10.6）的 B21–B32 已在 v1.10.7 全部修复并配回归测试。本报告针对 v1.10.7 当前代码做第三轮审查，共发现 **8 个新问题（2 中 / 6 低）**，编号从 B33 继续。B21–B32 修复经复核无回归；B1–B20、D1/D3 状态不变。

## 结论摘要

| 编号 | 严重度 | 位置 | 一句话 |
|------|--------|------|--------|
| B33 | 中 | reassembler.rs:1052-1092 | pending 字节预算逐出最旧条目时不发 RST/不写墓碑 → 被逐出连接的请求被静默截断（B23 只修了"当前 cid 丢弃"，逐出路径遗漏） |
| B34 | 中 | reassembler.rs:237-245 | pending TTL 清扫（30s）不发 RST → SYN 丢失且重发失败的连接陷入"缓冲→清扫→再缓冲"循环，splitter 侧连接存活至客户端 EOF+60s，数据全部静默丢弃 |
| B35 | 低 | splitter.rs:737-748 | BUG-10 修复不完整：失败应答在 REP_SUCCESS 之后发送，客户端已进入数据模式 → 10 字节垃圾进入响应流 |
| B36 | 低 | reassembler.rs:995-1019 vs 478-510 | RST 在 SYN handler 的 conns 检查之后、handshaking 注册之前到达时被静默丢弃（B4/B27 修复未覆盖的窗口）→ 幽灵 egress 空转至 300s |
| B37 | 低 | reassembler.rs:806-816 | DATA 分支在持有 DashMap shard 锁的情况下 await forward_udp_datagram，域名目标还会触发同步 DNS → 同 shard 其他连接被头部阻塞 |
| B38 | 低 | reassembler.rs:551 | UDP SYN 的 bind_udp_pair() 失败经 `?` 传播杀死整个隧道读循环（重连 3s+），handshaking 条目残留 120s |
| B39 | 低 | splitter.rs:1004-1013 | UDP 中继不校验数据报来源：任何能到达 relay 端口的主机可注入转发，响应发给第一个发送者 |
| B40 | 低 | splitter.rs:435 | conn_id 在握手前分配但握手期间（≤15s）不占位 → 理论可重复分配导致 conns.insert 互相覆盖（概率 2⁻³²） |

---

## 中严重度

### B33 — pending 字节预算逐出最旧条目：被逐出连接静默数据截断

- 位置：src/reassembler.rs:1052-1092（try_reserve_pending 的逐出分支），特别是 :1080-1091。
- 触发条件：`try_reserve_pending` 超预算时逐出最旧的其他 pending 条目（`ctx.pending.remove(&key)` + 退还字节），但**不写 closed 墓碑、不发 RST、不标记 cancelled**。被逐出 cid 的已排队 DATA 帧随条目一起消失。随后该 cid 的 SYN（可能在慢隧道/慢握手上）到达 → 建连 → 逐出的帧（seq 1..N-1）永远缺失 → 重放的剩余帧（seq N+）全部卡在 reorder 缓冲区等待永不到来的空洞（TCP 隧道不重传）→ **目标端收到的请求是空/残缺的，且没有任何错误信号**。客户端侧一切正常（FIN 后靠 10s 强制兜底关写半部，目标端看到的是截断请求 + EOF）。
- 触发可达性：预算 64MB，每 cid 满缓冲可达 16MB（256 帧 × 64KB）→ 仅 4~5 个 cid 的大数据 pre-SYN 缓冲即可触发逐出。快隧道先于 SYN 到达是正常路径（B7 已确认），SYN 卡在慢隧道时即为常态。
- 影响：与 B23 同一缺陷类别（丢弃即毁 seq 流），但 B23 修的是"当前 cid 丢弃 → fail fast"，逐出路径静默丢弃了他人的帧：请求静默截断，TLS 类协议靠对端报错才暴露，明文上传则静默损坏。
- 修复建议：逐出时对被逐出 cid 执行 fail_pending_conn（cancelled + closed 墓碑 + `pool.send(Frame::rst(cid))`），与 B23 对齐。注意逐出发生在 CAS 循环内，需先取出 (key, bytes) 再发 RST，且 exclude 自身的 cid 不受影响。

### B34 — pending TTL 清扫不发 RST：SYN 丢失的连接无限"缓冲→清扫"循环

- 位置：src/reassembler.rs:237-245（心跳 pending 清扫）。
- 触发条件：SYN 随隧道死亡丢失，splitter 重连循环的 lost-frame 重发（splitter.rs:223-224）恰好也失败（`pool.send` 在无活隧道/队列全满时返回 false，且**重发只尝试一次**）→ SYN 永久丢失。此后该 cid 的 DATA 每批都被 pending 缓冲 30s 后静默清扫（只退还字节），下一批 DATA 又新建 pending 条目 → 无限循环，**全程无 RST**。splitter 侧连接在客户端 EOF 前一直存活（重排窗口正常，无任何失败信号），目标端从未收到数据。
- 影响：连接停摆 + 静默丢数据，且无 fail-fast（与 B33 同类的观测盲区）；splitter 连接最长存活到"客户端 EOF + 60s 静默超时"。
- 修复建议：清扫超龄 pending 条目时对每个被清扫 cid 发 `Frame::rst` + 写 closed 墓碑（复用 fail_pending_conn 的语义）。RST 到达 splitter 后 reset_conn 会立即拆除连接，客户端快速失败而非无限等待。

---

## 低严重度

- **B35** — src/splitter.rs:737-748：BUG-10 的修复不完整。`socks5_server_accept` 在解析目标后、返回前**已发出 REP_SUCCESS**（socks5.rs:87）；当 `pool.send(syn_frame)` 失败时，代码又写一条 `[0x05,0x01,0x00,0x01,0,0,0,0,0,0]`（REP_GENERAL_FAILURE）作为"失败告知"。但此时客户端已处于数据模式，这 10 字节会被当作响应流数据解析（HTTP 客户端表现为解析错误而非"连接被拒绝"），目的（区分目标拒绝与隧道故障）未达成。建议：把 REP_SUCCESS 的发送推迟到 SYN 入队成功之后，失败时直接发失败应答并关闭——客户端才能得到确定性的失败语义。
- **B36** — src/reassembler.rs:995-1019 vs 478-510：B4/B27 修复了"RST 在握手期间/幽灵条目"窗口，但 RST 若恰在 SYN handler 的 `conns.contains_key` 检查（:478）之后、`handshaking.insert`（:488）之前被其他隧道读循环处理，则 RST 分支三个条件（conns/handshaking/pending）都不命中，静默丢弃；SYN 继续走完建连 → 为已重置的连接建立幽灵 egress，空转至 300s idle sweep。窗口为几指令宽（多线程任务间可交错），概率低但修复廉价：RST 分支对未知 cid 也写 closed 墓碑（60s TTL 清扫兜底，不会无界增长）。
- **B37** — src/reassembler.rs:806-816：DATA 分支对 UDP 连接持有 `conns.get` 返回的 DashMap shard 引用跨越 `await forward_udp_datagram(...)`；后者在目标为域名时还会执行 `tokio::net::lookup_host`（可能秒级阻塞）。同一 shard 上的其他连接（含 RST/拆除处理）在此期间全部阻塞。建议：UDP 分支先克隆 payload + vconn（Arc），drop 掉 shard 引用再 await；DNS 解析也可移出锁外。
- **B38** — src/reassembler.rs:551：UDP SYN 处理中 `bind_udp_pair().await?` 的失败经 `?` 一路传播到 `tunnel_read_loop`，**杀死整条隧道的读循环**（该隧道所有连接断流 3s+ 重连），且该 cid 的 handshaking 条目残留至 120s TTL 清扫。UDP socket 绑定失败概率低（fd 耗尽等），但后果与成因不成比例。建议：失败仅回 RST + 清理 pending/handshaking，不传播。
- **B39** — src/splitter.rs:1004-1013：UDP 中继 socket 未 `connect`，`recv_from` 不校验来源：任何能到达 relay 端口的主机（本地/局域网）都能注入数据报并转发到目标；响应发送给**第一个**发送者（client_addr 固定为首次来源）→ 响应窃取/注入通道。默认监听 127.0.0.1 时威胁面低，但配置为 0.0.0.0 时成立。建议：记录首个客户端地址后仅接受该地址的数据报。
- **B40** — src/splitter.rs:435：conn_id 在 SOCKS5 握手前分配，但握手期间（最长 HANDSHAKE_TIMEOUT=15s）该 id 不在 conns 也不在 time_wait → `alloc_conn_id` 可能把同一 id 分配给第二个客户端；两个 handler 随后各自 `conns.insert` 互相覆盖 → 帧错投到错误连接。概率 2⁻³²（与 B25 同类，B25 已用墓碑降低复用概率，但握手占位仍缺）。建议：握手开始时即以轻量占位（或把 conn_id 分配推迟到握手完成后）。

---

## 已确认良好 / 不再复现的旧问题

- B21–B32 全部按 v1.10.7 落地，逐项复核无回归：fin_sweep_decision 纯函数与心跳接入正确；drain 任务 stop 通知对称（splitter/reassembler 两侧一致）；pending 丢弃 fail-fast（B23 当前 cid 路径）；TIME_WAIT 墓碑覆盖 reset/sweep/拆除三路径；CAS 预算与字节退款在 remove_pending/清扫/逐出三处一致（仅逐出的 RST 缺失，见 B33/B34）。
- 半关闭三态状态机（close_write_half 的 0/1/2 AtomicU8）、egress 写半部 drain-then-shutdown、finish_if_done 三条件（fin_received ∧ egress_eof ∧ state=2）经推演无丢失路径；force 兜底与正常关闭的 CAS 竞争正确（1→0 回滚不会被 force 覆盖）。
- BUG-2 的 fin_seq 语义（FIN 携带 next_seq + is_complete_through）在 splitter grace 循环、心跳 fin 清扫、reassembler start_half_close 三处一致。
- 帧解码器缓冲上限、SynTarget 编码拒绝超长、encode_into 拒绝超限 payload 且上报 lost（B24）均正确；UDP 响应超 MAX_PAYLOAD 在 reassembler 侧有显式丢弃检查（:623-630），客户端→中继方向受 UDP 报文上限 65507 约束不会超限。

## 本轮验证

- cargo build --all-targets：通过
- cargo clippy --all-targets：0 警告
- cargo test：30/30 单元 + 4/4 e2e 通过（新增：pending_eviction_resets_evicted_cid、pending_sweep_resets_swept_cid、rst_for_unknown_cid_tombstones）
- 测试覆盖缺口：UDP 数据报来源校验（B39）、conn_id 分配时机（B40）为集成级行为，无单元测试；B35 由既有 e2e（成功路径）间接覆盖。
