# CHANGELOG.md — Round Robin Refactoring

> **重构日期**: 2026-07-27  
> **基线**: v1.9.1  
> **原则**: 每次单阶段修改，cargo fmt + check + test + clippy 全部通过

---

## Phase 22: 第十轮代码审查修复（v1.10.17）

> **基线**: v1.10.16
> **来源**: 快速代码审查（BUG_REVIEW_v11016 遗留 + 本轮新增静态走查项）

### 修改文件

| 文件 | 修改内容 | 原因 |
|------|----------|------|
| `src/splitter.rs` | DATA 发送超时改为强制 RST，不再发 FIN；heartbeat 清扫连接时通知 reassembler；EOF 且 FIN 发送失败时跳过 grace；TIME_WAIT 迟到 DATA 回 RST；修复握手成功后过早唤醒 accept 循环 | B61/B64/新增清理与等待问题 |
| `src/reassembler.rs` | SYN 排空 pending 时 egress 写失败/reorder overflow 立即 reset；heartbeat idle 清扫向 splitter 发 RST | B62/B63 |
| `src/tunnel.rs` | `send_async` 全链路满时清空 `full` 并短暂重试，避免永久阻塞在单条已满链路上 | B65 |
| `src/socks5.rs` | UDP ASSOCIATE 携带并固定客户端声明的具体地址，非零地址不再被首个发包者劫持 | 安全加固 |
| `src/config.rs` | reassembler.ports 与 splitter.tunnel 拒绝端口 0 | 配置校验 |
| `Cargo.toml` | 版本 1.10.16 → 1.10.17 | 发布 |

### 行为变化

- **B61 修复**: splitter 上行 DATA 因无隧道超时后直接 RST，不再用 FIN 造成静默截断
- **B62 修复**: reassembler 在 SYN 握手期间回放 DATA 时遇到 egress 写失败或 reorder overflow 会 reset 连接
- **B63 修复**: reassembler idle 清扫会向 splitter 发 RST，对端不再等自己的 idle 超时
- **B64 修复**: splitter 握手成功到 conn 插入之间的窗口不再提前唤醒 accept 循环，连接数上限语义恢复
- **B65 修复**: 所有隧道队列满时，任意隧道腾出空间后 `send_async` 能立即使用，不再卡在单个 `best` 链路
- **新增清理修复**: splitter heartbeat 清扫孤儿/idle/FIN 完成连接时发送 RST；EOF 且 FIN 发送失败时不再空等 60s grace
- **安全/配置**: UDP ASSOCIATE 支持固定客户端地址；端口 0 被配置校验拒绝

### 测试结果

- `cargo test`: 63 单元测试 + 5 e2e 集成测试通过
- `cargo clippy --all-targets -- -D warnings`: 0
- `cargo fmt`: 通过
- `cargo build --all-targets`: 通过

---


## Phase 21: splitter 就绪判定改用存活隧道数（v1.10.16）

> **基线**: v1.10.15
> **来源**: splitter 启动时用 `link_count()` 等待首个隧道，上一轮运行遗留的 stale/dead 链路会被计入，导致 splitter 在 reassembler 实际不可达时提前宣告 ready。

### 修改文件

| 文件 | 修改内容 | 原因 |
|------|----------|------|
| `src/splitter.rs` | 就绪等待与 ready 日志改用 `alive_count()`（活隧道数）替代 `link_count()`（含死链路） | B60 |
| `Cargo.toml` | 版本 1.10.15 → 1.10.16 | 发布 |

### 行为变化

- **B60 修复**: 只有至少一条**存活**隧道时才宣告 splitter ready；死链路不再造成提前就绪

### 测试结果

- `cargo test`: 通过
- `cargo clippy --all-targets -- -D warnings`: 0
- `cargo fmt`: 通过
- `cargo build --all-targets`: 通过

---

## Phase 20: 链路空闲清扫方向盲区修复（v1.10.15）

> **基线**: v1.10.14
> **来源**: 第八轮复查——B57 的空闲清扫只统计入站帧活动，而纯下载时 reassembler 侧隧道只出站、纯上传时 splitter 侧隧道只出站：超过 `LINK_IDLE_TIMEOUT`(600s) 的单向大传输会被误判空闲而回收，传输中断。

### 修改文件

| 文件 | 修改内容 | 原因 |
|------|----------|------|
| `src/tunnel.rs` | `TunnelLink.last_recv_ms` 改名 `last_active_ms`；`drain_frames` 每次写出成功后盖章（与读循环的入站盖章对称——双向任一活动都刷新时钟）；`sweep_idle` 注释更新为双向语义；新增 1 个单测 | B59 |
| `src/splitter.rs` / `src/reassembler.rs` | 读循环盖章注释与字段名同步 | B59 |
| `Cargo.toml` | 版本 1.10.14 → 1.10.15 | 发布 |

### 行为变化

- **B59 修复**: 空闲清扫改为"双向均无活动才回收"——静默连接占用槽位的防护（B57）保持不变，但单向长传输（>10 分钟的上传或下载）不再被误判回收

### 测试结果

- `cargo test`: 59/59 单元测试（新增 `drain_writes_stamp_link_activity`）+ 5/5 e2e 集成测试通过
- `cargo clippy --all-targets -- -D warnings`: 0
- `cargo fmt`: 通过
- `cargo build --all-targets`: 通过

---

## Phase 19: 重排窗口溢出修复（v1.10.14）

> **基线**: v1.10.13
> **来源**: 线上故障——4 条 TUIC 隧道下每个大文件下载必失败。日志定位：`reorder/channel overflow, resetting connection`（seq ~750-1250，传输 20-40MB 后）+ 数百帧 "late DATA on TIME_WAIT — possible data loss"。

### 修改文件

| 文件 | 修改内容 | 原因 |
|------|----------|------|
| `src/frame.rs` | `MAX_REORDER_BYTES` 8MB → 64MB（新默认窗口预算，注释重写为 B58 语义） | B58 |
| `src/reorder.rs` | `ReorderBuf` 增加 `max_bytes`/`max_entries` 字段与 `with_limit()`；条目上限按 `max_bytes / MAX_PAYLOAD`（下限 512）推导，字节预算不足一个 chunk 的窗口不再可能；新增 B58 回归测试（默认窗口容纳 4 隧道 32MB 全在途倾斜） | B58 |
| `src/config.rs` | splitter/reassembler 增加 `reorder_window_bytes`（默认 64MB，serde default）+ 校验（≥ chunk_size、≤ 1GiB）+ 单测 | B58 |
| `src/splitter.rs` / `src/reassembler.rs` | 配置贯通 `SplitterConfig`/`ClientCtx`/`ReassemblerConfig`/`ListenerCtx`/`ReadLoopCtx` → 两侧 VirtConn 构造用 `with_limit` | B58 |
| `src/main.rs` | 新配置传入两侧 Config | B58 |
| `tests/e2e.rs` | 全部配置构造点补新字段 | 编译 |
| `config.example.toml` / `config.reassembler.example.toml` / `README.md` | `reorder_window_bytes` 文档（含"必须 ≥ 隧道数 × 128 × chunk_size"的选型公式） | B58 |
| `Cargo.toml` | 版本 1.10.13 → 1.10.14 | 发布 |

### 行为变化

- **B58 修复**: 重排窗口默认预算 8MB → 64MB 且可配置。旧 8MB 远小于发送端在途窗口（4 隧道 × 128 帧 × 64KB = 32MB），隧道间延迟差稍大即溢出窗口并重置连接——大文件下载必断（Intel 镜像站 20-40MB 处，日志 seq 750-1250 与 8MB/64KB=128 帧倾斜完全吻合）。新默认覆盖 8 条隧道的全在途倾斜；条目上限随字节预算推导，小分片场景不失控
- **配置**: `reorder_window_bytes` 两侧可配（默认 67108864）；校验拒绝小于一个 chunk 的窗口（否则首个乱序帧即溢出重置）

### 测试结果

- `cargo test`: 58/58 单元测试（新增 `default_window_tolerates_four_tunnel_skew`、`reorder_window_defaults_and_validation`；重写 `overflow_and_completion`/`byte_budget_bounds_window` 为显式 8MB 限额）+ 5/5 e2e 集成测试通过
- `cargo clippy --all-targets -- -D warnings`: 0
- `cargo fmt`: 通过
- `cargo build --all-targets`: 通过

---

## Phase 18: 第七轮 Bug 审查修复（v1.10.13）

> **基线**: v1.10.12
> **来源**: 第七轮审查（B57，报告留本地，见 .gitignore 约定）

### 修改文件

| 文件 | 修改内容 | 原因 |
|------|----------|------|
| `src/tunnel.rs` | `TunnelLink` 增加 `last_recv_ms: AtomicU64`（读循环逐帧盖章，`now_millis()` 时钟）；`TunnelPool::sweep_idle(now_ms, idle_limit)` 回收静默超过 `LINK_IDLE_TIMEOUT`(600s) 的活链路（置 dead + 触发 `stop`，经既有链条 drain→writer_died→读循环拆除）；新增 1 个单测 | B57 |
| `src/splitter.rs` | 读循环逐帧更新 `last_recv_ms`；心跳在 compact 后调用 `sweep_idle`（回收数 warn 日志）；测试构造点补字段 | B57 |
| `src/reassembler.rs` | 同上（读循环 + 心跳清扫 + 构造点） | B57 |
| `tests/e2e.rs` | 新增 `client_disconnect_propagates_teardown`：客户端写 8KB 后直接断开（drop 不 shutdown），断言目标端在 10s 内看到 EOF（覆盖拆分拆除链 B48–B52 的端到端路径） | 测试覆盖 |
| `Cargo.toml` | 版本 1.10.12 → 1.10.13 | 发布 |

### 行为变化

- **B57 修复**: 隧道链路（监听路径上唯一没有显式上界的资源）获得空闲回收——静默 TCP 连接不再永久占用链路槽位（此前 64 条即可永久拒绝真实隧道，MAX_TUNNEL_LINKS）；对端静默卡死的隧道在 600s 无入站流量后自动回收重建。健康空闲隧道（如夜间零流量）最长每 600s 重建一次（3s 重连，无流量损失）
- **测试覆盖**: 新增 e2e 覆盖客户端中途断开 → 目标端及时 EOF 的完整拆除链（此前该路径只有单元测试）

### 测试结果

- `cargo test`: 56/56 单元测试（新增 `sweep_idle_recycles_silent_links`）+ 5/5 e2e 集成测试通过（新增 `client_disconnect_propagates_teardown`）
- `cargo clippy --all-targets -- -D warnings`: 0
- `cargo fmt`: 通过
- `cargo build --all-targets`: 通过

---

## Phase 17: 第六轮 Bug 审查修复 + 优化（v1.10.12）

> **基线**: v1.10.11
> **来源**: `BUG_REVIEW_v11012.md`（B50–B56）+ `OPTIMIZATION_PLAN_v11012.md`（O8/O9）

### 修改文件

| 文件 | 修改内容 | 原因 |
|------|----------|------|
| `src/reassembler.rs` | `VirtConnDe` 的 cancel 拆分为 `cancel`（读任务/UDP 响应读任务）与 `cancel_writer`（写任务）双 Notify；新增 `signal_teardown` helper，7 处拆除点（心跳清扫/RST/溢出/egress 写失败/B42/两处 pending-cancelled drain）统一唤醒两个任务；`write_to_egress` 的 half_close 排空臂逐块写与 cancel 竞争（biased）；`read_from_egress` 的 DATA/FIN 发送与 cancel 竞争；`tunnel_read_loop` 监听 `writer_died`；新增 5 个单测 | B50/B51/B52/B56/O8 |
| `src/splitter.rs` | `tunnel_read_loop` 监听 `link.writer_died`（写侧 60s 超时即读侧退出上界，静默对端不再把重连延迟到 TCP RTO）；新增 1 个单测 | B56 |
| `src/tunnel.rs` | `TunnelLink` 增加 `writer_died: Notify`，`drain_frames` 任一退出路径触发（写侧停滞超时=隧道死亡探针）；测试构造点同步 | B56 |
| `src/reorder.rs` | `push` 改 Entry API：窗口内重复帧按普通重复丢弃，不再覆盖旧条目泄漏 `pending_bytes` 字节预算；新增 1 个单测 | B54 |
| `src/config.rs` | `parse_ports` 的 `MAX_PORTS` 上限扩展到 `Ports::List`（与 Range 一致）；新增 `SplitterConfig::validate()`/`ReassemblerConfig::validate()`（chunk_size + 零值超时校验）；新增 2 个单测 | B53/B55/O9 |
| `src/main.rs` | 两处内联 chunk_size 校验替换为 `validate()` 调用 | B55/O9 |
| `Cargo.toml` | 版本 1.10.11 → 1.10.12 | 发布 |

### 行为变化

- **B50 修复**: egress 读/写任务不再共享一个 cancel Notify——每次拆除两个任务同时回收（此前 `notify_one` 只唤醒一个等待者，另一半概率下读任务在对端 EOF 上无限期挂起、或写任务排空最多 32MB 陈旧数据）
- **B51 修复**: half_close 排空窗口内的 RST/清扫即时停写（B48 修复的最后一个盲臂）
- **B52 修复**: 拆除后 egress 读任务不再坐满最长 30s 的发送等待，也不会在隧道恢复后发出一帧死 cid 的陈旧数据
- **B53 修复**: 超长端口列表与超长 Range 同等待遇——启动即报错而非 spawn 数千监听任务
- **B54 修复**: 重排窗口字节预算在重复帧下不再漂移（防御纵深；正常对端恰好一次投递）
- **B55 修复**: `heartbeat_secs = 0`/`data_send_timeout_secs = 0` 启动即报错（此前分别导致心跳忙等烧核 / 全部连接批量重置）
- **B56 修复**: 隧道读循环退出上界从 TCP RTO（分钟级）收敛到写侧 60s 超时——静默分区后隧道槽位恢复时间确定化、读任务不泄漏至 RTO
- **O8**: 拆除路径收敛为 `signal_teardown` 单一 helper，双任务唤醒不变式单点保证
- **O9**: 配置校验集中在 `validate()`（E4 部分落地），可单测

### 测试结果

- `cargo test`: 55/55 单元测试 + 4/4 e2e 集成测试通过（新增 7 个：`teardown_wakes_both_egress_tasks`、`write_to_egress_cancel_during_half_close_drain`、`tunnel_read_loop_exits_when_writer_dies`（splitter + reassembler 各一）、`duplicate_of_pending_frame_does_not_leak_bytes`、`parse_ports_rejects_huge_list`、`validate_rejects_zero_timeouts`）
- `cargo clippy --all-targets -- -D warnings`: 0
- `cargo fmt`: 通过
- `cargo build --all-targets`: 通过

---

## Phase 16: 第五轮 Bug 审查修复 + 优化（v1.10.11）

> **基线**: v1.10.10
> **来源**: `BUG_REVIEW_v11010.md`（B47–B49）+ `OPTIMIZATION_PLAN_v11010.md`（O5/O6/O7）

### 修改文件

| 文件 | 修改内容 | 原因 |
|------|----------|------|
| `src/tunnel.rs` | `send_async` 在每轮循环开头、pick **之前**创建 `added.notified()` future（tokio 保证创建于 `notify_waiters` 之前的 future 必被唤醒，pick 与等待注册之间的漏唤醒窗口消除）；`queue_depth` 过滤死链（死链关闭通道 capacity=0 曾按满深计入指标）；新增 2 个单测 | B47/O6 |
| `src/reassembler.rs` | `write_to_egress` 增加 `cancel: Arc<Notify>`（biased 优先 + 与写操作竞争，RST/清扫即时停写，不再排空最多 32MB 陈旧数据）；移除 `finish_if_done` 中发给 egress reader 的死信号 `cancel.notify_one()`（writer 监听 cancel 后该信号会抢在 half-close 排空前截断 egress 流）；`UdpPair::send_to` 域名解析包 5s 超时（提取 `resolve_target`，DNS 卡住不再头部阻塞隧道读循环）；`VirtConnDe.egress` 改 `Option<EgressConn>`（UDP 关联不再分配死通道）；`ReassemblerConfig` 增加 `data_send_timeout`/`heartbeat_interval` 并贯穿 `ListenerCtx`/`ReadLoopCtx`；新增 4 个单测 | B48/B49/O5/O7 |
| `src/splitter.rs` | `SplitterConfig` 增加 `data_send_timeout` 并贯穿 `ClientCtx`（客户端读循环 DATA/FIN 发送超时可配）；删除 `DATA_SEND_TIMEOUT` 常量 | O5 |
| `src/config.rs` | `splitter`/`reassembler` 增加 `data_send_timeout_secs`（默认 30）与 `heartbeat_secs`（默认 60）+ 解析单测 | O5 |
| `src/main.rs` | 把新配置项传入 `SplitterConfig`/`ReassemblerConfig` | O5 |
| `tests/e2e.rs` | 8 处 config 构造补新字段 | 编译 |
| `config.example.toml` / `config.reassembler.example.toml` / `README.md` | 新配置项文档 | O5 |
| `Cargo.toml` | 版本 1.10.10 → 1.10.11 | 发布 |

### 行为变化

- **B47 修复**: `send_async` 的等待注册窗口不再漏掉并发重连——隧道恢复后帧立即续传，不会拖满 30s 超时后重置连接（B45 注释中"无竞态窗口"的论断与 tokio 实际语义不符，本轮以源码核对修正）
- **B48 修复**: RST/心跳清扫后 egress 写任务立即停止（含停滞中的写），不再向已被放弃的目标排空最多 512 块陈旧数据，任务与 socket 即刻回收；`finish_if_done` 的死信号移除后，半关闭排空不再被 cancel 抢先截断
- **B49 修复**: UDP 域名目标的 DNS 解析 5s 封顶，解析器故障不再阻塞同隧道全部帧处理
- **O5**: `data_send_timeout_secs`（默认 30s）与 `heartbeat_secs`（默认 60s）对 splitter/reassembler 均可配
- **O6**: `queue_depth` 指标只计活链路，隧道抖动期不再虚高
- **O7**: UDP 关联不再分配死 egress 通道（类型层面表达不变式）

### 测试结果

- `cargo test`: 48/48 单元测试 + 4/4 e2e 集成测试通过（新增 7 个：`send_async_sees_link_added_during_first_poll`、`write_to_egress_aborts_on_cancel`、`write_to_egress_stops_immediately_when_pre_cancelled`、`udp_pair_send_to_ip_literal`、`queue_depth_ignores_dead_links`、`udp_syn_creates_no_egress_channel`、`timeout_fields_default_and_parse`）
- `cargo clippy --all-targets -- -D warnings`: 0
- `cargo fmt`: 通过
- `cargo build --all-targets`: 通过

---

## Phase 15: 第四轮 Bug 审查修复（v1.10.10）

> **基线**: v1.10.9
> **来源**: `BUG_REVIEW_v1109.md`（B41–B45）

### 修改文件

| 文件 | 修改内容 | 原因 |
|------|----------|------|
| `src/tunnel.rs` | `TunnelPool` 新增 `added: Notify`（`add()` 时 `notify_waiters`）；`send_async` 在"无任何活链路"时等待新链路加入而非立即返回 false（调用方 `DATA_SEND_TIMEOUT` 30s 兜底）；更新 2 个旧单测（超时包裹）+ 新增 `send_async_waits_for_new_link` 回归单测 | B45 |
| `src/reassembler.rs` | SYN 处理器的 egress 连接失败/超时两分支补 `closed` 墓碑（与其余 SYN 失败路径对齐，新增单测）；`read_from_egress` 发送失败时 fail-fast（墓碑 + 移除 conn + 尽力 RST）替代"FIN 送入永远填不上的 seq 空洞"流程，FIN 发送失败补 RST 兜底，发送超时经 `EgressReaderCtx.data_send_timeout` 注入（可测），统计改为直接用任务持有的 `vconn` Arc（省去每块一次 DashMap 查找）；新增 `dispatch_frame`：SYN 帧在带 `Semaphore` 上限（64）的并发任务中处理，egress connect（≤10s）不再头部阻塞隧道读循环（上限耗尽时降级为内联处理），`ReadLoopCtx`/`ListenerCtx` 派生 Clone 并携带 `syn_limit`；新增 3 个回归单测 | B41/B42/B46 |
| `src/splitter.rs` | `handle_client` 分配的 conn_id 直接传入 `handle_udp_client`（删除重复分配）；UDP keepalive 监听任务保存句柄，中继先结束时 `abort`（心跳清扫/RST 后不再泄漏任务与控制 TCP 连接） | B43/B44 |
| `Cargo.toml` | 版本 1.10.9 → 1.10.10 | 发布 |

### 行为变化

- **B45 修复**: 全部隧道同时短暂中断（如网络抖动导致的重连 3s 窗口）不再瞬间判死所有在途连接——`send_async` 等待链路回归（有界于调用方 30s 超时），短暂抖动不再截断所有传输；真故障仍按原 30s 语义失败
- **B42 修复**: egress 响应发送失败（30s 无活隧道）时立即拆除双方连接 + 尽力 RST，客户端快速失败而非挂起至 60s 静默超时；FIN 无法送达时补 RST 兜底，隧道恢复后客户端即时感知连接已断
- **B41 修复**: egress 连接失败/超时的 cid 写 closed 墓碑，迟到的 DATA 帧立即收到确定性 RST，不再生成僵尸 pending 条目（30s 内占用字节预算并可逐出健康条目）
- **B43 清理**: UDP 关联复用握手完成后分配的 conn_id，删除一次无用的随机分配
- **B44 修复**: UDP 中继被心跳清扫/重置后，keepalive 监听任务立即中止，控制 TCP 连接随即关闭（原实现任务与 socket 存活至客户端主动关闭）
- **B46 修复**: SYN 握手（egress connect ≤10s）不再阻塞所在隧道读循环——同隧道其他连接的帧即时处理；SYN 并发以 64 个信号量限流，SYN 洪泛时自动降级为原内联行为

### 测试结果

- `cargo test`: 41/41 单元测试 + 4/4 e2e 集成测试通过（新增 4 个：`syn_connect_failure_tombstones_cid`、`egress_send_failure_resets_conn`、`send_async_waits_for_new_link`、`syn_connect_stall_does_not_block_other_cids`）
- `cargo clippy --all-targets -- -D warnings`: 0
- `cargo fmt`: 通过
- `cargo build --all-targets`: 通过

---

## Phase 14: 加权 DATA 调度器（v1.10.9）

> **基线**: v1.10.8
> **动机**: 原 `send_async` 采用 least-loaded（选队列空位最多的隧道）。在"低延迟但被运营商限速"的隧道场景下，该策略失明：限速不表现为本地队列积压（数据先被 TCP 内核缓冲吸收），快隧道持续垄断流量，限速隧道与其他健康隧道都被喂错比例，总吞吐被拖到限速隧道水平，且被冷落的隧道拥塞窗口萎缩。

### 修改文件

| 文件 | 修改内容 | 原因 |
|------|----------|------|
| `src/tunnel.rs` | `TunnelLink` 新增 `rate_bps`（f64 bits，drain 任务唯一写者）；`drain_frames` 在每次成功写出后按写间隔维护时间衰减 EWMA（τ=2.5s，纯函数 `ewma_rate`）；`send_async` 改为加权选择：锚点网格确定性加权轮询（`weighted_pick`），权重=EWMA 速率（未测量=乐观均值，全体未知=均匀），每链路保底份额 `FLOOR_SHARE=5%`；队列满的链路本轮跳过（try_send 失败即重选），全部饱和时回退阻塞在最高权重链路；新增 7 个单测 | 调度器 |
| `src/splitter.rs` / `src/reassembler.rs` | `TunnelLink` 构造点补 `rate_bps` 字段 | 编译 |
| `Cargo.toml` | 版本 1.10.8 → 1.10.9 | 发布 |

### 行为变化

- **限速自适应**: 被限速的隧道被过量喂入时，drain 写被传输层背压卡住 → 写间隔拉长 → EWMA 速率收敛到该隧道真实容量 → 权重自动下调，流量分流到健康隧道；隧道恢复后权重自动回升（保底份额保证其始终获得探测流量）
- **无饿死**: 每链路至少 5% 权重份额；完全未测量的新隧道按均值乐观赋权，加入即被探测
- **不阻塞快路径**: 加权选中的链路队列满时立即重选（权重在未饱和链路间重新归一化），单帧不再被慢隧道拖住；只有全部链路饱和才回退阻塞（真背压语义不变）
- **分布确定**: 锚点网格游标使选路确定可复现（1024 次选择精确符合权重比例），便于测试与观测
- **保持不变的语义**: `send`（SYN/FIN/RST 控制帧）仍为轮询 try_send；调用方 `DATA_SEND_TIMEOUT` 包裹不变；`send_async` 返回值语义（是否有活链路收下）不变

### 测试结果

- `cargo test`: 37/37 单元测试 + 4/4 e2e 集成测试通过（新增 7 个：`ewma_decays_and_converges`、`weighted_pick_distributes_by_rate`、`weighted_pick_floor_guarantees_share`、`weighted_pick_optimistic_for_unmeasured`、`weighted_pick_cold_start_uniform`、`send_async_skips_full_link`、`send_async_blocks_when_all_full`）
- `cargo clippy --all-targets -- -D warnings`: 0
- `cargo fmt`: 通过
- `cargo build --release`: 见发布流程

### 已知限制

- 权重测量点是 drain 写速率（无应用层 ACK），限速检测延迟 ≈ 内核缓冲填满时间 + EWMA 时间常数（τ=2.5s），无法消除但可接受
- 未饱和链路的测量值等于其喂入速率而非容量——保底份额保证最低喂入，需求超过总容量时全部链路进入背压、测量值收敛到容量比例
- 若运营商按账号/出口聚合限速（3 条共享同一限速池），任何选路策略均无效

---

## Phase 13: 第三轮 Bug 审查修复（v1.10.8）

> **基线**: v1.10.7
> **来源**: `BUG_REVIEW_v1107.md`（B33–B40）

### 修改文件

| 文件 | 修改内容 | 原因 |
|------|----------|------|
| `src/reassembler.rs` | pending 字节预算逐出最旧条目时对被逐出 cid 失败快（取消在途握手 + closed 墓碑 + RST，含单测）；pending TTL 清扫提取为 `sweep_stale_pending()` 并在清扫时同样失败快（含单测）；RST 分支对完全未知 cid 也写 closed 墓碑（含单测）；SYN 建连后补 closed 复核，封堵逐出/清扫/早到 RST 的幽灵 egress 窗口；UDP DATA 分支先克隆 vconn、释放 DashMap shard 引用再 await（DNS/send_to 不再头部阻塞同 shard 连接）；`bind_udp_pair` 失败只重置该 cid，不再经 `?` 杀死整条隧道读循环 | B33/B34/B36/B37/B38 |
| `src/socks5.rs` | `socks5_server_accept` 不再发送成功应答，改为返回 `(Socks5Result, Vec<u8>)`；新增 `REPLY_GENERAL_FAILURE` 常量 | B35 |
| `src/splitter.rs` | 成功应答推迟到隧道 SYN 入队成功后发送（TCP 与 UDP ASSOCIATE 两路径，新增带 5s 超时的 `send_socks5_reply`），失败时发确定性失败应答；UDP 中继只接受首个客户端来源的数据报；conn_id 分配从 accept 循环移到 SOCKS5 握手完成后（消除握手期 id 可重复分配窗口）；`handle_tcp_client`/`handle_udp_client` 参数改为传 `&ClientCtx`（消除 clippy too_many_arguments） | B35/B39/B40 |
| `Cargo.toml` | 版本 1.10.7 → 1.10.8 | 发布 |

### 行为变化

- **B33 修复**: pending 预算逐出不再静默丢弃被逐出连接的请求数据——逐出即 RST，双方快速失败，目标端不再收到截断请求
- **B34 修复**: SYN 丢失且重发失败的连接不再陷入"缓冲→30s 清扫→再缓冲"死循环——清扫即 RST，splitter 立即拆除
- **B35 修复**: 无隧道时 SOCKS5 客户端收到 REP_GENERAL_FAILURE 而非"成功应答 + 垃圾字节 + EOF"，可确定性区分目标拒绝与隧道故障
- **B36 修复**: RST 在 SYN 注册前到达不再被丢弃，未知 cid 一律写墓碑（60s TTL 清扫兜底）
- **B37 修复**: UDP 域名目标的 DNS 解析不再阻塞 DashMap 同 shard 的其他连接
- **B38 修复**: UDP socket 绑定失败只影响该关联，不再中断整条隧道的所有连接
- **B39 修复**: UDP 中继仅接受关联客户端的来源地址，阻断注入与响应窃取
- **B40 修复**: conn_id 仅在握手完成后分配，消除握手期（≤15s）重复分配的窗口

### 测试结果

- `cargo test`: 30/30 单元测试 + 4/4 e2e 集成测试通过（新增 3 个回归：`pending_eviction_resets_evicted_cid`、`pending_sweep_resets_swept_cid`、`rst_for_unknown_cid_tombstones`）
- `cargo clippy --all-targets -- -D warnings`: 0
- `cargo fmt`: 通过
- `cargo build --release`: 见发布流程

---

## Phase 12: 第二轮 Bug 审查修复（v1.10.7）

> **基线**: v1.10.6
> **来源**: `BUG_REVIEW_v1106.md`（B21–B32）+ `OPTIMIZATION_PLAN_v1106.md` P0/P1

### 修改文件

| 文件 | 修改内容 | 原因 |
|------|----------|------|
| `src/splitter.rs` | 心跳 FIN 清扫判定改为"完整 + 30s 静默"（纯函数 `fin_sweep_decision` + 单测）；心跳周期可配置（`SplitterConfig.heartbeat_interval`）；清扫路径写 TIME_WAIT 墓碑；拆除路径改 `remove_if + Arc::ptr_eq`；close grace 循环感知 RST；重连退避可中断（1s 粒度检查 shutdown）；连接上限改 Notify 唤醒；UDP 中继补 bytes/frames 统计；UDP keepalive 仅 EOF 结束；移除 conn 0 分支与 `_tunnel_idx` | B21/B25/B26/B28/B29/B31/B32 |
| `src/reassembler.rs` | 隧道读循环结束补 `link.stop.notify_one()`；pending 丢弃 DATA/FIN 一律失败快（`fail_pending_conn`：取消 + 墓碑 + RST，含单测）；RST 分支不再种幽灵条目、SYN 路径复核 closed 墓碑；pending 字节预算改 CAS 原子预留；SYN proto 校验（未知 proto 回 RST）；handshaking 带时间戳 + 120s TTL 清扫；UDP 响应补统计并刷新活跃时间；删除 conn 0 遗留中继（全局 UdpPair + 常驻任务） | B22/B23/B27/B28/B29/B30/B32 |
| `src/tunnel.rs` | `drain_frames` 泛型化（可注入测试 writer）；stop 竞速与 encode 失败时在途帧记入 `lost_frames`；新增 stop 退出/丢帧上报两个单测 | B24 |
| `src/frame.rs` | 删除 `UDP_CONN_ID`；`SynTarget::encode` 返回 `Result`（地址超长报错而非 `as u16` 截断） | B28/B30 |
| `src/socks5.rs` | nmethods 上限放宽到 255（u8 天然上限） | B31 |
| `src/main.rs` | SplitterConfig 传 `heartbeat_interval = 60s` | B21 |
| `tests/e2e.rs` | 全部 SplitterConfig 显式心跳周期；D3 测试改为 2s 心跳 + FIN 后跨多个心跳持续发送 6 段数据（B21 回归） | 回归 |
| `Cargo.toml` | 版本 1.10.6 → 1.10.7 | 发布 |

### 行为变化

- **B21 修复**: 远端 FIN 且响应流完整后，连接仅在 30s 无活动时才被心跳回收——客户端在 FIN 后持续上传（D3 半关闭）不再被中途切断（旧逻辑在下一次心跳就无条件清扫）
- **B22 修复**: reassembler 隧道死亡不再永久泄漏 drain 任务 + socket（补 stop 通知，与 splitter 对齐）
- **B23 修复**: pending 缓冲丢弃 DATA/FIN 立即 RST 失败快，不再产生 seq 空洞导致数分钟停摆与目标端静默截断
- **B24 修复**: 隧道死亡时在途帧计入 `lost_frames`，D1 快恢复不再漏掉恰好 1 帧
- **B25 修复**: 心跳清扫先写 TIME_WAIT、拆除用 `remove_if + ptr_eq`，封死 conn_id 复用竞态窗口
- **B26 修复**: RST 后 close grace 循环立即退出，不再空等最多 60s
- **B27 修复**: RST 不再产生幽灵 pending 条目；SYN 路径复核 closed 墓碑
- **B28 清理**: 删除 conn 0 遗留单客户端 UDP 中继（每关联独立 conn_id 已是唯一路径）
- **B29 补齐**: UDP 双向统计、handshaking 120s TTL 清扫
- **B30 加固**: SYN 地址超长报错、未知 proto 回 RST
- **B31 兼容**: nmethods ≤255；UDP 关联随 keepalive TCP EOF 结束而非任意字节
- **B32 细节**: 可中断退避、连接上限 Notify 唤醒、pending 预算 CAS 原子预留

### 测试结果

- `cargo test`: 27/27 单元测试 + 4/4 e2e 集成测试通过
- `cargo clippy --all-targets -- -D warnings`: 0
- `cargo fmt`: 通过
- `cargo build --release`: 见发布流程

---

## Phase 11: 快恢复 + 半关闭语义 + 性能（v1.10.6）

> **基线**: v1.10.5  
> **来源**: `OPTIMIZATION_PLAN.md` P3（D1/D3）与 P2 性能/工程项（O1/O2/O4/E1/E5）

### 修改文件

| 文件 | 修改内容 | 原因 |
|------|----------|------|
| `src/tunnel.rs` | `TunnelLink` 新增 `stop` Notify 与 `lost_frames`；`drain_frames` 复用编码缓冲（O2）+ stop 竞速写 + 死亡时上报未写出帧；`TunnelPool::queue_depth()` | D1/O2/指标 |
| `src/splitter.rs` | 隧道死亡时重置丢帧连接、重发丢失的控制帧（D1）；远端 FIN 不再断开（D3 半关闭，继续转发客户端数据）；`grace_waiting` 标记保护 FIN grace；心跳增加 resets/queue_depth/half_open/time_wait 指标 | D1/D3/可观测性 |
| `src/reassembler.rs` | egress EOF 后保留连接直到 splitter FIN 完成半关闭（`egress_eof` + `finish_if_done`）；丢帧回 RST/重发控制帧；心跳 resets/queue_depth 指标；egress reader 参数收敛为 `EgressReaderCtx` | D1/D3/clippy |
| `src/frame.rs` | `try_next` 用 `read_buf` 直接读入解码缓冲（消除 8KB 栈拷贝）；`encode_into` 复用缓冲编码 | O1/O2 |
| `Cargo.toml` | 版本 1.10.5 → 1.10.6；release 增加 `codegen-units=1` + `strip=true` | O4/发布 |
| `.github/workflows/ci.yml`（新增） | push/PR 触发 build + clippy `-D warnings` + test | E1 |
| `.github/workflows/release.yml` | 发布前加 clippy 与 test 门槛 | E1 |
| `tests/e2e.rs` | 新增 D1 隧道死亡快恢复测试、D3 远端 FIN 后继续发送测试 | 回归 |
| `README.md` | 端口示例与 config.example 对齐 + 默认值说明；v1.10.6 变更日志 | E5 |

### 行为变化

- **D1 隧道故障快恢复**: 隧道死亡时，队列中未写出的 DATA 帧导致对应连接立即重置（两端 RST），不再静默停滞直到重排窗口溢出（8MB）；未写出的 SYN/FIN/RST 自动重发
- **D3 客户端侧半关闭**: 远端 FIN 只表示响应结束——splitter 继续转发客户端数据，egress 连接保留到客户端 FIN 才完整拆除；两个方向的关闭精确独立
- **修复**: 乱序交付竞态——重排缓冲的锁现在覆盖"入队 + 写出"全过程，多隧道并发送达时 ready 块不再交错（此前可能产生字节乱序）
- **修复**: splitter 先绑定 SOCKS 监听再等首隧道，客户端在隧道建连期间得到明确失败应答而非 ECONNREFUSED；e2e 测试客户端握手加重试，消除 CI 调度竞态
- **性能**: 帧解码零拷贝（read_buf 直读）；隧道写路径每帧少一次堆分配（复用编码缓冲）；release 加 codegen-units=1/strip
- **可观测性**: 心跳日志新增隧道队列深度、连接重置计数、半开握手数、TIME_WAIT 数
- **CI**: 新增 push/PR 流水线（clippy deny warnings + 全量测试），release 发布前同样跑门槛

### 测试结果

- `cargo test`: 20/20 单元测试 + 4/4 e2e 集成测试通过
- `cargo clippy --all-targets -- -D warnings`: 0
- `cargo build --release`: 成功

---

## Phase 10: Bug 审查修复（v1.10.5）

> **基线**: v1.10.4  
> **来源**: `BUG_REVIEW.md`（20 个 bug）+ `OPTIMIZATION_PLAN.md` P0/P1/P2

### 修改文件

| 文件 | 修改内容 | 原因 |
|------|----------|------|
| `src/lib.rs`（新增） | 抽取 lib target + `shutdown_signal()`（SIGINT/SIGTERM） | 支持集成测试；BUG-13 |
| `src/main.rs` | 模块迁移至 lib；横幅 parse_ports 错误 warn 不再吞 | BUG-20 |
| `src/frame.rs` | `encode() → Result<Bytes>`（拒绝超长而非截断）；`PROTO_UDP`；`MAX_REORDER_BYTES`(8MB)；`MAX_PENDING_BYTES`(64MB) | BUG-12/19/8/7 |
| `src/reorder.rs` | 窗口按字节预算（帧数上限保留）；测试更新 | BUG-8 |
| `src/config.rs` | 端口去重（保留顺序 + warn）；范围上限 256；空端口集报错 | BUG-14 |
| `src/tunnel.rs` | `alive_count()`；`drain_frames` 处理 encode 失败 | BUG-17/12 |
| `src/logging.rs` | `DailyWriter` 循环完整写（处理部分写/Interrupted） | BUG-16 |
| `src/splitter.rs` | FIN 携带 next_seq 精确半关闭（complete_through + 15s 兜底 / 60s 静默上限）；UDP 旁路重排；SOCKS5 握手 15s 超时 + 半开连接计数；SYN 失败回 REP_GENERAL_FAILURE；FIN 发送失败回 RST；退避 3→6→12→24 修正；多客户端 UDP（每关联独立 conn_id + SYN proto=0x11）；等首隧道响应 shutdown | BUG-2/3/6/9/10/11/15/19 |
| `src/reassembler.rs` | FIN/RST 在 SYN 握手期排队进 pending（FIN 半关闭、RST 取消建连 + select 竞速）；`close_write_half` 三态状态机；pending 字节预算 + 最旧驱逐；UDP 每连接独立 v4/v6 socket 对 + 独立响应读取任务（同目标多客户端不再串流）；UDP seq 仅在成功发送后递增；双栈 UDP（Windows WSAEADDRNOTAVAIL 兼容）；隧道链路上限只数活链；心跳清理 pending 字节与日志 | BUG-1/3/4/5/7/17/18/19 |
| `install.sh` | systemd 单元补 `KillSignal=SIGTERM` + `TimeoutStopSec=10` | BUG-13 |
| `tests/e2e.rs`（新增） | 端到端集成测试：3 隧道（1 慢）乱序重组 + FIN-before-SYN 竞态 + 双客户端 UDP 并发 | F9/回归 |
| `Cargo.toml` | 版本 1.10.4 → 1.10.5 | 发布 |
| `README.md` | 协议节补 UDP 每关联 conn_id 语义 | 文档 |

### 行为变化

- **修复**: FIN 先于 SYN 到达（异构隧道延迟）不再被丢弃——egress 照常半关闭，依赖 EOF 的协议不再挂死 300s（BUG-1）
- **修复**: splitter 收到 FIN 后按 next_seq 等待在途 DATA，慢隧道响应不再被 3s 固定 grace 截断（BUG-2）
- **修复**: UDP 响应丢弃不再产生永久 seq 空洞杀死整个中继；UDP 完全旁路重排缓冲（BUG-3）
- **修复**: SYN 握手期 RST 可中止在建 egress 连接（BUG-4）；half-close 状态机消除强制兜底丢失竞态（BUG-5）
- **修复**: SOCKS5 握手 15s 超时且计入 4096 连接上限；pending 帧字节预算 64MB（理论峰值 4.3GB→64MB）；重排窗口 32MB/连接→8MB/连接（BUG-6/7/8）
- **修复**: 多客户端 UDP ASSOCIATE 并发中继，同目标不再互相串流（每关联独立 conn_id 与 socket）（BUG-19）
- **修复**: IPv6 UDP 目标（双栈 socket 对）；Windows 双栈 sendto 兼容（BUG-18）
- **改进**: 无隧道时 SOCKS 客户端收到明确失败应答；FIN 发不出时回 RST 避免对端 300s 泄漏；SIGTERM 优雅关闭；端口配置去重校验（BUG-10/11/13/14/16/17/20）

### 测试结果

- `cargo test`: 20/20 单元测试 + 2/2 e2e 集成测试通过
- `cargo clippy --all-targets`: 0 warnings
- `cargo build --release`: 成功

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
