# BUG_REVIEW.md — round_robin v1.10.4 Bug 审查报告

> 审查日期：2026-02（当前工作副本 commit `06ae415`）
> 审查方式：逐文件静态审查（config / frame / reorder / socks5 / tunnel / splitter / reassembler / main / logging）+ `cargo build` / `cargo clippy --all-targets` / `cargo test` 验证。
> 验证结果：**编译通过、clippy 0 警告、15 个单元测试全部通过**。以下问题均为静态审查发现、无法被现有测试覆盖的逻辑/并发/协议缺陷。
>
> ## 修复状态
>
> B1–B20 已在 v1.10.5 全部修复，详见 `CHANGELOG.md` Phase 10；行号引用以 v1.10.4 为准。
> - B19 的修复方案为每关联独立 conn_id（SYN proto=0x11）+ 每连接独立 UDP socket 对（v4/v6），彻底替代 conn_id 0 单客户端路径（旧路径保留兼容）。
> - **D1 与 D3 已于 v1.10.6 修复**（见 CHANGELOG Phase 11）：隧道死亡时队列帧上报 → 受影响连接立即重置、控制帧重发；远端 FIN 改为半关闭语义，splitter 继续转发客户端数据，reassembler 保留 egress 至双方关闭完成。D4（UDP ICMP 透传）仍开放。

## 结论摘要

历史上 BUG-1~12（CHANGELOG Phase 9）已修复，本轮审查发现 **20 个新问题**（2 高 / 8 中 / 10 低）与 4 项已接受的设计限制：

| 编号 | 严重度 | 位置 | 一句话 |
|------|--------|------|--------|
| B1 | **高** | reassembler.rs:585-590 | FIN 在 SYN 握手期间被静默丢弃 → egress 永不半关闭 → 连接挂起最长 300s |
| B2 | **高** | splitter.rs:412-423,596-609 | FIN 携带的 next_seq 被忽略，固定 3s grace → 慢隧道在途 DATA 到达即丢（数据截断） |
| B3 | 中 | reassembler.rs:147-151 | UDP 响应丢弃时 seq 已递增 → 永久空洞 → 512 个报文后整个 UDP 中继被重置 |
| B4 | 中 | reassembler.rs:593-601 | SYN 握手期间的 RST 被忽略 → 为已死连接白白建立 egress |
| B5 | 中 | reassembler.rs:646-663 | half_closed 非原子 swap/store 竞态 → 10s 强制兜底可能丢失 → 写半部悬挂 |
| B6 | 中 | splitter.rs:279-292,450; socks5.rs:43-115 | SOCKS5 握手无超时且不计入连接上限 → 慢握手绕过 4096 限制（资源 DoS） |
| B7 | 中 | reassembler.rs:558-578; frame.rs:28-29 | pending 缓冲只限条数不限字节 → 理论峰值 ~4.3GB 内存 |
| B8 | 中 | reorder.rs:61; frame.rs:27 | 重排窗口按帧数（512×64KB=32MB/连接）计 → 高并发内存峰值 + 溢出重置更易触发 |
| B10 | 中 | socks5.rs:72-84; splitter.rs:520-523 | SOCKS 成功应答先于隧道 SYN → 无隧道时客户端得到 success 后立刻 EOF |
| B9 | 低 | splitter.rs:184-186 | 等待首个隧道的循环无 shutdown 检查 → 配置错误时 Ctrl+C 无法退出 |
| B11 | 低 | splitter.rs:598-599 | FIN 发送失败不重试 → reassembler 侧 egress 泄漏最长 300s |
| B12 | 低 | frame.rs:82-92 | encode 仅 debug_assert，release 下超长 payload 静默截断毁流 |
| B13 | 低 | main.rs:1,110-111,238; install.sh:53-67 | windows_subsystem 无控制台 + 只处理 Ctrl+C → 优雅关闭在 Windows/systemd SIGTERM 下失效 |
| B14 | 低 | config.rs:111-127; reassembler.rs:256 | 端口列表允许重复 → 第二个 listener 绑定失败后该端口永久静默失效 |
| B15 | 低 | splitter.rs:173-177 | 重连退避实际为 3,3,6,12,24s，与注释 3,6,12,24 不符 |
| B16 | 低 | logging.rs:63-75 | Write 契约允许部分写，tracing 不处理 → 日志行罕见交错 |
| B17 | 低 | reassembler.rs:277 | 隧道链路上限把未 compact 的死链计入 → 重连高峰可能误拒新链 |
| B18 | 低 | reassembler.rs:112 | UDP 只绑 0.0.0.0:0 → IPv6 目标数据报必然 send_to 失败 |
| B19 | 低 | splitter.rs:648-651 | 仅支持单客户端 UDP ASSOCIATE（第二个被拒）— 设计限制 |
| B20 | 低 | main.rs:50 | 启动横幅吞掉 parse_ports 错误，配置错误延迟到后面才暴露 |

---

## 高严重度

### B1 — FIN 在 SYN 握手期间被静默丢弃（egress 永不半关闭）

- **位置**：`src/reassembler.rs:585-590`（FIN 分支），对比 `:504-580`（DATA 分支）与 `:489-496`（drain 逻辑期望 pending 中有 FIN）
- **触发条件**：splitter 的 SYN 走 `pool.send()`（轮询 try_send，reassembler 侧 :370-501），FIN 走 `pool.send_async()`（least-loaded，splitter.rs:599）。二者可能落在不同隧道；只要 SYN 所在隧道 RTT 更高（多隧道异构延迟是常态），FIN 就先于 SYN 到达。此时 `ctx.conns.get(&cid)` 为 `None`，FIN 分支什么都不做直接返回——**FIN 没有像 DATA 那样排队进 `pending`**。
- **影响**：
  1. egress 写半部永远不会 `shutdown()`，目标服务器（依赖 EOF 判定请求结束的协议：HTTP 无 Content-Length、SMTP、FTP、大量 RPC 协议）永远等不到请求结束 → 连接挂起，直到 300s idle sweep 才清理；
  2. 该连接期间 splitter 已发完 FIN 并关闭，用户侧表现为"请求挂死/超时"；
  3. egress 连接 + 两个任务泄漏最长 300s。
- **修复建议**：FIN/RST 分支与 DATA 分支一致——conn 不存在且不在 `closed` 时，把控制帧也排队进 `pending`（drain 逻辑已经能处理 FIN，见 :489-496）；同时 SYN 建立完成后立即对 queued FIN 调用 `start_half_close`。RST 则应在 pending 中标记"建连即取消"，避免为已死连接建 egress（见 B4）。

### B2 — splitter 忽略 FIN 的 next_seq，固定 3s grace 导致数据截断

- **位置**：`src/splitter.rs:412-423`（FIN 处理丢弃 `frame.seq`）、`:596-609`（固定 `FIN_GRACE_MS = 3000` 后移除连接）、`:221-236`（heartbeat 的 10s FIN 清理同样忽略 seq）
- **触发条件**：目标发完响应后关闭 → reassembler 把响应 DATA 帧分散到多条隧道并最后发 FIN(seq=next_seq)。FIN 所在隧道更快时，FIN 先到 → splitter 立即通知客户端循环退出 → 3 秒后 `conns.remove` → 更慢隧道上的尾部 DATA 到达时命中 TIME_WAIT，仅打 warn 后**丢弃**（`:396-403`）。隧道延迟差 > 3s 即触发（本工具恰恰服务慢/抖动隧道）。
- **影响**：响应数据静默截断，日志只留一条 "possible data loss" 警告。heartbeat 的 10s 路径（fin_idle>10 移除且不写 TIME_WAIT）同样存在，且晚到帧会回 RST 使对端误判。
- **修复建议**：镜像 reassembler 已有的 `start_half_close` 逻辑——FIN 到达时记录 `fin_seq`，等待 `ReorderBuf::is_complete_through(fin_seq)` 再关闭（每次 DATA 送达后复查），配 10-15s 强制兜底超时；`VirtConn` 侧复用现成的 `reorder` 字段即可，改动约 30 行。

---

## 中严重度

### B3 — UDP 响应被丢弃时 seq 已消耗，产生永久空洞

- **位置**：`src/reassembler.rs:147-151`
- **触发条件**：`udp_seq = udp_seq.wrapping_add(1)` 在 `pool.send()` 之前执行；`send` 返回 false（无活隧道/队列全满）时该序号被吞掉但帧未送达。splitter 侧 `ReorderBuf`（splitter.rs:72-88）对 UDP_CONN_ID 同样要求严格有序 → expected 永远停在空洞处，后续所有响应进入 pending。
- **影响**：累计 512 个响应后 `overflow=true` → splitter 重置并移除 UDP 中继（splitter.rs:379-394），客户端 DNS 等 UDP 流量整体瘫痪，直到客户端重新 ASSOCIATE；而 reassembler 对 RST(cid=0) 一律忽略（:365），两端状态永久不一致。
- **修复建议**：① 最小修复——`send` 成功后再递增 seq；② 更优修复——UDP 帧完全旁路 `ReorderBuf`（数据报无顺序语义，乱序直接交付），一并消除 B8 对 UDP 的影响与窗口溢出重置。

### B4 — SYN 握手期间的 RST 被忽略，为已死连接建 egress

- **位置**：`src/reassembler.rs:593-601`
- **触发条件**：splitter 在 SYN 尚未完成（egress connect 最长 10s）时发出 RST（如客户端中断、溢出重置）。RST 分支 `conns.remove` 为 None 时什么都不做：不写 `closed` 墓碑、不标记 pending、不取消在建 egress。
- **影响**：egress 连接照常建立然后空转，直至 300s idle sweep；RST 语义丢失。
- **修复建议**：在 pending/handshaking 条目上记录"RST 已到达"，SYN 完成后立即拆除；或让 egress connect 与 cancel Notify 进入 `tokio::select!` 竞争，RST 到达即中止连接。

### B5 — `close_write_half` 的 half_closed 非原子竞态

- **位置**：`src/reassembler.rs:646-663`（`swap(true)` → 检查 → `store(false)`），兜底定时器在 `:637-640` 10s 后 force
- **触发条件**：DATA 送达线程执行 `swap(true)` 后发现 gap 未填满、尚未 `store(false)` 的瞬间，10s 强制兜底定时器恰好触发：其 `swap(true)` 读到 true → 直接 return（误认为已处理）；随后 DATA 线程 `store(false)`。若 gap 永久存在（隧道死亡），再无任何路径重试 close → 写半部悬挂至 300s idle sweep。
- **影响**：低概率（微秒级窗口），命中后连接挂起 300s。
- **修复建议**：用 `Mutex<bool>` 或 `AtomicU8` 状态机（0=open, 1=closing, 2=closed）原子推进，force 路径在状态为 closing 时也要推进到 closed。

### B6 — SOCKS5 握手无超时 + 连接上限被慢握手绕过

- **位置**：`src/splitter.rs:279-292`（上限只统计 `conns.len()`，条目在握手完成后才插入）、`:450-451`（`socks5_server_accept` 无超时包装）、`src/socks5.rs:43-115`（所有 read 无 deadline）
- **触发条件**：客户端 connect 后只发 1 字节然后挂起；任务、socket、缓冲永久占用，且 `conns` 不含该连接 → `MAX_CONCURRENT_CONNS = 4096` 形同虚设，可无限累积。
- **影响**：本地/局域网恶意或异常客户端即可造成任务数与内存无界增长（Windows 侧 listen 在 127.0.0.1，实际威胁面低但并非零——用户可能改绑 0.0.0.0）。
- **修复建议**：给整个 `socks5_server_accept` 套 `tokio::time::timeout`（如 15s），并在 accept 循环用独立原子计数计入半开连接；超时连接关闭。

### B7 — pending 帧缓冲只限条数、不限字节

- **位置**：`src/reassembler.rs:558-578`、`src/frame.rs:28-29`
- **触发条件**：`MAX_PENDING_CIDS=256` × `MAX_PENDING_FRAMES_PER_CID=256` × 64KB ≈ **4.3GB** 理论峰值。DATA 先于 SYN 到达是正常路径（快隧道），SYN 若因隧道故障丢失，pending 会以 64KB 块迅速堆积，30s 后才被 sweep。
- **影响**：内存峰值失控风险（OOM）。
- **修复建议**：增加全局 pending 字节预算（如 64MB），超预算丢弃最旧条目并回 RST；或将每 CID 帧数上限按 chunk_size 折算为字节。

### B8 — 重排窗口按帧数计，内存与重置阈值与块大小脱钩

- **位置**：`src/reorder.rs:61`、`src/frame.rs:27`
- **触发条件**：`MAX_REORDER_WINDOW=512` 帧 × 默认 64KB = **32MB/连接**乱序缓冲；4096 连接极限下理论 128GB。同时"窗口满即整连接重置"的语义在 64KB 块下意味着 ~32MB 的缺口判定阈值，隧道故障后连接存活时间被拉长（大量数据先堆进窗口才重置）。
- **修复建议**：窗口改为字节预算（如 8MB/连接，独立于帧数），既限内存又让故障恢复更快；`PushResult.overflow` 语义不变。

### B10 — SOCKS 成功应答早于隧道 SYN，失败时客户端已收到 success

- **位置**：`src/socks5.rs:72-84`（REPL_SUCCESS 先行）、`src/splitter.rs:520-523`（随后 SYN 可能失败 → bail）
- **触发条件**：所有隧道同时断连时，客户端收到 SOCKS5 成功应答后立刻 EOF；客户端无法区分"目标拒绝"与"隧道故障"。
- **影响**：应用层（浏览器/curl）报错信息误导，重试逻辑退化。
- **修复建议**：SYN 发送失败时改走 RFC 1928 失败应答（REP_GENERAL_FAILURE=0x01）再关闭，或把成功应答推迟到 SYN 发出之后。

---

## 低严重度

- **B9** `splitter.rs:184-186`：`while pool.link_count() == 0` 无 shutdown 检查——配置错误（proxy 不可达）时 Ctrl+C 无法退出，且 listener 未绑定、SOCKS 客户端收到 connection refused 而非可诊断错误。建议：循环内检查 shutdown；或先绑定 listener 再等隧道，并在日志中周期性输出"waiting for first tunnel"。
- **B11** `splitter.rs:598-599`：FIN 发送失败（超时/无隧道）不重试 → 对端 egress 泄漏最长 300s。建议：close 时若 FIN 失败，改发 RST（尽力）并容忍丢失。
- **B12** `frame.rs:82-92`：`encode()` 长度校验只有 `debug_assert`。release 下 payload>65535 时 `as u16` 静默截断，尾部字节被解析为下一帧头 → 整条隧道解码器 bails（tunnel 重连）。当前所有调用点恰好受控，但属于埋雷。建议：`encode` 返回 `Result<Bytes>` 或内部 clamp 前 assert。
- **B13** `main.rs:1` `#![windows_subsystem = "windows"]` + `:110-111/:238` 只等 `ctrl_c`：Windows 无控制台时 Ctrl+C 事件不投递；Linux systemd 停止发 SIGTERM（install.sh:53-67 未设 KillSignal）也不会进入优雅关闭。建议：`#[cfg(unix)]` 增加 `signal(SignalKind::terminate())`，Windows 侧接受无优雅关闭（进程退出即释放）。
- **B14** `config.rs:111-127` 允许重复端口：第二个 `TcpListener::bind` 失败（reassembler.rs:256），该端口永久静默失效（仅一条 error 日志）。建议：启动时去重并 fail-fast。
- **B15** `splitter.rs:173-177`：实际退避序列 3,3,6,12,24s（首两次均为 3s），与注释"3→6→12→24"不符。建议改 `3 << retry_count.min(3)` 或修正注释。
- **B16** `logging.rs:63-75`：`DailyWriter::write` 直接返回 `state.file.write(buf)`，tracing 的 fmt 层不处理部分写 → 高并发下罕见日志行交错/截断。建议：循环写直至写完或出错。
- **B17** `reassembler.rs:277`：`link_count()` 含未 compact 的死链，重连高峰（每 3s 重连、60s 才 compact）可能把新链误拒。建议：上限检查只数 `alive`。
- **B18** `reassembler.rs:112`：`UdpSocket::bind("0.0.0.0:0")` 仅 IPv4，IPv6 目标的数据报 `send_to` 必失败。建议：绑定 `[::]:0`（双栈）或按目标族分别建 socket。
- **B19** `splitter.rs:648-651`：仅支持单个 UDP ASSOCIATE 客户端（第二个被拒）——已文档化的设计限制，多客户端场景需按客户端地址分 conn_id。
- **B20** `main.rs:50`：启动横幅中 `parse_ports` 错误被 `unwrap_or(0)` 吞掉，真正的错误要等到 run_reassembler 前才报。建议：横幅解析失败直接 warn。

---

## 已接受的设计限制（非 bug，供优化方案参考）

| 编号 | 说明 |
|------|------|
| D1 | 隧道死亡 → 其队列中未写出的帧全部丢失（splitter.rs:149-150 abort wr_task；tunnel.rs:147 break），无应用层重传；依赖重排窗口溢出（512 帧后）触发整连接重置恢复。故障期间连接静默停滞，恢复慢且代价大。 |
| D2 | 无帧级校验/ACK：TCP（TUIC）保证字节级可靠，应用层信任之（合理）。 |
| D3 | splitter 收到远端 FIN 即视为全关闭（splitter.rs:419-421），不支持 TCP 半关闭——远端 FIN 后客户端继续写的数据被丢弃。对"客户端先写完再等响应"的常见模式无影响，但严格半关闭语义不完整。 |
| D4 | UDP 中继无重传、无 ICMP 透传（RFC 1928 之外的行为），对端不可达时客户端收不到端口不可达。 |

## 已验证项

- `cargo build`（dev）：通过
- `cargo clippy --all-targets`：0 警告
- `cargo test`：15/15 通过（frame 5、reorder 1、config 4、socks5 2、tunnel 3）
- 现有测试覆盖缺口：无 splitter/reassembler 端到端测试、无 FIN 竞态/乱序注入测试、无 UDP 中继测试（TEST_REPORT.md 已自行指出），B1/B2/B3 均属此缺口。
