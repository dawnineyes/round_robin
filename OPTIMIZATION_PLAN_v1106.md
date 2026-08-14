# OPTIMIZATION_PLAN_v1106.md — round_robin v1.10.6 修复与优化方案（第二轮）

> 配套文档：BUG_REVIEW_v1106.md（B21–B32 详单）。本方案给出分优先级、可执行的路线，标注每项对应编号与预期收益。
> 基线：cargo build 通过、clippy 0 警告、20 单元 + 4 e2e 测试通过（2026-08-14）。

> ## 执行状态（2026-08-14 更新）
>
> **P0 与 P1 已全部实施**（v1.10.7，CHANGELOG Phase 12）：
> - P0 ✅：F1（B21 心跳清扫判定 + 心跳周期可配置）、F2（B22 stop 通知 + 单测）、F3（B23 失败快 + 单测）、F4（回归测试 7 个）
> - P1 ✅：F5–F13（B24–B32 全部修复，含 handshaking TTL、proto 校验、nmethods、可中断退避、Notify 限流、预算 CAS）
> - P2 仍开放：超时可配置化（O5）、ConnStats 抽取（E3）、常量集中、Prometheus 指标、BufWriter 试验。

## 优先级总览

| 阶段 | 内容 | 目标 |
|------|------|------|
| P0（1-2 天） | 修复 B21、B22、B23（D3 回归 / 资源泄漏 / 静默截断）+ 回归测试 | 正确性 |
| P1（1 周） | 修复 B24–B32；清理 conn 0 死代码；补齐防御性校验与统计 | 健壮性 + 防回归 |
| P2（1-2 周，可选） | 可配置化、工程收敛、可观测性增强 | 运维与长期演进 |

---

## P0 — 正确性修复

### F1. splitter：FIN 清扫判定改为“完整 + 静默期”（B21）

1. heartbeat retain 的 FIN 分支（splitter.rs:309）改为：
   if (complete && fin_idle > 30) || fin_idle > limit
   即：响应流完整后仍需 30s 无活动才回收（快速回收空闲客户端），活跃上传的连接绝不误杀；
2. 把判定逻辑抽成纯函数 should_sweep_fin(vc, now) -> bool，加单元测试覆盖 4 种组合（完整+活跃 / 完整+静默 / 不完整+静默超限 / grace_waiting）；
3. 心跳周期（60s）提为常量/配置项，e2e 中可缩短，补一个“FIN 后持续发送 > 心跳周期”的 D3 长窗回归测试（现有测试只跑 1s，正是它掩盖了 B21）。

收益：D3 半关闭语义在任何时长的上传下成立；消除 v1.10.6 头号功能回归。

### F2. reassembler：隧道读循环结束补 stop 通知（B22）

reader 任务（reassembler.rs:477-490）在 link.alive.store(false) 后补 link.stop.notify_one()，与 splitter 侧（splitter.rs:186-189）对齐。
配套回归：tunnel.rs 加单元测试——构造 TunnelLink + drain_frames，收到 stop 后断言任务在限定时间内退出且队列帧全部进入 lost_frames。

收益：消除每条死隧道的任务 + fd 永久泄漏，长稳运行不再被隧道重建拖垮。

### F3. reassembler：pending 丢弃 DATA/FIN 即失败快（B23）

handle_frame 的 pending 三条丢弃路径（:912-914 / :920-927 / :942-944，以及 FIN 分支的对应丢弃点）统一改为：
1. 标记/插入 cancelled 条目并 notify 其 cancel；
2. ctx.closed.insert(cid) 墓碑；
3. pool.send(Frame::rst(cid))。

收益：seq 空洞不再造成数分钟停摆与目标端静默截断；客户端快速失败并可重试。

### F4. 回归测试补强（覆盖 B21/B22/B23/B24）

1. drain_frames stop 竞速单元测试：stop 触发时断言在途帧被记入 lost_frames（B24 修复项）；
2. pending 丢弃 → RST 的 e2e/单测：伪造 pending 溢出，断言 splitter 侧收到 RST 且连接快速拆除；
3. D3 长窗 e2e：心跳缩短为 1-2s，FIN 后客户端持续写 > 心跳周期，断言目标端完整收到尾部数据（B21）；
4. reassembler 隧道死亡泄漏回归：读侧 EOF 后断言 drain 任务退出（B22）。

---

## P1 — 健壮性修复（B24–B32）

| 项 | 对应 | 做法 | 收益 |
|----|------|------|------|
| F5 | B24 | tunnel.rs drain_frames 内层 select 的 stop 分支把在途帧 push 进 lost_frames | D1 快恢复不再漏帧 |
| F6 | B25 | 心跳清扫路径先 time_wait.insert 再删除；handle_tcp_client/UDP 拆除统一 remove_if + Arc::ptr_eq | 封死 conn_id 复用竞态窗口 |
| F7 | B26 | splitter close grace 循环加 closed 检查（closed 即 break） | RST 后立即拆除，不再空等 60s |
| F8 | B27 | reassembler RST 分支先查 conns；SYN handler spawn I/O 前复核 closed 墓碑 | 消除幽灵 pending 条目 |
| F9 | B28 | 删除 conn 0 遗留 UDP 中继（全局 UdpPair + 常驻读取任务 + UDP_CONN_ID 分支） | 简化代码、少 1 对 socket + 1 常驻任务 |
| F10 | B29 | UDP 中继补 bytes/frames 计数；handshaking 加 TTL 清扫（复用 pending 模式）；删除 _tunnel_idx | 统计完整、防边缘泄漏 |
| F11 | B30 | SynTarget::encode 地址长度 > u16::MAX 时 bail；SYN proto 校验（仅 TCP/UDP，否则 RST） | 协议防线 |
| F12 | B31 | nmethods 上限放宽到 255；UDP keepalive 仅在 EOF 结束关联 | RFC 1928 兼容性 |
| F13 | B32 | 重连退避 sleep 拆成可中断片段（每 1s 检查 shutdown）；连接上限用 Notify 唤醒；pending 预算用 fetch_update 近似原子化 | 关闭延迟/CPU/预算精度 |

---

## P2 — 性能与工程（可选）

### 可配置化（承接原 O5）

把分散的 60s/30s/15s/10s/60s 超时、心跳周期、通道容量、重排窗口字节预算收敛进 config.toml（提供当前值作为默认），运维可按隧道质量调参。

### 工程收敛

- E3（原计划遗留）：splitter/reassembler 两个 VirtConn* 的统计字段组（bytes/frames/created_at/last_active）抽为共享 ConnStats 结构体，消除两处定义漂移；
- 常量集中：TUNNEL_CHANNEL_CAP、各类 timeout 移入 src/constants.rs；
- config.toml（52310-52314）与 config.example.toml（52030-52039）端口族仍不一致（原 E5 只改了 README），统一并注明来源；
- 启动自检（原 E4）：tunnel 目标可达性预检、日志目录可写性检查给出清晰错误。

### 可观测性（承接原 P3）

- 心跳日志增加：每连接 reorder pending 字节数（backlog）、reset 原因计数（overflow / egress_full / client_full / timeout / tunnel_loss），把 B23/B24 类事件从 warn 升级为可计数指标；
- 每隧道重连次数、队列深度已具备（queue_depth），补 lost_frames 计数；
- 可选：Prometheus textformat 端点（复用现有 DashMap/Atomic 统计，无新依赖）。

### 性能微优化（评估后取舍）

- 隧道写路径：小 chunk_size（512B）时每帧一次 write 系统调用占主导，可试验 BufWriter 批量写（需评估延迟影响与超时交互）；
- send_async 选择器：在持锁阶段跳过容量为 0 的链路（现选择 best_cap 可能为 0 后仍需 await，属正确但可更优）；
- UDP DNS 解析缓存：UdpPair::send_to 对域名目标每次 lookup，可加短 TTL 缓存（目标通常是 IP 字面量，收益有限）。

## 执行顺序建议

1. F2 → F1 → F3（三个 P0 修复互相独立，F2/F1 各约 10 行）；
2. F4 回归测试随修复同步落地；
3. P1 的 F5-F13 按表顺序一次性提交，cargo fmt + clippy -D warnings + test 全绿为准（延续 CHANGELOG 单阶段原则）。