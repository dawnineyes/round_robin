# OPTIMIZATION_PLAN_v11012.md — round_robin v1.10.12 优化实施记录

> 配套文档：`BUG_REVIEW_v11012.md`（B50–B56 详单）。本记录列出 v1.10.11 → v1.10.12（Phase 17）实施的优化项，标注对应待办来源与收益。

## 实施项

### O8 — 拆除路径统一 `signal_teardown`（随 B50 修复）

7 处 egress 拆除点（心跳 idle 清扫、RST 处理、reorder 溢出、egress 写失败、B42 发送失败、两处 pending-cancelled drain）原先各自 `cancel.notify_one()`。B50 拆分 `cancel`/`cancel_writer` 双 Notify 后，"每个任务必须都被唤醒"成为拆除路径的不变式——收敛为单一 `signal_teardown(vc)` helper（对两个 Notify 各 `notify_one`），由 helper 而非调用点记忆保证不变式。

**收益**：消除"未来新增拆除点只唤醒其一"的复发面（B50 正是 7 处复制粘贴式拆除点 + 1 个共享 Notify 的组合产物）；拆除语义单点审计。

### O9 — 配置校验集中（E4 部分落地，随 B55 修复）

`main.rs` 中两处内联 chunk_size 校验与零值超时校验收敛为 `SplitterConfig::validate()` / `ReassemblerConfig::validate()`（config.rs）：chunk_size 界（512..65535，线格式 u16 长度 + 解码器缓冲上界）与 `data_send_timeout_secs`/`heartbeat_secs` ≥ 1s（0 值分别导致心跳忙等烧核 / 发送全断连接雪崩）。main.rs 由内联检查改为调用，校验逻辑随 config 单测覆盖（`validate_rejects_zero_timeouts`）。

**收益**：启动期即时报错替代运行期神秘故障；校验与 serde 结构同文件，新增配置项时校验落点明确；可单测（原内联在二进制中不可测）。

### B50/B51/B52 修复即优化 — 拆除路径全臂即时停

- B50：拆除信号不再被单一等待者独占——读/写任务同时回收（此前每次拆除有一半概率遗留一个任务在对端 EOF 上或排空 32MB 陈旧数据）。
- B51：half_close 排空臂与 cancel 竞争——排空窗口内的拆除即时生效。
- B52：发送等待与 cancel 竞争——拆除后不再坐满最长 30s 的发送超时。
- B56：`writer_died` 把读循环存活上界从 TCP RTO（分钟级、OS 参数决定）收敛到写侧 60s 超时——静默分区后隧道槽位恢复时间确定化。

详见 BUG_REVIEW_v11012.md 对应条目。

## 未实施（记录待排期）

1. **E4 剩余——端口 bind 失败可见性**：本轮评估后**有意保持现状**。当前行为（bind 失败仅 error 日志、其余端口继续服务）比 fail-fast 更抗运维事故：端口被旧进程占用时，fail-fast 会在 systemd 下形成 crash-loop，而部分降级至少保住其余隧道。若需强化，方向是心跳日志增加 `bind_failures` 计数而非启动失败。
2. **UDP 域名解析缓存**：`UdpPair::send_to` 对域名目标逐数据报解析（B49 已加 5s 封顶）。域名目标在 UDP 中继里罕见（几乎全是 IP 字面量），加缓存属收益甚微的复杂度，暂不实施。
3. **其余常量可配置化**：`HANDSHAKE_TIMEOUT`（15s）、`CLOSE_GRACE_MAX`（15s）、`CLOSE_QUIET_TIMEOUT`（60s）、`EGRESS_WRITE_TIMEOUT`（60s）等——O5 模式已铺好，按需再做。
4. **O3 send_async 选择器再优化**：当前加权实现正确、收益有限，不动。
5. **E2/E3 常量集中/统计字段抽取**：纯重构，风险收益比不划算，不动。

## 验证基线

- `cargo build --all-targets`：通过
- `cargo clippy --all-targets -- -D warnings`：0 警告
- `cargo fmt`：通过
- `cargo test`：55/55 单元测试 + 4/4 e2e 集成测试通过
