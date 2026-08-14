# OPTIMIZATION_PLAN_v11010.md — round_robin v1.10.11 优化实施记录

> 配套文档：`BUG_REVIEW_v11010.md`（B47–B49 详单）。本记录列出 v1.10.10 → v1.10.11（Phase 16）实施的优化项，标注对应待办来源与收益。

## 实施项

### O5 — 超时可配置化（原 OPTIMIZATION_PLAN 待办，两轮记录未实施）

`DATA_SEND_TIMEOUT` 等常量下沉到 `config.toml`：

- `splitter.data_send_timeout_secs`（默认 30）：客户端读循环 DATA/FIN 发送超时——无活隧道在该窗口内收下帧 → 连接重置（B45 等待语义不变，只是窗口可调）。
- `splitter.heartbeat_secs`（默认 60）：心跳/连接清扫间隔（`heartbeat_interval` 原本就存在于 `SplitterConfig`，本轮补上 TOML 入口，与测试注入路径统一）。
- `reassembler.data_send_timeout_secs`（默认 30）：egress 响应 DATA/FIN 发送超时（B42 已把超时经 `EgressReaderCtx` 注入以便测试，本轮从 config 一路传到 `ReadLoopCtx`/`ListenerCtx`）。
- `reassembler.heartbeat_secs`（默认 60）：reassembler 心跳此前硬编码 60s，与 splitter 对齐为可配。

**收益**：运维可调（弱网场景放宽 30s、快失败场景收紧）；reassembler 测试注入不再需要改代码。改动面：config.rs（字段 + 默认值 + 解析测试）、main.rs（传递）、splitter.rs / reassembler.rs（ctx 传递，删除两处同名常量）、e2e 构造点、示例配置与 README。

### O6 — `queue_depth` 只计活链路（v1.10.9 审查记录的待办 #3）

死链的 drain 任务已退出、通道关闭，`tx.capacity() == 0` 使每条死链被按满深（`TUNNEL_CHANNEL_CAP`=128）计入积压指标，直到下一次 60s compact——心跳日志中的 `queue_depth` 在隧道抖动期系统性虚高。修复：过滤 `alive`。单测 `queue_depth_ignores_dead_links` 钉住语义。

**收益**：观测准确（backlog 指标不再被死链污染）。

### O7 — UDP vconn 死通道清理（v1.10.9 审查记录的待办 #1）

`VirtConnDe.egress` 由 `EgressConn` 改为 `Option<EgressConn>`：UDP 关联的数据报完全旁路 egress 通道，旧代码却为每个 UDP 关联分配一个 cap=1 的 mpsc 通道且接收端立即 drop（纯死分配）。TCP 路径 `Some(EgressConn)`，调用点经 `VirtConnDe::egress()` 访问（UDP 路径按构造不可达）。单测 `udp_syn_creates_no_egress_channel` 断言 UDP conn 不携带通道。

**收益**：每 UDP 关联省一次通道分配；类型层面表达"UDP 无 egress 通道"的不变式。

### B48 修复即优化 — 拆除路径即时停写

`write_to_egress` 增加 cancel 监听（biased 优先 + 与写操作竞争）：RST/心跳清扫后立即停止向 egress 写，陈旧数据（最多 512 块 ≈ 32MB）不再送达已放弃的目标，任务与 socket 即刻回收。详见 BUG_REVIEW_v11010.md B48。

## 未实施（记录待排期）

1. **UDP DATA 转发 spawn 化**（B37 备注 4）：域名目标的 `send_to` 已由 B49 的 5s 超时封顶；spawn 化可进一步消除头部阻塞但引入数据报重排（UDP 语义可容忍），收益有限，暂不实施。
2. **其余常量可配置化**：`HANDSHAKE_TIMEOUT`（15s）、`CLOSE_GRACE_MAX`（15s）、`CLOSE_QUIET_TIMEOUT`（60s）、`EGRESS_WRITE_TIMEOUT`（60s）等——O5 模式已铺好，逐项下沉属顺手之事，按需再做。
3. **O3 send_async 选择器再优化**：当前加权实现正确、收益有限，不动。
4. **E2/E3 常量集中/统计字段抽取**：纯重构，风险收益比不划算，不动。

## 验证基线

- `cargo build --all-targets`：通过
- `cargo clippy --all-targets -- -D warnings`：0 警告
- `cargo fmt`：通过
- `cargo test`：48/48 单元测试 + 4/4 e2e 集成测试通过
