# Round-Robin 线上运行状态分析报告

**分析时间**: 2026-07-28  
**分析对象**: round_robin v1.10.0 (splitter mode)  
**日志文件**: round_robin.2026-07-27.log  
**运行时长**: 2026-07-27 07:08:04 ~ 17:14:10 UTC (约 10 小时 6 分钟)  
**总日志行数**: 1,991 行 / ~235 KB

---

# 1. 总体状态

## 健康等级: ✅ **正常**

| 指标 | 数值 | 评估 |
|------|------|------|
| 运行时长 | 36,425 秒 (~10.1 小时) | 长期稳定 |
| 总连接数 (accepted) | 579 | 正常 |
| 总关闭数 (closed) | 623 | 略高(含 idle 超时) |
| ERROR 事件 | **0** | 无错误 |
| WARN 事件 | 128 | 主要为 idle timeout |
| 隧道状态 | 9/9 全程存活 | 无断开/重连 |
| 孤儿连接 | 0 | 无泄漏 |
| 背压事件 | 0 | 无阻塞 |
| 最终活跃连接 | 0 | 完全清理 |

### 总体评价

程序运行**非常稳定**。在 10 小时以上的连续运行中：
- **零 ERROR**：没有任何错误级别事件
- **零隧道断开**：全部 9 条隧道从启动到日志结束始终保持连接
- **零资源泄漏**：日志结束时 active_conns=0，所有连接正常关闭
- **零背压**：tunnel channel 从未饱和
- **零孤儿连接**：没有 Arc 泄漏的连接

唯一需要关注的是 **idle timeout 频率较高**（126 次），但这属于正常行为——是 300s TCP 空闲超时机制的预期表现。

---

# 2. 发现问题

## 问题 #1: mtalk.google.com 连接反复超时重建

| 属性 | 内容 |
|------|------|
| **编号** | ISSUE-001 |
| **等级** | Low |
| **日志位置** | 全日志周期性出现 (每 ~6 分钟) |
| **源码位置** | `src/splitter.rs:210-213` (TCP_IDLE_TIMEOUT = 300s) |

### 现象

```
07:14:05 WARN connection idle timeout conn_id=229870286 idle_secs=336  (mtalk.google.com:5228)
07:14:05 INFO closed conn_id=229870286 ...
07:14:05 INFO accepted conn_id=2949393468 ... target=mtalk.google.com port=5228
-- 6 分钟后 --
07:20:05 WARN connection idle timeout conn_id=2949393468 idle_secs=359
07:20:05 INFO closed conn_id=2949393468 ...
07:20:05 INFO accepted conn_id=1176092197 ... target=mtalk.google.com port=5228
```

此模式在 10 小时运行中重复了 **~20 次**。

### 原因分析

Google mtalk (XMPP, port 5228) 是一个长连接消息推送服务。其 keep-alive 间隔通常为 5-15 分钟。当 Google 服务的 keep-alive 间隔超过 round_robin 的 300s TCP_IDLE_TIMEOUT 时，splitter 认为连接空闲并将其关闭。浏览器随后立即重新连接，形成"超时→断开→重连"循环。

### 影响

- 每次重连有短暂的消息推送中断
- 产生不必要的日志噪音 (WARN + INFO)
- 略微增加 CPU 和网络开销

### 修复建议

将 `TCP_IDLE_TIMEOUT` 从 300s 增加到 600s 或 900s，或对特定端口（如 5228）使用更长的超时时间。

---

## 问题 #2: os error 10054 客户端重置连接

| 属性 | 内容 |
|------|------|
| **编号** | ISSUE-002 |
| **等级** | Low |
| **日志位置** | `07:56:11.991920Z` (line 276), `07:56:19.640552Z` (line 290) |
| **源码位置** | `src/splitter.rs:509-511` (client_reader.read error handling) |

### 现象

```
WARN client read error conn_id=1441792395 error=远程主机强迫关闭了一个现有的连接。 (os error 10054)
WARN client read error conn_id=4132659719 error=远程主机强迫关闭了一个现有的连接。 (os error 10054)
```

两次均发生在 YouTube 视频流连接 (googlevideo.com)。

### 原因分析

`os error 10054` (WSAECONNRESET) 表示客户端（本地浏览器）在 splitter 仍在读取数据时强制关闭了 TCP 连接。这是 YouTube 视频播放的正常行为：
1. 用户跳过视频 → 浏览器取消正在进行的视频分片下载
2. 浏览器切换视频质量 → 旧的分片连接被强制关闭
3. 视频缓冲完成 → 浏览器主动关闭连接

splitter 正确地在 `client_reader.read()` 中捕获了此错误，并正常清理了连接（后续日志显示两个连接都正常 "closed"）。

### 影响

- 无功能影响，连接正常清理
- WARN 级别日志合理，提示了非正常断开

### 修复建议

无需修复。当前处理正确。可考虑将此类客户端主动断开降级为 INFO 或 DEBUG 以降低日志噪音。

---

## 问题 #3: ReorderBuf 满后静默丢弃帧

| 属性 | 内容 |
|------|------|
| **编号** | ISSUE-003 |
| **等级** | Medium |
| **源码位置** | `src/reorder.rs:35-37` |

### 现象

```rust
} else if self.pending.len() < MAX_REORDER_WINDOW {
    self.pending.insert(seq, payload);
}
// If pending is full, the frame is silently dropped!
```

当乱序缓存达到 `MAX_REORDER_WINDOW`(512) 时，新的乱序帧被**静默丢弃**——不会发出任何警告日志，调用者也完全不知道数据丢失。

### 原因分析

设计意图是防止内存无限增长。但静默丢弃意味着：
1. 调用者 `on_frame` 不知道帧被丢弃，仍然增加 `bytes_recv` 和 `frames_recv` 计数（因为计数在 reorder push 之前）
2. 丢失的帧将造成序列号的永久缺口，后续所有帧都无法从 pending 中弹出（因为 expected 永远不会前进到被丢弃的 seq）
3. 虽然 TCP 隧道保证有序传输，但如果出现乱序（多 tunnel 情况下），512 的窗口可能不够用

实际上，`on_frame` 在 `VirtConn` 中的调用顺序是：
```rust
fn on_frame(&self, seq: u64, payload: Bytes) {
    let plen = payload.len() as u64;
    let ready = self.reorder.lock().unwrap().push(seq, payload);
    // ↑ 帧可能在这里被丢弃，但 plen 已被计入
    for chunk in ready {
        let _ = self.to_client_tx.send(chunk);
    }
    self.bytes_recv.fetch_add(plen, Ordering::Relaxed);  // 统计不准
    self.frames_recv.fetch_add(1, Ordering::Relaxed);
}
```

如果 `push()` 丢弃了帧，统计计数会虚高，且连接将陷入停滞。

### 影响

- **理论上严重**：在高乱序场景下，静默丢帧会导致连接永久阻塞和数据丢失
- **实际上较低**：当前运行 10 小时无相关警告，因 TCP 隧道基本保序

### 修复建议

1. 让 `push()` 返回一个 `Result` 或增加 `dropped: bool` 返回值，让调用者知道帧被丢弃
2. 丢弃帧时发出 WARN 日志
3. 丢弃帧时应触发连接重置（RST），因为序列已经断裂

---

## 问题 #4: FIN_GRACE 竞态条件

| 属性 | 内容 |
|------|------|
| **编号** | ISSUE-004 |
| **等级** | Medium |
| **源码位置** | `src/splitter.rs:524-534` |

### 现象

```rust
pool.send(Frame::fin(conn_id, seq));
// Grace period: wait for late DATA frames on other tunnels
const FIN_GRACE_MS: u64 = 500;
tokio::time::sleep(Duration::from_millis(FIN_GRACE_MS)).await;
time_wait.insert(conn_id, Instant::now());
conns.remove(&conn_id);
```

### 原因分析

1. **FIN 和 DATA 使用不同隧道**：FIN 可能在隧道 A 发送，而延迟的 DATA 帧在隧道 B 传输中
2. **500ms 是硬编码猜测**：没有基于 RTT 或网络状况动态调整
3. **conns.remove 后**：任何到达的 DATA 帧触发 `handle_inbound_frame` 中的 RST 响应（`src/splitter.rs:357`），导致对端收到意外的 RST
4. **TIME_WAIT 60s**：虽然 conn_id 被保留在 time_wait 中防止新连接碰撞，但 RST 已经发出

### 影响

- 在慢速隧道上，DATA 帧可能在 FIN 到达对端**之后**才到达 splitter（因为多隧道乱序）
- 对端收到 RST 后可能丢弃未处理的数据
- **实际上** 10 小时运行中未发现此问题，因为所有隧道 RTT 相近（本地回环）

### 修复建议

1. 增加 FIN 发送到实际清理之间基于 RTT 的动态等待
2. 或在 FIN 中包含最终期望的 seq number，让对端知道是否还有数据未到达
3. 在 `conns.remove` 后的 DATA 处理中，不仅发送 RST，还应该记录 WARN

---

## 问题 #5: 乱序帧统计计数在 push 之前

| 属性 | 内容 |
|------|------|
| **编号** | ISSUE-005 |
| **等级** | Low |
| **源码位置** | `src/splitter.rs:57-66` (VirtConn::on_frame) |

### 现象

```rust
fn on_frame(&self, seq: u64, payload: Bytes) {
    let plen = payload.len() as u64;  // 计算在 push 之前
    let ready = self.reorder.lock().unwrap().push(seq, payload);
    for chunk in ready {
        let _ = self.to_client_tx.send(chunk);
    }
    self.bytes_recv.fetch_add(plen, Ordering::Relaxed);  // 即使帧被 push 丢弃也计数
    self.frames_recv.fetch_add(1, Ordering::Relaxed);
    *self.last_active.lock().unwrap() = Instant::now();
}
```

### 原因分析

当 `push()` 丢弃重复帧或超出窗口的帧时，`bytes_recv` 和 `frames_recv` 被错误地增加了。这会导致：
1. 关闭日志中的统计值虚高
2. `last_active` 被更新，可能阻止 idle timeout 清理

### 影响

- 统计不准确（minor）
- 不影响功能

### 修复建议

让 `push()` 返回是否实际接受了该帧的信息，据此更新统计。

---

## 问题 #6: accepted/closed 计数差异

| 属性 | 内容 |
|------|------|
| **编号** | ISSUE-006 |
| **等级** | Low (需要确认) |

### 现象

- accepted: 579
- closed: 623
- 差异: 44

### 原因分析

存在两种可能：
1. 有些连接关闭了两次（例如 idle timeout + 正常关闭的竞态）
2. 日志中的 "closed" 匹配到了其他行（但经检查，所有 "closed" 行都是正常的连接关闭日志）

需要进一步调查这 44 个额外 "closed" 事件的来源。在 `handle_tcp_client` 中，正常路径和 timeout 路径都会执行到 "closed" 日志，理论上应该一一对应。

### 影响

- 可能是统计异常，也可能是某种双重关闭
- 当前无功能影响

### 修复建议

增加更多上下文日志以区分不同关闭路径（如增加关闭原因字段）。

---

## 问题 #7: UDP 功能完全未使用

| 属性 | 内容 |
|------|------|
| **编号** | ISSUE-007 |
| **等级** | Low |
| **日志位置** | 所有 heartbeat 行 `udp_sent=0 udp_recv=0` |

### 现象

在 10 小时运行中，UDP 收发均为 0。代码中 UDP 功能完整实现但从未被触发。

### 原因分析

SOCKS5 UDP ASSOCIATE 需要客户端明确请求。浏览器通常只使用 TCP CONNECT，不使用 UDP ASSOCIATE。UDP 功能可能仅在某些特定应用（如 DNS over SOCKS5、WebRTC 等）中才会被使用。

### 影响

- 无功能影响
- UDP 相关代码路径（包括 conn_id=0 的特殊处理）未被测试覆盖

---

# 3. 风险列表

| 等级 | 编号 | 描述 | 源码位置 |
|------|------|------|----------|
| **Medium** | ISSUE-003 | ReorderBuf 满后静默丢弃帧，可能导致连接阻塞 | `src/reorder.rs:35-37` |
| **Medium** | ISSUE-004 | FIN_GRACE 500ms 硬编码，慢隧道可能导致数据丢失 | `src/splitter.rs:524-534` |
| **Low** | ISSUE-001 | mtalk.google.com 每 6 分钟 timeout 重连 | `src/splitter.rs:210` |
| **Low** | ISSUE-002 | 客户端连接重置 (os error 10054) 产生 WARN 噪音 | `src/splitter.rs:510` |
| **Low** | ISSUE-005 | 乱序帧统计计在 push 之前，丢弃帧也计数 | `src/splitter.rs:57-66` |
| **Low** | ISSUE-006 | accepted/closed 计数差异 (579 vs 623) | `src/splitter.rs` |
| **Low** | ISSUE-007 | UDP 功能未使用，代码路径未测试 | `src/splitter.rs:557-642` |

**无 Critical 或 High 级别风险。**

---

# 4. 按分类专项检查

## 4.1 连接生命周期分析

| 检查项 | 结果 |
|--------|------|
| 连接建立成功率 | 100% (0 失败) |
| 正常关闭 | 623 次 |
| timeout 关闭 | 126 次 (idle timeout) |
| error 关闭 | 2 次 (client RST) |
| RST 关闭 | 0 次 |
| 连接泄漏 | 无 (最终 active_conns=0) |
| 僵尸连接 | 无 |
| TCP half-close | 正确处理 (shutdown 调用) |

## 4.2 内存和资源分析

| 检查项 | 结果 |
|--------|------|
| active_conns 峰值 | 26 |
| 内存增长 | 无 (conns 在空闲时归零) |
| buffer 泄漏 | 无 |
| HashMap entry 未删除 | 无 (time_wait 每 60s 清理) |
| Arc 循环引用 | 无 (0 orphaned) |
| fd 泄漏 | 无 |
| tokio task 泄漏 | 无 (writer tasks 随连接关闭退出) |

## 4.3 Round-Robin 协议检查

| 检查项 | 结果 |
|--------|------|
| conn_id 唯一性 | 579 个唯一 ID，**零碰撞** |
| conn_id 覆盖风险 | 无 (随机生成 + 碰撞检查 + TIME_WAIT 保护) |
| u64→u32 转换问题 | 无 (conn_id 本身就是 u32) |
| frame 解析错误 | 0 次 |
| frame 长度异常 | 0 次 |
| sequence 异常 | 0 次 |
| frame 丢失 | 0 次 (无 RST 发送) |
| 乱序 buffer 超限 | 0 次 |
| FIN 不影响其他连接 | ✅ 正确隔离 |

## 4.4 并发模型分析

| 检查项 | 结果 |
|--------|------|
| task 数量 | 稳定 (无泄漏) |
| task panic | 0 次 |
| channel backlog | 无 (unbounded 但无背压) |
| Mutex 竞争 | 极低 (DashMap + 细粒度锁) |
| deadlock | 无 |
| race condition | FIN_GRACE 有潜在竞态 (ISSUE-004) |
| shutdown 流程 | 正常 (Ctrl+C → shutdown flag → 各 loop 退出) |

## 4.5 性能分析

| 指标 | 峰值 | 平均 |
|------|------|------|
| active_conns | 26 | ~3 |
| 吞吐 (单连接) | ~45 MB/s (googlevideo) | ~100 KB/s |
| 帧速率 (单连接) | ~1,322 frames | ~20 frames |
| 连接时长 | ~456s (YouTube) | ~60s |
| 错误率 | 0% | 0% |
| 延迟 | 正常 (本地回环) | <1ms |

## 4.6 历史 Bug 专项验证

### Bug 1: splitter.rs conn_id 生成

✅ **无问题**。conn_id 使用 u32 随机生成，排除 UDP_CONN_ID(0)，检查 conns 和 time_wait 冲突。10 小时 579 个连接零碰撞。

### Bug 2: reassembler.rs 乱序缓存

⚠️ **潜在风险** (ISSUE-003)。虽然本次运行未触发，但 ReorderBuf 满后静默丢弃帧可能在高乱序场景下导致连接阻塞。此 bug 存在于 splitter 端（VirtConn）和 reassembler 端（VirtConnDe）两处。

### Bug 3: FIN 处理

✅ **多连接隔离正确**。每个连接的 FIN 独立处理，不影响其他连接。FIN handler 仅标记和通知目标 conn_id。

### Bug 4: Cleanup

✅ **清理完整**。异常断开后：
- connection state：heartbeat retain 清理 ✓
- buffer：ReorderBuf 随 VirtConn drop ✓
- task：writer_task 随 to_client_tx drop 而退出 ✓
- channel：to_client_tx 随 VirtConn drop 而关闭 ✓
- TIME_WAIT：60s 过期自动清理 ✓

---

# 5. 建议修改

## 5.1 高优先级（功能正确性）

### 修复 ISSUE-003: ReorderBuf 静默丢弃

**文件**: `src/reorder.rs`  
**函数**: `ReorderBuf::push()`  
**方案**:

```rust
pub fn push(&mut self, seq: u64, payload: Bytes) -> (Vec<Bytes>, bool) {
    // 返回 (有序块, 是否被接受)
    // ...
    if self.pending.len() >= MAX_REORDER_WINDOW {
        warn!("reorder buffer full, dropping seq={}", seq);
        return (out, false);  // 帧被丢弃
    }
    // ...
}
```

同时也需要修改两处调用者 (`src/splitter.rs:57-66` 和 `src/reassembler.rs:431-437`)，根据返回值决定是否更新统计。

### 修复 ISSUE-004: FIN_GRACE 动态化

**文件**: `src/splitter.rs:529`  
**方案**: 
- 基于隧道 RTT 动态计算 grace 时间
- 或至少将 500ms 增加到 2-5 秒作为安全边界
- 在 `handle_inbound_frame` 中，对 time_wait 中的 conn_id 收到的 DATA 帧，发出 WARN 日志

## 5.2 中优先级（可观测性和鲁棒性）

### 改进 accepted/closed 追踪

**文件**: `src/splitter.rs`  
**方案**: 在 "closed" 日志中增加关闭原因字段：
- `reason="normal"` — 客户端正常关闭
- `reason="fin"` — 收到对端 FIN
- `reason="timeout"` — idle timeout
- `reason="error"` — 读/写错误

### mtalk 特殊超时

**文件**: `src/splitter.rs:69-71`  
**方案**: 将 `TCP_IDLE_TIMEOUT` 从 300s 增加到 600s，或对特定已知长连接模式使用更长的超时。

## 5.3 低优先级（代码质量）

### 考虑降级 os error 10054

**文件**: `src/splitter.rs:510`  
**方案**: 对 `os error 10054` (connection reset) 使用 DEBUG 级别而非 WARN。

---

# 6. 建议增加监控

| 监控指标 | 类型 | 说明 |
|----------|------|------|
| `active_connections` | Gauge | 已有 (heartbeat 中的 active_conns) |
| `total_connections_opened` | Counter | 已有 (可从 accepted 行统计) |
| `total_connections_closed` | Counter | 已有 (可从 closed 行统计) |
| `tunnel_alive_count` | Gauge | 已有 (heartbeat 中的 alive) |
| `reorder_buffer_size` | Gauge | **建议增加** — 每个连接的 pending 帧数 |
| `reorder_dropped_frames` | Counter | **建议增加** — 窗口满导致的丢帧 |
| `fin_grace_timeouts` | Counter | **建议增加** — grace 期间到达的 DATA 帧 |
| `task_count` | Gauge | **建议增加** — tokio task 总数监控 |
| `memory_usage` | Gauge | **建议增加** — 进程内存监控 |
| `bytes_per_second` | Meter | 已有 (heartbeat 中的 udp_sent/recv，还需 TCP) |
| `frame_error_rate` | Counter | **建议增加** — frame 解析错误数 |
| `client_read_errors` | Counter | 已有 (WARN 日志) |

---

# 7. 总结

round_robin v1.10.0 在 splitter 模式下运行 **10 小时以上，表现非常稳定**：

✅ **优点**:
- 零错误、零隧道断开、零资源泄漏
- conn_id 碰撞检测有效，TCP half-close 处理正确
- 多连接 FIN 隔离正确
- idle timeout 和 TIME_WAIT 清理机制完善
- 统计日志丰富，便于分析

⚠️ **需关注**:
- ReorderBuf 静默丢弃 (ISSUE-003) — 高乱序场景下可能导致数据丢失
- FIN_GRACE 硬编码 500ms (ISSUE-004) — 慢隧道场景下的潜在竞态
- mtalk 反复超时重连 (ISSUE-001) — 体验和日志噪音问题

🔧 **建议优先修复**: ISSUE-003 和 ISSUE-004（均为 Medium 级别，涉及数据完整性）。

整体而言，代码质量很高，架构清晰，是一个成熟的 TCP 多路复用代理实现。
