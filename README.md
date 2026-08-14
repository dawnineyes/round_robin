# round_robin

多路径 TCP 隧道聚合 — 单条流拆分到 N 条 TUIC 隧道并行传输，对端重排。

```
App → SOCKS5 → round_robin(splitter) → N×TUIC → round_robin(reassembler) → Internet
                   Windows                                 Debian
```

## 架构

Splitter 收 SOCKS5 CONNECT → 分帧 → round-robin 写 N 条 TCP 隧道 → Reassembler 收帧 → 按 seq 重排 → 写目标。

TUIC TCP 保证送达，多 tunnel 只引入乱序。ReorderBuf 等缺失帧到齐后按序交付。

## 快速开始

### Windows 端（Splitter）

`config.toml`（放 exe 同目录）：

```toml
mode = "splitter"

[splitter]
listen = "127.0.0.1:52030"

[[splitter.tunnel]]
proxy = "127.0.0.1:52031"
target = "127.0.0.1"
port = 52031
# ... 每 tunnel 一组，proxy/port 对应 sing-box SOCKS5 入站
```

### Debian 端（Reassembler）

```bash
curl -sSfL https://raw.githubusercontent.com/dawnineyes/round_robin/master/install.sh | bash
sudo systemctl restart round_robin
```

安装指定版本（默认安装最新 release，如 `v1.10.11`）：

```bash
curl -sSfL https://raw.githubusercontent.com/dawnineyes/round_robin/master/install.sh | bash -s -- v1.10.4
```

`/opt/round_robin/config.toml`：

```toml
mode = "reassembler"

[reassembler]
listen = "127.0.0.1"
ports = "52031-52039"
local_target = "127.0.0.1:52040"
```

> 示例端口与 `config.example.toml` / `config.reassembler.example.toml` 保持一致（52030-52040 段）。
> 完全省略字段时的代码默认值：splitter `listen=127.0.0.1:52310`；reassembler `ports=52311-52319`、`local_target=127.0.0.1:52310`。

## 配置参考

### Splitter

| 字段 | 类型 | 说明 |
|------|------|------|
| `listen` | SocketAddr | SOCKS5 入站地址 |
| `chunk_size` | usize | 分片大小 512-65535，默认 65535 |
| `data_send_timeout_secs` | u64 | DATA 发送超时（秒），超时即重置连接，默认 30 |
| `heartbeat_secs` | u64 | 心跳/连接清扫间隔（秒），默认 60 |
| `[[splitter.tunnel]]` | array | 隧道列表 |
| `tunnel.proxy` | SocketAddr | sing-box SOCKS5 入站地址 |
| `tunnel.target` | String | Reassembler IP |
| `tunnel.port` | u16 | Reassembler 端口 |

### Reassembler

| 字段 | 类型 | 说明 |
|------|------|------|
| `listen` | IpAddr | 隧道监听 IP |
| `ports` | range/list | 监听端口 |
| `local_target` | SocketAddr | 出站 SOCKS5 目标 |
| `chunk_size` | usize | 分片大小，默认 65535 |
| `data_send_timeout_secs` | u64 | DATA 发送超时（秒），默认 30 |
| `heartbeat_secs` | u64 | 心跳/连接清扫间隔（秒），默认 60 |

## 协议

帧格式（大端序，15 字节头）：

```
ConnID    u32   4 bytes
Sequence  u64   8 bytes
Flags     u8    1 byte    SYN=0x01 DATA=0x02 FIN=0x04 RST=0x08 ACK=0x10
Length    u16   2 bytes
Payload   [u8]  Length bytes
```

SYN payload: `Proto(u8) + AddrLen(u16) + Address(variable) + Port(u16)`

SYN+ACK 与 ACK 帧保留在协议定义中但当前版本不使用（v1.10.4 起不再发送 SYN+ACK）——TUIC TCP 保证送达，无需应用层握手确认或流量控制。FIN 帧携带 next_seq，两端均据此在所有在途 DATA 送达后精确半关闭写端（v1.10.5 起 splitter 侧同样生效）。

UDP（v1.10.5 起）：每个 UDP ASSOCIATE 分配独立 conn_id，SYN 帧 `Proto=0x11`（UDP）宣告，reassembler 为该关联创建独立 UDP 中继（无 egress TCP）；DATA 帧携带 SOCKS5 UDP 数据报，不参与重排。conn_id 0 为旧版单客户端路径保留（v1.10.4 及更早 splitter 兼容）。

## 发布

```bash
git tag v1.10.6
git push origin v1.10.6
```

GitHub Actions 自动编译 Linux x86_64 并发布 Release（发布前跑 clippy `-D warnings` 与全量测试）。

## 变更日志

### v1.10.6

- **D1 隧道故障快恢复**: 隧道死亡时队列中未写出的帧不再静默丢失——受影响连接立即重置（RST），未发出的 SYN/FIN/RST 自动重发，不再停滞到重排窗口溢出
- **D3 半关闭语义**: 远端 FIN 后客户端可继续发送数据（egress 连接保留至双方完成关闭）
- **性能**: 帧解码零拷贝（`read_buf` 直读）；隧道写路径复用编码缓冲；release 启用 `codegen-units=1` + `strip`
- **可观测性**: 心跳增加隧道队列深度、连接重置计数、半开握手数、TIME_WAIT 数
- **CI**: 新增 push/PR 流水线（build + clippy deny + 全量测试，含 4 个端到端测试）

### v1.10.5

- **修复**: FIN 先于 SYN 到达（异构隧道延迟）时不再丢弃——egress 照常半关闭，依赖 EOF 的协议不再挂死最长 300s
- **修复**: splitter 按 FIN 的 next_seq 等待在途 DATA（15s 兜底），慢隧道响应不再被固定 3s grace 截断
- **修复**: UDP 响应丢弃不再产生永久 seq 空洞；UDP 数据报完全旁路重排缓冲
- **修复**: SYN 握手期 RST 可中止在建 egress 连接；half-close 状态机消除强制兜底丢失竞态
- **修复**: SOCKS5 握手 15s 超时并计入 4096 连接上限；pending 帧字节预算 64MB；重排窗口 32MB/连接 → 8MB/连接
- **修复**: 多客户端 UDP ASSOCIATE 并发中继（每关联独立 conn_id + socket，同目标不再串流）；IPv6 UDP 目标
- **修复**: 端口配置去重与范围上限；隧道链路上限只数活链；重连退避序列修正；日志完整写
- **改进**: SOCKS 无隧道时返回明确失败应答；FIN 发送失败回 RST；SIGTERM 优雅关闭；`Frame::encode` 超长 payload 报错而非截断
- **测试**: 新增 5 个单元测试 + 2 个端到端集成测试（乱序/FIN 竞态/双客户端 UDP）

### v1.9.1

- **修复**: UDP 帧处理错误不再导致整条隧道链路断开（`handle_udp_frame` 内部捕获错误）
- **修复**: SYN 帧解码失败不再导致隧道断开
- **修复**: `encode_address` 域名超过 255 字节时不再静默截断，改为返回错误
- **修复**: UDP 中继响应数据报在无可用隧道时增加警告日志
- **修复**: 防止 `UDP_CONN_ID` 的非 DATA 帧意外关闭 UDP 中继
- **修复**: heartbeat 中清理因 task panic 残留的孤儿虚拟连接（splitter 侧）

## License

MIT
