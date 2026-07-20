# round_robin

多路径 SOCKS5 负载均衡隧道 — 单条 TCP 流拆分到 9 条 TUIC 隧道并行传输，对端重组，单连接速度 ≈ 9 × 单隧道速度。

## 架构

```
Windows 11                                     Debian 13
─────────                                      ─────────
App → sing-box TUN                             Internet ← sing-box direct
  ↓                                               ↑
SOCKS5 :52030                                  SOCKS5 :52030 ←──────────┐
  ↓                                               ↑                     │
round_robin splitter                            round_robin reassembler │
  ↓ 9 条持久 SOCKS5 连接                         ↑ 9 个 SOCKS5 server    │
  ├→ sing-box :52031 → TUIC 1 → Internet ─→ TUIC 1 → sing-box → Rust :52031
  ├→ sing-box :52032 → TUIC 2 → Internet ─→ TUIC 2 → sing-box → Rust :52032
  ┊      ⋮                                         ⋮              ⋮
  └→ sing-box :52039 → TUIC 9 → Internet ─→ TUIC 9 → sing-box → Rust :52039
```

## 快速开始

### Windows 端（Splitter）

```bash
cargo build --release
```

`config.toml`（放在 exe 同目录）：

```toml
mode = "splitter"

[splitter]
listen = "127.0.0.1:52030"
chunk_size = 16384   # 分片大小 512-65535，默认 16384

[[splitter.tunnel]]
proxy = "127.0.0.1:52031"
target = "127.0.0.1"
port = 52031
# ... 复制 9 份，端口 52031-52039
```

双击 exe 即可（无控制台，日志写入 `round_robin.*.log`，7 天自动清理）。

### Debian 端（Reassembler）

```bash
curl -sSfL https://raw.githubusercontent.com/dawnineyes/round_robin/master/install.sh | bash
```

`/opt/round_robin/config.toml`：

```toml
mode = "reassembler"

[reassembler]
listen = "127.0.0.1"
ports = "52031-52039"
local_target = "127.0.0.1:52030"
chunk_size = 16384
```

```bash
sudo systemctl enable --now round_robin
journalctl -u round_robin -f
```

## 配置参考

### Splitter

| 字段 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `listen` | SocketAddr | `127.0.0.1:52030` | SOCKS5 入站监听地址 |
| `chunk_size` | usize | `16384` | 分片大小，范围 512-65535 |
| `[[splitter.tunnel]]` | array | 必填 | 隧道列表 |
| `tunnel.proxy` | SocketAddr | — | Windows sing-box SOCKS5 入站地址 |
| `tunnel.target` | String | — | Debian Rust 监听 IP |
| `tunnel.port` | u16 | — | Debian Rust 监听端口 |

### Reassembler

| 字段 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `listen` | IpAddr | `127.0.0.1` | 隧道监听 IP |
| `ports` | range/list | `52031-52039` | 监听端口范围 |
| `local_target` | SocketAddr | `127.0.0.1:52030` | 重组后出站 SOCKS5 目标 |
| `chunk_size` | usize | `16384` | 分片大小，范围 512-65535 |

## sing-box 配置要点

**Debian — TUIC 和 Rust 端口必须分开**（TUIC 544xx，Rust 520xx）：

```json
{
  "inbounds": [
    { "type": "tuic", "tag": "tuic-in-1", "listen_port": 54431 },
    { "type": "socks5", "tag": "socks-from-rust", "listen_port": 52030 }
  ],
  "outbounds": [
    { "type": "socks5", "tag": "to-rust-1", "server": "127.0.0.1", "server_port": 52031 },
    { "type": "direct", "tag": "direct-out" }
  ],
  "route": {
    "rules": [
      { "inbound": "tuic-in-1", "outbound": "to-rust-1" },
      { "inbound": "socks-from-rust", "outbound": "direct-out" }
    ]
  }
}
```

**Windows — 每个 SOCKS5 入站绑定固定 TUIC 出站**：

```json
{
  "inbounds": [
    { "type": "socks5", "tag": "socks-in-1", "listen_port": 52031 }
  ],
  "outbounds": [
    { "type": "tuic", "tag": "tuic-out-1", "server_port": 54431 }
  ],
  "route": {
    "rules": [
      { "inbound": "socks-in-1", "outbound": "tuic-out-1" }
    ]
  }
}
```

## 协议

帧格式（大端序，15 字节头）：

```
ConnID    u32   4 bytes
Sequence  u64   8 bytes
Flags     u8    1 byte    SYN=0x01 DATA=0x02 FIN=0x04 RST=0x08 ACK=0x10
Length    u16   2 bytes   payload 长度，0-65535
Payload   [u8]  Length bytes
```

SYN 帧 Payload: `Proto(u8) + AddrLen(u16) + Address(variable) + Port(u16)`

## 发布

```bash
git tag v1.0
git push origin v1.0
```

GitHub Actions 自动编译 Linux x86_64 二进制并发布 Release。

## License

MIT
