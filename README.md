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
listen = "127.0.0.1:52035"

[[splitter.tunnel]]
proxy = "127.0.0.1:52036"
target = "127.0.0.1"
port = 52036
# ... 每 tunnel 一组，proxy/port 对应 sing-box SOCKS5 入站
```

### Debian 端（Reassembler）

```bash
curl -sSfL https://raw.githubusercontent.com/dawnineyes/round_robin/master/install.sh | bash
sudo systemctl restart round_robin
```

`/opt/round_robin/config.toml`：

```toml
mode = "reassembler"

[reassembler]
listen = "127.0.0.1"
ports = "52036-52039"
local_target = "127.0.0.1:52035"
```

## 配置参考

### Splitter

| 字段 | 类型 | 说明 |
|------|------|------|
| `listen` | SocketAddr | SOCKS5 入站地址 |
| `chunk_size` | usize | 分片大小 512-65535，默认 16384 |
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
| `chunk_size` | usize | 分片大小，默认 16384 |

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

ACK 帧保留在协议中但当前版本不使用——TUIC TCP 保证送达，无需应用层流量控制。

## 发布

```bash
git tag v1.8.1
git push origin v1.8.1
```

GitHub Actions 自动编译 Linux x86_64 并发布 Release。

## License

MIT
