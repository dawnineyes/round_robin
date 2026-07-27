#![windows_subsystem = "windows"]

mod config;
mod frame;
mod logging;
mod reassembler;
mod reorder;
mod socks5;
mod splitter;
mod tunnel;

use anyhow::{Result, bail};
use config::{find_config, parse_ports};
use std::path::PathBuf;

// ── Main ──────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<()> {
    let content = find_config()?;
    let cfg: config::Config = toml::from_str(&content)?;

    // File logging: daily rotation, no ANSI, compact format
    let log_dir = config::exe_dir().unwrap_or_else(|| PathBuf::from("."));
    if cfg.log {
        let writer = logging::DailyLogWriter::new(log_dir.clone(), "round_robin", 7)
            .expect("failed to create log writer");
        tracing_subscriber::fmt()
            .with_ansi(false)
            .with_target(false)
            .compact()
            .with_writer(writer)
            .init();
    }
    // Stale-log purge happens on daily rotation inside DailyLogWriter.

    // Startup banner (goes to log file if logging enabled, otherwise discarded)
    let (listen, tunnels) = match cfg.mode.as_str() {
        "splitter" => {
            let sc = cfg.splitter.as_ref();
            (
                sc.map(|s| s.listen.to_string()).unwrap_or_default(),
                sc.map(|s| s.tunnel.len()).unwrap_or(0),
            )
        }
        "reassembler" => {
            let rc = cfg.reassembler.as_ref();
            (
                rc.map(|r| r.local_target.to_string()).unwrap_or_default(),
                rc.map(|r| parse_ports(&r.ports).map(|v| v.len()).unwrap_or(0))
                    .unwrap_or(0),
            )
        }
        _ => (String::new(), 0),
    };
    tracing::info!(version = env!("CARGO_PKG_VERSION"), mode = %cfg.mode, log = cfg.log, listen, tunnels, "round_robin starting");

    match cfg.mode.as_str() {
        "splitter" => {
            let sc = cfg
                .splitter
                .ok_or_else(|| anyhow::anyhow!("config missing [splitter] section"))?;
            if sc.chunk_size < frame::MIN_CHUNK || sc.chunk_size > frame::MAX_CHUNK {
                bail!(
                    "splitter.chunk_size must be {}..{}",
                    frame::MIN_CHUNK,
                    frame::MAX_CHUNK
                );
            }
            let tunnels: Vec<splitter::TunnelEndpoint> = sc
                .tunnel
                .iter()
                .map(|t| splitter::TunnelEndpoint {
                    proxy: t.proxy,
                    target: t.target.clone(),
                    port: t.port,
                })
                .collect();
            if tunnels.is_empty() {
                bail!("[splitter] has no [[splitter.tunnel]] entries");
            }
            splitter::run_splitter(splitter::SplitterConfig {
                listen_addr: sc.listen,
                tunnels,
                chunk_size: sc.chunk_size,
            })
            .await
        }
        "reassembler" => {
            let rc = cfg
                .reassembler
                .ok_or_else(|| anyhow::anyhow!("config missing [reassembler] section"))?;
            if rc.chunk_size < frame::MIN_CHUNK || rc.chunk_size > frame::MAX_CHUNK {
                bail!(
                    "reassembler.chunk_size must be {}..{}",
                    frame::MIN_CHUNK,
                    frame::MAX_CHUNK
                );
            }
            let ports = parse_ports(&rc.ports)?;
            reassembler::run_reassembler(reassembler::ReassemblerConfig {
                listen_ip: rc.listen,
                listen_ports: ports,
                local_target: rc.local_target,
                chunk_size: rc.chunk_size,
            })
            .await
        }
        other => bail!("unknown mode: {other}, expected \"splitter\" or \"reassembler\""),
    }
}
