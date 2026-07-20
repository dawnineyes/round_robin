#![windows_subsystem = "windows"]

mod frame;
mod reassembler;
mod socks5;
mod splitter;

use anyhow::{bail, Result};
use serde::Deserialize;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use tracing_appender::rolling::{RollingFileAppender, Rotation};

// ── TOML config schema ────────────────────────────────────────────────

#[derive(Deserialize)]
struct Config {
    /// "splitter" or "reassembler"
    mode: String,

    /// Enable daily rolling file logging (default true).
    #[serde(default = "default_true")]
    log: bool,

    #[serde(default)]
    splitter: Option<SplitterConfig>,
    #[serde(default)]
    reassembler: Option<ReassemblerConfig>,
}

fn default_true() -> bool {
    true
}

#[derive(Deserialize)]
struct SplitterConfig {
    #[serde(default = "default_splitter_listen")]
    listen: SocketAddr,
    #[serde(default = "default_chunk")]
    chunk_size: usize,
    tunnel: Vec<Tunnel>,
}

#[derive(Deserialize)]
struct ReassemblerConfig {
    #[serde(default = "default_listen_ip")]
    listen: std::net::IpAddr,
    #[serde(default = "default_reassembler_ports")]
    ports: Ports,
    #[serde(default = "default_local_target")]
    local_target: SocketAddr,
    #[serde(default = "default_chunk")]
    chunk_size: usize,
}

fn default_listen_ip() -> std::net::IpAddr {
    "127.0.0.1".parse().unwrap()
}

#[derive(Deserialize)]
#[serde(untagged)]
enum Ports {
    Range(String),
    List(Vec<u16>),
}

#[derive(Deserialize)]
struct Tunnel {
    proxy: SocketAddr,
    target: String,
    port: u16,
}

fn default_chunk() -> usize {
    16384
}

fn default_splitter_listen() -> SocketAddr {
    "127.0.0.1:52030".parse().unwrap()
}

fn default_reassembler_ports() -> Ports {
    Ports::Range("52031-52039".into())
}

fn default_local_target() -> SocketAddr {
    "127.0.0.1:52030".parse().unwrap()
}

// ── Path helpers ──────────────────────────────────────────────────────

fn exe_dir() -> Option<PathBuf> {
    std::env::current_exe()
        .ok()
        .and_then(|p| p.parent().map(|d| d.to_path_buf()))
}

// ── Config loading ────────────────────────────────────────────────────

fn find_config() -> Result<String> {
    for name in &["config.toml", "round_robin.toml"] {
        if let Some(ref dir) = exe_dir() {
            let path = dir.join(name);
            if path.is_file() {
                return Ok(std::fs::read_to_string(&path)?);
            }
        }
        if Path::new(name).is_file() {
            return Ok(std::fs::read_to_string(name)?);
        }
    }
    bail!("no config file found: tried config.toml, round_robin.toml")
}

// ── Log cleanup ───────────────────────────────────────────────────────

fn purge_old_logs(log_dir: &Path, keep_days: u64) {
    let cutoff = std::time::SystemTime::now()
        .checked_sub(std::time::Duration::from_secs(keep_days * 86400));
    let Some(cutoff) = cutoff else { return };

    let Ok(entries) = std::fs::read_dir(log_dir) else { return };
    for entry in entries.flatten() {
        let path = entry.path();
        let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
        // Match round_robin.YYYY-MM-DD.log
        if !name.starts_with("round_robin.") || !name.ends_with(".log") {
            continue;
        }
        if let Ok(meta) = entry.metadata() {
            if let Ok(mod_time) = meta.modified() {
                if mod_time < cutoff {
                    let _ = std::fs::remove_file(&path);
                }
            }
        }
    }
}

fn parse_ports(ports: &Ports) -> Result<Vec<u16>> {
    match ports {
        Ports::List(v) => Ok(v.clone()),
        Ports::Range(s) => {
            if let Some((start, end)) = s.split_once('-') {
                let start: u16 = start.trim().parse()?;
                let end: u16 = end.trim().parse()?;
                if start > end {
                    bail!("port range: start > end");
                }
                Ok((start..=end).collect())
            } else {
                Ok(vec![s.trim().parse()?])
            }
        }
    }
}

// ── Main ──────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<()> {
    let content = find_config()?;
    let cfg: Config = toml::from_str(&content)?;

    // Conditional file logging
    let log_dir = exe_dir().unwrap_or_else(|| PathBuf::from("."));
    let _guard: Option<Box<dyn std::any::Any + Send>> = if cfg.log {
        let file_appender = RollingFileAppender::new(Rotation::DAILY, &log_dir, "round_robin");
        let (non_blocking, guard) = tracing_appender::non_blocking(file_appender);
        tracing_subscriber::fmt()
            .with_writer(non_blocking)
            .init();
        Some(Box::new(guard))
    } else {
        None
    };
    // _guard lives until main() exits, flushes remaining logs on drop

    // Background: purge log files older than 7 days, check once per day
    tokio::spawn(async move {
        loop {
            purge_old_logs(&log_dir, 7);
            tokio::time::sleep(std::time::Duration::from_secs(86400)).await;
        }
    });

    // Startup banner (goes to log file if logging enabled, otherwise discarded)
    let (listen, tunnels) = match cfg.mode.as_str() {
        "splitter" => {
            let sc = cfg.splitter.as_ref();
            (sc.map(|s| s.listen.to_string()).unwrap_or_default(), sc.map(|s| s.tunnel.len()).unwrap_or(0))
        }
        "reassembler" => {
            let rc = cfg.reassembler.as_ref();
            (rc.map(|r| r.local_target.to_string()).unwrap_or_default(),
             rc.map(|r| parse_ports(&r.ports).map(|v| v.len()).unwrap_or(0)).unwrap_or(0))
        }
        _ => (String::new(), 0),
    };
    tracing::info!(version = "1.4", mode = %cfg.mode, log = cfg.log, listen, tunnels, "round_robin starting");

    match cfg.mode.as_str() {
        "splitter" => {
            let sc = cfg.splitter.ok_or_else(|| anyhow::anyhow!("config missing [splitter] section"))?;
            if sc.chunk_size < frame::MIN_CHUNK || sc.chunk_size > frame::MAX_CHUNK {
                bail!("splitter.chunk_size must be {}..{}", frame::MIN_CHUNK, frame::MAX_CHUNK);
            }
            let tunnels: Vec<splitter::TunnelEndpoint> = sc.tunnel.iter().map(|t| {
                splitter::TunnelEndpoint { proxy: t.proxy, target: t.target.clone(), port: t.port }
            }).collect();
            if tunnels.is_empty() {
                bail!("[splitter] has no [[splitter.tunnel]] entries");
            }
            splitter::run_splitter(splitter::SplitterConfig {
                listen_addr: sc.listen,
                tunnels,
                chunk_size: sc.chunk_size,
            }).await
        }
        "reassembler" => {
            let rc = cfg.reassembler.ok_or_else(|| anyhow::anyhow!("config missing [reassembler] section"))?;
            if rc.chunk_size < frame::MIN_CHUNK || rc.chunk_size > frame::MAX_CHUNK {
                bail!("reassembler.chunk_size must be {}..{}", frame::MIN_CHUNK, frame::MAX_CHUNK);
            }
            let ports = parse_ports(&rc.ports)?;
            reassembler::run_reassembler(reassembler::ReassemblerConfig {
                listen_ip: rc.listen,
                listen_ports: ports,
                local_target: rc.local_target,
                chunk_size: rc.chunk_size,
            }).await
        }
        other => bail!("unknown mode: {other}, expected \"splitter\" or \"reassembler\""),
    }
}
