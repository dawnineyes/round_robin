use anyhow::{Result, bail};
use serde::Deserialize;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};

// ── TOML config schema ────────────────────────────────────────────────

#[derive(Deserialize)]
pub struct Config {
    /// "splitter" or "reassembler"
    pub mode: String,

    /// Enable daily rolling file logging (default true).
    #[serde(default = "default_true")]
    pub log: bool,

    #[serde(default)]
    pub splitter: Option<SplitterConfig>,
    #[serde(default)]
    pub reassembler: Option<ReassemblerConfig>,
}

fn default_true() -> bool {
    true
}

#[derive(Deserialize)]
pub struct SplitterConfig {
    #[serde(default = "default_splitter_listen")]
    pub listen: SocketAddr,
    #[serde(default = "default_chunk")]
    pub chunk_size: usize,
    pub tunnel: Vec<Tunnel>,
}

#[derive(Deserialize)]
pub struct ReassemblerConfig {
    #[serde(default = "default_listen_ip")]
    pub listen: std::net::IpAddr,
    #[serde(default = "default_reassembler_ports")]
    pub ports: Ports,
    #[serde(default = "default_local_target")]
    pub local_target: SocketAddr,
    #[serde(default = "default_chunk")]
    pub chunk_size: usize,
}

fn default_listen_ip() -> std::net::IpAddr {
    "127.0.0.1".parse().unwrap()
}

#[derive(Deserialize)]
#[serde(untagged)]
pub enum Ports {
    Range(String),
    List(Vec<u16>),
}

#[derive(Deserialize)]
pub struct Tunnel {
    pub proxy: SocketAddr,
    pub target: String,
    pub port: u16,
}

fn default_chunk() -> usize {
    65535
}

fn default_splitter_listen() -> SocketAddr {
    "127.0.0.1:52310".parse().unwrap()
}

fn default_reassembler_ports() -> Ports {
    Ports::Range("52311-52319".into())
}

fn default_local_target() -> SocketAddr {
    "127.0.0.1:52310".parse().unwrap()
}

// ── Path helpers ──────────────────────────────────────────────────────

pub fn exe_dir() -> Option<PathBuf> {
    std::env::current_exe()
        .ok()
        .and_then(|p| p.parent().map(|d| d.to_path_buf()))
}

// ── Config loading ────────────────────────────────────────────────────

pub fn find_config() -> Result<String> {
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

// Log cleanup handled by logging::DailyLogWriter on daily rotation.

// ── Port parsing ──────────────────────────────────────────────────────

pub fn parse_ports(ports: &Ports) -> Result<Vec<u16>> {
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

// ── tests ─────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_ports_range() {
        let ports = Ports::Range("52311-52313".into());
        let result = parse_ports(&ports).unwrap();
        assert_eq!(result, vec![52311, 52312, 52313]);
    }

    #[test]
    fn parse_ports_list() {
        let ports = Ports::List(vec![52311, 52312]);
        let result = parse_ports(&ports).unwrap();
        assert_eq!(result, vec![52311, 52312]);
    }

    #[test]
    fn parse_ports_single() {
        let ports = Ports::Range("52311".into());
        let result = parse_ports(&ports).unwrap();
        assert_eq!(result, vec![52311]);
    }

    #[test]
    fn parse_ports_invalid_range() {
        let ports = Ports::Range("52313-52311".into());
        assert!(parse_ports(&ports).is_err());
    }
}
