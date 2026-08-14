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
    /// DATA send timeout: no live tunnel can take a frame within this
    /// window → the connection cannot proceed (O5, was a hardcoded 30s).
    #[serde(default = "default_data_send_timeout")]
    pub data_send_timeout_secs: u64,
    /// Heartbeat / connection-sweep interval in seconds (O5).
    #[serde(default = "default_heartbeat")]
    pub heartbeat_secs: u64,
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
    /// DATA send timeout, same semantics as the splitter's (O5).
    #[serde(default = "default_data_send_timeout")]
    pub data_send_timeout_secs: u64,
    /// Heartbeat / connection-sweep interval in seconds (O5; the
    /// reassembler previously hardcoded 60s).
    #[serde(default = "default_heartbeat")]
    pub heartbeat_secs: u64,
}

fn default_data_send_timeout() -> u64 {
    30
}

fn default_heartbeat() -> u64 {
    60
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

// Log cleanup handled by logging::DailyLogWriter at startup and on daily rotation.

// ── Port parsing ──────────────────────────────────────────────────────

/// BUG-14: sanity cap on a single port range — a config typo like
/// "1-65535" would otherwise spawn 65535 listeners at startup.
const MAX_PORTS: usize = 256;

pub fn parse_ports(ports: &Ports) -> Result<Vec<u16>> {
    let mut out = match ports {
        Ports::List(v) => v.clone(),
        Ports::Range(s) => {
            if let Some((start, end)) = s.split_once('-') {
                let start: u16 = start.trim().parse()?;
                let end: u16 = end.trim().parse()?;
                if start > end {
                    bail!("port range: start > end");
                }
                let count = end as usize - start as usize + 1;
                if count > MAX_PORTS {
                    bail!("port range {start}-{end} has {count} ports, max {MAX_PORTS}");
                }
                (start..=end).collect()
            } else {
                vec![s.trim().parse()?]
            }
        }
    };
    // BUG-14: duplicate ports make the second listener fail its bind and
    // silently die — dedup and warn instead of half-working configs.
    let mut seen = std::collections::HashSet::new();
    let before = out.len();
    out.retain(|p| seen.insert(*p));
    if out.len() != before {
        tracing::warn!(
            duplicates = before - out.len(),
            "duplicate ports removed from configuration"
        );
    }
    if out.is_empty() {
        bail!("no valid ports in configuration");
    }
    Ok(out)
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

    #[test]
    fn parse_ports_dedups_keeping_order() {
        let ports = Ports::List(vec![52312, 52311, 52312, 52311, 52313]);
        let result = parse_ports(&ports).unwrap();
        assert_eq!(result, vec![52312, 52311, 52313]);
    }

    #[test]
    fn parse_ports_rejects_huge_range() {
        let ports = Ports::Range("1-65535".into());
        assert!(parse_ports(&ports).is_err());
    }

    /// O5: the new timeout fields default and parse from TOML.
    #[test]
    fn timeout_fields_default_and_parse() {
        let cfg: Config = toml::from_str(
            r#"
mode = "splitter"

[splitter]
tunnel = [{ proxy = "127.0.0.1:1080", target = "127.0.0.1", port = 1 }]

[reassembler]
"#,
        )
        .unwrap();
        let sc = cfg.splitter.unwrap();
        assert_eq!(sc.data_send_timeout_secs, 30);
        assert_eq!(sc.heartbeat_secs, 60);
        let rc = cfg.reassembler.unwrap();
        assert_eq!(rc.data_send_timeout_secs, 30);
        assert_eq!(rc.heartbeat_secs, 60);

        let cfg: Config = toml::from_str(
            r#"
mode = "splitter"

[splitter]
data_send_timeout_secs = 5
heartbeat_secs = 2
tunnel = []

[reassembler]
data_send_timeout_secs = 7
heartbeat_secs = 3
"#,
        )
        .unwrap();
        assert_eq!(cfg.splitter.unwrap().data_send_timeout_secs, 5);
        assert_eq!(cfg.reassembler.unwrap().heartbeat_secs, 3);
    }
}
