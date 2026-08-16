use anyhow::{Result, bail};
use serde::Deserialize;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use crate::frame;

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
    /// B58: per-connection reorder-window byte budget.  Must cover the
    /// sender's in-flight window (tunnels × 128 frames × chunk_size —
    /// 32 MB for 4 tunnels at the default chunk) or latency skew
    /// between tunnels overflows the window and resets the connection.
    #[serde(default = "default_reorder_window")]
    pub reorder_window_bytes: u64,
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
    /// B58: per-connection reorder-window byte budget (uploads flow this
    /// direction; same in-flight math as the splitter's).
    #[serde(default = "default_reorder_window")]
    pub reorder_window_bytes: u64,
}

fn default_data_send_timeout() -> u64 {
    30
}

fn default_heartbeat() -> u64 {
    60
}

/// B58: default reorder-window byte budget (see `MAX_REORDER_BYTES`).
fn default_reorder_window() -> u64 {
    64 * 1024 * 1024
}

fn default_listen_ip() -> std::net::IpAddr {
    "127.0.0.1".parse().unwrap()
}

// ── Runtime validation ─────────────────────────────────────────────────

/// B55: validate a duration in seconds — serde cannot express the
/// invariants.  0 is a foot-gun in both directions: a 0-interval
/// heartbeat makes the sweep task spin (sleep(0) + a full map sweep per
/// iteration, 100% CPU), and a 0 send timeout makes every DATA/FIN send
/// time out instantly (all connections reset in a cascade).
fn validate_secs(name: &str, secs: u64) -> Result<()> {
    if secs == 0 {
        bail!("{name} must be >= 1 second, got 0");
    }
    Ok(())
}

impl SplitterConfig {
    /// B55: runtime invariants that serde defaults alone don't guard.
    /// chunk_size is bounded by the wire format (u16 length) and the
    /// decoder buffer — oversized chunks would make every tunnel die on
    /// its first encode (BUG-12 semantics).
    pub fn validate(&self) -> Result<()> {
        if self.chunk_size < frame::MIN_CHUNK || self.chunk_size > frame::MAX_CHUNK {
            bail!(
                "splitter.chunk_size must be {}..{}",
                frame::MIN_CHUNK,
                frame::MAX_CHUNK
            );
        }
        validate_secs(
            "splitter.data_send_timeout_secs",
            self.data_send_timeout_secs,
        )?;
        validate_secs("splitter.heartbeat_secs", self.heartbeat_secs)?;
        validate_reorder_window(
            "splitter.reorder_window_bytes",
            self.reorder_window_bytes,
            self.chunk_size as u64,
        )?;
        if self.tunnel.iter().any(|t| t.port == 0) {
            bail!("splitter.tunnel port must not be 0");
        }
        if self.tunnel.iter().any(|t| t.proxy.port() == 0) {
            bail!("splitter.tunnel proxy port must not be 0");
        }
        Ok(())
    }
}

impl ReassemblerConfig {
    /// B55: see SplitterConfig::validate.
    pub fn validate(&self) -> Result<()> {
        if self.chunk_size < frame::MIN_CHUNK || self.chunk_size > frame::MAX_CHUNK {
            bail!(
                "reassembler.chunk_size must be {}..{}",
                frame::MIN_CHUNK,
                frame::MAX_CHUNK
            );
        }
        validate_secs(
            "reassembler.data_send_timeout_secs",
            self.data_send_timeout_secs,
        )?;
        validate_secs("reassembler.heartbeat_secs", self.heartbeat_secs)?;
        validate_reorder_window(
            "reassembler.reorder_window_bytes",
            self.reorder_window_bytes,
            self.chunk_size as u64,
        )?;
        Ok(())
    }
}

/// B58: a window smaller than one chunk means every out-of-order frame
/// overflows (instant reset); 1 GB is the sanity cap.
fn validate_reorder_window(name: &str, bytes: u64, chunk_size: u64) -> Result<()> {
    if bytes < chunk_size {
        bail!("{name} must be >= chunk_size ({chunk_size} bytes), got {bytes}");
    }
    if bytes > 1024 * 1024 * 1024 {
        bail!("{name} must be <= 1 GiB, got {bytes}");
    }
    Ok(())
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
    // B53: the sanity cap applies to explicit lists too — a generated
    // list of thousands of ports would spawn that many listener tasks
    // (and bind failures) at startup.  Range and List share one limit.
    if out.len() > MAX_PORTS {
        bail!("port list has {} entries, max {MAX_PORTS}", out.len());
    }
    // Port 0 has no valid listener meaning here: a reassembler would bind a
    // random port while the splitter still tries to connect to port 0.
    if out.contains(&0) {
        bail!("port 0 is not allowed in reassembler.ports");
    }
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

    /// B53: explicit lists share the Range sanity cap — a generated list
    /// of thousands of ports must not spawn that many listeners at
    /// startup.
    #[test]
    fn parse_ports_rejects_huge_list() {
        let ports = Ports::List((1..=257).map(|p| p as u16).collect());
        assert!(parse_ports(&ports).is_err());
    }

    #[test]
    fn parse_ports_rejects_zero() {
        assert!(parse_ports(&Ports::List(vec![0])).is_err());
        assert!(parse_ports(&Ports::Range("0-10".into())).is_err());
        assert!(parse_ports(&Ports::Range("52310".into())).is_ok());
    }

    /// B55: zero timeouts must be rejected at startup — a 0 heartbeat
    /// interval spins the sweep task at 100% CPU, a 0 send timeout makes
    /// every DATA/FIN send time out instantly (connection cascade).
    #[test]
    fn validate_rejects_zero_timeouts() {
        let cfg: Config = toml::from_str(
            r#"
mode = "splitter"

[splitter]
heartbeat_secs = 0
tunnel = []
"#,
        )
        .unwrap();
        assert!(cfg.splitter.unwrap().validate().is_err());

        let cfg: Config = toml::from_str(
            r#"
mode = "reassembler"

[reassembler]
data_send_timeout_secs = 0
"#,
        )
        .unwrap();
        assert!(cfg.reassembler.unwrap().validate().is_err());

        // Positive control: defaults validate.
        let cfg: Config = toml::from_str(
            r#"
mode = "splitter"

[splitter]
tunnel = []
"#,
        )
        .unwrap();
        cfg.splitter.unwrap().validate().unwrap();
    }

    #[test]
    fn validate_rejects_tunnel_port_zero() {
        let cfg: Config = toml::from_str(
            r#"
mode = "splitter"

[splitter]
[[splitter.tunnel]]
proxy = "127.0.0.1:1080"
target = "127.0.0.1"
port = 0
"#,
        )
        .unwrap();
        assert!(cfg.splitter.unwrap().validate().is_err());
    }

    /// B58: the reorder window defaults to 64 MB and must at least hold
    /// one chunk (a smaller window overflows on the first out-of-order
    /// frame and resets every connection).
    #[test]
    fn reorder_window_defaults_and_validation() {
        let cfg: Config = toml::from_str(
            r#"
mode = "splitter"

[splitter]
tunnel = []
"#,
        )
        .unwrap();
        let sc = cfg.splitter.unwrap();
        assert_eq!(sc.reorder_window_bytes, 64 * 1024 * 1024);
        sc.validate().unwrap();

        // Window smaller than the default chunk → rejected.
        let cfg: Config = toml::from_str(
            r#"
mode = "splitter"

[splitter]
reorder_window_bytes = 65534
tunnel = []
"#,
        )
        .unwrap();
        assert!(cfg.splitter.unwrap().validate().is_err());

        // Sanity cap: > 1 GiB rejected.
        let cfg: Config = toml::from_str(
            r#"
mode = "splitter"

[splitter]
reorder_window_bytes = 2147483648
tunnel = []
"#,
        )
        .unwrap();
        assert!(cfg.splitter.unwrap().validate().is_err());
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
