//! round_robin — multi-path TCP tunnel aggregation.
//!
//! Library target: exposes the modules so integration tests
//! (`tests/e2e.rs`) can drive splitter and reassembler in-process.

pub mod config;
pub mod frame;
pub mod logging;
pub mod reassembler;
pub mod reorder;
pub mod socks5;
pub mod splitter;
pub mod tunnel;

/// Graceful shutdown on both Ctrl+C (SIGINT) and SIGTERM.
/// BUG-13: systemd sends SIGTERM on `systemctl stop`; without this the
/// process is killed mid-flight. On Windows this degrades to Ctrl+C only
/// (the GUI-subsystem binary has no console to receive either).
#[cfg(unix)]
pub async fn shutdown_signal() {
    use tokio::signal::unix::{SignalKind, signal};
    let mut term = signal(SignalKind::terminate()).expect("failed to install SIGTERM handler");
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {},
        _ = term.recv() => {},
    }
    tracing::info!("shutdown signal received");
}

#[cfg(not(unix))]
pub async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
    tracing::info!("shutdown signal received");
}
