use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

/// Custom daily-rolling log writer.
///
/// Creates files named `prefix.YYYY-MM-DD.log` in `dir`, rotates at
/// midnight UTC, and purges files older than `keep_days`.
pub struct DailyLogWriter {
    inner: Mutex<Inner>,
    dir: PathBuf,
    prefix: String,
    keep_days: u64,
}

struct Inner {
    file: File,
    date: String, // "YYYY-MM-DD"
}

impl DailyLogWriter {
    pub fn new(dir: PathBuf, prefix: &str, keep_days: u64) -> io::Result<Self> {
        let date = today_utc();
        let file = open_log(&dir, prefix, &date)?;
        Ok(Self { inner: Mutex::new(Inner { file, date }), dir, prefix: prefix.to_string(), keep_days })
    }
}

// ── MakeWriter impl for tracing-subscriber ────────────────────────────

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for DailyLogWriter {
    type Writer = DailyWriter<'a>;

    fn make_writer(&'a self) -> Self::Writer {
        DailyWriter {
            inner: &self.inner,
            dir: &self.dir,
            prefix: &self.prefix,
            keep_days: self.keep_days,
        }
    }
}

pub struct DailyWriter<'a> {
    inner: &'a Mutex<Inner>,
    dir: &'a Path,
    prefix: &'a str,
    keep_days: u64,
}

impl<'a> Write for DailyWriter<'a> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let mut state = self.inner.lock().unwrap();
        let today = today_utc();
        if today != state.date {
            // Rotate: new file for the new day
            state.file = open_log(self.dir, self.prefix, &today)?;
            state.date = today;
            // Best-effort purge (once per day, first write after midnight)
            purge_old_logs(self.dir, self.prefix, self.keep_days);
        }
        state.file.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.lock().unwrap().file.flush()
    }
}

// ── Helpers ───────────────────────────────────────────────────────────

fn today_utc() -> String {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64;
    days_to_civil(secs / 86400)
}

/// Howard Hinnant's algorithm: days since 1970-01-01 → YYYY-MM-DD.
fn days_to_civil(days: i64) -> String {
    let z = days + 719468;
    let era = (if z >= 0 { z } else { z - 146096 }) / 146097;
    let doe = (z - era * 146097) as u32;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    format!("{:04}-{:02}-{:02}", y, m, d)
}

fn open_log(dir: &Path, prefix: &str, date: &str) -> io::Result<File> {
    let _ = fs::create_dir_all(dir);
    OpenOptions::new()
        .create(true)
        .append(true)
        .open(dir.join(format!("{}.{}.log", prefix, date)))
}

pub fn purge_old_logs(dir: &Path, prefix: &str, keep_days: u64) {
    let cutoff = std::time::SystemTime::now()
        .checked_sub(std::time::Duration::from_secs(keep_days * 86400));
    let Some(cutoff) = cutoff else { return };
    let Ok(entries) = fs::read_dir(dir) else { return };
    for entry in entries.flatten() {
        let path = entry.path();
        let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
        if !name.starts_with(prefix) || !name.ends_with(".log") {
            continue;
        }
        if let Ok(meta) = entry.metadata() {
            if let Ok(mod_time) = meta.modified() {
                if mod_time < cutoff {
                    let _ = fs::remove_file(&path);
                }
            }
        }
    }
}
