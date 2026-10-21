//! Warnings for when the load generator itself may be the bottleneck: too few
//! open files for the VUs, too few ephemeral ports, a saturated CPU. Numbers
//! measured on a machine that's out of resources describe the machine, not
//! the system under test.

/// File descriptors the run needs besides one socket per VU.
#[cfg(unix)]
const FD_OVERHEAD: u64 = 64;
/// Share of CPU time busy that counts as saturated…
const CPU_BUSY: f64 = 0.9;
/// …for this many samples (seconds) in a row.
const CPU_STREAK: u32 = 3;
/// Share of the ephemeral port range in TIME_WAIT that's worth a warning.
const TIME_WAIT_SHARE: f64 = 0.5;

/// A warning if `limit` open files can't hold a socket per VU (Windows has no
/// such limit).
#[cfg(unix)]
fn fd_warning(limit: u64, max_vus: u64) -> Option<String> {
    (limit < max_vus + FD_OVERHEAD).then(|| {
        format!(
            "the open-file limit ({limit}) is too low for {max_vus} VUs, which need a socket each: requests will fail with \"too many open files\"; raise it with `ulimit -n`"
        )
    })
}

/// A warning if there are more VUs than ephemeral ports (each VU's connection
/// to a target takes one).
fn ports_warning(range: (u32, u32), max_vus: u64) -> Option<String> {
    let (low, high) = range;
    let ports = u64::from(high.saturating_sub(low)) + 1;
    (max_vus > ports).then(|| {
        format!(
            "{max_vus} VUs may need more local ports than the {ports} ephemeral ports ({low}-{high}) available per target: connections may fail with \"address not available\"; widen net.ipv4.ip_local_port_range or spread the VUs over more machines"
        )
    })
}

/// `/proc/sys/net/ipv4/ip_local_port_range`: `low high`.
fn parse_port_range(text: &str) -> Option<(u32, u32)> {
    let mut parts = text.split_whitespace().map(str::parse::<u32>);
    match (parts.next(), parts.next()) {
        (Some(Ok(low)), Some(Ok(high))) if low <= high => Some((low, high)),
        _ => None,
    }
}

/// Sockets in TIME_WAIT, from `/proc/net/sockstat`'s `TCP:` line.
fn parse_time_wait(text: &str) -> Option<u64> {
    let line = text.lines().find(|l| l.starts_with("TCP:"))?;
    let fields: Vec<&str> = line.split_whitespace().collect();
    let at = fields.iter().position(|f| *f == "tw")?;
    fields.get(at + 1)?.parse().ok()
}

/// (busy, total) CPU time, from `/proc/stat`'s first line (idle and iowait
/// count as not busy).
fn parse_cpu(text: &str) -> Option<(u64, u64)> {
    let line = text.lines().next()?.strip_prefix("cpu ")?;
    let times: Vec<u64> = line
        .split_whitespace()
        .map(str::parse)
        .collect::<Result<_, _>>()
        .ok()?;
    if times.len() < 5 {
        return None;
    }
    // user nice system idle iowait irq softirq steal (guest time is in user).
    let total: u64 = times.iter().take(8).sum();
    let idle = times[3] + times[4];
    Some((total - idle, total))
}

/// Warns once when the CPU stays saturated.
#[derive(Default)]
struct CpuWatch {
    streak: u32,
    warned: bool,
}

impl CpuWatch {
    /// Takes one second's share of CPU time busy (0–1).
    fn observe(&mut self, busy: f64) -> Option<String> {
        if busy < CPU_BUSY {
            self.streak = 0;
            return None;
        }
        self.streak += 1;
        if self.streak < CPU_STREAK || self.warned {
            return None;
        }
        self.warned = true;
        Some(format!(
            "the CPU is {:.0}% busy: the load generator may be the bottleneck, and response times include its own delays; use fewer VUs, --workers, or more machines",
            busy * 100.0
        ))
    }
}

/// Warns once when TIME_WAIT sockets fill much of the port range.
fn time_wait_warning(time_wait: u64, range: (u32, u32)) -> Option<String> {
    let ports = f64::from(range.1.saturating_sub(range.0)) + 1.0;
    (time_wait as f64 > ports * TIME_WAIT_SHARE).then(|| {
        format!(
            "{time_wait} sockets on this machine are in TIME_WAIT, against {ports} ephemeral ports: connections are being opened and closed instead of reused, and new ones may soon fail with \"address not available\""
        )
    })
}

/// Before the run: raises the open-file limit as far as allowed (worker
/// processes started later inherit it), then warns about open files and
/// ports for `max_vus`.
pub(crate) fn check_at_start(max_vus: u64) -> Vec<String> {
    let mut warnings = Vec::new();
    #[cfg(unix)]
    if let Ok(limit) = rlimit::increase_nofile_limit(u64::MAX) {
        warnings.extend(fd_warning(limit, max_vus));
    }
    if let Some(range) = port_range() {
        warnings.extend(ports_warning(range, max_vus));
    }
    warnings
}

/// Checks the CPU and TIME_WAIT sockets every second until `done`, printing
/// each warning once. `live`: clear the status line first.
/// Watches the load generator's own CPU and ports, passing each warning to
/// `report`.
pub(crate) async fn watch(
    done: tokio_util::sync::CancellationToken,
    report: impl Fn(String) + Send + 'static,
) {
    let range = port_range();
    let mut cpu = CpuWatch::default();
    let mut last = read_proc("/proc/stat").and_then(|t| parse_cpu(&t));
    let mut time_wait_warned = false;
    let mut interval = tokio::time::interval(std::time::Duration::from_secs(1));
    interval.tick().await;
    loop {
        tokio::select! {
            _ = interval.tick() => {}
            _ = done.cancelled() => return,
        }
        let now = read_proc("/proc/stat").and_then(|t| parse_cpu(&t));
        if let (Some((busy0, total0)), Some((busy1, total1))) = (last, now)
            && total1 > total0
        {
            let busy = (busy1.saturating_sub(busy0)) as f64 / (total1 - total0) as f64;
            if let Some(warning) = cpu.observe(busy) {
                report(warning);
            }
        }
        last = now;
        if let (false, Some(range), Some(time_wait)) = (
            time_wait_warned,
            range,
            read_proc("/proc/net/sockstat").and_then(|t| parse_time_wait(&t)),
        ) && let Some(warning) = time_wait_warning(time_wait, range)
        {
            time_wait_warned = true;
            report(warning);
        }
    }
}

pub(crate) fn warn(warning: &str, live: bool) {
    if live {
        eprint!("\r\x1b[2K"); // the status line redraws on its next tick
    }
    eprintln!("WARN {warning}");
}

fn port_range() -> Option<(u32, u32)> {
    read_proc("/proc/sys/net/ipv4/ip_local_port_range").and_then(|t| parse_port_range(&t))
}

/// A `/proc` file's text (Linux only; elsewhere, nothing).
fn read_proc(path: &str) -> Option<String> {
    if cfg!(target_os = "linux") {
        std::fs::read_to_string(path).ok()
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[cfg(unix)]
    fn open_files_must_hold_a_socket_per_vu() {
        assert_eq!(fd_warning(1024, 900), None);
        let warning = fd_warning(1024, 1000).expect("1000 VUs + overhead > 1024");
        assert!(
            warning.contains("1024") && warning.contains("1000 VUs"),
            "{warning}"
        );
        assert!(warning.contains("ulimit -n"), "{warning}");
    }

    #[test]
    fn ports_must_outnumber_vus() {
        let range = (44620, 48715); // 4096 ports
        assert_eq!(ports_warning(range, 4096), None);
        let warning = ports_warning(range, 5000).expect("more VUs than ports");
        assert!(
            warning.contains("4096") && warning.contains("44620-48715"),
            "{warning}"
        );
    }

    #[test]
    fn proc_files_are_parsed() {
        assert_eq!(parse_port_range("44620\t48715\n"), Some((44620, 48715)));
        assert_eq!(parse_port_range("nonsense"), None);
        let sockstat =
            "sockets: used 323\nTCP: inuse 31 orphan 0 tw 38 alloc 32 mem 0\nUDP: inuse 4 mem 0\n";
        assert_eq!(parse_time_wait(sockstat), Some(38));
        assert_eq!(parse_time_wait("UDP: inuse 4\n"), None);
        // user nice system idle iowait irq softirq steal
        let stat = "cpu  100 10 50 800 40 0 0 0 0 0\ncpu0 1 2 3 4 5 6 7 8 9 10\n";
        assert_eq!(parse_cpu(stat), Some((160, 1000)));
    }

    #[test]
    fn a_saturated_cpu_is_reported_once_after_a_streak() {
        let mut watch = CpuWatch::default();
        assert_eq!(watch.observe(0.95), None);
        assert_eq!(watch.observe(0.95), None);
        assert_eq!(watch.observe(0.5), None, "the streak breaks");
        assert_eq!(watch.observe(0.95), None);
        assert_eq!(watch.observe(0.92), None);
        let warning = watch.observe(0.97).expect("three busy seconds in a row");
        assert!(warning.contains("97%"), "{warning}");
        assert_eq!(watch.observe(0.99), None, "only once");
    }

    #[test]
    fn time_wait_filling_the_port_range_is_reported() {
        let range = (1000, 1999);
        assert_eq!(time_wait_warning(400, range), None);
        let warning = time_wait_warning(600, range).expect("over half the range");
        assert!(
            warning.contains("600") && warning.contains("TIME_WAIT"),
            "{warning}"
        );
    }
}
