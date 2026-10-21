//! `--out statsd[=ADDR]` and `--out datadog[=ADDR]`: samples as StatsD lines
//! over UDP, like k6's outputs. Counters are counts (`|c`), gauges gauges
//! (`|g`) and trends timings (`|ms`); a check's samples count
//! `check.<name>.pass` or `.fail`, other rates count 0 or 1. With tags on
//! (always for `datadog`), tags go in DogStatsD's `|#key:value` form.

use std::io;
use std::net::{SocketAddr, ToSocketAddrs, UdpSocket};
use std::time::{Duration, Instant};

use k9_metrics::{MetricKind, Metrics, Sample};

use crate::Output;
use crate::push::Settings;

const DEFAULT_ADDR: &str = "localhost:8125";
/// Fits a datagram in a typical MTU.
const MAX_DATAGRAM: usize = 1432;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatsdFlavor {
    Statsd,
    Datadog,
}

#[derive(Debug, Clone, PartialEq)]
pub struct StatsdConfig {
    pub flavor: StatsdFlavor,
    pub address: SocketAddr,
    pub namespace: String,
    pub push_interval: Duration,
    /// Lines per datagram, at most.
    pub buffer_size: usize,
    pub enable_tags: bool,
    pub tag_blocklist: Vec<String>,
}

impl StatsdConfig {
    /// k6's settings: `K6_STATSD_*` (or `K6_DATADOG_*`) variables, each of
    /// which can also be given as `K9_…` (which wins). `address` (from
    /// `--out statsd=ADDR`) beats both.
    pub fn from_env(
        flavor: StatsdFlavor,
        address: Option<&str>,
        vars: impl IntoIterator<Item = (String, String)>,
    ) -> Result<StatsdConfig, String> {
        let group = match flavor {
            StatsdFlavor::Statsd => "STATSD",
            StatsdFlavor::Datadog => "DATADOG",
        };
        let settings = Settings::from_vars(group, vars);
        let (from, address) = match address.filter(|a| !a.is_empty()) {
            Some(address) => ("--out", address),
            None => settings
                .get("ADDR")
                .unwrap_or(("the default", DEFAULT_ADDR)),
        };
        let resolved = address
            .to_socket_addrs()
            .ok()
            .and_then(|mut all| all.next())
            .ok_or_else(|| format!("{from}: expected host:port, got {address:?}"))?;
        let buffer_size = match settings.get("BUFFER_SIZE") {
            Some((name, value)) => value
                .trim()
                .parse::<usize>()
                .ok()
                .filter(|n| *n > 0)
                .ok_or_else(|| format!("{name}: expected a number above 0, got {value:?}"))?,
            None => 20,
        };
        let blocklist = settings
            .get("TAG_BLOCKLIST")
            .or_else(|| settings.get("TAG_BLACKLIST"))
            .map_or("vu,iter,url", |(_, v)| v);
        Ok(StatsdConfig {
            flavor,
            address: resolved,
            namespace: settings
                .get("NAMESPACE")
                .map_or("k6.", |(_, v)| v)
                .to_owned(),
            push_interval: settings
                .duration("PUSH_INTERVAL")?
                .unwrap_or(Duration::from_secs(1)),
            buffer_size,
            enable_tags: settings
                .bool("ENABLE_TAGS")?
                .unwrap_or(flavor == StatsdFlavor::Datadog),
            tag_blocklist: blocklist
                .split(',')
                .map(str::trim)
                .filter(|t| !t.is_empty())
                .map(str::to_owned)
                .collect(),
        })
    }
}

/// What names can't hold.
const NAME: &[char] = &[':', '|', '@'];

/// Characters StatsD gives a meaning to, in names and tags, become `_`.
fn clean(out: &mut String, text: &str, specials: &[char]) {
    out.extend(text.chars().map(|c| {
        if specials.contains(&c) || c.is_control() {
            '_'
        } else {
            c
        }
    }));
}

pub struct StatsdOutput {
    config: StatsdConfig,
    socket: UdpSocket,
    lines: Vec<String>,
    last_push: Instant,
    warned: bool,
}

impl StatsdOutput {
    pub fn start(config: StatsdConfig) -> io::Result<StatsdOutput> {
        let local: SocketAddr = if config.address.is_ipv4() {
            ([0, 0, 0, 0], 0).into()
        } else {
            (std::net::Ipv6Addr::UNSPECIFIED, 0).into()
        };
        let socket = UdpSocket::bind(local)?;
        socket.connect(config.address)?;
        Ok(StatsdOutput {
            config,
            socket,
            lines: Vec::new(),
            last_push: Instant::now(),
            warned: false,
        })
    }

    fn line(&self, sample: &Sample, metrics: &Metrics) -> Option<String> {
        if !sample.value.is_finite() {
            return None;
        }
        let def = metrics.registry().def(sample.metric);
        let tags = metrics.interner().get(sample.tags);
        let mut line = self.config.namespace.clone();
        let check = tags.get("check").filter(|c| !c.is_empty());
        let (value, kind) = match (def.kind, check) {
            (MetricKind::Rate, Some(check)) => {
                let outcome = if sample.value != 0.0 { "pass" } else { "fail" };
                clean(&mut line, &format!("check.{check}.{outcome}"), NAME);
                ("1".to_owned(), "c")
            }
            (kind, _) => {
                clean(&mut line, &def.name, NAME);
                match kind {
                    MetricKind::Counter => ((sample.value.trunc() as i64).to_string(), "c"),
                    MetricKind::Gauge => (sample.value.to_string(), "g"),
                    MetricKind::Trend => (sample.value.to_string(), "ms"),
                    MetricKind::Rate => (u8::from(sample.value != 0.0).to_string(), "c"),
                }
            }
        };
        line.push_str(&format!(":{value}|{kind}"));
        if self.config.enable_tags {
            let mut first = true;
            for (key, value) in tags.iter() {
                if value.is_empty() || self.config.tag_blocklist.iter().any(|b| b == key) {
                    continue;
                }
                line.push_str(if first { "|#" } else { "," });
                first = false;
                clean(&mut line, key, &[':', '|', ',', '#']);
                line.push(':');
                clean(&mut line, value, &['|', ',', '#']);
            }
        }
        Some(line)
    }

    /// Sends the buffered lines, packed into datagrams.
    fn flush(&mut self) {
        self.last_push = Instant::now();
        let mut datagram = String::new();
        let mut count = 0;
        let lines = std::mem::take(&mut self.lines);
        for line in lines {
            if count > 0
                && (count == self.config.buffer_size
                    || datagram.len() + 1 + line.len() > MAX_DATAGRAM)
            {
                self.send(&datagram);
                datagram.clear();
                count = 0;
            }
            if count > 0 {
                datagram.push('\n');
            }
            datagram.push_str(&line);
            count += 1;
        }
        if count > 0 {
            self.send(&datagram);
        }
    }

    fn send(&mut self, datagram: &str) {
        if let Err(e) = self.socket.send(datagram.as_bytes())
            && !self.warned
        {
            eprintln!("WARN {}: {e}", self.description());
            self.warned = true;
        }
    }
}

impl Output for StatsdOutput {
    fn description(&self) -> String {
        let name = match self.config.flavor {
            StatsdFlavor::Statsd => "statsd",
            StatsdFlavor::Datadog => "datadog",
        };
        format!("{name} ({})", self.config.address)
    }

    fn add_samples(&mut self, samples: &[Sample], metrics: &Metrics) -> io::Result<()> {
        for sample in samples {
            if let Some(line) = self.line(sample, metrics) {
                self.lines.push(line);
            }
        }
        let interval = self.config.push_interval;
        if self.last_push.elapsed() + interval / 10 >= interval {
            self.flush();
        }
        Ok(())
    }

    fn stop(&mut self, _metrics: &Metrics) -> io::Result<()> {
        self.flush();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lines_are_packed_into_datagrams_that_fit() {
        let receiver = UdpSocket::bind("127.0.0.1:0").unwrap();
        receiver
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        let address = receiver.local_addr().unwrap().to_string();
        let config =
            StatsdConfig::from_env(StatsdFlavor::Statsd, Some(&address), Vec::new()).unwrap();
        assert_eq!(config.buffer_size, 20);
        assert_eq!(config.tag_blocklist, ["vu", "iter", "url"]);
        assert!(!config.enable_tags);
        let mut output = StatsdOutput::start(config).unwrap();
        // 30 lines of 100 bytes: 14 fit in 1432 bytes with their newlines.
        output.lines = (0..30).map(|i| format!("{i:0>100}")).collect();
        output.flush();
        let mut buf = [0u8; 4096];
        let sizes: Vec<usize> = (0..3)
            .map(|_| {
                let n = receiver.recv(&mut buf).unwrap();
                std::str::from_utf8(&buf[..n]).unwrap().lines().count()
            })
            .collect();
        assert_eq!(sizes, [14, 14, 2]);
    }

    #[test]
    fn names_and_tags_are_cleaned() {
        let mut out = String::new();
        clean(&mut out, "a:b|c@d\ne", NAME);
        assert_eq!(out, "a_b_c_d_e");
    }
}
