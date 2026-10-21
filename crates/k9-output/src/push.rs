//! What the push outputs (Prometheus remote write, OTLP, InfluxDB) share: k6-style
//! settings from the environment, a thread that sends their bodies, and
//! protobuf encoding.

use std::collections::HashMap;
use std::io;
use std::sync::mpsc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use bytes::Bytes;
use http::header::{HeaderName, HeaderValue};

/// Pushes waiting to be sent; beyond this, a push is skipped (the outputs
/// send cumulative values, so the next push carries the same data).
const QUEUE: usize = 4;
const PUSH_TIMEOUT: Duration = Duration::from_secs(10);

/// An output's `K6_<GROUP>_*` settings, each of which can also be given as
/// `K9_<GROUP>_*` (which wins).
pub(crate) struct Settings {
    /// Key (after the prefix) → (from K9_, variable name, value).
    map: HashMap<String, (bool, String, String)>,
}

impl Settings {
    pub fn from_vars(group: &str, vars: impl IntoIterator<Item = (String, String)>) -> Settings {
        let (k9, k6) = (format!("K9_{group}_"), format!("K6_{group}_"));
        let mut map: HashMap<String, (bool, String, String)> = HashMap::new();
        for (name, value) in vars {
            for (prefix, is_k9) in [(k9.as_str(), true), (k6.as_str(), false)] {
                if let Some(key) = name.strip_prefix(prefix) {
                    let entry =
                        map.entry(key.to_owned())
                            .or_insert((is_k9, name.clone(), value.clone()));
                    if is_k9 && !entry.0 {
                        *entry = (is_k9, name.clone(), value.clone());
                    }
                }
            }
        }
        Settings { map }
    }

    /// (variable name, value).
    pub fn get(&self, key: &str) -> Option<(&str, &str)> {
        self.map
            .get(key)
            .map(|(_, name, value)| (name.as_str(), value.as_str()))
    }

    /// Settings whose key starts with `prefix`, as (rest of the key, value).
    pub fn prefixed(&self, prefix: &str) -> Vec<(String, String)> {
        let mut found: Vec<(String, String)> = self
            .map
            .iter()
            .filter_map(|(key, (_, _, value))| {
                Some((key.strip_prefix(prefix)?.to_owned(), value.clone()))
            })
            .collect();
        found.sort();
        found
    }

    pub fn duration(&self, key: &str) -> Result<Option<Duration>, String> {
        let Some((name, value)) = self.get(key) else {
            return Ok(None);
        };
        parse_duration(value)
            .filter(|d| !d.is_zero())
            .map(Some)
            .ok_or_else(|| format!("{name}: expected a duration like 5s, got {value:?}"))
    }

    pub fn bool(&self, key: &str) -> Result<Option<bool>, String> {
        let Some((name, value)) = self.get(key) else {
            return Ok(None);
        };
        match value.trim().to_ascii_lowercase().as_str() {
            "true" | "1" => Ok(Some(true)),
            "false" | "0" | "" => Ok(Some(false)),
            _ => Err(format!("{name}: expected true or false, got {value:?}")),
        }
    }
}

/// `a=1,b=2` (or with `:`) → pairs.
pub(crate) fn pairs(name: &str, value: &str, sep: char) -> Result<Vec<(String, String)>, String> {
    value
        .split(',')
        .filter(|p| !p.trim().is_empty())
        .map(|pair| {
            let (k, v) = pair
                .split_once(sep)
                .ok_or_else(|| format!("{name}: expected key{sep}value pairs, got {pair:?}"))?;
            Ok((k.trim().to_owned(), v.trim().to_owned()))
        })
        .collect()
}

/// `5s`, `500ms`, `1m`, `1.5s`.
fn parse_duration(text: &str) -> Option<Duration> {
    let text = text.trim();
    let split = text.find(|c: char| c.is_ascii_alphabetic())?;
    let (number, unit) = text.split_at(split);
    let number: f64 = number.parse().ok()?;
    let seconds = match unit {
        "ms" => number / 1e3,
        "s" => number,
        "m" => number * 60.0,
        "h" => number * 3600.0,
        _ => return None,
    };
    (seconds.is_finite() && seconds >= 0.0).then(|| Duration::from_secs_f64(seconds))
}

pub(crate) fn check_headers(output: &str, headers: &[(String, String)]) -> Result<(), String> {
    for (header, value) in headers {
        HeaderName::try_from(header.as_str())
            .map_err(|e| format!("{output} header `{header}`: {e}"))?;
        HeaderValue::try_from(value.as_str())
            .map_err(|e| format!("{output} header `{header}`: {e}"))?;
    }
    Ok(())
}

/// Sends an output's bodies from a thread of its own, so a slow endpoint
/// never holds up the run, and decides when the next push is due.
pub(crate) struct Sender {
    interval: Duration,
    last_push: Instant,
    queue: Option<mpsc::SyncSender<Vec<u8>>>,
    thread: Option<JoinHandle<Vec<String>>>,
}

impl Sender {
    /// `output` names it in warnings; `headers` include the content type.
    /// `prelude`, a URL, is posted to (with the same headers) before the
    /// first push; it failing is only a warning.
    pub fn start(
        output: &'static str,
        url: &str,
        headers: &[(String, String)],
        insecure_skip_tls_verify: bool,
        interval: Duration,
        prelude: Option<&str>,
    ) -> io::Result<Sender> {
        let client = k9_http::Client::new(k9_http::ClientConfig {
            insecure_skip_tls_verify,
            user_agent: Some(format!("k9/{}", env!("CARGO_PKG_VERSION"))),
            max_redirects: 0,
            ..k9_http::ClientConfig::default()
        })
        .map_err(io::Error::other)?;
        let mut request = k9_http::Request::new(http::Method::POST, url)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e.to_string()))?;
        for (name, value) in headers {
            // Validated with the config.
            if let (Ok(name), Ok(value)) = (
                HeaderName::try_from(name.as_str()),
                HeaderValue::try_from(value.as_str()),
            ) {
                request.headers.insert(name, value);
            }
        }
        request.keep_body = true;
        let prelude = prelude
            .map(|url| {
                let mut first = k9_http::Request::new(http::Method::POST, url)
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e.to_string()))?;
                first.headers = request.headers.clone();
                first.keep_body = true;
                Ok::<_, io::Error>(first)
            })
            .transpose()?;
        let pusher = Pusher {
            output,
            client,
            request,
            prelude,
            url: url.to_owned(),
        };
        let (queue, bodies) = mpsc::sync_channel::<Vec<u8>>(QUEUE);
        let thread = std::thread::Builder::new()
            .name(format!("k9-{output}"))
            .spawn(move || pusher.run(bodies))?;
        Ok(Sender {
            interval,
            last_push: Instant::now(),
            queue: Some(queue),
            thread: Some(thread),
        })
    }

    /// Whether a push is due. Samples arrive once a second: some slack keeps
    /// a 1s interval from slipping to 2s on a late tick.
    pub fn due(&self) -> bool {
        self.last_push.elapsed() + self.interval / 10 >= self.interval
    }

    /// Queues a push (`None`: nothing to send this time). Unless `wait`, a
    /// full queue skips it, and the body comes back.
    pub fn push(&mut self, body: Option<Vec<u8>>, wait: bool) -> Option<Vec<u8>> {
        self.last_push = Instant::now();
        if let (Some(body), Some(queue)) = (body, &self.queue) {
            if wait {
                let _ = queue.send(body);
            } else if let Err(mpsc::TrySendError::Full(body)) = queue.try_send(body) {
                return Some(body);
            }
        }
        None
    }

    /// Waits for queued pushes; an error lists what went wrong.
    pub fn finish(&mut self) -> io::Result<()> {
        drop(self.queue.take());
        let errors = match self.thread.take() {
            Some(thread) => thread.join().unwrap_or_default(),
            None => Vec::new(),
        };
        if errors.is_empty() {
            Ok(())
        } else {
            Err(io::Error::other(errors.join("; ")))
        }
    }
}

struct Pusher {
    output: &'static str,
    client: k9_http::Client,
    request: k9_http::Request,
    prelude: Option<k9_http::Request>,
    url: String,
}

impl Pusher {
    /// Sends bodies until the queue closes; returns the distinct errors.
    fn run(self, bodies: mpsc::Receiver<Vec<u8>>) -> Vec<String> {
        let runtime = match tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
        {
            Ok(runtime) => runtime,
            Err(e) => return vec![format!("{}: {e}", self.output)],
        };
        if let Some(prelude) = &self.prelude {
            let sent = runtime.block_on(self.client.send_with_timeout(prelude, PUSH_TIMEOUT));
            if let Some(error) = failure(sent) {
                eprintln!("WARN {}: posting to {}: {error}", self.output, prelude.url);
            }
        }
        let mut errors: Vec<String> = Vec::new();
        while let Ok(body) = bodies.recv() {
            let mut request = self.request.clone();
            request.body = Bytes::from(body);
            let sent = runtime.block_on(self.client.send_with_timeout(&request, PUSH_TIMEOUT));
            if let Some(error) = failure(sent) {
                let message = format!("pushing to {}: {error}", self.url);
                if !errors.contains(&message) {
                    eprintln!("WARN {}: {message}", self.output);
                    errors.push(message);
                }
            }
        }
        errors
    }
}

/// What went wrong with a push, if anything.
fn failure(sent: Result<k9_http::Response, k9_http::HttpError>) -> Option<String> {
    match sent {
        Ok(res) if res.status.is_success() => None,
        Ok(res) => {
            let body = String::from_utf8_lossy(&res.body);
            let body = body.trim();
            let detail = if body.is_empty() {
                String::new()
            } else {
                format!(": {}", body.chars().take(200).collect::<String>())
            };
            Some(format!("status {}{detail}", res.status.as_u16()))
        }
        Err(e) => Some(e.to_string()),
    }
}

// Protobuf encoding (the wire format of both outputs).

pub(crate) fn varint(out: &mut Vec<u8>, mut value: u64) {
    while value >= 0x80 {
        out.push(value as u8 | 0x80);
        value >>= 7;
    }
    out.push(value as u8);
}

/// A length-delimited field: bytes, a string or an embedded message.
pub(crate) fn bytes_field(out: &mut Vec<u8>, field: u64, bytes: &[u8]) {
    varint(out, field << 3 | 2);
    varint(out, bytes.len() as u64);
    out.extend_from_slice(bytes);
}

/// A `fixed64` or `double` field.
pub(crate) fn fixed64_field(out: &mut Vec<u8>, field: u64, bits: u64) {
    varint(out, field << 3 | 1);
    out.extend_from_slice(&bits.to_le_bytes());
}

pub(crate) fn varint_field(out: &mut Vec<u8>, field: u64, value: u64) {
    varint(out, field << 3);
    varint(out, value);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn vars(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn k9_settings_win_over_k6_ones() {
        let settings = Settings::from_vars(
            "OTEL",
            vars(&[
                ("K6_OTEL_A", "k6"),
                ("K9_OTEL_A", "k9"),
                ("K6_OTEL_B", "only"),
                ("K6_OTELX_C", "no"),
                ("K6_OTEL_HEADERS_X", "1"),
            ]),
        );
        assert_eq!(settings.get("A"), Some(("K9_OTEL_A", "k9")));
        assert_eq!(settings.get("B"), Some(("K6_OTEL_B", "only")));
        assert_eq!(settings.get("C"), None);
        assert_eq!(settings.prefixed("HEADERS_"), [("X".into(), "1".into())]);
    }

    #[test]
    fn durations_and_bools() {
        let settings = Settings::from_vars(
            "X",
            vars(&[("K6_X_D", "1.5s"), ("K6_X_Z", "0s"), ("K6_X_B", "maybe")]),
        );
        assert_eq!(
            settings.duration("D"),
            Ok(Some(Duration::from_millis(1500)))
        );
        assert!(settings.duration("Z").unwrap_err().contains("K6_X_Z"));
        assert_eq!(settings.duration("none"), Ok(None));
        assert!(settings.bool("B").unwrap_err().contains("K6_X_B"));
    }

    #[test]
    fn protobuf_fields() {
        let mut out = Vec::new();
        varint(&mut out, 300);
        assert_eq!(out, [0xac, 0x02]);
        let mut out = Vec::new();
        bytes_field(&mut out, 1, b"hi");
        assert_eq!(out, [0x0a, 2, b'h', b'i']);
        let mut out = Vec::new();
        fixed64_field(&mut out, 2, 1);
        assert_eq!(out, [0x11, 1, 0, 0, 0, 0, 0, 0, 0]);
    }
}
