//! `--out influxdb[=URL]` (InfluxDB v1, like k6's output) and
//! `--out xk6-influxdb[=URL]` (InfluxDB v2, like k6's extension): every sample
//! is a line-protocol point named like k6's, so its InfluxDB dashboards work
//! as they are.
//!
//! A point's measurement is the metric's name, its tags are the sample's
//! (empty ones are left out) and its `value` field is the sample's value
//! (times in milliseconds). Tags listed in `TAGS_AS_FIELDS` become fields,
//! optionally typed (`vu:int`). Points are sent in batches, every second by
//! default.

use std::io;
use std::time::{Duration, UNIX_EPOCH};

use k9_metrics::{Metrics, Sample};

use crate::Output;
use crate::prometheus::basic_auth;
use crate::push::{Sender, Settings, check_headers};

const DEFAULT_ADDR: &str = "http://localhost:8086";
const DEFAULT_DB: &str = "k6";
/// k6's default.
const DEFAULT_TAGS_AS_FIELDS: &str = "vu:int,iter:int,url";
/// Points kept while the endpoint can't keep up; beyond this, they're dropped.
const MAX_PENDING: usize = 64 << 20;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InfluxVersion {
    V1,
    V2,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Precision {
    Ns,
    Us,
    Ms,
    S,
}

impl Precision {
    fn parse(name: &str, value: &str) -> Result<Precision, String> {
        let unit = value.trim().strip_prefix('1').unwrap_or(value.trim());
        match unit {
            "ns" | "n" => Ok(Precision::Ns),
            "us" | "u" | "µs" => Ok(Precision::Us),
            "ms" => Ok(Precision::Ms),
            "s" => Ok(Precision::S),
            _ => Err(format!("{name}: expected ns, us, ms or s, got {value:?}")),
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Precision::Ns => "ns",
            Precision::Us => "us",
            Precision::Ms => "ms",
            Precision::S => "s",
        }
    }

    fn nanos(self) -> u128 {
        match self {
            Precision::Ns => 1,
            Precision::Us => 1_000,
            Precision::Ms => 1_000_000,
            Precision::S => 1_000_000_000,
        }
    }
}

/// The type a tag sent as a field gets.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FieldType {
    String,
    Int,
    Float,
    Bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct InfluxConfig {
    pub version: InfluxVersion,
    /// Where points go, with the query string.
    pub write_url: String,
    /// Shown in the run header (no credentials).
    target: String,
    /// v1: the `CREATE DATABASE` query sent first.
    create_url: Option<String>,
    pub push_interval: Duration,
    pub insecure_skip_tls_verify: bool,
    pub headers: Vec<(String, String)>,
    tags_as_fields: Vec<(String, FieldType)>,
    precision: Precision,
}

impl InfluxConfig {
    /// k6's settings: `K6_INFLUXDB_*` variables, each of which can also be
    /// given as `K9_INFLUXDB_*` (which wins). `url` (from
    /// `--out influxdb=URL`) beats both; for v1 its path names the database
    /// and it can carry `user:password@`.
    pub fn from_env(
        version: InfluxVersion,
        url: Option<&str>,
        vars: impl IntoIterator<Item = (String, String)>,
    ) -> Result<InfluxConfig, String> {
        let settings = Settings::from_vars("INFLUXDB", vars);
        let (address, from) = match url.filter(|u| !u.is_empty()) {
            Some(url) => (url.to_owned(), "--out"),
            None => match settings.get("ADDR") {
                Some((name, value)) => (value.to_owned(), name),
                None => (DEFAULT_ADDR.to_owned(), "the default address"),
            },
        };
        let address =
            Address::parse(&address).map_err(|e| format!("{from}: {e}, got {address:?}"))?;
        let precision = match settings.get("PRECISION") {
            Some((name, value)) => Precision::parse(name, value)?,
            None => Precision::Ns,
        };
        let tags_as_fields = match settings.get("TAGS_AS_FIELDS") {
            Some((name, value)) => tags_as_fields(name, value)?,
            None => tags_as_fields("", DEFAULT_TAGS_AS_FIELDS)?,
        };
        let mut headers = vec![(
            "Content-Type".to_owned(),
            "text/plain; charset=utf-8".to_owned(),
        )];
        let (write_url, target, create_url) = match version {
            InfluxVersion::V1 => {
                let db = match (&address.path, settings.get("DB")) {
                    (path, _) if !path.is_empty() => path.clone(),
                    (_, Some((_, db))) if !db.is_empty() => db.to_owned(),
                    _ => DEFAULT_DB.to_owned(),
                };
                let user = settings.get("USERNAME").map(|(_, v)| v.to_owned());
                let password = settings.get("PASSWORD").map(|(_, v)| v.to_owned());
                let credentials = match (user, address.user.clone()) {
                    (Some(user), _) => Some((user, password.unwrap_or_default())),
                    (None, Some((user, from_url))) => Some((user, password.unwrap_or(from_url))),
                    (None, None) => None,
                };
                if let Some((user, password)) = credentials {
                    headers.push(basic_auth(&user, &password));
                }
                let mut query = format!("db={}&precision={}", encode(&db), precision.as_str());
                for (key, param) in [("RETENTION", "rp"), ("CONSISTENCY", "consistency")] {
                    if let Some((_, value)) = settings.get(key).filter(|(_, v)| !v.is_empty()) {
                        query.push_str(&format!("&{param}={}", encode(value)));
                    }
                }
                let base = &address.base;
                (
                    format!("{base}/write?{query}"),
                    format!("{base}/{db}"),
                    Some(format!(
                        "{base}/query?q={}",
                        encode(&format!("CREATE DATABASE \"{db}\""))
                    )),
                )
            }
            InfluxVersion::V2 => {
                let base = format!("{}{}", address.base, address.path_prefix());
                let bucket = settings
                    .get("BUCKET")
                    .map(|(_, v)| v.to_owned())
                    .filter(|b| !b.is_empty())
                    .ok_or("xk6-influxdb needs a bucket: set K6_INFLUXDB_BUCKET")?;
                let mut query = String::new();
                if let Some((_, org)) = settings.get("ORGANIZATION").filter(|(_, v)| !v.is_empty())
                {
                    query.push_str(&format!("org={}&", encode(org)));
                }
                query.push_str(&format!(
                    "bucket={}&precision={}",
                    encode(&bucket),
                    precision.as_str()
                ));
                if let Some((_, token)) = settings.get("TOKEN").filter(|(_, v)| !v.is_empty()) {
                    headers.push(("Authorization".into(), format!("Token {token}")));
                }
                (
                    format!("{base}/api/v2/write?{query}"),
                    format!("{base}, bucket {bucket}"),
                    None,
                )
            }
        };
        check_headers("influxdb", &headers)?;
        Ok(InfluxConfig {
            version,
            write_url,
            target,
            create_url,
            push_interval: settings
                .duration("PUSH_INTERVAL")?
                .unwrap_or(Duration::from_secs(1)),
            insecure_skip_tls_verify: settings.bool("INSECURE")?.unwrap_or(false),
            headers,
            tags_as_fields,
            precision,
        })
    }
}

/// `scheme://[user[:password]@]host[:port][/path]`.
struct Address {
    /// `scheme://host[:port]`.
    base: String,
    user: Option<(String, String)>,
    /// Without its slashes.
    path: String,
}

impl Address {
    fn parse(text: &str) -> Result<Address, String> {
        let text = text.trim();
        let (scheme, rest) = text
            .split_once("://")
            .filter(|(s, _)| *s == "http" || *s == "https")
            .ok_or("expected an http:// or https:// URL")?;
        let (authority, path) = rest.split_once('/').unwrap_or((rest, ""));
        let (user, host) = match authority.rsplit_once('@') {
            Some((user, host)) => {
                let (name, password) = user.split_once(':').unwrap_or((user, ""));
                (Some((name.to_owned(), password.to_owned())), host)
            }
            None => (None, authority),
        };
        if host.is_empty() {
            return Err("expected a host".into());
        }
        Ok(Address {
            base: format!("{scheme}://{host}"),
            user,
            path: path.trim_matches('/').to_owned(),
        })
    }

    fn path_prefix(&self) -> String {
        if self.path.is_empty() {
            String::new()
        } else {
            format!("/{}", self.path)
        }
    }
}

/// `vu:int,iter:int,url` → (tag, type) pairs.
fn tags_as_fields(name: &str, value: &str) -> Result<Vec<(String, FieldType)>, String> {
    value
        .split(',')
        .map(str::trim)
        .filter(|t| !t.is_empty())
        .map(|entry| {
            let (tag, kind) = entry.split_once(':').unwrap_or((entry, "string"));
            let kind = match kind.trim() {
                "string" => FieldType::String,
                "int" => FieldType::Int,
                "float" => FieldType::Float,
                "bool" => FieldType::Bool,
                other => {
                    return Err(format!(
                        "{name}: {tag:?} has type {other:?}; expected string, int, float or bool"
                    ));
                }
            };
            Ok((tag.trim().to_owned(), kind))
        })
        .collect()
}

/// Percent-encodes a query-string value.
fn encode(text: &str) -> String {
    let mut out = String::new();
    for byte in text.bytes() {
        if byte.is_ascii_alphanumeric() || b"-_.~".contains(&byte) {
            out.push(byte as char);
        } else {
            out.push_str(&format!("%{byte:02X}"));
        }
    }
    out
}

/// Escapes a measurement (`specials` = `, `) or a tag key, tag value or
/// field key (`specials` = `,= `).
fn escape(out: &mut String, text: &str, specials: &str) {
    for c in text.chars() {
        match c {
            '\n' | '\r' => out.push(' '),
            c if specials.contains(c) || c == '\\' => {
                out.push('\\');
                out.push(c);
            }
            c => out.push(c),
        }
    }
}

/// A tag's value as a typed field value, or `None` if it isn't one.
fn field_value(kind: FieldType, value: &str) -> Option<String> {
    Some(match kind {
        FieldType::String => {
            let mut out = String::from('"');
            for c in value.chars() {
                if c == '"' || c == '\\' {
                    out.push('\\');
                }
                out.push(c);
            }
            out.push('"');
            out
        }
        FieldType::Int => format!("{}i", value.trim().parse::<i64>().ok()?),
        FieldType::Float => {
            let v = value.trim().parse::<f64>().ok().filter(|v| v.is_finite())?;
            format!("{v}")
        }
        FieldType::Bool => match value.trim().to_ascii_lowercase().as_str() {
            "true" | "t" | "1" => "true".into(),
            "false" | "f" | "0" => "false".into(),
            _ => return None,
        },
    })
}

pub struct InfluxOutput {
    config: InfluxConfig,
    /// Line protocol not sent yet.
    pending: String,
    dropped: bool,
    sender: Sender,
}

impl InfluxOutput {
    pub fn start(config: InfluxConfig) -> io::Result<InfluxOutput> {
        let name = match config.version {
            InfluxVersion::V1 => "influxdb",
            InfluxVersion::V2 => "xk6-influxdb",
        };
        let sender = Sender::start(
            name,
            &config.write_url,
            &config.headers,
            config.insecure_skip_tls_verify,
            config.push_interval,
            config.create_url.as_deref(),
        )?;
        Ok(InfluxOutput {
            config,
            pending: String::new(),
            dropped: false,
            sender,
        })
    }

    /// Appends a sample's point to `pending`.
    fn write_point(&mut self, sample: &Sample, metrics: &Metrics) {
        if !sample.value.is_finite() {
            return;
        }
        let out = &mut self.pending;
        let def = metrics.registry().def(sample.metric);
        let tags = metrics.interner().get(sample.tags);
        escape(out, &def.name, ", ");
        let as_field = |key: &str| {
            self.config
                .tags_as_fields
                .iter()
                .find(|(tag, _)| tag == key)
                .map(|(_, kind)| *kind)
        };
        for (key, value) in tags.iter() {
            if !value.is_empty() && as_field(key).is_none() {
                out.push(',');
                escape(out, key, ",= ");
                out.push('=');
                escape(out, value, ",= ");
            }
        }
        out.push_str(&format!(" value={}", sample.value));
        for (key, value) in tags.iter() {
            if let Some(field) = as_field(key).and_then(|kind| field_value(kind, value)) {
                out.push(',');
                escape(out, key, ",= ");
                out.push('=');
                out.push_str(&field);
            }
        }
        let nanos = sample
            .time
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        out.push_str(&format!(" {}\n", nanos / self.config.precision.nanos()));
    }

    fn push(&mut self, wait: bool) {
        if self.pending.is_empty() {
            return;
        }
        let body = std::mem::take(&mut self.pending).into_bytes();
        if let Some(body) = self.sender.push(Some(body), wait) {
            if body.len() > MAX_PENDING {
                if !self.dropped {
                    eprintln!(
                        "WARN {}: the endpoint can't keep up; dropping points",
                        self.description()
                    );
                    self.dropped = true;
                }
            } else {
                // Line protocol only: still UTF-8.
                self.pending = String::from_utf8(body).unwrap_or_default();
            }
        }
    }
}

impl Output for InfluxOutput {
    fn description(&self) -> String {
        match self.config.version {
            InfluxVersion::V1 => format!("influxdb ({})", self.config.target),
            InfluxVersion::V2 => format!("xk6-influxdb ({})", self.config.target),
        }
    }

    fn add_samples(&mut self, samples: &[Sample], metrics: &Metrics) -> io::Result<()> {
        for sample in samples {
            self.write_point(sample, metrics);
        }
        if self.sender.due() {
            self.push(false);
        }
        Ok(())
    }

    fn stop(&mut self, _metrics: &Metrics) -> io::Result<()> {
        self.push(true);
        self.sender.finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn env(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn v1_defaults_match_k6() {
        let config = InfluxConfig::from_env(InfluxVersion::V1, None, env(&[])).unwrap();
        assert_eq!(
            config.write_url,
            "http://localhost:8086/write?db=k6&precision=ns"
        );
        assert_eq!(
            config.create_url.as_deref(),
            Some("http://localhost:8086/query?q=CREATE%20DATABASE%20%22k6%22")
        );
        assert_eq!(config.push_interval, Duration::from_secs(1));
        assert_eq!(
            config.tags_as_fields,
            [
                ("vu".to_owned(), FieldType::Int),
                ("iter".to_owned(), FieldType::Int),
                ("url".to_owned(), FieldType::String)
            ]
        );
    }

    #[test]
    fn v1_urls_carry_the_database_and_credentials() {
        let config = InfluxConfig::from_env(
            InfluxVersion::V1,
            Some("https://me:secret@influx:8086/load/"),
            env(&[("K6_INFLUXDB_RETENTION", "a week")]),
        )
        .unwrap();
        assert_eq!(
            config.write_url,
            "https://influx:8086/write?db=load&precision=ns&rp=a%20week"
        );
        assert!(config.headers.contains(&basic_auth("me", "secret")));
        assert_eq!(config.target, "https://influx:8086/load", "no credentials");
    }

    #[test]
    fn escaping_follows_the_line_protocol() {
        let mut out = String::new();
        escape(&mut out, "a b,c=d\\e\nf", ",= ");
        assert_eq!(out, "a\\ b\\,c\\=d\\\\e f");
        let mut out = String::new();
        escape(&mut out, "m=1 x", ", ");
        assert_eq!(out, "m=1\\ x");
        assert_eq!(
            field_value(FieldType::String, "say \"hi\"\\").as_deref(),
            Some("\"say \\\"hi\\\"\\\\\"")
        );
        assert_eq!(field_value(FieldType::Int, "12").as_deref(), Some("12i"));
        assert_eq!(field_value(FieldType::Int, "x"), None);
        assert_eq!(field_value(FieldType::Float, "1.5").as_deref(), Some("1.5"));
        assert_eq!(
            field_value(FieldType::Bool, "True").as_deref(),
            Some("true")
        );
    }

    #[test]
    fn precisions() {
        assert_eq!(Precision::parse("P", "1ms"), Ok(Precision::Ms));
        assert_eq!(Precision::parse("P", "u"), Ok(Precision::Us));
        assert!(Precision::parse("P", "1d").unwrap_err().contains('P'));
    }
}
