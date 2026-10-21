//! The plan model: the single source of truth that both the Python API and the
//! CLI produce, and the only thing the engine consumes.
//!
//! There are two layers:
//! - [`PlanConfig`] mirrors what users write (k6 option names, shortcuts, optional
//!   fields) and rejects unknown keys.
//! - [`Plan`] is the validated, fully resolved form: shortcuts are expanded into a
//!   `default` scenario, defaults are filled in, and every executor carries exactly
//!   the fields it uses.
//!
//! Every count in an [`Executor`] is an integer (VUs, iterations, rate) so that a
//! plan can later be split into execution segments across workers.

use std::fmt;

use indexmap::IndexMap;
use serde::{Deserialize, Serialize};

use k9_metrics::threshold::{Expr as ThresholdExpr, Selector};

use crate::duration::Duration;

pub const DEFAULT_FLOW: &str = "default";
pub const DEFAULT_SCENARIO: &str = "default";

const DEFAULT_GRACEFUL_STOP: Duration = Duration::from_secs(30);
const DEFAULT_GRACEFUL_RAMP_DOWN: Duration = Duration::from_secs(30);
const DEFAULT_MAX_DURATION: Duration = Duration::from_secs(600);
const DEFAULT_TIME_UNIT: Duration = Duration::from_secs(1);
/// Largest sum of a scenario's transaction weights.
const MAX_TRANSACTION_ROUND: u64 = 10_000;
const DEFAULT_HOOK_TIMEOUT: Duration = Duration::from_secs(60);

// ---------------------------------------------------------------------------
// Resolved plan
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Plan {
    pub options: Options,
    pub flows: IndexMap<String, Vec<Step>>,
    /// Test data for flows' templates, by name.
    #[serde(default, skip_serializing_if = "IndexMap::is_empty")]
    pub feeders: IndexMap<String, Feeder>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Options {
    pub scenarios: IndexMap<String, Scenario>,
    pub thresholds: IndexMap<String, Vec<Threshold>>,
    pub tags: IndexMap<String, String>,
    /// Longest a script's `setup()` may run (k6 default: 60s).
    pub setup_timeout: Duration,
    /// Longest a script's `teardown()` may run (k6 default: 60s).
    pub teardown_timeout: Duration,
    /// The values the summary shows for trends, e.g. `p(99)` (k6's names).
    #[serde(default = "default_trend_stats")]
    pub summary_trend_stats: Vec<String>,
    /// One unit for every time in the summary: `s`, `ms` or `us`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub summary_time_unit: Option<String>,
    /// Iterations shorter than this are padded with a sleep.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min_iteration_duration: Option<Duration>,
    /// Script requests that get no response raise instead of returning.
    #[serde(default)]
    pub throw: bool,
    /// The system tags samples carry (k6's names).
    #[serde(default = "default_system_tags")]
    pub system_tags: Vec<String>,
    /// VUs keep their cookies from one iteration to the next.
    #[serde(default)]
    pub no_cookies_reset: bool,
    #[serde(flatten)]
    pub http: HttpOptions,
}

/// k6's system tags on by default.
pub const DEFAULT_SYSTEM_TAGS: [&str; 15] = [
    "proto",
    "subproto",
    "status",
    "method",
    "url",
    "name",
    "group",
    "check",
    "error",
    "error_code",
    "tls_version",
    "scenario",
    "service",
    "rpc_type",
    "expected_response",
];

/// k6's system tags k9 doesn't add yet.
const UNSUPPORTED_SYSTEM_TAGS: [&str; 4] = ["vu", "iter", "ip", "ocsp_status"];

fn default_system_tags() -> Vec<String> {
    DEFAULT_SYSTEM_TAGS.iter().map(|t| t.to_string()).collect()
}

fn default_trend_stats() -> Vec<String> {
    k9_output::TrendStat::DEFAULT
        .iter()
        .map(ToString::to_string)
        .collect()
}

/// k6's HTTP client options.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HttpOptions {
    /// Redirects to follow per request (k6 default: 10).
    pub max_redirects: u32,
    /// Accept any server certificate.
    #[serde(rename = "insecureSkipTLSVerify")]
    pub insecure_skip_tls_verify: bool,
    /// `User-Agent` header; empty means none is sent.
    pub user_agent: String,
    /// Read response bodies but don't keep them.
    pub discard_response_bodies: bool,
    /// The HTTP versions VUs may speak.
    pub http_version: HttpVersion,
    /// Requests of a `batch` step in flight at once (k6 default: 20).
    pub batch: u32,
    /// The same, per host (k6 default: 6; 0: no limit).
    pub batch_per_host: u32,
    /// Client certificates by domain.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tls_auth: Vec<TlsAuth>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tls_version: Option<TlsVersionOption>,
    /// IANA cipher suite names to offer (default: rustls's).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tls_cipher_suites: Vec<String>,
    /// Names sent to other addresses: `name`, `name:port` or `*.domain`, to
    /// `ip` or `ip:port`.
    #[serde(default, skip_serializing_if = "IndexMap::is_empty")]
    pub hosts: IndexMap<String, String>,
    #[serde(default)]
    pub dns: DnsOptions,
    /// Hostnames (or `*.domain`) requests may not go to.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub block_hostnames: Vec<String>,
    /// IPs, CIDR blocks or `first-last` spans requests may not go to.
    #[serde(
        default,
        rename = "blacklistIPs",
        skip_serializing_if = "Vec::is_empty"
    )]
    pub blacklist_ips: Vec<String>,
    /// Addresses to send from, handed out to VUs in turn (same forms).
    #[serde(default, rename = "localIPs", skip_serializing_if = "Vec::is_empty")]
    pub local_ips: Vec<String>,
    /// A new connection for every request.
    #[serde(default)]
    pub no_connection_reuse: bool,
    /// A VU's connections are closed between its iterations.
    #[serde(default, rename = "noVUConnectionReuse")]
    pub no_vu_connection_reuse: bool,
    /// Dump requests and responses to stderr: `headers` or `full` (bodies too).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub http_debug: Option<String>,
}

/// The network options, parsed for the HTTP client.
#[derive(Debug, Clone, Default)]
pub struct Network {
    pub hosts: Vec<k9_http::HostOverride>,
    pub dns: k9_http::DnsConfig,
    pub block_hostnames: Vec<k9_http::HostPattern>,
    pub blacklist_ips: Vec<k9_http::IpRange>,
    pub local_ips: k9_http::IpPool,
}

impl HttpOptions {
    /// Parses the network options, or lists what's wrong with them.
    pub fn network(&self) -> Result<Network, Vec<Issue>> {
        let mut issues = Issues::default();
        let hosts = self
            .hosts
            .iter()
            .filter_map(|(from, to)| {
                k9_http::HostOverride::new(from, to)
                    .map_err(|e| issues.push(format!("options.hosts.{from}"), e))
                    .ok()
            })
            .collect();
        let dns = self
            .dns
            .config()
            .map_err(|e| issues.push("options.dns", e))
            .unwrap_or_default();
        let block_hostnames = parse_each(&self.block_hostnames, "blockHostnames", &mut issues);
        let blacklist_ips = parse_each(&self.blacklist_ips, "blacklistIPs", &mut issues);
        let local_ips = k9_http::IpPool::new(parse_each(&self.local_ips, "localIPs", &mut issues));
        if issues.0.is_empty() {
            Ok(Network {
                hosts,
                dns,
                block_hostnames,
                blacklist_ips,
                local_ips,
            })
        } else {
            Err(issues.0)
        }
    }
}

fn parse_each<T: std::str::FromStr<Err = String>>(
    items: &[String],
    option: &str,
    issues: &mut Issues,
) -> Vec<T> {
    items
        .iter()
        .enumerate()
        .filter_map(|(i, item)| {
            item.parse()
                .map_err(|e| issues.push(format!("options.{option}[{i}]"), e))
                .ok()
        })
        .collect()
}

/// k6's `dns` option, each part as k6 spells it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DnsOptions {
    /// How long lookups are cached: a duration, `0` (not at all) or `inf`.
    pub ttl: String,
    /// `first`, `random` or `roundRobin`.
    pub select: String,
    /// `preferIPv4`, `preferIPv6`, `onlyIPv4`, `onlyIPv6` or `any`.
    pub policy: String,
}

impl Default for DnsOptions {
    fn default() -> Self {
        DnsOptions {
            ttl: "5m".to_owned(),
            select: "random".to_owned(),
            policy: "preferIPv4".to_owned(),
        }
    }
}

impl DnsOptions {
    pub fn config(&self) -> Result<k9_http::DnsConfig, String> {
        let ttl = match self.ttl.trim() {
            "inf" => None,
            ttl => Some(ttl.parse::<Duration>().map_err(|e| format!("ttl: {e}"))?.0),
        };
        Ok(k9_http::DnsConfig {
            ttl,
            select: self.select.parse()?,
            policy: self.policy.parse()?,
        })
    }

    /// Sets the parts in k6's text form, `ttl=1m,select=first,policy=any`.
    pub fn apply_text(&mut self, text: &str) -> Result<(), String> {
        for part in text.split(',').map(str::trim).filter(|p| !p.is_empty()) {
            let (key, value) = part
                .split_once('=')
                .ok_or_else(|| format!("`{part}`: expected key=value"))?;
            let value = value.trim().to_owned();
            match key.trim() {
                "ttl" => self.ttl = value,
                "select" => self.select = value,
                "policy" => self.policy = value,
                other => {
                    return Err(format!(
                        "unknown part `{other}` (expected ttl, select or policy)"
                    ));
                }
            }
        }
        Ok(())
    }
}

/// `dns` as written: k6's text form or an object, parts left out keeping
/// their defaults.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(untagged)]
pub enum DnsOption {
    Text(String),
    Fields {
        #[serde(default)]
        ttl: Option<String>,
        #[serde(default)]
        select: Option<String>,
        #[serde(default)]
        policy: Option<String>,
    },
}

/// A list, or k6's comma-separated text.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(untagged)]
pub enum ListOption {
    Text(String),
    List(Vec<String>),
}

impl ListOption {
    pub fn into_vec(self) -> Vec<String> {
        match self {
            ListOption::Text(text) => text
                .split(',')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_owned)
                .collect(),
            ListOption::List(list) => list,
        }
    }
}

/// One `tlsAuth` entry: a client certificate for `domains` (`example.com`,
/// `*.example.com`; every host when empty). `cert` and `key` are PEM text or
/// files (relative to the plan).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TlsAuth {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub domains: Vec<String>,
    pub cert: String,
    pub key: String,
    /// k6 accepts encrypted keys; k9 doesn't (rejected when the plan resolves).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub password: Option<String>,
}

impl TlsAuth {
    /// Whether `value` (a `cert` or `key`) is PEM text rather than a file.
    pub fn is_pem(value: &str) -> bool {
        value.contains("-----BEGIN")
    }
}

/// `tlsVersion`: one version (`tls1.2`), or a range (`{min, max}`, either
/// end optional).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum TlsVersionOption {
    One(String),
    Range {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        min: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        max: Option<String>,
    },
}

impl TlsVersionOption {
    /// The lowest and highest versions, or why they're invalid.
    pub fn range(&self) -> Result<(k9_http::TlsVersion, k9_http::TlsVersion), String> {
        use k9_http::TlsVersion;
        let parse = |v: &Option<String>, default| match v {
            Some(v) => v.parse::<TlsVersion>(),
            None => Ok(default),
        };
        let (min, max) = match self {
            TlsVersionOption::One(v) => {
                let v = v.parse::<TlsVersion>()?;
                (v, v)
            }
            TlsVersionOption::Range { min, max } => (
                parse(min, TlsVersion::Tls12)?,
                parse(max, TlsVersion::Tls13)?,
            ),
        };
        if min > max {
            return Err(format!("min {} is above max {}", min.name(), max.name()));
        }
        Ok((min, max))
    }
}

/// `httpVersion`: `auto` (HTTP/2 when an HTTPS server accepts it, otherwise
/// HTTP/1.1), `1.1`, `2` (HTTP/2 only; prior knowledge over plain HTTP) or `3`
/// (HTTP/3 over QUIC, HTTPS only; experimental).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "VersionText", into = "String")]
pub enum HttpVersion {
    #[default]
    Auto,
    Http1,
    Http2,
    Http3,
}

impl std::str::FromStr for HttpVersion {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, String> {
        match s {
            "auto" => Ok(HttpVersion::Auto),
            "1.1" => Ok(HttpVersion::Http1),
            "2" | "2.0" => Ok(HttpVersion::Http2),
            "3" | "3.0" => Ok(HttpVersion::Http3),
            other => Err(format!(
                "unknown HTTP version {other:?} (expected auto, 1.1, 2 or 3)"
            )),
        }
    }
}

impl From<HttpVersion> for String {
    fn from(version: HttpVersion) -> String {
        match version {
            HttpVersion::Auto => "auto",
            HttpVersion::Http1 => "1.1",
            HttpVersion::Http2 => "2",
            HttpVersion::Http3 => "3",
        }
        .to_owned()
    }
}

/// `httpVersion` as written: `2` and `1.1` may be numbers in YAML or JSON.
#[derive(Deserialize)]
#[serde(untagged)]
enum VersionText {
    Text(String),
    Number(f64),
}

impl TryFrom<VersionText> for HttpVersion {
    type Error = String;

    fn try_from(text: VersionText) -> Result<Self, String> {
        match text {
            VersionText::Text(s) => s.parse(),
            VersionText::Number(n) => n.to_string().parse(),
        }
    }
}

impl Default for HttpOptions {
    fn default() -> Self {
        HttpOptions {
            max_redirects: 10,
            insecure_skip_tls_verify: false,
            user_agent: format!("k9/{}", crate::VERSION),
            discard_response_bodies: false,
            http_version: HttpVersion::Auto,
            batch: 20,
            batch_per_host: 6,
            tls_auth: Vec::new(),
            tls_version: None,
            tls_cipher_suites: Vec::new(),
            hosts: IndexMap::new(),
            dns: DnsOptions::default(),
            block_hostnames: Vec::new(),
            blacklist_ips: Vec::new(),
            local_ips: Vec::new(),
            no_connection_reuse: false,
            no_vu_connection_reuse: false,
            http_debug: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Scenario {
    #[serde(flatten)]
    pub executor: Executor,
    /// Name of the flow (or, for Python scripts, the function) each iteration
    /// runs; with `transactions`, the first of them.
    pub exec: String,
    /// Goose's weighted transactions: flow (or function) → weight. Each
    /// iteration runs one of them; every VU runs them in the exact ratio of
    /// their weights, in a shuffled order.
    #[serde(default, skip_serializing_if = "IndexMap::is_empty")]
    pub transactions: IndexMap<String, u32>,
    /// Runs once per VU, before its first iteration (not an iteration itself).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub on_start: Option<String>,
    /// Runs once per VU when it stops gracefully (not when interrupted).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub on_stop: Option<String>,
    pub start_time: Duration,
    pub graceful_stop: Duration,
    pub tags: IndexMap<String, String>,
    pub env: IndexMap<String, String>,
}

impl Plan {
    /// Longest possible run: the latest-ending scenario, including graceful stops.
    pub fn max_duration(&self) -> Duration {
        self.options
            .scenarios
            .values()
            .map(Scenario::max_duration)
            .max()
            .unwrap_or(Duration::ZERO)
    }
}

impl Scenario {
    /// Every flow (or function) the scenario runs, with the field naming it:
    /// `exec` (or each of `transactions`), `onStart` and `onStop`.
    pub fn entry_points(&self) -> Vec<(String, &str)> {
        let mut entries: Vec<(String, &str)> = if self.transactions.is_empty() {
            vec![("exec".to_owned(), self.exec.as_str())]
        } else {
            self.transactions
                .keys()
                .map(|t| (format!("transactions.{t}"), t.as_str()))
                .collect()
        };
        entries.extend(self.on_start.as_deref().map(|f| ("onStart".to_owned(), f)));
        entries.extend(self.on_stop.as_deref().map(|f| ("onStop".to_owned(), f)));
        entries
    }

    /// k6-style one-line description, e.g. `10 looping VUs for 30s (gracefulStop: 30s)`.
    pub fn describe(&self) -> String {
        let mut details = Vec::new();
        let what = match &self.executor {
            Executor::ConstantVus { vus, duration } => format!("{vus} looping VUs for {duration}"),
            Executor::RampingVus {
                stages,
                graceful_ramp_down,
                ..
            } => {
                details.push(format!("gracefulRampDown: {graceful_ramp_down}"));
                format!(
                    "Up to {} looping VUs for {} over {} stages",
                    self.executor.max_vus(),
                    stages.iter().map(|s| s.duration).sum::<Duration>(),
                    stages.len()
                )
            }
            Executor::PerVuIterations {
                vus,
                iterations,
                max_duration,
            } => {
                details.push(format!("maxDuration: {max_duration}"));
                format!("{iterations} iterations for each of {vus} VUs")
            }
            Executor::SharedIterations {
                vus,
                iterations,
                max_duration,
            } => {
                details.push(format!("maxDuration: {max_duration}"));
                format!("{iterations} iterations shared among {vus} VUs")
            }
            Executor::ConstantArrivalRate {
                rate,
                time_unit,
                duration,
                pre_allocated_vus,
                max_vus,
            } => {
                details.push(format!("maxVUs: {pre_allocated_vus}-{max_vus}"));
                format!(
                    "{:.2} iterations/s for {duration}",
                    per_second(*rate, *time_unit)
                )
            }
            Executor::RampingArrivalRate {
                start_rate,
                time_unit,
                stages,
                pre_allocated_vus,
                max_vus,
            } => {
                details.push(format!("maxVUs: {pre_allocated_vus}-{max_vus}"));
                let peak = stages.iter().map(|s| s.target).fold(*start_rate, u64::max);
                format!(
                    "Up to {:.2} iterations/s for {} over {} stages",
                    per_second(peak, *time_unit),
                    stages.iter().map(|s| s.duration).sum::<Duration>(),
                    stages.len()
                )
            }
            Executor::ExternallyControlled {
                max_vus, duration, ..
            } => format!("Up to {max_vus} VUs for {duration}, externally controlled"),
        };
        if !self.transactions.is_empty() {
            let mix: Vec<String> = self
                .transactions
                .iter()
                .map(|(t, w)| format!("{t}×{w}"))
                .collect();
            details.push(format!("transactions: {}", mix.join(" ")));
        } else if self.exec != DEFAULT_FLOW {
            details.push(format!("exec: {}", self.exec));
        }
        if !self.start_time.is_zero() {
            details.push(format!("startTime: {}", self.start_time));
        }
        details.push(format!("gracefulStop: {}", self.graceful_stop));
        format!("{what} ({})", details.join(", "))
    }

    /// Longest this scenario can take from the start of the run: start time,
    /// its executor's run time, and its graceful stop (and ramp-down).
    pub fn max_duration(&self) -> Duration {
        let run = match &self.executor {
            Executor::ConstantVus { duration, .. }
            | Executor::ConstantArrivalRate { duration, .. }
            | Executor::ExternallyControlled { duration, .. } => *duration,
            Executor::PerVuIterations { max_duration, .. }
            | Executor::SharedIterations { max_duration, .. } => *max_duration,
            Executor::RampingVus {
                stages,
                graceful_ramp_down,
                ..
            } => stages.iter().map(|s| s.duration).sum::<Duration>() + *graceful_ramp_down,
            Executor::RampingArrivalRate { stages, .. } => stages.iter().map(|s| s.duration).sum(),
        };
        self.start_time + run + self.graceful_stop
    }
}

fn per_second(rate: u64, time_unit: Duration) -> f64 {
    rate as f64 / time_unit.as_std().as_secs_f64()
}

/// The seven k6 executors. An executor only decides *when* iterations run and
/// *how many* VUs exist; what an iteration does is the scenario's flow.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "executor", rename_all = "kebab-case")]
pub enum Executor {
    #[serde(rename_all = "camelCase")]
    ConstantVus { vus: u32, duration: Duration },
    #[serde(rename_all = "camelCase")]
    RampingVus {
        #[serde(rename = "startVUs")]
        start_vus: u32,
        stages: Vec<Stage>,
        graceful_ramp_down: Duration,
    },
    #[serde(rename_all = "camelCase")]
    PerVuIterations {
        vus: u32,
        iterations: u64,
        max_duration: Duration,
    },
    #[serde(rename_all = "camelCase")]
    SharedIterations {
        vus: u32,
        iterations: u64,
        max_duration: Duration,
    },
    #[serde(rename_all = "camelCase")]
    ConstantArrivalRate {
        rate: u64,
        time_unit: Duration,
        duration: Duration,
        #[serde(rename = "preAllocatedVUs")]
        pre_allocated_vus: u32,
        #[serde(rename = "maxVUs")]
        max_vus: u32,
    },
    #[serde(rename_all = "camelCase")]
    RampingArrivalRate {
        start_rate: u64,
        time_unit: Duration,
        stages: Vec<Stage>,
        #[serde(rename = "preAllocatedVUs")]
        pre_allocated_vus: u32,
        #[serde(rename = "maxVUs")]
        max_vus: u32,
    },
    #[serde(rename_all = "camelCase")]
    ExternallyControlled {
        vus: u32,
        #[serde(rename = "maxVUs")]
        max_vus: u32,
        duration: Duration,
    },
}

impl Executor {
    /// The most VUs this executor can use at once.
    pub fn max_vus(&self) -> u32 {
        match self {
            Executor::ConstantVus { vus, .. }
            | Executor::PerVuIterations { vus, .. }
            | Executor::SharedIterations { vus, .. } => *vus,
            Executor::RampingVus {
                start_vus, stages, ..
            } => stages
                .iter()
                .map(|s| u32::try_from(s.target).unwrap_or(u32::MAX))
                .fold(*start_vus, u32::max),
            Executor::ConstantArrivalRate { max_vus, .. }
            | Executor::RampingArrivalRate { max_vus, .. }
            | Executor::ExternallyControlled { max_vus, .. } => *max_vus,
        }
    }

    pub fn kind(&self) -> ExecutorKind {
        match self {
            Executor::ConstantVus { .. } => ExecutorKind::ConstantVus,
            Executor::RampingVus { .. } => ExecutorKind::RampingVus,
            Executor::PerVuIterations { .. } => ExecutorKind::PerVuIterations,
            Executor::SharedIterations { .. } => ExecutorKind::SharedIterations,
            Executor::ConstantArrivalRate { .. } => ExecutorKind::ConstantArrivalRate,
            Executor::RampingArrivalRate { .. } => ExecutorKind::RampingArrivalRate,
            Executor::ExternallyControlled { .. } => ExecutorKind::ExternallyControlled,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutorKind {
    ConstantVus,
    RampingVus,
    PerVuIterations,
    SharedIterations,
    ConstantArrivalRate,
    RampingArrivalRate,
    ExternallyControlled,
}

impl ExecutorKind {
    pub const ALL: [ExecutorKind; 7] = [
        ExecutorKind::ConstantVus,
        ExecutorKind::RampingVus,
        ExecutorKind::PerVuIterations,
        ExecutorKind::SharedIterations,
        ExecutorKind::ConstantArrivalRate,
        ExecutorKind::RampingArrivalRate,
        ExecutorKind::ExternallyControlled,
    ];

    pub fn name(self) -> &'static str {
        match self {
            ExecutorKind::ConstantVus => "constant-vus",
            ExecutorKind::RampingVus => "ramping-vus",
            ExecutorKind::PerVuIterations => "per-vu-iterations",
            ExecutorKind::SharedIterations => "shared-iterations",
            ExecutorKind::ConstantArrivalRate => "constant-arrival-rate",
            ExecutorKind::RampingArrivalRate => "ramping-arrival-rate",
            ExecutorKind::ExternallyControlled => "externally-controlled",
        }
    }

    fn from_name(name: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|k| k.name() == name)
    }

    /// Executor-specific config keys this executor accepts.
    fn allowed_fields(self) -> &'static [&'static str] {
        match self {
            ExecutorKind::ConstantVus => &["vus", "duration"],
            ExecutorKind::RampingVus => &["startVUs", "stages", "gracefulRampDown"],
            ExecutorKind::PerVuIterations | ExecutorKind::SharedIterations => {
                &["vus", "iterations", "maxDuration"]
            }
            ExecutorKind::ConstantArrivalRate => {
                &["rate", "timeUnit", "duration", "preAllocatedVUs", "maxVUs"]
            }
            ExecutorKind::RampingArrivalRate => &[
                "startRate",
                "timeUnit",
                "stages",
                "preAllocatedVUs",
                "maxVUs",
            ],
            ExecutorKind::ExternallyControlled => &["vus", "maxVUs", "duration"],
        }
    }
}

impl fmt::Display for ExecutorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Stage {
    pub duration: Duration,
    pub target: u64,
}

/// k6's `--stage` syntax: `duration:target`, e.g. `30s:10`.
impl std::str::FromStr for Stage {
    type Err = String;

    fn from_str(s: &str) -> Result<Stage, String> {
        let (duration, target) = s.split_once(':').ok_or_else(|| {
            format!("invalid stage {s:?} (expected duration:target, e.g. 30s:10)")
        })?;
        Ok(Stage {
            duration: duration.parse().map_err(|e| format!("{e}"))?,
            target: target
                .trim()
                .parse()
                .map_err(|_| format!("invalid stage target {target:?} in {s:?}"))?,
        })
    }
}

/// A threshold as k6 writes it: either `"p(95)<500"` or the long form with
/// `abortOnFail`. Expressions are parsed by the threshold engine (k9-metrics).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Threshold {
    Expr(String),
    #[serde(rename_all = "camelCase")]
    Detailed {
        threshold: String,
        #[serde(default)]
        abort_on_fail: bool,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        delay_abort_eval: Option<Duration>,
    },
}

impl Threshold {
    pub fn expression(&self) -> &str {
        match self {
            Threshold::Expr(e) => e,
            Threshold::Detailed { threshold, .. } => threshold,
        }
    }
}

/// One step of a declarative flow (Tier 1).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum Step {
    Request(Box<RequestSpec>),
    Sleep(Duration),
    Group {
        name: String,
        steps: Vec<Step>,
    },
    Websocket(Box<WsSpec>),
    Grpc(Box<GrpcSpec>),
    /// Requests sent in parallel (k6's `http.batch`), within the `batch` and
    /// `batchPerHost` limits; each is recorded, checked and extracted from
    /// as usual, in order, once they have all finished.
    Batch(Vec<RequestSpec>),
}

/// A unary gRPC call, recorded in `grpc_req_duration`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct GrpcSpec {
    /// `host:port` (TLS unless `plaintext`) or an `http(s)://` URL; no templates.
    pub target: String,
    /// h2c (HTTP/2 without TLS).
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub plaintext: bool,
    /// `package.Service/Method`.
    pub method: String,
    /// `.proto` files defining the method; without any, the server's
    /// reflection service is asked (once, before the run).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub protos: Vec<String>,
    /// Where `protos` and their imports are found (default: each file's directory).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub import_paths: Vec<String>,
    /// `protos` compiled, as a base64 `FileDescriptorSet`: set by
    /// [`Plan::inline_files`] for machines without the files.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub descriptor_set: Option<String>,
    /// The request message in protobuf's JSON mapping; strings in it may
    /// hold templates. Default: an empty message.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<serde_json::Value>,
    /// For a streaming method: the messages to send, in order (then the
    /// stream is half-closed and every answer is read). `checks` and
    /// `extract` then see the answers as a JSON array.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub messages: Vec<serde_json::Value>,
    /// Sent as request headers; values may hold templates.
    #[serde(default, skip_serializing_if = "IndexMap::is_empty")]
    pub metadata: IndexMap<String, String>,
    /// The `name` tag (default: the method's path).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default, skip_serializing_if = "IndexMap::is_empty")]
    pub tags: IndexMap<String, String>,
    /// The call's deadline (default 60s).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout: Option<Duration>,
    /// `gzip`: compress the messages sent (answers in gzip are read either way).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compression: Option<String>,
    /// The largest message to send, in bytes (k6's `maxSendSize`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_send_size: Option<u64>,
    /// The largest message to accept, in bytes (k6's `maxReceiveSize`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_receive_size: Option<u64>,
    /// `status` (the gRPC code), `bodyContains` (the response message as
    /// JSON) and `durationBelow` (`grpc_req_duration`).
    #[serde(default, skip_serializing_if = "IndexMap::is_empty")]
    pub checks: IndexMap<String, Check>,
    /// From the response message (`json`, `regex`) or its metadata (`header`:
    /// headers, then trailers).
    #[serde(default, skip_serializing_if = "IndexMap::is_empty")]
    pub extract: IndexMap<String, Extractor>,
}

/// A WebSocket session: the handshake, then `steps` in order on the open
/// socket, then a normal close (unless a `close` step did it). Recorded in
/// k6's `ws_*` metrics.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WsSpec {
    /// `ws://` or `wss://`; may hold templates.
    pub url: String,
    #[serde(default, skip_serializing_if = "IndexMap::is_empty")]
    pub headers: IndexMap<String, String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default, skip_serializing_if = "IndexMap::is_empty")]
    pub tags: IndexMap<String, String>,
    /// Checks on the handshake: `status` (101, or 0 if it failed) and
    /// `durationBelow` (`ws_connecting`).
    #[serde(default, skip_serializing_if = "IndexMap::is_empty")]
    pub checks: IndexMap<String, Check>,
    #[serde(default)]
    pub steps: Vec<WsStep>,
}

/// One action on an open WebSocket.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum WsStep {
    /// Sends a text message (templates allowed).
    Send(String),
    /// Waits for the next message.
    Receive(WsReceive),
    /// Sends a ping and waits for its pong (`ws_ping`).
    Ping,
    Sleep(Duration),
    /// Closes the WebSocket now; later steps are skipped.
    Close(WsClose),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WsReceive {
    /// How long to wait for the message (default 60s).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout: Option<Duration>,
    /// Checks on the message: `bodyContains`, and `durationBelow` (the wait).
    #[serde(default, skip_serializing_if = "IndexMap::is_empty")]
    pub checks: IndexMap<String, Check>,
    /// Values taken from the message (`json` or `regex`) into variables.
    #[serde(default, skip_serializing_if = "IndexMap::is_empty")]
    pub extract: IndexMap<String, Extractor>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WsClose {
    #[serde(default = "normal_close")]
    pub code: u16,
    #[serde(default)]
    pub reason: String,
}

fn normal_close() -> u16 {
    1000
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RequestSpec {
    #[serde(default)]
    pub method: Method,
    pub url: String,
    #[serde(default, skip_serializing_if = "IndexMap::is_empty")]
    pub headers: IndexMap<String, String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub body: Option<String>,
    /// A `multipart/form-data` body instead of `body`: text fields (templates
    /// allowed) and files (read once, before the run).
    #[serde(default, skip_serializing_if = "IndexMap::is_empty")]
    pub multipart: IndexMap<String, FormPart>,
    /// Compresses the body: `gzip`, `deflate`, `br` or `zstd`, or several
    /// (`gzip, br`, applied in order); sets `Content-Encoding`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compression: Option<String>,
    /// Groups dynamic URLs under one metric name to keep tag cardinality bounded.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default, skip_serializing_if = "IndexMap::is_empty")]
    pub tags: IndexMap<String, String>,
    /// Named checks on the response, recorded in the `checks` metric.
    #[serde(default, skip_serializing_if = "IndexMap::is_empty")]
    pub checks: IndexMap<String, Check>,
    /// Values taken from the response into the VU's variables, by name.
    #[serde(default, skip_serializing_if = "IndexMap::is_empty")]
    pub extract: IndexMap<String, Extractor>,
}

/// One part of a multipart form.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum FormPart {
    /// A text field.
    Text(String),
    File(FilePart),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct FilePart {
    /// The file to send (relative to the plan).
    pub file: String,
    /// Default: the file's name.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filename: Option<String>,
    /// Default: `application/octet-stream`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_type: Option<String>,
    /// The file's contents in base64, used instead of reading `file` (set by
    /// [`Plan::inline_files`] for machines without the file).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<String>,
}

/// Where an extracted variable's value comes from. If nothing is found, the
/// iteration stops there.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub enum Extractor {
    /// A JSON path into the body: `$.user.name`, `$.items[0]`, `$['a-b']`.
    Json(String),
    /// The regex's first capture group in the body (or the whole match).
    Regex(String),
    /// A response header's value.
    Header(String),
}

/// Rows of test data: each iteration that uses a feeder gets one row.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Feeder {
    /// A `.csv` file (with a header row) or `.json` file (an array of objects).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub file: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub rows: Vec<IndexMap<String, serde_json::Value>>,
    #[serde(default)]
    pub mode: FeederMode,
}

/// Which row an iteration gets.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum FeederMode {
    /// The next row, round and round. Split between workers, so no two
    /// workers use the same row.
    #[default]
    Sequential,
    /// A random row each time.
    Random,
    /// VU `n` always gets row `n − 1` (wrapping around).
    PerVu,
}

impl Plan {
    /// Puts every file the plan names into the plan itself, so it runs where
    /// the files aren't (agents on other machines): feeder files become
    /// `rows`, `tlsAuth` files PEM text, `.proto` files a compiled
    /// `descriptorSet`, multipart files base64 `data`.
    pub fn inline_files(&mut self) -> Result<(), String> {
        use base64::Engine as _;
        for (i, auth) in self.options.http.tls_auth.iter_mut().enumerate() {
            for value in [&mut auth.cert, &mut auth.key] {
                if !TlsAuth::is_pem(value) {
                    *value = std::fs::read_to_string(&*value)
                        .map_err(|e| format!("tlsAuth[{i}]: reading {value}: {e}"))?;
                }
            }
        }
        for (name, feeder) in &mut self.feeders {
            if let Some(file) = feeder.file.take() {
                feeder.rows = crate::engine::feeder_rows(&file)
                    .map_err(|e| format!("feeder `{name}`: {e}"))?;
            }
        }
        let (grpc, forms) = file_steps(&mut self.flows);
        for (location, spec) in grpc {
            if spec.protos.is_empty() || spec.descriptor_set.is_some() {
                continue;
            }
            let protos = k9_http::grpc::Protos::load(&spec.protos, &spec.import_paths)
                .map_err(|e| format!("{location}: {e}"))?;
            spec.descriptor_set =
                Some(base64::engine::general_purpose::STANDARD.encode(protos.encode()));
        }
        for (location, spec) in forms {
            for (name, part) in &mut spec.multipart {
                if let FormPart::File(file) = part
                    && file.data.is_none()
                {
                    let data = std::fs::read(&file.file).map_err(|e| {
                        format!("{location}: multipart `{name}`: reading {}: {e}", file.file)
                    })?;
                    file.data = Some(base64::engine::general_purpose::STANDARD.encode(data));
                }
            }
        }
        Ok(())
    }

    /// Makes relative feeder files, multipart files, `.proto` files and import paths
    /// relative to `dir` (the plan file's directory). With import paths,
    /// `.proto` files are named relative to them, so they stay as they are.
    pub fn rebase_files(&mut self, dir: &std::path::Path) {
        let rebase = |file: &mut String| {
            if std::path::Path::new(file.as_str()).is_relative() {
                *file = dir.join(&*file).display().to_string();
            }
        };
        for feeder in self.feeders.values_mut() {
            if let Some(file) = &mut feeder.file {
                rebase(file);
            }
        }
        for auth in &mut self.options.http.tls_auth {
            for value in [&mut auth.cert, &mut auth.key] {
                if !TlsAuth::is_pem(value) {
                    rebase(value);
                }
            }
        }
        let (specs, forms) = file_steps(&mut self.flows);
        for (_, form) in forms {
            for part in form.multipart.values_mut() {
                if let FormPart::File(file) = part {
                    rebase(&mut file.file);
                }
            }
        }
        for (_, spec) in specs {
            if spec.import_paths.is_empty() {
                spec.protos.iter_mut().for_each(rebase);
            } else {
                spec.import_paths.iter_mut().for_each(rebase);
            }
        }
    }
}

/// A native check, evaluated in Rust on every response (no Python involved).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub enum Check {
    /// The status is this code, or one of these codes.
    Status(StatusCheck),
    /// The body contains this text.
    BodyContains(String),
    /// `http_req_duration` is below this.
    DurationBelow(Duration),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum StatusCheck {
    One(u16),
    Any(Vec<u16>),
}

impl StatusCheck {
    pub fn matches(&self, status: u16) -> bool {
        match self {
            StatusCheck::One(code) => *code == status,
            StatusCheck::Any(codes) => codes.contains(&status),
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum Method {
    #[default]
    #[serde(alias = "get")]
    Get,
    #[serde(alias = "post")]
    Post,
    #[serde(alias = "put")]
    Put,
    #[serde(alias = "patch")]
    Patch,
    #[serde(alias = "delete")]
    Delete,
    #[serde(alias = "head")]
    Head,
    #[serde(alias = "options")]
    Options,
}

// ---------------------------------------------------------------------------
// User-facing config
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct PlanConfig {
    pub options: OptionsConfig,
    pub flows: IndexMap<String, Vec<Step>>,
    pub feeders: IndexMap<String, Feeder>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default, deny_unknown_fields, rename_all = "camelCase")]
pub struct OptionsConfig {
    pub vus: Option<u32>,
    pub duration: Option<Duration>,
    pub iterations: Option<u64>,
    pub stages: Option<Vec<Stage>>,
    pub scenarios: IndexMap<String, ScenarioConfig>,
    pub thresholds: IndexMap<String, Vec<Threshold>>,
    pub tags: IndexMap<String, String>,
    pub max_redirects: Option<u32>,
    #[serde(rename = "insecureSkipTLSVerify")]
    pub insecure_skip_tls_verify: Option<bool>,
    pub user_agent: Option<String>,
    pub discard_response_bodies: Option<bool>,
    pub http_version: Option<HttpVersion>,
    pub batch: Option<u32>,
    pub batch_per_host: Option<u32>,
    pub tls_auth: Option<Vec<TlsAuth>>,
    pub tls_version: Option<TlsVersionOption>,
    pub tls_cipher_suites: Option<Vec<String>>,
    pub hosts: Option<IndexMap<String, String>>,
    pub dns: Option<DnsOption>,
    pub block_hostnames: Option<ListOption>,
    #[serde(rename = "blacklistIPs")]
    pub blacklist_ips: Option<ListOption>,
    #[serde(rename = "localIPs")]
    pub local_ips: Option<ListOption>,
    pub no_connection_reuse: Option<bool>,
    #[serde(rename = "noVUConnectionReuse")]
    pub no_vu_connection_reuse: Option<bool>,
    pub http_debug: Option<String>,
    pub setup_timeout: Option<Duration>,
    pub teardown_timeout: Option<Duration>,
    pub summary_trend_stats: Option<Vec<String>>,
    pub summary_time_unit: Option<String>,
    pub min_iteration_duration: Option<Duration>,
    pub throw: Option<bool>,
    pub system_tags: Option<ListOption>,
    pub no_cookies_reset: Option<bool>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default, deny_unknown_fields, rename_all = "camelCase")]
pub struct ScenarioConfig {
    pub executor: Option<String>,
    pub exec: Option<String>,
    pub transactions: Option<IndexMap<String, u32>>,
    pub on_start: Option<String>,
    pub on_stop: Option<String>,
    pub start_time: Option<Duration>,
    pub graceful_stop: Option<Duration>,
    pub tags: IndexMap<String, String>,
    pub env: IndexMap<String, String>,

    pub vus: Option<u32>,
    pub duration: Option<Duration>,
    pub iterations: Option<u64>,
    pub max_duration: Option<Duration>,
    #[serde(rename = "startVUs")]
    pub start_vus: Option<u32>,
    pub stages: Option<Vec<Stage>>,
    pub graceful_ramp_down: Option<Duration>,
    pub rate: Option<u64>,
    pub start_rate: Option<u64>,
    pub time_unit: Option<Duration>,
    #[serde(rename = "preAllocatedVUs")]
    pub pre_allocated_vus: Option<u32>,
    #[serde(rename = "maxVUs")]
    pub max_vus: Option<u32>,
}

impl ScenarioConfig {
    /// Executor-specific keys that were set, by their config names.
    fn set_fields(&self) -> Vec<&'static str> {
        [
            ("vus", self.vus.is_some()),
            ("duration", self.duration.is_some()),
            ("iterations", self.iterations.is_some()),
            ("maxDuration", self.max_duration.is_some()),
            ("startVUs", self.start_vus.is_some()),
            ("stages", self.stages.is_some()),
            ("gracefulRampDown", self.graceful_ramp_down.is_some()),
            ("rate", self.rate.is_some()),
            ("startRate", self.start_rate.is_some()),
            ("timeUnit", self.time_unit.is_some()),
            ("preAllocatedVUs", self.pre_allocated_vus.is_some()),
            ("maxVUs", self.max_vus.is_some()),
        ]
        .into_iter()
        .filter_map(|(name, set)| set.then_some(name))
        .collect()
    }
}

// ---------------------------------------------------------------------------
// Validation and resolution
// ---------------------------------------------------------------------------

/// A plan that failed validation. Collects every problem instead of stopping at
/// the first, so users can fix a config in one pass.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub struct InvalidPlan {
    pub issues: Vec<Issue>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Issue {
    /// Dotted location in the config, e.g. `options.scenarios.browse.rate`.
    pub path: String,
    pub message: String,
}

impl fmt::Display for InvalidPlan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let n = self.issues.len();
        write!(
            f,
            "invalid plan ({n} problem{}):",
            if n == 1 { "" } else { "s" }
        )?;
        for issue in &self.issues {
            write!(f, "\n  - {}: {}", issue.path, issue.message)?;
        }
        Ok(())
    }
}

#[derive(Default)]
struct Issues(Vec<Issue>);

impl Issues {
    fn push(&mut self, path: impl Into<String>, message: impl Into<String>) {
        self.0.push(Issue {
            path: path.into(),
            message: message.into(),
        });
    }
}

/// What a scenario's `exec` names.
enum Entries<'a> {
    /// The plan's own flows.
    Flows,
    /// Functions defined by a Python script.
    Functions(&'a [&'a str]),
}

impl PlanConfig {
    /// Validates the config and resolves it into a [`Plan`].
    pub fn resolve(self) -> Result<Plan, InvalidPlan> {
        self.resolve_with(Entries::Flows)
    }

    /// Like [`resolve`](Self::resolve) for a script's `options`: each
    /// scenario's `exec` must name one of the script's `functions`.
    pub fn resolve_script(self, functions: &[&str]) -> Result<Plan, InvalidPlan> {
        self.resolve_with(Entries::Functions(functions))
    }

    fn resolve_with(self, entries: Entries<'_>) -> Result<Plan, InvalidPlan> {
        let mut issues = Issues::default();
        let OptionsConfig {
            vus,
            duration,
            iterations,
            stages,
            scenarios: scenario_configs,
            thresholds,
            tags,
            max_redirects,
            insecure_skip_tls_verify,
            user_agent,
            discard_response_bodies,
            http_version,
            batch,
            batch_per_host,
            tls_auth,
            tls_version,
            tls_cipher_suites,
            hosts,
            dns,
            block_hostnames,
            blacklist_ips,
            local_ips,
            no_connection_reuse,
            no_vu_connection_reuse,
            http_debug,
            setup_timeout,
            teardown_timeout,
            summary_trend_stats,
            summary_time_unit,
            min_iteration_duration,
            throw,
            system_tags,
            no_cookies_reset,
        } = self.options;
        let defaults = HttpOptions::default();
        let http = HttpOptions {
            max_redirects: max_redirects.unwrap_or(defaults.max_redirects),
            insecure_skip_tls_verify: insecure_skip_tls_verify
                .unwrap_or(defaults.insecure_skip_tls_verify),
            user_agent: user_agent.unwrap_or(defaults.user_agent),
            discard_response_bodies: discard_response_bodies
                .unwrap_or(defaults.discard_response_bodies),
            http_version: http_version.unwrap_or(defaults.http_version),
            batch: batch.unwrap_or(defaults.batch),
            batch_per_host: batch_per_host.unwrap_or(defaults.batch_per_host),
            tls_auth: tls_auth.unwrap_or_default(),
            tls_version,
            tls_cipher_suites: tls_cipher_suites.unwrap_or_default(),
            hosts: hosts.unwrap_or_default(),
            dns: DnsOptions::default(),
            block_hostnames: block_hostnames
                .map(ListOption::into_vec)
                .unwrap_or_default(),
            blacklist_ips: blacklist_ips.map(ListOption::into_vec).unwrap_or_default(),
            local_ips: local_ips.map(ListOption::into_vec).unwrap_or_default(),
            no_connection_reuse: no_connection_reuse.unwrap_or(defaults.no_connection_reuse),
            no_vu_connection_reuse: no_vu_connection_reuse
                .unwrap_or(defaults.no_vu_connection_reuse),
            http_debug,
        };
        let mut http = http;
        match dns {
            None => {}
            Some(DnsOption::Text(text)) => {
                if let Err(e) = http.dns.apply_text(&text) {
                    issues.push("options.dns", e);
                }
            }
            Some(DnsOption::Fields {
                ttl,
                select,
                policy,
            }) => {
                let dns = &mut http.dns;
                dns.ttl = ttl.unwrap_or(std::mem::take(&mut dns.ttl));
                dns.select = select.unwrap_or(std::mem::take(&mut dns.select));
                dns.policy = policy.unwrap_or(std::mem::take(&mut dns.policy));
            }
        }
        if let Some(Err(e)) = http
            .http_debug
            .as_deref()
            .map(str::parse::<k9_http::HttpDebug>)
        {
            issues.push("options.httpDebug", e);
        }
        if let Err(network) = http.network() {
            issues.0.extend(network);
        }
        if http.batch == 0 {
            issues.push("options.batch", "must be at least 1");
        }
        if let Some(Err(message)) = http.tls_version.as_ref().map(TlsVersionOption::range) {
            issues.push("options.tlsVersion", message);
        }
        for (i, auth) in http.tls_auth.iter().enumerate() {
            if auth.password.is_some() {
                issues.push(
                    format!("options.tlsAuth[{i}].password"),
                    "encrypted keys aren't supported; decrypt the key first",
                );
            }
        }

        let mut scenarios = IndexMap::new();
        let has_shortcut =
            vus.is_some() || duration.is_some() || iterations.is_some() || stages.is_some();
        if !scenario_configs.is_empty() {
            if has_shortcut {
                issues.push(
                    "options",
                    "`vus`, `duration`, `iterations` and `stages` can't be combined with \
                     `scenarios`; move them into a scenario",
                );
            }
            for (name, cfg) in scenario_configs {
                let path = format!("options.scenarios.{name}");
                if !is_valid_scenario_name(&name) {
                    issues.push(
                        &path,
                        "scenario names may only contain letters, digits, `_` and `-`",
                    );
                }
                if let Some(scenario) = resolve_scenario(cfg, &path, &mut issues) {
                    scenarios.insert(name, scenario);
                }
            }
        } else {
            let executor = executor_from_shortcuts(vus, duration, iterations, stages, &mut issues);
            scenarios.insert(
                DEFAULT_SCENARIO.to_owned(),
                Scenario {
                    executor,
                    exec: DEFAULT_FLOW.to_owned(),
                    transactions: IndexMap::new(),
                    on_start: None,
                    on_stop: None,
                    start_time: Duration::ZERO,
                    graceful_stop: DEFAULT_GRACEFUL_STOP,
                    tags: IndexMap::new(),
                    env: IndexMap::new(),
                },
            );
        }

        let list = |names: Vec<&str>| {
            if names.is_empty() {
                "none".to_owned()
            } else {
                let quoted: Vec<_> = names.iter().map(|n| format!("`{n}`")).collect();
                quoted.join(", ")
            }
        };
        for (name, scenario) in &scenarios {
            for (field, exec) in scenario.entry_points() {
                let problem = match entries {
                    Entries::Flows if !self.flows.contains_key(exec) => Some(format!(
                        "no flow named `{exec}` (defined flows: {})",
                        list(self.flows.keys().map(String::as_str).collect())
                    )),
                    Entries::Functions(functions) if !functions.contains(&exec) => Some(format!(
                        "the script has no function `{exec}` (functions it defines: {})",
                        list(functions.to_vec())
                    )),
                    _ => None,
                };
                if let Some(problem) = problem {
                    issues.push(format!("options.scenarios.{name}.{field}"), problem);
                }
            }
        }
        let external: Vec<&str> = scenarios
            .iter()
            .filter(|(_, s)| matches!(s.executor, Executor::ExternallyControlled { .. }))
            .map(|(name, _)| name.as_str())
            .collect();
        if external.len() > 1 {
            issues.push(
                "options.scenarios",
                format!(
                    "at most one scenario can be externally-controlled (the control API scales one number of VUs), found {}",
                    list(external)
                ),
            );
        }
        for (name, steps) in &self.flows {
            validate_steps(steps, &format!("flows.{name}"), &mut issues);
        }
        if let Some(Err(e)) = summary_time_unit.as_deref().map(k9_output::TimeUnit::parse) {
            issues.push("options.summaryTimeUnit", e);
        }
        let system_tags = system_tags
            .map(ListOption::into_vec)
            .unwrap_or_else(default_system_tags);
        for (i, tag) in system_tags.iter().enumerate() {
            let path = format!("options.systemTags[{i}]");
            if UNSUPPORTED_SYSTEM_TAGS.contains(&tag.as_str()) {
                issues.push(path, format!("the `{tag}` tag isn't supported yet"));
            } else if !DEFAULT_SYSTEM_TAGS.contains(&tag.as_str()) {
                issues.push(
                    path,
                    format!(
                        "unknown system tag `{tag}` (expected one of {})",
                        DEFAULT_SYSTEM_TAGS.join(", ")
                    ),
                );
            }
        }
        if let Some(stats) = &summary_trend_stats {
            if stats.is_empty() {
                issues.push("options.summaryTrendStats", "needs at least one stat");
            }
            for (i, stat) in stats.iter().enumerate() {
                if let Err(e) = k9_output::TrendStat::parse(stat) {
                    issues.push(format!("options.summaryTrendStats[{i}]"), e);
                }
            }
        }
        for (selector, list) in &thresholds {
            if let Err(e) = selector.parse::<Selector>() {
                issues.push(format!("options.thresholds.{selector}"), e);
            }
            for (i, t) in list.iter().enumerate() {
                if let Err(e) = t.expression().parse::<ThresholdExpr>() {
                    issues.push(format!("options.thresholds.{selector}[{i}]"), e);
                }
            }
        }

        if !issues.0.is_empty() {
            return Err(InvalidPlan { issues: issues.0 });
        }
        Ok(Plan {
            options: Options {
                scenarios,
                thresholds,
                tags,
                setup_timeout: setup_timeout.unwrap_or(DEFAULT_HOOK_TIMEOUT),
                summary_trend_stats: summary_trend_stats.unwrap_or_else(default_trend_stats),
                teardown_timeout: teardown_timeout.unwrap_or(DEFAULT_HOOK_TIMEOUT),
                summary_time_unit,
                min_iteration_duration,
                throw: throw.unwrap_or(false),
                system_tags,
                no_cookies_reset: no_cookies_reset.unwrap_or(false),
                http,
            },
            flows: self.flows,
            feeders: self.feeders,
        })
    }
}

/// Steps that name files, with where they are: gRPC calls, and requests
/// (for their multipart files).
type FileSteps<'a> = (
    Vec<(String, &'a mut GrpcSpec)>,
    Vec<(String, &'a mut RequestSpec)>,
);

fn file_steps(flows: &mut IndexMap<String, Vec<Step>>) -> FileSteps<'_> {
    fn walk<'a>(steps: &'a mut [Step], path: &str, out: &mut FileSteps<'a>) {
        for (i, step) in steps.iter_mut().enumerate() {
            let at = format!("{path}[{i}]");
            match step {
                Step::Grpc(spec) => out.0.push((at, spec)),
                Step::Request(spec) => out.1.push((at, spec)),
                Step::Batch(specs) => {
                    for (j, spec) in specs.iter_mut().enumerate() {
                        out.1.push((format!("{at}.batch[{j}]"), spec));
                    }
                }
                Step::Group { steps, .. } => walk(steps, &format!("{at}.steps"), out),
                _ => {}
            }
        }
    }
    let mut out = (Vec::new(), Vec::new());
    for (name, steps) in flows {
        walk(steps, &format!("flows.{name}"), &mut out);
    }
    out
}

fn is_valid_scenario_name(name: &str) -> bool {
    !name.is_empty()
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
}

/// k6's shortcut rules: `stages` → ramping-vus, `iterations` → shared-iterations
/// (with `duration` as its maxDuration), `duration` → constant-vus. With no
/// shortcut at all, each VU runs one iteration.
fn executor_from_shortcuts(
    vus: Option<u32>,
    duration: Option<Duration>,
    iterations: Option<u64>,
    stages: Option<Vec<Stage>>,
    issues: &mut Issues,
) -> Executor {
    let vus_or_one = vus.unwrap_or(1);
    let executor = if let Some(stages) = stages {
        if duration.is_some() || iterations.is_some() {
            issues.push(
                "options",
                "`stages` can't be combined with `duration` or `iterations`",
            );
        }
        Executor::RampingVus {
            start_vus: vus_or_one,
            stages,
            graceful_ramp_down: DEFAULT_GRACEFUL_RAMP_DOWN,
        }
    } else if let Some(iterations) = iterations {
        Executor::SharedIterations {
            vus: vus_or_one,
            iterations,
            max_duration: duration.unwrap_or(DEFAULT_MAX_DURATION),
        }
    } else if let Some(duration) = duration {
        Executor::ConstantVus {
            vus: vus_or_one,
            duration,
        }
    } else {
        Executor::SharedIterations {
            vus: vus_or_one,
            iterations: u64::from(vus_or_one),
            max_duration: DEFAULT_MAX_DURATION,
        }
    };
    validate_executor(&executor, "options", issues);
    executor
}

fn resolve_scenario(cfg: ScenarioConfig, path: &str, issues: &mut Issues) -> Option<Scenario> {
    let Some(kind_name) = cfg.executor.as_deref() else {
        issues.push(
            format!("{path}.executor"),
            format!("missing (one of: {})", executor_names()),
        );
        return None;
    };
    let Some(kind) = ExecutorKind::from_name(kind_name) else {
        issues.push(
            format!("{path}.executor"),
            format!(
                "unknown executor `{kind_name}` (one of: {})",
                executor_names()
            ),
        );
        return None;
    };

    let issues_before = issues.0.len();
    let allowed = kind.allowed_fields();
    for field in cfg.set_fields() {
        if !allowed.contains(&field) {
            issues.push(
                format!("{path}.{field}"),
                format!(
                    "not valid for {kind} (valid: {})",
                    allowed
                        .iter()
                        .map(|f| format!("`{f}`"))
                        .collect::<Vec<_>>()
                        .join(", ")
                ),
            );
        }
    }

    let mut require = |value: Option<Duration>, field: &str| {
        value.unwrap_or_else(|| {
            issues.push(format!("{path}.{field}"), format!("required for {kind}"));
            Duration::ZERO
        })
    };
    let executor = match kind {
        ExecutorKind::ConstantVus => Executor::ConstantVus {
            vus: cfg.vus.unwrap_or(1),
            duration: require(cfg.duration, "duration"),
        },
        ExecutorKind::RampingVus => Executor::RampingVus {
            start_vus: cfg.start_vus.unwrap_or(1),
            stages: cfg.stages.unwrap_or_default(),
            graceful_ramp_down: cfg.graceful_ramp_down.unwrap_or(DEFAULT_GRACEFUL_RAMP_DOWN),
        },
        ExecutorKind::PerVuIterations => Executor::PerVuIterations {
            vus: cfg.vus.unwrap_or(1),
            iterations: cfg.iterations.unwrap_or(1),
            max_duration: cfg.max_duration.unwrap_or(DEFAULT_MAX_DURATION),
        },
        ExecutorKind::SharedIterations => Executor::SharedIterations {
            vus: cfg.vus.unwrap_or(1),
            iterations: cfg.iterations.unwrap_or(1),
            max_duration: cfg.max_duration.unwrap_or(DEFAULT_MAX_DURATION),
        },
        ExecutorKind::ConstantArrivalRate => {
            let duration = require(cfg.duration, "duration");
            let pre = required_count(cfg.pre_allocated_vus, path, "preAllocatedVUs", kind, issues);
            Executor::ConstantArrivalRate {
                rate: required_count(cfg.rate, path, "rate", kind, issues),
                time_unit: cfg.time_unit.unwrap_or(DEFAULT_TIME_UNIT),
                duration,
                pre_allocated_vus: pre,
                max_vus: cfg.max_vus.unwrap_or(pre),
            }
        }
        ExecutorKind::RampingArrivalRate => {
            let pre = required_count(cfg.pre_allocated_vus, path, "preAllocatedVUs", kind, issues);
            Executor::RampingArrivalRate {
                start_rate: cfg.start_rate.unwrap_or(0),
                time_unit: cfg.time_unit.unwrap_or(DEFAULT_TIME_UNIT),
                stages: cfg.stages.unwrap_or_default(),
                pre_allocated_vus: pre,
                max_vus: cfg.max_vus.unwrap_or(pre),
            }
        }
        ExecutorKind::ExternallyControlled => {
            let duration = require(cfg.duration, "duration");
            let vus = cfg.vus.unwrap_or(1);
            Executor::ExternallyControlled {
                vus,
                max_vus: cfg.max_vus.unwrap_or(vus),
                duration,
            }
        }
    };
    if issues.0.len() == issues_before {
        validate_executor(&executor, path, issues);
    }

    if let Some(transactions) = &cfg.transactions {
        if transactions.is_empty() {
            issues.push(
                format!("{path}.transactions"),
                "needs at least one transaction",
            );
        }
        if cfg.exec.is_some() {
            issues.push(
                format!("{path}.exec"),
                "give `exec` or `transactions`, not both",
            );
        }
    }
    let transactions = cfg.transactions.unwrap_or_default();
    for (name, weight) in &transactions {
        if *weight == 0 {
            issues.push(
                format!("{path}.transactions.{name}"),
                "the weight must be at least 1",
            );
        }
    }
    // Each VU holds a round of `sum` transactions.
    if transactions.values().map(|w| u64::from(*w)).sum::<u64>() > MAX_TRANSACTION_ROUND {
        issues.push(
            format!("{path}.transactions"),
            format!("the weights add up to more than {MAX_TRANSACTION_ROUND}"),
        );
    }
    let exec = match transactions.keys().next() {
        Some(first) => first.clone(),
        None => cfg.exec.unwrap_or_else(|| DEFAULT_FLOW.to_owned()),
    };
    Some(Scenario {
        executor,
        exec,
        transactions,
        on_start: cfg.on_start,
        on_stop: cfg.on_stop,
        start_time: cfg.start_time.unwrap_or(Duration::ZERO),
        graceful_stop: cfg.graceful_stop.unwrap_or(DEFAULT_GRACEFUL_STOP),
        tags: cfg.tags,
        env: cfg.env,
    })
}

fn required_count<T: Default>(
    value: Option<T>,
    path: &str,
    field: &str,
    kind: ExecutorKind,
    issues: &mut Issues,
) -> T {
    value.unwrap_or_else(|| {
        issues.push(format!("{path}.{field}"), format!("required for {kind}"));
        T::default()
    })
}

/// Numeric sanity checks on a resolved executor. Only run when the scenario had
/// no field errors, so a missing field isn't also reported as "must be > 0".
fn validate_executor(executor: &Executor, path: &str, issues: &mut Issues) {
    let mut positive = |ok: bool, field: &str, what: &str| {
        if !ok {
            issues.push(format!("{path}.{field}"), format!("must be {what}"));
        }
    };
    match executor {
        Executor::ConstantVus { vus, duration } => {
            positive(*vus > 0, "vus", "at least 1");
            positive(!duration.is_zero(), "duration", "greater than 0");
        }
        Executor::RampingVus { stages, .. } => {
            positive(!stages.is_empty(), "stages", "a non-empty list");
        }
        Executor::PerVuIterations {
            vus, iterations, ..
        } => {
            positive(*vus > 0, "vus", "at least 1");
            positive(*iterations > 0, "iterations", "at least 1");
        }
        Executor::SharedIterations {
            vus, iterations, ..
        } => {
            positive(*vus > 0, "vus", "at least 1");
            positive(
                *iterations >= u64::from(*vus),
                "iterations",
                "at least `vus` (each VU needs one iteration)",
            );
        }
        Executor::ConstantArrivalRate {
            rate,
            time_unit,
            duration,
            pre_allocated_vus,
            max_vus,
        } => {
            positive(!time_unit.is_zero(), "timeUnit", "greater than 0");
            if *rate > 0 && !duration.is_zero() && *pre_allocated_vus > 0 {
                positive(
                    max_vus >= pre_allocated_vus,
                    "maxVUs",
                    "at least `preAllocatedVUs`",
                );
            }
        }
        Executor::RampingArrivalRate {
            time_unit,
            stages,
            pre_allocated_vus,
            max_vus,
            ..
        } => {
            positive(!time_unit.is_zero(), "timeUnit", "greater than 0");
            positive(!stages.is_empty(), "stages", "a non-empty list");
            if *pre_allocated_vus > 0 {
                positive(
                    max_vus >= pre_allocated_vus,
                    "maxVUs",
                    "at least `preAllocatedVUs`",
                );
            }
        }
        Executor::ExternallyControlled { vus, max_vus, .. } => {
            positive(max_vus >= vus, "maxVUs", "at least `vus`");
        }
    }
}

fn validate_steps(steps: &[Step], path: &str, issues: &mut Issues) {
    for (i, step) in steps.iter().enumerate() {
        let path = format!("{path}[{i}]");
        match step {
            Step::Request(req) => {
                if req.url.trim().is_empty() {
                    issues.push(format!("{path}.url"), "must not be empty");
                }
            }
            Step::Sleep(_) => {}
            Step::Websocket(ws) => {
                if ws.url.trim().is_empty() {
                    issues.push(format!("{path}.url"), "must not be empty");
                }
            }
            Step::Batch(requests) => {
                if requests.is_empty() {
                    issues.push(path.clone(), "a batch needs at least one request");
                }
                for (j, req) in requests.iter().enumerate() {
                    if req.url.trim().is_empty() {
                        issues.push(format!("{path}.batch[{j}].url"), "must not be empty");
                    }
                }
            }
            Step::Grpc(grpc) => {
                if grpc.target.trim().is_empty() {
                    issues.push(format!("{path}.target"), "must not be empty");
                }
                if grpc.method.trim().is_empty() {
                    issues.push(format!("{path}.method"), "must not be empty");
                }
            }
            Step::Group { name, steps } => {
                if name.trim().is_empty() {
                    issues.push(format!("{path}.name"), "group name must not be empty");
                }
                validate_steps(steps, &format!("{path}.steps"), issues);
            }
        }
    }
}

fn executor_names() -> String {
    ExecutorKind::ALL.map(ExecutorKind::name).join(", ")
}
