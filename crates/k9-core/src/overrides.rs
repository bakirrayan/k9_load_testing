//! Option overrides from CLI flags and `K9_*` environment variables.
//!
//! Precedence, highest first: CLI flags, environment, the plan, defaults.
//! As in k6, setting `duration`, `iterations` or `stages` in a higher layer
//! replaces the plan's scenarios and its own execution shortcuts.

use crate::duration::Duration;
use crate::plan::{DnsOption, HttpVersion, InvalidPlan, Issue, ListOption, PlanConfig, Stage};

#[derive(Debug, Clone, Default, PartialEq)]
pub struct Overrides {
    pub vus: Option<u32>,
    pub duration: Option<Duration>,
    pub iterations: Option<u64>,
    pub stages: Option<Vec<Stage>>,
    pub max_redirects: Option<u32>,
    pub insecure_skip_tls_verify: Option<bool>,
    pub user_agent: Option<String>,
    pub discard_response_bodies: Option<bool>,
    pub http_version: Option<HttpVersion>,
    pub batch: Option<u32>,
    pub batch_per_host: Option<u32>,
    /// k6's text form, `ttl=1m,select=first,policy=any`.
    pub dns: Option<String>,
    pub block_hostnames: Option<Vec<String>>,
    pub blacklist_ips: Option<Vec<String>>,
    pub local_ips: Option<Vec<String>>,
    pub no_connection_reuse: Option<bool>,
    pub no_vu_connection_reuse: Option<bool>,
    pub summary_trend_stats: Option<Vec<String>>,
    pub summary_time_unit: Option<String>,
    pub min_iteration_duration: Option<Duration>,
    pub throw: Option<bool>,
    pub system_tags: Option<Vec<String>>,
    pub no_cookies_reset: Option<bool>,
    /// `headers` or `full`.
    pub http_debug: Option<String>,
}

impl Overrides {
    /// Combines two layers; values set in `self` win over `lower`.
    pub fn or(self, lower: Overrides) -> Overrides {
        Overrides {
            vus: self.vus.or(lower.vus),
            duration: self.duration.or(lower.duration),
            iterations: self.iterations.or(lower.iterations),
            stages: self.stages.or(lower.stages),
            max_redirects: self.max_redirects.or(lower.max_redirects),
            insecure_skip_tls_verify: self
                .insecure_skip_tls_verify
                .or(lower.insecure_skip_tls_verify),
            user_agent: self.user_agent.or(lower.user_agent),
            discard_response_bodies: self
                .discard_response_bodies
                .or(lower.discard_response_bodies),
            http_version: self.http_version.or(lower.http_version),
            batch: self.batch.or(lower.batch),
            batch_per_host: self.batch_per_host.or(lower.batch_per_host),
            dns: self.dns.or(lower.dns),
            block_hostnames: self.block_hostnames.or(lower.block_hostnames),
            blacklist_ips: self.blacklist_ips.or(lower.blacklist_ips),
            local_ips: self.local_ips.or(lower.local_ips),
            no_connection_reuse: self.no_connection_reuse.or(lower.no_connection_reuse),
            no_vu_connection_reuse: self.no_vu_connection_reuse.or(lower.no_vu_connection_reuse),
            summary_trend_stats: self.summary_trend_stats.or(lower.summary_trend_stats),
            summary_time_unit: self.summary_time_unit.or(lower.summary_time_unit),
            min_iteration_duration: self.min_iteration_duration.or(lower.min_iteration_duration),
            throw: self.throw.or(lower.throw),
            system_tags: self.system_tags.or(lower.system_tags),
            no_cookies_reset: self.no_cookies_reset.or(lower.no_cookies_reset),
            http_debug: self.http_debug.or(lower.http_debug),
        }
    }

    /// Reads `K9_VUS`, `K9_DURATION`, `K9_ITERATIONS`, `K9_STAGES` (`30s:10,1m:0`),
    /// `K9_MAX_REDIRECTS`, `K9_INSECURE_SKIP_TLS_VERIFY`, `K9_USER_AGENT`, `K9_BATCH`, `K9_BATCH_PER_HOST`,
    /// `K9_DISCARD_RESPONSE_BODIES`, `K9_HTTP_VERSION`, `K9_DNS` (`ttl=1m,select=first`),
    /// `K9_BLOCK_HOSTNAMES`, `K9_BLACKLIST_IPS`, `K9_LOCAL_IPS` (comma-separated),
    /// `K9_NO_CONNECTION_REUSE`, `K9_NO_VU_CONNECTION_REUSE`, `K9_MIN_ITERATION_DURATION`,
    /// `K9_THROW`, `K9_SYSTEM_TAGS`, `K9_NO_COOKIES_RESET`, `K9_SUMMARY_TIME_UNIT`, `K9_HTTP_DEBUG` and
    /// `K9_SUMMARY_TREND_STATS` (`avg,p(99)`).
    /// Other variables are ignored.
    pub fn from_env(
        vars: impl IntoIterator<Item = (String, String)>,
    ) -> Result<Overrides, InvalidPlan> {
        let mut o = Overrides::default();
        let mut issues = Vec::new();
        let mut bad = |name: &str, message: String| {
            issues.push(Issue {
                path: name.to_owned(),
                message,
            });
        };
        for (name, value) in vars {
            match name.as_str() {
                "K9_VUS" => match value.parse() {
                    Ok(v) => o.vus = Some(v),
                    Err(_) => bad(&name, format!("expected a number of VUs, got {value:?}")),
                },
                "K9_DURATION" => match value.parse() {
                    Ok(v) => o.duration = Some(v),
                    Err(e) => bad(&name, format!("{e}")),
                },
                "K9_ITERATIONS" => match value.parse() {
                    Ok(v) => o.iterations = Some(v),
                    Err(_) => bad(
                        &name,
                        format!("expected a number of iterations, got {value:?}"),
                    ),
                },
                "K9_STAGES" => match value.split(',').map(str::parse).collect() {
                    Ok(v) => o.stages = Some(v),
                    Err(e) => bad(&name, e),
                },
                "K9_MAX_REDIRECTS" => match value.parse() {
                    Ok(v) => o.max_redirects = Some(v),
                    Err(_) => bad(&name, format!("expected a number, got {value:?}")),
                },
                "K9_INSECURE_SKIP_TLS_VERIFY" => match parse_bool(&value) {
                    Some(v) => o.insecure_skip_tls_verify = Some(v),
                    None => bad(&name, format!("expected true or false, got {value:?}")),
                },
                "K9_USER_AGENT" => o.user_agent = Some(value),
                "K9_BATCH" => match value.parse() {
                    Ok(v) => o.batch = Some(v),
                    Err(_) => bad(&name, format!("expected a number, got {value:?}")),
                },
                "K9_BATCH_PER_HOST" => match value.parse() {
                    Ok(v) => o.batch_per_host = Some(v),
                    Err(_) => bad(&name, format!("expected a number, got {value:?}")),
                },
                "K9_HTTP_VERSION" => match value.parse() {
                    Ok(v) => o.http_version = Some(v),
                    Err(e) => bad(&name, e),
                },
                "K9_DNS" => o.dns = Some(value),
                "K9_BLOCK_HOSTNAMES" => o.block_hostnames = Some(split_list(&value)),
                "K9_BLACKLIST_IPS" => o.blacklist_ips = Some(split_list(&value)),
                "K9_LOCAL_IPS" => o.local_ips = Some(split_list(&value)),
                "K9_NO_CONNECTION_REUSE" => match parse_bool(&value) {
                    Some(v) => o.no_connection_reuse = Some(v),
                    None => bad(&name, format!("expected true or false, got {value:?}")),
                },
                "K9_NO_VU_CONNECTION_REUSE" => match parse_bool(&value) {
                    Some(v) => o.no_vu_connection_reuse = Some(v),
                    None => bad(&name, format!("expected true or false, got {value:?}")),
                },
                "K9_MIN_ITERATION_DURATION" => match value.parse() {
                    Ok(v) => o.min_iteration_duration = Some(v),
                    Err(e) => bad(&name, format!("{e}")),
                },
                "K9_THROW" => match parse_bool(&value) {
                    Some(v) => o.throw = Some(v),
                    None => bad(&name, format!("expected true or false, got {value:?}")),
                },
                "K9_SYSTEM_TAGS" => o.system_tags = Some(split_list(&value)),
                "K9_NO_COOKIES_RESET" => match parse_bool(&value) {
                    Some(v) => o.no_cookies_reset = Some(v),
                    None => bad(&name, format!("expected true or false, got {value:?}")),
                },
                "K9_SUMMARY_TIME_UNIT" => o.summary_time_unit = Some(value),
                "K9_HTTP_DEBUG" => o.http_debug = Some(value),
                "K9_SUMMARY_TREND_STATS" => o.summary_trend_stats = Some(split_list(&value)),
                "K9_DISCARD_RESPONSE_BODIES" => match parse_bool(&value) {
                    Some(v) => o.discard_response_bodies = Some(v),
                    None => bad(&name, format!("expected true or false, got {value:?}")),
                },
                _ => {}
            }
        }
        if issues.is_empty() {
            Ok(o)
        } else {
            Err(InvalidPlan { issues })
        }
    }

    fn sets_execution(&self) -> bool {
        self.duration.is_some() || self.iterations.is_some() || self.stages.is_some()
    }
}

/// `avg, p(99)` → `["avg", "p(99)"]`.
pub fn split_list(value: &str) -> Vec<String> {
    value.split(',').map(|s| s.trim().to_owned()).collect()
}

fn parse_bool(value: &str) -> Option<bool> {
    match value.trim().to_ascii_lowercase().as_str() {
        "true" | "1" | "yes" | "on" => Some(true),
        "false" | "0" | "no" | "off" => Some(false),
        _ => None,
    }
}

impl PlanConfig {
    /// Applies higher-precedence overrides to this config, before resolving.
    pub fn apply(&mut self, o: &Overrides) -> Result<(), InvalidPlan> {
        let opts = &mut self.options;
        if o.sets_execution() {
            opts.scenarios.clear();
            opts.duration = o.duration;
            opts.iterations = o.iterations;
            opts.stages = o.stages.clone();
            opts.vus = o.vus.or(opts.vus);
        } else if let Some(vus) = o.vus {
            if !opts.scenarios.is_empty() {
                return Err(InvalidPlan {
                    issues: vec![Issue {
                        path: "--vus".to_owned(),
                        message: "can't replace the plan's scenarios on its own; also pass \
                                  --duration, --iterations or --stage"
                            .to_owned(),
                    }],
                });
            }
            opts.vus = Some(vus);
        }
        if let Some(v) = o.max_redirects {
            opts.max_redirects = Some(v);
        }
        if let Some(v) = o.insecure_skip_tls_verify {
            opts.insecure_skip_tls_verify = Some(v);
        }
        if let Some(v) = &o.user_agent {
            opts.user_agent = Some(v.clone());
        }
        if let Some(v) = o.discard_response_bodies {
            opts.discard_response_bodies = Some(v);
        }
        if let Some(v) = o.http_version {
            opts.http_version = Some(v);
        }
        if let Some(v) = o.batch {
            opts.batch = Some(v);
        }
        if let Some(v) = o.batch_per_host {
            opts.batch_per_host = Some(v);
        }
        if let Some(v) = &o.dns {
            opts.dns = Some(DnsOption::Text(v.clone()));
        }
        if let Some(v) = &o.block_hostnames {
            opts.block_hostnames = Some(ListOption::List(v.clone()));
        }
        if let Some(v) = &o.blacklist_ips {
            opts.blacklist_ips = Some(ListOption::List(v.clone()));
        }
        if let Some(v) = &o.local_ips {
            opts.local_ips = Some(ListOption::List(v.clone()));
        }
        if let Some(v) = o.no_connection_reuse {
            opts.no_connection_reuse = Some(v);
        }
        if let Some(v) = o.no_vu_connection_reuse {
            opts.no_vu_connection_reuse = Some(v);
        }
        if let Some(v) = &o.summary_time_unit {
            opts.summary_time_unit = Some(v.clone());
        }
        if let Some(v) = o.min_iteration_duration {
            opts.min_iteration_duration = Some(v);
        }
        if let Some(v) = o.throw {
            opts.throw = Some(v);
        }
        if let Some(v) = &o.system_tags {
            opts.system_tags = Some(ListOption::List(v.clone()));
        }
        if let Some(v) = o.no_cookies_reset {
            opts.no_cookies_reset = Some(v);
        }
        if let Some(v) = &o.http_debug {
            opts.http_debug = Some(v.clone());
        }
        if let Some(v) = &o.summary_trend_stats {
            opts.summary_trend_stats = Some(v.clone());
        }
        Ok(())
    }
}
