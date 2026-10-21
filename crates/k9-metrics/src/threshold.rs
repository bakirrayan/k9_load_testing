//! k6's threshold language: `p(95)<500`, `rate<0.01`, `count>100` on metrics
//! selected like `http_req_duration{scenario:checkout}`.

use std::fmt;
use std::str::FromStr;
use std::time::Duration;

use crate::registry::MetricKind;
use crate::value::Value;

/// A metric plus an optional tag filter: `name` or `name{key:value,...}`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Selector {
    pub metric: String,
    pub tags: Vec<(String, String)>,
}

/// One threshold expression, e.g. `p(95)<500`.
#[derive(Debug, Clone, PartialEq)]
pub struct Expr {
    pub aggregation: Aggregation,
    pub op: Op,
    pub value: f64,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Aggregation {
    Avg,
    Min,
    Max,
    Med,
    /// `p(N)`, N in 0–100.
    Percentile(f64),
    /// Counter total.
    Count,
    /// Counter per second, or a Rate metric's share of non-zero samples.
    Rate,
    /// Gauge's latest value.
    Value,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Op {
    Lt,
    Le,
    Gt,
    Ge,
    Eq,
    Ne,
}

impl FromStr for Selector {
    type Err = String;

    fn from_str(s: &str) -> Result<Selector, String> {
        let s = s.trim();
        let bad = |why: &str| format!("invalid metric selector {s:?}: {why}");
        let (metric, tags) = match s.split_once('{') {
            None => (s, Vec::new()),
            Some((metric, rest)) => {
                let inner = rest
                    .strip_suffix('}')
                    .ok_or_else(|| bad("missing closing `}`"))?;
                let mut tags = Vec::new();
                for pair in inner.split(',') {
                    let (key, value) = pair
                        .split_once(':')
                        .ok_or_else(|| bad("tag filters look like {key:value}"))?;
                    let key = key.trim();
                    if key.is_empty() {
                        return Err(bad("empty tag name"));
                    }
                    tags.push((key.to_owned(), value.trim().to_owned()));
                }
                (metric.trim(), tags)
            }
        };
        if metric.is_empty() || metric.contains(char::is_whitespace) {
            return Err(bad("expected a metric name"));
        }
        Ok(Selector {
            metric: metric.to_owned(),
            tags,
        })
    }
}

impl FromStr for Expr {
    type Err = String;

    fn from_str(s: &str) -> Result<Expr, String> {
        let s = s.trim();
        let bad = |why: &str| {
            format!(
                "invalid threshold {s:?}: {why} (expected e.g. `p(95)<500`, `rate<0.01`, `count>100`)"
            )
        };
        let at = s
            .find(['<', '>', '=', '!'])
            .ok_or_else(|| bad("missing comparison operator"))?;
        let aggregation = parse_aggregation(s[..at].trim()).map_err(|why| bad(&why))?;
        let rest = &s[at..];
        let (op, len) = [
            ("===", Op::Eq),
            ("<=", Op::Le),
            (">=", Op::Ge),
            ("==", Op::Eq),
            ("!=", Op::Ne),
            ("<", Op::Lt),
            (">", Op::Gt),
        ]
        .into_iter()
        .find(|(text, _)| rest.starts_with(text))
        .map(|(text, op)| (op, text.len()))
        .ok_or_else(|| bad("unknown operator"))?;
        let value: f64 = rest[len..]
            .trim()
            .parse()
            .map_err(|_| bad("expected a number after the operator"))?;
        if !value.is_finite() {
            return Err(bad("the threshold value must be finite"));
        }
        Ok(Expr {
            aggregation,
            op,
            value,
        })
    }
}

impl Expr {
    /// Whether this aggregation applies to a metric of `kind`; the error lists
    /// the ones that do.
    pub fn check_kind(&self, kind: MetricKind) -> Result<(), String> {
        use Aggregation::*;
        let ok = match kind {
            MetricKind::Trend => matches!(self.aggregation, Avg | Min | Max | Med | Percentile(_)),
            MetricKind::Counter => matches!(self.aggregation, Count | Rate),
            MetricKind::Gauge => matches!(self.aggregation, Value),
            MetricKind::Rate => matches!(self.aggregation, Rate),
        };
        if ok {
            return Ok(());
        }
        let valid = match kind {
            MetricKind::Trend => "avg, min, max, med, p(N)",
            MetricKind::Counter => "count, rate",
            MetricKind::Gauge => "value",
            MetricKind::Rate => "rate",
        };
        Err(format!(
            "`{}` doesn't apply to a {kind} metric (use {valid})",
            aggregation_name(self.aggregation)
        ))
    }

    /// The observed number this expression compares, from a metric's value
    /// (`None` = no samples, which counts as 0) after `elapsed` of the run.
    pub fn observe(&self, value: Option<&Value>, elapsed: Duration) -> f64 {
        let Some(value) = value else {
            return 0.0;
        };
        match (self.aggregation, value) {
            (Aggregation::Avg, Value::Trend(t)) => t.avg(),
            (Aggregation::Min, Value::Trend(t)) => t.min,
            (Aggregation::Max, Value::Trend(t)) => t.max,
            (Aggregation::Med, Value::Trend(t)) => t.med(),
            (Aggregation::Percentile(p), Value::Trend(t)) => t.percentile(p),
            (Aggregation::Count, Value::Counter(c)) => c.sum,
            (Aggregation::Rate, Value::Counter(c)) => {
                let secs = elapsed.as_secs_f64();
                if secs > 0.0 { c.sum / secs } else { 0.0 }
            }
            (Aggregation::Rate, Value::Rate(r)) => r.rate(),
            (Aggregation::Value, Value::Gauge(g)) => g.value,
            // Mismatches are rejected by `check_kind` before a run.
            _ => 0.0,
        }
    }

    pub fn passes(&self, observed: f64) -> bool {
        match self.op {
            Op::Lt => observed < self.value,
            Op::Le => observed <= self.value,
            Op::Gt => observed > self.value,
            Op::Ge => observed >= self.value,
            Op::Eq => observed == self.value,
            Op::Ne => observed != self.value,
        }
    }
}

impl fmt::Display for Expr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let op = match self.op {
            Op::Lt => "<",
            Op::Le => "<=",
            Op::Gt => ">",
            Op::Ge => ">=",
            Op::Eq => "==",
            Op::Ne => "!=",
        };
        write!(
            f,
            "{}{op}{}",
            aggregation_name(self.aggregation),
            self.value
        )
    }
}

impl fmt::Display for Selector {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.metric)?;
        if !self.tags.is_empty() {
            let tags: Vec<String> = self.tags.iter().map(|(k, v)| format!("{k}:{v}")).collect();
            write!(f, "{{{}}}", tags.join(","))?;
        }
        Ok(())
    }
}

fn parse_aggregation(s: &str) -> Result<Aggregation, String> {
    Ok(match s {
        "avg" => Aggregation::Avg,
        "min" => Aggregation::Min,
        "max" => Aggregation::Max,
        "med" => Aggregation::Med,
        "count" => Aggregation::Count,
        "rate" => Aggregation::Rate,
        "value" => Aggregation::Value,
        _ => {
            let inner = s
                .strip_prefix("p(")
                .and_then(|rest| rest.strip_suffix(')'))
                .ok_or_else(|| {
                    format!("unknown aggregation `{s}` (use avg, min, max, med, p(N), count, rate or value)")
                })?;
            let p: f64 = inner
                .trim()
                .parse()
                .map_err(|_| format!("`{s}`: expected a percentile like p(95)"))?;
            if !(0.0..=100.0).contains(&p) {
                return Err(format!("`{s}`: percentiles go from 0 to 100"));
            }
            Aggregation::Percentile(p)
        }
    })
}

fn aggregation_name(a: Aggregation) -> String {
    match a {
        Aggregation::Avg => "avg".into(),
        Aggregation::Min => "min".into(),
        Aggregation::Max => "max".into(),
        Aggregation::Med => "med".into(),
        Aggregation::Percentile(p) => format!("p({p})"),
        Aggregation::Count => "count".into(),
        Aggregation::Rate => "rate".into(),
        Aggregation::Value => "value".into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::value::{CounterValue, RateValue, TrendValue};

    fn expr(s: &str) -> Expr {
        s.parse().unwrap_or_else(|e| panic!("{s}: {e}"))
    }

    #[test]
    fn parses_k6_expressions() {
        assert_eq!(
            expr("p(95)<500"),
            Expr {
                aggregation: Aggregation::Percentile(95.0),
                op: Op::Lt,
                value: 500.0
            }
        );
        assert_eq!(
            expr(" p(99.9) <= 1.5 "),
            Expr {
                aggregation: Aggregation::Percentile(99.9),
                op: Op::Le,
                value: 1.5
            }
        );
        assert_eq!(expr("rate<0.01").aggregation, Aggregation::Rate);
        assert_eq!(expr("count>100").op, Op::Gt);
        assert_eq!(expr("avg>=200").op, Op::Ge);
        assert_eq!(expr("value===3").op, Op::Eq);
        assert_eq!(expr("med==3").op, Op::Eq);
        assert_eq!(expr("max!=0").op, Op::Ne);
        assert_eq!(expr("min > -5").value, -5.0);
    }

    #[test]
    fn rejects_malformed_expressions() {
        for bad in [
            "", "p95<500", "p(101)<1", "avg", "avg<", "avg<x", "mean<3", "avg<>3", "p(x)<1",
        ] {
            assert!(bad.parse::<Expr>().is_err(), "{bad:?} should fail");
        }
    }

    #[test]
    fn display_round_trips() {
        for s in [
            "p(95)<500",
            "rate<0.01",
            "count>=100",
            "value!=3",
            "p(99.9)<=1.5",
        ] {
            assert_eq!(expr(s).to_string(), s);
        }
    }

    #[test]
    fn parses_selectors_with_tag_filters() {
        let s: Selector = "http_req_duration".parse().unwrap();
        assert_eq!((s.metric.as_str(), s.tags.len()), ("http_req_duration", 0));
        let s: Selector = "http_req_duration{scenario:checkout, status:200}"
            .parse()
            .unwrap();
        assert_eq!(
            s.tags,
            [
                ("scenario".into(), "checkout".into()),
                ("status".into(), "200".into())
            ]
        );
        assert_eq!(
            s.to_string(),
            "http_req_duration{scenario:checkout,status:200}"
        );
        let url: Selector = "http_reqs{url:http://x/y}".parse().unwrap();
        assert_eq!(url.tags[0].1, "http://x/y", "only the first ':' splits");
        for bad in ["", "{a:b}", "m{a}", "m{a:b", "m{:b}"] {
            assert!(bad.parse::<Selector>().is_err(), "{bad:?}");
        }
    }

    #[test]
    fn aggregations_must_suit_the_metric_kind() {
        assert!(expr("p(95)<1").check_kind(MetricKind::Trend).is_ok());
        assert!(expr("count>1").check_kind(MetricKind::Counter).is_ok());
        assert!(expr("rate>1").check_kind(MetricKind::Counter).is_ok());
        assert!(expr("rate<0.1").check_kind(MetricKind::Rate).is_ok());
        assert!(expr("value<3").check_kind(MetricKind::Gauge).is_ok());
        let err = expr("p(95)<1").check_kind(MetricKind::Rate).unwrap_err();
        assert!(err.contains("rate"), "{err}");
        assert!(expr("avg<1").check_kind(MetricKind::Counter).is_err());
    }

    #[test]
    fn observes_values() {
        let trend = Value::Trend(TrendValue::from_samples(&[10.0, 20.0, 30.0, 40.0]));
        let secs = Duration::from_secs(2);
        assert_eq!(expr("avg<1").observe(Some(&trend), secs), 25.0);
        assert_eq!(expr("max<1").observe(Some(&trend), secs), 40.0);
        let counter = Value::Counter(CounterValue { sum: 10.0 });
        assert_eq!(expr("count<1").observe(Some(&counter), secs), 10.0);
        assert_eq!(
            expr("rate<1").observe(Some(&counter), secs),
            5.0,
            "per second"
        );
        let rate = Value::Rate(RateValue { trues: 1, total: 4 });
        assert_eq!(expr("rate<1").observe(Some(&rate), secs), 0.25);
        assert_eq!(
            expr("p(95)<1").observe(None, secs),
            0.0,
            "no samples counts as 0"
        );
    }

    #[test]
    fn compares_with_each_operator() {
        assert!(expr("avg<5").passes(4.0) && !expr("avg<5").passes(5.0));
        assert!(expr("avg<=5").passes(5.0));
        assert!(expr("avg>5").passes(6.0) && !expr("avg>5").passes(5.0));
        assert!(expr("avg>=5").passes(5.0));
        assert!(expr("avg==5").passes(5.0) && !expr("avg==5").passes(5.1));
        assert!(expr("avg!=5").passes(5.1));
    }
}
