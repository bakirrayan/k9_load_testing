//! k6-style durations: `"30s"`, `"1m30s"`, `"1.5h"`, `"250ms"`.
//!
//! As in k6, a bare number is read as milliseconds. Durations serialize back to
//! their canonical string form, so `k9 inspect` output can be fed back in.

use std::fmt;
use std::str::FromStr;
use std::time::Duration as StdDuration;

use serde::de::{self, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Duration(pub StdDuration);

impl Duration {
    pub const ZERO: Duration = Duration(StdDuration::ZERO);

    pub const fn from_secs(secs: u64) -> Self {
        Duration(StdDuration::from_secs(secs))
    }

    pub const fn from_millis(millis: u64) -> Self {
        Duration(StdDuration::from_millis(millis))
    }

    pub const fn as_std(self) -> StdDuration {
        self.0
    }

    pub const fn is_zero(self) -> bool {
        self.0.is_zero()
    }
}

impl std::ops::Add for Duration {
    type Output = Duration;

    fn add(self, rhs: Duration) -> Duration {
        Duration(self.0.saturating_add(rhs.0))
    }
}

impl std::iter::Sum for Duration {
    fn sum<I: Iterator<Item = Duration>>(iter: I) -> Duration {
        iter.fold(Duration::ZERO, |a, b| a + b)
    }
}

impl From<StdDuration> for Duration {
    fn from(d: StdDuration) -> Self {
        Duration(d)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("invalid duration {input:?}: {reason} (expected e.g. \"30s\", \"1m30s\", \"250ms\")")]
pub struct ParseDurationError {
    input: String,
    reason: &'static str,
}

impl FromStr for Duration {
    type Err = ParseDurationError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let err = |reason| ParseDurationError {
            input: input.to_owned(),
            reason,
        };
        let s = input.trim();
        if s.is_empty() {
            return Err(err("empty string"));
        }
        if let Ok(ms) = s.parse::<f64>() {
            return nanos_to_duration(ms * 1e6).ok_or_else(|| err("out of range"));
        }

        let mut total_nanos = 0f64;
        let mut rest = s;
        while !rest.is_empty() {
            let num_len = rest
                .find(|c: char| !(c.is_ascii_digit() || c == '.'))
                .ok_or_else(|| err("missing unit"))?;
            if num_len == 0 {
                return Err(err("expected a number"));
            }
            let value: f64 = rest[..num_len].parse().map_err(|_| err("bad number"))?;
            rest = &rest[num_len..];

            let unit_len = rest
                .find(|c: char| c.is_ascii_digit() || c == '.')
                .unwrap_or(rest.len());
            let nanos_per_unit = match &rest[..unit_len] {
                "ns" => 1.0,
                "us" | "µs" => 1e3,
                "ms" => 1e6,
                "s" => 1e9,
                "m" => 60e9,
                "h" => 3600e9,
                "d" => 86_400e9,
                _ => return Err(err("unknown unit (use ns, us, ms, s, m, h or d)")),
            };
            rest = &rest[unit_len..];
            total_nanos += value * nanos_per_unit;
        }
        nanos_to_duration(total_nanos).ok_or_else(|| err("out of range"))
    }
}

fn nanos_to_duration(nanos: f64) -> Option<Duration> {
    if !nanos.is_finite() || nanos < 0.0 || nanos > u64::MAX as f64 {
        return None;
    }
    Some(Duration(StdDuration::from_nanos(nanos.round() as u64)))
}

impl fmt::Display for Duration {
    /// Canonical form: largest units first, e.g. `1h2m3s`, `1m30.5s`, `250ms`, `0s`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let total = self.0.as_nanos();
        if total == 0 {
            return f.write_str("0s");
        }
        if total < 1_000_000_000 {
            return if total.is_multiple_of(1_000_000) {
                write!(f, "{}ms", total / 1_000_000)
            } else if total.is_multiple_of(1_000) {
                write!(f, "{}us", total / 1_000)
            } else {
                write!(f, "{total}ns")
            };
        }
        let secs = self.0.as_secs();
        let (h, m, s) = (secs / 3600, (secs / 60) % 60, secs % 60);
        if h > 0 {
            write!(f, "{h}h")?;
        }
        if m > 0 {
            write!(f, "{m}m")?;
        }
        let sub = self.0.subsec_nanos();
        if sub > 0 {
            let frac = format!("{:09}", sub);
            write!(f, "{s}.{}s", frac.trim_end_matches('0'))
        } else if s > 0 {
            write!(f, "{s}s")
        } else {
            Ok(())
        }
    }
}

impl Serialize for Duration {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for Duration {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct DurationVisitor;

        impl Visitor<'_> for DurationVisitor {
            type Value = Duration;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a duration string like \"30s\" or a number of milliseconds")
            }

            fn visit_str<E: de::Error>(self, v: &str) -> Result<Duration, E> {
                v.parse().map_err(E::custom)
            }

            fn visit_u64<E: de::Error>(self, v: u64) -> Result<Duration, E> {
                Ok(Duration::from_millis(v))
            }

            fn visit_i64<E: de::Error>(self, v: i64) -> Result<Duration, E> {
                u64::try_from(v)
                    .map(Duration::from_millis)
                    .map_err(|_| E::custom("duration cannot be negative"))
            }

            fn visit_f64<E: de::Error>(self, v: f64) -> Result<Duration, E> {
                nanos_to_duration(v * 1e6).ok_or_else(|| E::custom("duration out of range"))
            }
        }

        deserializer.deserialize_any(DurationVisitor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn d(s: &str) -> Duration {
        s.parse().unwrap()
    }

    #[test]
    fn parses_k6_forms() {
        assert_eq!(d("30s"), Duration::from_secs(30));
        assert_eq!(d("1m30s"), Duration::from_secs(90));
        assert_eq!(d("1h"), Duration::from_secs(3600));
        assert_eq!(d("250ms"), Duration::from_millis(250));
        assert_eq!(d("1.5s"), Duration::from_millis(1500));
        assert_eq!(d("2h1m1s"), Duration::from_secs(7261));
        assert_eq!(d("10us"), Duration(StdDuration::from_micros(10)));
        assert_eq!(d("0"), Duration::ZERO);
        assert_eq!(
            d("1500"),
            Duration::from_millis(1500),
            "bare numbers are ms"
        );
    }

    #[test]
    fn rejects_garbage() {
        for bad in ["", "s", "10x", "-5s", "-5", "1..5s", "abc", "5s3"] {
            assert!(bad.parse::<Duration>().is_err(), "{bad:?} should fail");
        }
    }

    #[test]
    fn display_is_canonical_and_round_trips() {
        for (input, canonical) in [
            ("90s", "1m30s"),
            ("1h", "1h"),
            ("3661s", "1h1m1s"),
            ("250ms", "250ms"),
            ("1.5s", "1.5s"),
            ("0s", "0s"),
            ("2m", "2m"),
        ] {
            let parsed = d(input);
            assert_eq!(parsed.to_string(), canonical);
            assert_eq!(d(canonical), parsed);
        }
    }

    #[test]
    fn deserializes_strings_and_millis() {
        let from_str: Duration = serde_json::from_str("\"2m\"").unwrap();
        let from_num: Duration = serde_json::from_str("120000").unwrap();
        assert_eq!(from_str, from_num);
        assert!(serde_json::from_str::<Duration>("-1").is_err());
    }
}
