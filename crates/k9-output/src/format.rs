//! Number formatting in k6's summary style.

/// A time value given in milliseconds: `0s`, `3.51µs`, `101.92ms`, `1.5s`, `1m5s`.
pub fn duration_ms(ms: f64) -> String {
    if !ms.is_finite() {
        return ms.to_string();
    }
    if ms < 0.0 {
        return format!("-{}", duration_ms(-ms));
    }
    let ns = ms * 1e6;
    if ns < 0.5 {
        "0s".to_owned()
    } else if ms < 0.001 {
        format!("{}ns", fixed2(ns))
    } else if ms < 1.0 {
        format!("{}µs", fixed2(ms * 1e3))
    } else if ms < 1e3 {
        format!("{}ms", fixed2(ms))
    } else if ms < 60e3 {
        format!("{}s", fixed2(ms / 1e3))
    } else {
        let total_s = ms / 1e3;
        let h = (total_s / 3600.0).floor();
        let m = ((total_s - h * 3600.0) / 60.0).floor();
        let s = total_s - h * 3600.0 - m * 60.0;
        let mut out = String::new();
        if h > 0.0 {
            out.push_str(&format!("{h}h"));
        }
        out.push_str(&format!("{m}m{}s", fixed2(s)));
        out
    }
}

/// k6's `summaryTimeUnit`: every time in the summary in one unit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimeUnit {
    S,
    Ms,
    Us,
}

impl TimeUnit {
    pub fn parse(s: &str) -> Result<TimeUnit, String> {
        match s {
            "s" => Ok(TimeUnit::S),
            "ms" => Ok(TimeUnit::Ms),
            "us" => Ok(TimeUnit::Us),
            _ => Err(format!("unknown time unit `{s}` (expected s, ms or us)")),
        }
    }
}

impl std::fmt::Display for TimeUnit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            TimeUnit::S => "s",
            TimeUnit::Ms => "ms",
            TimeUnit::Us => "us",
        })
    }
}

/// A time value given in milliseconds, in `unit` (`1.5s`, `0.25ms`,
/// `1500µs`), or as [`duration_ms`] picks without one.
pub fn duration_in(ms: f64, unit: Option<TimeUnit>) -> String {
    match unit {
        None => duration_ms(ms),
        Some(TimeUnit::S) => format!("{}s", fixed2(ms / 1e3)),
        Some(TimeUnit::Ms) => format!("{}ms", fixed2(ms)),
        Some(TimeUnit::Us) => format!("{}µs", fixed2(ms * 1e3)),
    }
}

/// A byte count with SI units: `830 B`, `1.2 kB`, `76 kB`, `2.3 MB`.
pub fn bytes(value: f64) -> String {
    const UNITS: [&str; 6] = ["B", "kB", "MB", "GB", "TB", "PB"];
    if value.abs() < 1000.0 {
        return format!("{value:.0} B");
    }
    let exp = ((value.abs().log10() / 3.0).floor() as usize).min(UNITS.len() - 1);
    let scaled = value / 1000f64.powi(exp as i32);
    if scaled.abs() < 10.0 {
        format!("{scaled:.1} {}", UNITS[exp])
    } else {
        format!("{scaled:.0} {}", UNITS[exp])
    }
}

/// A plain number: integers without decimals, otherwise up to 6 decimals.
pub fn number(value: f64) -> String {
    if value.fract() == 0.0 && value.abs() < 1e15 {
        format!("{value:.0}")
    } else {
        trim_zeros(format!("{value:.6}"))
    }
}

/// RFC 3339 in UTC with nanoseconds: `2023-11-14T22:13:20.500000000Z`.
pub fn rfc3339(time: std::time::SystemTime) -> String {
    let since = time
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    let secs = since.as_secs();
    let (days, rem) = (secs / 86_400, secs % 86_400);
    let (year, month, day) = civil_from_days(days as i64);
    format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}.{:09}Z",
        rem / 3600,
        (rem / 60) % 60,
        rem % 60,
        since.subsec_nanos()
    )
}

/// Days since 1970-01-01 to (year, month, day), per Howard Hinnant's
/// `civil_from_days` (proleptic Gregorian calendar).
fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let month = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    let year = yoe + era * 400 + i64::from(month <= 2);
    (year, month, day)
}

/// A share in `0..=1` as a percentage with two decimals: `12.50%`.
pub fn percent(share: f64) -> String {
    format!("{:.2}%", share * 100.0)
}

/// Two decimals, trailing zeros dropped: `101.92`, `1.5`, `100`.
fn fixed2(value: f64) -> String {
    trim_zeros(format!("{value:.2}"))
}

fn trim_zeros(s: String) -> String {
    if s.contains('.') {
        s.trim_end_matches('0').trim_end_matches('.').to_owned()
    } else {
        s
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_time_units() {
        assert_eq!(duration_in(1500.0, Some(TimeUnit::S)), "1.5s");
        assert_eq!(duration_in(0.25, Some(TimeUnit::Ms)), "0.25ms");
        assert_eq!(duration_in(1.5, Some(TimeUnit::Us)), "1500µs");
        assert_eq!(duration_in(1500.0, None), "1.5s");
        assert_eq!(TimeUnit::parse("us"), Ok(TimeUnit::Us));
        assert!(TimeUnit::parse("h").is_err());
        assert_eq!(TimeUnit::Ms.to_string(), "ms");
    }

    #[test]
    fn durations_pick_a_unit_like_k6() {
        assert_eq!(duration_ms(0.0), "0s");
        assert_eq!(duration_ms(0.00351), "3.51µs");
        assert_eq!(duration_ms(0.5), "500µs");
        assert_eq!(duration_ms(1.2345), "1.23ms");
        assert_eq!(duration_ms(101.92), "101.92ms");
        assert_eq!(duration_ms(100.0), "100ms");
        assert_eq!(duration_ms(1500.0), "1.5s");
        assert_eq!(duration_ms(65_000.0), "1m5s");
        assert_eq!(duration_ms(3_723_500.0), "1h2m3.5s");
        assert_eq!(duration_ms(0.0000004), "0s", "sub-nanosecond rounds to 0s");
    }

    #[test]
    fn bytes_use_si_units() {
        assert_eq!(bytes(0.0), "0 B");
        assert_eq!(bytes(830.0), "830 B");
        assert_eq!(bytes(1234.0), "1.2 kB");
        assert_eq!(bytes(76_000.0), "76 kB");
        assert_eq!(bytes(2_300_000.0), "2.3 MB");
        assert_eq!(bytes(1e9), "1.0 GB");
    }

    #[test]
    fn timestamps_are_rfc3339_utc() {
        use std::time::{Duration, UNIX_EPOCH};
        assert_eq!(rfc3339(UNIX_EPOCH), "1970-01-01T00:00:00.000000000Z");
        assert_eq!(
            rfc3339(UNIX_EPOCH + Duration::new(1_700_000_000, 500_000_000)),
            "2023-11-14T22:13:20.500000000Z"
        );
        // 700 ns: Windows' SystemTime counts in units of 100 ns.
        assert_eq!(
            rfc3339(UNIX_EPOCH + Duration::new(951_782_400, 700)),
            "2000-02-29T00:00:00.000000700Z",
            "leap day"
        );
        assert_eq!(
            rfc3339(UNIX_EPOCH + Duration::from_secs(4_102_444_799)),
            "2099-12-31T23:59:59.000000000Z"
        );
    }

    #[test]
    fn numbers_and_percentages() {
        assert_eq!(number(300.0), "300");
        assert_eq!(number(29.636791234), "29.636791");
        assert_eq!(number(9.5), "9.5");
        assert_eq!(percent(0.0), "0.00%");
        assert_eq!(percent(0.125), "12.50%");
        assert_eq!(percent(1.0), "100.00%");
    }
}
