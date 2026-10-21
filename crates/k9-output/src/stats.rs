//! k6's `summaryTrendStats`: which values the summary shows for trends.

use std::fmt;

use k9_metrics::TrendValue;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum TrendStat {
    Avg,
    Min,
    Med,
    Max,
    Count,
    /// `p(N)`, with 0 ≤ N ≤ 100.
    Percentile(f64),
}

impl TrendStat {
    /// k6's default: `avg, min, med, max, p(90), p(95)`.
    pub const DEFAULT: [TrendStat; 6] = [
        TrendStat::Avg,
        TrendStat::Min,
        TrendStat::Med,
        TrendStat::Max,
        TrendStat::Percentile(90.0),
        TrendStat::Percentile(95.0),
    ];

    pub fn parse(text: &str) -> Result<TrendStat, String> {
        let text = text.trim();
        Ok(match text {
            "avg" => TrendStat::Avg,
            "min" => TrendStat::Min,
            "med" => TrendStat::Med,
            "max" => TrendStat::Max,
            "count" => TrendStat::Count,
            _ => {
                let p = text
                    .strip_prefix("p(")
                    .and_then(|rest| rest.strip_suffix(')'))
                    .and_then(|n| n.trim().parse::<f64>().ok())
                    .filter(|p| (0.0..=100.0).contains(p))
                    .ok_or_else(|| {
                        format!(
                            "unknown trend stat `{text}` (use avg, min, med, max, count or p(N) with 0 ≤ N ≤ 100)"
                        )
                    })?;
                TrendStat::Percentile(p)
            }
        })
    }

    pub fn value(&self, t: &TrendValue) -> f64 {
        match self {
            TrendStat::Avg => t.avg(),
            TrendStat::Min => t.min,
            TrendStat::Med => t.med(),
            TrendStat::Max => t.max,
            TrendStat::Count => t.count as f64,
            TrendStat::Percentile(p) => t.percentile(*p),
        }
    }
}

impl fmt::Display for TrendStat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TrendStat::Avg => f.write_str("avg"),
            TrendStat::Min => f.write_str("min"),
            TrendStat::Med => f.write_str("med"),
            TrendStat::Max => f.write_str("max"),
            TrendStat::Count => f.write_str("count"),
            TrendStat::Percentile(p) => write!(f, "p({p})"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_and_prints_k6_names() {
        for name in [
            "avg", "min", "med", "max", "count", "p(90)", "p(99.9)", "p(0)", "p(100)",
        ] {
            assert_eq!(TrendStat::parse(name).unwrap().to_string(), name);
        }
        assert_eq!(
            TrendStat::parse(" p( 99 ) ").unwrap(),
            TrendStat::Percentile(99.0)
        );
        for bad in ["p(101)", "p(-1)", "p()", "p95", "mean", ""] {
            assert!(TrendStat::parse(bad).is_err(), "{bad}");
        }
    }
}
