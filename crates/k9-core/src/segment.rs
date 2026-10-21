//! Execution segments: how one run is divided among workers.
//!
//! A run is cut into consecutive segments by a *sequence* of points
//! `0 = p₀ < p₁ < … < pₙ = 1`, like k6's `--execution-segment-sequence`; each
//! worker runs one segment `pᵢ:pᵢ₊₁` (k6's `--execution-segment`).
//!
//! Work is divided by VU *slots* (the j-th VU of a scenario) and iterations
//! alike: over one cycle of `L` slots (`L` = the sequence's common
//! denominator), segment `i` owns `(pᵢ₊₁ − pᵢ)·L` of them, dealt out one at a
//! time to whichever segment is furthest behind its proportional share; slot
//! `j` belongs to the owner of position `j mod L`. So:
//! - every slot has exactly one owner, and a segment's share of any quantity
//!   `k` (the slots below `k` it owns) grows with `k` and sums to exactly `k`
//!   across the sequence: `shared-iterations` totals and VU counts stay exact;
//! - owners take turns, so any number of VUs is spread evenly (each share is
//!   within one of proportional);
//! - a worker can follow a ramp's global schedule, starting only the VUs in
//!   its own slots, and all workers together match it exactly.
//!
//! This is k6's scheme in spirit (its "execution tuple" striping), written
//! from its documented behaviour.

use std::fmt;
use std::str::FromStr;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

/// Longest cycle a sequence may need (its common denominator).
const MAX_CYCLE: u64 = 1 << 20;

/// A fraction in lowest terms, `0 ≤ num/den ≤ 1`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
struct Fraction {
    num: u64,
    den: u64,
}

impl Fraction {
    fn new(num: u64, den: u64) -> Option<Fraction> {
        if den == 0 || num > den {
            return None;
        }
        let g = gcd(num, den);
        Some(Fraction {
            num: num / g,
            den: den / g,
        })
    }

    fn below(self, other: Fraction) -> bool {
        u128::from(self.num) * u128::from(other.den) < u128::from(other.num) * u128::from(self.den)
    }

    fn as_f64(self) -> f64 {
        self.num as f64 / self.den as f64
    }
}

fn gcd(a: u64, b: u64) -> u64 {
    if b == 0 { a.max(1) } else { gcd(b, a % b) }
}

impl fmt::Display for Fraction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.den {
            1 => write!(f, "{}", self.num),
            _ => write!(f, "{}/{}", self.num, self.den),
        }
    }
}

impl FromStr for Fraction {
    type Err = ();

    fn from_str(s: &str) -> Result<Fraction, ()> {
        let (num, den) = s.split_once('/').unwrap_or((s, "1"));
        let num = num.trim().parse().map_err(|_| ())?;
        let den = den.trim().parse().map_err(|_| ())?;
        Fraction::new(num, den).ok_or(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SegmentError {
    #[error(
        "invalid execution segment `{0}`: the start must be below the end, both between 0 and 1"
    )]
    Segment(String),
    #[error(
        "invalid execution segment sequence `{0}`: expected increasing fractions from 0 to 1, e.g. 0,1/3,2/3,1"
    )]
    Sequence(String),
    #[error(
        "execution segment sequence `{0}` is too fine-grained (common denominator over {MAX_CYCLE})"
    )]
    TooFine(String),
}

/// One worker's part of a run: segment `index` of `sequence`.
#[derive(Clone, Serialize, Deserialize)]
#[serde(into = "SegmentSpec", try_from = "SegmentSpec")]
pub struct Segment {
    sequence: Arc<[Fraction]>,
    index: usize,
    /// Positions in `0..cycle` this segment owns, ascending.
    owned: Arc<[u64]>,
    cycle: u64,
}

/// How a segment travels (to workers): its sequence and index.
#[derive(Serialize, Deserialize)]
struct SegmentSpec {
    sequence: Vec<(u64, u64)>,
    index: usize,
}

impl From<Segment> for SegmentSpec {
    fn from(s: Segment) -> SegmentSpec {
        SegmentSpec {
            sequence: s.sequence.iter().map(|f| (f.num, f.den)).collect(),
            index: s.index,
        }
    }
}

impl TryFrom<SegmentSpec> for Segment {
    type Error = SegmentError;

    fn try_from(spec: SegmentSpec) -> Result<Segment, SegmentError> {
        let text = || format!("{:?}", spec.sequence);
        let points = spec
            .sequence
            .iter()
            .map(|&(n, d)| Fraction::new(n, d))
            .collect::<Option<Vec<_>>>()
            .ok_or_else(|| SegmentError::Sequence(text()))?;
        let mut all = build(&points, &text())?;
        if spec.index >= all.len() {
            return Err(SegmentError::Sequence(text()));
        }
        Ok(all.swap_remove(spec.index))
    }
}

/// All segments of a sequence of points (validated: 0 first, 1 last, increasing).
fn build(points: &[Fraction], text: &str) -> Result<Vec<Segment>, SegmentError> {
    let valid = points.len() >= 2
        && points[0].num == 0
        && points[points.len() - 1] == Fraction { num: 1, den: 1 }
        && points.windows(2).all(|w| w[0].below(w[1]));
    if !valid {
        return Err(SegmentError::Sequence(text.to_owned()));
    }
    let mut cycle: u64 = 1;
    for p in points {
        cycle = cycle / gcd(cycle, p.den) * p.den;
        if cycle > MAX_CYCLE {
            return Err(SegmentError::TooFine(text.to_owned()));
        }
    }
    // Each segment's positions per cycle, then dealt out one at a time to the
    // segment furthest behind its share (ties: the earlier segment).
    let weights: Vec<u64> = points
        .windows(2)
        .map(|w| w[1].num * (cycle / w[1].den) - w[0].num * (cycle / w[0].den))
        .collect();
    let mut owned: Vec<Vec<u64>> = vec![Vec::new(); weights.len()];
    for t in 0..cycle {
        let deficit = |i: usize| {
            i128::from(weights[i]) * i128::from(t + 1) - i128::from(cycle) * owned[i].len() as i128
        };
        let best = (0..weights.len())
            .filter(|&i| (owned[i].len() as u64) < weights[i])
            .max_by(|&a, &b| deficit(a).cmp(&deficit(b)).then(b.cmp(&a)))
            .expect("the weights sum to the cycle");
        owned[best].push(t);
    }
    let sequence: Arc<[Fraction]> = points.into();
    Ok(owned
        .into_iter()
        .enumerate()
        .map(|(index, owned)| Segment {
            sequence: sequence.clone(),
            index,
            owned: owned.into(),
            cycle,
        })
        .collect())
}

impl Segment {
    /// The sequence's points as (numerator, denominator), as they travel to
    /// workers with [`Segment::index`].
    pub fn points(&self) -> Vec<(u64, u64)> {
        self.sequence.iter().map(|f| (f.num, f.den)).collect()
    }

    /// Which segment of the sequence this is (0 = the first).
    pub fn index(&self) -> usize {
        self.index
    }

    /// Segment `index` of the sequence `points` (see [`Segment::points`]).
    pub fn from_points(points: Vec<(u64, u64)>, index: usize) -> Result<Segment, SegmentError> {
        Segment::try_from(SegmentSpec {
            sequence: points,
            index,
        })
    }

    /// The whole run.
    pub fn full() -> Segment {
        Segment::split(1).remove(0)
    }

    /// `n` equal consecutive segments covering the whole run.
    pub fn split(n: u32) -> Vec<Segment> {
        let n = u64::from(n.max(1));
        let points: Vec<Fraction> = (0..=n)
            .map(|i| Fraction::new(i, n).expect("i <= n"))
            .collect();
        build(&points, "").expect("equal parts are a valid sequence")
    }

    /// Parts in proportion to `weights` (a zero weight counts as 1), e.g.
    /// `[1, 1, 2]` → `0:1/4`, `1/4:1/2`, `1/2:1`.
    pub fn weighted(weights: &[u64]) -> Vec<Segment> {
        if weights.is_empty() {
            return vec![Segment::full()];
        }
        let weights: Vec<u64> = weights.iter().map(|w| (*w).max(1)).collect();
        let total: u64 = weights.iter().sum::<u64>().max(1);
        let mut sum = 0;
        let mut points = vec![Fraction::new(0, 1).expect("0 is a fraction")];
        for w in &weights {
            sum += w;
            points.push(Fraction::new(sum, total).expect("sum <= total"));
        }
        build(&points, "").expect("weighted parts are a valid sequence")
    }

    /// Every segment of a sequence like `0,1/4,1/2,1`.
    pub fn sequence(text: &str) -> Result<Vec<Segment>, SegmentError> {
        let points = text
            .split(',')
            .map(str::parse)
            .collect::<Result<Vec<Fraction>, ()>>()
            .map_err(|()| SegmentError::Sequence(text.to_owned()))?;
        build(&points, text)
    }

    /// This segment's share of `k`: how many of slots `0..k` it owns.
    pub fn share(&self, k: u64) -> u64 {
        let per_cycle = self.owned.len() as u64;
        let partial = self.owned.partition_point(|&p| p < k % self.cycle) as u64;
        k / self.cycle * per_cycle + partial
    }

    /// This segment's part of `total` units (iterations) spread over `slots`
    /// VU slots: slot `j` carries `total / slots` of them, plus one for the
    /// first `total % slots` slots. Zero when the segment owns none of the
    /// slots, so every unit lands with a VU that can run it.
    pub fn share_per_slot(&self, total: u64, slots: u64) -> u64 {
        if slots == 0 {
            return 0;
        }
        total / slots * self.share(slots) + self.share(total % slots)
    }

    /// Whether VU slot `slot` (0-based, in the whole scenario) is this segment's.
    pub fn owns(&self, slot: u64) -> bool {
        self.owned.binary_search(&(slot % self.cycle)).is_ok()
    }

    /// The global slot of this segment's `local`-th VU (0-based).
    pub fn slot(&self, local: u64) -> u64 {
        let per_cycle = self.owned.len() as u64;
        local / per_cycle * self.cycle + self.owned[(local % per_cycle) as usize]
    }

    /// The segment's width, e.g. 0.25 for `1/4:1/2`.
    pub fn fraction(&self) -> f64 {
        self.to().as_f64() - self.from().as_f64()
    }

    pub fn is_full(&self) -> bool {
        self.sequence.len() == 2
    }

    fn from(&self) -> Fraction {
        self.sequence[self.index]
    }

    fn to(&self) -> Fraction {
        self.sequence[self.index + 1]
    }
}

impl Default for Segment {
    fn default() -> Segment {
        Segment::full()
    }
}

impl PartialEq for Segment {
    fn eq(&self, other: &Segment) -> bool {
        self.index == other.index && self.sequence == other.sequence
    }
}

impl Eq for Segment {}

impl fmt::Debug for Segment {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let sequence: Vec<String> = self.sequence.iter().map(ToString::to_string).collect();
        write!(f, "Segment({self} of {})", sequence.join(","))
    }
}

impl fmt::Display for Segment {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.from(), self.to())
    }
}

impl FromStr for Segment {
    /// A segment `a:b` on its own, in the sequence `0,a,b,1` (k6's default).
    type Err = SegmentError;

    fn from_str(s: &str) -> Result<Segment, SegmentError> {
        let invalid = || SegmentError::Segment(s.to_owned());
        let (from, to) = s.split_once(':').ok_or_else(invalid)?;
        let from: Fraction = from.parse().map_err(|()| invalid())?;
        let to: Fraction = to.parse().map_err(|()| invalid())?;
        if !from.below(to) {
            return Err(invalid());
        }
        let mut points = vec![Fraction { num: 0, den: 1 }];
        points.extend(
            [from, to]
                .into_iter()
                .filter(|p| p.num != 0 && p.num != p.den),
        );
        points.push(Fraction { num: 1, den: 1 });
        let index = usize::from(from.num != 0);
        let mut all = build(&points, s)?;
        Ok(all.swap_remove(index))
    }
}
