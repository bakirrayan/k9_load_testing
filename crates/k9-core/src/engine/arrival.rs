//! Arrival-rate executors (`constant-arrival-rate`, `ramping-arrival-rate`):
//! iterations start on a schedule, whether or not earlier ones have finished
//! (an open model). An idle VU takes each one; if none is idle, a new VU is
//! started (up to `maxVUs`); otherwise the iteration is dropped
//! (`dropped_iterations`), as in k6.
//!
//! Coordinated omission (the PDF's "dual recording"): `iteration_duration` is
//! measured from when an iteration actually started, as in k6, and
//! `iteration_duration_corrected` from when it was *due*. A dropped iteration
//! is counted there too, as if it had waited for the next VU to come free,
//! so a server stall shows up in the corrected numbers instead of vanishing.

use std::collections::VecDeque;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use crate::plan::Stage;

/// Dropped iterations waiting to be counted in the corrected metric, beyond
/// which only the count grows (bounded memory under sustained overload).
pub(crate) const MAX_OMITTED: usize = 1 << 20;

/// When each iteration is due, as offsets from the scenario's start:
/// iteration `i` starts when the cumulative number of arrivals (the integral
/// of the rate) reaches `i`, so a constant rate starts one at `t = 0`.
pub(crate) struct Arrivals {
    /// (start offset, length, rate at start, rate at end), rates per second.
    stages: Vec<(f64, f64, f64, f64)>,
    stage: usize,
    /// Arrivals before the current stage.
    before: f64,
    next: u64,
}

impl Arrivals {
    pub fn constant(rate: u64, time_unit: Duration, duration: Duration) -> Arrivals {
        let r = rate as f64 / time_unit.as_secs_f64();
        Arrivals::new(vec![(duration.as_secs_f64(), r, r)])
    }

    /// Starts at `start_rate` and moves linearly to each stage's target.
    pub fn ramping(start_rate: u64, time_unit: Duration, stages: &[Stage]) -> Arrivals {
        let unit = time_unit.as_secs_f64();
        let mut from = start_rate as f64 / unit;
        let parts = stages
            .iter()
            .map(|s| {
                let to = s.target as f64 / unit;
                let part = (s.duration.as_std().as_secs_f64(), from, to);
                from = to;
                part
            })
            .collect();
        Arrivals::new(parts)
    }

    fn new(parts: Vec<(f64, f64, f64)>) -> Arrivals {
        let mut offset = 0.0;
        let stages = parts
            .into_iter()
            .map(|(length, r0, r1)| {
                let stage = (offset, length, r0, r1);
                offset += length;
                stage
            })
            .collect();
        Arrivals {
            stages,
            stage: 0,
            before: 0.0,
            next: 0,
        }
    }
}

impl Iterator for Arrivals {
    type Item = Duration;

    fn next(&mut self) -> Option<Duration> {
        let target = self.next as f64;
        loop {
            let &(offset, length, r0, r1) = self.stages.get(self.stage)?;
            let arrivals = (r0 + r1) / 2.0 * length;
            if target < self.before + arrivals {
                // Solve r0·t + (r1 − r0)·t² / (2·length) = x for t in the stage.
                let x = target - self.before;
                let a = (r1 - r0) / (2.0 * length);
                let t = if a.abs() < 1e-12 {
                    x / r0
                } else {
                    (-r0 + (r0 * r0 + 4.0 * a * x).max(0.0).sqrt()) / (2.0 * a)
                };
                self.next += 1;
                return Some(Duration::from_secs_f64(offset + t.clamp(0.0, length)));
            }
            self.before += arrivals;
            self.stage += 1;
        }
    }
}

/// A scenario's dropped iterations, by when they were due.
#[derive(Default)]
pub(crate) struct Omitted(Mutex<VecDeque<Instant>>);

impl Omitted {
    pub fn push(&self, due: Instant) {
        let mut queue = self.0.lock().unwrap_or_else(|p| p.into_inner());
        if queue.len() < MAX_OMITTED {
            queue.push_back(due);
        }
    }

    /// The oldest dropped iteration, which the VU that just came free would
    /// have run next.
    pub fn pop(&self) -> Option<Instant> {
        self.0.lock().unwrap_or_else(|p| p.into_inner()).pop_front()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::duration::Duration as PlanDuration;

    fn ms(d: Duration) -> f64 {
        d.as_secs_f64() * 1e3
    }

    #[test]
    fn a_constant_rate_starts_evenly_from_zero() {
        let times: Vec<Duration> =
            Arrivals::constant(100, Duration::from_secs(1), Duration::from_secs(2)).collect();
        assert_eq!(times.len(), 200);
        assert_eq!(ms(times[0]), 0.0);
        assert!((ms(times[1]) - 10.0).abs() < 1e-6);
        assert!((ms(times[199]) - 1990.0).abs() < 1e-6);
    }

    #[test]
    fn the_time_unit_scales_the_rate() {
        let per_minute = Arrivals::constant(30, Duration::from_secs(60), Duration::from_secs(10));
        assert_eq!(per_minute.count(), 5);
    }

    #[test]
    fn a_ramp_follows_the_integral_of_the_rate() {
        let stage = |secs, target| Stage {
            duration: PlanDuration::from(Duration::from_secs(secs)),
            target,
        };
        let times: Vec<Duration> =
            Arrivals::ramping(0, Duration::from_secs(1), &[stage(1, 100), stage(1, 100)]).collect();
        // 50 arrivals while ramping 0 → 100/s, then 100 at 100/s.
        assert_eq!(times.len(), 150);
        assert!(times.windows(2).all(|w| w[0] <= w[1]));
        // Half the ramp's arrivals come in its last 29% (√0.5 ≈ 0.707 s).
        let before_707 = times.iter().filter(|t| ms(**t) < 707.2).count();
        assert!((25..=26).contains(&before_707), "{before_707}");
        assert!(ms(times[50]) >= 1000.0 - 1e-6);
        // Down to zero: the arrivals thin out, and none come after the end.
        let down: Vec<Duration> =
            Arrivals::ramping(100, Duration::from_secs(1), &[stage(1, 0)]).collect();
        assert_eq!(down.len(), 50);
        assert!(ms(*down.last().unwrap()) < 1000.0);
    }

    #[test]
    fn omitted_iterations_are_bounded() {
        let omitted = Omitted::default();
        let now = Instant::now();
        for _ in 0..MAX_OMITTED + 10 {
            omitted.push(now);
        }
        let mut n = 0;
        while omitted.pop().is_some() {
            n += 1;
        }
        assert_eq!(n, MAX_OMITTED);
    }
}
