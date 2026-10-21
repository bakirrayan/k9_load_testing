//! The `ramping-vus` schedule: when the VU count changes, and to what.

use std::time::Duration;

use crate::plan::Stage;

/// k6's step schedule: starts at `start_vus`, and across a stage from `a` to
/// `b` VUs lasting `d`, changes by one VU at each `d * i / |b - a|` into the
/// stage (`i = 1..=|b - a|`). A zero-length stage jumps straight to its target.
/// Returns `(offset from scenario start, VU count)` pairs in time order.
pub(crate) fn steps(start_vus: u32, stages: &[Stage]) -> Vec<(Duration, u32)> {
    let mut out = vec![(Duration::ZERO, start_vus)];
    let mut stage_start = Duration::ZERO;
    let mut from = i64::from(start_vus);
    for stage in stages {
        let length = stage.duration.as_std();
        let to = i64::try_from(stage.target).unwrap_or(i64::from(u32::MAX));
        let diff = to - from;
        if diff != 0 {
            if length.is_zero() {
                out.push((stage_start, to as u32));
            } else {
                let n = diff.unsigned_abs();
                for i in 1..=n {
                    let offset = length.as_nanos() * u128::from(i) / u128::from(n);
                    let vus = from + diff.signum() * i as i64;
                    out.push((
                        stage_start + Duration::from_nanos(offset as u64),
                        vus as u32,
                    ));
                }
            }
        }
        stage_start += length;
        from = to;
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stage(secs: u64, target: u64) -> Stage {
        Stage {
            duration: crate::Duration::from_secs(secs),
            target,
        }
    }

    fn s(secs: f64) -> Duration {
        Duration::from_secs_f64(secs)
    }

    #[test]
    fn ramps_up_one_vu_per_step() {
        assert_eq!(
            steps(0, &[stage(4, 4)]),
            [
                (s(0.0), 0),
                (s(1.0), 1),
                (s(2.0), 2),
                (s(3.0), 3),
                (s(4.0), 4)
            ]
        );
    }

    #[test]
    fn ramps_down_one_vu_per_step() {
        assert_eq!(
            steps(3, &[stage(3, 0)]),
            [(s(0.0), 3), (s(1.0), 2), (s(2.0), 1), (s(3.0), 0)]
        );
    }

    #[test]
    fn uneven_ramps_spread_steps_across_the_stage() {
        assert_eq!(
            steps(0, &[stage(10, 4)]),
            [
                (s(0.0), 0),
                (s(2.5), 1),
                (s(5.0), 2),
                (s(7.5), 3),
                (s(10.0), 4)
            ]
        );
    }

    #[test]
    fn multiple_stages_continue_from_the_previous_target() {
        assert_eq!(
            steps(1, &[stage(2, 3), stage(5, 3), stage(1, 2)]),
            [(s(0.0), 1), (s(1.0), 2), (s(2.0), 3), (s(8.0), 2)],
            "a flat stage adds no steps"
        );
    }

    #[test]
    fn zero_length_stages_jump() {
        assert_eq!(
            steps(0, &[stage(0, 5), stage(2, 5)]),
            [(s(0.0), 0), (s(0.0), 5)]
        );
    }
}
