//! Execution segments: how a run is divided among workers.

use k9_core::Segment;
use proptest::prelude::*;

fn seg(s: &str) -> Segment {
    s.parse().unwrap_or_else(|e| panic!("{s}: {e}"))
}

#[test]
fn parses_and_prints_k6s_syntax() {
    assert_eq!(seg("1/4:2/4"), seg("1/4:1/2"), "fractions are reduced");
    assert_eq!(seg("1/4:2/4").to_string(), "1/4:1/2");
    assert_eq!(seg("0:1"), Segment::full());
    assert_eq!(Segment::full().to_string(), "0:1");
    for bad in ["", "1/2", "1/2:1/4", "0:3/2", "a:1", "1/0:1", "1/3:1/3"] {
        assert!(
            bad.parse::<Segment>().is_err(),
            "{bad:?} should be rejected"
        );
    }
    let err = "1/2:1/4".parse::<Segment>().unwrap_err();
    assert_eq!(
        err.to_string(),
        "invalid execution segment `1/2:1/4`: the start must be below the end, both between 0 and 1"
    );
}

#[test]
fn splits_into_equal_parts() {
    let parts = Segment::split(3);
    assert_eq!(
        parts.iter().map(|s| s.to_string()).collect::<Vec<_>>(),
        ["0:1/3", "1/3:2/3", "2/3:1"]
    );
    assert_eq!(Segment::split(1), [Segment::full()]);
    let sequence = Segment::sequence("0,1/4,1/2,1").unwrap();
    assert_eq!(
        sequence.iter().map(|s| s.to_string()).collect::<Vec<_>>(),
        ["0:1/4", "1/4:1/2", "1/2:1"]
    );
    for bad in ["", "0,1/2", "1/4,1", "0,1/2,1/4,1", "0,1/2,1/2,1", "0,x,1"] {
        assert!(
            Segment::sequence(bad).is_err(),
            "{bad:?} should be rejected"
        );
    }
}

#[test]
fn shares_of_a_quantity_sum_to_it() {
    let parts = Segment::split(3);
    let shares: Vec<u64> = parts.iter().map(|s| s.share(10)).collect();
    assert_eq!(shares.iter().sum::<u64>(), 10);
    assert_eq!(shares, [4, 3, 3]);
    assert_eq!(Segment::full().share(7), 7);
}

#[test]
fn slots_are_striped_across_parts() {
    // Slot j (the j-th VU of a scenario) belongs to exactly one part; the
    // parts take turns, so any number of active VUs is spread evenly.
    let parts = Segment::split(3);
    let owners: Vec<usize> = (0..9)
        .map(|slot| parts.iter().position(|p| p.owns(slot)).unwrap())
        .collect();
    assert_eq!(owners, [0, 1, 2, 0, 1, 2, 0, 1, 2]);
    assert_eq!(
        (0..3).map(|i| parts[1].slot(i)).collect::<Vec<_>>(),
        [1, 4, 7]
    );
}

fn partition() -> impl Strategy<Value = Vec<Segment>> {
    // 1–8 parts with random cut points on a denominator up to 100.
    (1u64..=100, prop::collection::vec(any::<u64>(), 0..8)).prop_map(|(den, cuts)| {
        let mut points: Vec<u64> = cuts.iter().map(|c| c % den).filter(|&c| c > 0).collect();
        points.sort_unstable();
        points.dedup();
        let mut bounds = vec![0];
        bounds.extend(points);
        bounds.push(den);
        let text: Vec<String> = bounds.iter().map(|b| format!("{b}/{den}")).collect();
        Segment::sequence(&text.join(",")).unwrap()
    })
}

proptest! {
    #[test]
    fn any_partition_shares_every_quantity_exactly(parts in partition(), k in 0u64..100_000) {
        let total: u64 = parts.iter().map(|p| p.share(k)).sum();
        prop_assert_eq!(total, k);
        for p in &parts {
            // Within one of the proportional share.
            let exact = k as f64 * p.fraction();
            prop_assert!((p.share(k) as f64 - exact).abs() < 1.0, "{} of {}: {}", p, k, p.share(k));
        }
    }

    #[test]
    fn every_slot_has_exactly_one_owner(parts in partition(), slot in 0u64..100_000) {
        prop_assert_eq!(parts.iter().filter(|p| p.owns(slot)).count(), 1);
    }

    #[test]
    fn local_slots_are_the_owned_slots_in_order(parts in partition(), n in 0u64..300) {
        for p in &parts {
            let owned: Vec<u64> = (0..n).filter(|&j| p.owns(j)).collect();
            let local: Vec<u64> = (0..p.share(n)).map(|i| p.slot(i)).collect();
            prop_assert_eq!(local, owned);
        }
    }
}

#[test]
fn segments_travel_as_their_sequence_and_index() {
    for part in Segment::sequence("0,1/4,1/2,1").unwrap() {
        let json = serde_json::to_string(&part).unwrap();
        let back: Segment = serde_json::from_str(&json).unwrap();
        assert_eq!(back, part);
        assert_eq!(
            (0..40).map(|j| back.owns(j)).collect::<Vec<_>>(),
            (0..40).map(|j| part.owns(j)).collect::<Vec<_>>()
        );
    }
    let bad: Result<Segment, _> = serde_json::from_str(r#"{"sequence":[[0,1],[1,1]],"index":1}"#);
    assert!(bad.is_err());
}

#[test]
fn weighted_splits_follow_the_weights() {
    let parts = Segment::weighted(&[1, 1, 2]);
    let text: Vec<String> = parts.iter().map(ToString::to_string).collect();
    assert_eq!(text, ["0:1/4", "1/4:1/2", "1/2:1"]);
    assert_eq!(parts.iter().map(|s| s.share(8)).sum::<u64>(), 8);
    assert_eq!(parts[2].share(8), 4);
    let equal: Vec<String> = Segment::weighted(&[3, 3, 3])
        .iter()
        .map(ToString::to_string)
        .collect();
    let split: Vec<String> = Segment::split(3).iter().map(ToString::to_string).collect();
    assert_eq!(equal, split);
    assert_eq!(
        Segment::weighted(&[0, 5]).len(),
        2,
        "a zero weight counts as 1"
    );
}
