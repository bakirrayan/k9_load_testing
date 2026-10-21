//! Running one execution segment of a plan: N engines, each given its segment,
//! together do exactly what one engine does with the whole plan.

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};

use k9_core::engine::{
    self, BoxFuture, RunControl, RunResult, Script, ScriptContext, ScriptError, ScriptVu, VuInfo,
};
use k9_core::{Plan, PlanConfig, Segment};

/// Records which VUs (id, scenario) it was asked for.
#[derive(Default)]
struct Recorder {
    vus: Mutex<Vec<(u64, String)>>,
}

impl Script for Recorder {
    fn setup(&self, _: ScriptContext) -> BoxFuture<'_, Result<(), ScriptError>> {
        Box::pin(async { Ok(()) })
    }
    fn teardown(&self, _: ScriptContext) -> BoxFuture<'_, Result<(), ScriptError>> {
        Box::pin(async { Ok(()) })
    }
    fn vu(&self, vu: VuInfo, _: ScriptContext) -> Box<dyn ScriptVu> {
        self.vus.lock().unwrap().push((vu.id, vu.scenario));
        Box::new(Quick)
    }
}

struct Quick;

impl ScriptVu for Quick {
    fn iteration(&mut self, _function: &str) -> BoxFuture<'_, Result<(), ScriptError>> {
        Box::pin(async {
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
            Ok(())
        })
    }
}

fn plan(options: serde_json::Value) -> Plan {
    PlanConfig {
        options: serde_json::from_value(options).unwrap(),
        ..Default::default()
    }
    .resolve_script(&["default"])
    .unwrap_or_else(|e| panic!("{e}"))
}

/// Runs `options` split into `parts` segments, concurrently; returns each
/// part's result and the VUs it created.
async fn run_split(options: serde_json::Value, parts: u32) -> Vec<(RunResult, Vec<(u64, String)>)> {
    let plan = plan(options);
    let handles: Vec<_> = Segment::split(parts)
        .into_iter()
        .map(|segment| {
            let plan = plan.clone();
            tokio::spawn(async move {
                let script = Arc::new(Recorder::default());
                let result = engine::run_segment(
                    &plan,
                    segment,
                    RunControl::new(),
                    Vec::new(),
                    Some(script.clone()),
                )
                .await
                .unwrap();
                let vus = script.vus.lock().unwrap().clone();
                (result, vus)
            })
        })
        .collect();
    let mut out = Vec::new();
    for handle in handles {
        out.push(handle.await.unwrap());
    }
    out
}

fn ids(parts: &[(RunResult, Vec<(u64, String)>)]) -> Vec<BTreeSet<u64>> {
    parts
        .iter()
        .map(|(_, vus)| vus.iter().map(|(id, _)| *id).collect())
        .collect()
}

fn assert_disjoint_cover(sets: &[BTreeSet<u64>], expected: impl IntoIterator<Item = u64>) {
    let mut union = BTreeSet::new();
    for set in sets {
        for id in set {
            assert!(
                union.insert(*id),
                "VU id {id} used by two segments: {sets:?}"
            );
        }
    }
    assert_eq!(union, expected.into_iter().collect(), "{sets:?}");
}

#[tokio::test]
async fn shared_iterations_add_up_exactly() {
    let parts = run_split(
        serde_json::json!({"scenarios": {"s": {"executor": "shared-iterations", "vus": 5, "iterations": 101}}}),
        3,
    )
    .await;
    let counts: Vec<u64> = parts.iter().map(|(r, _)| r.iterations_complete).collect();
    assert_eq!(counts.iter().sum::<u64>(), 101, "{counts:?}");
    assert_eq!(
        counts,
        [41, 40, 20],
        "each VU slot carries its share of the iterations"
    );
    assert_disjoint_cover(&ids(&parts), 1..=5);
    let max: Vec<u32> = parts.iter().map(|(r, _)| r.max_vus).collect();
    assert_eq!(max, [2, 2, 1]);
}

#[tokio::test]
async fn a_segment_without_vus_gets_no_iterations() {
    // 1 VU, 3 segments: only the VU's segment may take iterations.
    let parts = run_split(
        serde_json::json!({"scenarios": {"s": {"executor": "shared-iterations", "vus": 1, "iterations": 10}}}),
        3,
    )
    .await;
    let counts: Vec<u64> = parts.iter().map(|(r, _)| r.iterations_complete).collect();
    assert_eq!(counts, [10, 0, 0]);
}

#[tokio::test]
async fn per_vu_iterations_and_constant_vus_split_by_vu() {
    let parts = run_split(
        serde_json::json!({"scenarios": {
            "each": {"executor": "per-vu-iterations", "vus": 4, "iterations": 3},
            "steady": {"executor": "constant-vus", "vus": 7, "duration": "100ms"},
        }}),
        3,
    )
    .await;
    let each: u64 = parts
        .iter()
        .map(|(_, vus)| vus.iter().filter(|(_, s)| s == "each").count() as u64 * 3)
        .sum();
    assert_eq!(each, 12);
    // Ids: `each` owns 1–4, `steady` 5–11, whichever segment runs them.
    assert_disjoint_cover(&ids(&parts), 1..=11);
    for (_, vus) in &parts {
        for (id, scenario) in vus {
            assert_eq!(scenario == "each", *id <= 4, "{vus:?}");
        }
    }
}

#[tokio::test]
async fn ramping_vus_follow_the_global_schedule() {
    let parts = run_split(
        serde_json::json!({"scenarios": {"r": {"executor": "ramping-vus", "startVUs": 0,
            "stages": [{"duration": "300ms", "target": 6}, {"duration": "100ms", "target": 6}]}}}),
        3,
    )
    .await;
    let sets = ids(&parts);
    assert_eq!(
        sets.iter().map(BTreeSet::len).collect::<Vec<_>>(),
        [2, 2, 2]
    );
    assert_disjoint_cover(&sets, 1..=6);
    // Slots are striped: segment 0 holds VU slots 0 and 3, i.e. ids 1 and 4.
    assert_eq!(sets[0], BTreeSet::from([1, 4]));
}

#[tokio::test]
async fn the_full_segment_is_the_plain_run() {
    let plan = plan(serde_json::json!({"vus": 3, "iterations": 9}));
    let script = Arc::new(Recorder::default());
    let result = engine::run_segment(
        &plan,
        Segment::full(),
        RunControl::new(),
        Vec::new(),
        Some(script.clone()),
    )
    .await
    .unwrap();
    assert_eq!(result.iterations_complete, 9);
    let mut vus: Vec<u64> = script
        .vus
        .lock()
        .unwrap()
        .iter()
        .map(|(id, _)| *id)
        .collect();
    vus.sort_unstable();
    assert_eq!(vus, [1, 2, 3]);
}
