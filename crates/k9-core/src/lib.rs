//! K9 engine core.
//!
//! - [`plan`]: the options, scenarios, executors and flows that every front end
//!   (Python API, declarative files, CLI flags) resolves into.
//! - [`engine`]: runs a plan: scenarios, VUs, iterations, metrics.
//!
//! This crate must never depend on Python.

pub mod duration;
pub mod engine;
pub mod load;
pub mod overrides;
pub mod plan;
pub mod protocol;
pub mod segment;

pub use duration::Duration;
pub use load::{Format, LoadError, load_plan};
pub use overrides::Overrides;
pub use plan::{
    Executor, ExecutorKind, InvalidPlan, OptionsConfig, Plan, PlanConfig, Scenario, Step,
};
pub use segment::{Segment, SegmentError};

pub const VERSION: &str = env!("CARGO_PKG_VERSION");
