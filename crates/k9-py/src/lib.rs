//! `k9._native`: the compiled half of the `k9` Python package.
//!
//! Only this crate depends on pyo3; the engine crates stay Python-free.

mod bodies;
mod grpc;
mod script;
mod ws;

use pyo3::prelude::*;

/// Runs the k9 command line with `argv` (including the program name) and
/// returns its exit code. The GIL is released while the command runs; `.py`
/// scripts are loaded and run through [`script::PyHost`].
#[pyfunction]
fn cli_main(py: Python<'_>, argv: Vec<String>) -> i32 {
    py.detach(|| k9_cli::run_with_host(argv, &script::PyHost))
}

#[pymodule]
fn _native(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("__version__", k9_core::VERSION)?;
    m.add_function(wrap_pyfunction!(cli_main, m)?)?;
    m.add_function(wrap_pyfunction!(script::metric_error, m)?)?;
    m.add_class::<script::NativeContext>()?;
    m.add_class::<script::RequestHandle>()?;
    m.add_class::<ws::NativeWebSocket>()?;
    m.add_class::<ws::BlockingWebSocket>()?;
    m.add_class::<grpc::NativeProtos>()?;
    m.add_class::<grpc::NativeMethod>()?;
    m.add_class::<grpc::NativeGrpcStream>()?;
    m.add_function(wrap_pyfunction!(grpc::grpc_target, m)?)?;
    m.add_function(wrap_pyfunction!(bodies::compress, m)?)?;
    m.add_function(wrap_pyfunction!(bodies::multipart_form, m)?)?;
    Ok(())
}
