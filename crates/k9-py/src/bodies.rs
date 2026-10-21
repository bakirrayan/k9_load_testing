//! Request bodies built in Rust for `k9.http`: compression and multipart
//! forms (the same code declarative plans use).

use k9_http::compression::{Encoding, compress as compress_with};
use k9_http::multipart::Form;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyBytes;

/// `data` compressed with `encodings` (e.g. `"gzip, br"`), and the
/// `Content-Encoding` value; `ValueError` for an unknown encoding.
#[pyfunction]
pub(crate) fn compress<'py>(
    py: Python<'py>,
    data: &[u8],
    encodings: &str,
) -> PyResult<(Bound<'py, PyBytes>, String)> {
    let encodings = Encoding::parse_list(encodings).map_err(PyValueError::new_err)?;
    let compressed = py
        .detach(|| compress_with(data, &encodings))
        .map_err(|e| PyValueError::new_err(e.to_string()))?;
    Ok((
        PyBytes::new(py, &compressed),
        Encoding::header_value(&encodings),
    ))
}

/// `(name, filename, content_type, data)`: a text field has no filename.
type Part = (String, Option<String>, Option<String>, Vec<u8>);

/// A multipart form from its parts: its `Content-Type` and body.
#[pyfunction]
pub(crate) fn multipart_form<'py>(
    py: Python<'py>,
    parts: Vec<Part>,
) -> (String, Bound<'py, PyBytes>) {
    let mut form = Form::new();
    for (name, filename, content_type, data) in parts {
        form = match filename {
            Some(filename) => form.file(&name, &filename, content_type.as_deref(), data),
            None => form.text(&name, &String::from_utf8_lossy(&data)),
        };
    }
    let (content_type, body) = form.finish();
    (content_type, PyBytes::new(py, &body))
}
