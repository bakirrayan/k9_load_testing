//! k6's `httpDebug`: requests and responses dumped to stderr as they go.

use std::fmt::Write;

use http::{HeaderMap, Request, StatusCode, Version};

/// How much to dump: headers only, or bodies too.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HttpDebug {
    Headers,
    Full,
}

impl std::str::FromStr for HttpDebug {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, String> {
        match s {
            "headers" => Ok(HttpDebug::Headers),
            "full" => Ok(HttpDebug::Full),
            _ => Err(format!("expected headers or full, got `{s}`")),
        }
    }
}

/// `Request:`, the request line, its headers and (with `Full`) its body.
pub(crate) fn request_text<B>(req: &Request<B>, body: &[u8], debug: HttpDebug) -> String {
    let uri = req.uri();
    let target = uri.path_and_query().map_or("/", |p| p.as_str());
    let mut out = format!(
        "Request:\n{} {target} {}\n",
        req.method(),
        version_name(req.version())
    );
    if !req.headers().contains_key(http::header::HOST)
        && let Some(authority) = uri.authority()
    {
        let _ = writeln!(out, "host: {authority}");
    }
    write_rest(&mut out, req.headers(), body, debug);
    out
}

/// `Response:`, the status line, its headers and (with `Full`) its body.
pub(crate) fn response_text(
    version: Version,
    status: StatusCode,
    headers: &HeaderMap,
    body: &[u8],
    debug: HttpDebug,
) -> String {
    let mut out = format!(
        "Response:\n{} {} {}\n",
        version_name(version),
        status.as_u16(),
        status.canonical_reason().unwrap_or("")
    );
    write_rest(&mut out, headers, body, debug);
    out
}

fn write_rest(out: &mut String, headers: &HeaderMap, body: &[u8], debug: HttpDebug) {
    for (name, value) in headers {
        let _ = writeln!(out, "{name}: {}", String::from_utf8_lossy(value.as_bytes()));
    }
    out.push('\n');
    if debug == HttpDebug::Full && !body.is_empty() {
        match std::str::from_utf8(body) {
            Ok(text) => out.push_str(text),
            Err(_) => {
                let _ = write!(out, "[{} bytes of binary data]", body.len());
            }
        }
        out.push_str("\n\n");
    }
}

fn version_name(version: Version) -> &'static str {
    match version {
        Version::HTTP_10 => "HTTP/1.0",
        Version::HTTP_2 => "HTTP/2.0",
        Version::HTTP_3 => "HTTP/3.0",
        _ => "HTTP/1.1",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dumps_look_like_k6s() {
        let req = Request::post("http://example.com:8080/a/b?q=1")
            .header("x-probe", "yes")
            .body(())
            .unwrap();
        assert_eq!(
            request_text(&req, b"hi", HttpDebug::Full),
            "Request:\nPOST /a/b?q=1 HTTP/1.1\nhost: example.com:8080\nx-probe: yes\n\nhi\n\n"
        );
        assert_eq!(
            request_text(&req, b"hi", HttpDebug::Headers),
            "Request:\nPOST /a/b?q=1 HTTP/1.1\nhost: example.com:8080\nx-probe: yes\n\n"
        );
        let mut headers = HeaderMap::new();
        headers.insert("content-length", "3".parse().unwrap());
        assert_eq!(
            response_text(
                Version::HTTP_2,
                StatusCode::NOT_FOUND,
                &headers,
                &[0xff, 0, 1],
                HttpDebug::Full
            ),
            "Response:\nHTTP/2.0 404 Not Found\ncontent-length: 3\n\n[3 bytes of binary data]\n\n"
        );
        assert_eq!("full".parse(), Ok(HttpDebug::Full));
        assert!("bodies".parse::<HttpDebug>().is_err());
    }
}
