//! `k9 convert`: a HAR recording (a browser's developer tools, a proxy) into
//! a declarative plan. Pages become groups and pauses sleeps. Values a
//! response handed out and later requests sent back (tokens, session and
//! request ids, CSRF fields) are correlated: taken from that response with an
//! extractor and sent as `{{ variable }}`.

use std::collections::HashSet;
use std::path::Path;

use base64::Engine as _;
use serde::Deserialize;
use serde_json::{Map, Value, json};

use crate::exit_code;

#[derive(Debug, Default, Clone)]
pub struct ConvertOptions {
    /// Keep only requests to these hosts (and their subdomains).
    pub only: Vec<String>,
    /// Leave out requests to these hosts (and their subdomains).
    pub skip: Vec<String>,
    /// Leave out images, fonts, stylesheets and scripts.
    pub no_static: bool,
    /// Send recorded values as recorded.
    pub no_correlate: bool,
}

#[derive(Debug)]
pub struct Converted {
    pub yaml: String,
    pub requests: usize,
    pub left_out: usize,
    pub correlated: usize,
    pub warnings: Vec<String>,
}

// The parts of HAR 1.2 a plan needs.

#[derive(Deserialize)]
struct Har {
    log: Log,
}

#[derive(Deserialize)]
struct Log {
    #[serde(default)]
    pages: Vec<Page>,
    entries: Vec<Entry>,
}

#[derive(Deserialize)]
struct Page {
    id: String,
    #[serde(default)]
    title: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Entry {
    #[serde(default)]
    pageref: Option<String>,
    #[serde(default)]
    started_date_time: String,
    #[serde(default)]
    time: f64,
    request: HarRequest,
    response: HarResponse,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct HarRequest {
    method: String,
    url: String,
    #[serde(default)]
    headers: Vec<NameValue>,
    #[serde(default)]
    post_data: Option<PostData>,
}

#[derive(Deserialize)]
struct NameValue {
    name: String,
    #[serde(default)]
    value: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct PostData {
    #[serde(default)]
    mime_type: String,
    #[serde(default)]
    text: Option<String>,
    #[serde(default)]
    params: Vec<NameValue>,
}

#[derive(Deserialize)]
struct HarResponse {
    #[serde(default)]
    status: i64,
    #[serde(default)]
    headers: Vec<NameValue>,
    #[serde(default)]
    content: Content,
}

#[derive(Deserialize, Default)]
#[serde(rename_all = "camelCase")]
struct Content {
    #[serde(default)]
    mime_type: String,
    #[serde(default)]
    text: Option<String>,
    #[serde(default)]
    encoding: Option<String>,
}

/// One request of the plan, with the response it got (the last of its
/// redirects: k9 follows them).
struct Req {
    page: Option<String>,
    start_ms: Option<f64>,
    end_ms: Option<f64>,
    method: String,
    url: String,
    headers: Vec<(String, String)>,
    body: Option<String>,
    status: i64,
    /// Lowercase names.
    response_headers: Vec<(String, String)>,
    response_body: Option<String>,
    extract: Vec<(String, Value)>,
}

impl Req {
    /// Everything sent, for finding where a value was first used.
    fn sent(&self) -> impl Iterator<Item = &str> {
        std::iter::once(self.url.as_str())
            .chain(self.headers.iter().map(|(_, v)| v.as_str()))
            .chain(self.body.as_deref())
    }
}

/// Headers the client sets itself.
const DROPPED_HEADERS: &[&str] = &[
    "host",
    "content-length",
    "connection",
    "keep-alive",
    "proxy-connection",
    "transfer-encoding",
    "upgrade",
    "te",
];

/// Request headers whose values are never correlated.
const PLAIN_REQUEST_HEADERS: &[&str] = &[
    "accept",
    "accept-language",
    "accept-encoding",
    "user-agent",
    "content-type",
    "origin",
    "referer",
    "cache-control",
    "pragma",
    "upgrade-insecure-requests",
    "dnt",
    "priority",
];

/// Response headers never taken as a value's source.
const PLAIN_RESPONSE_HEADERS: &[&str] = &[
    "set-cookie",
    "date",
    "content-type",
    "content-length",
    "content-encoding",
    "transfer-encoding",
    "connection",
    "keep-alive",
    "server",
    "vary",
    "cache-control",
    "expires",
    "last-modified",
    "age",
    "accept-ranges",
    "pragma",
];

const STATIC_EXTENSIONS: &[&str] = &[
    "js", "mjs", "css", "png", "jpg", "jpeg", "gif", "svg", "ico", "webp", "avif", "woff", "woff2",
    "ttf", "otf", "eot", "map", "mp4", "webm", "mp3",
];

/// Shortest value correlated.
const MIN_VALUE: usize = 8;

/// Gaps shorter than this between requests aren't think time.
const MIN_SLEEP_MS: f64 = 500.0;

pub fn convert(har: &str, source: &str, options: &ConvertOptions) -> Result<Converted, String> {
    let har: Har = serde_json::from_str(har).map_err(|e| format!("not a HAR file: {e}"))?;
    let titles: Vec<(String, String)> = har
        .log
        .pages
        .into_iter()
        .map(|p| {
            let title = if p.title.is_empty() {
                p.id.clone()
            } else {
                p.title
            };
            (p.id, title)
        })
        .collect();
    let mut warnings = Vec::new();
    let mut cookies_set = HashSet::new();
    let mut reqs: Vec<Req> = Vec::new();
    let mut left_out = 0;
    let mut entries = har.log.entries.into_iter().peekable();
    while let Some(entry) = entries.next() {
        let host = host(&entry.request.url);
        let kept = (options.only.is_empty() || options.only.iter().any(|d| in_domain(host, d)))
            && !options.skip.iter().any(|d| in_domain(host, d))
            && !(options.no_static && is_static(&entry));
        if !kept {
            left_out += 1;
            continue;
        }
        let mut req = request(entry, &cookies_set);
        // A redirect k9 follows: the next entry is its target, not a request.
        while (300..400).contains(&req.status) {
            let Some(location) = header(&req.response_headers, "location") else {
                break;
            };
            let target = resolve(&req.url, location);
            let follows = entries.peek().is_some_and(|next| {
                next.request.method.eq_ignore_ascii_case("GET")
                    && strip_fragment(&next.request.url) == strip_fragment(&target)
            });
            if !follows {
                break;
            }
            let next = entries.next().expect("peeked");
            cookies_set.extend(cookie_names(&req.response_headers));
            let next = request(next, &cookies_set);
            req.status = next.status;
            req.response_headers = next.response_headers;
            req.response_body = next.response_body;
            req.end_ms = next.end_ms.or(req.end_ms);
        }
        cookies_set.extend(cookie_names(&req.response_headers));
        if req.sent().any(|s| s.contains("{{")) {
            warnings.push(format!(
                "{} {}: `{{{{` in it would be read as a template",
                req.method, req.url
            ));
        }
        reqs.push(req);
    }
    let correlated = if options.no_correlate {
        0
    } else {
        correlate(&mut reqs)
    };
    let flow = steps(&reqs, &titles);
    let mut yaml = format!(
        "# Converted from {source} by `k9 convert`: {} requests, {correlated} correlated values.\n\
         # Review it, add options (vus, duration, thresholds), then `k9 run` it.\n",
        reqs.len()
    );
    yaml.push_str(
        &serde_saphyr::to_string(&json!({ "flows": { "default": flow } }))
            .map_err(|e| e.to_string())?,
    );
    Ok(Converted {
        yaml,
        requests: reqs.len(),
        left_out,
        correlated,
        warnings,
    })
}

fn request(entry: Entry, cookies_set: &HashSet<String>) -> Req {
    let start_ms = timestamp_ms(&entry.started_date_time);
    let mut headers = Vec::new();
    for NameValue { name, value } in entry.request.headers {
        let lower = name.to_ascii_lowercase();
        if lower.starts_with(':') || DROPPED_HEADERS.contains(&lower.as_str()) {
            continue;
        }
        if lower == "cookie" {
            // Cookies a response set are in the VU's jar already.
            let kept: Vec<&str> = value
                .split(';')
                .map(str::trim)
                .filter(|pair| {
                    let name = pair.split('=').next().unwrap_or("");
                    !pair.is_empty() && !cookies_set.contains(name)
                })
                .collect();
            if !kept.is_empty() {
                headers.push((name, kept.join("; ")));
            }
            continue;
        }
        headers.push((name, value));
    }
    let body = entry.request.post_data.and_then(|data| {
        let body = match data.text.filter(|t| !t.is_empty()) {
            Some(text) => text,
            None if !data.params.is_empty() => data
                .params
                .iter()
                .map(|p| format!("{}={}", form_encode(&p.name), form_encode(&p.value)))
                .collect::<Vec<_>>()
                .join("&"),
            None => return None,
        };
        let typed = headers
            .iter()
            .any(|(n, _)| n.eq_ignore_ascii_case("content-type"));
        if !typed && !data.mime_type.is_empty() {
            headers.push(("Content-Type".to_owned(), data.mime_type));
        }
        Some(body)
    });
    let content = entry.response.content;
    let response_body = match (content.text, content.encoding.as_deref()) {
        (Some(text), Some("base64")) => base64::engine::general_purpose::STANDARD
            .decode(text.trim())
            .ok()
            .and_then(|bytes| String::from_utf8(bytes).ok()),
        (text, _) => text,
    };
    Req {
        page: entry.pageref,
        start_ms,
        end_ms: start_ms.map(|s| s + entry.time.max(0.0)),
        method: entry.request.method.to_ascii_uppercase(),
        url: entry.request.url,
        headers,
        body,
        status: entry.response.status,
        response_headers: entry
            .response
            .headers
            .into_iter()
            .map(|h| (h.name.to_ascii_lowercase(), h.value))
            .collect(),
        response_body,
        extract: Vec::new(),
    }
}

/// The plan's steps: requests, in groups by page, with sleeps for pauses.
fn steps(reqs: &[Req], titles: &[(String, String)]) -> Vec<Value> {
    let mut top = Vec::new();
    let mut group: Option<(&str, Vec<Value>)> = None;
    let mut last_end: Option<f64> = None;
    for req in reqs {
        let sleep = match (req.start_ms, last_end) {
            (Some(start), Some(end)) if start - end >= MIN_SLEEP_MS => {
                let ms = ((start - end) / 100.0).round() as u64 * 100;
                Some(json!({
                    "sleep": k9_core::Duration(std::time::Duration::from_millis(ms)).to_string()
                }))
            }
            _ => None,
        };
        last_end = match (last_end, req.end_ms) {
            (Some(a), Some(b)) => Some(a.max(b)),
            (a, b) => a.or(b),
        };
        let page = req.page.as_deref();
        if group.as_ref().map(|(p, _)| *p) != page {
            if let Some((p, steps)) = group.take() {
                top.push(group_step(p, steps, titles));
            }
            top.extend(sleep);
            group = page.map(|p| (p, Vec::new()));
        } else if let Some((_, steps)) = &mut group {
            steps.extend(sleep);
        } else {
            top.extend(sleep);
        }
        let step = json!({ "request": request_spec(req) });
        match &mut group {
            Some((_, steps)) => steps.push(step),
            None => top.push(step),
        }
    }
    if let Some((p, steps)) = group {
        top.push(group_step(p, steps, titles));
    }
    top
}

fn group_step(page: &str, steps: Vec<Value>, titles: &[(String, String)]) -> Value {
    let name = titles
        .iter()
        .find(|(id, _)| id == page)
        .map_or(page, |(_, title)| title.as_str());
    json!({ "group": { "name": name, "steps": steps } })
}

fn request_spec(req: &Req) -> Value {
    let mut spec = Map::new();
    if req.method != "GET" {
        spec.insert("method".into(), req.method.clone().into());
    }
    spec.insert("url".into(), req.url.clone().into());
    if !req.headers.is_empty() {
        let mut headers = Map::new();
        for (name, value) in &req.headers {
            let joined = match headers.get(name).and_then(Value::as_str) {
                Some(before) => format!("{before}, {value}"),
                None => value.clone(),
            };
            headers.insert(name.clone(), joined.into());
        }
        spec.insert("headers".into(), headers.into());
    }
    if let Some(body) = &req.body {
        spec.insert("body".into(), body.clone().into());
    }
    if (100..600).contains(&req.status) && req.status != 304 {
        let status = req.status;
        spec.insert(
            "checks".into(),
            json!({ format!("status is {status}"): { "status": status } }),
        );
    }
    if !req.extract.is_empty() {
        spec.insert(
            "extract".into(),
            req.extract.iter().cloned().collect::<Map<_, _>>().into(),
        );
    }
    spec.into()
}

// ---------------------------------------------------------------------------
// Correlation
// ---------------------------------------------------------------------------

/// Finds values requests sent that an earlier response handed out, adds an
/// extractor for each to that response's request and puts `{{ name }}` in
/// their place. Returns how many it found.
fn correlate(reqs: &mut [Req]) -> usize {
    let mut tokens: Vec<(String, usize)> = Vec::new();
    let mut seen = HashSet::new();
    for (i, req) in reqs.iter().enumerate() {
        for token in request_tokens(req) {
            if token.len() >= MIN_VALUE && seen.insert(token.clone()) {
                // First sent by request `i` (or earlier, inside a longer value).
                let first = reqs[..=i]
                    .iter()
                    .position(|r| r.sent().any(|s| s.contains(token.as_str())))
                    .unwrap_or(i);
                tokens.push((token, first));
            }
        }
    }
    // Longest first, so a value inside another isn't replaced inside it.
    tokens.sort_by_key(|(token, _)| std::cmp::Reverse(token.len()));
    let mut names: HashSet<String> = HashSet::new();
    let mut found = 0;
    for (token, first) in tokens {
        let source = (0..first)
            .rev()
            .find_map(|j| source(&token, &reqs[j]).map(|s| (j, s)));
        let Some((j, (hint, extractor))) = source else {
            continue;
        };
        let name = match reqs[j].extract.iter().find(|(_, e)| *e == extractor) {
            Some((name, _)) => name.clone(),
            None => {
                let name = unique_name(&hint, &mut names);
                reqs[j].extract.push((name.clone(), extractor));
                name
            }
        };
        let template = format!("{{{{ {name} }}}}");
        for req in &mut reqs[j + 1..] {
            req.url = replace_outside_templates(&req.url, &token, &template);
            for (_, value) in &mut req.headers {
                *value = replace_outside_templates(value, &token, &template);
            }
            if let Some(body) = &mut req.body {
                *body = replace_outside_templates(body, &token, &template);
            }
        }
        found += 1;
    }
    found
}

/// The values a request sends that could have come from a response.
fn request_tokens(req: &Req) -> Vec<String> {
    let mut out = Vec::new();
    let rest = req
        .url
        .split_once("://")
        .map_or(req.url.as_str(), |(_, r)| r);
    let path_and_query = &rest[rest.find('/').unwrap_or(rest.len())..];
    let (path, query) = strip_fragment(path_and_query)
        .split_once('?')
        .unwrap_or((strip_fragment(path_and_query), ""));
    // Path segments, except file names.
    out.extend(
        path.split('/')
            .filter(|s| !s.contains('.'))
            .map(str::to_owned),
    );
    for pair in query.split('&') {
        out.extend(words(pair.split_once('=').map_or(pair, |(_, v)| v)));
    }
    for (name, value) in &req.headers {
        let lower = name.to_ascii_lowercase();
        if PLAIN_REQUEST_HEADERS.contains(&lower.as_str()) || lower.starts_with("sec-") {
            continue;
        }
        out.push(value.clone());
        for piece in value.split([' ', ',', ';']).filter(|p| !p.is_empty()) {
            out.push(piece.to_owned());
            if let Some((_, v)) = piece.split_once('=') {
                out.push(v.to_owned());
            }
        }
    }
    if let Some(body) = &req.body {
        match serde_json::from_str::<Value>(body) {
            Ok(json) if json.is_object() || json.is_array() => {
                let mut leaves = Vec::new();
                json_leaves(&json, &mut String::from("$"), None, &mut leaves);
                out.extend(leaves.into_iter().map(|l| l.text));
            }
            _ => out.extend(words(body)),
        }
    }
    out
}

/// Runs of characters a token is made of.
fn words(text: &str) -> impl Iterator<Item = String> + '_ {
    text.split(|c: char| !(c.is_ascii_alphanumeric() || "-_.~+%".contains(c)))
        .filter(|w| !w.is_empty())
        .map(str::to_owned)
}

/// Where `token` is in `req`'s response, as a name for the variable and an
/// extractor; only if it looks like a handed-out value.
fn source(token: &str, req: &Req) -> Option<(String, Value)> {
    let body = req.response_body.as_deref().unwrap_or("");
    if let Ok(json) = serde_json::from_str::<Value>(body)
        && (json.is_object() || json.is_array())
    {
        let mut leaves = Vec::new();
        json_leaves(&json, &mut String::from("$"), None, &mut leaves);
        if let Some(leaf) = leaves
            .into_iter()
            .find(|l| l.text == token && handed_out(token, l.key.as_deref()))
        {
            let hint = leaf.key.unwrap_or_else(|| "value".to_owned());
            return Some((hint, json!({ "json": leaf.path })));
        }
    }
    if let Some((name, _)) = req.response_headers.iter().find(|(name, value)| {
        value == token
            && !PLAIN_RESPONSE_HEADERS.contains(&name.as_str())
            && handed_out(token, Some(name))
    }) {
        return Some((name.clone(), json!({ "header": name })));
    }
    regex_source(token, body)
}

/// A regex taking `token` from `body`: the text just before it, then the
/// token's characters. Only if it finds `token` there.
fn regex_source(token: &str, body: &str) -> Option<(String, Value)> {
    if token.contains(|c: char| c.is_whitespace() || "\"'<>&;,".contains(c)) {
        return None;
    }
    let at = body.find(token)?;
    let mut from = at.saturating_sub(32);
    while !body.is_char_boundary(from) {
        from += 1;
    }
    let before = &body[from..at];
    let before = &before[before.rfind('\n').map_or(0, |i| i + 1)..];
    if before.trim().len() < 2 {
        return None;
    }
    let hint = name_before(before);
    if !handed_out(token, hint.as_deref()) {
        return None;
    }
    let pattern = format!("{}([^\"'<>&;,\\s]+)", regex::escape(before));
    let re = regex::Regex::new(&pattern).ok()?;
    let found = re.captures(body)?.get(1)?.as_str();
    (found == token).then(|| {
        (
            hint.unwrap_or_else(|| "value".to_owned()),
            json!({ "regex": pattern }),
        )
    })
}

/// The field a value in HTML or text belongs to: `name="csrf" value="…`,
/// `"token": "…`, `sid=…`.
fn name_before(before: &str) -> Option<String> {
    let attribute = regex::Regex::new(r#"name=["']([^"']+)["']"#).expect("valid");
    if let Some(c) = attribute.captures_iter(before).last() {
        return Some(c[1].to_owned());
    }
    let key =
        regex::Regex::new(r#"([A-Za-z_][A-Za-z0-9_-]*)["']?\s*[:=]\s*["']?$"#).expect("valid");
    key.captures(before).map(|c| c[1].to_owned())
}

/// Whether a value looks handed out by the server rather than typed: long
/// enough, and opaque (letters and digits) or under a name like `token`.
fn handed_out(value: &str, name: Option<&str>) -> bool {
    const NAMES: &[&str] = &[
        "token", "session", "sid", "csrf", "xsrf", "nonce", "state", "code", "key", "auth",
        "ticket", "jwt",
    ];
    let opaque =
        value.chars().any(|c| c.is_ascii_digit()) && value.chars().any(|c| c.is_ascii_alphabetic());
    let named = name.is_some_and(|n| {
        let n = n.to_ascii_lowercase();
        n.ends_with("id") || NAMES.iter().any(|k| n.contains(k))
    });
    value.len() >= MIN_VALUE && (opaque || named)
}

struct Leaf {
    path: String,
    key: Option<String>,
    text: String,
}

/// Every string and number in a JSON document, with its path (in the
/// syntax `json` extractors take) and the key it's under.
fn json_leaves(value: &Value, path: &mut String, key: Option<&str>, out: &mut Vec<Leaf>) {
    let text = match value {
        Value::String(s) => Some(s.clone()),
        Value::Number(n) => Some(n.to_string()),
        _ => None,
    };
    if let Some(text) = text {
        out.push(Leaf {
            path: path.clone(),
            key: key.map(str::to_owned),
            text,
        });
        return;
    }
    let len = path.len();
    match value {
        Value::Object(map) => {
            for (k, v) in map {
                if k.is_empty() || k.contains(['\'', ']']) {
                    continue;
                }
                if k.contains(['.', '[', '"', ' ']) {
                    path.push_str(&format!("['{k}']"));
                } else {
                    path.push('.');
                    path.push_str(k);
                }
                json_leaves(v, path, Some(k), out);
                path.truncate(len);
            }
        }
        Value::Array(items) => {
            for (i, v) in items.iter().enumerate() {
                path.push_str(&format!("[{i}]"));
                json_leaves(v, path, key, out);
                path.truncate(len);
            }
        }
        _ => {}
    }
}

/// A variable name from `hint`, unlike any in `names`.
fn unique_name(hint: &str, names: &mut HashSet<String>) -> String {
    let mut base: String = hint
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
        .collect::<String>()
        .trim_matches('_')
        .to_owned();
    if base.is_empty() || base.starts_with(|c: char| c.is_ascii_digit()) {
        base = format!("value_{base}").trim_end_matches('_').to_owned();
    }
    let mut name = base.clone();
    let mut n = 1;
    while !names.insert(name.clone()) {
        n += 1;
        name = format!("{base}_{n}");
    }
    name
}

/// Replaces `from` in `text`, except inside `{{ … }}`.
fn replace_outside_templates(text: &str, from: &str, to: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut rest = text;
    loop {
        let (plain, template, after) = match rest.find("{{") {
            Some(open) => match rest[open..].find("}}") {
                Some(close) => (
                    &rest[..open],
                    &rest[open..open + close + 2],
                    &rest[open + close + 2..],
                ),
                None => (rest, "", ""),
            },
            None => (rest, "", ""),
        };
        out.push_str(&plain.replace(from, to));
        out.push_str(template);
        if after.is_empty() {
            return out;
        }
        rest = after;
    }
}

// ---------------------------------------------------------------------------
// URLs, headers, times
// ---------------------------------------------------------------------------

fn header<'a>(headers: &'a [(String, String)], name: &str) -> Option<&'a str> {
    headers
        .iter()
        .find(|(n, _)| n == name)
        .map(|(_, v)| v.as_str())
}

fn cookie_names(headers: &[(String, String)]) -> impl Iterator<Item = String> + '_ {
    headers
        .iter()
        .filter(|(n, _)| n == "set-cookie")
        .flat_map(|(_, v)| v.lines())
        .filter_map(|cookie| cookie.split_once('=').map(|(n, _)| n.trim().to_owned()))
}

fn host(url: &str) -> &str {
    let rest = url.split_once("://").map_or(url, |(_, r)| r);
    let authority = &rest[..rest.find(['/', '?', '#']).unwrap_or(rest.len())];
    let authority = authority.rsplit_once('@').map_or(authority, |(_, h)| h);
    match authority.strip_prefix('[') {
        Some(v6) => &v6[..v6.find(']').unwrap_or(v6.len())],
        None => authority.split(':').next().unwrap_or(""),
    }
}

fn in_domain(host: &str, domain: &str) -> bool {
    let (host, domain) = (host.to_ascii_lowercase(), domain.to_ascii_lowercase());
    host == domain || host.ends_with(&format!(".{domain}"))
}

fn is_static(entry: &Entry) -> bool {
    let mime = entry.response.content.mime_type.to_ascii_lowercase();
    if ["image/", "font/", "audio/", "video/", "text/css"]
        .iter()
        .any(|p| mime.starts_with(p))
        || mime.contains("javascript")
    {
        return true;
    }
    let url = strip_fragment(&entry.request.url);
    let path = url.split('?').next().unwrap_or("");
    let file = path.rsplit('/').next().unwrap_or("");
    file.rsplit_once('.')
        .is_some_and(|(_, ext)| STATIC_EXTENSIONS.contains(&ext.to_ascii_lowercase().as_str()))
}

fn strip_fragment(url: &str) -> &str {
    url.split('#').next().unwrap_or(url)
}

/// `location` resolved against `base`.
fn resolve(base: &str, location: &str) -> String {
    if location.contains("://") {
        return location.to_owned();
    }
    let (scheme, rest) = base.split_once("://").unwrap_or(("http", base));
    if let Some(location) = location.strip_prefix("//") {
        return format!("{scheme}://{location}");
    }
    let end = rest.find(['/', '?', '#']).unwrap_or(rest.len());
    let origin = format!("{scheme}://{}", &rest[..end]);
    if location.starts_with('/') {
        return origin + location;
    }
    let path = rest[end..].split(['?', '#']).next().unwrap_or("");
    if location.starts_with('?') {
        return format!("{origin}{path}{location}");
    }
    let dir = &path[..path.rfind('/').map_or(0, |i| i + 1)];
    let dir = if dir.is_empty() { "/" } else { dir };
    format!("{origin}{dir}{location}")
}

fn form_encode(text: &str) -> String {
    let mut out = String::new();
    for byte in text.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(byte as char)
            }
            b' ' => out.push('+'),
            _ => out.push_str(&format!("%{byte:02X}")),
        }
    }
    out
}

/// Milliseconds since the epoch of an RFC 3339 time (HAR's
/// `startedDateTime`).
fn timestamp_ms(text: &str) -> Option<f64> {
    let (date, time) = text.split_once('T')?;
    let mut parts = date.splitn(3, '-').map(|p| p.parse::<i64>().ok());
    let (y, m, d) = (parts.next()??, parts.next()??, parts.next()??);
    let (clock, offset_min) = match time.strip_suffix(['Z', 'z']) {
        Some(clock) => (clock, 0),
        None => {
            let at = time.rfind(['+', '-'])?;
            let (clock, offset) = time.split_at(at);
            let digits: String = offset[1..].chars().filter(char::is_ascii_digit).collect();
            let h: i64 = digits.get(..2)?.parse().ok()?;
            let mi: i64 = digits
                .get(2..)
                .filter(|s| !s.is_empty())
                .map_or(Some(0), |s| s.parse().ok())?;
            let sign = if offset.starts_with('-') { -1 } else { 1 };
            (clock, sign * (h * 60 + mi))
        }
    };
    let mut clock = clock.splitn(3, ':');
    let h: i64 = clock.next()?.parse().ok()?;
    let mi: i64 = clock.next()?.parse().ok()?;
    let s: f64 = clock.next()?.parse().ok()?;
    // Days from 1970-01-01 (Howard Hinnant's days_from_civil).
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let doy = (153 * ((m + 9) % 12) + 2) / 5 + d - 1;
    let days = era * 146_097 + yoe * 365 + yoe / 4 - yoe / 100 + doy - 719_468;
    let minutes = days * 1440 + h * 60 + mi - offset_min;
    Some(minutes as f64 * 60_000.0 + s * 1000.0)
}

/// `k9 convert FILE [-O OUT]`.
pub(crate) fn command(file: &Path, output: Option<&Path>, options: &ConvertOptions) -> i32 {
    let failed = |e: String| {
        eprintln!("error: {}: {e}", file.display());
        exit_code::COMMAND_FAILED
    };
    let har = match std::fs::read_to_string(file) {
        Ok(har) => har,
        Err(e) => return failed(e.to_string()),
    };
    let name = file.file_name().map_or_else(
        || file.display().to_string(),
        |n| n.to_string_lossy().into_owned(),
    );
    let converted = match convert(&har, &name, options) {
        Ok(converted) => converted,
        Err(e) => return failed(e),
    };
    for warning in &converted.warnings {
        eprintln!("WARN {warning}");
    }
    match output {
        Some(path) => {
            if let Err(e) = std::fs::write(path, &converted.yaml) {
                eprintln!("error: writing {}: {e}", path.display());
                return exit_code::COMMAND_FAILED;
            }
        }
        None => print!("{}", converted.yaml),
    }
    eprintln!(
        "converted {} requests ({} left out), {} correlated values",
        converted.requests, converted.left_out, converted.correlated
    );
    exit_code::OK
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(
        method: &str,
        url: &str,
        headers: Value,
        body: Option<&str>,
        response: Value,
    ) -> Value {
        let mut request = json!({ "method": method, "url": url, "headers": headers });
        if let Some(body) = body {
            request["postData"] = json!({
                "mimeType": "application/x-www-form-urlencoded",
                "text": body,
            });
        }
        json!({ "request": request, "response": response })
    }

    fn html(body: &str) -> Value {
        json!({ "status": 200, "content": { "mimeType": "text/html", "text": body } })
    }

    fn run(entries: Vec<Value>, options: &ConvertOptions) -> (Converted, Value) {
        let har = json!({ "log": { "entries": entries } }).to_string();
        let converted = convert(&har, "test.har", options).unwrap();
        k9_core::load_plan(&converted.yaml, k9_core::Format::Yaml).expect("a valid plan");
        let plan: Value = serde_saphyr::from_str(&converted.yaml).unwrap();
        (converted, plan)
    }

    fn request(plan: &Value, i: usize) -> &Value {
        &plan["flows"]["default"][i]["request"]
    }

    #[test]
    fn a_csrf_field_in_html_is_taken_with_a_regex() {
        let page = r#"<form><input type="hidden" name="csrf_token" value="f00dcafe1234"><a href="/news.php">news</a></form>"#;
        let (converted, plan) = run(
            vec![
                entry(
                    "GET",
                    "https://shop.test/login",
                    json!([]),
                    None,
                    html(page),
                ),
                entry(
                    "POST",
                    "https://shop.test/login",
                    json!([]),
                    Some("user=bob&csrf_token=f00dcafe1234"),
                    html("ok"),
                ),
                entry(
                    "GET",
                    "https://shop.test/news.php",
                    json!([]),
                    None,
                    html(""),
                ),
            ],
            &ConvertOptions::default(),
        );
        assert_eq!(converted.correlated, 1);
        let pattern = request(&plan, 0)["extract"]["csrf_token"]["regex"]
            .as_str()
            .unwrap();
        let re = regex::Regex::new(pattern).unwrap();
        assert_eq!(&re.captures(page).unwrap()[1], "f00dcafe1234");
        assert_eq!(
            request(&plan, 1)["body"],
            "user=bob&csrf_token={{ csrf_token }}"
        );
        // Plain words are left alone, links included.
        assert_eq!(request(&plan, 2)["url"], "https://shop.test/news.php");
        // Form bodies keep their type.
        assert_eq!(
            request(&plan, 1)["headers"]["Content-Type"],
            "application/x-www-form-urlencoded"
        );
    }

    #[test]
    fn values_the_client_sent_first_are_not_correlated() {
        // The server echoes an id the client made up: it's not handed out.
        let echo =
            json!({ "status": 200, "content": { "text": r#"{"id":"cl1ent-generated-42"}"# } });
        let (converted, plan) = run(
            vec![
                entry(
                    "POST",
                    "http://api.test/items",
                    json!([]),
                    Some("id=cl1ent-generated-42"),
                    echo,
                ),
                entry(
                    "GET",
                    "http://api.test/items/cl1ent-generated-42",
                    json!([]),
                    None,
                    html(""),
                ),
            ],
            &ConvertOptions::default(),
        );
        assert_eq!(converted.correlated, 0);
        assert_eq!(
            request(&plan, 1)["url"],
            "http://api.test/items/cl1ent-generated-42"
        );
    }

    #[test]
    fn opaque_ids_are_correlated_and_no_correlate_keeps_them() {
        let created =
            json!({ "status": 201, "content": { "text": r#"{"items":[{"ref":"a1b2c3d4e5"}]}"# } });
        let entries = || {
            vec![
                entry(
                    "POST",
                    "http://api.test/items",
                    json!([]),
                    None,
                    created.clone(),
                ),
                entry(
                    "GET",
                    "http://api.test/items/a1b2c3d4e5?x=1",
                    json!([]),
                    None,
                    html(""),
                ),
            ]
        };
        let (_, plan) = run(entries(), &ConvertOptions::default());
        assert_eq!(
            request(&plan, 0)["extract"]["ref"]["json"],
            "$.items[0].ref"
        );
        assert_eq!(
            request(&plan, 1)["url"],
            "http://api.test/items/{{ ref }}?x=1"
        );
        assert_eq!(request(&plan, 0)["checks"]["status is 201"]["status"], 201);
        let (converted, plan) = run(
            entries(),
            &ConvertOptions {
                no_correlate: true,
                ..Default::default()
            },
        );
        assert_eq!(converted.correlated, 0);
        assert!(request(&plan, 0).get("extract").is_none());
    }

    #[test]
    fn cookies_not_set_by_a_response_are_kept() {
        let set = json!({ "status": 200, "headers": [{ "name": "Set-Cookie", "value": "sid=1; Path=/" }] });
        let (_, plan) = run(
            vec![
                entry("GET", "http://a.test/", json!([]), None, set),
                entry(
                    "GET",
                    "http://a.test/x",
                    json!([{ "name": "cookie", "value": "sid=1; theme=dark" }]),
                    None,
                    html(""),
                ),
            ],
            &ConvertOptions::default(),
        );
        assert_eq!(request(&plan, 1)["headers"]["cookie"], "theme=dark");
    }

    #[test]
    fn hosts_and_static_files_can_be_left_out() {
        let image = json!({ "status": 200, "content": { "mimeType": "image/png" } });
        let entries = vec![
            entry("GET", "https://www.shop.test/", json!([]), None, html("")),
            entry(
                "GET",
                "https://cdn.test/app.js?v=3",
                json!([]),
                None,
                html(""),
            ),
            entry("GET", "https://www.shop.test/logo", json!([]), None, image),
            entry("GET", "https://ads.test/pixel", json!([]), None, html("")),
        ];
        let har = json!({ "log": { "entries": entries } }).to_string();
        let only = ConvertOptions {
            only: vec!["shop.test".into()],
            no_static: true,
            ..Default::default()
        };
        let converted = convert(&har, "t.har", &only).unwrap();
        assert_eq!((converted.requests, converted.left_out), (1, 3));
        let skip = ConvertOptions {
            skip: vec!["ADS.test".into()],
            ..Default::default()
        };
        let converted = convert(&har, "t.har", &skip).unwrap();
        assert_eq!((converted.requests, converted.left_out), (3, 1));
    }

    #[test]
    fn times_urls_and_forms() {
        assert_eq!(timestamp_ms("1970-01-01T00:00:01.5Z"), Some(1500.0));
        assert_eq!(
            timestamp_ms("2026-10-01T12:00:00.000+02:00"),
            timestamp_ms("2026-10-01T10:00:00Z")
        );
        assert_eq!(
            timestamp_ms("2000-03-01T00:00:00-0130"),
            Some(951_874_200_000.0)
        );
        assert_eq!(timestamp_ms("yesterday"), None);
        assert_eq!(resolve("http://a.test/x/y?q", "z"), "http://a.test/x/z");
        assert_eq!(resolve("http://a.test/x/y", "/z"), "http://a.test/z");
        assert_eq!(resolve("https://a.test/x", "//b.test/"), "https://b.test/");
        assert_eq!(resolve("http://a.test", "?p=1"), "http://a.test?p=1");
        assert_eq!(form_encode("a b&ü"), "a+b%26%C3%BC");
        assert_eq!(host("https://u:p@[::1]:8443/x"), "::1");
        assert_eq!(
            replace_outside_templates("t {{ t }} t", "t", "{{ v }}"),
            "{{ v }} {{ t }} {{ v }}"
        );
    }

    #[test]
    fn not_a_har_file() {
        let e = convert("{}", "x.har", &ConvertOptions::default()).unwrap_err();
        assert!(e.contains("not a HAR file"), "{e}");
    }
}
