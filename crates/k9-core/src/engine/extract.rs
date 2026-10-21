//! Extractors: values taken from a response into a VU's variables.

use http::HeaderMap;
use http::header::HeaderName;
use regex::bytes::Regex;

use crate::plan::Extractor;

pub(crate) enum CExtractor {
    Json(Vec<Segment>),
    Regex(Regex),
    Header(HeaderName),
}

#[derive(Debug, PartialEq)]
pub(crate) enum Segment {
    Key(String),
    Index(usize),
}

impl CExtractor {
    pub fn compile(spec: &Extractor) -> Result<CExtractor, String> {
        match spec {
            Extractor::Json(path) => parse_path(path).map(CExtractor::Json),
            Extractor::Regex(re) => Regex::new(re)
                .map(CExtractor::Regex)
                .map_err(|e| format!("invalid regex `{re}`: {e}")),
            Extractor::Header(name) => HeaderName::try_from(name.as_str())
                .map(CExtractor::Header)
                .map_err(|e| format!("invalid header name `{name}`: {e}")),
        }
    }

    pub fn needs_body(&self) -> bool {
        !matches!(self, CExtractor::Header(_))
    }

    /// The value, or `None` if the response doesn't have it. `json` caches the
    /// parsed body across a response's extractors (`Some(None)`: not JSON).
    pub fn extract(
        &self,
        headers: &HeaderMap,
        body: &[u8],
        json: &mut Option<Option<serde_json::Value>>,
    ) -> Option<String> {
        match self {
            CExtractor::Json(path) => {
                let root = json
                    .get_or_insert_with(|| serde_json::from_slice(body).ok())
                    .as_ref()?;
                let value = path.iter().try_fold(root, |value, segment| match segment {
                    Segment::Key(key) => value.get(key),
                    Segment::Index(i) => value.get(i),
                })?;
                match value {
                    serde_json::Value::Null => None,
                    serde_json::Value::String(s) => Some(s.clone()),
                    other => Some(other.to_string()),
                }
            }
            CExtractor::Regex(re) => {
                let captures = re.captures(body)?;
                let found = captures.get(1).or_else(|| captures.get(0))?;
                Some(String::from_utf8_lossy(found.as_bytes()).into_owned())
            }
            CExtractor::Header(name) => headers
                .get(name)
                .map(|v| String::from_utf8_lossy(v.as_bytes()).into_owned()),
        }
    }
}

/// A JSON path: `$`, then `.key`, `['key']` or `[index]` steps.
fn parse_path(src: &str) -> Result<Vec<Segment>, String> {
    let invalid = |why: &str| format!("invalid JSON path `{src}`: {why}");
    let mut rest = src
        .strip_prefix('$')
        .ok_or_else(|| invalid("it must start with `$`"))?;
    let mut path = Vec::new();
    while !rest.is_empty() {
        if let Some(after) = rest.strip_prefix('.') {
            let end = after.find(['.', '[']).unwrap_or(after.len());
            if end == 0 {
                return Err(invalid("empty key after `.`"));
            }
            path.push(Segment::Key(after[..end].to_owned()));
            rest = &after[end..];
        } else if let Some(after) = rest.strip_prefix('[') {
            let close = after.find(']').ok_or_else(|| invalid("unclosed `[`"))?;
            let inside = after[..close].trim();
            let quoted = ['\'', '"']
                .into_iter()
                .find_map(|q| inside.strip_prefix(q)?.strip_suffix(q));
            path.push(match quoted {
                Some(key) => Segment::Key(key.to_owned()),
                None => Segment::Index(
                    inside
                        .parse()
                        .map_err(|_| invalid("use `[index]` or `['key']`"))?,
                ),
            });
            rest = &after[close + 1..];
        } else {
            return Err(invalid("expected `.` or `[`"));
        }
    }
    Ok(path)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn extract(spec: Extractor, body: &str) -> Option<String> {
        let mut headers = HeaderMap::new();
        headers.insert("x-id", "7".parse().unwrap());
        CExtractor::compile(&spec)
            .unwrap()
            .extract(&headers, body.as_bytes(), &mut None)
    }

    #[test]
    fn json_paths() {
        use Segment::*;
        assert_eq!(parse_path("$").unwrap(), vec![]);
        assert_eq!(
            parse_path("$.a[2]['b.c'][\"d\"].e").unwrap(),
            vec![
                Key("a".into()),
                Index(2),
                Key("b.c".into()),
                Key("d".into()),
                Key("e".into())
            ]
        );
        for bad in ["a", "$.", "$..a", "$[x]", "$[1", "$a"] {
            assert!(parse_path(bad).is_err(), "{bad}");
        }
        let body = r#"{"a": [{"b": "x"}, {"b": 2}], "n": null, "o": {"k": [1]}}"#;
        let json = |path: &str| extract(Extractor::Json(path.into()), body);
        assert_eq!(json("$.a[0].b").as_deref(), Some("x"));
        assert_eq!(json("$.a[1].b").as_deref(), Some("2"));
        assert_eq!(json("$.o").as_deref(), Some(r#"{"k":[1]}"#));
        assert_eq!(json("$.n"), None);
        assert_eq!(json("$.a[5]"), None);
        assert_eq!(json("$.missing"), None);
        assert_eq!(extract(Extractor::Json("$".into()), "not json"), None);
    }

    #[test]
    fn regexes_take_the_first_group_or_the_match() {
        let body = "id=42; id=43";
        assert_eq!(
            extract(Extractor::Regex(r"id=(\d+)".into()), body).as_deref(),
            Some("42")
        );
        assert_eq!(
            extract(Extractor::Regex(r"\d+".into()), body).as_deref(),
            Some("42")
        );
        assert_eq!(extract(Extractor::Regex("nope".into()), body), None);
    }

    #[test]
    fn headers_ignore_case() {
        assert_eq!(
            extract(Extractor::Header("X-Id".into()), "").as_deref(),
            Some("7")
        );
        assert_eq!(extract(Extractor::Header("x-other".into()), ""), None);
    }
}
