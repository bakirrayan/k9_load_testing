//! Templates in declarative flows: `{{ … }}` in a request's URL, header
//! values and body, parsed once when the plan is compiled. Inside the braces:
//!
//! - `name`: a variable set by an extractor earlier in the flow;
//! - `feeder.column`: a column of the iteration's row from a feeder;
//! - `__VU`, `__ITER`, `__ENV.NAME` (k6's names);
//! - generators: `uuid()`, `timestamp()` (Unix ms), `randomInt(min, max)`,
//!   `randomString(length)`.

use std::fmt::Write;
use std::time::{SystemTime, UNIX_EPOCH};

use indexmap::IndexMap;

use crate::engine::feeder::CFeeder;

enum Part {
    Text(String),
    Var(usize),
    Column { feeder: usize, column: usize },
    Vu,
    Iter,
    Env(String),
    Uuid,
    Timestamp,
    RandomInt(i64, i64),
    RandomString(usize),
}

pub(crate) struct Template {
    parts: Vec<Part>,
}

/// What a template can refer to.
pub(crate) struct Scope<'a> {
    /// Extracted variables' names; a variable's id is its position.
    pub vars: &'a [String],
    pub feeders: &'a [CFeeder],
}

/// A VU's values for rendering.
pub(crate) struct RenderCx<'a> {
    pub vars: &'a [Option<String>],
    pub var_names: &'a [String],
    pub feeders: &'a [CFeeder],
    /// The iteration's row from each feeder, picked on first use.
    pub rows: &'a mut [Option<usize>],
    pub vu: u64,
    pub iter: u64,
    pub env: &'a IndexMap<String, String>,
}

/// Longest `randomString()`, to keep a typo from allocating gigabytes.
const MAX_RANDOM_STRING: usize = 1 << 20;

impl Template {
    /// `None` if `src` has no `{{`: it's sent as is.
    pub fn parse(src: &str, scope: &Scope<'_>) -> Result<Option<Template>, String> {
        if !src.contains("{{") {
            return Ok(None);
        }
        let mut parts = Vec::new();
        let mut rest = src;
        while let Some(open) = rest.find("{{") {
            if open > 0 {
                parts.push(Part::Text(rest[..open].to_owned()));
            }
            let after = &rest[open + 2..];
            let close = after.find("}}").ok_or("unclosed `{{`")?;
            let expr = after[..close].trim();
            parts.push(parse_expr(expr, scope).map_err(|e| format!("`{{{{ {expr} }}}}`: {e}"))?);
            rest = &after[close + 2..];
        }
        if !rest.is_empty() {
            parts.push(Part::Text(rest.to_owned()));
        }
        Ok(Some(Template { parts }))
    }

    pub fn render(&self, out: &mut String, cx: &mut RenderCx<'_>) -> Result<(), String> {
        for part in &self.parts {
            match part {
                Part::Text(text) => out.push_str(text),
                Part::Var(id) => match &cx.vars[*id] {
                    Some(value) => out.push_str(value),
                    None => {
                        let name = &cx.var_names[*id];
                        return Err(format!(
                            "`{{{{ {name} }}}}`: `{name}` isn't set (its extractor hasn't run yet)"
                        ));
                    }
                },
                Part::Column { feeder, column } => {
                    let source = &cx.feeders[*feeder];
                    let row = *cx.rows[*feeder].get_or_insert_with(|| source.pick(cx.vu));
                    out.push_str(&source.rows[row][*column]);
                }
                Part::Vu => write!(out, "{}", cx.vu).unwrap(),
                Part::Iter => write!(out, "{}", cx.iter).unwrap(),
                Part::Env(name) => match cx.env.get(name) {
                    Some(value) => out.push_str(value),
                    None => match std::env::var(name) {
                        Ok(value) => out.push_str(&value),
                        Err(_) => {
                            return Err(format!("environment variable `{name}` isn't set"));
                        }
                    },
                },
                Part::Uuid => uuid(out),
                Part::Timestamp => {
                    let ms = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_millis();
                    write!(out, "{ms}").unwrap();
                }
                Part::RandomInt(min, max) => write!(out, "{}", fastrand::i64(*min..=*max)).unwrap(),
                Part::RandomString(len) => {
                    out.extend((0..*len).map(|_| fastrand::alphanumeric()));
                }
            }
        }
        Ok(())
    }
}

fn parse_expr(expr: &str, scope: &Scope<'_>) -> Result<Part, String> {
    if let Some(open) = expr.find('(') {
        let name = expr[..open].trim();
        let args = expr[open + 1..]
            .strip_suffix(')')
            .ok_or("missing `)`")?
            .trim();
        let ints = || -> Result<Vec<i64>, String> {
            if args.is_empty() {
                return Ok(Vec::new());
            }
            args.split(',')
                .map(|a| a.trim().parse::<i64>())
                .collect::<Result<_, _>>()
                .map_err(|_| format!("`{name}` takes whole numbers"))
        };
        let ints = ints()?;
        return match (name, ints.as_slice()) {
            ("uuid", []) => Ok(Part::Uuid),
            ("timestamp", []) => Ok(Part::Timestamp),
            ("randomInt", &[min, max]) if min <= max => Ok(Part::RandomInt(min, max)),
            ("randomString", &[len]) if (0..=MAX_RANDOM_STRING as i64).contains(&len) => {
                Ok(Part::RandomString(len as usize))
            }
            ("uuid" | "timestamp", _) => Err(format!("`{name}()` takes no arguments")),
            ("randomInt", _) => Err("use `randomInt(min, max)`, with min ≤ max".to_owned()),
            ("randomString", _) => Err(format!(
                "use `randomString(length)`, with length ≤ {MAX_RANDOM_STRING}"
            )),
            _ => Err(format!(
                "unknown function `{name}` (there are uuid(), timestamp(), randomInt(min, max) and randomString(length))"
            )),
        };
    }
    match expr {
        "__VU" => return Ok(Part::Vu),
        "__ITER" => return Ok(Part::Iter),
        _ => {}
    }
    if let Some(name) = expr.strip_prefix("__ENV.") {
        if name.is_empty() {
            return Err("`__ENV.` needs a variable name".to_owned());
        }
        return Ok(Part::Env(name.to_owned()));
    }
    if let Some((feeder, column)) = expr.split_once('.') {
        let (id, source) = scope
            .feeders
            .iter()
            .enumerate()
            .find(|(_, f)| f.name == feeder)
            .ok_or_else(|| format!("no feeder named `{feeder}`"))?;
        let column = source
            .columns
            .iter()
            .position(|c| c == column)
            .ok_or_else(|| {
                format!(
                    "feeder `{feeder}` has no column `{column}` (it has {})",
                    source.columns.join(", ")
                )
            })?;
        return Ok(Part::Column { feeder: id, column });
    }
    if is_identifier(expr) {
        return match scope.vars.iter().position(|v| v == expr) {
            Some(id) => Ok(Part::Var(id)),
            None => Err(format!("no extractor sets `{expr}`")),
        };
    }
    Err(
        "expected a variable, `feeder.column`, `__VU`, `__ITER`, `__ENV.NAME` or a function"
            .to_owned(),
    )
}

pub(crate) fn is_identifier(name: &str) -> bool {
    let mut chars = name.chars();
    chars
        .next()
        .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// A random (version 4) UUID.
fn uuid(out: &mut String) {
    let mut bytes = fastrand::u128(..).to_be_bytes();
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    for (i, byte) in bytes.iter().enumerate() {
        if matches!(i, 4 | 6 | 8 | 10) {
            out.push('-');
        }
        write!(out, "{byte:02x}").unwrap();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn render(src: &str, vars: &[(&str, Option<&str>)]) -> Result<String, String> {
        let names: Vec<String> = vars.iter().map(|(n, _)| n.to_string()).collect();
        let values: Vec<Option<String>> = vars.iter().map(|(_, v)| v.map(str::to_owned)).collect();
        let scope = Scope {
            vars: &names,
            feeders: &[],
        };
        let template = Template::parse(src, &scope)?.expect("a template");
        let mut out = String::new();
        let env = IndexMap::from([("A".to_owned(), "a".to_owned())]);
        template.render(
            &mut out,
            &mut RenderCx {
                vars: &values,
                var_names: &names,
                feeders: &[],
                rows: &mut [],
                vu: 3,
                iter: 7,
                env: &env,
            },
        )?;
        Ok(out)
    }

    #[test]
    fn text_without_braces_is_not_a_template() {
        let scope = Scope {
            vars: &[],
            feeders: &[],
        };
        assert!(Template::parse("/a/{b}/c", &scope).unwrap().is_none());
    }

    #[test]
    fn parts_render_in_order() {
        assert_eq!(
            render(
                "x{{t}}-{{ __VU }}/{{__ITER}}:{{ __ENV.A }}!",
                &[("t", Some("T"))]
            )
            .unwrap(),
            "xT-3/7:a!"
        );
    }

    #[test]
    fn an_unset_variable_is_an_error() {
        let err = render("{{ t }}", &[("t", None)]).unwrap_err();
        assert!(err.contains("`t` isn't set"), "{err}");
    }

    #[test]
    fn generators_produce_the_right_shapes() {
        let out = render(
            "{{ uuid() }} {{ randomInt(-2, -2) }} {{ randomString(5) }} {{ randomString(0) }}.",
            &[],
        )
        .unwrap();
        let parts: Vec<&str> = out.split(' ').collect();
        let uuid = parts[0];
        assert_eq!(uuid.len(), 36);
        assert_eq!(&uuid[14..15], "4");
        assert!("89ab".contains(&uuid[19..20]));
        assert_eq!(parts[1], "-2");
        assert_eq!(parts[2].len(), 5);
        assert!(parts[2].chars().all(|c| c.is_ascii_alphanumeric()));
        assert_eq!(parts[3], ".");
        assert_ne!(
            render("{{ uuid() }}", &[]).unwrap(),
            render("{{ uuid() }}", &[]).unwrap()
        );
    }

    #[test]
    fn bad_expressions_are_rejected() {
        for (src, expected) in [
            ("{{ a b }}", "expected a variable"),
            ("{{ randomString(-1) }}", "randomString(length)"),
            ("{{ randomInt(1) }}", "randomInt(min, max)"),
            ("{{ randomInt(a, b) }}", "whole numbers"),
            ("{{ uuid(1) }}", "no arguments"),
            ("{{ __ENV. }}", "variable name"),
            ("a }} {{", "unclosed"),
        ] {
            let err = render(src, &[]).unwrap_err();
            assert!(err.contains(expected), "{src}: {err}");
        }
    }
}
