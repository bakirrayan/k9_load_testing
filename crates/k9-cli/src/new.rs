//! `k9 new`: a starter plan or script, by tier. The templates are in
//! `crates/k9-cli/templates`; `@URL@` and `@FILE@` are filled in.

use std::path::{Path, PathBuf};

use clap::ValueEnum;

use crate::exit_code;

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub(crate) enum Template {
    /// A declarative YAML plan (Tier 1)
    Yaml,
    /// A Python script building a plan with `k9.plan` (Tier 1)
    Plan,
    /// An `async def default()` script (Tier 3)
    Async,
    /// A `def default()` script, one thread per VU (Tier 3)
    Sync,
}

impl Template {
    fn source(self) -> &'static str {
        match self {
            Template::Yaml => include_str!("../templates/plan.yaml"),
            Template::Plan => include_str!("../templates/plan.py"),
            Template::Async => include_str!("../templates/async.py"),
            Template::Sync => include_str!("../templates/sync.py"),
        }
    }

    fn default_file(self) -> &'static str {
        match self {
            Template::Yaml => "plan.yaml",
            _ => "script.py",
        }
    }

    fn describe(self) -> &'static str {
        match self {
            Template::Yaml => "a declarative plan (Tier 1)",
            Template::Plan => "a `k9.plan` script (Tier 1)",
            Template::Async => "an async script (Tier 3)",
            Template::Sync => "a sync script (Tier 3)",
        }
    }
}

pub(crate) const DEFAULT_URL: &str = "http://localhost:8080/ok";

/// The file's template: `template` if it fits the extension, else the
/// extension's (`.yaml`/`.yml`: a plan, `.py`: an async script).
fn pick(file: Option<&Path>, template: Option<Template>) -> Result<(PathBuf, Template), String> {
    let Some(file) = file else {
        let template = template.unwrap_or(Template::Async);
        return Ok((PathBuf::from(template.default_file()), template));
    };
    let ext = file
        .extension()
        .and_then(|e| e.to_str())
        .map(str::to_ascii_lowercase);
    let yaml = match ext.as_deref() {
        Some("yaml" | "yml") => true,
        Some("py") => false,
        _ => {
            return Err(format!(
                "{}: name a .yaml or .yml plan, or a .py script",
                file.display()
            ));
        }
    };
    match template {
        None if yaml => Ok((file.to_owned(), Template::Yaml)),
        None => Ok((file.to_owned(), Template::Async)),
        Some(t) if (t == Template::Yaml) == yaml => Ok((file.to_owned(), t)),
        Some(t) => Err(format!(
            "{}: --template {} writes {}, which needs a {} file",
            file.display(),
            t.to_possible_value().expect("not skipped").get_name(),
            t.describe(),
            if t == Template::Yaml { ".yaml" } else { ".py" },
        )),
    }
}

/// The template's text for `url`, which goes in a quoted string.
pub(crate) fn render(template: Template, url: &str, file: &str) -> Result<String, String> {
    if !(url.starts_with("http://") || url.starts_with("https://")) {
        return Err(format!(
            "--url: expected an http:// or https:// URL, got {url:?}"
        ));
    }
    if url.contains(|c: char| c.is_whitespace() || c.is_control() || "\"'\\@".contains(c)) {
        return Err(format!(
            "--url: {url:?} has characters a template can't quote"
        ));
    }
    Ok(template
        .source()
        .replace("@URL@", url)
        .replace("@FILE@", file))
}

/// `k9 new [FILE] [--template T] [--url URL] [--force]`.
pub(crate) fn command(
    file: Option<&Path>,
    template: Option<Template>,
    url: &str,
    force: bool,
) -> i32 {
    let failed = |e: String| {
        eprintln!("error: {e}");
        exit_code::COMMAND_FAILED
    };
    let (path, template) = match pick(file, template) {
        Ok(picked) => picked,
        Err(e) => return failed(e),
    };
    let name = path.display().to_string();
    let file_name = path
        .file_name()
        .map_or_else(|| name.clone(), |n| n.to_string_lossy().into_owned());
    let text = match render(template, url, &file_name) {
        Ok(text) => text,
        Err(e) => return failed(e),
    };
    if path.exists() && !force {
        return failed(format!("{name} exists; use --force to overwrite it"));
    }
    if let Err(e) = std::fs::write(&path, text) {
        return failed(format!("writing {name}: {e}"));
    }
    println!(
        "wrote {name}, {}. Run it with:\n\n    k9 run {name}",
        template.describe()
    );
    exit_code::OK
}
