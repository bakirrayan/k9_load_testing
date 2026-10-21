//! Parsing declarative plan files (YAML, TOML, JSON) into a resolved [`Plan`].

use std::path::Path;

use crate::plan::{InvalidPlan, Plan, PlanConfig};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Format {
    Yaml,
    Toml,
    Json,
}

impl Format {
    pub fn from_path(path: &Path) -> Option<Format> {
        let ext = path.extension()?.to_str()?.to_ascii_lowercase();
        match ext.as_str() {
            "yaml" | "yml" => Some(Format::Yaml),
            "toml" => Some(Format::Toml),
            "json" => Some(Format::Json),
            _ => None,
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            Format::Yaml => "YAML",
            Format::Toml => "TOML",
            Format::Json => "JSON",
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum LoadError {
    #[error("could not parse {format}: {message}", format = .format.name())]
    Parse { format: Format, message: String },
    #[error(transparent)]
    Invalid(#[from] InvalidPlan),
}

impl PlanConfig {
    pub fn parse(source: &str, format: Format) -> Result<PlanConfig, LoadError> {
        let parse_err = |message: String| LoadError::Parse { format, message };
        match format {
            Format::Yaml => serde_saphyr::from_str(source).map_err(|e| parse_err(e.to_string())),
            Format::Toml => toml::from_str(source).map_err(|e| parse_err(e.to_string())),
            Format::Json => serde_json::from_str(source).map_err(|e| parse_err(e.to_string())),
        }
    }
}

/// Parses and resolves a plan in one step.
pub fn load_plan(source: &str, format: Format) -> Result<Plan, LoadError> {
    Ok(PlanConfig::parse(source, format)?.resolve()?)
}
