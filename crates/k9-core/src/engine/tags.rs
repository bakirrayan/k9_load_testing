//! k6's `systemTags`: the tags the engine adds that samples keep.

use k9_metrics::{Interner, TagSetId};

use crate::plan::DEFAULT_SYSTEM_TAGS;

/// The system tags left out of the plan's `systemTags`.
#[derive(Debug, Clone, Default)]
pub(crate) struct SystemTags {
    off: Vec<&'static str>,
}

impl SystemTags {
    pub fn new(enabled: &[String]) -> SystemTags {
        SystemTags {
            off: DEFAULT_SYSTEM_TAGS
                .into_iter()
                .filter(|t| !enabled.iter().any(|e| e == t))
                .collect(),
        }
    }

    /// Interns `tags` without the system tags that are off.
    pub fn intern(&self, interner: &Interner, tags: &[(&str, &str)]) -> TagSetId {
        if !tags.iter().any(|(k, _)| self.off.contains(k)) {
            return interner.intern(tags);
        }
        let kept: Vec<(&str, &str)> = tags
            .iter()
            .filter(|(k, _)| !self.off.contains(k))
            .copied()
            .collect();
        interner.intern(&kept)
    }
}
