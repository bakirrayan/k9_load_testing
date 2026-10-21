//! Tag-set interning: each distinct set of tags is stored once and referred to
//! by a small integer, so recording a sample never allocates strings.

use std::hash::{Hash, Hasher};
use std::sync::{Arc, RwLock};

use indexmap::{Equivalent, IndexSet};

/// Tag sets up to this size are normalized on the stack.
const INLINE_TAGS: usize = 16;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TagSetId(pub(crate) u32);

impl TagSetId {
    /// The empty tag set; always id 0.
    pub const EMPTY: TagSetId = TagSetId(0);
}

/// Tags sorted by key. Keys are unique.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Default)]
pub struct TagSet {
    pairs: Box<[(Box<str>, Box<str>)]>,
}

impl TagSet {
    pub fn get(&self, key: &str) -> Option<&str> {
        self.pairs
            .binary_search_by(|(k, _)| (**k).cmp(key))
            .ok()
            .map(|i| &*self.pairs[i].1)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&str, &str)> {
        self.pairs.iter().map(|(k, v)| (&**k, &**v))
    }

    pub fn len(&self) -> usize {
        self.pairs.len()
    }

    pub fn is_empty(&self) -> bool {
        self.pairs.is_empty()
    }

    /// True if every `(key, value)` in `filter` is present, as in k6 sub-metric
    /// selectors like `http_req_duration{scenario:checkout}`.
    pub fn matches(&self, filter: &[(&str, &str)]) -> bool {
        filter.iter().all(|(k, v)| self.get(k) == Some(v))
    }
}

/// Shared tag-set table. Lookups of known sets take a read lock and don't
/// allocate; only a new set allocates.
#[derive(Debug)]
pub struct Interner {
    sets: RwLock<IndexSet<Arc<TagSet>>>,
}

impl Default for Interner {
    fn default() -> Self {
        Interner::new()
    }
}

impl Interner {
    pub fn new() -> Interner {
        let mut sets = IndexSet::new();
        sets.insert(Arc::new(TagSet::default())); // TagSetId::EMPTY
        Interner {
            sets: RwLock::new(sets),
        }
    }

    /// Returns the id for this set of tags. Order doesn't matter; if a key
    /// repeats, the last value wins.
    pub fn intern(&self, tags: &[(&str, &str)]) -> TagSetId {
        if tags.len() <= INLINE_TAGS {
            let mut buf = [("", ""); INLINE_TAGS];
            buf[..tags.len()].copy_from_slice(tags);
            let len = normalize(&mut buf[..tags.len()]);
            self.intern_sorted(&buf[..len])
        } else {
            let mut buf = tags.to_vec();
            let len = normalize(&mut buf);
            self.intern_sorted(&buf[..len])
        }
    }

    fn intern_sorted(&self, sorted: &[(&str, &str)]) -> TagSetId {
        let query = Query(sorted);
        if let Some(i) = self.read().get_index_of(&query) {
            return to_id(i);
        }
        let set = Arc::new(TagSet {
            pairs: sorted
                .iter()
                .map(|(k, v)| (Box::from(*k), Box::from(*v)))
                .collect(),
        });
        // Another thread may have inserted the same set meanwhile; insert_full
        // returns the existing index in that case.
        to_id(self.write().insert_full(set).0)
    }

    pub fn get(&self, id: TagSetId) -> Arc<TagSet> {
        self.read()
            .get_index(id.0 as usize)
            .expect("tag set ids come from this interner")
            .clone()
    }

    pub fn len(&self) -> usize {
        self.read().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl Interner {
    fn read(&self) -> std::sync::RwLockReadGuard<'_, IndexSet<Arc<TagSet>>> {
        // Insertions are single calls that can't leave the set half-updated.
        self.sets
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn write(&self) -> std::sync::RwLockWriteGuard<'_, IndexSet<Arc<TagSet>>> {
        self.sets
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

fn to_id(index: usize) -> TagSetId {
    TagSetId(u32::try_from(index).expect("fewer than 2^32 tag sets"))
}

/// Sorts by key and drops earlier duplicates (last value wins). Returns the new length.
fn normalize(tags: &mut [(&str, &str)]) -> usize {
    tags.sort_by(|a, b| a.0.cmp(b.0)); // stable: equal keys keep input order
    let mut len = 0;
    for i in 0..tags.len() {
        if len > 0 && tags[len - 1].0 == tags[i].0 {
            tags[len - 1] = tags[i];
        } else {
            tags[len] = tags[i];
            len += 1;
        }
    }
    len
}

/// A borrowed, normalized tag set used to look up an interned one without
/// allocating. Hashes exactly like [`TagSet`] (a slice of string pairs).
struct Query<'a>(&'a [(&'a str, &'a str)]);

impl Hash for Query<'_> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.hash(state);
    }
}

impl Equivalent<Arc<TagSet>> for Query<'_> {
    fn equivalent(&self, key: &Arc<TagSet>) -> bool {
        self.0.len() == key.pairs.len()
            && self
                .0
                .iter()
                .zip(key.pairs.iter())
                .all(|((qk, qv), (k, v))| *qk == &**k && *qv == &**v)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::hash::BuildHasher;

    #[test]
    fn query_hashes_like_the_stored_set() {
        let set = TagSet {
            pairs: vec![
                (Box::from("a"), Box::from("1")),
                (Box::from("b"), Box::from("2")),
            ]
            .into_boxed_slice(),
        };
        let pairs = [("a", "1"), ("b", "2")];
        let s = std::collections::hash_map::RandomState::new();
        assert_eq!(s.hash_one(Arc::new(set)), s.hash_one(Query(&pairs)));
    }
}
