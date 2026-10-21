//! Raw samples that cross process boundaries, so a manager's `--out` outputs
//! get its workers' samples. Metrics and tag sets are sent by value the first
//! time and by number after that, so a batch is mostly small numbers. Batches
//! must be decoded in the order they were encoded.

use std::collections::HashMap;
use std::time::{Duration, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use crate::metrics::Sample;
use crate::registry::{MetricId, Registry};
use crate::tags::{Interner, TagSetId};

#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct PortableSamples {
    /// Metric names first used in this batch; they're numbered on from the
    /// previous batches'.
    pub new_metrics: Vec<String>,
    /// Tag sets first used in this batch, numbered the same way.
    pub new_tag_sets: Vec<Vec<(String, String)>>,
    pub samples: Vec<PortableSample>,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct PortableSample {
    pub metric: u32,
    pub tags: u32,
    /// Microseconds since the Unix epoch.
    pub micros: u64,
    pub value: f64,
}

/// The sending side: remembers which metrics and tag sets it has sent.
#[derive(Default)]
pub struct SampleEncoder {
    metrics: HashMap<MetricId, u32>,
    tags: HashMap<TagSetId, u32>,
}

impl SampleEncoder {
    pub fn encode(
        &mut self,
        samples: &[Sample],
        registry: &Registry,
        interner: &Interner,
    ) -> PortableSamples {
        let mut batch = PortableSamples::default();
        for sample in samples {
            let next = self.metrics.len() as u32;
            let metric = *self.metrics.entry(sample.metric).or_insert_with(|| {
                batch
                    .new_metrics
                    .push(registry.def(sample.metric).name.clone());
                next
            });
            let next = self.tags.len() as u32;
            let tags = *self.tags.entry(sample.tags).or_insert_with(|| {
                let set = interner.get(sample.tags);
                batch.new_tag_sets.push(
                    set.iter()
                        .map(|(k, v)| (k.to_owned(), v.to_owned()))
                        .collect(),
                );
                next
            });
            let micros = sample
                .time
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_micros() as u64;
            batch.samples.push(PortableSample {
                metric,
                tags,
                micros,
                value: sample.value,
            });
        }
        batch
    }
}

/// The receiving side, one per sender.
#[derive(Default)]
pub struct SampleDecoder {
    /// `None` for a metric this process doesn't have (its samples are dropped).
    metrics: Vec<Option<MetricId>>,
    tags: Vec<TagSetId>,
}

impl SampleDecoder {
    pub fn decode(
        &mut self,
        batch: PortableSamples,
        registry: &Registry,
        interner: &Interner,
    ) -> Vec<Sample> {
        self.metrics
            .extend(batch.new_metrics.iter().map(|name| registry.get(name)));
        for set in &batch.new_tag_sets {
            let pairs: Vec<(&str, &str)> =
                set.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
            self.tags.push(interner.intern(&pairs));
        }
        batch
            .samples
            .iter()
            .filter_map(|s| {
                Some(Sample {
                    metric: (*self.metrics.get(s.metric as usize)?)?,
                    tags: *self.tags.get(s.tags as usize)?,
                    value: s.value,
                    time: UNIX_EPOCH + Duration::from_micros(s.micros),
                })
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::registry::{MetricKind, ValueType};

    #[test]
    fn batches_round_trip_between_registries() {
        // The receiver registers metrics in another order, and lacks one.
        let mut sender = Registry::new();
        let a = sender
            .register("a", MetricKind::Counter, ValueType::Default)
            .unwrap();
        let b = sender
            .register("b", MetricKind::Trend, ValueType::Time)
            .unwrap();
        let only_here = sender
            .register("only_sender", MetricKind::Counter, ValueType::Default)
            .unwrap();
        let mut receiver = Registry::new();
        let rb = receiver
            .register("b", MetricKind::Trend, ValueType::Time)
            .unwrap();
        let ra = receiver
            .register("a", MetricKind::Counter, ValueType::Default)
            .unwrap();
        let (send_tags, recv_tags) = (Interner::new(), Interner::new());
        let _ = recv_tags.intern(&[("other", "x")]);
        let t1 = send_tags.intern(&[("k", "1")]);
        let t2 = send_tags.intern(&[("k", "2"), ("z", "y")]);
        let time = UNIX_EPOCH + Duration::from_micros(1_700_000_000_123_456);
        let sample = |metric, tags, value| Sample {
            metric,
            tags,
            value,
            time,
        };

        let mut encoder = SampleEncoder::default();
        let mut decoder = SampleDecoder::default();
        let first = encoder.encode(
            &[
                sample(a, t1, 1.0),
                sample(b, t1, 2.5),
                sample(only_here, t2, 9.0),
            ],
            &sender,
            &send_tags,
        );
        assert_eq!(first.new_metrics, ["a", "b", "only_sender"]);
        assert_eq!(first.new_tag_sets.len(), 2);
        // Round-trips through postcard, as on the wire.
        let bytes = postcard::to_allocvec(&first).unwrap();
        let first: PortableSamples = postcard::from_bytes(&bytes).unwrap();
        let got = decoder.decode(first, &receiver, &recv_tags);
        assert_eq!(got.len(), 2, "the unknown metric's sample is dropped");
        assert_eq!((got[0].metric, got[0].value, got[0].time), (ra, 1.0, time));
        assert_eq!(got[1].metric, rb);
        assert_eq!(
            recv_tags.get(got[0].tags).iter().collect::<Vec<_>>(),
            [("k", "1")]
        );

        // Known metrics and tag sets go by number from then on.
        let second = encoder.encode(&[sample(b, t2, 3.0)], &sender, &send_tags);
        assert!(second.new_metrics.is_empty() && second.new_tag_sets.is_empty());
        let got = decoder.decode(second, &receiver, &recv_tags);
        assert_eq!(got[0].metric, rb);
        assert_eq!(
            recv_tags.get(got[0].tags).iter().collect::<Vec<_>>(),
            [("k", "2"), ("z", "y")]
        );
    }
}
