//! Feature-gated tracing smoke tests.

#![cfg(feature = "tracing")]
#![allow(missing_docs)]

use std::{
    collections::BTreeMap,
    fmt,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use borsuk::{
    BorsukIndex, CompactionOptions, GarbageCollectionOptions, IndexConfig, LeafMode, SearchMode,
    SearchOptions, VectorMetric, VectorRecord,
};
use tracing::{
    Event, Id, Metadata, Subscriber,
    field::{Field, Visit},
    span::{Attributes, Record},
};

#[derive(Debug, Clone)]
struct CapturedSpan {
    name: String,
    fields: BTreeMap<String, String>,
}

#[derive(Debug, Clone)]
struct CapturedEvent {
    fields: BTreeMap<String, String>,
}

#[derive(Debug, Default)]
struct CaptureInner {
    next_id: AtomicU64,
    spans: Mutex<BTreeMap<u64, CapturedSpan>>,
    events: Mutex<Vec<CapturedEvent>>,
}

#[derive(Debug)]
struct CaptureSubscriber {
    inner: Arc<CaptureInner>,
}

#[derive(Debug, Default)]
struct FieldVisitor {
    fields: BTreeMap<String, String>,
}

impl Visit for FieldVisitor {
    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        self.fields
            .insert(field.name().to_string(), normalize_debug_value(value));
    }
}

impl Subscriber for CaptureSubscriber {
    fn enabled(&self, _metadata: &Metadata<'_>) -> bool {
        true
    }

    fn new_span(&self, attributes: &Attributes<'_>) -> Id {
        let id = self.inner.next_id.fetch_add(1, Ordering::Relaxed) + 1;
        let mut visitor = FieldVisitor::default();
        attributes.record(&mut visitor);
        self.inner.spans.lock().unwrap().insert(
            id,
            CapturedSpan {
                name: attributes.metadata().name().to_string(),
                fields: visitor.fields,
            },
        );
        Id::from_u64(id)
    }

    fn record(&self, span: &Id, values: &Record<'_>) {
        let mut visitor = FieldVisitor::default();
        values.record(&mut visitor);
        if let Some(captured) = self.inner.spans.lock().unwrap().get_mut(&span.into_u64()) {
            captured.fields.extend(visitor.fields);
        }
    }

    fn record_follows_from(&self, _span: &Id, _follows: &Id) {}

    fn event(&self, event: &Event<'_>) {
        let mut visitor = FieldVisitor::default();
        event.record(&mut visitor);
        self.inner.events.lock().unwrap().push(CapturedEvent {
            fields: visitor.fields,
        });
    }

    fn enter(&self, _span: &Id) {}

    fn exit(&self, _span: &Id) {}
}

#[test]
fn tracing_feature_emits_operation_spans_and_segment_skip_reason() {
    let capture = Arc::new(CaptureInner::default());
    let subscriber = CaptureSubscriber {
        inner: Arc::clone(&capture),
    };
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();

    tracing::subscriber::with_default(subscriber, || {
        let mut index = BorsukIndex::create(IndexConfig {
            uri: uri.clone(),
            metric: VectorMetric::Euclidean,
            dimensions: 2,
            segment_max_vectors: 2,
            ram_budget_bytes: None,
        })
        .unwrap();

        index
            .add(vec![
                VectorRecord::new("a", vec![0.0, 0.0]),
                VectorRecord::new("b", vec![0.1, 0.0]),
                VectorRecord::new("c", vec![1.0, 0.0]),
                VectorRecord::new("d", vec![1.1, 0.0]),
                VectorRecord::new("e", vec![2.0, 0.0]),
                VectorRecord::new("f", vec![2.1, 0.0]),
            ])
            .unwrap();

        let mut reopened = BorsukIndex::open(&uri).unwrap();
        reopened
            .compact(CompactionOptions {
                source_level: 0,
                target_level: 1,
                max_segments: Some(2),
                min_segments: 2,
                target_segment_max_vectors: Some(4),
                target_segment_max_radius: None,
            })
            .unwrap();
        reopened
            .gc_obsolete_segments(GarbageCollectionOptions {
                dry_run: true,
                min_age: Duration::ZERO,
            })
            .unwrap();
        reopened
            .search_with_report(
                &[0.0, 0.0],
                SearchOptions {
                    k: 2,
                    mode: SearchMode::Approx {
                        leaf_mode: LeafMode::FlatScan,
                        eps: None,
                        max_segments: Some(1),
                        max_bytes: None,
                        max_latency_ms: None,
                        routing_page_overfetch: None,
                        max_candidates_per_segment: None,
                    },
                    guaranteed_recall: false,
                    prefetch_depth: 1,
                },
            )
            .unwrap();
    });

    let spans = capture
        .spans
        .lock()
        .unwrap()
        .values()
        .cloned()
        .collect::<Vec<_>>();
    for expected in [
        "borsuk.open",
        "borsuk.add",
        "borsuk.compact",
        "borsuk.publish",
        "borsuk.gc",
        "borsuk.search",
    ] {
        assert!(
            spans.iter().any(|span| span.name == expected),
            "missing span {expected}; captured spans: {spans:?}"
        );
    }

    assert_span_field(&spans, "borsuk.add", "segments_written", "3");
    assert_span_field(&spans, "borsuk.compact", "compacted", "true");
    assert_span_has_field(&spans, "borsuk.publish", "manifest_tables_written");
    assert_span_has_field(&spans, "borsuk.gc", "objects_scanned");
    assert_span_field(
        &spans,
        "borsuk.search",
        "termination_reason",
        "max-segments",
    );
    assert_span_field(&spans, "borsuk.search", "segments_skipped", "1");
    assert_span_has_field(&spans, "borsuk.search", "records_scored");

    let events = capture.events.lock().unwrap().clone();
    assert!(
        events.iter().any(|event| {
            event.fields.get("reason").map(String::as_str) == Some("max-segments")
                && event.fields.contains_key("segments_skipped")
        }),
        "missing segment skip event with reason; captured events: {events:?}"
    );
}

fn assert_span_field(spans: &[CapturedSpan], span_name: &str, field: &str, expected: &str) {
    assert!(
        spans
            .iter()
            .filter(|span| span.name == span_name)
            .any(|span| span.fields.get(field).map(String::as_str) == Some(expected)),
        "missing {span_name}.{field}={expected}; captured spans: {spans:?}"
    );
}

fn assert_span_has_field(spans: &[CapturedSpan], span_name: &str, field: &str) {
    assert!(
        spans
            .iter()
            .filter(|span| span.name == span_name)
            .any(|span| span.fields.contains_key(field)),
        "missing {span_name}.{field}; captured spans: {spans:?}"
    );
}

fn normalize_debug_value(value: &dyn fmt::Debug) -> String {
    let value = format!("{value:?}");
    value
        .strip_prefix('"')
        .and_then(|value| value.strip_suffix('"'))
        .unwrap_or(&value)
        .to_string()
}
