#[cfg(feature = "tracing")]
use std::time::Duration;

use crate::{
    manifest::Manifest,
    record::{
        AddReport, CompactionOptions, CompactionReport, GarbageCollectionOptions,
        GarbageCollectionReport, SearchOptions, SearchReport, SearchTerminationReason,
    },
    storage::StorageWriteReport,
};

#[cfg(feature = "tracing")]
use crate::record::SearchMode;

#[cfg(feature = "tracing")]
pub(crate) type Span = tracing::Span;

#[cfg(not(feature = "tracing"))]
#[derive(Debug, Clone, Copy)]
pub(crate) struct Span;

#[cfg(not(feature = "tracing"))]
#[derive(Debug, Clone, Copy)]
pub(crate) struct EnteredSpan;

#[cfg(not(feature = "tracing"))]
impl Span {
    pub(crate) const fn enter(&self) -> EnteredSpan {
        EnteredSpan
    }
}

#[cfg(feature = "tracing")]
pub(crate) fn open_span(resident_routing: bool) -> Span {
    tracing::info_span!(
        "borsuk.open",
        resident_routing,
        manifest_version = tracing::field::Empty,
        routing_max_level = tracing::field::Empty,
        segments = tracing::field::Empty
    )
}

#[cfg(not(feature = "tracing"))]
pub(crate) const fn open_span(_resident_routing: bool) -> Span {
    Span
}

#[cfg(feature = "tracing")]
pub(crate) fn record_open(span: &Span, manifest: &Manifest) {
    record_u64(span, "manifest_version", manifest.version);
    record_usize(
        span,
        "routing_max_level",
        usize::from(manifest.routing_max_level),
    );
    record_usize(span, "segments", manifest.segments.len());
}

#[cfg(not(feature = "tracing"))]
pub(crate) fn record_open(_span: &Span, _manifest: &Manifest) {}

#[cfg(feature = "tracing")]
pub(crate) fn add_span(vectors_added: usize, base_manifest_version: u64) -> Span {
    tracing::info_span!(
        "borsuk.add",
        vectors_added = vectors_added as u64,
        base_manifest_version,
        manifest_version = tracing::field::Empty,
        segments_written = tracing::field::Empty,
        graph_payloads_written = tracing::field::Empty,
        manifest_tables_written = tracing::field::Empty,
        routing_pages_written = tracing::field::Empty,
        total_bytes_written = tracing::field::Empty
    )
}

#[cfg(not(feature = "tracing"))]
pub(crate) const fn add_span(_vectors_added: usize, _base_manifest_version: u64) -> Span {
    Span
}

#[cfg(feature = "tracing")]
pub(crate) fn record_add_report(span: &Span, report: &AddReport, manifest_version: u64) {
    record_u64(span, "manifest_version", manifest_version);
    record_usize(span, "segments_written", report.segments_written);
    record_usize(
        span,
        "graph_payloads_written",
        report.graph_payloads_written,
    );
    record_usize(
        span,
        "manifest_tables_written",
        report.manifest_tables_written,
    );
    record_usize(span, "routing_pages_written", report.routing_pages_written);
    record_u64(span, "total_bytes_written", report.total_bytes_written);
}

#[cfg(not(feature = "tracing"))]
pub(crate) fn record_add_report(_span: &Span, _report: &AddReport, _manifest_version: u64) {}

#[cfg(feature = "tracing")]
pub(crate) fn compact_span(options: &CompactionOptions, base_manifest_version: u64) -> Span {
    tracing::info_span!(
        "borsuk.compact",
        source_level = options.source_level,
        target_level = options.target_level,
        max_segments = ?options.max_segments,
        min_segments = options.min_segments as u64,
        target_segment_max_vectors = ?options.target_segment_max_vectors,
        base_manifest_version,
        compacted = tracing::field::Empty,
        manifest_version = tracing::field::Empty,
        segments_read = tracing::field::Empty,
        segments_written = tracing::field::Empty,
        records_rewritten = tracing::field::Empty,
        bytes_read = tracing::field::Empty,
        bytes_written = tracing::field::Empty
    )
}

#[cfg(not(feature = "tracing"))]
pub(crate) const fn compact_span(
    _options: &CompactionOptions,
    _base_manifest_version: u64,
) -> Span {
    Span
}

#[cfg(feature = "tracing")]
pub(crate) fn record_compaction_report(span: &Span, report: &CompactionReport) {
    span.record("compacted", report.compacted);
    record_u64(span, "manifest_version", report.manifest_version);
    record_usize(span, "segments_read", report.segments_read);
    record_usize(span, "segments_written", report.segments_written);
    record_usize(span, "records_rewritten", report.records_rewritten);
    record_u64(span, "bytes_read", report.bytes_read);
    record_u64(span, "bytes_written", report.bytes_written);
}

#[cfg(not(feature = "tracing"))]
pub(crate) fn record_compaction_report(_span: &Span, _report: &CompactionReport) {}

#[cfg(feature = "tracing")]
pub(crate) fn gc_span(options: &GarbageCollectionOptions, manifest_version: u64) -> Span {
    tracing::info_span!(
        "borsuk.gc",
        dry_run = options.dry_run,
        min_age_ms = duration_millis(options.min_age),
        manifest_version,
        objects_scanned = tracing::field::Empty,
        objects_deleted = tracing::field::Empty,
        routing_objects_deleted = tracing::field::Empty,
        tables_deleted = tracing::field::Empty,
        bytes_reclaimable = tracing::field::Empty,
        bytes_reclaimed = tracing::field::Empty
    )
}

#[cfg(not(feature = "tracing"))]
pub(crate) const fn gc_span(_options: &GarbageCollectionOptions, _manifest_version: u64) -> Span {
    Span
}

#[cfg(feature = "tracing")]
pub(crate) fn record_gc_report(span: &Span, report: &GarbageCollectionReport) {
    record_usize(span, "objects_scanned", report.objects_scanned);
    record_usize(span, "objects_deleted", report.objects_deleted);
    record_usize(
        span,
        "routing_objects_deleted",
        report.routing_objects_deleted,
    );
    record_usize(span, "tables_deleted", report.tables_deleted);
    record_u64(span, "bytes_reclaimable", report.bytes_reclaimable);
    record_u64(span, "bytes_reclaimed", report.bytes_reclaimed);
}

#[cfg(not(feature = "tracing"))]
pub(crate) fn record_gc_report(_span: &Span, _report: &GarbageCollectionReport) {}

#[cfg(feature = "tracing")]
pub(crate) fn search_span(
    query_dimensions: usize,
    options: &SearchOptions,
    manifest_version: u64,
) -> Span {
    tracing::info_span!(
        "borsuk.search",
        k = options.k as u64,
        query_dimensions = query_dimensions as u64,
        mode = search_mode_name(&options.mode),
        leaf_mode = %options.mode.leaf_mode(),
        guaranteed_recall = options.guaranteed_recall,
        prefetch_depth = options.prefetch_depth as u64,
        manifest_version,
        hits = tracing::field::Empty,
        termination_reason = tracing::field::Empty,
        recall_guarantee = tracing::field::Empty,
        segments_total = tracing::field::Empty,
        segments_searched = tracing::field::Empty,
        segments_skipped = tracing::field::Empty,
        routing_page_indexes_read = tracing::field::Empty,
        routing_pages_read = tracing::field::Empty,
        bytes_read = tracing::field::Empty,
        prefetched_bytes_unused = tracing::field::Empty,
        graph_bytes_read = tracing::field::Empty,
        object_cache_hits = tracing::field::Empty,
        object_cache_misses = tracing::field::Empty,
        cache_repairs = tracing::field::Empty,
        records_considered = tracing::field::Empty,
        records_scored = tracing::field::Empty,
        graph_candidates_added = tracing::field::Empty
    )
}

#[cfg(not(feature = "tracing"))]
pub(crate) const fn search_span(
    _query_dimensions: usize,
    _options: &SearchOptions,
    _manifest_version: u64,
) -> Span {
    Span
}

#[cfg(feature = "tracing")]
pub(crate) fn record_search_report(span: &Span, report: &SearchReport) {
    record_usize(span, "hits", report.hits.len());
    span.record("termination_reason", report.termination_reason.as_str());
    span.record("recall_guarantee", report.recall_guarantee.as_str());
    record_usize(span, "segments_total", report.segments_total);
    record_usize(span, "segments_searched", report.segments_searched);
    record_usize(span, "segments_skipped", report.segments_skipped);
    record_usize(
        span,
        "routing_page_indexes_read",
        report.routing_page_indexes_read,
    );
    record_usize(span, "routing_pages_read", report.routing_pages_read);
    record_u64(span, "bytes_read", report.bytes_read);
    record_u64(
        span,
        "prefetched_bytes_unused",
        report.prefetched_bytes_unused,
    );
    record_u64(span, "graph_bytes_read", report.graph_bytes_read);
    record_usize(span, "object_cache_hits", report.object_cache_hits);
    record_usize(span, "object_cache_misses", report.object_cache_misses);
    record_usize(span, "cache_repairs", report.cache_repairs);
    record_usize(span, "records_considered", report.records_considered);
    record_usize(span, "records_scored", report.records_scored);
    record_usize(
        span,
        "graph_candidates_added",
        report.graph_candidates_added,
    );
}

#[cfg(not(feature = "tracing"))]
pub(crate) fn record_search_report(_span: &Span, _report: &SearchReport) {}

#[cfg(feature = "tracing")]
pub(crate) fn publish_span(manifest_version: u64) -> Span {
    tracing::info_span!(
        "borsuk.publish",
        manifest_version,
        routing_max_level = tracing::field::Empty,
        manifest_tables_written = tracing::field::Empty,
        routing_pages_written = tracing::field::Empty,
        total_bytes_written = tracing::field::Empty
    )
}

#[cfg(not(feature = "tracing"))]
pub(crate) const fn publish_span(_manifest_version: u64) -> Span {
    Span
}

#[cfg(feature = "tracing")]
pub(crate) fn record_publish_report(span: &Span, manifest: &Manifest, report: &StorageWriteReport) {
    record_usize(
        span,
        "routing_max_level",
        usize::from(manifest.routing_max_level),
    );
    record_usize(
        span,
        "manifest_tables_written",
        report.metadata_tables_written,
    );
    record_usize(span, "routing_pages_written", report.routing_pages_written);
    record_u64(span, "total_bytes_written", report.bytes_written);
}

#[cfg(not(feature = "tracing"))]
pub(crate) fn record_publish_report(
    _span: &Span,
    _manifest: &Manifest,
    _report: &StorageWriteReport,
) {
}

#[cfg(feature = "tracing")]
pub(crate) fn segment_skip_event(reason: SearchTerminationReason, segments_skipped: usize) {
    tracing::debug!(
        reason = reason.as_str(),
        segments_skipped = segments_skipped as u64,
        "segment_skip"
    );
}

#[cfg(not(feature = "tracing"))]
pub(crate) const fn segment_skip_event(_reason: SearchTerminationReason, _segments_skipped: usize) {
}

#[cfg(feature = "tracing")]
fn search_mode_name(mode: &SearchMode) -> &'static str {
    match mode {
        SearchMode::Exact => "exact",
        SearchMode::Approx { .. } => "approx",
    }
}

#[cfg(feature = "tracing")]
fn duration_millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

#[cfg(feature = "tracing")]
fn record_usize(span: &Span, field: &'static str, value: usize) {
    span.record(field, value as u64);
}

#[cfg(feature = "tracing")]
fn record_u64(span: &Span, field: &'static str, value: u64) {
    span.record(field, value);
}
