use std::{
    cmp::Ordering,
    collections::{HashMap, HashSet},
    path::PathBuf,
    time::Instant,
};

use uuid::Uuid;

use crate::{
    error::{BorsukError, Result},
    format::{
        graph_from_parquet, graph_to_parquet, routing_layer_page_from_parquet,
        segment_from_parquet, segment_to_parquet,
    },
    manifest::{
        Manifest, ROUTING_PAGE_FANOUT, RoutingLayerPageRef, SegmentSummary, segment_id_bloom,
        segment_vector_signature_bloom,
    },
    metric::VectorMetric,
    record::{
        CompactionOptions, CompactionReport, GarbageCollectionOptions, GarbageCollectionReport,
        IndexStats, LeafMode, SearchHit, SearchMode, SearchOptions, SearchReport, VectorRecord,
    },
    segment::{
        Segment, SegmentGraph, pq_code_for_query, routing_code, vector_locality_key,
        vector_signature,
    },
    storage::Storage,
};

const LOCAL_GRAPH_NEIGHBORS: usize = 8;

#[derive(Debug, Default)]
struct RoutingSummariesRead {
    summaries: Vec<SegmentSummary>,
    bytes_read: u64,
    object_cache_hits: usize,
    object_cache_misses: usize,
}

/// Parse a human-readable byte budget.
///
/// Accepts plain bytes (`"1024"`), bytes (`"1024B"`), decimal units
/// (`KB`, `MB`, `GB`, `TB`), and binary units (`KiB`, `MiB`, `GiB`, `TiB`).
pub fn parse_byte_size(value: &str, field_name: &str) -> Result<u64> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(BorsukError::InvalidMetricInput(format!(
            "{field_name} must not be empty"
        )));
    }

    let split_at = trimmed
        .find(|ch: char| !ch.is_ascii_digit())
        .unwrap_or(trimmed.len());
    if split_at == 0 {
        return Err(BorsukError::InvalidMetricInput(format!(
            "{field_name} `{value}` must start with an integer byte count"
        )));
    }

    let amount = trimmed[..split_at].parse::<u64>().map_err(|err| {
        BorsukError::InvalidMetricInput(format!("invalid {field_name} `{value}`: {err}"))
    })?;
    let unit = trimmed[split_at..].trim().to_ascii_uppercase();
    let multiplier = match unit.as_str() {
        "" | "B" => 1_u64,
        "KB" => 1_000,
        "MB" => 1_000_000,
        "GB" => 1_000_000_000,
        "TB" => 1_000_000_000_000,
        "KIB" => 1_024,
        "MIB" => 1_048_576,
        "GIB" => 1_073_741_824,
        "TIB" => 1_099_511_627_776,
        _ => {
            return Err(BorsukError::InvalidMetricInput(format!(
                "unknown {field_name} unit `{}`",
                trimmed[split_at..].trim()
            )));
        }
    };

    amount.checked_mul(multiplier).ok_or_else(|| {
        BorsukError::InvalidMetricInput(format!("{field_name} `{value}` exceeds u64"))
    })
}

/// Parse a human-readable resident RAM budget.
///
/// Accepts the same units as [`parse_byte_size`].
pub fn parse_ram_budget(value: &str) -> Result<u64> {
    parse_byte_size(value, "ram_budget")
}

/// Configuration used when creating a new BORSUK index.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct IndexConfig {
    /// Index root URI. Plain local paths, `file://...`, and object-store URIs are supported.
    pub uri: String,
    /// Metric fixed for this physical index.
    pub metric: VectorMetric,
    /// Required vector dimensionality.
    pub dimensions: usize,
    /// Maximum number of vectors written to each immutable segment.
    pub segment_max_vectors: usize,
    /// Optional resident manifest/routing memory budget in bytes.
    pub ram_budget_bytes: Option<u64>,
}

/// Options used when opening an existing BORSUK index.
#[derive(Debug, Clone, Default)]
pub struct OpenOptions {
    /// Optional local read-through cache directory.
    pub cache_dir: Option<PathBuf>,
    /// Optional runtime resident manifest/routing memory budget in bytes.
    pub ram_budget_bytes: Option<u64>,
}

/// A BORSUK index handle.
#[derive(Debug, Clone)]
pub struct BorsukIndex {
    storage: Storage,
    manifest: Manifest,
    runtime_ram_budget_bytes: Option<u64>,
}

#[derive(Debug, Clone, Copy, Default)]
struct StatsTotals {
    segments: usize,
    records: usize,
    segment_bytes: u64,
    graph_bytes: u64,
}

impl BorsukIndex {
    /// Create a new empty index and publish its first manifest.
    pub fn create(config: IndexConfig) -> Result<Self> {
        Self::create_with_cache(config, None)
    }

    /// Create a new empty index with an optional local read-through cache.
    pub fn create_with_cache(config: IndexConfig, cache_dir: Option<PathBuf>) -> Result<Self> {
        if config.dimensions == 0 {
            return Err(BorsukError::InvalidMetricInput(
                "index dimensions must be greater than zero".to_string(),
            ));
        }

        if config.segment_max_vectors == 0 {
            return Err(BorsukError::InvalidMetricInput(
                "segment_max_vectors must be greater than zero".to_string(),
            ));
        }

        let storage = if let Some(cache_dir) = cache_dir {
            Storage::from_uri_with_cache(&config.uri, Some(cache_dir))?
        } else {
            Storage::from_uri(&config.uri)?
        };
        storage.create_layout()?;

        let manifest = Manifest::new(config);
        enforce_ram_budget(&manifest, None)?;
        storage.publish_manifest(&manifest)?;

        Ok(Self {
            storage,
            manifest,
            runtime_ram_budget_bytes: None,
        })
    }

    /// Open an existing index from a local URI or path.
    pub fn open(uri: &str) -> Result<Self> {
        Self::open_with_options(uri, OpenOptions::default())
    }

    /// Open an existing index with an optional local read-through cache.
    pub fn open_with_cache(uri: &str, cache_dir: Option<PathBuf>) -> Result<Self> {
        Self::open_with_options(
            uri,
            OpenOptions {
                cache_dir,
                ram_budget_bytes: None,
            },
        )
    }

    /// Open an existing index with cache and runtime budget options.
    pub fn open_with_options(uri: &str, options: OpenOptions) -> Result<Self> {
        let storage = if let Some(cache_dir) = options.cache_dir {
            Storage::from_uri_with_cache(uri, Some(cache_dir))?
        } else {
            Storage::from_uri(uri)?
        };
        let manifest = storage.load_current_manifest()?;
        enforce_ram_budget(&manifest, options.ram_budget_bytes)?;
        Ok(Self {
            storage,
            manifest,
            runtime_ram_budget_bytes: options.ram_budget_bytes,
        })
    }

    /// Return the active manifest metadata.
    #[must_use]
    pub fn manifest(&self) -> &Manifest {
        &self.manifest
    }

    /// Return active index statistics without scanning segment or graph payloads.
    #[must_use]
    pub fn stats(&self) -> IndexStats {
        self.try_stats().unwrap_or_else(|_| {
            let totals = self.manifest_stats_totals();
            self.index_stats_from_totals(totals)
        })
    }

    /// Return active index statistics or an error when required metadata is corrupt.
    pub fn try_stats(&self) -> Result<IndexStats> {
        let totals = self.stats_totals()?;
        Ok(self.index_stats_from_totals(totals))
    }

    fn index_stats_from_totals(&self, totals: StatsTotals) -> IndexStats {
        IndexStats {
            metric: self.manifest.config.metric.to_string(),
            dimensions: self.manifest.config.dimensions,
            segment_max_vectors: self.manifest.config.segment_max_vectors,
            ram_budget_bytes: self.effective_ram_budget_bytes(),
            manifest_version: self.manifest.version,
            segments: totals.segments,
            records: totals.records,
            segment_bytes: totals.segment_bytes,
            graph_bytes: totals.graph_bytes,
            resident_bytes_estimate: self.manifest.resident_bytes_estimate(),
        }
    }

    fn stats_totals(&self) -> Result<StatsTotals> {
        if !self.manifest.segments.is_empty() {
            return Ok(self.manifest_stats_totals());
        }

        let page_refs = self
            .storage
            .read_routing_layer_page_index(self.manifest.version, 0)?;

        Ok(StatsTotals {
            segments: page_refs
                .iter()
                .map(|page_ref| page_ref.page_segments)
                .sum(),
            records: page_refs.iter().map(|page_ref| page_ref.page_records).sum(),
            segment_bytes: page_refs
                .iter()
                .map(|page_ref| page_ref.page_segment_bytes)
                .sum(),
            graph_bytes: page_refs
                .iter()
                .map(|page_ref| page_ref.page_graph_bytes)
                .sum(),
        })
    }

    fn manifest_stats_totals(&self) -> StatsTotals {
        StatsTotals {
            segments: self.manifest.segments.len(),
            records: self
                .manifest
                .segments
                .iter()
                .map(|segment| segment.object_count)
                .sum(),
            segment_bytes: self
                .manifest
                .segments
                .iter()
                .map(|segment| segment.size_bytes)
                .sum(),
            graph_bytes: self
                .manifest
                .segments
                .iter()
                .map(|segment| segment.graph_size_bytes)
                .sum(),
        }
    }

    /// Add records by writing one or more immutable L0 segments and publishing a new manifest.
    pub fn add(&mut self, records: Vec<VectorRecord>) -> Result<()> {
        let next_generated_id =
            next_generated_id_after_explicit_records(self.manifest.next_generated_id, &records)?;
        self.add_records(records, true, next_generated_id)
    }

    /// Add vectors with generated collision-free numeric ids.
    pub fn add_vectors(&mut self, vectors: Vec<Vec<f32>>) -> Result<Vec<String>> {
        let ids = self.generate_ids(vectors.len())?;
        let records = records_from_ids_and_vectors(ids.clone(), vectors)?;
        let next_generated_id = advance_generated_id(self.manifest.next_generated_id, ids.len())?;
        self.add_records(records, false, next_generated_id)?;
        Ok(ids)
    }

    /// Add vectors with caller-supplied ids.
    pub fn add_vectors_with_ids(
        &mut self,
        vectors: Vec<Vec<f32>>,
        ids: Vec<String>,
    ) -> Result<Vec<String>> {
        let records = records_from_ids_and_vectors(ids.clone(), vectors)?;
        self.add(records)?;
        Ok(ids)
    }

    fn add_records(
        &mut self,
        records: Vec<VectorRecord>,
        scan_existing_ids: bool,
        next_generated_id: u64,
    ) -> Result<()> {
        if records.is_empty() {
            return Ok(());
        }

        for record in &records {
            self.validate_vector(&record.vector)?;
        }
        self.validate_record_ids(&records, scan_existing_ids)?;

        let page_refs = self
            .storage
            .read_routing_layer_page_index(self.manifest.version, 0)?;
        if self.manifest.segments.is_empty() && !page_refs.is_empty() {
            return self.add_records_to_routing_page_refs(records, next_generated_id, page_refs);
        }

        let chunks = records.chunks(self.manifest.config.segment_max_vectors);
        let mut manifest = self.manifest.next_version();
        manifest.next_generated_id = next_generated_id;

        for chunk in chunks {
            let segment_id = Uuid::new_v4().to_string();
            let segment = Segment::from_records(
                segment_id.clone(),
                0,
                self.manifest.config.metric.clone(),
                self.manifest.config.dimensions,
                chunk.to_vec(),
            )?;
            manifest.segments.push(self.write_segment(segment)?);
        }

        manifest.rebuild_pivots();
        enforce_ram_budget(&manifest, self.runtime_ram_budget_bytes)?;
        self.storage
            .publish_manifest_reusing_routing_pages(&manifest, Some(&self.manifest))?;
        self.manifest = manifest;
        Ok(())
    }

    fn add_records_to_routing_page_refs(
        &mut self,
        records: Vec<VectorRecord>,
        next_generated_id: u64,
        mut page_refs: Vec<RoutingLayerPageRef>,
    ) -> Result<()> {
        let chunks = records.chunks(self.manifest.config.segment_max_vectors);
        let mut manifest = self.manifest.next_version();
        manifest.segments.clear();
        manifest.pivots.clear();
        manifest.next_generated_id = next_generated_id;

        let mut new_summaries = Vec::<SegmentSummary>::new();
        for chunk in chunks {
            let segment_id = Uuid::new_v4().to_string();
            let segment = Segment::from_records(
                segment_id,
                0,
                self.manifest.config.metric.clone(),
                self.manifest.config.dimensions,
                chunk.to_vec(),
            )?;
            new_summaries.push(self.write_segment(segment)?);
        }

        for summaries in new_summaries.chunks(ROUTING_PAGE_FANOUT) {
            let page_ordinal = page_refs.len();
            let page_ref =
                self.storage
                    .write_routing_layer_page(&manifest, 0, page_ordinal, summaries)?;
            page_refs.push(page_ref);
        }

        enforce_ram_budget(&manifest, self.runtime_ram_budget_bytes)?;
        self.storage
            .publish_manifest_with_routing_page_refs(&manifest, &page_refs)?;
        self.manifest = manifest;
        Ok(())
    }

    /// Generate collision-free numeric string ids without scanning segment payloads.
    pub fn generate_ids(&self, count: usize) -> Result<Vec<String>> {
        let start = self.manifest.next_generated_id;
        let end = advance_generated_id(start, count)?;
        Ok((start..end).map(|id| id.to_string()).collect())
    }

    /// Load a stored vector by its identifier.
    pub fn get_vector(&self, id: &str) -> Result<Option<Vec<f32>>> {
        if id.trim().is_empty() {
            return Err(BorsukError::InvalidRecordInput(
                "record ids must not be empty".to_string(),
            ));
        }

        for summary in self.manifest.segments.iter().rev() {
            if !summary.might_contain_record_id(id) {
                continue;
            }
            let (segment, _, _) = self.read_segment(summary)?;
            if let Some(record) = segment.records.iter().rev().find(|record| record.id == id) {
                return Ok(Some(record.vector.clone()));
            }
        }

        if self.manifest.segments.is_empty() {
            return self.get_vector_from_routing_pages(id);
        }

        Ok(None)
    }

    fn get_vector_from_routing_pages(&self, id: &str) -> Result<Option<Vec<f32>>> {
        let mut page_refs = self
            .storage
            .read_routing_layer_page_index(self.manifest.version, 0)?;
        page_refs.retain(|page_ref| page_ref.might_contain_record_id(id));

        for page_ref in page_refs.iter().rev() {
            let summaries =
                self.routing_summaries_from_page_refs(std::slice::from_ref(page_ref))?;
            for summary in summaries.iter().rev() {
                if !summary.might_contain_record_id(id) {
                    continue;
                }
                let (segment, _, _) = self.read_segment(summary)?;
                if let Some(record) = segment.records.iter().rev().find(|record| record.id == id) {
                    return Ok(Some(record.vector.clone()));
                }
            }
        }

        Ok(None)
    }

    fn validate_record_ids(&self, records: &[VectorRecord], scan_existing_ids: bool) -> Result<()> {
        let mut batch_ids = HashSet::<&str>::with_capacity(records.len());
        for record in records {
            if record.id.trim().is_empty() {
                return Err(BorsukError::InvalidRecordInput(
                    "record ids must not be empty".to_string(),
                ));
            }
            if !batch_ids.insert(record.id.as_str()) {
                return Err(BorsukError::InvalidRecordInput(format!(
                    "duplicate record id `{}` in add batch",
                    record.id
                )));
            }
        }

        if scan_existing_ids {
            self.validate_record_ids_against_existing_segments(records)?;
        }

        Ok(())
    }

    fn validate_record_ids_against_existing_segments(
        &self,
        records: &[VectorRecord],
    ) -> Result<()> {
        if self.manifest.segments.is_empty() {
            return self.validate_record_ids_against_routing_pages(records);
        }

        for summary in &self.manifest.segments {
            if !records
                .iter()
                .any(|record| summary.might_contain_record_id(&record.id))
            {
                continue;
            }

            let (segment, _, _) = self.read_segment(summary)?;
            for record in records {
                if segment
                    .records
                    .iter()
                    .any(|existing| existing.id == record.id)
                {
                    return Err(BorsukError::InvalidRecordInput(format!(
                        "duplicate record id `{}` already exists",
                        record.id
                    )));
                }
            }
        }

        Ok(())
    }

    fn validate_record_ids_against_routing_pages(&self, records: &[VectorRecord]) -> Result<()> {
        let page_refs = self
            .storage
            .read_routing_layer_page_index(self.manifest.version, 0)?;
        for page_ref in page_refs.iter().rev() {
            if !records
                .iter()
                .any(|record| page_ref.might_contain_record_id(&record.id))
            {
                continue;
            }

            let summaries =
                self.routing_summaries_from_page_refs(std::slice::from_ref(page_ref))?;
            for summary in summaries.iter().rev() {
                if !records
                    .iter()
                    .any(|record| summary.might_contain_record_id(&record.id))
                {
                    continue;
                }

                let (segment, _, _) = self.read_segment(summary)?;
                for record in records {
                    if segment
                        .records
                        .iter()
                        .any(|existing| existing.id == record.id)
                    {
                        return Err(BorsukError::InvalidRecordInput(format!(
                            "duplicate record id `{}` already exists",
                            record.id
                        )));
                    }
                }
            }
        }

        Ok(())
    }

    /// Compact immutable segments out-of-place into a higher target level.
    pub fn compact(&mut self, options: CompactionOptions) -> Result<CompactionReport> {
        validate_compaction_options(&options)?;

        let max_segments = options.max_segments.unwrap_or(usize::MAX);
        let page_refs = self
            .storage
            .read_routing_layer_page_index(self.manifest.version, 0)?;
        if self.manifest.segments.is_empty() && !page_refs.is_empty() {
            return self.compact_from_routing_page_refs(options, max_segments, page_refs);
        }

        let active_summaries = self.active_segment_summaries()?;
        let selected = active_summaries
            .iter()
            .filter(|summary| summary.level == options.source_level)
            .take(max_segments)
            .cloned()
            .collect::<Vec<_>>();

        if selected.len() < options.min_segments {
            return Ok(CompactionReport {
                compacted: false,
                source_level: options.source_level,
                target_level: options.target_level,
                segments_read: 0,
                segments_written: 0,
                records_rewritten: 0,
                bytes_read: 0,
                bytes_written: 0,
                object_cache_hits: 0,
                object_cache_misses: 0,
                manifest_version: self.manifest.version,
            });
        }

        let target_segment_max_vectors = options
            .target_segment_max_vectors
            .unwrap_or(self.manifest.config.segment_max_vectors);
        if target_segment_max_vectors == 0 {
            return Err(BorsukError::InvalidCompactionInput(
                "target_segment_max_vectors must be greater than zero".to_string(),
            ));
        }

        let mut records = Vec::<VectorRecord>::new();
        let mut bytes_read = 0_u64;
        let mut object_cache_hits = 0_usize;
        let mut object_cache_misses = 0_usize;

        for summary in &selected {
            let (segment, segment_bytes_read, segment_cache_hit) = self.read_segment(summary)?;
            bytes_read += segment_bytes_read;
            count_cache_read(
                segment_cache_hit,
                &mut object_cache_hits,
                &mut object_cache_misses,
            );
            records.extend(segment.records);
        }
        sort_records_by_vector_locality(
            &mut records,
            self.manifest.config.dimensions,
            target_segment_max_vectors,
        );

        let selected_ids = selected
            .iter()
            .map(|summary| summary.id.as_str())
            .collect::<HashSet<_>>();
        let mut manifest = self.manifest.next_version();
        manifest.segments = active_summaries;
        manifest
            .segments
            .retain(|summary| !selected_ids.contains(summary.id.as_str()));

        let mut segments_written = 0_usize;
        let mut bytes_written = 0_u64;

        for chunk in records.chunks(target_segment_max_vectors) {
            let segment_id = Uuid::new_v4().to_string();
            let segment = Segment::from_records(
                segment_id,
                options.target_level,
                self.manifest.config.metric.clone(),
                self.manifest.config.dimensions,
                chunk.to_vec(),
            )?;
            let summary = self.write_segment(segment)?;
            bytes_written += summary.size_bytes;
            segments_written += 1;
            manifest.segments.push(summary);
        }

        manifest.rebuild_pivots();
        enforce_ram_budget(&manifest, self.runtime_ram_budget_bytes)?;
        self.storage
            .publish_manifest_reusing_routing_pages(&manifest, Some(&self.manifest))?;
        self.manifest = manifest;

        Ok(CompactionReport {
            compacted: true,
            source_level: options.source_level,
            target_level: options.target_level,
            segments_read: selected.len(),
            segments_written,
            records_rewritten: records.len(),
            bytes_read,
            bytes_written,
            object_cache_hits,
            object_cache_misses,
            manifest_version: self.manifest.version,
        })
    }

    fn compact_from_routing_page_refs(
        &mut self,
        options: CompactionOptions,
        max_segments: usize,
        mut page_refs: Vec<RoutingLayerPageRef>,
    ) -> Result<CompactionReport> {
        let mut selected_page = None;
        for page_ref in page_refs
            .iter()
            .filter(|page_ref| page_ref.might_contain_level(options.source_level))
        {
            let page_summaries =
                self.routing_summaries_from_page_refs(std::slice::from_ref(page_ref))?;
            let selected = page_summaries
                .iter()
                .filter(|summary| summary.level == options.source_level)
                .take(max_segments)
                .cloned()
                .collect::<Vec<_>>();
            if selected.len() >= options.min_segments {
                selected_page = Some((page_ref.page_ordinal, page_summaries, selected));
                break;
            }
        }

        let Some((page_ordinal, page_summaries, selected)) = selected_page else {
            return Ok(CompactionReport {
                compacted: false,
                source_level: options.source_level,
                target_level: options.target_level,
                segments_read: 0,
                segments_written: 0,
                records_rewritten: 0,
                bytes_read: 0,
                bytes_written: 0,
                object_cache_hits: 0,
                object_cache_misses: 0,
                manifest_version: self.manifest.version,
            });
        };

        let target_segment_max_vectors = options
            .target_segment_max_vectors
            .unwrap_or(self.manifest.config.segment_max_vectors);
        if target_segment_max_vectors == 0 {
            return Err(BorsukError::InvalidCompactionInput(
                "target_segment_max_vectors must be greater than zero".to_string(),
            ));
        }

        let mut records = Vec::<VectorRecord>::new();
        let mut bytes_read = 0_u64;
        let mut object_cache_hits = 0_usize;
        let mut object_cache_misses = 0_usize;

        for summary in &selected {
            let (segment, segment_bytes_read, segment_cache_hit) = self.read_segment(summary)?;
            bytes_read += segment_bytes_read;
            count_cache_read(
                segment_cache_hit,
                &mut object_cache_hits,
                &mut object_cache_misses,
            );
            records.extend(segment.records);
        }
        sort_records_by_vector_locality(
            &mut records,
            self.manifest.config.dimensions,
            target_segment_max_vectors,
        );

        let selected_ids = selected
            .iter()
            .map(|summary| summary.id.as_str())
            .collect::<HashSet<_>>();
        let mut replacement_summaries = page_summaries
            .into_iter()
            .filter(|summary| !selected_ids.contains(summary.id.as_str()))
            .collect::<Vec<_>>();

        let mut manifest = self.manifest.next_version();
        manifest.segments.clear();
        manifest.pivots.clear();

        let mut segments_written = 0_usize;
        let mut bytes_written = 0_u64;
        for chunk in records.chunks(target_segment_max_vectors) {
            let segment_id = Uuid::new_v4().to_string();
            let segment = Segment::from_records(
                segment_id,
                options.target_level,
                self.manifest.config.metric.clone(),
                self.manifest.config.dimensions,
                chunk.to_vec(),
            )?;
            let summary = self.write_segment(segment)?;
            bytes_written += summary.size_bytes;
            segments_written += 1;
            replacement_summaries.push(summary);
        }

        let replacement_pages = replacement_summaries
            .chunks(ROUTING_PAGE_FANOUT)
            .map(|chunk| chunk.to_vec())
            .collect::<Vec<_>>();
        for (chunk_index, summaries) in replacement_pages.iter().enumerate() {
            let target_page_ordinal = if chunk_index == 0 {
                page_ordinal
            } else {
                page_refs.len()
            };
            let page_ref = self.storage.write_routing_layer_page(
                &manifest,
                0,
                target_page_ordinal,
                summaries,
            )?;
            if chunk_index == 0 {
                page_refs[page_ordinal] = page_ref;
            } else {
                page_refs.push(page_ref);
            }
        }

        enforce_ram_budget(&manifest, self.runtime_ram_budget_bytes)?;
        self.storage
            .publish_manifest_with_routing_page_refs(&manifest, &page_refs)?;
        self.manifest = manifest;

        Ok(CompactionReport {
            compacted: true,
            source_level: options.source_level,
            target_level: options.target_level,
            segments_read: selected.len(),
            segments_written,
            records_rewritten: records.len(),
            bytes_read,
            bytes_written,
            object_cache_hits,
            object_cache_misses,
            manifest_version: self.manifest.version,
        })
    }

    /// Delete inactive segment objects that are no longer referenced by the active manifest.
    pub fn gc_obsolete_segments(
        &self,
        options: GarbageCollectionOptions,
    ) -> Result<GarbageCollectionReport> {
        let active_paths = self.active_segment_object_paths()?;
        let mut objects = self.storage.list_objects("segments")?;
        objects.extend(self.storage.list_objects("graphs")?);
        let objects_scanned = objects.len();
        let candidates = objects
            .into_iter()
            .filter(|object| {
                object.path.ends_with(".parquet") && !active_paths.contains(&object.path)
            })
            .collect::<Vec<_>>();
        let bytes_reclaimable = candidates.iter().map(|object| object.size).sum::<u64>();
        let candidate_paths = candidates
            .iter()
            .map(|object| object.path.clone())
            .collect::<Vec<_>>();

        if options.dry_run {
            return Ok(GarbageCollectionReport {
                dry_run: true,
                objects_scanned,
                objects_deleted: 0,
                bytes_reclaimable,
                bytes_reclaimed: 0,
                candidates: candidate_paths,
            });
        }

        let mut objects_deleted = 0_usize;
        let mut bytes_reclaimed = 0_u64;
        for object in &candidates {
            if self.storage.delete_object(&object.path)? {
                objects_deleted += 1;
                bytes_reclaimed += object.size;
            }
        }

        Ok(GarbageCollectionReport {
            dry_run: false,
            objects_scanned,
            objects_deleted,
            bytes_reclaimable,
            bytes_reclaimed,
            candidates: candidate_paths,
        })
    }

    fn active_segment_object_paths(&self) -> Result<HashSet<String>> {
        let mut active_paths = HashSet::new();
        for summary in self.active_segment_summaries()? {
            active_paths.insert(summary.path);
            active_paths.insert(summary.graph_path);
        }
        Ok(active_paths)
    }

    fn active_segment_summaries(&self) -> Result<Vec<SegmentSummary>> {
        if !self.manifest.segments.is_empty() {
            return Ok(self.manifest.segments.clone());
        }

        let page_refs = self
            .storage
            .read_routing_layer_page_index(self.manifest.version, 0)?;
        if page_refs.is_empty() {
            return Ok(Vec::new());
        }

        self.routing_summaries_from_page_refs(&page_refs)
    }

    fn search_hits(&self, query: &[f32], options: SearchOptions) -> Result<Vec<SearchHit>> {
        Ok(self.search_with_report(query, options)?.hits)
    }

    /// Search the index and return only matching identifiers.
    pub fn search_ids(&self, query: &[f32], options: SearchOptions) -> Result<Vec<String>> {
        Ok(self
            .search_hits(query, options)?
            .into_iter()
            .map(|hit| hit.id)
            .collect())
    }

    /// Search the index and return stored vectors for the nearest neighbors.
    pub fn search_vectors(&self, query: &[f32], options: SearchOptions) -> Result<Vec<Vec<f32>>> {
        self.search_ids(query, options)?
            .into_iter()
            .map(|id| {
                self.get_vector(&id)?.ok_or_else(|| {
                    BorsukError::InvalidStorage(format!(
                        "search hit id `{id}` was not found in active segments"
                    ))
                })
            })
            .collect()
    }

    fn search_hits_batch(
        &self,
        queries: &[Vec<f32>],
        options: SearchOptions,
    ) -> Result<Vec<Vec<SearchHit>>> {
        queries
            .iter()
            .map(|query| self.search_hits(query, options.clone()))
            .collect()
    }

    /// Search multiple queries and return only matching identifiers for each query.
    pub fn search_ids_batch(
        &self,
        queries: &[Vec<f32>],
        options: SearchOptions,
    ) -> Result<Vec<Vec<String>>> {
        Ok(self
            .search_hits_batch(queries, options)?
            .into_iter()
            .map(|hits| hits.into_iter().map(|hit| hit.id).collect())
            .collect())
    }

    /// Search multiple queries and return stored vectors for each query's nearest neighbors.
    pub fn search_vectors_batch(
        &self,
        queries: &[Vec<f32>],
        options: SearchOptions,
    ) -> Result<Vec<Vec<Vec<f32>>>> {
        self.search_ids_batch(queries, options)?
            .into_iter()
            .map(|ids| {
                ids.into_iter()
                    .map(|id| {
                        self.get_vector(&id)?.ok_or_else(|| {
                            BorsukError::InvalidStorage(format!(
                                "search hit id `{id}` was not found in active segments"
                            ))
                        })
                    })
                    .collect()
            })
            .collect()
    }

    /// Search multiple queries and return execution measurements for each query in input order.
    pub fn search_batch_with_report(
        &self,
        queries: &[Vec<f32>],
        options: SearchOptions,
    ) -> Result<Vec<SearchReport>> {
        queries
            .iter()
            .map(|query| self.search_with_report(query, options.clone()))
            .collect()
    }

    /// Search the index and return execution measurements along with the hits.
    pub fn search_with_report(
        &self,
        query: &[f32],
        options: SearchOptions,
    ) -> Result<SearchReport> {
        self.validate_vector(query)?;
        validate_search_options(&options)?;

        let started = Instant::now();
        let segments_total = self.routing_segments_total()?;
        let resident_bytes_estimate = self.manifest.resident_bytes_estimate();

        if options.k == 0 {
            return Ok(SearchReport {
                hits: Vec::new(),
                leaf_mode: options.mode.leaf_mode().to_string(),
                segments_total,
                segments_searched: 0,
                segments_skipped: segments_total,
                bytes_read: 0,
                graph_bytes_read: 0,
                object_cache_hits: 0,
                object_cache_misses: 0,
                records_considered: 0,
                records_scored: 0,
                graph_candidates_added: 0,
                resident_bytes_estimate,
                elapsed_ms: started.elapsed().as_millis() as u64,
            });
        }

        let routing_read = self.routing_summaries_for_search(query, &options)?;
        let metric = &self.manifest.config.metric;
        let prioritize_signature = should_prioritize_vector_signature(&options.mode);
        let query_signature = prioritize_signature.then(|| vector_signature(query));
        let mut candidates = routing_read
            .summaries
            .iter()
            .map(|summary| {
                let lower_bound = summary.lower_bound(query, metric).unwrap_or(0.0);
                let signature_miss = query_signature
                    .is_some_and(|signature| !summary.might_contain_vector_signature(signature));
                (summary, signature_miss, lower_bound)
            })
            .collect::<Vec<_>>();

        candidates.sort_by(
            |(_, left_signature_miss, left), (_, right_signature_miss, right)| {
                left.partial_cmp(right)
                    .unwrap_or(Ordering::Equal)
                    .then_with(|| left_signature_miss.cmp(right_signature_miss))
            },
        );

        let mut hits = Vec::<SearchHit>::new();
        let mut segments_searched = 0_usize;
        let candidates_total = candidates.len();
        let mut segments_skipped = segments_total.saturating_sub(candidates_total);
        let mut bytes_read = routing_read.bytes_read;
        let mut graph_bytes_read = 0_u64;
        let mut object_cache_hits = routing_read.object_cache_hits;
        let mut object_cache_misses = routing_read.object_cache_misses;
        let mut records_considered = 0_usize;
        let mut records_scored = 0_usize;
        let mut graph_candidates_added = 0_usize;

        for (candidate_index, (summary, _, lower_bound)) in candidates.into_iter().enumerate() {
            if should_stop_before_segment(
                &hits,
                options.k,
                &options.mode,
                segments_searched,
                bytes_read,
                lower_bound,
                started.elapsed().as_millis() as u64,
            ) {
                segments_skipped += candidates_total - candidate_index;
                break;
            }

            let (segment, segment_bytes_read, segment_cache_hit) = self.read_segment(summary)?;
            segments_searched += 1;
            bytes_read += segment_bytes_read;
            count_cache_read(
                segment_cache_hit,
                &mut object_cache_hits,
                &mut object_cache_misses,
            );
            records_considered += segment.records.len();

            let graph = if should_expand_segment_graph(&options.mode, summary.leaf_mode) {
                let (graph, graph_bytes, graph_cache_hit) = self.read_graph(summary, &segment)?;
                graph_bytes_read += graph_bytes;
                count_cache_read(
                    graph_cache_hit,
                    &mut object_cache_hits,
                    &mut object_cache_misses,
                );
                Some(graph)
            } else {
                None
            };
            let candidates = candidate_record_indices(
                &segment,
                graph.as_ref(),
                query,
                &options.mode,
                effective_leaf_mode(&options.mode, summary.leaf_mode),
                options.k,
            )?;
            graph_candidates_added += candidates.graph_candidates_added;

            for record_index in candidates.indices {
                let record = &segment.records[record_index];
                let distance = metric.distance(query, &record.vector)?;
                records_scored += 1;
                push_hit(
                    &mut hits,
                    SearchHit {
                        id: record.id.clone(),
                        distance,
                    },
                    options.k,
                );
            }
        }

        Ok(SearchReport {
            hits,
            leaf_mode: options.mode.leaf_mode().to_string(),
            segments_total,
            segments_searched,
            segments_skipped,
            bytes_read,
            graph_bytes_read,
            object_cache_hits,
            object_cache_misses,
            records_considered,
            records_scored,
            graph_candidates_added,
            resident_bytes_estimate,
            elapsed_ms: started.elapsed().as_millis() as u64,
        })
    }

    fn routing_summaries_for_search(
        &self,
        query: &[f32],
        options: &SearchOptions,
    ) -> Result<RoutingSummariesRead> {
        let page_refs = self
            .storage
            .read_routing_layer_page_index(self.manifest.version, 0)?;
        if !page_refs.is_empty() {
            let selected_page_refs =
                self.routing_layer_page_refs_for_search(query, options, &page_refs)?;
            return self.routing_summaries_read_from_page_refs(&selected_page_refs);
        }

        if self.manifest.segments.is_empty() {
            return Ok(RoutingSummariesRead::default());
        }

        self.routing_summaries_from_legacy_pages()
    }

    fn routing_segments_total(&self) -> Result<usize> {
        if !self.manifest.segments.is_empty() {
            return Ok(self.manifest.segments.len());
        }

        let page_refs = self
            .storage
            .read_routing_layer_page_index(self.manifest.version, 0)?;
        Ok(page_refs
            .iter()
            .map(|page_ref| page_ref.page_segments)
            .sum())
    }

    fn routing_layer_page_refs_for_search(
        &self,
        query: &[f32],
        options: &SearchOptions,
        page_refs: &[RoutingLayerPageRef],
    ) -> Result<Vec<RoutingLayerPageRef>> {
        let SearchMode::Approx {
            max_segments: Some(max_segments),
            ..
        } = &options.mode
        else {
            return Ok(page_refs.to_vec());
        };
        if !self.manifest.config.metric.supports_centroid_lower_bound()
            || page_refs
                .iter()
                .any(|page_ref| page_ref.centroid.len() != self.manifest.config.dimensions)
        {
            return Ok(page_refs.to_vec());
        }

        let pages_to_read = max_segments
            .div_ceil(ROUTING_PAGE_FANOUT)
            .max(1)
            .min(page_refs.len());
        let mut ranked_pages = page_refs
            .iter()
            .map(|page_ref| {
                let center_distance = self
                    .manifest
                    .config
                    .metric
                    .distance(query, &page_ref.centroid)?;
                Ok((
                    (center_distance - page_ref.radius).max(0.0),
                    page_ref.page_ordinal,
                    page_ref.clone(),
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        ranked_pages.sort_by(|left, right| {
            left.0
                .total_cmp(&right.0)
                .then_with(|| left.1.cmp(&right.1))
        });
        ranked_pages.truncate(pages_to_read);
        ranked_pages.sort_by_key(|(_, ordinal, _)| *ordinal);

        Ok(ranked_pages
            .into_iter()
            .map(|(_, _, page_ref)| page_ref)
            .collect())
    }

    fn routing_summaries_from_page_refs(
        &self,
        page_refs: &[RoutingLayerPageRef],
    ) -> Result<Vec<SegmentSummary>> {
        Ok(self
            .routing_summaries_read_from_page_refs(page_refs)?
            .summaries)
    }

    fn routing_summaries_read_from_page_refs(
        &self,
        page_refs: &[RoutingLayerPageRef],
    ) -> Result<RoutingSummariesRead> {
        let expected_summaries = page_refs
            .iter()
            .map(|page_ref| page_ref.page_segments)
            .sum::<usize>();
        let mut read_result = RoutingSummariesRead {
            summaries: Vec::with_capacity(expected_summaries),
            ..Default::default()
        };

        for page_ref in page_refs {
            let read = self
                .storage
                .read_bytes_with_cache_status(&page_ref.path)
                .map_err(|err| {
                    BorsukError::InvalidStorage(format!(
                        "routing layer page `{}` could not be read: {err}",
                        page_ref.path
                    ))
                })?;
            read_result.bytes_read += read.bytes.len() as u64;
            count_cache_read(
                read.cache_hit,
                &mut read_result.object_cache_hits,
                &mut read_result.object_cache_misses,
            );
            let checksum = blake3::hash(&read.bytes).to_hex().to_string();
            if checksum != page_ref.checksum {
                return Err(BorsukError::InvalidStorage(format!(
                    "routing layer page `{}` checksum mismatch: expected {}, got {}",
                    page_ref.path, page_ref.checksum, checksum
                )));
            }
            let mut page_summaries = routing_layer_page_from_parquet(
                &read.bytes,
                self.manifest.version,
                page_ref.routing_level,
                page_ref.page_ordinal,
                self.manifest.config.dimensions,
            )
            .map_err(|err| {
                BorsukError::InvalidStorage(format!(
                    "routing layer page `{}` could not be decoded: {err}",
                    page_ref.path
                ))
            })?;
            if page_summaries.len() != page_ref.page_segments {
                return Err(BorsukError::InvalidStorage(format!(
                    "routing layer page `{}` yielded {} segment summaries, expected {}",
                    page_ref.path,
                    page_summaries.len(),
                    page_ref.page_segments
                )));
            }
            read_result.summaries.append(&mut page_summaries);
        }

        if read_result.summaries.len() != expected_summaries {
            return Err(BorsukError::InvalidStorage(format!(
                "routing layer pages yielded {} segment summaries, expected {}",
                read_result.summaries.len(),
                expected_summaries
            )));
        }

        Ok(read_result)
    }

    fn routing_summaries_from_legacy_pages(&self) -> Result<RoutingSummariesRead> {
        let page_count = self.manifest.segments.len().div_ceil(ROUTING_PAGE_FANOUT);
        let mut read_result = RoutingSummariesRead {
            summaries: Vec::with_capacity(self.manifest.segments.len()),
            ..Default::default()
        };

        for page_ordinal in 0..page_count {
            let path =
                Manifest::routing_layer_page_file_name(self.manifest.version, 0, page_ordinal);
            let read = match self.storage.read_bytes_with_cache_status(&path) {
                Ok(read) => read,
                Err(err) if page_ordinal == 0 && is_missing_routing_page(&err) => {
                    return Ok(RoutingSummariesRead {
                        summaries: self.manifest.segments.clone(),
                        ..Default::default()
                    });
                }
                Err(err) => {
                    return Err(BorsukError::InvalidStorage(format!(
                        "routing layer page `{path}` could not be read: {err}"
                    )));
                }
            };
            read_result.bytes_read += read.bytes.len() as u64;
            count_cache_read(
                read.cache_hit,
                &mut read_result.object_cache_hits,
                &mut read_result.object_cache_misses,
            );
            let mut page_summaries = routing_layer_page_from_parquet(
                &read.bytes,
                self.manifest.version,
                0,
                page_ordinal,
                self.manifest.config.dimensions,
            )
            .map_err(|err| {
                BorsukError::InvalidStorage(format!(
                    "routing layer page `{path}` could not be decoded: {err}"
                ))
            })?;
            read_result.summaries.append(&mut page_summaries);
        }

        read_result.summaries = self.validate_routing_summary_count(read_result.summaries)?;
        Ok(read_result)
    }

    fn validate_routing_summary_count(
        &self,
        summaries: Vec<SegmentSummary>,
    ) -> Result<Vec<SegmentSummary>> {
        if summaries.len() != self.manifest.segments.len() {
            return Err(BorsukError::InvalidStorage(format!(
                "routing layer pages yielded {} segment summaries, expected {}",
                summaries.len(),
                self.manifest.segments.len()
            )));
        }

        Ok(summaries)
    }

    fn write_segment(&self, segment: Segment) -> Result<SegmentSummary> {
        let bytes = segment_to_parquet(&segment)?;
        let checksum = blake3::hash(&bytes).to_hex().to_string();
        let prefix = &checksum[..2];
        let path = format!(
            "segments/L{}/{prefix}/seg-{}.parquet",
            segment.level, segment.id
        );

        let graph = SegmentGraph::from_segment(&segment, LOCAL_GRAPH_NEIGHBORS)?;
        let graph_bytes = graph_to_parquet(&graph)?;
        let graph_checksum = blake3::hash(&graph_bytes).to_hex().to_string();
        let graph_prefix = &graph_checksum[..2];
        let graph_path = format!(
            "graphs/L{}/{graph_prefix}/graph-{}.parquet",
            segment.level, segment.id
        );

        self.storage.write_bytes(&path, &bytes)?;
        self.storage.write_bytes(&graph_path, &graph_bytes)?;
        let id_bloom = segment_id_bloom(segment.records.iter().map(|record| record.id.as_str()));
        let vector_signature_bloom = segment_vector_signature_bloom(
            segment
                .records
                .iter()
                .map(|record| record.vector.as_slice()),
        );

        Ok(SegmentSummary {
            id: segment.id,
            level: segment.level,
            path,
            object_count: segment.records.len(),
            dimensions: segment.dimensions,
            centroid: segment.centroid,
            radius: segment.radius,
            checksum,
            size_bytes: bytes.len() as u64,
            graph_path,
            graph_checksum,
            graph_size_bytes: graph_bytes.len() as u64,
            leaf_mode: leaf_mode_for_segment_level(segment.level),
            id_bloom,
            vector_signature_bloom,
            created_at: segment.created_at,
        })
    }

    fn read_segment(&self, summary: &SegmentSummary) -> Result<(Segment, u64, bool)> {
        let read = self.storage.read_bytes_with_cache_status(&summary.path)?;
        let bytes_read = read.bytes.len() as u64;
        validate_object_size("segment", &summary.path, summary.size_bytes, bytes_read)?;
        let checksum = blake3::hash(&read.bytes).to_hex().to_string();
        if checksum != summary.checksum {
            return Err(BorsukError::ChecksumMismatch {
                path: summary.path.clone(),
                expected: summary.checksum.clone(),
                actual: checksum,
            });
        }

        let segment = segment_from_parquet(&read.bytes)?;
        validate_segment_metadata(summary, &segment, &self.manifest.config.metric)?;

        Ok((segment, bytes_read, read.cache_hit))
    }

    fn read_graph(
        &self,
        summary: &SegmentSummary,
        segment: &Segment,
    ) -> Result<(SegmentGraph, u64, bool)> {
        let read = self
            .storage
            .read_bytes_with_cache_status(&summary.graph_path)?;
        let bytes_read = read.bytes.len() as u64;
        validate_object_size(
            "graph",
            &summary.graph_path,
            summary.graph_size_bytes,
            bytes_read,
        )?;
        let checksum = blake3::hash(&read.bytes).to_hex().to_string();
        if checksum != summary.graph_checksum {
            return Err(BorsukError::ChecksumMismatch {
                path: summary.graph_path.clone(),
                expected: summary.graph_checksum.clone(),
                actual: checksum,
            });
        }

        let graph = graph_from_parquet(&read.bytes, &summary.id, summary.level, &segment.records)?;
        validate_graph_record_references(&summary.graph_path, segment, &graph)?;

        Ok((graph, bytes_read, read.cache_hit))
    }

    fn validate_vector(&self, vector: &[f32]) -> Result<()> {
        if vector.len() != self.manifest.config.dimensions {
            return Err(BorsukError::DimensionMismatch {
                expected: self.manifest.config.dimensions,
                actual: vector.len(),
            });
        }

        if let Some((coordinate_index, value)) = vector
            .iter()
            .copied()
            .enumerate()
            .find(|(_, value)| !value.is_finite())
        {
            return Err(BorsukError::InvalidMetricInput(format!(
                "vectors must contain only finite f32 values; coordinate {coordinate_index} was {value}"
            )));
        }

        Ok(())
    }

    fn effective_ram_budget_bytes(&self) -> Option<u64> {
        [
            self.manifest.config.ram_budget_bytes,
            self.runtime_ram_budget_bytes,
        ]
        .into_iter()
        .flatten()
        .min()
    }
}

fn sort_records_by_vector_locality(
    records: &mut Vec<VectorRecord>,
    dimensions: usize,
    target_segment_max_vectors: usize,
) {
    let mut reordered = std::mem::take(records);
    kd_order_records(
        &mut reordered,
        dimensions,
        target_segment_max_vectors.max(1),
    );
    records.extend(reordered);
}

fn kd_order_records(records: &mut [VectorRecord], dimensions: usize, leaf_size: usize) {
    if records.len() <= leaf_size {
        sort_leaf_records(records);
        return;
    }

    let split_dimension = widest_dimension(records, dimensions);
    records.sort_by(|left, right| {
        left.vector[split_dimension]
            .partial_cmp(&right.vector[split_dimension])
            .unwrap_or(Ordering::Equal)
            .then_with(|| {
                vector_locality_key(&left.vector)
                    .cmp(&vector_locality_key(&right.vector))
                    .then_with(|| left.id.cmp(&right.id))
            })
    });

    let split = aligned_split(records.len(), leaf_size);
    let (left, right) = records.split_at_mut(split);
    kd_order_records(left, dimensions, leaf_size);
    kd_order_records(right, dimensions, leaf_size);
}

fn sort_leaf_records(records: &mut [VectorRecord]) {
    records.sort_by(|left, right| {
        vector_locality_key(&left.vector)
            .cmp(&vector_locality_key(&right.vector))
            .then_with(|| left.id.cmp(&right.id))
    });
}

fn widest_dimension(records: &[VectorRecord], dimensions: usize) -> usize {
    let mut best_dimension = 0_usize;
    let mut best_width = f32::NEG_INFINITY;
    for dimension in 0..dimensions {
        let mut min = f32::INFINITY;
        let mut max = f32::NEG_INFINITY;
        for record in records {
            let value = record.vector[dimension];
            min = min.min(value);
            max = max.max(value);
        }
        let width = max - min;
        if width > best_width {
            best_width = width;
            best_dimension = dimension;
        }
    }
    best_dimension
}

fn aligned_split(len: usize, leaf_size: usize) -> usize {
    let midpoint = len / 2;
    let lower = (midpoint / leaf_size) * leaf_size;
    let upper = lower.saturating_add(leaf_size);
    let mut split = if midpoint.saturating_sub(lower) <= upper.saturating_sub(midpoint) {
        lower
    } else {
        upper
    };
    if split == 0 {
        split = midpoint.max(1);
    }
    if split >= len {
        split = len - 1;
    }
    split
}

fn leaf_mode_for_segment_level(level: u8) -> LeafMode {
    if level == 0 {
        LeafMode::Graph
    } else {
        LeafMode::VamanaPq
    }
}

fn is_missing_routing_page(err: &BorsukError) -> bool {
    matches!(
        err,
        BorsukError::ObjectStore(object_store::Error::NotFound { .. })
    )
}

fn validate_object_size(kind: &str, path: &str, expected: u64, actual: u64) -> Result<()> {
    if actual == expected {
        return Ok(());
    }

    Err(BorsukError::InvalidStorage(format!(
        "{kind} object size mismatch for `{path}`: expected {expected} bytes, got {actual}"
    )))
}

fn validate_segment_metadata(
    summary: &SegmentSummary,
    segment: &Segment,
    expected_metric: &VectorMetric,
) -> Result<()> {
    validate_segment_metadata_field("id", &summary.path, &summary.id, &segment.id)?;
    validate_segment_metadata_field("level", &summary.path, summary.level, segment.level)?;
    validate_segment_metadata_field(
        "dimensions",
        &summary.path,
        summary.dimensions,
        segment.dimensions,
    )?;
    validate_segment_metadata_field("metric", &summary.path, expected_metric, &segment.metric)?;
    validate_segment_metadata_field(
        "centroid",
        &summary.path,
        summary.centroid.as_slice(),
        segment.centroid.as_slice(),
    )?;
    validate_segment_metadata_field("radius", &summary.path, summary.radius, segment.radius)?;
    validate_segment_object_count(&summary.path, summary.object_count, segment.records.len())?;

    Ok(())
}

fn validate_segment_metadata_field<T>(field: &str, path: &str, expected: T, actual: T) -> Result<()>
where
    T: PartialEq + std::fmt::Debug,
{
    if actual == expected {
        return Ok(());
    }

    Err(BorsukError::InvalidStorage(format!(
        "segment metadata {field} mismatch for `{path}`: expected {expected:?}, got {actual:?}"
    )))
}

fn validate_segment_object_count(path: &str, expected: usize, actual: usize) -> Result<()> {
    if actual == expected {
        return Ok(());
    }

    Err(BorsukError::InvalidStorage(format!(
        "segment object_count mismatch for `{path}`: expected {expected}, got {actual}"
    )))
}

fn validate_graph_record_references(
    path: &str,
    segment: &Segment,
    graph: &SegmentGraph,
) -> Result<()> {
    validate_graph_has_edges_for_multi_record_segment(path, segment, graph)?;

    let mut graph_edges = HashSet::with_capacity(graph.edges.len());
    let mut source_out_degree = HashMap::<usize, usize>::new();
    for edge in &graph.edges {
        validate_graph_edge_not_self_referential(path, edge)?;
        validate_graph_edge_not_duplicate(path, edge, &mut graph_edges)?;
        validate_graph_source_out_degree(path, edge, &mut source_out_degree)?;
        let source = graph_edge_record(path, "source", edge.source_record_index, segment)?;
        let neighbor = graph_edge_record(path, "neighbor", edge.neighbor_record_index, segment)?;
        let expected_distance = segment.metric.distance(&source.vector, &neighbor.vector)?;
        validate_graph_edge_distance(path, edge, expected_distance)?;
    }

    Ok(())
}

fn validate_graph_has_edges_for_multi_record_segment(
    path: &str,
    segment: &Segment,
    graph: &SegmentGraph,
) -> Result<()> {
    if segment.records.len() <= 1 || !graph.edges.is_empty() {
        return Ok(());
    }

    Err(BorsukError::InvalidStorage(format!(
        "graph table must contain at least one edge for multi-record segment in `{path}`"
    )))
}

fn validate_graph_source_out_degree(
    path: &str,
    edge: &crate::segment::GraphEdge,
    source_out_degree: &mut HashMap<usize, usize>,
) -> Result<()> {
    let count = source_out_degree
        .entry(edge.source_record_index)
        .or_default();
    *count += 1;
    if *count <= LOCAL_GRAPH_NEIGHBORS {
        return Ok(());
    }

    Err(BorsukError::InvalidStorage(format!(
        "graph source out-degree exceeds local limit in `{path}`: source index {} has {} edges, limit is {LOCAL_GRAPH_NEIGHBORS}",
        edge.source_record_index, *count
    )))
}

fn validate_graph_edge_not_duplicate(
    path: &str,
    edge: &crate::segment::GraphEdge,
    graph_edges: &mut HashSet<(usize, usize)>,
) -> Result<()> {
    if graph_edges.insert((edge.source_record_index, edge.neighbor_record_index)) {
        return Ok(());
    }

    Err(BorsukError::InvalidStorage(format!(
        "duplicate graph edge in `{path}`: {} -> {}",
        edge.source_record_index, edge.neighbor_record_index
    )))
}

fn validate_graph_edge_not_self_referential(
    path: &str,
    edge: &crate::segment::GraphEdge,
) -> Result<()> {
    if edge.source_record_index != edge.neighbor_record_index {
        return Ok(());
    }

    Err(BorsukError::InvalidStorage(format!(
        "graph edge self-reference in `{path}`: record index {}",
        edge.source_record_index
    )))
}

fn graph_edge_record<'a>(
    path: &str,
    role: &str,
    record_index: usize,
    segment: &'a Segment,
) -> Result<&'a VectorRecord> {
    if let Some(record) = segment.records.get(record_index) {
        return Ok(record);
    }

    Err(BorsukError::InvalidStorage(format!(
        "graph edge references missing segment record in `{path}`: {role} record index {record_index}"
    )))
}

fn validate_graph_edge_distance(
    path: &str,
    edge: &crate::segment::GraphEdge,
    expected: f32,
) -> Result<()> {
    let actual = edge.distance;
    let tolerance = 1e-5_f32 * expected.abs().max(actual.abs()).max(1.0);
    if (actual - expected).abs() <= tolerance {
        return Ok(());
    }

    Err(BorsukError::InvalidStorage(format!(
        "graph edge distance mismatch in `{path}`: edge {} -> {} expected {expected}, got {actual}",
        edge.source_record_index, edge.neighbor_record_index
    )))
}

fn records_from_ids_and_vectors(
    ids: Vec<String>,
    vectors: Vec<Vec<f32>>,
) -> Result<Vec<VectorRecord>> {
    if ids.len() != vectors.len() {
        return Err(BorsukError::InvalidRecordInput(format!(
            "ids length {} must match vectors length {}",
            ids.len(),
            vectors.len()
        )));
    }

    Ok(ids
        .into_iter()
        .zip(vectors)
        .map(|(id, vector)| VectorRecord::new(id, vector))
        .collect())
}

fn next_generated_id_after_explicit_records(current: u64, records: &[VectorRecord]) -> Result<u64> {
    let mut next = current;
    for record in records {
        if let Ok(id) = record.id.parse::<u64>() {
            let after_id = id.checked_add(1).ok_or_else(|| {
                BorsukError::InvalidRecordInput(format!(
                    "numeric record id `{}` leaves no generated id range",
                    record.id
                ))
            })?;
            next = next.max(after_id);
        }
    }
    Ok(next)
}

fn advance_generated_id(current: u64, count: usize) -> Result<u64> {
    let count = u64::try_from(count).map_err(|_| {
        BorsukError::InvalidRecordInput("generated id count does not fit u64".to_string())
    })?;
    current.checked_add(count).ok_or_else(|| {
        BorsukError::InvalidRecordInput("generated id exceeds u64 range".to_string())
    })
}

fn count_cache_read(cache_hit: bool, hits: &mut usize, misses: &mut usize) {
    if cache_hit {
        *hits += 1;
    } else {
        *misses += 1;
    }
}

fn validate_compaction_options(options: &CompactionOptions) -> Result<()> {
    if options.source_level == options.target_level {
        return Err(BorsukError::InvalidCompactionInput(
            "source_level and target_level must differ".to_string(),
        ));
    }

    if options.min_segments == 0 {
        return Err(BorsukError::InvalidCompactionInput(
            "min_segments must be greater than zero".to_string(),
        ));
    }

    if options.max_segments == Some(0) {
        return Err(BorsukError::InvalidCompactionInput(
            "max_segments must be greater than zero when set".to_string(),
        ));
    }

    Ok(())
}

fn validate_search_options(options: &SearchOptions) -> Result<()> {
    if options.k == 0 {
        return Err(BorsukError::InvalidSearchOptions(
            "k must be greater than zero".to_string(),
        ));
    }

    let SearchMode::Approx {
        leaf_mode: _,
        eps,
        max_segments,
        max_bytes,
        max_latency_ms,
        max_candidates_per_segment,
    } = &options.mode
    else {
        return Ok(());
    };

    if let Some(eps) = eps
        && (!eps.is_finite() || *eps < 0.0)
    {
        return Err(BorsukError::InvalidSearchOptions(
            "eps must be finite and non-negative when set".to_string(),
        ));
    }

    if *max_segments == Some(0) {
        return Err(BorsukError::InvalidSearchOptions(
            "max_segments must be greater than zero when set".to_string(),
        ));
    }

    if *max_bytes == Some(0) {
        return Err(BorsukError::InvalidSearchOptions(
            "max_bytes must be greater than zero when set".to_string(),
        ));
    }

    if *max_latency_ms == Some(0) {
        return Err(BorsukError::InvalidSearchOptions(
            "max_latency_ms must be greater than zero when set".to_string(),
        ));
    }

    if *max_candidates_per_segment == Some(0) {
        return Err(BorsukError::InvalidSearchOptions(
            "max_candidates_per_segment must be greater than zero when set".to_string(),
        ));
    }

    Ok(())
}

fn enforce_ram_budget(manifest: &Manifest, runtime_budget_bytes: Option<u64>) -> Result<()> {
    let Some(budget_bytes) = [manifest.config.ram_budget_bytes, runtime_budget_bytes]
        .into_iter()
        .flatten()
        .min()
    else {
        return Ok(());
    };

    let resident_bytes = manifest.resident_bytes_estimate();
    if resident_bytes > budget_bytes {
        return Err(BorsukError::RamBudgetExceeded {
            resident_bytes,
            budget_bytes,
        });
    }

    Ok(())
}

struct CandidateRecordSelection {
    indices: Vec<usize>,
    graph_candidates_added: usize,
}

fn candidate_record_indices(
    segment: &Segment,
    graph: Option<&SegmentGraph>,
    query: &[f32],
    mode: &SearchMode,
    leaf_mode: LeafMode,
    k: usize,
) -> Result<CandidateRecordSelection> {
    let Some(max_candidates_per_segment) = max_candidates_per_segment(mode) else {
        return Ok(CandidateRecordSelection {
            indices: (0..segment.records.len()).collect(),
            graph_candidates_added: 0,
        });
    };

    let limit = max_candidates_per_segment.min(segment.records.len());
    let query_code = routing_code(query);
    let query_pq_code = if matches!(leaf_mode, LeafMode::PqScan | LeafMode::VamanaPq) {
        Some(pq_code_for_query(segment, query)?)
    } else {
        None
    };
    let mut indices = (0..segment.records.len()).collect::<Vec<_>>();
    indices.sort_by(|left, right| {
        let left_distance =
            candidate_code_distance(segment, *left, query_code, query_pq_code.as_deref());
        let right_distance =
            candidate_code_distance(segment, *right, query_code, query_pq_code.as_deref());
        left_distance
            .partial_cmp(&right_distance)
            .unwrap_or(Ordering::Equal)
            .then_with(|| segment.records[*left].id.cmp(&segment.records[*right].id))
    });

    let Some(graph) = graph else {
        indices.truncate(limit);
        return Ok(CandidateRecordSelection {
            indices,
            graph_candidates_added: 0,
        });
    };

    let mut selected = Vec::with_capacity(limit);
    let mut selected_set = HashSet::with_capacity(limit);
    let entry_count = k.max(1).min(limit).min(indices.len());
    for record_index in indices.iter().copied().take(entry_count) {
        selected.push(record_index);
        selected_set.insert(record_index);
    }

    let mut adjacency = HashMap::<usize, Vec<usize>>::new();
    for edge in &graph.edges {
        if edge.source_record_index >= segment.records.len()
            || edge.neighbor_record_index >= segment.records.len()
        {
            continue;
        }
        adjacency
            .entry(edge.source_record_index)
            .or_default()
            .push(edge.neighbor_record_index);
    }

    let mut graph_candidates_added = 0_usize;
    let mut frontier = selected
        .iter()
        .filter_map(|index| adjacency.get(index))
        .flatten()
        .copied()
        .collect::<Vec<_>>();

    while selected.len() < limit {
        let Some(frontier_position) =
            best_frontier_position(segment, query, &frontier, &selected_set)?
        else {
            break;
        };
        let neighbor_index = frontier.swap_remove(frontier_position);
        if selected_set.insert(neighbor_index) {
            selected.push(neighbor_index);
            graph_candidates_added += 1;
            if let Some(neighbors) = adjacency.get(&neighbor_index) {
                frontier.extend(neighbors.iter().copied());
            }
        }
    }

    for record_index in indices {
        if selected.len() >= limit {
            break;
        }
        if selected_set.insert(record_index) {
            selected.push(record_index);
        }
    }

    Ok(CandidateRecordSelection {
        indices: selected,
        graph_candidates_added,
    })
}

fn best_frontier_position(
    segment: &Segment,
    query: &[f32],
    frontier: &[usize],
    selected: &HashSet<usize>,
) -> Result<Option<usize>> {
    let mut best = None::<(usize, f32)>;
    for (position, record_index) in frontier.iter().copied().enumerate() {
        if selected.contains(&record_index) {
            continue;
        }

        let distance = segment
            .metric
            .distance(query, &segment.records[record_index].vector)?;
        let is_better = best.is_none_or(|(best_position, best_distance)| {
            distance
                .partial_cmp(&best_distance)
                .unwrap_or(Ordering::Equal)
                .then_with(|| {
                    segment.records[record_index]
                        .id
                        .cmp(&segment.records[frontier[best_position]].id)
                })
                .is_lt()
        });
        if is_better {
            best = Some((position, distance));
        }
    }

    Ok(best.map(|(position, _)| position))
}

fn effective_leaf_mode(mode: &SearchMode, stored_leaf_mode: LeafMode) -> LeafMode {
    match mode {
        SearchMode::Approx {
            leaf_mode: LeafMode::Hybrid,
            ..
        } => stored_leaf_mode,
        _ => mode.leaf_mode(),
    }
}

fn should_expand_segment_graph(mode: &SearchMode, stored_leaf_mode: LeafMode) -> bool {
    let SearchMode::Approx {
        leaf_mode,
        max_candidates_per_segment: Some(_),
        ..
    } = mode
    else {
        return false;
    };

    match leaf_mode {
        LeafMode::Graph | LeafMode::VamanaPq => true,
        LeafMode::Hybrid => matches!(stored_leaf_mode, LeafMode::Graph | LeafMode::VamanaPq),
        LeafMode::FlatScan | LeafMode::SqScan | LeafMode::PqScan => false,
    }
}

fn should_prioritize_vector_signature(mode: &SearchMode) -> bool {
    matches!(
        mode,
        SearchMode::Approx {
            eps: None,
            max_segments: Some(_),
            ..
        }
    )
}

fn max_candidates_per_segment(mode: &SearchMode) -> Option<usize> {
    match mode {
        SearchMode::Exact => None,
        SearchMode::Approx {
            leaf_mode: _,
            max_candidates_per_segment,
            ..
        } => *max_candidates_per_segment,
    }
}

fn routing_code_distance(segment: &Segment, record_index: usize, query_code: f32) -> f32 {
    let code = segment
        .routing_codes
        .get(record_index)
        .copied()
        .unwrap_or_else(|| routing_code(&segment.records[record_index].vector));
    (code - query_code).abs()
}

fn candidate_code_distance(
    segment: &Segment,
    record_index: usize,
    query_code: f32,
    query_pq_code: Option<&[u8]>,
) -> f32 {
    if let Some(query_pq_code) = query_pq_code {
        return pq_code_distance(segment, record_index, query_pq_code);
    }

    routing_code_distance(segment, record_index, query_code)
}

fn pq_code_distance(segment: &Segment, record_index: usize, query_code: &[u8]) -> f32 {
    let Some(code) = segment.pq_codes.get(record_index) else {
        return f32::INFINITY;
    };

    code.iter()
        .zip(query_code)
        .map(|(left, right)| {
            let diff = f32::from(*left) - f32::from(*right);
            diff * diff
        })
        .sum()
}

fn push_hit(hits: &mut Vec<SearchHit>, hit: SearchHit, k: usize) {
    hits.push(hit);
    hits.sort_by(|left, right| {
        left.distance
            .partial_cmp(&right.distance)
            .unwrap_or(Ordering::Equal)
            .then_with(|| left.id.cmp(&right.id))
    });
    hits.truncate(k);
}

fn should_stop_before_segment(
    hits: &[SearchHit],
    k: usize,
    mode: &SearchMode,
    searched_segments: usize,
    bytes_read: u64,
    lower_bound: f32,
    elapsed_ms: u64,
) -> bool {
    match mode {
        SearchMode::Exact => hits
            .get(k.saturating_sub(1))
            .is_some_and(|best_k| lower_bound > best_k.distance),
        SearchMode::Approx {
            leaf_mode: _,
            eps,
            max_segments,
            max_bytes,
            max_latency_ms,
            max_candidates_per_segment: _,
        } => {
            if max_segments.is_some_and(|limit| searched_segments >= limit) {
                return true;
            }

            if max_bytes.is_some_and(|limit| bytes_read >= limit) {
                return true;
            }

            if max_latency_ms.is_some_and(|limit| elapsed_ms >= limit) {
                return true;
            }

            if let (Some(eps), Some(best_k)) = (eps, hits.get(k.saturating_sub(1))) {
                return lower_bound >= best_k.distance / (1.0 + eps);
            }

            false
        }
    }
}
