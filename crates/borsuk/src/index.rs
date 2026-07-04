use std::{
    cmp::Ordering,
    collections::{BTreeMap, HashMap, HashSet, VecDeque},
    ops::Range,
    path::PathBuf,
    time::Instant,
};

use uuid::Uuid;

use crate::{
    error::{BorsukError, Result},
    format::{
        graph_from_parquet, graph_to_parquet, routing_layer_page_from_parquet,
        routing_layer_page_index_from_parquet_relaxed_manifest_version, segment_from_parquet,
        segment_to_parquet,
    },
    manifest::{
        DEFAULT_ROUTING_PAGE_FANOUT, Manifest, RoutingLayerPageRef, SegmentSummary,
        segment_id_bloom, segment_vector_signature_bloom,
    },
    metric::VectorMetric,
    record::{
        CompactionOptions, CompactionReport, GarbageCollectionOptions, GarbageCollectionReport,
        IndexStats, LeafMode, RebuildOptions, RebuildReport, SearchHit, SearchMode, SearchOptions,
        SearchReport, SearchTerminationReason, VectorRecord,
    },
    segment::{
        Segment, SegmentGraph, pq_code_for_query, routing_code, vector_bounds, vector_locality_key,
        vector_signature,
    },
    storage::{RoutingLayerPageIndexRead, Storage},
};

const LOCAL_GRAPH_NEIGHBORS: usize = 8;
const ROUTING_SEARCH_PAGE_OVERFETCH: usize = 8;

#[derive(Debug, Default)]
struct RoutingSummariesRead {
    summaries: Vec<SegmentSummary>,
    bytes_read: u64,
    routing_page_indexes_read: usize,
    routing_pages_read: usize,
    object_cache_hits: usize,
    object_cache_misses: usize,
}

#[derive(Debug, Default)]
struct ActiveSegmentObjectPathsRead {
    paths: HashSet<String>,
    bytes_read: u64,
    routing_page_indexes_read: usize,
    routing_pages_read: usize,
    object_cache_hits: usize,
    object_cache_misses: usize,
}

#[derive(Debug, Default)]
struct RoutingPageRefsRead {
    page_refs: Vec<RoutingLayerPageRef>,
    bytes_read: u64,
    routing_page_indexes_read: usize,
    routing_pages_read: usize,
    object_cache_hits: usize,
    object_cache_misses: usize,
}

#[derive(Debug, Default)]
struct CompactionSourceSelectionRead {
    selected: Vec<SegmentSummary>,
    dirty_pages: Vec<(usize, Vec<SegmentSummary>)>,
    decoded_parent_pages: HashMap<String, Vec<RoutingLayerPageRef>>,
    bytes_read: u64,
    routing_page_indexes_read: usize,
    routing_pages_read: usize,
    object_cache_hits: usize,
    object_cache_misses: usize,
}

#[derive(Debug, Default)]
struct CompactionRoutingPatch {
    page_refs: Vec<RoutingLayerPageRef>,
    bytes_read: u64,
    routing_pages_read: usize,
    routing_pages_written: usize,
    object_cache_hits: usize,
    object_cache_misses: usize,
}

#[derive(Debug)]
struct CompactionRoutingPageUpdate {
    page_ref: RoutingLayerPageRef,
    patch: CompactionRoutingPatch,
}

#[derive(Debug)]
struct CompactionTopRoutingPageRefs {
    routing_level: u8,
    page_refs: Vec<RoutingLayerPageRef>,
    routing_pages_written: usize,
}

#[derive(Debug, Clone)]
struct SearchHitWithVector {
    hit: SearchHit,
    vector: Option<Vec<f32>>,
}

#[derive(Debug)]
struct SearchExecution {
    report: SearchReport,
    vectors: Vec<Vec<f32>>,
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
#[derive(Debug, Clone)]
pub struct OpenOptions {
    /// Optional local read-through cache directory.
    pub cache_dir: Option<PathBuf>,
    /// Optional runtime resident manifest/routing memory budget in bytes.
    pub ram_budget_bytes: Option<u64>,
    /// Keep full segment routing summaries resident after open.
    ///
    /// Set to `false` for large object-store indexes that should resolve
    /// segments from persisted routing pages instead of resident summaries.
    pub resident_routing: bool,
}

impl Default for OpenOptions {
    fn default() -> Self {
        Self {
            cache_dir: None,
            ram_budget_bytes: None,
            resident_routing: true,
        }
    }
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
    routing_leaf_pages: usize,
    routing_pages: usize,
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

    /// Create a new empty index with an explicit routing page fanout.
    pub fn create_with_routing_page_fanout(
        config: IndexConfig,
        routing_page_fanout: usize,
    ) -> Result<Self> {
        Self::create_with_cache_and_routing_page_fanout(config, None, routing_page_fanout)
    }

    /// Create a new empty index with an optional local read-through cache.
    pub fn create_with_cache(config: IndexConfig, cache_dir: Option<PathBuf>) -> Result<Self> {
        Self::create_with_cache_and_routing_page_fanout(
            config,
            cache_dir,
            DEFAULT_ROUTING_PAGE_FANOUT,
        )
    }

    /// Create a new empty index with cache and explicit routing fanout options.
    pub fn create_with_cache_and_routing_page_fanout(
        config: IndexConfig,
        cache_dir: Option<PathBuf>,
        routing_page_fanout: usize,
    ) -> Result<Self> {
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
        if routing_page_fanout <= 1 {
            return Err(BorsukError::InvalidMetricInput(
                "routing_page_fanout must be greater than one".to_string(),
            ));
        }

        let storage = if let Some(cache_dir) = cache_dir {
            Storage::from_uri_with_cache(&config.uri, Some(cache_dir))?
        } else {
            Storage::from_uri(&config.uri)?
        };
        storage.create_layout()?;

        let manifest = Manifest::new_with_routing_page_fanout(config, routing_page_fanout);
        enforce_ram_budget(&manifest, None)?;
        let manifest = storage.publish_manifest(&manifest)?;

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
                resident_routing: true,
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
        let manifest = if options.resident_routing {
            storage.load_current_manifest()?
        } else {
            let manifest = storage.load_current_manifest_metadata()?;
            let page_refs = storage
                .read_routing_layer_page_index(manifest.version, manifest.routing_max_level)?;
            if page_refs.is_empty() {
                return Err(BorsukError::InvalidStorage(
                    "paged routing open requires a routing page index".to_string(),
                ));
            }
            manifest
        };
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
            routing_max_level: self.manifest.routing_max_level,
            routing_page_fanout: self.manifest.routing_page_fanout,
            routing_leaf_pages: totals.routing_leaf_pages,
            routing_pages: totals.routing_pages,
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

        let page_refs = self.storage.read_routing_layer_page_index(
            self.manifest.version,
            self.manifest.routing_max_level,
        )?;
        let (routing_leaf_pages, routing_pages) =
            self.routing_topology_totals_from_top_page_refs(&page_refs)?;

        Ok(StatsTotals {
            routing_leaf_pages,
            routing_pages,
            segments: page_refs
                .iter()
                .map(|page_ref| page_ref.leaf_segments)
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
        let segments = self.manifest.segments.len();
        StatsTotals {
            routing_leaf_pages: routing_leaf_page_count(
                segments,
                self.manifest.routing_page_fanout,
            ),
            routing_pages: routing_page_tree_content_page_count(
                segments,
                self.manifest.routing_page_fanout,
            ),
            segments,
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

    fn routing_topology_totals_from_top_page_refs(
        &self,
        top_page_refs: &[RoutingLayerPageRef],
    ) -> Result<(usize, usize)> {
        let Some(first_page_ref) = top_page_refs.first() else {
            return Ok((0, 0));
        };
        let routing_level = first_page_ref.routing_level;
        if top_page_refs
            .iter()
            .any(|page_ref| page_ref.routing_level != routing_level)
        {
            return Err(BorsukError::InvalidStorage(
                "routing stats found mixed top routing levels".to_string(),
            ));
        }
        if routing_level == 0 {
            return Ok((top_page_refs.len(), top_page_refs.len()));
        }
        if top_page_refs
            .iter()
            .all(|page_ref| page_ref.leaf_pages > 0 && page_ref.routing_pages > 0)
        {
            return Ok((
                top_page_refs
                    .iter()
                    .map(|page_ref| page_ref.leaf_pages)
                    .sum(),
                top_page_refs
                    .iter()
                    .map(|page_ref| page_ref.routing_pages)
                    .sum(),
            ));
        }

        let leaf_read = self.routing_leaf_page_refs_for_filter_read(top_page_refs, |_| true)?;
        let routing_leaf_pages = leaf_read.page_refs.len();
        let routing_pages = leaf_read
            .routing_pages_read
            .saturating_add(routing_leaf_pages);
        Ok((routing_leaf_pages, routing_pages))
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

        if self.manifest.segments.is_empty() {
            let top_read = self.storage.read_routing_layer_page_index_with_status(
                self.manifest.version,
                self.manifest.routing_max_level,
            )?;
            if !top_read.page_refs.is_empty() {
                return self.add_records_to_top_routing_page_refs(
                    records,
                    next_generated_id,
                    self.manifest.routing_max_level,
                    top_read.page_refs,
                );
            }
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
        self.manifest = self
            .storage
            .publish_manifest_reusing_routing_pages(&manifest, Some(&self.manifest))?;
        Ok(())
    }

    fn add_records_to_top_routing_page_refs(
        &mut self,
        records: Vec<VectorRecord>,
        next_generated_id: u64,
        top_routing_level: u8,
        mut top_page_refs: Vec<RoutingLayerPageRef>,
    ) -> Result<()> {
        if top_page_refs
            .iter()
            .any(|page_ref| page_ref.routing_level != top_routing_level)
        {
            return Err(BorsukError::InvalidStorage(
                "top routing page refs contain mixed routing levels".to_string(),
            ));
        }

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

        let mut decoded_parent_pages = HashMap::new();
        if top_routing_level > 0
            && self
                .cache_rightmost_routing_branch(&top_page_refs, &mut decoded_parent_pages)
                .is_err()
        {
            decoded_parent_pages.clear();
        }

        let mut occupied_leaf_ranges = leaf_page_occupied_ranges_from_cached_tree(
            &top_page_refs,
            &decoded_parent_pages,
            self.manifest.routing_page_fanout,
        )?;
        let mut next_leaf_page_ordinal = 0_usize;
        let mut new_leaf_page_refs = Vec::new();
        for summaries in new_summaries.chunks(self.manifest.routing_page_fanout) {
            let page_ordinal = next_available_leaf_page_ordinal(
                &mut next_leaf_page_ordinal,
                &mut occupied_leaf_ranges,
            )?;
            let page_ref =
                self.storage
                    .write_routing_layer_page(&manifest, 0, page_ordinal, summaries)?;
            new_leaf_page_refs.push(page_ref);
        }

        if top_routing_level == 0 {
            top_page_refs.extend(new_leaf_page_refs);
            top_page_refs.sort_by_key(|page_ref| page_ref.page_ordinal);
            enforce_ram_budget(&manifest, self.runtime_ram_budget_bytes)?;
            self.manifest = self
                .storage
                .publish_manifest_with_routing_page_refs(&manifest, &top_page_refs)?;
            return Ok(());
        }

        let patch = self.routing_top_page_refs_with_leaf_updates(
            &manifest,
            top_routing_level,
            &top_page_refs,
            &new_leaf_page_refs,
            &mut decoded_parent_pages,
        )?;
        let promoted_top_refs = self.promote_top_routing_page_refs_if_needed(
            &manifest,
            top_routing_level,
            patch.page_refs,
        )?;
        enforce_ram_budget(&manifest, self.runtime_ram_budget_bytes)?;
        self.manifest = self.storage.publish_manifest_with_top_routing_page_refs(
            &manifest,
            promoted_top_refs.routing_level,
            &promoted_top_refs.page_refs,
        )?;
        Ok(())
    }

    fn cache_rightmost_routing_branch(
        &self,
        top_page_refs: &[RoutingLayerPageRef],
        decoded_parent_pages: &mut HashMap<String, Vec<RoutingLayerPageRef>>,
    ) -> Result<()> {
        let Some(mut page_ref) = top_page_refs
            .iter()
            .max_by_key(|page_ref| page_ref.page_ordinal)
            .cloned()
        else {
            return Ok(());
        };

        while page_ref.routing_level > 0 {
            let child_read = self.routing_child_page_refs_read_from_parent_refs_with_cache(
                std::slice::from_ref(&page_ref),
                Some(decoded_parent_pages),
            )?;
            let Some(rightmost_child) = child_read
                .page_refs
                .into_iter()
                .max_by_key(|page_ref| page_ref.page_ordinal)
            else {
                return Ok(());
            };
            page_ref = rightmost_child;
        }

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

        self.get_vector_by_id(id.as_bytes())
    }

    /// Load a stored vector by its byte identifier.
    pub fn get_vector_by_id(&self, id: impl AsRef<[u8]>) -> Result<Option<Vec<f32>>> {
        let id_bytes = id.as_ref();
        if id_bytes.is_empty() {
            return Err(BorsukError::InvalidRecordInput(
                "record ids must not be empty".to_string(),
            ));
        }

        for summary in self.manifest.segments.iter().rev() {
            if !summary.might_contain_record_id(id_bytes) {
                continue;
            }
            let (segment, _, _) = self.read_segment(summary)?;
            if let Some(record) = segment
                .records
                .iter()
                .rev()
                .find(|record| record.id.as_bytes() == id_bytes)
            {
                return Ok(Some(record.vector.clone()));
            }
        }

        if self.manifest.segments.is_empty() {
            return self.get_vector_from_routing_pages(id_bytes);
        }

        Ok(None)
    }

    fn get_vector_from_routing_pages(&self, id_bytes: &[u8]) -> Result<Option<Vec<f32>>> {
        let page_index_read = self.routing_layer_page_index_read_for_search()?;
        let page_refs = self
            .routing_leaf_page_refs_for_filter(&page_index_read.page_refs, |page_ref| {
                page_ref.might_contain_record_id(id_bytes)
            })?;

        for page_ref in page_refs.iter().rev() {
            let summaries =
                self.routing_summaries_from_page_refs(std::slice::from_ref(page_ref))?;
            for summary in summaries.iter().rev() {
                if !summary.might_contain_record_id(id_bytes) {
                    continue;
                }
                let (segment, _, _) = self.read_segment(summary)?;
                if let Some(record) = segment
                    .records
                    .iter()
                    .rev()
                    .find(|record| record.id.as_bytes() == id_bytes)
                {
                    return Ok(Some(record.vector.clone()));
                }
            }
        }

        Ok(None)
    }

    fn validate_record_ids(&self, records: &[VectorRecord], scan_existing_ids: bool) -> Result<()> {
        let mut batch_ids = HashSet::<&[u8]>::with_capacity(records.len());
        for record in records {
            if record.id.is_empty() {
                return Err(BorsukError::InvalidRecordInput(
                    "record ids must not be empty".to_string(),
                ));
            }
            if !batch_ids.insert(record.id.as_bytes()) {
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
        let page_index_read = self.routing_layer_page_index_read_for_search()?;
        let page_refs =
            self.routing_leaf_page_refs_for_filter(&page_index_read.page_refs, |page_ref| {
                records
                    .iter()
                    .any(|record| page_ref.might_contain_record_id(&record.id))
            })?;
        for page_ref in page_refs.iter().rev() {
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
        let page_index_read = self.routing_layer_page_index_read_for_compaction()?;
        if !page_index_read.page_refs.is_empty() {
            return self.compact_from_routing_tree(options, max_segments, page_index_read);
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
                routing_page_indexes_read: page_index_read.page_indexes_read,
                routing_pages_read: 0,
                routing_page_indexes_written: 0,
                routing_pages_written: 0,
                graph_payloads_read: 0,
                graph_bytes_read: 0,
                bytes_read: page_index_read.bytes_read,
                bytes_written: 0,
                object_cache_hits: page_index_read.object_cache_hits,
                object_cache_misses: page_index_read.object_cache_misses,
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
        let mut bytes_read = page_index_read.bytes_read;
        let mut object_cache_hits = page_index_read.object_cache_hits;
        let mut object_cache_misses = page_index_read.object_cache_misses;

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
        let routing_pages_written = routing_page_tree_content_page_count(
            manifest.segments.len(),
            manifest.routing_page_fanout,
        );
        enforce_ram_budget(&manifest, self.runtime_ram_budget_bytes)?;
        self.manifest = self
            .storage
            .publish_manifest_reusing_routing_pages(&manifest, Some(&self.manifest))?;
        let routing_page_indexes_written = usize::from(self.manifest.routing_max_level) + 1;

        Ok(CompactionReport {
            compacted: true,
            source_level: options.source_level,
            target_level: options.target_level,
            segments_read: selected.len(),
            segments_written,
            records_rewritten: records.len(),
            routing_page_indexes_read: page_index_read.page_indexes_read,
            routing_pages_read: 0,
            routing_page_indexes_written,
            routing_pages_written,
            graph_payloads_read: 0,
            graph_bytes_read: 0,
            bytes_read,
            bytes_written,
            object_cache_hits,
            object_cache_misses,
            manifest_version: self.manifest.version,
        })
    }

    fn routing_layer_page_index_read_for_compaction(&self) -> Result<RoutingLayerPageIndexRead> {
        let top_read = self.storage.read_routing_layer_page_index_with_status(
            self.manifest.version,
            self.manifest.routing_max_level,
        )?;
        if !top_read.page_refs.is_empty() {
            return Ok(top_read);
        }

        if self.manifest.routing_max_level == 0 {
            return Ok(top_read);
        }

        let mut leaf_read = self
            .storage
            .read_routing_layer_page_index_with_status(self.manifest.version, 0)?;
        leaf_read.bytes_read += top_read.bytes_read;
        leaf_read.page_indexes_read += top_read.page_indexes_read;
        leaf_read.object_cache_hits += top_read.object_cache_hits;
        leaf_read.object_cache_misses += top_read.object_cache_misses;
        Ok(leaf_read)
    }

    fn compact_from_routing_tree(
        &mut self,
        options: CompactionOptions,
        max_segments: usize,
        page_index_read: RoutingLayerPageIndexRead,
    ) -> Result<CompactionReport> {
        let top_routing_level = page_index_read
            .page_refs
            .first()
            .map(|page_ref| page_ref.routing_level)
            .unwrap_or(0);
        let top_page_refs = page_index_read.page_refs.clone();
        let full_leaf_page_refs = page_index_read
            .page_refs
            .first()
            .is_some_and(|page_ref| page_ref.routing_level == 0)
            .then(|| page_index_read.page_refs.clone());
        let source_selection = self.compaction_source_selection_from_routing_tree(
            options.source_level,
            max_segments,
            page_index_read,
        )?;
        let selected = source_selection.selected;
        let dirty_pages = source_selection.dirty_pages;
        let mut decoded_parent_pages = source_selection.decoded_parent_pages;
        let routing_page_indexes_read = source_selection.routing_page_indexes_read;
        let routing_bytes_read = source_selection.bytes_read;
        let mut routing_pages_read = source_selection.routing_pages_read;
        let mut routing_pages_written = 0_usize;
        let routing_page_indexes_written;
        let routing_object_cache_hits = source_selection.object_cache_hits;
        let routing_object_cache_misses = source_selection.object_cache_misses;

        if selected.len() < options.min_segments {
            return Ok(CompactionReport {
                compacted: false,
                source_level: options.source_level,
                target_level: options.target_level,
                segments_read: 0,
                segments_written: 0,
                records_rewritten: 0,
                routing_page_indexes_read,
                routing_pages_read,
                routing_page_indexes_written: 0,
                routing_pages_written: 0,
                graph_payloads_read: 0,
                graph_bytes_read: 0,
                bytes_read: routing_bytes_read,
                bytes_written: 0,
                object_cache_hits: routing_object_cache_hits,
                object_cache_misses: routing_object_cache_misses,
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
        let mut bytes_read = routing_bytes_read;
        let mut object_cache_hits = routing_object_cache_hits;
        let mut object_cache_misses = routing_object_cache_misses;

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
        let dirty_page_count = dirty_pages.len();
        let dirty_page_ordinals = dirty_pages
            .iter()
            .map(|(page_ordinal, _)| *page_ordinal)
            .collect::<Vec<_>>();
        let mut replacement_summaries = dirty_pages
            .into_iter()
            .flat_map(|(_, page_summaries)| page_summaries)
            .filter(|summary| !selected_ids.contains(summary.id.as_str()))
            .collect::<Vec<_>>();

        let mut manifest = self.manifest.next_version();
        manifest.segments.clear();
        manifest.pivots.clear();

        let mut segments_written = 0_usize;
        let mut bytes_written = 0_u64;
        let min_output_segments = dirty_page_count
            .saturating_sub(replacement_summaries.len())
            .max(1);
        let output_chunk_size = output_segment_chunk_size(
            records.len(),
            target_segment_max_vectors,
            min_output_segments,
        );

        for chunk in records.chunks(output_chunk_size) {
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

        let replacement_pages = split_summaries_for_routing_pages(
            replacement_summaries,
            dirty_page_count,
            manifest.routing_page_fanout,
        );
        let needs_leaf_page_append = replacement_pages.len() > dirty_page_count;
        if let Some(mut page_refs) = full_leaf_page_refs {
            let mut occupied_leaf_ranges = leaf_page_occupied_ranges_from_cached_tree(
                &page_refs,
                &HashMap::new(),
                manifest.routing_page_fanout,
            )?;
            let mut next_appended_leaf_ordinal = dirty_page_ordinals.first().copied().unwrap_or(0);

            for (chunk_index, summaries) in replacement_pages.iter().enumerate() {
                let target_page_ordinal = if chunk_index < dirty_page_count {
                    dirty_page_ordinals[chunk_index]
                } else {
                    next_available_leaf_page_ordinal(
                        &mut next_appended_leaf_ordinal,
                        &mut occupied_leaf_ranges,
                    )?
                };
                let page_ref = self.storage.write_routing_layer_page(
                    &manifest,
                    0,
                    target_page_ordinal,
                    summaries,
                )?;
                routing_pages_written += 1;
                upsert_leaf_page_ref_by_ordinal(&mut page_refs, page_ref)?;
            }
            let promoted_top_refs =
                self.promote_top_routing_page_refs_if_needed(&manifest, 0, page_refs)?;
            routing_pages_written += promoted_top_refs.routing_pages_written;
            enforce_ram_budget(&manifest, self.runtime_ram_budget_bytes)?;
            self.manifest = self.storage.publish_manifest_with_top_routing_page_refs(
                &manifest,
                promoted_top_refs.routing_level,
                &promoted_top_refs.page_refs,
            )?;
            routing_page_indexes_written = 1;
        } else if needs_leaf_page_append {
            let mut occupied_leaf_ranges = leaf_page_occupied_ranges_from_cached_tree(
                &top_page_refs,
                &decoded_parent_pages,
                manifest.routing_page_fanout,
            )?;
            let mut next_appended_leaf_ordinal = dirty_page_ordinals.first().copied().unwrap_or(0);
            let mut updated_leaf_page_refs = Vec::with_capacity(replacement_pages.len());

            for (chunk_index, summaries) in replacement_pages.iter().enumerate() {
                let target_page_ordinal = if chunk_index < dirty_page_count {
                    dirty_page_ordinals[chunk_index]
                } else {
                    next_available_leaf_page_ordinal(
                        &mut next_appended_leaf_ordinal,
                        &mut occupied_leaf_ranges,
                    )?
                };
                updated_leaf_page_refs.push(self.storage.write_routing_layer_page(
                    &manifest,
                    0,
                    target_page_ordinal,
                    summaries,
                )?);
                routing_pages_written += 1;
            }

            let patch = self.routing_top_page_refs_with_leaf_updates(
                &manifest,
                top_routing_level,
                &top_page_refs,
                &updated_leaf_page_refs,
                &mut decoded_parent_pages,
            )?;
            bytes_read += patch.bytes_read;
            routing_pages_read += patch.routing_pages_read;
            routing_pages_written += patch.routing_pages_written;
            object_cache_hits += patch.object_cache_hits;
            object_cache_misses += patch.object_cache_misses;
            let promoted_top_refs = self.promote_top_routing_page_refs_if_needed(
                &manifest,
                top_routing_level,
                patch.page_refs,
            )?;
            routing_pages_written += promoted_top_refs.routing_pages_written;
            enforce_ram_budget(&manifest, self.runtime_ram_budget_bytes)?;
            self.manifest = self.storage.publish_manifest_with_top_routing_page_refs(
                &manifest,
                promoted_top_refs.routing_level,
                &promoted_top_refs.page_refs,
            )?;
            routing_page_indexes_written = 1;
        } else {
            let mut replacement_leaf_page_refs = Vec::with_capacity(replacement_pages.len());
            for (chunk_index, summaries) in replacement_pages.iter().enumerate() {
                let target_page_ordinal = dirty_page_ordinals[chunk_index];
                replacement_leaf_page_refs.push(self.storage.write_routing_layer_page(
                    &manifest,
                    0,
                    target_page_ordinal,
                    summaries,
                )?);
                routing_pages_written += 1;
            }
            let patch = self.routing_top_page_refs_with_leaf_updates(
                &manifest,
                top_routing_level,
                &top_page_refs,
                &replacement_leaf_page_refs,
                &mut decoded_parent_pages,
            )?;
            bytes_read += patch.bytes_read;
            routing_pages_read += patch.routing_pages_read;
            routing_pages_written += patch.routing_pages_written;
            object_cache_hits += patch.object_cache_hits;
            object_cache_misses += patch.object_cache_misses;
            let promoted_top_refs = self.promote_top_routing_page_refs_if_needed(
                &manifest,
                top_routing_level,
                patch.page_refs,
            )?;
            routing_pages_written += promoted_top_refs.routing_pages_written;
            enforce_ram_budget(&manifest, self.runtime_ram_budget_bytes)?;
            self.manifest = self.storage.publish_manifest_with_top_routing_page_refs(
                &manifest,
                promoted_top_refs.routing_level,
                &promoted_top_refs.page_refs,
            )?;
            routing_page_indexes_written = 1;
        }

        Ok(CompactionReport {
            compacted: true,
            source_level: options.source_level,
            target_level: options.target_level,
            segments_read: selected.len(),
            segments_written,
            records_rewritten: records.len(),
            routing_page_indexes_read,
            routing_pages_read,
            routing_page_indexes_written,
            routing_pages_written,
            graph_payloads_read: 0,
            graph_bytes_read: 0,
            bytes_read,
            bytes_written,
            object_cache_hits,
            object_cache_misses,
            manifest_version: self.manifest.version,
        })
    }

    fn promote_top_routing_page_refs_if_needed(
        &self,
        manifest: &Manifest,
        mut routing_level: u8,
        mut page_refs: Vec<RoutingLayerPageRef>,
    ) -> Result<CompactionTopRoutingPageRefs> {
        let mut routing_pages_written = 0_usize;

        while page_refs.len() > manifest.routing_page_fanout {
            if page_refs
                .iter()
                .any(|page_ref| page_ref.routing_level != routing_level)
            {
                return Err(BorsukError::InvalidStorage(
                    "top routing page refs contain mixed routing levels".to_string(),
                ));
            }
            let parent_routing_level = routing_level.checked_add(1).ok_or_else(|| {
                BorsukError::InvalidStorage("routing layer depth exceeds u8".to_string())
            })?;
            let grouped_child_refs =
                routing_page_refs_by_parent_ordinal(&page_refs, manifest.routing_page_fanout);
            let mut promoted_page_refs = Vec::with_capacity(grouped_child_refs.len());
            for (page_ordinal, child_refs) in grouped_child_refs {
                promoted_page_refs.push(self.storage.write_parent_routing_layer_page(
                    manifest,
                    parent_routing_level,
                    page_ordinal,
                    &child_refs,
                )?);
                routing_pages_written += 1;
            }
            routing_level = parent_routing_level;
            page_refs = promoted_page_refs;
        }

        Ok(CompactionTopRoutingPageRefs {
            routing_level,
            page_refs,
            routing_pages_written,
        })
    }

    fn routing_top_page_refs_with_leaf_updates(
        &self,
        manifest: &Manifest,
        top_routing_level: u8,
        top_page_refs: &[RoutingLayerPageRef],
        updated_leaf_page_refs: &[RoutingLayerPageRef],
        decoded_parent_pages: &mut HashMap<String, Vec<RoutingLayerPageRef>>,
    ) -> Result<CompactionRoutingPatch> {
        if top_routing_level == 0 {
            return Err(BorsukError::InvalidStorage(
                "top routing update without L0 page refs".to_string(),
            ));
        }
        let updates = leaf_page_ref_updates_by_ordinal(updated_leaf_page_refs)?;
        let mut rewritten_top_refs = Vec::with_capacity(top_page_refs.len());
        let mut patch = CompactionRoutingPatch::default();
        for page_ref in top_page_refs {
            if routing_subtree_contains_leaf_update(
                page_ref,
                &updates,
                manifest.routing_page_fanout,
            ) {
                let update = self.routing_parent_page_ref_with_leaf_updates(
                    manifest,
                    page_ref,
                    &updates,
                    decoded_parent_pages,
                )?;
                patch.bytes_read += update.patch.bytes_read;
                patch.routing_pages_read += update.patch.routing_pages_read;
                patch.routing_pages_written += update.patch.routing_pages_written;
                patch.object_cache_hits += update.patch.object_cache_hits;
                patch.object_cache_misses += update.patch.object_cache_misses;
                rewritten_top_refs.push(update.page_ref);
            } else {
                rewritten_top_refs.push(page_ref.clone());
            }
        }

        let existing_top_page_ordinals = top_page_refs
            .iter()
            .map(|page_ref| page_ref.page_ordinal)
            .collect::<HashSet<_>>();
        let new_top_leaf_updates = leaf_page_ref_updates_by_parent_ordinal(
            top_routing_level,
            updated_leaf_page_refs.iter().filter(|page_ref| {
                !top_page_refs.iter().any(|top_page_ref| {
                    routing_subtree_contains_leaf_ordinal(
                        top_page_ref,
                        page_ref.page_ordinal,
                        manifest.routing_page_fanout,
                    )
                })
            }),
            manifest.routing_page_fanout,
        )?;
        for (top_page_ordinal, leaf_updates) in new_top_leaf_updates {
            if existing_top_page_ordinals.contains(&top_page_ordinal) {
                continue;
            }
            let update = self.routing_parent_page_ref_from_leaf_updates(
                manifest,
                top_routing_level,
                top_page_ordinal,
                &leaf_updates,
            )?;
            patch.routing_pages_written += update.patch.routing_pages_written;
            rewritten_top_refs.push(update.page_ref);
        }
        rewritten_top_refs.sort_by_key(|page_ref| page_ref.page_ordinal);
        patch.page_refs = rewritten_top_refs;
        Ok(patch)
    }

    fn routing_parent_page_ref_with_leaf_updates(
        &self,
        manifest: &Manifest,
        parent_ref: &RoutingLayerPageRef,
        updates: &HashMap<usize, RoutingLayerPageRef>,
        decoded_parent_pages: &mut HashMap<String, Vec<RoutingLayerPageRef>>,
    ) -> Result<CompactionRoutingPageUpdate> {
        let child_routing_level = parent_ref.routing_level.checked_sub(1).ok_or_else(|| {
            BorsukError::InvalidStorage("cannot rewrite children below L0 routing page".to_string())
        })?;
        let child_read = self.routing_child_page_refs_read_from_parent_refs_with_cache(
            std::slice::from_ref(parent_ref),
            Some(decoded_parent_pages),
        )?;
        let mut patch = CompactionRoutingPatch {
            bytes_read: child_read.bytes_read,
            routing_pages_read: child_read.routing_pages_read,
            object_cache_hits: child_read.object_cache_hits,
            object_cache_misses: child_read.object_cache_misses,
            ..Default::default()
        };
        let mut child_refs = child_read.page_refs;
        let mut existing_child_ordinals = HashSet::with_capacity(child_refs.len());
        for child_ref in &mut child_refs {
            existing_child_ordinals.insert(child_ref.page_ordinal);
            if child_routing_level == 0 {
                if let Some(update) = updates.get(&child_ref.page_ordinal) {
                    *child_ref = update.clone();
                }
            } else if routing_subtree_contains_leaf_update(
                child_ref,
                updates,
                manifest.routing_page_fanout,
            ) {
                let update = self.routing_parent_page_ref_with_leaf_updates(
                    manifest,
                    child_ref,
                    updates,
                    decoded_parent_pages,
                );
                let update = update?;
                patch.bytes_read += update.patch.bytes_read;
                patch.routing_pages_read += update.patch.routing_pages_read;
                patch.routing_pages_written += update.patch.routing_pages_written;
                patch.object_cache_hits += update.patch.object_cache_hits;
                patch.object_cache_misses += update.patch.object_cache_misses;
                *child_ref = update.page_ref;
            }
        }

        let new_child_updates = leaf_page_ref_updates_by_parent_ordinal(
            child_routing_level,
            updates
                .values()
                .filter(|page_ref| {
                    routing_subtree_contains_leaf_ordinal(
                        parent_ref,
                        page_ref.page_ordinal,
                        manifest.routing_page_fanout,
                    )
                })
                .filter(|page_ref| {
                    let child_ordinal = routing_parent_ordinal_for_leaf(
                        child_routing_level,
                        page_ref.page_ordinal,
                        manifest.routing_page_fanout,
                    )
                    .ok();
                    child_ordinal.is_some_and(|ordinal| !existing_child_ordinals.contains(&ordinal))
                }),
            manifest.routing_page_fanout,
        )?;
        for (child_page_ordinal, leaf_updates) in new_child_updates {
            if child_routing_level == 0 {
                child_refs.extend(leaf_updates);
            } else {
                let update = self.routing_parent_page_ref_from_leaf_updates(
                    manifest,
                    child_routing_level,
                    child_page_ordinal,
                    &leaf_updates,
                )?;
                patch.routing_pages_written += update.patch.routing_pages_written;
                child_refs.push(update.page_ref);
            }
        }
        child_refs.sort_by_key(|page_ref| page_ref.page_ordinal);

        let page_ref = self.storage.write_parent_routing_layer_page(
            manifest,
            parent_ref.routing_level,
            parent_ref.page_ordinal,
            &child_refs,
        )?;
        patch.routing_pages_written += 1;
        Ok(CompactionRoutingPageUpdate { page_ref, patch })
    }

    fn routing_parent_page_ref_from_leaf_updates(
        &self,
        manifest: &Manifest,
        routing_level: u8,
        page_ordinal: usize,
        leaf_updates: &[RoutingLayerPageRef],
    ) -> Result<CompactionRoutingPageUpdate> {
        if routing_level == 0 {
            return Err(BorsukError::InvalidStorage(
                "cannot build parent routing page at L0".to_string(),
            ));
        }
        for leaf_update in leaf_updates {
            let parent_ordinal = routing_parent_ordinal_for_leaf(
                routing_level,
                leaf_update.page_ordinal,
                manifest.routing_page_fanout,
            )?;
            if parent_ordinal != page_ordinal {
                return Err(BorsukError::InvalidStorage(format!(
                    "leaf routing page {} does not belong to L{} parent page {}",
                    leaf_update.page_ordinal, routing_level, page_ordinal
                )));
            }
        }
        let child_routing_level = routing_level.checked_sub(1).ok_or_else(|| {
            BorsukError::InvalidStorage("cannot build children below L0 routing page".to_string())
        })?;
        let grouped_updates = leaf_page_ref_updates_by_parent_ordinal(
            child_routing_level,
            leaf_updates.iter(),
            manifest.routing_page_fanout,
        )?;
        let mut child_refs = Vec::with_capacity(grouped_updates.len());
        let mut patch = CompactionRoutingPatch::default();
        for (child_page_ordinal, leaf_updates) in grouped_updates {
            if child_routing_level == 0 {
                child_refs.extend(leaf_updates);
            } else {
                let update = self.routing_parent_page_ref_from_leaf_updates(
                    manifest,
                    child_routing_level,
                    child_page_ordinal,
                    &leaf_updates,
                )?;
                patch.routing_pages_written += update.patch.routing_pages_written;
                child_refs.push(update.page_ref);
            }
        }
        child_refs.sort_by_key(|page_ref| page_ref.page_ordinal);

        let page_ref = self.storage.write_parent_routing_layer_page(
            manifest,
            routing_level,
            page_ordinal,
            &child_refs,
        )?;
        patch.routing_pages_written += 1;
        Ok(CompactionRoutingPageUpdate { page_ref, patch })
    }

    /// Rebuild a full source level into a target level, then report or delete obsolete objects.
    pub fn rebuild(&mut self, options: RebuildOptions) -> Result<RebuildReport> {
        let compaction = self.compact(CompactionOptions {
            source_level: options.source_level,
            target_level: options.target_level,
            max_segments: None,
            min_segments: options.min_segments,
            target_segment_max_vectors: options.target_segment_max_vectors,
        })?;
        let garbage_collection = self.gc_obsolete_segments(GarbageCollectionOptions {
            dry_run: !options.delete_obsolete,
        })?;

        Ok(RebuildReport {
            compaction,
            garbage_collection,
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
                object.path.ends_with(".parquet") && !active_paths.paths.contains(&object.path)
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
                routing_page_indexes_read: active_paths.routing_page_indexes_read,
                routing_pages_read: active_paths.routing_pages_read,
                bytes_read: active_paths.bytes_read,
                bytes_reclaimable,
                bytes_reclaimed: 0,
                object_cache_hits: active_paths.object_cache_hits,
                object_cache_misses: active_paths.object_cache_misses,
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
            routing_page_indexes_read: active_paths.routing_page_indexes_read,
            routing_pages_read: active_paths.routing_pages_read,
            bytes_read: active_paths.bytes_read,
            bytes_reclaimable,
            bytes_reclaimed,
            object_cache_hits: active_paths.object_cache_hits,
            object_cache_misses: active_paths.object_cache_misses,
            candidates: candidate_paths,
        })
    }

    fn active_segment_object_paths(&self) -> Result<ActiveSegmentObjectPathsRead> {
        let active_summaries = self.active_segment_summaries_with_report()?;
        let mut paths = HashSet::new();
        for summary in &active_summaries.summaries {
            paths.insert(summary.path.clone());
            paths.insert(summary.graph_path.clone());
        }
        Ok(ActiveSegmentObjectPathsRead {
            paths,
            bytes_read: active_summaries.bytes_read,
            routing_page_indexes_read: active_summaries.routing_page_indexes_read,
            routing_pages_read: active_summaries.routing_pages_read,
            object_cache_hits: active_summaries.object_cache_hits,
            object_cache_misses: active_summaries.object_cache_misses,
        })
    }

    fn active_segment_summaries(&self) -> Result<Vec<SegmentSummary>> {
        if !self.manifest.segments.is_empty() {
            return Ok(self.manifest.segments.clone());
        }

        let page_refs = self
            .routing_leaf_page_refs_for_metadata_scan_with_report()?
            .page_refs;
        if page_refs.is_empty() {
            return Ok(Vec::new());
        }

        self.routing_summaries_from_page_refs(&page_refs)
    }

    fn active_segment_summaries_with_report(&self) -> Result<RoutingSummariesRead> {
        if !self.manifest.segments.is_empty() {
            return Ok(RoutingSummariesRead {
                summaries: self.manifest.segments.clone(),
                ..Default::default()
            });
        }

        let page_refs_read = self.routing_leaf_page_refs_for_metadata_scan_with_report()?;
        if page_refs_read.page_refs.is_empty() {
            return Ok(RoutingSummariesRead {
                bytes_read: page_refs_read.bytes_read,
                routing_page_indexes_read: page_refs_read.routing_page_indexes_read,
                routing_pages_read: page_refs_read.routing_pages_read,
                object_cache_hits: page_refs_read.object_cache_hits,
                object_cache_misses: page_refs_read.object_cache_misses,
                ..Default::default()
            });
        }

        let mut summaries_read =
            self.routing_summaries_read_from_page_refs(&page_refs_read.page_refs)?;
        summaries_read.bytes_read += page_refs_read.bytes_read;
        summaries_read.routing_page_indexes_read += page_refs_read.routing_page_indexes_read;
        summaries_read.routing_pages_read += page_refs_read.routing_pages_read;
        summaries_read.object_cache_hits += page_refs_read.object_cache_hits;
        summaries_read.object_cache_misses += page_refs_read.object_cache_misses;
        Ok(summaries_read)
    }

    fn search_hits(&self, query: &[f32], options: SearchOptions) -> Result<Vec<SearchHit>> {
        Ok(self.search_with_report(query, options)?.hits)
    }

    /// Search the index and return only matching identifiers.
    pub fn search_ids(&self, query: &[f32], options: SearchOptions) -> Result<Vec<String>> {
        self.search_hits(query, options)?
            .into_iter()
            .map(|hit| hit.id.to_utf8_string())
            .collect()
    }

    /// Search the index and return matching byte identifiers.
    pub fn search_id_bytes(&self, query: &[f32], options: SearchOptions) -> Result<Vec<Vec<u8>>> {
        Ok(self
            .search_hits(query, options)?
            .into_iter()
            .map(|hit| hit.id.as_bytes().to_vec())
            .collect())
    }

    /// Search the index and return stored vectors for the nearest neighbors.
    pub fn search_vectors(&self, query: &[f32], options: SearchOptions) -> Result<Vec<Vec<f32>>> {
        Ok(self.search_execution(query, options, true)?.vectors)
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
        self.search_hits_batch(queries, options)?
            .into_iter()
            .map(|hits| {
                hits.into_iter()
                    .map(|hit| hit.id.to_utf8_string())
                    .collect()
            })
            .collect()
    }

    /// Search multiple queries and return matching byte identifiers for each query.
    pub fn search_id_bytes_batch(
        &self,
        queries: &[Vec<f32>],
        options: SearchOptions,
    ) -> Result<Vec<Vec<Vec<u8>>>> {
        Ok(self
            .search_hits_batch(queries, options)?
            .into_iter()
            .map(|hits| {
                hits.into_iter()
                    .map(|hit| hit.id.as_bytes().to_vec())
                    .collect()
            })
            .collect())
    }

    /// Search multiple queries and return stored vectors for each query's nearest neighbors.
    pub fn search_vectors_batch(
        &self,
        queries: &[Vec<f32>],
        options: SearchOptions,
    ) -> Result<Vec<Vec<Vec<f32>>>> {
        queries
            .iter()
            .map(|query| self.search_vectors(query, options.clone()))
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
        Ok(self.search_execution(query, options, false)?.report)
    }

    fn search_execution(
        &self,
        query: &[f32],
        options: SearchOptions,
        include_vectors: bool,
    ) -> Result<SearchExecution> {
        self.validate_vector(query)?;
        validate_search_options(&options)?;

        let started = Instant::now();
        let page_index_read = self.routing_layer_page_index_read_for_search()?;
        let segments_total = self.routing_segments_total(&page_index_read.page_refs);
        let resident_bytes_estimate = self.manifest.resident_bytes_estimate();

        if options.k == 0 {
            return Ok(SearchExecution {
                report: SearchReport {
                    hits: Vec::new(),
                    leaf_mode: options.mode.leaf_mode().to_string(),
                    termination_reason: SearchTerminationReason::Complete,
                    segments_total,
                    segments_searched: 0,
                    segments_skipped: segments_total,
                    routing_page_indexes_read: 0,
                    routing_pages_read: 0,
                    bytes_read: 0,
                    graph_bytes_read: 0,
                    object_cache_hits: 0,
                    object_cache_misses: 0,
                    records_considered: 0,
                    records_scored: 0,
                    graph_candidates_added: 0,
                    resident_bytes_estimate,
                    elapsed_ms: started.elapsed().as_millis() as u64,
                },
                vectors: Vec::new(),
            });
        }

        let routing_read = self.routing_summaries_for_search(query, &options, page_index_read)?;
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

        let mut hits = Vec::<SearchHitWithVector>::new();
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
        let mut termination_reason = SearchTerminationReason::Complete;

        for (candidate_index, (summary, _, lower_bound)) in candidates.into_iter().enumerate() {
            if let Some(stop_reason) = search_stop_reason_before_segment(
                &hits,
                options.k,
                &options.mode,
                segments_searched,
                bytes_read,
                lower_bound,
                started.elapsed().as_millis() as u64,
            ) {
                termination_reason = stop_reason;
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

            let graph = if should_expand_segment_graph(
                &options.mode,
                options.k,
                summary.leaf_mode,
                segment.records.len(),
            ) {
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
                push_hit_with_vector(
                    &mut hits,
                    SearchHit {
                        id: record.id.clone(),
                        distance,
                    },
                    include_vectors.then(|| record.vector.clone()),
                    options.k,
                );
            }
        }

        let vectors = hits
            .iter()
            .filter_map(|hit| hit.vector.clone())
            .collect::<Vec<_>>();
        let hits = hits.into_iter().map(|hit| hit.hit).collect::<Vec<_>>();

        Ok(SearchExecution {
            report: SearchReport {
                hits,
                leaf_mode: options.mode.leaf_mode().to_string(),
                termination_reason,
                segments_total,
                segments_searched,
                segments_skipped,
                routing_page_indexes_read: routing_read.routing_page_indexes_read,
                routing_pages_read: routing_read.routing_pages_read,
                bytes_read,
                graph_bytes_read,
                object_cache_hits,
                object_cache_misses,
                records_considered,
                records_scored,
                graph_candidates_added,
                resident_bytes_estimate,
                elapsed_ms: started.elapsed().as_millis() as u64,
            },
            vectors,
        })
    }

    fn routing_summaries_for_search(
        &self,
        query: &[f32],
        options: &SearchOptions,
        page_index_read: RoutingLayerPageIndexRead,
    ) -> Result<RoutingSummariesRead> {
        let mut routing_read = RoutingSummariesRead {
            bytes_read: page_index_read.bytes_read,
            routing_page_indexes_read: page_index_read.page_indexes_read,
            object_cache_hits: page_index_read.object_cache_hits,
            object_cache_misses: page_index_read.object_cache_misses,
            ..Default::default()
        };

        if !page_index_read.page_refs.is_empty() {
            let selected_leaf_page_refs_read =
                self.routing_leaf_page_refs_for_search(query, options, &page_index_read.page_refs)?;
            routing_read.bytes_read += selected_leaf_page_refs_read.bytes_read;
            routing_read.routing_pages_read += selected_leaf_page_refs_read.routing_pages_read;
            routing_read.object_cache_hits += selected_leaf_page_refs_read.object_cache_hits;
            routing_read.object_cache_misses += selected_leaf_page_refs_read.object_cache_misses;
            let selected_pages_read = self
                .routing_summaries_read_from_page_refs(&selected_leaf_page_refs_read.page_refs)?;
            routing_read.bytes_read += selected_pages_read.bytes_read;
            routing_read.routing_pages_read += selected_pages_read.routing_pages_read;
            routing_read.object_cache_hits += selected_pages_read.object_cache_hits;
            routing_read.object_cache_misses += selected_pages_read.object_cache_misses;
            routing_read.summaries = selected_pages_read.summaries;
            return Ok(routing_read);
        }

        if self.manifest.segments.is_empty() {
            return Ok(routing_read);
        }

        let legacy_pages_read = self.routing_summaries_from_legacy_pages()?;
        routing_read.bytes_read += legacy_pages_read.bytes_read;
        routing_read.routing_pages_read += legacy_pages_read.routing_pages_read;
        routing_read.object_cache_hits += legacy_pages_read.object_cache_hits;
        routing_read.object_cache_misses += legacy_pages_read.object_cache_misses;
        routing_read.summaries = legacy_pages_read.summaries;
        Ok(routing_read)
    }

    fn routing_layer_page_index_read_for_search(&self) -> Result<RoutingLayerPageIndexRead> {
        if self.manifest.segments.is_empty() {
            let top_read = self.storage.read_routing_layer_page_index_with_status(
                self.manifest.version,
                self.manifest.routing_max_level,
            )?;
            if !top_read.page_refs.is_empty() || self.manifest.routing_max_level == 0 {
                return Ok(top_read);
            }
        }

        self.storage
            .read_routing_layer_page_index_with_status(self.manifest.version, 0)
    }

    fn routing_segments_total(&self, page_refs: &[RoutingLayerPageRef]) -> usize {
        if !self.manifest.segments.is_empty() {
            return self.manifest.segments.len();
        }

        page_refs
            .iter()
            .map(|page_ref| page_ref.leaf_segments)
            .sum()
    }

    fn routing_leaf_page_refs_for_metadata_scan_with_report(&self) -> Result<RoutingPageRefsRead> {
        let top_read = self.storage.read_routing_layer_page_index_with_status(
            self.manifest.version,
            self.manifest.routing_max_level,
        )?;
        let mut read_result = RoutingPageRefsRead {
            bytes_read: top_read.bytes_read,
            routing_page_indexes_read: top_read.page_indexes_read,
            object_cache_hits: top_read.object_cache_hits,
            object_cache_misses: top_read.object_cache_misses,
            ..Default::default()
        };
        if top_read.page_refs.is_empty() {
            return Ok(read_result);
        }
        if self.manifest.routing_max_level == 0 {
            read_result.page_refs = top_read.page_refs;
            return Ok(read_result);
        }
        let leaf_read =
            self.routing_leaf_page_refs_for_filter_read(&top_read.page_refs, |_| true)?;
        read_result.bytes_read += leaf_read.bytes_read;
        read_result.routing_pages_read += leaf_read.routing_pages_read;
        read_result.object_cache_hits += leaf_read.object_cache_hits;
        read_result.object_cache_misses += leaf_read.object_cache_misses;
        read_result.page_refs = leaf_read.page_refs;
        Ok(read_result)
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

        let prioritize_signature = should_prioritize_vector_signature(&options.mode);
        let query_signature = prioritize_signature.then(|| vector_signature(query));
        let mut ranked_pages = page_refs
            .iter()
            .map(|page_ref| {
                let lower_bound = page_ref.lower_bound(query, &self.manifest.config.metric)?;
                let signature_miss = query_signature
                    .is_some_and(|signature| !page_ref.might_contain_vector_signature(signature));
                Ok((
                    lower_bound,
                    signature_miss,
                    page_ref.page_ordinal,
                    page_ref.clone(),
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        ranked_pages.sort_by(|left, right| {
            left.0
                .total_cmp(&right.0)
                .then_with(|| left.1.cmp(&right.1))
                .then_with(|| left.2.cmp(&right.2))
        });
        if page_refs
            .first()
            .is_some_and(|page_ref| page_ref.routing_level == 0)
        {
            let target_leaf_segments = (*max_segments).max(1);
            let target_page_overfetch = routing_page_overfetch(&options.mode);
            let mut selected_leaf_segments = ranked_pages[0].3.leaf_segments.max(1);
            let target_overfetch_leaf_segments =
                target_leaf_segments.saturating_mul(target_page_overfetch);
            let cutoff = ranked_pages[0].0;
            let cutoff_margin = routing_lower_bound_overfetch_margin(query, ranked_pages.len());
            let mut pages_to_read = 1_usize;
            while pages_to_read < ranked_pages.len()
                && (pages_to_read < target_page_overfetch
                    || selected_leaf_segments < target_overfetch_leaf_segments)
                && ranked_pages[pages_to_read].0 <= cutoff + cutoff_margin
            {
                selected_leaf_segments = selected_leaf_segments
                    .saturating_add(ranked_pages[pages_to_read].3.leaf_segments.max(1));
                pages_to_read += 1;
            }
            ranked_pages.truncate(pages_to_read);
            ranked_pages.sort_by_key(|(_, _, ordinal, _)| *ordinal);
            return Ok(ranked_pages
                .into_iter()
                .map(|(_, _, _, page_ref)| page_ref)
                .collect());
        }

        let mut selected = Vec::new();
        let mut selected_leaf_segments = 0_usize;
        let mut cutoff = None::<f32>;
        let cutoff_margin = routing_lower_bound_overfetch_margin(query, ranked_pages.len());
        let target_page_overfetch = routing_page_overfetch(&options.mode);
        let target_leaf_segments = max_segments.saturating_mul(target_page_overfetch);
        for (lower_bound, _, ordinal, page_ref) in ranked_pages {
            if let Some(cutoff) = cutoff
                && lower_bound > cutoff + cutoff_margin
            {
                break;
            }
            selected_leaf_segments = selected_leaf_segments.saturating_add(page_ref.leaf_segments);
            selected.push((ordinal, page_ref));
            if *max_segments != usize::MAX && selected_leaf_segments >= *max_segments {
                if cutoff.is_none() {
                    cutoff = Some(lower_bound);
                }
                if selected.len() >= target_page_overfetch
                    && selected_leaf_segments >= target_leaf_segments
                {
                    break;
                }
            }
        }
        selected.sort_by_key(|(ordinal, _)| *ordinal);

        Ok(selected.into_iter().map(|(_, page_ref)| page_ref).collect())
    }

    fn routing_leaf_page_refs_for_search(
        &self,
        query: &[f32],
        options: &SearchOptions,
        page_refs: &[RoutingLayerPageRef],
    ) -> Result<RoutingPageRefsRead> {
        let mut read_result = RoutingPageRefsRead::default();
        let mut current_page_refs =
            self.routing_layer_page_refs_for_search(query, options, page_refs)?;

        loop {
            let Some(first_page_ref) = current_page_refs.first() else {
                return Ok(read_result);
            };
            let routing_level = first_page_ref.routing_level;
            if current_page_refs
                .iter()
                .any(|page_ref| page_ref.routing_level != routing_level)
            {
                return Err(BorsukError::InvalidStorage(
                    "routing page walk found mixed routing levels".to_string(),
                ));
            }
            if routing_level == 0 {
                read_result.page_refs = current_page_refs;
                return Ok(read_result);
            }

            let child_read =
                self.routing_child_page_refs_read_from_parent_refs(&current_page_refs)?;
            read_result.bytes_read += child_read.bytes_read;
            read_result.routing_pages_read += child_read.routing_pages_read;
            read_result.object_cache_hits += child_read.object_cache_hits;
            read_result.object_cache_misses += child_read.object_cache_misses;
            current_page_refs =
                self.routing_layer_page_refs_for_search(query, options, &child_read.page_refs)?;
        }
    }

    fn compaction_source_selection_from_routing_tree(
        &self,
        source_level: u8,
        max_segments: usize,
        page_index_read: RoutingLayerPageIndexRead,
    ) -> Result<CompactionSourceSelectionRead> {
        let mut read_result = CompactionSourceSelectionRead {
            bytes_read: page_index_read.bytes_read,
            routing_page_indexes_read: page_index_read.page_indexes_read,
            object_cache_hits: page_index_read.object_cache_hits,
            object_cache_misses: page_index_read.object_cache_misses,
            ..Default::default()
        };
        let mut pending = page_index_read
            .page_refs
            .into_iter()
            .filter(|page_ref| page_ref.might_contain_level(source_level))
            .collect::<VecDeque<_>>();

        while let Some(page_ref) = pending.pop_front() {
            if read_result.selected.len() >= max_segments {
                break;
            }
            if !page_ref.might_contain_level(source_level) {
                continue;
            }

            if page_ref.routing_level == 0 {
                let page_read =
                    self.routing_summaries_read_from_page_refs(std::slice::from_ref(&page_ref))?;
                read_result.bytes_read += page_read.bytes_read;
                read_result.routing_pages_read += page_read.routing_pages_read;
                read_result.object_cache_hits += page_read.object_cache_hits;
                read_result.object_cache_misses += page_read.object_cache_misses;
                let page_summaries = page_read.summaries;

                let selected_before_page = read_result.selected.len();
                for summary in page_summaries
                    .iter()
                    .filter(|summary| summary.level == source_level)
                {
                    if read_result.selected.len() >= max_segments {
                        break;
                    }
                    read_result.selected.push(summary.clone());
                }

                if read_result.selected.len() > selected_before_page {
                    read_result
                        .dirty_pages
                        .push((page_ref.page_ordinal, page_summaries));
                }
                continue;
            }

            let child_read = self.routing_child_page_refs_read_from_parent_refs_with_cache(
                std::slice::from_ref(&page_ref),
                Some(&mut read_result.decoded_parent_pages),
            )?;
            read_result.bytes_read += child_read.bytes_read;
            read_result.routing_pages_read += child_read.routing_pages_read;
            read_result.object_cache_hits += child_read.object_cache_hits;
            read_result.object_cache_misses += child_read.object_cache_misses;

            let mut children = child_read
                .page_refs
                .into_iter()
                .filter(|page_ref| page_ref.might_contain_level(source_level))
                .collect::<Vec<_>>();
            children.sort_by_key(|page_ref| page_ref.page_ordinal);
            for child in children.into_iter().rev() {
                pending.push_front(child);
            }
        }

        read_result
            .dirty_pages
            .sort_by_key(|(page_ordinal, _)| *page_ordinal);

        Ok(read_result)
    }

    fn routing_leaf_page_refs_for_filter<F>(
        &self,
        page_refs: &[RoutingLayerPageRef],
        page_filter: F,
    ) -> Result<Vec<RoutingLayerPageRef>>
    where
        F: FnMut(&RoutingLayerPageRef) -> bool,
    {
        Ok(self
            .routing_leaf_page_refs_for_filter_read(page_refs, page_filter)?
            .page_refs)
    }

    fn routing_leaf_page_refs_for_filter_read<F>(
        &self,
        page_refs: &[RoutingLayerPageRef],
        mut page_filter: F,
    ) -> Result<RoutingPageRefsRead>
    where
        F: FnMut(&RoutingLayerPageRef) -> bool,
    {
        let mut current_page_refs = page_refs
            .iter()
            .filter(|page_ref| page_filter(page_ref))
            .cloned()
            .collect::<Vec<_>>();
        let mut read_result = RoutingPageRefsRead::default();

        loop {
            let Some(first_page_ref) = current_page_refs.first() else {
                return Ok(read_result);
            };
            let routing_level = first_page_ref.routing_level;
            if current_page_refs
                .iter()
                .any(|page_ref| page_ref.routing_level != routing_level)
            {
                return Err(BorsukError::InvalidStorage(
                    "routing page filter found mixed routing levels".to_string(),
                ));
            }
            if routing_level == 0 {
                read_result.page_refs = current_page_refs;
                return Ok(read_result);
            }

            let child_read =
                self.routing_child_page_refs_read_from_parent_refs(&current_page_refs)?;
            read_result.bytes_read += child_read.bytes_read;
            read_result.routing_pages_read += child_read.routing_pages_read;
            read_result.object_cache_hits += child_read.object_cache_hits;
            read_result.object_cache_misses += child_read.object_cache_misses;
            current_page_refs = child_read
                .page_refs
                .into_iter()
                .filter(|page_ref| page_filter(page_ref))
                .collect();
        }
    }

    fn routing_child_page_refs_read_from_parent_refs(
        &self,
        parent_refs: &[RoutingLayerPageRef],
    ) -> Result<RoutingPageRefsRead> {
        self.routing_child_page_refs_read_from_parent_refs_with_cache(parent_refs, None)
    }

    fn routing_child_page_refs_read_from_parent_refs_with_cache(
        &self,
        parent_refs: &[RoutingLayerPageRef],
        mut decoded_parent_pages: Option<&mut HashMap<String, Vec<RoutingLayerPageRef>>>,
    ) -> Result<RoutingPageRefsRead> {
        let expected_page_refs = parent_refs
            .iter()
            .map(|page_ref| page_ref.page_segments)
            .sum::<usize>();
        let mut read_result = RoutingPageRefsRead {
            page_refs: Vec::with_capacity(expected_page_refs),
            ..Default::default()
        };

        for parent_ref in parent_refs {
            if let Some(cache) = decoded_parent_pages.as_deref_mut()
                && let Some(cached_page_refs) = cache.get(&parent_ref.path)
            {
                if cached_page_refs.len() != parent_ref.page_segments {
                    return Err(BorsukError::InvalidStorage(format!(
                        "cached routing parent page `{}` yielded {} child page refs, expected {}",
                        parent_ref.path,
                        cached_page_refs.len(),
                        parent_ref.page_segments
                    )));
                }
                read_result
                    .page_refs
                    .extend(cached_page_refs.iter().cloned());
                continue;
            }

            let child_routing_level = parent_ref.routing_level.checked_sub(1).ok_or_else(|| {
                BorsukError::InvalidStorage(
                    "routing parent page read requested for L0 page".to_string(),
                )
            })?;
            let read = self
                .storage
                .read_bytes_with_cache_status_and_checksum(&parent_ref.path, &parent_ref.checksum)
                .map_err(|err| {
                    BorsukError::InvalidStorage(format!(
                        "routing parent page `{}` could not be read: {err}",
                        parent_ref.path
                    ))
                })?;
            read_result.bytes_read += read.bytes.len() as u64;
            read_result.routing_pages_read += 1;
            count_cache_read(
                read.cache_hit,
                &mut read_result.object_cache_hits,
                &mut read_result.object_cache_misses,
            );
            let mut child_page_refs =
                routing_layer_page_index_from_parquet_relaxed_manifest_version(
                    &read.bytes,
                    self.manifest.version,
                    child_routing_level,
                )
                .map_err(|err| {
                    BorsukError::InvalidStorage(format!(
                        "routing parent page `{}` could not be decoded: {err}",
                        parent_ref.path
                    ))
                })?;
            if child_page_refs.len() != parent_ref.page_segments {
                return Err(BorsukError::InvalidStorage(format!(
                    "routing parent page `{}` yielded {} child page refs, expected {}",
                    parent_ref.path,
                    child_page_refs.len(),
                    parent_ref.page_segments
                )));
            }
            if let Some(cache) = decoded_parent_pages.as_deref_mut() {
                cache.insert(parent_ref.path.clone(), child_page_refs.clone());
            }
            read_result.page_refs.append(&mut child_page_refs);
        }

        if read_result.page_refs.len() != expected_page_refs {
            return Err(BorsukError::InvalidStorage(format!(
                "routing parent pages yielded {} child page refs, expected {}",
                read_result.page_refs.len(),
                expected_page_refs
            )));
        }
        read_result
            .page_refs
            .sort_by_key(|page_ref| page_ref.page_ordinal);

        Ok(read_result)
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
                .read_bytes_with_cache_status_and_checksum(&page_ref.path, &page_ref.checksum)
                .map_err(|err| {
                    BorsukError::InvalidStorage(format!(
                        "routing layer page `{}` could not be read: {err}",
                        page_ref.path
                    ))
                })?;
            read_result.bytes_read += read.bytes.len() as u64;
            read_result.routing_pages_read += 1;
            count_cache_read(
                read.cache_hit,
                &mut read_result.object_cache_hits,
                &mut read_result.object_cache_misses,
            );
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
        let page_count = self
            .manifest
            .segments
            .len()
            .div_ceil(self.manifest.routing_page_fanout);
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
        let id_bloom = segment_id_bloom(segment.records.iter().map(|record| record.id.as_bytes()));
        let vector_signature_bloom = segment_vector_signature_bloom(
            segment
                .records
                .iter()
                .map(|record| record.vector.as_slice()),
        );
        let (bounds_min, bounds_max) = vector_bounds(&segment.records, segment.dimensions)?;

        Ok(SegmentSummary {
            id: segment.id,
            level: segment.level,
            path,
            object_count: segment.records.len(),
            dimensions: segment.dimensions,
            centroid: segment.centroid,
            radius: segment.radius,
            bounds_min,
            bounds_max,
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
        let read = self
            .storage
            .read_bytes_with_cache_status_and_checksum(&summary.path, &summary.checksum)?;
        let bytes_read = read.bytes.len() as u64;
        validate_object_size("segment", &summary.path, summary.size_bytes, bytes_read)?;

        let segment = segment_from_parquet(&read.bytes)?;
        validate_segment_metadata(summary, &segment, &self.manifest.config.metric)?;

        Ok((segment, bytes_read, read.cache_hit))
    }

    fn read_graph(
        &self,
        summary: &SegmentSummary,
        segment: &Segment,
    ) -> Result<(SegmentGraph, u64, bool)> {
        let read = self.storage.read_bytes_with_cache_status_and_checksum(
            &summary.graph_path,
            &summary.graph_checksum,
        )?;
        let bytes_read = read.bytes.len() as u64;
        validate_object_size(
            "graph",
            &summary.graph_path,
            summary.graph_size_bytes,
            bytes_read,
        )?;

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
        if let Some(id) = record
            .id
            .try_as_str()
            .ok()
            .and_then(|id| id.parse::<u64>().ok())
        {
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

fn output_segment_chunk_size(
    record_count: usize,
    target_segment_max_vectors: usize,
    min_output_segments: usize,
) -> usize {
    let min_output_segments = min_output_segments.max(1).min(record_count.max(1));
    record_count
        .div_ceil(min_output_segments)
        .min(target_segment_max_vectors)
        .max(1)
}

fn split_summaries_for_routing_pages(
    summaries: Vec<SegmentSummary>,
    min_pages: usize,
    routing_page_fanout: usize,
) -> Vec<Vec<SegmentSummary>> {
    if summaries.is_empty() {
        return Vec::new();
    }

    let min_pages = min_pages.max(1).min(summaries.len());
    let mut pages = Vec::new();
    let mut start = 0_usize;

    for page_index in 0..min_pages {
        let remaining = summaries.len() - start;
        let remaining_pages = min_pages - page_index;
        let reserved_for_later_pages = remaining_pages - 1;
        let page_len = (remaining - reserved_for_later_pages).clamp(1, routing_page_fanout);
        pages.push(summaries[start..start + page_len].to_vec());
        start += page_len;
    }

    while start < summaries.len() {
        let page_len = (summaries.len() - start).min(routing_page_fanout);
        pages.push(summaries[start..start + page_len].to_vec());
        start += page_len;
    }

    pages
}

fn routing_page_tree_content_page_count(segment_count: usize, routing_page_fanout: usize) -> usize {
    if segment_count == 0 {
        return 0;
    }

    let mut page_count = segment_count.div_ceil(routing_page_fanout);
    let mut total = 0_usize;
    loop {
        total += page_count;
        if page_count <= 1 {
            return total;
        }
        page_count = page_count.div_ceil(routing_page_fanout);
    }
}

fn routing_leaf_page_count(segment_count: usize, routing_page_fanout: usize) -> usize {
    if segment_count == 0 {
        0
    } else {
        segment_count.div_ceil(routing_page_fanout)
    }
}

fn leaf_page_occupied_ranges_from_cached_tree(
    top_page_refs: &[RoutingLayerPageRef],
    decoded_parent_pages: &HashMap<String, Vec<RoutingLayerPageRef>>,
    routing_page_fanout: usize,
) -> Result<Vec<Range<usize>>> {
    let mut ranges = Vec::new();
    for page_ref in top_page_refs {
        reserve_leaf_page_range(
            page_ref,
            decoded_parent_pages,
            routing_page_fanout,
            &mut ranges,
        )?;
    }
    Ok(ranges)
}

fn reserve_leaf_page_range(
    page_ref: &RoutingLayerPageRef,
    decoded_parent_pages: &HashMap<String, Vec<RoutingLayerPageRef>>,
    routing_page_fanout: usize,
    ranges: &mut Vec<Range<usize>>,
) -> Result<()> {
    if page_ref.routing_level == 0 {
        let end = page_ref.page_ordinal.checked_add(1).ok_or_else(|| {
            BorsukError::InvalidStorage("routing leaf page ordinal overflow".to_string())
        })?;
        ranges.push(page_ref.page_ordinal..end);
        return Ok(());
    }

    if let Some(child_refs) = decoded_parent_pages.get(&page_ref.path) {
        for child_ref in child_refs {
            reserve_leaf_page_range(child_ref, decoded_parent_pages, routing_page_fanout, ranges)?;
        }
        return Ok(());
    }

    let span =
        routing_leaf_page_span(page_ref.routing_level, routing_page_fanout).ok_or_else(|| {
            BorsukError::InvalidStorage("routing leaf page span overflow".to_string())
        })?;
    let start = page_ref.page_ordinal.checked_mul(span).ok_or_else(|| {
        BorsukError::InvalidStorage("routing leaf page range overflow".to_string())
    })?;
    let end = start.checked_add(span).ok_or_else(|| {
        BorsukError::InvalidStorage("routing leaf page range overflow".to_string())
    })?;
    ranges.push(start..end);
    Ok(())
}

fn next_available_leaf_page_ordinal(
    cursor: &mut usize,
    occupied_ranges: &mut Vec<Range<usize>>,
) -> Result<usize> {
    loop {
        let mut advanced = false;
        for range in occupied_ranges.iter() {
            if range.contains(cursor) {
                *cursor = range.end;
                advanced = true;
                break;
            }
        }
        if advanced {
            continue;
        }

        let ordinal = *cursor;
        let end = ordinal.checked_add(1).ok_or_else(|| {
            BorsukError::InvalidStorage("routing leaf page ordinal overflow".to_string())
        })?;
        occupied_ranges.push(ordinal..end);
        *cursor = end;
        return Ok(ordinal);
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

    if let Some(max_segments) = options.max_segments
        && options.min_segments > max_segments
    {
        return Err(BorsukError::InvalidCompactionInput(
            "min_segments must be less than or equal to max_segments when max_segments is set"
                .to_string(),
        ));
    }

    if options.target_segment_max_vectors == Some(0) {
        return Err(BorsukError::InvalidCompactionInput(
            "target_segment_max_vectors must be greater than zero when set".to_string(),
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
        routing_page_overfetch,
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

    if *routing_page_overfetch == Some(0) {
        return Err(BorsukError::InvalidSearchOptions(
            "routing_page_overfetch must be greater than zero when set".to_string(),
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

fn should_expand_segment_graph(
    mode: &SearchMode,
    k: usize,
    stored_leaf_mode: LeafMode,
    segment_len: usize,
) -> bool {
    let SearchMode::Approx {
        leaf_mode,
        max_candidates_per_segment: Some(max_candidates_per_segment),
        ..
    } = mode
    else {
        return false;
    };
    if (*max_candidates_per_segment).min(segment_len) <= k.max(1) {
        return false;
    }

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

fn routing_page_overfetch(mode: &SearchMode) -> usize {
    match mode {
        SearchMode::Exact => ROUTING_SEARCH_PAGE_OVERFETCH,
        SearchMode::Approx {
            routing_page_overfetch,
            ..
        } => routing_page_overfetch.unwrap_or(ROUTING_SEARCH_PAGE_OVERFETCH),
    }
}

fn leaf_page_ref_updates_by_ordinal(
    page_refs: &[RoutingLayerPageRef],
) -> Result<HashMap<usize, RoutingLayerPageRef>> {
    let mut updates = HashMap::with_capacity(page_refs.len());
    for page_ref in page_refs {
        if page_ref.routing_level != 0 {
            return Err(BorsukError::InvalidStorage(format!(
                "routing leaf update must be an L0 page ref, got L{}",
                page_ref.routing_level
            )));
        }
        if updates
            .insert(page_ref.page_ordinal, page_ref.clone())
            .is_some()
        {
            return Err(BorsukError::InvalidStorage(format!(
                "duplicate routing leaf update for page {}",
                page_ref.page_ordinal
            )));
        }
    }
    Ok(updates)
}

fn upsert_leaf_page_ref_by_ordinal(
    page_refs: &mut Vec<RoutingLayerPageRef>,
    page_ref: RoutingLayerPageRef,
) -> Result<()> {
    if page_ref.routing_level != 0 {
        return Err(BorsukError::InvalidStorage(format!(
            "routing leaf update must be an L0 page ref, got L{}",
            page_ref.routing_level
        )));
    }
    page_refs.retain(|existing| existing.page_ordinal != page_ref.page_ordinal);
    page_refs.push(page_ref);
    page_refs.sort_by_key(|page_ref| page_ref.page_ordinal);
    Ok(())
}

fn routing_page_refs_by_parent_ordinal(
    page_refs: &[RoutingLayerPageRef],
    routing_page_fanout: usize,
) -> BTreeMap<usize, Vec<RoutingLayerPageRef>> {
    let mut grouped = BTreeMap::<usize, Vec<RoutingLayerPageRef>>::new();
    for page_ref in page_refs {
        grouped
            .entry(page_ref.page_ordinal / routing_page_fanout)
            .or_default()
            .push(page_ref.clone());
    }
    for refs in grouped.values_mut() {
        refs.sort_by_key(|page_ref| page_ref.page_ordinal);
    }
    grouped
}

fn leaf_page_ref_updates_by_parent_ordinal<'a>(
    routing_level: u8,
    page_refs: impl IntoIterator<Item = &'a RoutingLayerPageRef>,
    routing_page_fanout: usize,
) -> Result<BTreeMap<usize, Vec<RoutingLayerPageRef>>> {
    let mut grouped = BTreeMap::<usize, Vec<RoutingLayerPageRef>>::new();
    for page_ref in page_refs {
        if page_ref.routing_level != 0 {
            return Err(BorsukError::InvalidStorage(format!(
                "routing leaf update must be an L0 page ref, got L{}",
                page_ref.routing_level
            )));
        }
        grouped
            .entry(routing_parent_ordinal_for_leaf(
                routing_level,
                page_ref.page_ordinal,
                routing_page_fanout,
            )?)
            .or_default()
            .push(page_ref.clone());
    }
    for updates in grouped.values_mut() {
        updates.sort_by_key(|page_ref| page_ref.page_ordinal);
    }
    Ok(grouped)
}

fn routing_subtree_contains_leaf_update(
    page_ref: &RoutingLayerPageRef,
    updates: &HashMap<usize, RoutingLayerPageRef>,
    routing_page_fanout: usize,
) -> bool {
    updates.keys().any(|leaf_ordinal| {
        routing_subtree_contains_leaf_ordinal(page_ref, *leaf_ordinal, routing_page_fanout)
    })
}

fn routing_subtree_contains_leaf_ordinal(
    page_ref: &RoutingLayerPageRef,
    leaf_ordinal: usize,
    routing_page_fanout: usize,
) -> bool {
    let Some(span) = routing_leaf_page_span(page_ref.routing_level, routing_page_fanout) else {
        return true;
    };
    let Some(start) = page_ref.page_ordinal.checked_mul(span) else {
        return true;
    };
    let end = start.saturating_add(span);
    leaf_ordinal >= start && leaf_ordinal < end
}

fn routing_parent_ordinal_for_leaf(
    routing_level: u8,
    leaf_page_ordinal: usize,
    routing_page_fanout: usize,
) -> Result<usize> {
    let Some(span) = routing_leaf_page_span(routing_level, routing_page_fanout) else {
        return Err(BorsukError::InvalidStorage(
            "routing leaf page span overflow".to_string(),
        ));
    };
    Ok(leaf_page_ordinal / span)
}

fn routing_leaf_page_span(routing_level: u8, routing_page_fanout: usize) -> Option<usize> {
    let mut span = 1_usize;
    for _ in 0..routing_level {
        span = span.checked_mul(routing_page_fanout)?;
    }
    Some(span)
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

fn push_hit_with_vector(
    hits: &mut Vec<SearchHitWithVector>,
    hit: SearchHit,
    vector: Option<Vec<f32>>,
    k: usize,
) {
    hits.push(SearchHitWithVector { hit, vector });
    hits.sort_by(|left, right| {
        left.hit
            .distance
            .partial_cmp(&right.hit.distance)
            .unwrap_or(Ordering::Equal)
            .then_with(|| left.hit.id.cmp(&right.hit.id))
    });
    hits.truncate(k);
}

fn search_stop_reason_before_segment(
    hits: &[SearchHitWithVector],
    k: usize,
    mode: &SearchMode,
    searched_segments: usize,
    bytes_read: u64,
    lower_bound: f32,
    elapsed_ms: u64,
) -> Option<SearchTerminationReason> {
    match mode {
        SearchMode::Exact => hits
            .get(k.saturating_sub(1))
            .is_some_and(|best_k| lower_bound > best_k.hit.distance)
            .then_some(SearchTerminationReason::ExactPruned),
        SearchMode::Approx {
            leaf_mode: _,
            eps,
            max_segments,
            max_bytes,
            max_latency_ms,
            routing_page_overfetch: _,
            max_candidates_per_segment: _,
        } => {
            if max_segments.is_some_and(|limit| searched_segments >= limit) {
                return Some(SearchTerminationReason::MaxSegments);
            }

            if max_bytes.is_some_and(|limit| bytes_read >= limit) {
                return Some(SearchTerminationReason::MaxBytes);
            }

            if max_latency_ms.is_some_and(|limit| elapsed_ms >= limit) {
                return Some(SearchTerminationReason::MaxLatency);
            }

            if let (Some(eps), Some(best_k)) = (eps, hits.get(k.saturating_sub(1))) {
                return (lower_bound >= best_k.hit.distance / (1.0 + eps))
                    .then_some(SearchTerminationReason::Epsilon);
            }

            None
        }
    }
}

fn routing_lower_bound_overfetch_margin(query: &[f32], ranked_page_count: usize) -> f32 {
    if ranked_page_count <= ROUTING_SEARCH_PAGE_OVERFETCH * 2 {
        return 1.0e-6;
    }

    query
        .iter()
        .map(|value| value.abs())
        .fold(1.0_f32, f32::max)
}

#[cfg(test)]
mod tests {
    use chrono::Utc;

    use super::*;

    #[test]
    fn l0_page_routing_uses_leaf_segment_counts_for_sparse_pages() {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_string_lossy().into_owned();
        let index = BorsukIndex::create_with_routing_page_fanout(
            IndexConfig {
                uri,
                metric: VectorMetric::Euclidean,
                dimensions: 2,
                segment_max_vectors: 1,
                ram_budget_bytes: None,
            },
            8,
        )
        .unwrap();
        let page_refs = (0..5)
            .map(|ordinal| {
                let centroid = if ordinal < 3 {
                    vec![0.0, 0.0]
                } else {
                    vec![100.0 + ordinal as f32, 0.0]
                };
                fake_l0_page_ref(ordinal, centroid, 1)
            })
            .collect::<Vec<_>>();

        let selected = index
            .routing_layer_page_refs_for_search(
                &[0.0, 0.0],
                &SearchOptions::approx(3, LeafMode::PqScan).with_max_segments(3),
                &page_refs,
            )
            .unwrap();

        assert_eq!(
            selected
                .iter()
                .map(|page_ref| page_ref.page_ordinal)
                .collect::<Vec<_>>(),
            vec![0, 1, 2]
        );
        assert_eq!(
            selected
                .iter()
                .map(|page_ref| page_ref.leaf_segments)
                .sum::<usize>(),
            3
        );
    }

    #[test]
    fn l0_page_routing_overfetch_is_search_option() {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_string_lossy().into_owned();
        let index = BorsukIndex::create_with_routing_page_fanout(
            IndexConfig {
                uri,
                metric: VectorMetric::Euclidean,
                dimensions: 2,
                segment_max_vectors: 1,
                ram_budget_bytes: None,
            },
            8,
        )
        .unwrap();
        let page_refs = (0..8)
            .map(|ordinal| fake_l0_page_ref(ordinal, vec![0.0, 0.0], 1))
            .collect::<Vec<_>>();

        let selected = index
            .routing_layer_page_refs_for_search(
                &[0.0, 0.0],
                &SearchOptions::approx(1, LeafMode::PqScan)
                    .with_max_segments(1)
                    .with_routing_page_overfetch(2),
                &page_refs,
            )
            .unwrap();

        assert_eq!(
            selected
                .iter()
                .map(|page_ref| page_ref.page_ordinal)
                .collect::<Vec<_>>(),
            vec![0, 1]
        );
    }

    #[test]
    fn l0_page_routing_overfetch_reads_sibling_pages_when_first_page_is_dense() {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_string_lossy().into_owned();
        let index = BorsukIndex::create_with_routing_page_fanout(
            IndexConfig {
                uri,
                metric: VectorMetric::Euclidean,
                dimensions: 2,
                segment_max_vectors: 1,
                ram_budget_bytes: None,
            },
            8,
        )
        .unwrap();
        let page_refs = (0..4)
            .map(|ordinal| fake_l0_page_ref(ordinal, vec![0.0, 0.0], 4))
            .collect::<Vec<_>>();

        let selected = index
            .routing_layer_page_refs_for_search(
                &[0.0, 0.0],
                &SearchOptions::approx(2, LeafMode::PqScan)
                    .with_max_segments(2)
                    .with_routing_page_overfetch(2),
                &page_refs,
            )
            .unwrap();

        assert_eq!(
            selected
                .iter()
                .map(|page_ref| page_ref.page_ordinal)
                .collect::<Vec<_>>(),
            vec![0, 1],
            "routing overfetch should decode sibling L0 metadata pages even when one dense page already covers the segment-count target"
        );
    }

    #[test]
    fn parent_page_routing_overfetch_reads_sibling_branches_when_first_branch_is_dense() {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_string_lossy().into_owned();
        let index = BorsukIndex::create_with_routing_page_fanout(
            IndexConfig {
                uri,
                metric: VectorMetric::Euclidean,
                dimensions: 2,
                segment_max_vectors: 1,
                ram_budget_bytes: None,
            },
            8,
        )
        .unwrap();
        let page_refs = (0..4)
            .map(|ordinal| {
                let mut page_ref = fake_l0_page_ref(ordinal, vec![0.0, 0.0], 4);
                page_ref.routing_level = 1;
                page_ref.path = format!("routing/pages/L1/fake-{ordinal}.parquet");
                page_ref
            })
            .collect::<Vec<_>>();

        let selected = index
            .routing_layer_page_refs_for_search(
                &[0.0, 0.0],
                &SearchOptions::approx(2, LeafMode::PqScan)
                    .with_max_segments(2)
                    .with_routing_page_overfetch(2),
                &page_refs,
            )
            .unwrap();

        assert_eq!(
            selected
                .iter()
                .map(|page_ref| page_ref.page_ordinal)
                .collect::<Vec<_>>(),
            vec![0, 1],
            "routing overfetch should keep sibling parent branches eligible even when one dense branch already covers the segment-count target"
        );
    }

    #[test]
    fn compact_overflow_does_not_read_unrelated_parent_routing_branches() {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_string_lossy().into_owned();
        let mut index = BorsukIndex::create(IndexConfig {
            uri,
            metric: VectorMetric::Euclidean,
            dimensions: 2,
            segment_max_vectors: 1,
            ram_budget_bytes: None,
        })
        .unwrap();

        let selected_segment = Segment::from_records(
            "selected".to_string(),
            1,
            VectorMetric::Euclidean,
            2,
            vec![
                VectorRecord::new("selected-a", vec![0.0, 0.0]),
                VectorRecord::new("selected-b", vec![1.0, 0.0]),
            ],
        )
        .unwrap();
        let selected_summary = index.write_segment(selected_segment).unwrap();

        let mut manifest = index.manifest.next_version();
        manifest.segments.clear();
        manifest.pivots.clear();
        manifest.routing_max_level = 2;

        let mut dirty_summaries = Vec::with_capacity(DEFAULT_ROUTING_PAGE_FANOUT);
        dirty_summaries.push(selected_summary);
        dirty_summaries.extend(
            (1..DEFAULT_ROUTING_PAGE_FANOUT)
                .map(|ordinal| fake_segment_summary(format!("dirty-{ordinal}"), 1, ordinal)),
        );

        let dirty_leaf = index
            .storage
            .write_routing_layer_page(&manifest, 0, 0, &dirty_summaries)
            .unwrap();
        let unrelated_middle_leaf = index
            .storage
            .write_routing_layer_page(
                &manifest,
                0,
                DEFAULT_ROUTING_PAGE_FANOUT,
                &[fake_segment_summary(
                    "middle",
                    0,
                    DEFAULT_ROUTING_PAGE_FANOUT,
                )],
            )
            .unwrap();
        let append_parent_leaf = index
            .storage
            .write_routing_layer_page(
                &manifest,
                0,
                DEFAULT_ROUTING_PAGE_FANOUT * 2,
                &[fake_segment_summary(
                    "append",
                    0,
                    DEFAULT_ROUTING_PAGE_FANOUT * 2,
                )],
            )
            .unwrap();

        let l1_dirty = index
            .storage
            .write_parent_routing_layer_page(&manifest, 1, 0, &[dirty_leaf])
            .unwrap();
        let l1_middle = index
            .storage
            .write_parent_routing_layer_page(&manifest, 1, 1, &[unrelated_middle_leaf])
            .unwrap();
        let l1_append = index
            .storage
            .write_parent_routing_layer_page(&manifest, 1, 2, &[append_parent_leaf])
            .unwrap();
        let l2_root = index
            .storage
            .write_parent_routing_layer_page(&manifest, 2, 0, &[l1_dirty, l1_middle, l1_append])
            .unwrap();

        index.manifest = index
            .storage
            .publish_manifest_with_top_routing_page_refs(&manifest, 2, &[l2_root])
            .unwrap();
        let top_page_paths = index
            .storage
            .read_routing_layer_page_index(index.manifest.version, 2)
            .unwrap();
        let root_children = index
            .routing_child_page_refs_read_from_parent_refs(&top_page_paths)
            .unwrap();
        let middle_path = root_children.page_refs[1].path.clone();
        let append_path = root_children.page_refs[2].path.clone();
        index
            .storage
            .write_bytes(&middle_path, b"corrupt unrelated parent routing page")
            .unwrap();
        index
            .storage
            .write_bytes(&append_path, b"corrupt append parent routing page")
            .unwrap();

        let compaction = index
            .compact(CompactionOptions {
                source_level: 1,
                target_level: 2,
                max_segments: Some(1),
                min_segments: 1,
                target_segment_max_vectors: Some(1),
            })
            .unwrap();

        assert!(compaction.compacted);
        assert_eq!(compaction.segments_read, 1);
        assert_eq!(compaction.segments_written, 2);
        assert_eq!(compaction.records_rewritten, 2);
        assert_eq!(compaction.routing_page_indexes_read, 1);
        assert_eq!(
            compaction.routing_pages_read, 3,
            "overflow compaction should read only the selected root, parent, and leaf pages"
        );
        assert_eq!(compaction.routing_page_indexes_written, 1);
        assert_eq!(
            compaction.routing_pages_written, 4,
            "overflow compaction should write two leaf pages and the two dirty parent pages"
        );
        assert_eq!(compaction.graph_payloads_read, 0);
        assert_eq!(compaction.graph_bytes_read, 0);
        assert!(index.manifest.segments.is_empty());
    }

    #[test]
    fn compact_max_segments_does_not_read_unneeded_source_parent_branches() {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_string_lossy().into_owned();
        let mut index = BorsukIndex::create(IndexConfig {
            uri,
            metric: VectorMetric::Euclidean,
            dimensions: 2,
            segment_max_vectors: 1,
            ram_budget_bytes: None,
        })
        .unwrap();

        let selected_segment = Segment::from_records(
            "selected".to_string(),
            1,
            VectorMetric::Euclidean,
            2,
            vec![VectorRecord::new("selected", vec![0.0, 0.0])],
        )
        .unwrap();
        let selected_summary = index.write_segment(selected_segment).unwrap();

        let unneeded_segment = Segment::from_records(
            "unneeded".to_string(),
            1,
            VectorMetric::Euclidean,
            2,
            vec![VectorRecord::new("unneeded", vec![1000.0, 0.0])],
        )
        .unwrap();
        let unneeded_summary = index.write_segment(unneeded_segment).unwrap();

        let mut manifest = index.manifest.next_version();
        manifest.segments.clear();
        manifest.pivots.clear();
        manifest.routing_max_level = 2;

        let dirty_leaf = index
            .storage
            .write_routing_layer_page(&manifest, 0, 0, &[selected_summary])
            .unwrap();
        let unneeded_leaf = index
            .storage
            .write_routing_layer_page(
                &manifest,
                0,
                DEFAULT_ROUTING_PAGE_FANOUT,
                &[unneeded_summary],
            )
            .unwrap();

        let l1_dirty = index
            .storage
            .write_parent_routing_layer_page(&manifest, 1, 0, &[dirty_leaf])
            .unwrap();
        let l1_unneeded = index
            .storage
            .write_parent_routing_layer_page(&manifest, 1, 1, &[unneeded_leaf])
            .unwrap();
        let l2_root = index
            .storage
            .write_parent_routing_layer_page(&manifest, 2, 0, &[l1_dirty, l1_unneeded])
            .unwrap();

        index.manifest = index
            .storage
            .publish_manifest_with_top_routing_page_refs(&manifest, 2, &[l2_root])
            .unwrap();
        let top_page_paths = index
            .storage
            .read_routing_layer_page_index(index.manifest.version, 2)
            .unwrap();
        let root_children = index
            .routing_child_page_refs_read_from_parent_refs(&top_page_paths)
            .unwrap();
        let unneeded_parent_path = root_children.page_refs[1].path.clone();
        index
            .storage
            .write_bytes(
                &unneeded_parent_path,
                b"corrupt unneeded source-level parent branch",
            )
            .unwrap();

        let compaction = index
            .compact(CompactionOptions {
                source_level: 1,
                target_level: 2,
                max_segments: Some(1),
                min_segments: 1,
                target_segment_max_vectors: Some(1),
            })
            .unwrap();

        assert!(compaction.compacted);
        assert_eq!(compaction.segments_read, 1);
        assert_eq!(compaction.records_rewritten, 1);
        assert_eq!(index.get_vector("selected").unwrap(), Some(vec![0.0, 0.0]));
    }

    #[test]
    fn compact_stops_parent_branch_reads_once_source_batch_is_covered() {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_string_lossy().into_owned();
        let mut index = BorsukIndex::create(IndexConfig {
            uri,
            metric: VectorMetric::Euclidean,
            dimensions: 2,
            segment_max_vectors: 1,
            ram_budget_bytes: None,
        })
        .unwrap();

        let selected_summaries = (0..32)
            .map(|ordinal| {
                let segment = Segment::from_records(
                    format!("selected-{ordinal}"),
                    1,
                    VectorMetric::Euclidean,
                    2,
                    vec![VectorRecord::new(
                        format!("selected-{ordinal}"),
                        vec![ordinal as f32, 0.0],
                    )],
                )
                .unwrap();
                index.write_segment(segment).unwrap()
            })
            .collect::<Vec<_>>();

        let mut manifest = index.manifest.next_version();
        manifest.segments.clear();
        manifest.pivots.clear();
        manifest.routing_max_level = 2;

        let selected_leaf = index
            .storage
            .write_routing_layer_page(&manifest, 0, 0, &selected_summaries)
            .unwrap();
        let unneeded_leaf = index
            .storage
            .write_routing_layer_page(
                &manifest,
                0,
                DEFAULT_ROUTING_PAGE_FANOUT,
                &[fake_segment_summary(
                    "unneeded",
                    1,
                    DEFAULT_ROUTING_PAGE_FANOUT,
                )],
            )
            .unwrap();
        let l1_selected = index
            .storage
            .write_parent_routing_layer_page(&manifest, 1, 0, &[selected_leaf])
            .unwrap();
        let l1_unneeded = index
            .storage
            .write_parent_routing_layer_page(&manifest, 1, 1, &[unneeded_leaf])
            .unwrap();
        let l2_root = index
            .storage
            .write_parent_routing_layer_page(&manifest, 2, 0, &[l1_selected, l1_unneeded])
            .unwrap();

        index.manifest = index
            .storage
            .publish_manifest_with_top_routing_page_refs(&manifest, 2, &[l2_root])
            .unwrap();
        let top_page_paths = index
            .storage
            .read_routing_layer_page_index(index.manifest.version, 2)
            .unwrap();
        let root_children = index
            .routing_child_page_refs_read_from_parent_refs(&top_page_paths)
            .unwrap();
        let unneeded_parent_path = root_children.page_refs[1].path.clone();
        index
            .storage
            .write_bytes(
                &unneeded_parent_path,
                b"corrupt sibling parent branch that compact must not read",
            )
            .unwrap();

        let compaction = index
            .compact(CompactionOptions {
                source_level: 1,
                target_level: 2,
                max_segments: Some(32),
                min_segments: 32,
                target_segment_max_vectors: Some(1),
            })
            .unwrap();

        assert!(compaction.compacted);
        assert_eq!(compaction.segments_read, 32);
        assert_eq!(compaction.records_rewritten, 32);
        assert_eq!(compaction.routing_page_indexes_read, 1);
        assert_eq!(compaction.routing_pages_read, 3);
        assert_eq!(compaction.routing_page_indexes_written, 1);
        assert_eq!(compaction.routing_pages_written, 3);
        assert_eq!(compaction.graph_payloads_read, 0);
        assert_eq!(compaction.graph_bytes_read, 0);
        assert_eq!(
            index.get_vector("selected-31").unwrap(),
            Some(vec![31.0, 0.0])
        );
    }

    #[test]
    fn compact_promotes_oversized_top_routing_index_without_reading_unrelated_parents() {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_string_lossy().into_owned();
        let mut index = BorsukIndex::create(IndexConfig {
            uri,
            metric: VectorMetric::Euclidean,
            dimensions: 2,
            segment_max_vectors: 1,
            ram_budget_bytes: None,
        })
        .unwrap();

        let selected_segment = Segment::from_records(
            "selected".to_string(),
            1,
            VectorMetric::Euclidean,
            2,
            vec![VectorRecord::new("selected", vec![0.0, 0.0])],
        )
        .unwrap();
        let selected_summary = index.write_segment(selected_segment).unwrap();

        let mut manifest = index.manifest.next_version();
        manifest.segments.clear();
        manifest.pivots.clear();
        manifest.routing_max_level = 1;

        let dirty_leaf = index
            .storage
            .write_routing_layer_page(&manifest, 0, 0, &[selected_summary])
            .unwrap();
        let mut top_refs = vec![
            index
                .storage
                .write_parent_routing_layer_page(&manifest, 1, 0, &[dirty_leaf])
                .unwrap(),
        ];

        for ordinal in 1..=DEFAULT_ROUTING_PAGE_FANOUT {
            let leaf_ordinal = ordinal * DEFAULT_ROUTING_PAGE_FANOUT;
            let cold_leaf = index
                .storage
                .write_routing_layer_page(
                    &manifest,
                    0,
                    leaf_ordinal,
                    &[fake_segment_summary(
                        format!("cold-{ordinal}"),
                        0,
                        leaf_ordinal,
                    )],
                )
                .unwrap();
            top_refs.push(
                index
                    .storage
                    .write_parent_routing_layer_page(&manifest, 1, ordinal, &[cold_leaf])
                    .unwrap(),
            );
        }

        let unrelated_parent_path = top_refs[1].path.clone();
        index.manifest = index
            .storage
            .publish_manifest_with_top_routing_page_refs(&manifest, 1, &top_refs)
            .unwrap();
        index
            .storage
            .write_bytes(
                &unrelated_parent_path,
                b"corrupt unrelated parent page that compaction must not read",
            )
            .unwrap();

        let compaction = index
            .compact(CompactionOptions {
                source_level: 1,
                target_level: 2,
                max_segments: Some(1),
                min_segments: 1,
                target_segment_max_vectors: Some(1),
            })
            .unwrap();

        assert!(compaction.compacted);
        assert_eq!(compaction.segments_read, 1);
        assert_eq!(compaction.records_rewritten, 1);
        assert_eq!(compaction.graph_payloads_read, 0);
        assert_eq!(compaction.graph_bytes_read, 0);
        assert_eq!(
            index.manifest.routing_max_level, 2,
            "scoped compaction should add a routing layer once the top page index exceeds fanout"
        );
        let promoted_top_refs = index
            .storage
            .read_routing_layer_page_index(index.manifest.version, 2)
            .unwrap();
        assert_eq!(promoted_top_refs.len(), 2);
        assert_eq!(index.get_vector("selected").unwrap(), Some(vec![0.0, 0.0]));
    }

    #[test]
    fn compact_updates_sparse_top_l0_page_refs_by_ordinal() {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_string_lossy().into_owned();
        let mut index = BorsukIndex::create(IndexConfig {
            uri,
            metric: VectorMetric::Euclidean,
            dimensions: 2,
            segment_max_vectors: 1,
            ram_budget_bytes: None,
        })
        .unwrap();

        let selected_segment = Segment::from_records(
            "selected".to_string(),
            1,
            VectorMetric::Euclidean,
            2,
            vec![VectorRecord::new("selected", vec![0.0, 0.0])],
        )
        .unwrap();
        let selected_summary = index.write_segment(selected_segment).unwrap();

        let mut manifest = index.manifest.next_version();
        manifest.segments.clear();
        manifest.pivots.clear();
        manifest.routing_max_level = 0;
        let sparse_leaf_ordinal = DEFAULT_ROUTING_PAGE_FANOUT;
        let sparse_leaf = index
            .storage
            .write_routing_layer_page(&manifest, 0, sparse_leaf_ordinal, &[selected_summary])
            .unwrap();

        index.manifest = index
            .storage
            .publish_manifest_with_top_routing_page_refs(&manifest, 0, &[sparse_leaf])
            .unwrap();

        let compaction = index
            .compact(CompactionOptions {
                source_level: 1,
                target_level: 2,
                max_segments: Some(1),
                min_segments: 1,
                target_segment_max_vectors: Some(1),
            })
            .unwrap();

        assert!(compaction.compacted);
        assert_eq!(compaction.segments_read, 1);
        assert_eq!(compaction.records_rewritten, 1);
        assert_eq!(compaction.routing_pages_read, 1);
        assert_eq!(compaction.routing_pages_written, 1);
        assert_eq!(compaction.graph_payloads_read, 0);
        assert_eq!(compaction.graph_bytes_read, 0);
        let page_refs = index
            .storage
            .read_routing_layer_page_index(index.manifest.version, 0)
            .unwrap();
        assert_eq!(page_refs.len(), 1);
        assert_eq!(page_refs[0].page_ordinal, sparse_leaf_ordinal);
        assert_eq!(index.get_vector("selected").unwrap(), Some(vec![0.0, 0.0]));
    }

    #[test]
    fn stats_reports_actual_sparse_page_backed_routing_topology() {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_string_lossy().into_owned();
        let mut index = BorsukIndex::create_with_routing_page_fanout(
            IndexConfig {
                uri,
                metric: VectorMetric::Euclidean,
                dimensions: 2,
                segment_max_vectors: 1,
                ram_budget_bytes: None,
            },
            2,
        )
        .unwrap();

        let mut manifest = index.manifest.next_version();
        manifest.segments.clear();
        manifest.pivots.clear();
        manifest.routing_max_level = 1;

        let first_leaf = index
            .storage
            .write_routing_layer_page(&manifest, 0, 0, &[fake_segment_summary("first", 0, 0)])
            .unwrap();
        let sparse_leaf = index
            .storage
            .write_routing_layer_page(
                &manifest,
                0,
                DEFAULT_ROUTING_PAGE_FANOUT,
                &[fake_segment_summary(
                    "sparse",
                    0,
                    DEFAULT_ROUTING_PAGE_FANOUT,
                )],
            )
            .unwrap();
        let first_parent = index
            .storage
            .write_parent_routing_layer_page(&manifest, 1, 0, &[first_leaf])
            .unwrap();
        let sparse_parent = index
            .storage
            .write_parent_routing_layer_page(
                &manifest,
                1,
                DEFAULT_ROUTING_PAGE_FANOUT / 2,
                &[sparse_leaf],
            )
            .unwrap();

        index.manifest = index
            .storage
            .publish_manifest_with_top_routing_page_refs(
                &manifest,
                1,
                &[first_parent, sparse_parent],
            )
            .unwrap();

        let stats = index.try_stats().unwrap();

        assert_eq!(stats.segments, 2);
        assert_eq!(
            stats.routing_leaf_pages, 2,
            "stats should report actual L0 page refs for sparse page-backed routing"
        );
        assert_eq!(
            stats.routing_pages, 4,
            "stats should count the two L0 leaf pages plus the two L1 parent pages"
        );
    }

    #[test]
    fn stats_uses_top_index_page_count_aggregates_without_parent_reads() {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_string_lossy().into_owned();
        let mut index = BorsukIndex::create_with_routing_page_fanout(
            IndexConfig {
                uri,
                metric: VectorMetric::Euclidean,
                dimensions: 2,
                segment_max_vectors: 1,
                ram_budget_bytes: None,
            },
            2,
        )
        .unwrap();

        let mut manifest = index.manifest.next_version();
        manifest.segments.clear();
        manifest.pivots.clear();
        manifest.routing_max_level = 1;

        let first_leaf = index
            .storage
            .write_routing_layer_page(&manifest, 0, 0, &[fake_segment_summary("first", 0, 0)])
            .unwrap();
        let second_leaf = index
            .storage
            .write_routing_layer_page(&manifest, 0, 1, &[fake_segment_summary("second", 0, 1)])
            .unwrap();
        let first_parent = index
            .storage
            .write_parent_routing_layer_page(&manifest, 1, 0, &[first_leaf])
            .unwrap();
        let second_parent = index
            .storage
            .write_parent_routing_layer_page(&manifest, 1, 1, &[second_leaf])
            .unwrap();
        let second_parent_path = second_parent.path.clone();

        index.manifest = index
            .storage
            .publish_manifest_with_top_routing_page_refs(
                &manifest,
                1,
                &[first_parent, second_parent],
            )
            .unwrap();
        index
            .storage
            .write_bytes(
                &second_parent_path,
                b"corrupt parent body stats must not read",
            )
            .unwrap();

        let stats = index.try_stats().unwrap();

        assert_eq!(stats.segments, 2);
        assert_eq!(stats.routing_leaf_pages, 2);
        assert_eq!(stats.routing_pages, 4);
    }

    fn fake_l0_page_ref(
        page_ordinal: usize,
        vector: Vec<f32>,
        leaf_segments: usize,
    ) -> RoutingLayerPageRef {
        RoutingLayerPageRef {
            routing_level: 0,
            page_ordinal,
            path: format!("routing/pages/L0/fake-{page_ordinal}.parquet"),
            checksum: format!("{page_ordinal:064x}"),
            page_segments: leaf_segments,
            leaf_segments,
            leaf_pages: 1,
            routing_pages: 1,
            dimensions: vector.len(),
            centroid: vector.clone(),
            radius: 0.0,
            bounds_min: vector.clone(),
            bounds_max: vector.clone(),
            id_bloom: Vec::new(),
            vector_signature_bloom: segment_vector_signature_bloom([vector.as_slice()]),
            level_mask: u64::MAX,
            page_records: leaf_segments,
            page_segment_bytes: leaf_segments as u64,
            page_graph_bytes: 0,
        }
    }

    fn fake_segment_summary(id: impl Into<String>, level: u8, ordinal: usize) -> SegmentSummary {
        let id = id.into();
        let vector = vec![ordinal as f32, 0.0];
        SegmentSummary {
            id: id.clone(),
            level,
            path: format!("segments/L{level}/fake-{ordinal}.parquet"),
            object_count: 1,
            dimensions: 2,
            centroid: vector.clone(),
            radius: 0.0,
            bounds_min: vector.clone(),
            bounds_max: vector.clone(),
            checksum: format!("{ordinal:064x}"),
            size_bytes: 1,
            graph_path: format!("graphs/L{level}/fake-{ordinal}.parquet"),
            graph_checksum: format!("{:064x}", ordinal + 1),
            graph_size_bytes: 1,
            leaf_mode: LeafMode::FlatScan,
            id_bloom: segment_id_bloom([id.as_str()]),
            vector_signature_bloom: segment_vector_signature_bloom([vector.as_slice()]),
            created_at: Utc::now(),
        }
    }
}
