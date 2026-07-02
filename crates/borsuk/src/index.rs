use std::{
    cmp::Ordering,
    collections::{HashMap, HashSet},
    path::PathBuf,
    time::Instant,
};

use uuid::Uuid;

use crate::{
    error::{BorsukError, Result},
    format::{graph_from_parquet, graph_to_parquet, segment_from_parquet, segment_to_parquet},
    manifest::{Manifest, SegmentSummary},
    metric::VectorMetric,
    record::{
        CompactionOptions, CompactionReport, GarbageCollectionOptions, GarbageCollectionReport,
        SearchHit, SearchMode, SearchOptions, SearchReport, VectorRecord,
    },
    segment::{Segment, SegmentGraph, routing_code},
    storage::Storage,
};

const LOCAL_GRAPH_NEIGHBORS: usize = 8;

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

    /// Add records by writing one or more immutable L0 segments and publishing a new manifest.
    pub fn add(&mut self, records: Vec<VectorRecord>) -> Result<()> {
        if records.is_empty() {
            return Ok(());
        }

        for record in &records {
            self.validate_vector(&record.vector)?;
        }

        let chunks = records.chunks(self.manifest.config.segment_max_vectors);
        let mut manifest = self.manifest.next_version();

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
        self.storage.publish_manifest(&manifest)?;
        self.manifest = manifest;
        Ok(())
    }

    /// Compact immutable segments out-of-place into a higher target level.
    pub fn compact(&mut self, options: CompactionOptions) -> Result<CompactionReport> {
        validate_compaction_options(&options)?;

        let max_segments = options.max_segments.unwrap_or(usize::MAX);
        let selected = self
            .manifest
            .segments
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

        for summary in &selected {
            let (segment, segment_bytes_read, _) = self.read_segment(summary)?;
            bytes_read += segment_bytes_read;
            records.extend(segment.records);
        }

        let selected_ids = selected
            .iter()
            .map(|summary| summary.id.as_str())
            .collect::<HashSet<_>>();
        let mut manifest = self.manifest.next_version();
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
        self.storage.publish_manifest(&manifest)?;
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
            manifest_version: self.manifest.version,
        })
    }

    /// Delete inactive segment objects that are no longer referenced by the active manifest.
    pub fn gc_obsolete_segments(
        &self,
        options: GarbageCollectionOptions,
    ) -> Result<GarbageCollectionReport> {
        let active_paths = self
            .manifest
            .segments
            .iter()
            .flat_map(|summary| [summary.path.as_str(), summary.graph_path.as_str()])
            .collect::<HashSet<_>>();
        let mut objects = self.storage.list_objects("segments")?;
        objects.extend(self.storage.list_objects("graphs")?);
        let objects_scanned = objects.len();
        let candidates = objects
            .into_iter()
            .filter(|object| {
                object.path.ends_with(".parquet") && !active_paths.contains(object.path.as_str())
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

    /// Search the index using exact lower-bound pruning or approximate budgeted traversal.
    pub fn search(&self, query: &[f32], options: SearchOptions) -> Result<Vec<SearchHit>> {
        Ok(self.search_with_report(query, options)?.hits)
    }

    /// Search multiple queries with the same options, preserving query order in the results.
    pub fn search_batch(
        &self,
        queries: &[Vec<f32>],
        options: SearchOptions,
    ) -> Result<Vec<Vec<SearchHit>>> {
        queries
            .iter()
            .map(|query| self.search(query, options.clone()))
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

        let started = Instant::now();
        let segments_total = self.manifest.segments.len();
        let resident_bytes_estimate = self.manifest.resident_bytes_estimate();

        if options.k == 0 {
            return Ok(SearchReport {
                hits: Vec::new(),
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

        let metric = &self.manifest.config.metric;
        let mut candidates = self
            .manifest
            .segments
            .iter()
            .map(|summary| {
                let lower_bound = summary.lower_bound(query, metric).unwrap_or(0.0);
                (summary, lower_bound)
            })
            .collect::<Vec<_>>();

        candidates
            .sort_by(|(_, left), (_, right)| left.partial_cmp(right).unwrap_or(Ordering::Equal));

        let mut hits = Vec::<SearchHit>::new();
        let mut segments_searched = 0_usize;
        let mut segments_skipped = 0_usize;
        let mut bytes_read = 0_u64;
        let mut graph_bytes_read = 0_u64;
        let mut object_cache_hits = 0_usize;
        let mut object_cache_misses = 0_usize;
        let mut records_considered = 0_usize;
        let mut records_scored = 0_usize;
        let mut graph_candidates_added = 0_usize;

        for (candidate_index, (summary, lower_bound)) in candidates.into_iter().enumerate() {
            if should_stop_before_segment(
                &hits,
                options.k,
                &options.mode,
                segments_searched,
                bytes_read,
                lower_bound,
                started.elapsed().as_millis() as u64,
            ) {
                segments_skipped = segments_total - candidate_index;
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

            let graph = if should_expand_segment_graph(&options.mode) {
                let (graph, graph_bytes, graph_cache_hit) = self.read_graph(summary)?;
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
            created_at: segment.created_at,
        })
    }

    fn read_segment(&self, summary: &SegmentSummary) -> Result<(Segment, u64, bool)> {
        let read = self.storage.read_bytes_with_cache_status(&summary.path)?;
        let bytes_read = read.bytes.len() as u64;
        let checksum = blake3::hash(&read.bytes).to_hex().to_string();
        if checksum != summary.checksum {
            return Err(BorsukError::ChecksumMismatch {
                path: summary.path.clone(),
                expected: summary.checksum.clone(),
                actual: checksum,
            });
        }

        Ok((
            segment_from_parquet(&read.bytes)?,
            bytes_read,
            read.cache_hit,
        ))
    }

    fn read_graph(&self, summary: &SegmentSummary) -> Result<(SegmentGraph, u64, bool)> {
        let read = self
            .storage
            .read_bytes_with_cache_status(&summary.graph_path)?;
        let bytes_read = read.bytes.len() as u64;
        let checksum = blake3::hash(&read.bytes).to_hex().to_string();
        if checksum != summary.graph_checksum {
            return Err(BorsukError::ChecksumMismatch {
                path: summary.graph_path.clone(),
                expected: summary.graph_checksum.clone(),
                actual: checksum,
            });
        }

        Ok((
            graph_from_parquet(&read.bytes, &summary.id, summary.level)?,
            bytes_read,
            read.cache_hit,
        ))
    }

    fn validate_vector(&self, vector: &[f32]) -> Result<()> {
        if vector.len() == self.manifest.config.dimensions {
            Ok(())
        } else {
            Err(BorsukError::DimensionMismatch {
                expected: self.manifest.config.dimensions,
                actual: vector.len(),
            })
        }
    }
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
    k: usize,
) -> Result<CandidateRecordSelection> {
    let Some(max_candidates_per_segment) = max_candidates_per_segment(mode) else {
        return Ok(CandidateRecordSelection {
            indices: (0..segment.records.len()).collect(),
            graph_candidates_added: 0,
        });
    };

    let limit = max_candidates_per_segment.max(k).min(segment.records.len());
    let query_code = routing_code(query);
    let mut indices = (0..segment.records.len()).collect::<Vec<_>>();
    indices.sort_by(|left, right| {
        let left_distance = routing_code_distance(segment, *left, query_code);
        let right_distance = routing_code_distance(segment, *right, query_code);
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

    let record_index_by_id = segment
        .records
        .iter()
        .enumerate()
        .map(|(index, record)| (record.id.as_str(), index))
        .collect::<HashMap<_, _>>();
    let mut adjacency = HashMap::<usize, Vec<usize>>::new();
    for edge in &graph.edges {
        let Some(source_index) = record_index_by_id
            .get(edge.source_record_id.as_str())
            .copied()
        else {
            continue;
        };
        let Some(neighbor_index) = record_index_by_id
            .get(edge.neighbor_record_id.as_str())
            .copied()
        else {
            continue;
        };
        adjacency
            .entry(source_index)
            .or_default()
            .push(neighbor_index);
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

fn should_expand_segment_graph(mode: &SearchMode) -> bool {
    matches!(
        mode,
        SearchMode::Approx {
            max_candidates_per_segment: Some(_),
            ..
        }
    )
}

fn max_candidates_per_segment(mode: &SearchMode) -> Option<usize> {
    match mode {
        SearchMode::Exact => None,
        SearchMode::Approx {
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
            .is_some_and(|best_k| lower_bound >= best_k.distance),
        SearchMode::Approx {
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
