//! BORSUK command-line administration tool.

use std::{
    fs,
    path::{Path, PathBuf},
    str::FromStr,
    time::Duration,
};

use borsuk::{
    BorsukError, BorsukIndex, CompactionOptions, DEFAULT_COMPACTION_MAX_SEGMENTS, Fusion,
    GarbageCollectionOptions, HybridOptions, HybridQuery, IncrementalMaintenanceOptions,
    IndexConfig, LeafMode, OpenOptions, RebuildOptions, RecordId, SearchHit, SearchMode,
    SearchOptions, SparseVector, VectorMetric, VectorRecord, metadata_from_json, metadata_to_json,
    vector_records_from_parquet,
};
use clap::{Parser, Subcommand};

/// Replace each serialized hit's tagged `metadata` with its plain-JSON form so
/// the CLI emits user-facing metadata (`{"genre":"rock"}`, not `{"Str":...}`).
/// Hits without returned metadata get a `null` field.
fn rewrite_hit_metadata(hit_values: &mut [serde_json::Value], hits: &[SearchHit]) {
    for (hit_value, hit) in hit_values.iter_mut().zip(hits.iter()) {
        if let Some(object) = hit_value.as_object_mut() {
            let metadata = match &hit.metadata {
                Some(metadata) => metadata_to_json(metadata),
                None => serde_json::Value::Null,
            };
            object.insert("metadata".to_string(), metadata);
        }
    }
}

fn print_search_output(search: &borsuk::SearchReport, report: bool) -> Result<()> {
    if report {
        let mut value = serde_json::to_value(search)?;
        if let Some(hits) = value.get_mut("hits").and_then(|hits| hits.as_array_mut()) {
            rewrite_hit_metadata(hits, &search.hits);
        }
        println!("{}", serde_json::to_string(&value)?);
    } else {
        let mut hits = serde_json::to_value(&search.hits)?;
        if let Some(hits) = hits.as_array_mut() {
            rewrite_hit_metadata(hits, &search.hits);
        }
        println!("{}", serde_json::to_string(&hits)?);
    }
    Ok(())
}

/// User-facing JSON shape for `borsuk add`: metadata is a plain JSON object
/// rather than the engine's internal tagged representation.
#[derive(serde::Deserialize)]
struct JsonRecord {
    id: RecordId,
    vector: Vec<f32>,
    #[serde(default)]
    metadata: serde_json::Value,
    #[serde(default)]
    sparse: Option<JsonSparse>,
    #[serde(default)]
    sparse_indices: Option<Vec<u32>>,
    #[serde(default)]
    sparse_values: Option<Vec<f32>>,
    #[serde(default)]
    text: Option<String>,
}

#[derive(serde::Deserialize)]
struct JsonSparse {
    indices: Vec<u32>,
    values: Vec<f32>,
}

impl JsonRecord {
    fn into_record(self) -> borsuk::Result<VectorRecord> {
        let JsonRecord {
            id,
            vector,
            metadata,
            sparse,
            sparse_indices,
            sparse_values,
            text,
        } = self;
        let metadata = metadata_from_json(&metadata)?;
        let mut record = VectorRecord::new(id, vector).with_metadata(metadata);
        if let Some((indices, values)) = json_sparse_payload(sparse, sparse_indices, sparse_values)?
        {
            record = record.with_sparse(indices, values)?;
        }
        if let Some(text) = text {
            record = record.with_text(text);
        }
        Ok(record)
    }
}

fn json_sparse_payload(
    sparse: Option<JsonSparse>,
    sparse_indices: Option<Vec<u32>>,
    sparse_values: Option<Vec<f32>>,
) -> borsuk::Result<Option<(Vec<u32>, Vec<f32>)>> {
    match (sparse, sparse_indices, sparse_values) {
        (Some(sparse), None, None) => Ok(Some((sparse.indices, sparse.values))),
        (None, Some(indices), Some(values)) => Ok(Some((indices, values))),
        (None, None, None) => Ok(None),
        (Some(_), _, _) => Err(BorsukError::InvalidRecordInput(
            "record cannot specify both `sparse` and `sparse_indices`/`sparse_values`".to_string(),
        )),
        (None, Some(_), None) | (None, None, Some(_)) => Err(BorsukError::InvalidRecordInput(
            "`sparse_indices` and `sparse_values` must be provided together".to_string(),
        )),
    }
}

fn main() {
    if let Err(error) = run() {
        eprintln!("{error}");
        std::process::exit(1);
    }
}

fn run() -> Result<()> {
    match Cli::parse().command {
        Commands::Create {
            uri,
            metric,
            dimensions,
            segment_max_vectors,
            routing_page_fanout,
            ram_budget,
            sparse,
            text,
        } => {
            let ram_budget_bytes = ram_budget
                .as_deref()
                .map(borsuk::parse_ram_budget)
                .transpose()?;
            let config = IndexConfig {
                uri,
                metric,
                dimensions,
                segment_max_vectors,
                ram_budget_bytes,
                sparse,
                text,
            };
            if let Some(routing_page_fanout) = routing_page_fanout {
                BorsukIndex::create_with_routing_page_fanout(config, routing_page_fanout)?;
            } else {
                BorsukIndex::create(config)?;
            }
            Ok(())
        }
        Commands::Add {
            uri,
            input,
            input_format,
            resident_routing,
        } => {
            let bytes = fs::read(&input).map_err(|source| CliError::Io {
                path: input.clone(),
                source,
            })?;
            let mut index = open_index(&uri, None, resident_routing)?;
            let records = match input_format.resolve(&input) {
                CliInputFormat::Parquet => {
                    vector_records_from_parquet(&bytes, index.manifest().config.dimensions)?
                }
                CliInputFormat::Json => serde_json::from_slice::<Vec<JsonRecord>>(&bytes)?
                    .into_iter()
                    .map(JsonRecord::into_record)
                    .collect::<borsuk::Result<Vec<_>>>()?,
                CliInputFormat::Auto => unreachable!("auto input format must be resolved"),
            };
            index.add(records)?;
            Ok(())
        }
        Commands::Search {
            uri,
            query,
            k,
            mode,
            eps,
            max_segments,
            max_bytes,
            max_latency_ms,
            routing_page_overfetch,
            max_candidates_per_segment,
            leaf_mode,
            filter,
            include_metadata,
            report,
            cache_dir,
            resident_routing,
        } => {
            let query = serde_json::from_str::<Vec<f32>>(&query)?;
            let max_bytes = max_bytes
                .as_deref()
                .map(|value| borsuk::parse_byte_size(value, "max_bytes"))
                .transpose()?;
            let filter = match filter.as_deref() {
                Some(value) => {
                    let parsed = serde_json::from_str::<serde_json::Value>(value)?;
                    Some(borsuk::Filter::from_json(&parsed)?)
                }
                None => None,
            };
            let index = open_index(&uri, cache_dir, resident_routing)?;
            let options = SearchOptions {
                k,
                mode: match mode {
                    CliSearchMode::Exact => SearchMode::Exact,
                    CliSearchMode::Approx => SearchMode::Approx {
                        leaf_mode: leaf_mode.into(),
                        eps,
                        max_segments,
                        max_bytes,
                        max_latency_ms,
                        routing_page_overfetch,
                        max_candidates_per_segment,
                    },
                },
                filter,
                include_metadata,
                guaranteed_recall: false,
                prefetch_depth: borsuk::DEFAULT_SEARCH_PREFETCH_DEPTH,
            };
            let search = index.search_with_report(&query, options)?;
            print_search_output(&search, report)?;
            Ok(())
        }
        Commands::SearchSparse {
            uri,
            indices,
            values,
            k,
            report,
            cache_dir,
            resident_routing,
        } => {
            let indices = parse_csv_values("indices", &indices)?;
            let values = parse_csv_values("values", &values)?;
            let query = SparseVector::new(indices, values)?;
            let index = open_index(&uri, cache_dir, resident_routing)?;
            let search = index.search_sparse(&query, k)?;
            print_search_output(&search, report)?;
            Ok(())
        }
        Commands::SearchText {
            uri,
            text,
            k,
            report,
            cache_dir,
            resident_routing,
        } => {
            let index = open_index(&uri, cache_dir, resident_routing)?;
            let search = index.search_text(&text, k)?;
            print_search_output(&search, report)?;
            Ok(())
        }
        Commands::SearchHybrid {
            uri,
            vector,
            indices,
            values,
            text,
            k,
            fusion,
            rrf_k,
            weights,
            report,
            cache_dir,
            resident_routing,
        } => {
            let mut query = HybridQuery::new();
            let mut has_query = false;
            if !vector.is_empty() {
                query = query.with_dense(parse_csv_values("vector", &vector)?);
                has_query = true;
            }
            match (indices.is_empty(), values.is_empty()) {
                (false, false) => {
                    let sparse = SparseVector::new(
                        parse_csv_values("indices", &indices)?,
                        parse_csv_values("values", &values)?,
                    )?;
                    query = query.with_sparse(sparse);
                    has_query = true;
                }
                (true, true) => {}
                _ => {
                    return Err(BorsukError::InvalidSearchOptions(
                        "`--indices` and `--values` must be provided together".to_string(),
                    )
                    .into());
                }
            }
            if let Some(text) = text {
                query = query.with_text(text);
                has_query = true;
            }
            if !has_query {
                return Err(BorsukError::InvalidSearchOptions(
                    "hybrid query must include at least one of `--vector`, `--indices`/`--values`, or `--text`"
                        .to_string(),
                )
                .into());
            }
            let mut options = HybridOptions::new(k);
            options.fusion = match fusion {
                CliFusion::Rrf => Fusion::Rrf { k: rrf_k },
                CliFusion::Weighted => {
                    let [dense, sparse, text] = parse_weights(weights.as_deref())?;
                    Fusion::Weighted {
                        dense,
                        sparse,
                        text,
                    }
                }
            };
            let index = open_index(&uri, cache_dir, resident_routing)?;
            let search = index.search_hybrid(&query, options)?;
            print_search_output(&search, report)?;
            Ok(())
        }
        Commands::Stats {
            uri,
            resident_routing,
        } => {
            let index = open_index(&uri, None, resident_routing)?;
            println!("{}", serde_json::to_string(&index.try_stats()?)?);
            Ok(())
        }
        Commands::Compact {
            uri,
            source_level,
            target_level,
            max_segments,
            all_matching,
            min_segments,
            target_segment_max_vectors,
            target_segment_max_radius,
            cache_dir,
            resident_routing,
        } => {
            let mut index = open_index(&uri, cache_dir, resident_routing)?;
            let max_segments = if all_matching {
                None
            } else {
                Some(max_segments.unwrap_or(DEFAULT_COMPACTION_MAX_SEGMENTS))
            };
            let report = index.compact(CompactionOptions {
                source_level,
                target_level,
                max_segments,
                min_segments,
                target_segment_max_vectors,
                target_segment_max_radius,
            })?;
            println!("{}", serde_json::to_string(&report)?);
            Ok(())
        }
        Commands::Rebuild {
            uri,
            source_level,
            target_level,
            min_segments,
            target_segment_max_vectors,
            delete_obsolete,
            cache_dir,
            resident_routing,
        } => {
            let mut index = open_index(&uri, cache_dir, resident_routing)?;
            let report = index.rebuild(RebuildOptions {
                source_level,
                target_level,
                min_segments,
                target_segment_max_vectors,
                delete_obsolete,
            })?;
            println!("{}", serde_json::to_string(&report)?);
            Ok(())
        }
        Commands::Gc {
            uri,
            delete,
            min_age_seconds,
            resident_routing,
        } => {
            let mut index = open_index(&uri, None, resident_routing)?;
            // Repo-policy anchor for the CLI dry-run flag: GarbageCollectionOptions { dry_run: !delete }.
            let report = index.gc_obsolete_segments(GarbageCollectionOptions {
                dry_run: !delete,
                min_age: Duration::from_secs(min_age_seconds),
            })?;
            println!("{}", serde_json::to_string(&report)?);
            Ok(())
        }
        Commands::Delete {
            uri,
            ids,
            cache_dir,
            resident_routing,
        } => {
            let mut index = open_index(&uri, cache_dir, resident_routing)?;
            let report = index.delete_with_report(ids)?;
            println!("{}", serde_json::to_string(&report)?);
            Ok(())
        }
        Commands::Purge {
            uri,
            cache_dir,
            resident_routing,
        } => {
            let mut index = open_index(&uri, cache_dir, resident_routing)?;
            let report = index.purge_with_report()?;
            println!("{}", serde_json::to_string(&report)?);
            Ok(())
        }
        Commands::Maintain {
            uri,
            max_segment_vectors,
            max_segment_radius,
            min_segment_vectors,
            max_operations,
            cache_dir,
            resident_routing,
        } => {
            let mut index = open_index(&uri, cache_dir, resident_routing)?;
            let defaults = IncrementalMaintenanceOptions::default();
            let report = index.run_incremental_maintenance(IncrementalMaintenanceOptions {
                max_segment_vectors: max_segment_vectors.unwrap_or(defaults.max_segment_vectors),
                max_segment_radius,
                min_segment_vectors: min_segment_vectors.unwrap_or(defaults.min_segment_vectors),
                max_operations: max_operations.unwrap_or(defaults.max_operations),
            })?;
            println!("{}", serde_json::to_string(&report)?);
            Ok(())
        }
    }
}

fn open_index(
    uri: &str,
    cache_dir: Option<PathBuf>,
    resident_routing: bool,
) -> Result<BorsukIndex> {
    Ok(BorsukIndex::open_with_options(
        uri,
        OpenOptions {
            cache_dir,
            resident_routing,
            ..OpenOptions::default()
        },
    )?)
}

#[derive(Debug, Parser)]
#[command(
    name = "borsuk",
    version,
    about = "BORSUK local/blob similarity search"
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Create a BORSUK index.
    Create {
        /// Index URI. Plain paths, `file://...`, `s3://...`, `gs://...`, and `az://...` are supported.
        #[arg(long)]
        uri: String,
        /// Fixed metric for the physical index.
        #[arg(long, value_parser = parse_metric)]
        metric: VectorMetric,
        /// Dense vector dimensionality.
        #[arg(long)]
        dimensions: usize,
        /// Maximum vectors per immutable segment.
        #[arg(long, default_value_t = 4096)]
        segment_max_vectors: usize,
        /// Routing page fanout used to compute the persisted hierarchy depth.
        #[arg(long)]
        routing_page_fanout: Option<usize>,
        /// Optional resident metadata RAM budget, for example `512MB` or `2GiB`.
        #[arg(long)]
        ram_budget: Option<String>,
        /// Enable sparse-vector payloads and sparse search for this index.
        #[arg(long)]
        sparse: bool,
        /// Enable text payloads and BM25 search for this index.
        #[arg(long)]
        text: bool,
    },
    /// Add records from a binary Parquet table or JSON fixture file.
    Add {
        /// Existing index URI.
        #[arg(long)]
        uri: String,
        /// Input containing records with ids and vectors.
        #[arg(long)]
        input: PathBuf,
        /// Input file format. `auto` treats `.parquet` and `.parq` as Parquet, otherwise JSON.
        #[arg(long, value_enum, default_value = "auto")]
        input_format: CliInputFormat,
        /// Keep routing summaries resident in RAM for lower latency on small, hot
        /// indexes. Default is paged routing (minimal RAM).
        #[arg(long)]
        resident_routing: bool,
    },
    /// Search an index and write JSON hits to stdout.
    Search {
        /// Existing index URI.
        #[arg(long)]
        uri: String,
        /// Query vector as a JSON array.
        #[arg(long)]
        query: String,
        /// Number of hits to return.
        #[arg(long, default_value_t = 10)]
        k: usize,
        /// Search mode.
        #[arg(long, default_value = "exact")]
        mode: CliSearchMode,
        /// Approximate epsilon.
        #[arg(long)]
        eps: Option<f32>,
        /// Approximate segment budget.
        #[arg(long)]
        max_segments: Option<usize>,
        /// Approximate segment payload byte budget.
        #[arg(long)]
        max_bytes: Option<String>,
        /// Approximate latency budget in milliseconds.
        #[arg(long)]
        max_latency_ms: Option<u64>,
        /// Approximate routing metadata page overfetch multiplier.
        #[arg(long)]
        routing_page_overfetch: Option<usize>,
        /// Approximate exact-scored candidate budget per fetched segment.
        #[arg(long)]
        max_candidates_per_segment: Option<usize>,
        /// Segment-local leaf engine for approximate candidate generation.
        #[arg(long, default_value = "graph")]
        leaf_mode: CliLeafMode,
        /// Metadata filter as a Pinecone-style JSON object, for example
        /// `{"genre":"rock","year":{"$gte":1990}}`. Records whose metadata does
        /// not match are never returned.
        #[arg(long)]
        filter: Option<String>,
        /// Include each hit's stored metadata in the JSON output.
        #[arg(long)]
        include_metadata: bool,
        /// Emit a full SearchReport JSON object instead of only hit rows.
        #[arg(long)]
        report: bool,
        /// Optional local read-through cache directory for fetched objects.
        #[arg(long)]
        cache_dir: Option<PathBuf>,
        /// Keep routing summaries resident in RAM for lower latency on small, hot
        /// indexes. Default is paged routing (minimal RAM).
        #[arg(long)]
        resident_routing: bool,
    },
    /// Search an index by sparse vector and write JSON hits to stdout.
    SearchSparse {
        /// Existing index URI.
        #[arg(long, alias = "index")]
        uri: String,
        /// Sparse query dimension ids. Repeat or pass comma-separated values.
        #[arg(long, required = true)]
        indices: Vec<String>,
        /// Sparse query values. Repeat or pass comma-separated values.
        #[arg(long, required = true)]
        values: Vec<String>,
        /// Number of hits to return.
        #[arg(long, default_value_t = 10)]
        k: usize,
        /// Emit a full SearchReport JSON object instead of only hit rows.
        #[arg(long)]
        report: bool,
        /// Optional local read-through cache directory for fetched objects.
        #[arg(long)]
        cache_dir: Option<PathBuf>,
        /// Keep routing summaries resident in RAM for lower latency on small, hot
        /// indexes. Default is paged routing (minimal RAM).
        #[arg(long)]
        resident_routing: bool,
    },
    /// Search an index by BM25 text query and write JSON hits to stdout.
    SearchText {
        /// Existing index URI.
        #[arg(long, alias = "index")]
        uri: String,
        /// Text query.
        #[arg(long)]
        text: String,
        /// Number of hits to return.
        #[arg(long, default_value_t = 10)]
        k: usize,
        /// Emit a full SearchReport JSON object instead of only hit rows.
        #[arg(long)]
        report: bool,
        /// Optional local read-through cache directory for fetched objects.
        #[arg(long)]
        cache_dir: Option<PathBuf>,
        /// Keep routing summaries resident in RAM for lower latency on small, hot
        /// indexes. Default is paged routing (minimal RAM).
        #[arg(long)]
        resident_routing: bool,
    },
    /// Search an index by dense, sparse, and/or text query fusion.
    SearchHybrid {
        /// Existing index URI.
        #[arg(long, alias = "index")]
        uri: String,
        /// Dense query vector. Repeat or pass comma-separated values.
        #[arg(long)]
        vector: Vec<String>,
        /// Sparse query dimension ids. Repeat or pass comma-separated values.
        #[arg(long)]
        indices: Vec<String>,
        /// Sparse query values. Repeat or pass comma-separated values.
        #[arg(long)]
        values: Vec<String>,
        /// Text query.
        #[arg(long)]
        text: Option<String>,
        /// Number of hits to return.
        #[arg(long, default_value_t = 10)]
        k: usize,
        /// Fusion strategy.
        #[arg(long, default_value = "rrf")]
        fusion: CliFusion,
        /// Reciprocal-rank-fusion rank constant.
        #[arg(long, default_value_t = 60)]
        rrf_k: usize,
        /// Weighted fusion weights as dense,sparse,text.
        #[arg(long)]
        weights: Option<String>,
        /// Emit a full SearchReport JSON object instead of only hit rows.
        #[arg(long)]
        report: bool,
        /// Optional local read-through cache directory for fetched objects.
        #[arg(long)]
        cache_dir: Option<PathBuf>,
        /// Keep routing summaries resident in RAM for lower latency on small, hot
        /// indexes. Default is paged routing (minimal RAM).
        #[arg(long)]
        resident_routing: bool,
    },
    /// Print manifest-derived index statistics as JSON.
    Stats {
        /// Existing index URI.
        #[arg(long)]
        uri: String,
        /// Keep routing summaries resident in RAM for lower latency on small, hot
        /// indexes. Default is paged routing (minimal RAM).
        #[arg(long)]
        resident_routing: bool,
    },
    /// Compact immutable segments out-of-place and publish a new manifest.
    Compact {
        /// Existing index URI.
        #[arg(long)]
        uri: String,
        /// Source LSM level to compact from.
        #[arg(long, default_value_t = 0)]
        source_level: u8,
        /// Target LSM level to compact into.
        #[arg(long, default_value_t = 1)]
        target_level: u8,
        /// Maximum number of source segments to compact.
        #[arg(long)]
        max_segments: Option<usize>,
        /// Compact all matching source segments instead of the bounded default batch.
        #[arg(long, conflicts_with = "max_segments")]
        all_matching: bool,
        /// Minimum matching source segments required before compaction runs.
        #[arg(long, default_value_t = 2)]
        min_segments: usize,
        /// Maximum vectors per compacted output segment.
        #[arg(long)]
        target_segment_max_vectors: Option<usize>,
        /// Optional maximum bubble radius per compacted output segment. Splits a
        /// spread-out cluster into tight, small-radius segments that prune better.
        #[arg(long)]
        target_segment_max_radius: Option<f32>,
        /// Optional local read-through cache directory for fetched objects.
        #[arg(long)]
        cache_dir: Option<PathBuf>,
        /// Keep routing summaries resident in RAM for lower latency on small, hot
        /// indexes. Default is paged routing (minimal RAM).
        #[arg(long)]
        resident_routing: bool,
    },
    /// Rebuild a full source level and report or delete obsolete objects.
    Rebuild {
        /// Existing index URI.
        #[arg(long)]
        uri: String,
        /// Source LSM level to rebuild from.
        #[arg(long, default_value_t = 0)]
        source_level: u8,
        /// Target LSM level to rebuild into.
        #[arg(long, default_value_t = 1)]
        target_level: u8,
        /// Minimum matching source segments required before rebuild compaction runs.
        #[arg(long, default_value_t = 1)]
        min_segments: usize,
        /// Maximum vectors per rebuilt output segment.
        #[arg(long)]
        target_segment_max_vectors: Option<usize>,
        /// Delete obsolete segment and graph objects after publishing the rebuilt manifest.
        #[arg(long)]
        delete_obsolete: bool,
        /// Optional local read-through cache directory for fetched objects.
        #[arg(long)]
        cache_dir: Option<PathBuf>,
        /// Keep routing summaries resident in RAM for lower latency on small, hot
        /// indexes. Default is paged routing (minimal RAM).
        #[arg(long)]
        resident_routing: bool,
    },
    /// Garbage collect inactive segment objects that are not referenced by the active manifest.
    Gc {
        /// Existing index URI.
        #[arg(long)]
        uri: String,
        /// Actually delete obsolete objects. Without this flag, GC only reports candidates.
        #[arg(long)]
        delete: bool,
        /// Minimum age in seconds before an obsolete object can be reported or deleted.
        #[arg(long, default_value_t = 86_400)]
        min_age_seconds: u64,
        /// Keep routing summaries resident in RAM for lower latency on small, hot
        /// indexes. Default is paged routing (minimal RAM).
        #[arg(long)]
        resident_routing: bool,
    },
    /// Logically delete records by id. Deletes are hidden from search immediately;
    /// storage is reclaimed later by compaction or `purge`.
    Delete {
        /// Existing index URI.
        #[arg(long)]
        uri: String,
        /// Record id to delete. Repeat `--id` to delete several ids in one call.
        #[arg(long = "id", required = true)]
        ids: Vec<String>,
        /// Optional local read-through cache directory for fetched objects.
        #[arg(long)]
        cache_dir: Option<PathBuf>,
        /// Keep routing summaries resident in RAM for lower latency on small, hot
        /// indexes. Default is paged routing (minimal RAM).
        #[arg(long)]
        resident_routing: bool,
    },
    /// Physically remove every deleted record and clear the tombstone, reclaiming
    /// storage synchronously and re-enabling those ids for `add`.
    Purge {
        /// Existing index URI.
        #[arg(long)]
        uri: String,
        /// Optional local read-through cache directory for fetched objects.
        #[arg(long)]
        cache_dir: Option<PathBuf>,
        /// Keep routing summaries resident in RAM for lower latency on small, hot
        /// indexes. Default is paged routing (minimal RAM).
        #[arg(long)]
        resident_routing: bool,
    },
    /// Run one incremental-maintenance pass: split oversized bubbles and merge
    /// sparse ones locally, touching only the affected segments.
    Maintain {
        /// Existing index URI.
        #[arg(long)]
        uri: String,
        /// Split a segment holding more than this many vectors.
        #[arg(long)]
        max_segment_vectors: Option<usize>,
        /// Also split a segment whose bubble radius exceeds this.
        #[arg(long)]
        max_segment_radius: Option<f32>,
        /// Merge a segment whose live vector count falls below this.
        #[arg(long)]
        min_segment_vectors: Option<usize>,
        /// Maximum split/merge operations to apply in this pass.
        #[arg(long)]
        max_operations: Option<usize>,
        /// Optional local read-through cache directory for fetched objects.
        #[arg(long)]
        cache_dir: Option<PathBuf>,
        /// Keep routing summaries resident in RAM for lower latency on small, hot
        /// indexes. Default is paged routing (minimal RAM).
        #[arg(long)]
        resident_routing: bool,
    },
}

#[derive(Debug, Clone, Copy, clap::ValueEnum)]
enum CliSearchMode {
    Exact,
    Approx,
}

#[derive(Debug, Clone, Copy, clap::ValueEnum)]
enum CliLeafMode {
    FlatScan,
    SqScan,
    PqScan,
    Graph,
    VamanaPq,
    Hybrid,
}

#[derive(Debug, Clone, Copy, clap::ValueEnum)]
enum CliFusion {
    Rrf,
    Weighted,
}

#[derive(Debug, Clone, Copy, clap::ValueEnum)]
enum CliInputFormat {
    Auto,
    Parquet,
    Json,
}

impl CliInputFormat {
    fn resolve(self, input: &Path) -> Self {
        match self {
            Self::Auto => match input.extension().and_then(|extension| extension.to_str()) {
                Some(extension) if extension.eq_ignore_ascii_case("parquet") => Self::Parquet,
                Some(extension) if extension.eq_ignore_ascii_case("parq") => Self::Parquet,
                _ => Self::Json,
            },
            format => format,
        }
    }
}

impl From<CliLeafMode> for LeafMode {
    fn from(mode: CliLeafMode) -> Self {
        match mode {
            CliLeafMode::FlatScan => Self::FlatScan,
            CliLeafMode::SqScan => Self::SqScan,
            CliLeafMode::PqScan => Self::PqScan,
            CliLeafMode::Graph => Self::Graph,
            CliLeafMode::VamanaPq => Self::VamanaPq,
            CliLeafMode::Hybrid => Self::Hybrid,
        }
    }
}

#[derive(Debug, thiserror::Error)]
enum CliError {
    #[error(transparent)]
    Borsuk(#[from] borsuk::BorsukError),

    #[error("I/O error at `{path}`: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error(transparent)]
    Json(#[from] serde_json::Error),
}

type Result<T> = std::result::Result<T, CliError>;

fn parse_metric(value: &str) -> std::result::Result<VectorMetric, String> {
    VectorMetric::from_str(value).map_err(|error| error.to_string())
}

fn parse_csv_values<T>(field: &str, values: &[String]) -> Result<Vec<T>>
where
    T: FromStr,
    T::Err: std::fmt::Display,
{
    let mut parsed = Vec::new();
    for value in values {
        for token in value.split(',') {
            let token = token.trim();
            if token.is_empty() {
                return Err(BorsukError::InvalidSearchOptions(format!(
                    "`--{field}` contains an empty value"
                ))
                .into());
            }
            parsed.push(token.parse::<T>().map_err(|error| {
                BorsukError::InvalidSearchOptions(format!(
                    "`--{field}` value `{token}` is invalid: {error}"
                ))
            })?);
        }
    }
    if parsed.is_empty() {
        return Err(BorsukError::InvalidSearchOptions(format!(
            "`--{field}` must contain at least one value"
        ))
        .into());
    }
    Ok(parsed)
}

fn parse_weights(weights: Option<&str>) -> Result<[f32; 3]> {
    let Some(weights) = weights else {
        return Ok([1.0, 1.0, 1.0]);
    };
    let values = parse_csv_values::<f32>("weights", &[weights.to_string()])?;
    match values.as_slice() {
        [dense, sparse, text] => Ok([*dense, *sparse, *text]),
        _ => Err(BorsukError::InvalidSearchOptions(
            "`--weights` must contain exactly three values: dense,sparse,text".to_string(),
        )
        .into()),
    }
}
