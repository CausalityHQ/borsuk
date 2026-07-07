//! BORSUK command-line administration tool.

use std::{
    fs,
    path::{Path, PathBuf},
    str::FromStr,
    time::Duration,
};

use borsuk::{
    BorsukIndex, CompactionOptions, DEFAULT_COMPACTION_MAX_SEGMENTS, GarbageCollectionOptions,
    IndexConfig, LeafMode, OpenOptions, RebuildOptions, SearchMode, SearchOptions, VectorMetric,
    VectorRecord, vector_records_from_parquet,
};
use clap::{Parser, Subcommand};

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
                CliInputFormat::Json => serde_json::from_slice::<Vec<VectorRecord>>(&bytes)?,
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
            report,
            cache_dir,
            resident_routing,
        } => {
            let query = serde_json::from_str::<Vec<f32>>(&query)?;
            let max_bytes = max_bytes
                .as_deref()
                .map(|value| borsuk::parse_byte_size(value, "max_bytes"))
                .transpose()?;
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
                guaranteed_recall: false,
                prefetch_depth: borsuk::DEFAULT_SEARCH_PREFETCH_DEPTH,
            };
            if report {
                println!(
                    "{}",
                    serde_json::to_string(&index.search_with_report(&query, options)?)?
                );
            } else {
                println!(
                    "{}",
                    serde_json::to_string(&index.search_with_report(&query, options)?.hits)?
                );
            }
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
