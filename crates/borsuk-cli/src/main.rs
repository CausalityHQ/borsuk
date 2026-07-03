//! BORSUK command-line administration tool.

use std::{
    fs,
    path::{Path, PathBuf},
    str::FromStr,
};

use borsuk::{
    BorsukIndex, CompactionOptions, DEFAULT_COMPACTION_MAX_SEGMENTS, GarbageCollectionOptions,
    IndexConfig, LeafMode, SearchMode, SearchOptions, VectorMetric, VectorRecord,
    vector_records_from_parquet,
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
            ram_budget,
        } => {
            let ram_budget_bytes = ram_budget
                .as_deref()
                .map(borsuk::parse_ram_budget)
                .transpose()?;
            BorsukIndex::create(IndexConfig {
                uri,
                metric,
                dimensions,
                segment_max_vectors,
                ram_budget_bytes,
            })?;
            Ok(())
        }
        Commands::Add {
            uri,
            input,
            input_format,
        } => {
            let bytes = fs::read(&input).map_err(|source| CliError::Io {
                path: input.clone(),
                source,
            })?;
            let mut index = BorsukIndex::open(&uri)?;
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
            max_candidates_per_segment,
            leaf_mode,
            report,
            cache_dir,
        } => {
            let query = serde_json::from_str::<Vec<f32>>(&query)?;
            let max_bytes = max_bytes
                .as_deref()
                .map(|value| borsuk::parse_byte_size(value, "max_bytes"))
                .transpose()?;
            let index = BorsukIndex::open_with_cache(&uri, cache_dir)?;
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
                        max_candidates_per_segment,
                    },
                },
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
        Commands::Stats { uri } => {
            let index = BorsukIndex::open(&uri)?;
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
        } => {
            let mut index = BorsukIndex::open_with_cache(&uri, cache_dir)?;
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
        Commands::Gc { uri, delete } => {
            let index = BorsukIndex::open(&uri)?;
            let report =
                index.gc_obsolete_segments(GarbageCollectionOptions { dry_run: !delete })?;
            println!("{}", serde_json::to_string(&report)?);
            Ok(())
        }
    }
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
    },
    /// Print manifest-derived index statistics as JSON.
    Stats {
        /// Existing index URI.
        #[arg(long)]
        uri: String,
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
    },
    /// Garbage collect inactive segment objects that are not referenced by the active manifest.
    Gc {
        /// Existing index URI.
        #[arg(long)]
        uri: String,
        /// Actually delete obsolete objects. Without this flag, GC only reports candidates.
        #[arg(long)]
        delete: bool,
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
