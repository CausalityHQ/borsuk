//! BORSUK command-line administration tool.

use std::{fs, path::PathBuf, str::FromStr};

use borsuk::{
    BorsukIndex, CompactionOptions, GarbageCollectionOptions, IndexConfig, SearchMode,
    SearchOptions, VectorMetric, VectorRecord,
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
        } => {
            BorsukIndex::create(IndexConfig {
                uri,
                metric,
                dimensions,
                segment_max_vectors,
                ram_budget_bytes: None,
            })?;
            Ok(())
        }
        Commands::Add { uri, input } => {
            let bytes = fs::read(&input).map_err(|source| CliError::Io {
                path: input.clone(),
                source,
            })?;
            let records = serde_json::from_slice::<Vec<VectorRecord>>(&bytes)?;
            let mut index = BorsukIndex::open(&uri)?;
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
        } => {
            let query = serde_json::from_str::<Vec<f32>>(&query)?;
            let index = BorsukIndex::open(&uri)?;
            let options = SearchOptions {
                k,
                mode: match mode {
                    CliSearchMode::Exact => SearchMode::Exact,
                    CliSearchMode::Approx => SearchMode::Approx {
                        eps,
                        max_segments,
                        max_bytes,
                        max_latency_ms,
                        max_candidates_per_segment,
                    },
                },
            };
            let hits = index.search(&query, options)?;
            println!("{}", serde_json::to_string(&hits)?);
            Ok(())
        }
        Commands::Stats { uri } => {
            let index = BorsukIndex::open(&uri)?;
            println!("{}", serde_json::to_string(&index.stats())?);
            Ok(())
        }
        Commands::Compact {
            uri,
            source_level,
            target_level,
            max_segments,
            min_segments,
            target_segment_max_vectors,
        } => {
            let mut index = BorsukIndex::open(&uri)?;
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
    },
    /// Add records from a JSON file.
    Add {
        /// Existing index URI.
        #[arg(long)]
        uri: String,
        /// JSON file containing an array of `{ "id": "...", "vector": [...] }`.
        #[arg(long)]
        input: PathBuf,
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
        max_bytes: Option<u64>,
        /// Approximate latency budget in milliseconds.
        #[arg(long)]
        max_latency_ms: Option<u64>,
        /// Approximate exact-scored candidate budget per fetched segment.
        #[arg(long)]
        max_candidates_per_segment: Option<usize>,
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
        /// Minimum matching source segments required before compaction runs.
        #[arg(long, default_value_t = 2)]
        min_segments: usize,
        /// Maximum vectors per compacted output segment.
        #[arg(long)]
        target_segment_max_vectors: Option<usize>,
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
