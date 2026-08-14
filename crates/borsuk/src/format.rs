use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    io::Cursor,
    panic::{AssertUnwindSafe, catch_unwind},
    str::FromStr,
    sync::Arc,
};

use arrow_array::{
    Array, ArrayRef, BinaryArray, BooleanArray, FixedSizeBinaryArray, FixedSizeListArray,
    Float16Array, Float32Array, Int64Array, ListArray, RecordBatch, StringArray, UInt8Array,
    UInt16Array, UInt32Array, UInt64Array,
    types::{
        Float16Type, Float32Type, Int8Type, Int64Type, UInt8Type, UInt16Type, UInt32Type,
        UInt64Type,
    },
};
use arrow_ipc::reader::StreamReader;
use arrow_schema::{DataType, Field, Schema};
use bytes::Bytes;
use chrono::{DateTime, Utc};
use parquet::{
    arrow::{
        ArrowWriter, ProjectionMask,
        arrow_reader::{ParquetRecordBatchReaderBuilder, RowSelection, RowSelector},
    },
    basic::Compression,
    file::properties::WriterProperties,
};

use crate::{
    error::{BorsukError, Result},
    index::IndexConfig,
    lexical_root::{
        Bm25Posting, LexicalKind, LexicalRoot, LexicalRowMetadata, LexicalRunRef, LexicalTermBlock,
        LexicalTermPage, LexicalTermPageRef, SparsePosting,
    },
    manifest::{
        DEFAULT_GRAPH_NEIGHBORS, DEFAULT_ROUTING_PAGE_FANOUT, Manifest, PivotSummary,
        RoutingLayerPageRef, SEGMENT_ID_BLOOM_BYTES, SEGMENT_VECTOR_SIGNATURE_BLOOM_BYTES,
        SegmentSummary,
    },
    metric::VectorMetric,
    mutation::{MutationOperation, MutationStamp, MutationState, MutationVersion},
    positioned_log::{
        PositionedMutationEnvelope, PositionedMutationPayloadRef, PositionedMutationStamp,
        PositionedPayloadFormat,
    },
    record::{LeafMode, RecordId, StorageEncoding, VectorElementType, VectorRecord},
    segment::{GraphEdge, Segment, SegmentGraph},
};

// Bumped 5 -> 6 when the segment coarse-code triplet (`pq_code`/`pq_min`/`pq_max`)
// began sizing to the quantizer's actual code length instead of `dimensions`:
// TurboQuant's SRHT rotation pads to the next power of two, so on non-power-of-two
// dims those three FixedSizeList columns are now wider than the raw dimensionality.
// That is a physical schema change to the segment table, so the table version bumps.
// Bumped 10 -> 11 when every segment summary gained a required durable table
// format and routing/routing-page schemas began persisting it.
// Bumped 4 -> 5 when sparse named vectors moved from one global rewritten
// object to immutable, generation-aware per-segment sidecars.
// The library is unreleased: incompatible layouts are recreated from source,
// not migrated. CURRENT_VERSION only rejects stale on-disk experiments instead
// of maintaining compatibility branches.
// Bumped 1 -> 2 when cosine/angular indexes began storing their segment and
// routing bubble geometry (centroid, radius, per-dimension bounds) as Euclidean
// geometry over unit-L2-normalized vectors. That changed the *meaning* of
// existing metadata values, so per the versioning policy the table-format
// version bumps: pre-existing v1 indexes are rejected with a clear
// "unsupported manifest table version" error rather than silently mis-pruned.
// Bumped 11 -> 12 when normal-segment and WAL table constants moved from ten
// values repeated in every row into one nullable packed `segment_header`
// value in row zero. This keeps the row scan columnar while avoiding a severe
// repeated per-row values in columnar tables.
// Bumped 12 -> 13 when WAL primary-vector types became a required physical
// column and packed binary WAL
// vectors moved from unsupported FixedSizeBinary to FixedSizeList<UInt8>.
// Bumped 13 -> 14 when WAL record runs stopped reusing the normal-segment
// header/routing/PQ schema and gained their dedicated record-only dimensions
// column. Old experimental indexes are rebuilt rather than migrated.
// Bumped 15 -> 16 when per-record MVCC generation counters became fixed,
// routing-independent shard range allocators and same-lane typed runs began
// sharing one frontier-head publication. The persistent coordination layout
// changed, so older experimental indexes are rejected rather than mixing both
// allocation protocols.
// Bumped 16 -> 17 when collection snapshots became the sole multimodal
// manifest authority and foreground visibility moved from cell/lane discovery
// to root-authorized, 64-way collection WAL frontier shards. Opening an older
// experimental index without that directory would silently omit acknowledged
// WAL transactions, so the pre-release format must reject it.
// Bumped 17 -> 18 when lane preparation gained expiring root reservations.
// Reservations fence crash cleanup against a writer that has not reached its
// final collection commit, and make failed/abandoned lane history reclaimable.
// Bumped 18 -> 19 when every WAL mutation began advancing its affected ID-claim
// shards. An absent or unchanged claim object is now a durable write epoch that
// lets insert-only writers avoid a collection-wide frontier refresh when no
// potentially conflicting ID write occurred.
// Bumped 19 -> 20 when ID claims expanded from 16 to 4,096 logical epochs
// using 12 BLAKE3 digest bits, packed sparsely into 22 coordination pages.
// Generation allocation remains independently bounded to 16 shards. The claim
// path and schema changed, so v19 indexes must not open under this protocol.
// Bumped 20 -> 21 when collection-root-authorized WAL writes stopped publishing
// redundant per-cell lane heads and inner commit markers. Root descriptors are
// now the sole foreground visibility layout for ordinary collection mutation.
// Bumped 21 -> 22 when live WAL mutations became transaction bundles: one
// record object and one ID-directory object per mutation, exact-scanned until
// flush assigns physical cells. Old per-cell tails must not mix with bundles.
// Bumped 22 -> 23 when bundled ID-directory entries became hash-partitioned by
// logical cell, allowing insert-only validation to avoid decoding vector runs.
// Bumped 23 -> 24 when production segment/routing ID blooms expanded while
// live tombstone-run blooms retained their separate compact width.
// Bumped 24 -> 25 when lane-log HEADs made the complete newest block inline so
// one conditional write is the durable acknowledgement boundary.
// Bumped 25 -> 26 when stable tombstone-page references gained required
// resident ID blooms. Without them, every ANN candidate whose hash bucket had
// any historical mutation fetched a separate object before reranking.
// Bumped 26 -> 27 when group commit replaced deterministic ID-ownership lanes
// with independently leased writer stripes and every mutation path converged
// on one group-amortized global generation range allocator. Opening an older
// manifest could otherwise mix incomparable per-shard/per-lane generations and
// return the wrong last-write-wins value across processes.
// Bumped 27 -> 28 when group commit gained a required checked active-stripe
// directory and a 64-slot pool independent of the per-cell WAL lane count.
// Opening v27 without the directory could either miss acknowledged extents or
// restore the old fixed eight-HEAD read fanout semantics.
// Bumped 28 -> 29 when lane materialization and directory retirement became
// manifest-version fenced. A v28 reader could hide a materialized WAL extent
// from a reader still pinned to the manifest that preceded its drain.
// Bumped 29 -> 30 when canonical 192-bit mutation versions and 256-bit
// mutation digests became typed columns in WAL, segment, and exact-vector
// artifacts. A v29 reader would discard the convergent multi-writer order.
// Bumped 30 -> 31 when writer-stripe mutation extents replaced the custom
// framed `.wal` envelope and nested Parquet payload with one stock-readable
// Arrow IPC stream carrying typed mutation identity and modality columns.
// Bumped 31 -> 32 when durable table roles became fixed to Parquet and the
// rejected alternate-backend fields/readers were removed. Pre-release v31
// artifacts are rejected instead of retaining a compatibility path.
// Bumped 32 -> 33 when logical-cell centroids moved from repeated inline
// manifest JSON into one checksum-pinned, content-addressed Parquet catalog.
// Experimental v32 manifests are rebuilt rather than keeping a dual reader.
// Bumped 33 -> 34 when consumed positioned-run identities stopped embedding
// removable logical-cell authority. Experimental v33 manifests are rejected
// rather than interpreting their consumed markers under the new identity.
// Bumped 34 -> 35 when positioned primary WAL rows gained required checked
// routing epoch/cell columns and every positioned envelope gained one dedicated
// route-plan payload. Experimental v34 artifacts cannot establish row owners.
// Bumped 35 -> 36 when the logical-cell routing strategy and exact HNSW
// parameters became required manifest authority. Recomputing them from current
// defaults on open could route an old collection differently after a constant
// change, so format-35 experiments are rejected rather than inferred.
// Bumped 36 -> 37 when full materialization gained one mutually exclusive V14
// cell-card ANN authority. Pre-release v36 manifests cannot name its groups.
// Bumped 37 -> 38 when V15 replaced one independently decoded Arrow head per
// cell-card with a shared contiguous PQ-code plane and moved exact-block
// authority into the authenticated Parquet root. Pre-release v37 manifests
// cannot safely interpret the new group and root layouts.
const CURRENT_VERSION: u16 = 38;
const SEGMENT_HEADER_MAGIC: &[u8; 4] = b"BSH1";
const SEGMENT_HEADER_CODEC_VERSION: u8 = 1;
const SEGMENT_HEADER_CHECKSUM_LEN: usize = 32;
const BLAKE3_HEX_CHECKSUM_LEN: usize = 64;
pub(crate) const LEAN_SEGMENT_HEADER_COLUMNS: &[&str] = &["segment_header"];
pub(crate) const LEAN_SEGMENT_ROW_COLUMNS: &[&str] = &[
    "routing_code",
    "pq_code",
    "record_id",
    "metadata",
    "sparse_indices",
    "sparse_values",
    "text_term_ids",
    "text_term_freqs",
    "generation",
    "mutation_hlc",
    "mutation_writer",
    "mutation_digest",
];
pub(crate) const LEAN_SEGMENT_SCORING_COLUMNS: &[&str] = &[
    "routing_code",
    "pq_code",
    "record_id",
    "generation",
    "mutation_hlc",
    "mutation_writer",
    "mutation_digest",
];
pub(crate) fn manifest_to_parquet(manifest: &Manifest) -> Result<Vec<u8>> {
    if manifest.global_ann_ref.is_some() && manifest.global_cell_card_ann_ref.is_some() {
        return Err(BorsukError::InvalidStorage(
            "manifest cannot publish V13 and V15 global ANN authority together".to_string(),
        ));
    }
    validate_manifest_config(
        manifest.config.dimensions,
        manifest.config.segment_max_vectors,
        manifest.routing_page_fanout,
        manifest.graph_neighbors,
    )?;
    let metric = manifest.config.metric.to_string();
    let named_vectors_json = if manifest.config.named_vectors.is_empty() {
        None
    } else {
        Some(
            serde_json::to_string(&manifest.config.named_vectors).map_err(|err| {
                BorsukError::InvalidStorage(format!(
                    "failed to serialize named vector schema: {err}"
                ))
            })?,
        )
    };
    // The leaf-capability cell is written only for the non-default
    // (`PqScanOnly`) capability. A `GraphEnabled` index leaves it null, so its
    // manifest bytes stay identical to a pre-capability index (which reloads as
    // `GraphEnabled` by default).
    let leaf_capability_json = if manifest.leaf_capability == crate::LeafCapability::GraphEnabled {
        None
    } else {
        Some(manifest.leaf_capability.as_str().to_string())
    };
    let logical_cell_routing_strategy_json = serde_json::to_string(
        &manifest
            .logical_cell_routing_strategy
            .validated_for_metric(&manifest.config.metric)?,
    )
    .map_err(|err| {
        BorsukError::InvalidStorage(format!(
            "failed to serialize logical-cell routing strategy: {err}"
        ))
    })?;
    // The WAL column is written only when the WAL is active or has a pending
    // frontier. A disabled, never-used WAL leaves the column absent, so its
    // manifest bytes are byte-for-byte identical to a pre-WAL index.
    let wal_json = wal_manifest_json(manifest)?;
    // The build-config cell is written only for a non-default config. A default
    // config leaves it absent, so its manifest bytes stay byte-for-byte identical
    // to a pre-build-config index (which reloads as the default).
    let build_config_json = build_config_manifest_json(manifest)?;
    // The quantizer-ref cell is written only when a persisted quantizer object
    // exists for this manifest. Its absence reloads as `None`, so a manifest
    // without a persisted quantizer stays byte-identical to a pre-quantizer one.
    let quantizer_ref_json = quantizer_ref_manifest_json(manifest)?;
    let global_ann_ref_json = global_ann_ref_manifest_json(manifest)?;
    let global_cell_card_ann_ref_json = manifest
        .global_cell_card_ann_ref
        .as_ref()
        .map(serde_json::to_string)
        .transpose()
        .map_err(|error| {
            BorsukError::InvalidStorage(format!(
                "failed to serialize V15 global cell-card ref: {error}"
            ))
        })?;
    let lexical_roots_json = serde_json::to_string(&manifest.lexical_roots).map_err(|err| {
        BorsukError::InvalidStorage(format!("failed to serialize lexical root refs: {err}"))
    })?;
    let bm25_stats_delta_json = manifest
        .bm25_stats_delta
        .as_ref()
        .map(serde_json::to_string)
        .transpose()
        .map_err(|err| {
            BorsukError::InvalidStorage(format!("failed to serialize BM25 statistics delta: {err}"))
        })?;
    let schema = manifest_schema_with_named_vectors_and_wal(
        named_vectors_json.is_some(),
        wal_json.is_some(),
        build_config_json.is_some(),
        quantizer_ref_json.is_some(),
    );
    let mut columns = vec![
        array(UInt16Array::from_iter_values([CURRENT_VERSION])),
        array(UInt64Array::from_iter_values([manifest.version])),
        array(StringArray::from_iter_values([manifest
            .config
            .uri
            .as_str()])),
        array(StringArray::from_iter_values([metric.as_str()])),
        array(UInt64Array::from_iter_values([
            manifest.config.dimensions as u64
        ])),
        array(UInt64Array::from_iter_values([
            manifest.config.segment_max_vectors as u64,
        ])),
        array(Int64Array::from_iter_values([manifest
            .created_at
            .timestamp_millis()])),
        array(UInt64Array::from_iter([manifest.config.ram_budget_bytes])),
        array(BooleanArray::from_iter([manifest.config.text])),
        array(StringArray::from_iter([manifest.text_tokenizer.clone()])),
        array(UInt64Array::from_iter_values([manifest.next_generated_id])),
        array(UInt8Array::from_iter_values([manifest.routing_max_level])),
        array(UInt64Array::from_iter_values([
            manifest.routing_page_fanout as u64,
        ])),
        array(UInt64Array::from_iter_values([
            manifest.graph_neighbors as u64
        ])),
        array(StringArray::from_iter([leaf_capability_json])),
        array(StringArray::from_iter_values([
            logical_cell_routing_strategy_json.as_str(),
        ])),
        array(StringArray::from_iter([manifest
            .tombstone
            .as_ref()
            .map(|tombstone| tombstone.path.clone())])),
        array(StringArray::from_iter([manifest
            .tombstone
            .as_ref()
            .map(|tombstone| tombstone.checksum.clone())])),
        array(UInt64Array::from_iter([manifest
            .tombstone
            .as_ref()
            .map(|tombstone| tombstone.count)])),
        array(BinaryArray::from_iter([manifest
            .tombstone
            .as_ref()
            .map(|tombstone| tombstone.id_bloom.as_slice())])),
        array(Int64Array::from_iter([manifest
            .tombstone
            .as_ref()
            .map(|tombstone| tombstone.created_at.timestamp_millis())])),
    ];
    if named_vectors_json.is_some() {
        columns.push(array(StringArray::from_iter([named_vectors_json])));
    }
    if wal_json.is_some() {
        columns.push(array(StringArray::from_iter([wal_json])));
    }
    if build_config_json.is_some() {
        columns.push(array(StringArray::from_iter([build_config_json])));
    }
    if quantizer_ref_json.is_some() {
        columns.push(array(StringArray::from_iter([quantizer_ref_json])));
    }
    columns.push(array(StringArray::from_iter([global_ann_ref_json])));
    columns.push(array(StringArray::from_iter([
        global_cell_card_ann_ref_json,
    ])));
    columns.push(array(StringArray::from_iter_values([
        lexical_roots_json.as_str()
    ])));
    columns.push(array(StringArray::from_iter([bm25_stats_delta_json])));
    let batch = RecordBatch::try_new(Arc::clone(&schema), columns)?;

    write_batch(batch)
}

/// Serialize the manifest's [`QuantizerRef`] for persistence, returning `None`
/// when there is no persisted quantizer so the column is omitted and the
/// manifest bytes stay byte-identical to a pre-quantizer table.
fn quantizer_ref_manifest_json(manifest: &Manifest) -> Result<Option<String>> {
    let Some(quantizer_ref) = &manifest.quantizer_ref else {
        return Ok(None);
    };
    Ok(Some(serde_json::to_string(quantizer_ref).map_err(
        |err| BorsukError::InvalidStorage(format!("failed to serialize quantizer ref: {err}")),
    )?))
}

/// Parse the optional [`QuantizerRef`] from a manifest batch. An absent column
/// (pre-quantizer tables) or a null cell yields `None`.
fn manifest_quantizer_ref(batch: &RecordBatch) -> Result<Option<crate::manifest::QuantizerRef>> {
    let Ok(column) = batch.schema().index_of("quantizer_ref_json") else {
        return Ok(None);
    };
    if batch.column(column).is_null(0) {
        return Ok(None);
    }
    let json = string_value(batch, column, 0, "quantizer_ref_json")?;
    serde_json::from_str(json)
        .map(Some)
        .map_err(|err| BorsukError::InvalidStorage(format!("failed to parse quantizer ref: {err}")))
}

fn global_ann_ref_manifest_json(manifest: &Manifest) -> Result<Option<String>> {
    let Some(global_ann_ref) = &manifest.global_ann_ref else {
        return Ok(None);
    };
    Ok(Some(serde_json::to_string(global_ann_ref).map_err(
        |err| BorsukError::InvalidStorage(format!("failed to serialize global ANN ref: {err}")),
    )?))
}

pub(crate) fn decode_global_ann_ref_json(
    json: &str,
) -> Result<crate::global_leaf_run::GlobalAnnRef> {
    let value: serde_json::Value = serde_json::from_str(json).map_err(|err| {
        BorsukError::InvalidStorage(format!(
            "failed to parse global ANN ref; rebuild the unreleased index: {err}"
        ))
    })?;
    if let Some(layout_version) = value
        .get("layout_version")
        .and_then(serde_json::Value::as_u64)
        && layout_version != u64::from(crate::global_leaf_run::GLOBAL_PQ_REF_LAYOUT_VERSION)
    {
        return Err(BorsukError::InvalidStorage(format!(
            "unsupported global ANN reference layout version {layout_version}; rebuild the unreleased index"
        )));
    }
    let reference: crate::global_leaf_run::GlobalAnnRef =
        serde_json::from_value(value).map_err(|err| {
            BorsukError::InvalidStorage(format!(
                "failed to parse global ANN ref; rebuild the unreleased index: {err}"
            ))
        })?;
    reference.validate()?;
    Ok(reference)
}

fn manifest_global_ann_ref(
    batch: &RecordBatch,
) -> Result<Option<crate::global_leaf_run::GlobalAnnRef>> {
    if let Ok(legacy_column) = batch.schema().index_of("global_pq_ref_json")
        && !batch.column(legacy_column).is_null(0)
    {
        return Err(BorsukError::InvalidStorage(
            "unsupported global ANN reference layout version 10; rebuild the unreleased index"
                .to_string(),
        ));
    }
    let column = batch
        .schema()
        .index_of("global_ann_ref_json")
        .map_err(|_| {
            BorsukError::InvalidStorage(
            "manifest is missing required global_ann_ref_json column; rebuild the unreleased index"
                .to_string(),
        )
        })?;
    if batch.column(column).is_null(0) {
        return Ok(None);
    }
    let json = string_value(batch, column, 0, "global_ann_ref_json")?;
    decode_global_ann_ref_json(json).map(Some)
}

fn manifest_global_cell_card_ann_ref(
    batch: &RecordBatch,
) -> Result<Option<crate::global_cell_card::GlobalCellCardAnnRef>> {
    let column = batch
        .schema()
        .index_of("global_cell_card_ann_ref_json")
        .map_err(|_| {
            BorsukError::InvalidStorage(
                "manifest is missing required global_cell_card_ann_ref_json column; rebuild the unreleased index"
                    .to_string(),
            )
        })?;
    if batch.column(column).is_null(0) {
        return Ok(None);
    }
    let reference: crate::global_cell_card::GlobalCellCardAnnRef = serde_json::from_str(
        string_value(batch, column, 0, "global_cell_card_ann_ref_json")?,
    )
    .map_err(|error| {
        BorsukError::InvalidStorage(format!("failed to parse V15 global cell-card ref: {error}"))
    })?;
    reference.validate()?;
    Ok(Some(reference))
}

fn validate_manifest_global_ann_authority(manifest: &Manifest) -> Result<()> {
    if manifest.global_ann_ref.is_some() && manifest.global_cell_card_ann_ref.is_some() {
        return Err(BorsukError::InvalidStorage(
            "manifest contains both V13 and V15 global ANN authority".to_string(),
        ));
    }
    Ok(())
}

fn manifest_lexical_roots(batch: &RecordBatch) -> Result<Vec<crate::manifest::LexicalRootRef>> {
    let column = batch.schema().index_of("lexical_roots_json").map_err(|_| {
        BorsukError::InvalidStorage(
            "manifest is missing required lexical_roots_json column; rebuild the unreleased index"
                .to_string(),
        )
    })?;
    if batch.column(column).is_null(0) {
        return Err(BorsukError::InvalidStorage(
            "manifest lexical_roots_json must not be null".to_string(),
        ));
    }
    serde_json::from_str(string_value(batch, column, 0, "lexical_roots_json")?).map_err(|err| {
        BorsukError::InvalidStorage(format!("failed to parse lexical root refs: {err}"))
    })
}

fn manifest_bm25_stats_delta(
    batch: &RecordBatch,
) -> Result<Option<crate::manifest::Bm25StatsDeltaRef>> {
    let column = batch
        .schema()
        .index_of("bm25_stats_delta_json")
        .map_err(|_| {
            BorsukError::InvalidStorage(
                "manifest is missing required bm25_stats_delta_json column; recreate the unreleased index"
                    .to_string(),
            )
        })?;
    if batch.column(column).is_null(0) {
        return Ok(None);
    }
    serde_json::from_str(string_value(batch, column, 0, "bm25_stats_delta_json")?)
        .map(Some)
        .map_err(|err| {
            BorsukError::InvalidStorage(format!("failed to parse BM25 statistics delta: {err}"))
        })
}

/// Serialize the manifest's [`BuildConfig`] for persistence, returning `None`
/// for the historical config so legacy-compatible indexes can still omit the
/// column. The current default is serialized because it enables normalized
/// angular coarse geometry and must be distinguishable from older raw codes.
fn build_config_manifest_json(manifest: &Manifest) -> Result<Option<String>> {
    if manifest.build_config == crate::BuildConfig::legacy_default() {
        return Ok(None);
    }
    Ok(Some(
        serde_json::to_string(&manifest.build_config).map_err(|err| {
            BorsukError::InvalidStorage(format!("failed to serialize build config: {err}"))
        })?,
    ))
}

/// Parse the optional [`BuildConfig`] from a manifest batch. An absent column
/// (pre-build-config tables) or a null cell yields historical defaults.
fn manifest_build_config(batch: &RecordBatch) -> Result<crate::BuildConfig> {
    let Ok(column) = batch.schema().index_of("build_config_json") else {
        return Ok(crate::BuildConfig::legacy_default());
    };
    if batch.column(column).is_null(0) {
        return Ok(crate::BuildConfig::legacy_default());
    }
    let json = string_value(batch, column, 0, "build_config_json")?;
    serde_json::from_str(json)
        .map_err(|err| BorsukError::InvalidStorage(format!("failed to parse build config: {err}")))
}

/// Serialized mutation/catalog region of a manifest.
#[derive(serde::Serialize, serde::Deserialize)]
struct WalManifestJson {
    config: crate::manifest::WalConfig,
    #[serde(default = "default_routing_epoch")]
    routing_epoch: u64,
    #[serde(default)]
    cell_wal_config: crate::CellWalConfig,
    logical_cell_catalog_ref: Option<crate::logical_cell_catalog::LogicalCellCatalogRef>,
    #[serde(default)]
    cell_wal_consumed_runs: BTreeSet<String>,
    #[serde(default)]
    tombstone_frontier: Vec<crate::manifest::TombstoneSummary>,
    #[serde(default)]
    bm25_stats_delta_frontier: Vec<crate::manifest::Bm25StatsDeltaRef>,
    #[serde(default)]
    tombstone_id_count: u64,
    #[serde(default)]
    tombstone_pages: Vec<crate::manifest::TombstonePageRef>,
}

type ManifestWalState = (
    crate::manifest::WalConfig,
    u64,
    crate::CellWalConfig,
    Option<crate::logical_cell_catalog::LogicalCellCatalogRef>,
    BTreeSet<String>,
    Vec<crate::manifest::TombstoneSummary>,
    Vec<crate::manifest::Bm25StatsDeltaRef>,
    u64,
    Vec<crate::manifest::TombstonePageRef>,
);

const fn default_routing_epoch() -> u64 {
    1
}

fn wal_manifest_json(manifest: &Manifest) -> Result<Option<String>> {
    // The production catalog always persists its routing epoch and cell-lane
    // policy, even when every value equals the current default. BORSUK is
    // unreleased, so old byte-equivalence is intentionally not a constraint.
    let payload = WalManifestJson {
        config: manifest.wal_config.clone(),
        routing_epoch: manifest.routing_epoch,
        cell_wal_config: manifest.cell_wal_config,
        logical_cell_catalog_ref: manifest.logical_cell_catalog_ref.clone(),
        cell_wal_consumed_runs: manifest.cell_wal_consumed_runs.clone(),
        tombstone_frontier: manifest.tombstone_frontier.clone(),
        bm25_stats_delta_frontier: manifest.bm25_stats_delta_frontier.clone(),
        tombstone_id_count: manifest.tombstone_id_count,
        tombstone_pages: manifest.tombstone_pages.clone(),
    };
    Ok(Some(serde_json::to_string(&payload).map_err(|err| {
        BorsukError::InvalidStorage(format!("failed to serialize WAL manifest region: {err}"))
    })?))
}

/// Parse the cell-WAL catalog and consolidated mutation state.
fn manifest_wal(batch: &RecordBatch) -> Result<ManifestWalState> {
    let Ok(column) = batch.schema().index_of("wal_json") else {
        return Ok((
            crate::manifest::WalConfig::default(),
            default_routing_epoch(),
            crate::CellWalConfig::default(),
            None,
            BTreeSet::new(),
            Vec::new(),
            Vec::new(),
            0,
            Vec::new(),
        ));
    };
    if batch.column(column).is_null(0) {
        return Ok((
            crate::manifest::WalConfig::default(),
            default_routing_epoch(),
            crate::CellWalConfig::default(),
            None,
            BTreeSet::new(),
            Vec::new(),
            Vec::new(),
            0,
            Vec::new(),
        ));
    }
    let json = string_value(batch, column, 0, "wal_json")?;
    let payload: WalManifestJson = serde_json::from_str(json).map_err(|err| {
        BorsukError::InvalidStorage(format!("failed to parse WAL manifest region: {err}"))
    })?;
    Ok((
        payload.config,
        payload.routing_epoch,
        payload.cell_wal_config,
        payload.logical_cell_catalog_ref,
        payload.cell_wal_consumed_runs,
        payload.tombstone_frontier,
        payload.bm25_stats_delta_frontier,
        payload.tombstone_id_count,
        payload.tombstone_pages,
    ))
}

/// Parse the optional tombstone summary from a manifest table batch. Absent
/// columns (older tables) or a null path both mean "no deletions".
fn manifest_tombstone(batch: &RecordBatch) -> Result<Option<crate::manifest::TombstoneSummary>> {
    let Ok(index) = batch.schema().index_of("tombstone_path") else {
        return Ok(None);
    };
    if batch.column(index).is_null(0) {
        return Ok(None);
    }
    Ok(Some(crate::manifest::TombstoneSummary {
        path: string_value_by_name(batch, 0, "tombstone_path")?.to_string(),
        checksum: string_value_by_name(batch, 0, "tombstone_checksum")?.to_string(),
        count: primitive_value_by_name::<UInt64Type>(batch, 0, "tombstone_count")?,
        id_bloom: binary_value_by_name(batch, 0, "tombstone_id_bloom")?.to_vec(),
        created_at: datetime_from_millis(primitive_value_by_name::<Int64Type>(
            batch,
            0,
            "tombstone_created_at_ms",
        )?)?,
    }))
}

fn manifest_leaf_capability(batch: &RecordBatch) -> Result<crate::LeafCapability> {
    let Ok(column) = batch.schema().index_of("leaf_capability") else {
        return Ok(crate::LeafCapability::GraphEnabled);
    };
    if batch.column(column).is_null(0) {
        return Ok(crate::LeafCapability::GraphEnabled);
    }
    let value = string_value(batch, column, 0, "leaf_capability")?;
    crate::LeafCapability::from_str(value)
}

fn manifest_catalog_routing_strategy(
    batch: &RecordBatch,
    metric: &VectorMetric,
) -> Result<crate::centroid_hnsw::CatalogRoutingStrategy> {
    let json = string_value_by_name(batch, 0, "logical_cell_routing_strategy_json")?;
    let strategy = serde_json::from_str::<crate::centroid_hnsw::CatalogRoutingStrategy>(json)
        .map_err(|err| {
            BorsukError::InvalidStorage(format!(
                "failed to parse logical-cell routing strategy: {err}"
            ))
        })?;
    strategy.validated_for_metric(metric)
}

fn manifest_text_enabled(batch: &RecordBatch) -> Result<bool> {
    let Ok(column) = batch.schema().index_of("text_enabled") else {
        return Ok(false);
    };
    if batch.column(column).is_null(0) {
        return Ok(false);
    }
    boolean_value(batch, column, 0, "text_enabled")
}

fn manifest_text_tokenizer(batch: &RecordBatch) -> Result<Option<String>> {
    let Ok(column) = batch.schema().index_of("text_tokenizer") else {
        return Ok(None);
    };
    if batch.column(column).is_null(0) {
        return Ok(None);
    }
    Ok(Some(
        string_value(batch, column, 0, "text_tokenizer")?.to_string(),
    ))
}

fn manifest_named_vectors(
    batch: &RecordBatch,
) -> Result<BTreeMap<String, crate::record::VectorSpec>> {
    let Ok(column) = batch.schema().index_of("named_vectors_json") else {
        return Ok(BTreeMap::new());
    };
    if batch.column(column).is_null(0) {
        return Ok(BTreeMap::new());
    }
    let json = string_value(batch, column, 0, "named_vectors_json")?;
    serde_json::from_str(json).map_err(|err| {
        BorsukError::InvalidStorage(format!("failed to parse named vector schema: {err}"))
    })
}

pub(crate) fn manifest_from_parquet(
    manifest_bytes: &[u8],
    routing_bytes: &[u8],
) -> Result<Manifest> {
    let batch = first_batch(manifest_bytes, "manifest")?;
    if batch.num_rows() != 1 {
        return Err(BorsukError::InvalidStorage(format!(
            "manifest table must contain one row, got {}",
            batch.num_rows()
        )));
    }

    let format_version = primitive_value_by_name::<UInt16Type>(&batch, 0, "format_version")?;
    if format_version != CURRENT_VERSION {
        return Err(BorsukError::InvalidStorage(format!(
            "unsupported manifest table version {format_version}"
        )));
    }

    let manifest_version = primitive_value_by_name::<UInt64Type>(&batch, 0, "version")?;
    let metric = VectorMetric::from_str(string_value_by_name(&batch, 0, "metric")?)?;
    let logical_cell_routing_strategy = manifest_catalog_routing_strategy(&batch, &metric)?;
    let segments = routing_from_parquet(routing_bytes, manifest_version)?;
    let dimensions = usize_from_u64(primitive_value_by_name::<UInt64Type>(
        &batch,
        0,
        "dimensions",
    )?)?;
    let segment_max_vectors = usize_from_u64(primitive_value_by_name::<UInt64Type>(
        &batch,
        0,
        "segment_max_vectors",
    )?)?;
    let routing_page_fanout = manifest_routing_page_fanout(&batch)?;
    let graph_neighbors = manifest_graph_neighbors(&batch)?;
    validate_manifest_config(
        dimensions,
        segment_max_vectors,
        routing_page_fanout,
        graph_neighbors,
    )?;
    let next_generated_id = if batch.schema().field_with_name("next_generated_id").is_ok() {
        primitive_value_by_name::<UInt64Type>(&batch, 0, "next_generated_id")?
    } else {
        segments.iter().try_fold(0_u64, |total, segment| {
            let count = u64::try_from(segment.object_count).map_err(|_| {
                BorsukError::InvalidStorage(format!(
                    "segment `{}` object_count does not fit u64",
                    segment.id
                ))
            })?;
            total.checked_add(count).ok_or_else(|| {
                BorsukError::InvalidStorage("stored segment object counts exceed u64".to_string())
            })
        })?
    };
    let (
        wal_config,
        routing_epoch,
        cell_wal_config,
        logical_cell_catalog_ref,
        cell_wal_consumed_runs,
        tombstone_frontier,
        bm25_stats_delta_frontier,
        tombstone_id_count,
        tombstone_pages,
    ) = manifest_wal(&batch)?;
    let manifest = Manifest {
        version: manifest_version,
        config: IndexConfig {
            uri: string_value_by_name(&batch, 0, "uri")?.to_string(),
            metric,
            dimensions,
            segment_max_vectors,
            ram_budget_bytes: if batch.schema().field_with_name("ram_budget_bytes").is_ok() {
                primitive_optional_value_by_name::<UInt64Type>(&batch, 0, "ram_budget_bytes")?
            } else {
                None
            },
            text: manifest_text_enabled(&batch)?,
            named_vectors: manifest_named_vectors(&batch)?,
        },
        text_tokenizer: manifest_text_tokenizer(&batch)?,
        segments,
        pivots: Vec::new(),
        next_generated_id,
        routing_max_level: manifest_routing_max_level(&batch)?,
        routing_page_fanout,
        graph_neighbors,
        leaf_capability: manifest_leaf_capability(&batch)?,
        build_config: manifest_build_config(&batch)?,
        tombstone: manifest_tombstone(&batch)?,
        tombstone_frontier,
        tombstone_pages,
        tombstone_id_count,
        wal_config,
        routing_epoch,
        cell_wal_config,
        logical_cell_catalog_ref,
        logical_cell_routing_strategy,
        logical_cell_catalog: None,
        logical_cell_router: None,
        cell_wal_consumed_runs,
        cell_wal_visible_runs: 0,
        cell_wal_visible_tombstone_runs: 0,
        quantizer_ref: manifest_quantizer_ref(&batch)?,
        global_ann_ref: manifest_global_ann_ref(&batch)?,
        global_cell_card_ann_ref: manifest_global_cell_card_ann_ref(&batch)?,
        lexical_roots: manifest_lexical_roots(&batch)?,
        bm25_stats_delta: manifest_bm25_stats_delta(&batch)?,
        bm25_stats_delta_frontier,
        created_at: datetime_from_millis(primitive_value_by_name::<Int64Type>(
            &batch,
            0,
            "created_at_ms",
        )?)?,
    };
    for segment in &manifest.segments {
        validate_routing_segment_dimensions(
            &segment.id,
            manifest.config.dimensions,
            segment.dimensions,
        )?;
    }
    validate_manifest_global_ann_authority(&manifest)?;

    Ok(manifest)
}

pub(crate) fn manifest_metadata_from_parquet(manifest_bytes: &[u8]) -> Result<Manifest> {
    let batch = first_batch(manifest_bytes, "manifest")?;
    if batch.num_rows() != 1 {
        return Err(BorsukError::InvalidStorage(format!(
            "manifest table must contain one row, got {}",
            batch.num_rows()
        )));
    }

    let format_version = primitive_value_by_name::<UInt16Type>(&batch, 0, "format_version")?;
    if format_version != CURRENT_VERSION {
        return Err(BorsukError::InvalidStorage(format!(
            "unsupported manifest table version {format_version}"
        )));
    }

    let dimensions = usize_from_u64(primitive_value_by_name::<UInt64Type>(
        &batch,
        0,
        "dimensions",
    )?)?;
    let segment_max_vectors = usize_from_u64(primitive_value_by_name::<UInt64Type>(
        &batch,
        0,
        "segment_max_vectors",
    )?)?;
    let routing_page_fanout = manifest_routing_page_fanout(&batch)?;
    let graph_neighbors = manifest_graph_neighbors(&batch)?;
    validate_manifest_config(
        dimensions,
        segment_max_vectors,
        routing_page_fanout,
        graph_neighbors,
    )?;
    let (
        wal_config,
        routing_epoch,
        cell_wal_config,
        logical_cell_catalog_ref,
        cell_wal_consumed_runs,
        tombstone_frontier,
        bm25_stats_delta_frontier,
        tombstone_id_count,
        tombstone_pages,
    ) = manifest_wal(&batch)?;

    let metric = VectorMetric::from_str(string_value_by_name(&batch, 0, "metric")?)?;
    let logical_cell_routing_strategy = manifest_catalog_routing_strategy(&batch, &metric)?;

    let manifest = Manifest {
        version: primitive_value_by_name::<UInt64Type>(&batch, 0, "version")?,
        config: IndexConfig {
            uri: string_value_by_name(&batch, 0, "uri")?.to_string(),
            metric,
            dimensions,
            segment_max_vectors,
            ram_budget_bytes: if batch.schema().field_with_name("ram_budget_bytes").is_ok() {
                primitive_optional_value_by_name::<UInt64Type>(&batch, 0, "ram_budget_bytes")?
            } else {
                None
            },
            text: manifest_text_enabled(&batch)?,
            named_vectors: manifest_named_vectors(&batch)?,
        },
        text_tokenizer: manifest_text_tokenizer(&batch)?,
        segments: Vec::new(),
        pivots: Vec::new(),
        next_generated_id: if batch.schema().field_with_name("next_generated_id").is_ok() {
            primitive_value_by_name::<UInt64Type>(&batch, 0, "next_generated_id")?
        } else {
            0
        },
        routing_max_level: manifest_routing_max_level(&batch)?,
        routing_page_fanout,
        graph_neighbors,
        leaf_capability: manifest_leaf_capability(&batch)?,
        build_config: manifest_build_config(&batch)?,
        tombstone: manifest_tombstone(&batch)?,
        tombstone_frontier,
        tombstone_pages,
        tombstone_id_count,
        wal_config,
        routing_epoch,
        cell_wal_config,
        logical_cell_catalog_ref,
        logical_cell_routing_strategy,
        logical_cell_catalog: None,
        logical_cell_router: None,
        cell_wal_consumed_runs,
        cell_wal_visible_runs: 0,
        cell_wal_visible_tombstone_runs: 0,
        quantizer_ref: manifest_quantizer_ref(&batch)?,
        global_ann_ref: manifest_global_ann_ref(&batch)?,
        global_cell_card_ann_ref: manifest_global_cell_card_ann_ref(&batch)?,
        lexical_roots: manifest_lexical_roots(&batch)?,
        bm25_stats_delta: manifest_bm25_stats_delta(&batch)?,
        bm25_stats_delta_frontier,
        created_at: datetime_from_millis(primitive_value_by_name::<Int64Type>(
            &batch,
            0,
            "created_at_ms",
        )?)?,
    };
    validate_manifest_global_ann_authority(&manifest)?;
    Ok(manifest)
}

pub(crate) fn manifest_has_next_generated_id(manifest_bytes: &[u8]) -> Result<bool> {
    let batch = first_batch(manifest_bytes, "manifest")?;
    Ok(batch.schema().field_with_name("next_generated_id").is_ok())
}

fn manifest_routing_max_level(batch: &RecordBatch) -> Result<u8> {
    let Ok(column_index) = batch.schema().index_of("routing_max_level") else {
        return Ok(0);
    };
    primitive_value::<UInt8Type>(batch, column_index, 0, "routing_max_level")
}

fn manifest_routing_page_fanout(batch: &RecordBatch) -> Result<usize> {
    let Ok(column_index) = batch.schema().index_of("routing_page_fanout") else {
        return Ok(DEFAULT_ROUTING_PAGE_FANOUT);
    };
    usize_from_u64(primitive_value::<UInt64Type>(
        batch,
        column_index,
        0,
        "routing_page_fanout",
    )?)
}

fn manifest_graph_neighbors(batch: &RecordBatch) -> Result<usize> {
    let Ok(column_index) = batch.schema().index_of("graph_neighbors") else {
        return Ok(DEFAULT_GRAPH_NEIGHBORS);
    };
    usize_from_u64(primitive_value::<UInt64Type>(
        batch,
        column_index,
        0,
        "graph_neighbors",
    )?)
}

pub(crate) fn routing_to_parquet(manifest: &Manifest) -> Result<Vec<u8>> {
    let dimensions = manifest.config.dimensions;
    let schema = routing_schema(dimensions);
    let segments = &manifest.segments;
    validate_routing_segment_ids(segments)?;
    validate_routing_segment_paths(segments)?;
    validate_routing_segment_summary_metadata(segments)?;
    for segment in segments {
        validate_routing_segment_dimensions(&segment.id, dimensions, segment.dimensions)?;
        validate_routing_centroid_dimensions(&segment.id, dimensions, segment.centroid.len())?;
        validate_routing_centroid_values(&segment.id, &segment.centroid)?;
        validate_routing_radius(&segment.id, segment.radius)?;
        validate_routing_bounds(
            &segment.id,
            dimensions,
            &segment.bounds_min,
            &segment.bounds_max,
        )?;
        validate_routing_id_bloom(&segment.id, &segment.id_bloom)?;
        validate_routing_vector_signature_bloom(&segment.id, &segment.vector_signature_bloom)?;
    }
    let lexical_shards_json = segments
        .iter()
        .map(|segment| {
            serde_json::to_string(&segment.lexical_shards).map_err(|err| {
                BorsukError::InvalidStorage(format!(
                    "failed to serialize segment lexical shard refs: {err}"
                ))
            })
        })
        .collect::<Result<Vec<_>>>()?;

    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            array(UInt16Array::from_iter_values(
                segments.iter().map(|_| CURRENT_VERSION),
            )),
            array(UInt64Array::from_iter_values(
                segments.iter().map(|_| manifest.version),
            )),
            array(StringArray::from_iter_values(
                segments.iter().map(|segment| segment.id.as_str()),
            )),
            array(UInt8Array::from_iter_values(
                segments.iter().map(|segment| segment.level),
            )),
            array(StringArray::from_iter_values(
                segments.iter().map(|segment| segment.path.as_str()),
            )),
            array(StringArray::from_iter_values(segments.iter().map(
                |segment| {
                    serde_json::to_string(&segment.layout)
                        .expect("physical layout reference is JSON serializable")
                },
            ))),
            array(UInt64Array::from_iter_values(
                segments.iter().map(|segment| segment.object_count as u64),
            )),
            array(UInt64Array::from_iter_values(
                segments.iter().map(|segment| segment.dimensions as u64),
            )),
            array(fixed_f32_array(
                segments.iter().map(|segment| segment.centroid.as_slice()),
                dimensions,
            )),
            array(Float32Array::from_iter_values(
                segments.iter().map(|segment| segment.radius),
            )),
            array(StringArray::from_iter_values(
                segments.iter().map(|segment| segment.checksum.as_str()),
            )),
            array(UInt64Array::from_iter_values(
                segments.iter().map(|segment| segment.size_bytes),
            )),
            array(UInt64Array::from_iter_values(
                segments.iter().map(|segment| segment.vector_size_bytes),
            )),
            array(StringArray::from_iter_values(
                segments.iter().map(|segment| segment.graph_path.as_str()),
            )),
            array(StringArray::from_iter_values(
                segments
                    .iter()
                    .map(|segment| segment.graph_checksum.as_str()),
            )),
            array(UInt64Array::from_iter_values(
                segments.iter().map(|segment| segment.graph_size_bytes),
            )),
            array(Int64Array::from_iter_values(
                segments
                    .iter()
                    .map(|segment| segment.created_at.timestamp_millis()),
            )),
            array(BinaryArray::from_iter_values(
                segments.iter().map(|segment| segment.id_bloom.as_slice()),
            )),
            array(StringArray::from_iter_values(
                segments.iter().map(|segment| segment.leaf_mode.to_string()),
            )),
            array(BinaryArray::from_iter_values(
                segments
                    .iter()
                    .map(|segment| segment.vector_signature_bloom.as_slice()),
            )),
            array(fixed_f32_array(
                segments.iter().map(|segment| segment.bounds_min.as_slice()),
                dimensions,
            )),
            array(fixed_f32_array(
                segments.iter().map(|segment| segment.bounds_max.as_slice()),
                dimensions,
            )),
            array(BinaryArray::from_iter_values(
                segments
                    .iter()
                    .map(|segment| segment.metadata_stats.to_bytes()),
            )),
            array(UInt32Array::from_iter_values(
                segments.iter().map(|segment| segment.text_doc_count),
            )),
            array(UInt64Array::from_iter_values(
                segments.iter().map(|segment| segment.text_total_doc_length),
            )),
            array(UInt64Array::from_iter_values(
                segments
                    .iter()
                    .map(|segment| segment.text_lexical_decoded_bytes),
            )),
            array(UInt64Array::from_iter_values(
                segments
                    .iter()
                    .map(|segment| segment.sparse_lexical_max_decoded_bytes),
            )),
            array(StringArray::from_iter_values(
                lexical_shards_json.iter().map(String::as_str),
            )),
            array(UInt64Array::from_iter_values(
                segments.iter().map(|segment| segment.sparse_encoded as u64),
            )),
            array(UInt64Array::from_iter_values(
                segments.iter().map(|segment| segment.dense_encoded as u64),
            )),
        ],
    )?;

    write_batch(batch)
}

pub(crate) fn routing_layer_page_to_parquet(
    manifest: &Manifest,
    routing_level: u8,
    page_ordinal: usize,
    segment_start_ordinal: usize,
    segments: &[SegmentSummary],
) -> Result<Vec<u8>> {
    let dimensions = manifest.config.dimensions;
    let schema = routing_layer_page_schema(dimensions);
    validate_routing_segment_ids(segments)?;
    validate_routing_segment_paths(segments)?;
    validate_routing_segment_summary_metadata(segments)?;
    for segment in segments {
        validate_routing_segment_dimensions(&segment.id, dimensions, segment.dimensions)?;
        validate_routing_centroid_dimensions(&segment.id, dimensions, segment.centroid.len())?;
        validate_routing_centroid_values(&segment.id, &segment.centroid)?;
        validate_routing_radius(&segment.id, segment.radius)?;
        validate_routing_bounds(
            &segment.id,
            dimensions,
            &segment.bounds_min,
            &segment.bounds_max,
        )?;
        validate_routing_id_bloom(&segment.id, &segment.id_bloom)?;
        validate_routing_vector_signature_bloom(&segment.id, &segment.vector_signature_bloom)?;
    }
    let lexical_shards_json = segments
        .iter()
        .map(|segment| {
            serde_json::to_string(&segment.lexical_shards).map_err(|err| {
                BorsukError::InvalidStorage(format!(
                    "failed to serialize segment lexical shard refs: {err}"
                ))
            })
        })
        .collect::<Result<Vec<_>>>()?;

    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            array(UInt16Array::from_iter_values(
                segments.iter().map(|_| CURRENT_VERSION),
            )),
            array(UInt64Array::from_iter_values(segments.iter().map(|_| 0))),
            array(UInt8Array::from_iter_values(
                segments.iter().map(|_| routing_level),
            )),
            array(UInt64Array::from_iter_values(
                segments.iter().map(|_| page_ordinal as u64),
            )),
            array(UInt64Array::from_iter_values(
                segments.iter().map(|_| segments.len() as u64),
            )),
            array(UInt64Array::from_iter_values(
                segments
                    .iter()
                    .enumerate()
                    .map(|(index, _)| (segment_start_ordinal + index) as u64),
            )),
            array(StringArray::from_iter_values(
                segments.iter().map(|segment| segment.id.as_str()),
            )),
            array(UInt8Array::from_iter_values(
                segments.iter().map(|segment| segment.level),
            )),
            array(UInt64Array::from_iter_values(
                segments.iter().map(|segment| segment.object_count as u64),
            )),
            array(UInt64Array::from_iter_values(
                segments.iter().map(|segment| segment.dimensions as u64),
            )),
            array(fixed_f32_array(
                segments.iter().map(|segment| segment.centroid.as_slice()),
                dimensions,
            )),
            array(Float32Array::from_iter_values(
                segments.iter().map(|segment| segment.radius),
            )),
            array(StringArray::from_iter_values(
                segments.iter().map(|segment| segment.path.as_str()),
            )),
            array(StringArray::from_iter_values(segments.iter().map(
                |segment| {
                    serde_json::to_string(&segment.layout)
                        .expect("physical layout reference is JSON serializable")
                },
            ))),
            array(StringArray::from_iter_values(
                segments.iter().map(|segment| segment.checksum.as_str()),
            )),
            array(UInt64Array::from_iter_values(
                segments.iter().map(|segment| segment.size_bytes),
            )),
            array(UInt64Array::from_iter_values(
                segments.iter().map(|segment| segment.vector_size_bytes),
            )),
            array(StringArray::from_iter_values(
                segments.iter().map(|segment| segment.graph_path.as_str()),
            )),
            array(StringArray::from_iter_values(
                segments
                    .iter()
                    .map(|segment| segment.graph_checksum.as_str()),
            )),
            array(UInt64Array::from_iter_values(
                segments.iter().map(|segment| segment.graph_size_bytes),
            )),
            array(BinaryArray::from_iter_values(
                segments.iter().map(|segment| segment.id_bloom.as_slice()),
            )),
            array(StringArray::from_iter_values(
                segments.iter().map(|segment| segment.leaf_mode.to_string()),
            )),
            array(BinaryArray::from_iter_values(
                segments
                    .iter()
                    .map(|segment| segment.vector_signature_bloom.as_slice()),
            )),
            array(Int64Array::from_iter_values(
                segments
                    .iter()
                    .map(|segment| segment.created_at.timestamp_millis()),
            )),
            array(fixed_f32_array(
                segments.iter().map(|segment| segment.bounds_min.as_slice()),
                dimensions,
            )),
            array(fixed_f32_array(
                segments.iter().map(|segment| segment.bounds_max.as_slice()),
                dimensions,
            )),
            array(BinaryArray::from_iter_values(
                segments
                    .iter()
                    .map(|segment| segment.metadata_stats.to_bytes()),
            )),
            array(UInt32Array::from_iter_values(
                segments.iter().map(|segment| segment.text_doc_count),
            )),
            array(UInt64Array::from_iter_values(
                segments.iter().map(|segment| segment.text_total_doc_length),
            )),
            array(UInt64Array::from_iter_values(
                segments
                    .iter()
                    .map(|segment| segment.text_lexical_decoded_bytes),
            )),
            array(UInt64Array::from_iter_values(
                segments
                    .iter()
                    .map(|segment| segment.sparse_lexical_max_decoded_bytes),
            )),
            array(StringArray::from_iter_values(
                lexical_shards_json.iter().map(String::as_str),
            )),
            array(UInt64Array::from_iter_values(
                segments.iter().map(|segment| segment.sparse_encoded as u64),
            )),
            array(UInt64Array::from_iter_values(
                segments.iter().map(|segment| segment.dense_encoded as u64),
            )),
        ],
    )?;

    write_batch(batch)
}

pub(crate) fn routing_layer_page_index_to_parquet(
    manifest: &Manifest,
    routing_level: u8,
    page_refs: &[RoutingLayerPageRef],
) -> Result<Vec<u8>> {
    routing_layer_page_index_to_parquet_with_manifest_version(
        manifest,
        manifest.version,
        routing_level,
        page_refs,
    )
}

pub(crate) fn routing_parent_page_to_parquet(
    manifest: &Manifest,
    routing_level: u8,
    page_refs: &[RoutingLayerPageRef],
) -> Result<Vec<u8>> {
    routing_layer_page_index_to_parquet_with_manifest_version(manifest, 0, routing_level, page_refs)
}

fn routing_layer_page_index_to_parquet_with_manifest_version(
    manifest: &Manifest,
    encoded_manifest_version: u64,
    routing_level: u8,
    page_refs: &[RoutingLayerPageRef],
) -> Result<Vec<u8>> {
    validate_routing_layer_page_refs(page_refs)?;

    let schema = routing_layer_page_index_schema(manifest.config.dimensions);
    for page_ref in page_refs {
        validate_routing_segment_dimensions(
            "routing-layer-page",
            manifest.config.dimensions,
            page_ref.dimensions,
        )?;
        validate_routing_bounds(
            "routing-layer-page",
            manifest.config.dimensions,
            &page_ref.bounds_min,
            &page_ref.bounds_max,
        )?;
    }
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            array(UInt16Array::from_iter_values(
                page_refs.iter().map(|_| CURRENT_VERSION),
            )),
            array(UInt64Array::from_iter_values(
                page_refs.iter().map(|_| encoded_manifest_version),
            )),
            array(UInt8Array::from_iter_values(
                page_refs.iter().map(|_| routing_level),
            )),
            array(UInt64Array::from_iter_values(
                page_refs
                    .iter()
                    .map(|page_ref| page_ref.page_ordinal as u64),
            )),
            array(StringArray::from_iter_values(
                page_refs.iter().map(|page_ref| page_ref.path.as_str()),
            )),
            array(StringArray::from_iter_values(
                page_refs.iter().map(|page_ref| page_ref.checksum.as_str()),
            )),
            array(UInt64Array::from_iter_values(
                page_refs
                    .iter()
                    .map(|page_ref| page_ref.page_segments as u64),
            )),
            array(UInt64Array::from_iter_values(
                page_refs
                    .iter()
                    .map(|page_ref| page_ref.leaf_segments as u64),
            )),
            array(UInt64Array::from_iter_values(
                page_refs.iter().map(|page_ref| page_ref.leaf_pages as u64),
            )),
            array(UInt64Array::from_iter_values(
                page_refs
                    .iter()
                    .map(|page_ref| page_ref.routing_pages as u64),
            )),
            array(UInt64Array::from_iter_values(
                page_refs.iter().map(|page_ref| page_ref.dimensions as u64),
            )),
            array(fixed_f32_array(
                page_refs
                    .iter()
                    .map(|page_ref| page_ref.centroid.as_slice()),
                manifest.config.dimensions,
            )),
            array(Float32Array::from_iter_values(
                page_refs.iter().map(|page_ref| page_ref.radius),
            )),
            array(BinaryArray::from_iter_values(
                page_refs
                    .iter()
                    .map(|page_ref| page_ref.id_bloom.as_slice()),
            )),
            array(BinaryArray::from_iter_values(
                page_refs
                    .iter()
                    .map(|page_ref| page_ref.vector_signature_bloom.as_slice()),
            )),
            array(UInt64Array::from_iter_values(
                page_refs.iter().map(|page_ref| page_ref.level_mask),
            )),
            array(UInt64Array::from_iter_values(
                page_refs
                    .iter()
                    .map(|page_ref| page_ref.page_records as u64),
            )),
            array(UInt64Array::from_iter_values(
                page_refs.iter().map(|page_ref| page_ref.page_segment_bytes),
            )),
            array(UInt64Array::from_iter_values(
                page_refs.iter().map(|page_ref| page_ref.page_vector_bytes),
            )),
            array(UInt64Array::from_iter_values(
                page_refs.iter().map(|page_ref| page_ref.page_graph_bytes),
            )),
            array(UInt64Array::from_iter_values(
                page_refs
                    .iter()
                    .map(|page_ref| page_ref.page_sparse_encoded_vectors as u64),
            )),
            array(UInt64Array::from_iter_values(
                page_refs
                    .iter()
                    .map(|page_ref| page_ref.page_dense_encoded_vectors as u64),
            )),
            array(fixed_f32_array(
                page_refs
                    .iter()
                    .map(|page_ref| page_ref.bounds_min.as_slice()),
                manifest.config.dimensions,
            )),
            array(fixed_f32_array(
                page_refs
                    .iter()
                    .map(|page_ref| page_ref.bounds_max.as_slice()),
                manifest.config.dimensions,
            )),
        ],
    )?;

    write_batch(batch)
}

pub(crate) fn routing_layer_page_index_from_parquet(
    bytes: &[u8],
    expected_manifest_version: u64,
    expected_routing_level: u8,
) -> Result<Vec<RoutingLayerPageRef>> {
    routing_layer_page_index_from_parquet_with_version_policy(
        bytes,
        expected_manifest_version,
        expected_routing_level,
        false,
    )
}

pub(crate) fn routing_layer_page_index_from_parquet_relaxed_manifest_version(
    bytes: &[u8],
    expected_manifest_version: u64,
    expected_routing_level: u8,
) -> Result<Vec<RoutingLayerPageRef>> {
    routing_layer_page_index_from_parquet_with_version_policy(
        bytes,
        expected_manifest_version,
        expected_routing_level,
        true,
    )
}

fn routing_layer_page_index_from_parquet_with_version_policy(
    bytes: &[u8],
    expected_manifest_version: u64,
    expected_routing_level: u8,
    allow_manifest_version_mismatch: bool,
) -> Result<Vec<RoutingLayerPageRef>> {
    let mut page_refs = Vec::new();
    for batch in read_batches(bytes)? {
        for row in 0..batch.num_rows() {
            let format_version =
                primitive_value_by_name::<UInt16Type>(&batch, row, "format_version")?;
            if format_version != CURRENT_VERSION {
                return Err(BorsukError::InvalidStorage(format!(
                    "unsupported routing layer page index version {format_version}"
                )));
            }
            let manifest_version =
                primitive_value_by_name::<UInt64Type>(&batch, row, "manifest_version")?;
            if !allow_manifest_version_mismatch && manifest_version != 0 {
                validate_table_manifest_version(
                    "routing layer page index",
                    expected_manifest_version,
                    manifest_version,
                )?;
            }
            validate_routing_layer_page_field(
                "routing_level",
                u64::from(expected_routing_level),
                u64::from(primitive_value_by_name::<UInt8Type>(
                    &batch,
                    row,
                    "routing_level",
                )?),
            )?;
            let page_segments = usize_from_u64(primitive_value_by_name::<UInt64Type>(
                &batch,
                row,
                "page_segments",
            )?)?;
            if page_segments == 0 {
                return Err(BorsukError::InvalidStorage(
                    "routing layer page index must not reference empty pages".to_string(),
                ));
            }

            page_refs.push(RoutingLayerPageRef {
                routing_level: expected_routing_level,
                page_ordinal: usize_from_u64(primitive_value_by_name::<UInt64Type>(
                    &batch,
                    row,
                    "page_ordinal",
                )?)?,
                path: string_value_by_name(&batch, row, "page_path")?.to_string(),
                checksum: string_value_by_name(&batch, row, "page_checksum")?.to_string(),
                page_segments,
                leaf_segments: routing_page_ref_leaf_segments(&batch, row, page_segments)?,
                leaf_pages: routing_page_ref_leaf_pages(&batch, row)?,
                routing_pages: routing_page_ref_routing_pages(&batch, row)?,
                dimensions: routing_page_ref_dimensions(&batch, row)?,
                centroid: routing_page_ref_centroid(&batch, row)?,
                radius: routing_page_ref_radius(&batch, row)?,
                bounds_min: routing_page_ref_bounds(&batch, row, "bounds_min")?,
                bounds_max: routing_page_ref_bounds(&batch, row, "bounds_max")?,
                id_bloom: routing_page_ref_id_bloom(&batch, row)?,
                vector_signature_bloom: routing_page_ref_vector_signature_bloom(&batch, row)?,
                level_mask: routing_page_ref_level_mask(&batch, row)?,
                page_records: routing_page_ref_page_records(&batch, row)?,
                page_segment_bytes: routing_page_ref_page_segment_bytes(&batch, row)?,
                page_vector_bytes: routing_page_ref_page_vector_bytes(&batch, row)?,
                page_graph_bytes: routing_page_ref_page_graph_bytes(&batch, row)?,
                page_sparse_encoded_vectors: routing_page_ref_page_sparse_encoded_vectors(
                    &batch, row,
                )?,
                page_dense_encoded_vectors: routing_page_ref_page_dense_encoded_vectors(
                    &batch, row,
                )?,
            });
        }
    }

    validate_routing_layer_page_refs(&page_refs)?;
    Ok(page_refs)
}

pub(crate) fn routing_layer_page_from_parquet(
    bytes: &[u8],
    expected_manifest_version: u64,
    expected_routing_level: u8,
    expected_page_ordinal: usize,
    expected_dimensions: usize,
) -> Result<Vec<SegmentSummary>> {
    let mut summaries = Vec::new();
    for batch in read_batches(bytes)? {
        for row in 0..batch.num_rows() {
            let format_version =
                primitive_value_by_name::<UInt16Type>(&batch, row, "format_version")?;
            if format_version != CURRENT_VERSION {
                return Err(BorsukError::InvalidStorage(format!(
                    "unsupported routing layer page version {format_version}"
                )));
            }
            let page_manifest_version =
                primitive_value_by_name::<UInt64Type>(&batch, row, "manifest_version")?;
            if page_manifest_version != 0 {
                validate_table_manifest_version(
                    "routing layer page",
                    expected_manifest_version,
                    page_manifest_version,
                )?;
            }
            validate_routing_layer_page_field(
                "routing_level",
                u64::from(expected_routing_level),
                u64::from(primitive_value_by_name::<UInt8Type>(
                    &batch,
                    row,
                    "routing_level",
                )?),
            )?;
            validate_routing_layer_page_field(
                "page_ordinal",
                expected_page_ordinal as u64,
                primitive_value_by_name::<UInt64Type>(&batch, row, "page_ordinal")?,
            )?;
            let page_segments =
                primitive_value_by_name::<UInt64Type>(&batch, row, "page_segments")?;
            if page_segments == 0 {
                return Err(BorsukError::InvalidStorage(
                    "routing layer page must declare at least one segment".to_string(),
                ));
            }

            let id = string_value_by_name(&batch, row, "segment_id")?.to_string();
            let dimensions = usize_from_u64(primitive_value_by_name::<UInt64Type>(
                &batch,
                row,
                "dimensions",
            )?)?;
            validate_routing_segment_dimensions(&id, expected_dimensions, dimensions)?;
            let centroid = fixed_f32_value_by_name(&batch, row, "centroid")?;
            validate_routing_centroid_dimensions(&id, dimensions, centroid.len())?;
            validate_routing_centroid_values(&id, &centroid)?;
            let radius = primitive_value_by_name::<Float32Type>(&batch, row, "radius")?;
            validate_routing_radius(&id, radius)?;
            let bounds_min = routing_bounds(&batch, row, "bounds_min", &id)?;
            let bounds_max = routing_bounds(&batch, row, "bounds_max", &id)?;
            let id_bloom = binary_value_by_name(&batch, row, "id_bloom")?.to_vec();
            validate_routing_id_bloom(&id, &id_bloom)?;
            let vector_signature_bloom = routing_vector_signature_bloom(&batch, row, &id)?;
            validate_routing_vector_signature_bloom(&id, &vector_signature_bloom)?;
            let leaf_mode = routing_leaf_mode(&batch, row)?;

            summaries.push(SegmentSummary {
                id,
                level: primitive_value_by_name::<UInt8Type>(&batch, row, "segment_level")?,
                path: string_value_by_name(&batch, row, "segment_path")?.to_string(),
                layout: serde_json::from_str(string_value_by_name(
                    &batch,
                    row,
                    "segment_layout_json",
                )?)
                .map_err(|error| {
                    BorsukError::InvalidStorage(format!(
                        "segment physical layout reference is invalid: {error}"
                    ))
                })?,
                object_count: usize_from_u64(primitive_value_by_name::<UInt64Type>(
                    &batch,
                    row,
                    "object_count",
                )?)?,
                dimensions,
                centroid,
                radius,
                bounds_min,
                bounds_max,
                checksum: string_value_by_name(&batch, row, "segment_checksum")?.to_string(),
                size_bytes: primitive_value_by_name::<UInt64Type>(
                    &batch,
                    row,
                    "segment_size_bytes",
                )?,
                vector_size_bytes: routing_u64_or_zero(&batch, row, "vector_size_bytes")?,
                graph_path: string_value_by_name(&batch, row, "graph_path")?.to_string(),
                graph_checksum: string_value_by_name(&batch, row, "graph_checksum")?.to_string(),
                graph_size_bytes: primitive_value_by_name::<UInt64Type>(
                    &batch,
                    row,
                    "graph_size_bytes",
                )?,
                leaf_mode,
                id_bloom,
                vector_signature_bloom,
                metadata_stats: routing_metadata_stats(&batch, row)?,
                sparse_encoded: routing_sparse_encoded(&batch, row)?,
                dense_encoded: routing_dense_encoded(&batch, row)?,
                text_doc_count: routing_text_doc_count(&batch, row)?,
                text_total_doc_length: routing_text_total_doc_length(&batch, row)?,
                text_lexical_decoded_bytes: routing_u64_or_zero(
                    &batch,
                    row,
                    "text_lexical_decoded_bytes",
                )?,
                sparse_lexical_max_decoded_bytes: routing_u64_or_zero(
                    &batch,
                    row,
                    "sparse_lexical_max_decoded_bytes",
                )?,
                lexical_shards: routing_lexical_shards(&batch, row)?,
                created_at: datetime_from_millis(primitive_value_by_name::<Int64Type>(
                    &batch,
                    row,
                    "created_at_ms",
                )?)?,
            });
        }
    }

    validate_routing_segment_ids(&summaries)?;
    validate_routing_segment_paths(&summaries)?;
    validate_routing_segment_summary_metadata(&summaries)?;

    Ok(summaries)
}

pub(crate) fn pivots_to_parquet(manifest: &Manifest) -> Result<Vec<u8>> {
    let dimensions = manifest.config.dimensions;
    let schema = pivots_schema(dimensions);
    let pivots = &manifest.pivots;
    validate_pivot_ids(pivots)?;
    for pivot in pivots {
        validate_pivot_vector_dimensions(&pivot.id, dimensions, pivot.vector.len())?;
        validate_pivot_vector_values(&pivot.id, &pivot.vector)?;
    }

    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            array(UInt16Array::from_iter_values(
                pivots.iter().map(|_| CURRENT_VERSION),
            )),
            array(UInt64Array::from_iter_values(
                pivots.iter().map(|_| manifest.version),
            )),
            array(UInt64Array::from_iter_values(
                pivots.iter().map(|pivot| pivot.ordinal as u64),
            )),
            array(StringArray::from_iter_values(
                pivots.iter().map(|pivot| pivot.id.as_str()),
            )),
            array(fixed_f32_array(
                pivots.iter().map(|pivot| pivot.vector.as_slice()),
                dimensions,
            )),
        ],
    )?;

    write_batch(batch)
}

pub(crate) fn pivots_from_parquet(
    bytes: &[u8],
    dimensions: usize,
    expected_manifest_version: u64,
) -> Result<Vec<PivotSummary>> {
    let mut pivots = Vec::new();
    for batch in read_batches(bytes)? {
        for row in 0..batch.num_rows() {
            let format_version =
                primitive_value_by_name::<UInt16Type>(&batch, row, "format_version")?;
            if format_version != CURRENT_VERSION {
                return Err(BorsukError::InvalidStorage(format!(
                    "unsupported pivot table version {format_version}"
                )));
            }

            validate_table_manifest_version(
                "pivot table",
                expected_manifest_version,
                primitive_value_by_name::<UInt64Type>(&batch, row, "manifest_version")?,
            )?;
            let ordinal = usize_from_u64(primitive_value_by_name::<UInt64Type>(
                &batch, row, "ordinal",
            )?)?;
            let id = string_value_by_name(&batch, row, "pivot_id")?.to_string();
            let vector = fixed_f32_value_by_name(&batch, row, "vector")?;
            validate_pivot_vector_dimensions(&id, dimensions, vector.len())?;
            validate_pivot_vector_values(&id, &vector)?;

            pivots.push(PivotSummary {
                id,
                ordinal,
                vector,
            });
        }
    }

    validate_pivot_ids(&pivots)?;

    Ok(pivots)
}

/// Encode vector records as a compact Parquet table.
pub fn vector_records_to_parquet(records: &[VectorRecord], dimensions: usize) -> Result<Vec<u8>> {
    if dimensions == 0 {
        return Err(BorsukError::InvalidRecordInput(
            "vector record dimensions must be greater than zero".to_string(),
        ));
    }
    validate_vector_record_ids(records)?;
    for record in records {
        if record.vector.len() != dimensions {
            return Err(BorsukError::DimensionMismatch {
                expected: dimensions,
                actual: record.vector.len(),
            });
        }
        validate_vector_record_values(&record.id, &record.vector)?;
    }

    let schema = vector_records_schema(dimensions);
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            array(UInt16Array::from_iter_values(
                records.iter().map(|_| CURRENT_VERSION),
            )),
            array(UInt64Array::from_iter_values(
                records.iter().map(|_| dimensions as u64),
            )),
            array(BinaryArray::from_iter_values(
                records.iter().map(|record| record.id.as_bytes()),
            )),
            array(fixed_f32_array(
                records.iter().map(|record| record.vector.as_slice()),
                dimensions,
            )),
            array(BinaryArray::from_iter_values(
                records
                    .iter()
                    .map(|record| crate::metadata::encode(&record.metadata)),
            )),
        ],
    )?;

    write_batch(batch)
}

/// Decode vector records from a Parquet table and validate their fixed width.
pub fn vector_records_from_parquet(
    bytes: &[u8],
    expected_dimensions: usize,
) -> Result<Vec<VectorRecord>> {
    if expected_dimensions == 0 {
        return Err(BorsukError::InvalidRecordInput(
            "expected dimensions must be greater than zero".to_string(),
        ));
    }

    let mut records = Vec::new();
    for batch in read_batches(bytes)? {
        for row in 0..batch.num_rows() {
            let format_version = primitive_value::<UInt16Type>(&batch, 0, row, "format_version")?;
            if format_version != CURRENT_VERSION {
                return Err(BorsukError::InvalidStorage(format!(
                    "unsupported vector records table version {format_version}"
                )));
            }

            let dimensions =
                usize_from_u64(primitive_value::<UInt64Type>(&batch, 1, row, "dimensions")?)?;
            if dimensions != expected_dimensions {
                return Err(BorsukError::DimensionMismatch {
                    expected: expected_dimensions,
                    actual: dimensions,
                });
            }

            let vector = fixed_f32_value(&batch, 3, row, "vector")?;
            if vector.len() != expected_dimensions {
                return Err(BorsukError::DimensionMismatch {
                    expected: expected_dimensions,
                    actual: vector.len(),
                });
            }
            let id = record_id_value(&batch, 2, row, "record_id")?;
            validate_vector_record_values(&id, &vector)?;

            let metadata = match batch.schema().index_of("metadata").ok() {
                Some(column) => {
                    crate::metadata::decode(binary_value(&batch, column, row, "metadata")?)?
                }
                None => crate::Metadata::new(),
            };
            records.push(VectorRecord {
                id,
                vector,
                extra_vectors: BTreeMap::new(),
                extra_sparse: BTreeMap::new(),
                extra_multi_vectors: BTreeMap::new(),
                storage: crate::StorageEncoding::Auto,
                text: None,
                text_term_ids: Vec::new(),
                text_term_freqs: Vec::new(),
                metadata,
                generation: 0,
                mutation_stamp: None,
            });
        }
    }

    validate_vector_record_ids(&records)?;

    Ok(records)
}

fn validate_manifest_config(
    dimensions: usize,
    segment_max_vectors: usize,
    routing_page_fanout: usize,
    graph_neighbors: usize,
) -> Result<()> {
    if dimensions == 0 {
        return Err(BorsukError::InvalidStorage(
            "manifest dimensions must be greater than zero".to_string(),
        ));
    }
    if segment_max_vectors == 0 {
        return Err(BorsukError::InvalidStorage(
            "manifest segment_max_vectors must be greater than zero".to_string(),
        ));
    }
    if routing_page_fanout <= 1 {
        return Err(BorsukError::InvalidStorage(
            "manifest routing_page_fanout must be greater than one".to_string(),
        ));
    }
    if graph_neighbors == 0 {
        return Err(BorsukError::InvalidStorage(
            "manifest graph_neighbors must be greater than zero".to_string(),
        ));
    }

    Ok(())
}

fn validate_table_manifest_version(table: &str, expected: u64, actual: u64) -> Result<()> {
    if actual != expected {
        return Err(BorsukError::InvalidStorage(format!(
            "{table} manifest_version {actual} does not match manifest version {expected}"
        )));
    }

    Ok(())
}

fn validate_vector_record_ids(records: &[VectorRecord]) -> Result<()> {
    let mut ids = HashSet::with_capacity(records.len());
    for record in records {
        if record.id.is_empty() {
            return Err(BorsukError::InvalidRecordInput(
                "record ids must not be empty".to_string(),
            ));
        }
        if !ids.insert(record.id.as_bytes()) {
            return Err(BorsukError::InvalidRecordInput(format!(
                "duplicate record id `{}` in vector records table",
                record.id
            )));
        }
    }

    Ok(())
}

fn validate_vector_record_values(record_id: &RecordId, vector: &[f32]) -> Result<()> {
    if let Some((coordinate_index, value)) = vector
        .iter()
        .copied()
        .enumerate()
        .find(|(_, value)| !value.is_finite())
    {
        return Err(BorsukError::InvalidRecordInput(format!(
            "vector records must contain only finite f32 values; record `{record_id}` coordinate {coordinate_index} was {value}"
        )));
    }

    Ok(())
}

fn validate_pivot_vector_values(pivot_id: &str, vector: &[f32]) -> Result<()> {
    if let Some((coordinate_index, value)) = non_finite_coordinate(vector) {
        return Err(BorsukError::InvalidStorage(format!(
            "pivot vectors must contain only finite f32 values; pivot `{pivot_id}` coordinate {coordinate_index} was {value}"
        )));
    }

    Ok(())
}

fn validate_pivot_vector_dimensions(pivot_id: &str, expected: usize, actual: usize) -> Result<()> {
    validate_stored_vector_dimensions("pivot vector", pivot_id, expected, actual)
}

fn validate_pivot_ids(pivots: &[PivotSummary]) -> Result<()> {
    let mut ids = HashSet::with_capacity(pivots.len());
    for pivot in pivots {
        if pivot.id.trim().is_empty() {
            return Err(BorsukError::InvalidStorage(
                "pivot ids must not be empty".to_string(),
            ));
        }
        if !ids.insert(pivot.id.as_str()) {
            return Err(BorsukError::InvalidStorage(format!(
                "duplicate pivot id `{}`",
                pivot.id
            )));
        }
    }

    Ok(())
}

fn validate_routing_segment_dimensions(
    segment_id: &str,
    expected: usize,
    actual: usize,
) -> Result<()> {
    if actual != expected {
        return Err(BorsukError::InvalidStorage(format!(
            "routing segment `{segment_id}` declares {actual} dimensions, expected {expected}"
        )));
    }

    Ok(())
}

fn validate_routing_segment_ids(segments: &[SegmentSummary]) -> Result<()> {
    let mut ids = HashSet::with_capacity(segments.len());
    for segment in segments {
        if segment.id.trim().is_empty() {
            return Err(BorsukError::InvalidStorage(
                "routing segment ids must not be empty".to_string(),
            ));
        }
        if !ids.insert(segment.id.as_str()) {
            return Err(BorsukError::InvalidStorage(format!(
                "duplicate routing segment id `{}`",
                segment.id
            )));
        }
    }

    Ok(())
}

fn validate_routing_segment_paths(segments: &[SegmentSummary]) -> Result<()> {
    let mut segment_paths = HashSet::with_capacity(segments.len());
    let mut graph_paths = HashSet::with_capacity(segments.len());
    for segment in segments {
        if segment.path.trim().is_empty() {
            return Err(BorsukError::InvalidStorage(
                "routing segment paths must not be empty".to_string(),
            ));
        }
        if !segment_paths.insert(segment.path.as_str()) {
            return Err(BorsukError::InvalidStorage(format!(
                "duplicate routing segment path `{}`",
                segment.path
            )));
        }
        segment
            .layout
            .validate_for(crate::PhysicalObjectRole::NormalSegment)?;
        let required_suffix = format!(".{}", segment.layout.physical_format.extension());
        if !segment.path.ends_with(&required_suffix) {
            return Err(BorsukError::InvalidStorage(format!(
                "routing segment `{}` uses {} but path `{}` does not end with `{required_suffix}`",
                segment.id, segment.layout.physical_format, segment.path
            )));
        }

        // An empty graph path marks a graph-free segment (a `PqScanOnly` index
        // never builds one). Only non-empty paths must be present and unique.
        if !segment.graph_path.trim().is_empty() && !graph_paths.insert(segment.graph_path.as_str())
        {
            return Err(BorsukError::InvalidStorage(format!(
                "duplicate routing graph path `{}`",
                segment.graph_path
            )));
        }
    }

    Ok(())
}

fn validate_routing_segment_summary_metadata(segments: &[SegmentSummary]) -> Result<()> {
    for segment in segments {
        if segment.object_count == 0 {
            return Err(BorsukError::InvalidStorage(format!(
                "routing segment object_count must be greater than zero; segment `{}`",
                segment.id
            )));
        }
        let encoded_count = segment.sparse_encoded.saturating_add(segment.dense_encoded);
        if encoded_count != 0 && encoded_count != segment.object_count {
            return Err(BorsukError::InvalidStorage(format!(
                "routing segment encoded counts must sum to object_count; segment `{}`",
                segment.id
            )));
        }
        if segment.text_doc_count as usize > segment.object_count {
            return Err(BorsukError::InvalidStorage(format!(
                "routing segment text_doc_count must not exceed object_count; segment `{}`",
                segment.id
            )));
        }
        if segment.text_doc_count == 0 && segment.text_total_doc_length != 0 {
            return Err(BorsukError::InvalidStorage(format!(
                "routing segment text_total_doc_length must be zero when text_doc_count is zero; segment `{}`",
                segment.id
            )));
        }
        if segment.text_doc_count > 0
            && segment.text_total_doc_length < u64::from(segment.text_doc_count)
        {
            return Err(BorsukError::InvalidStorage(format!(
                "routing segment text_total_doc_length must be at least text_doc_count; segment `{}`",
                segment.id
            )));
        }
        validate_routing_checksum("routing segment checksum", &segment.id, &segment.checksum)?;
        if segment.size_bytes == 0 {
            return Err(BorsukError::InvalidStorage(format!(
                "routing segment size_bytes must be greater than zero; segment `{}`",
                segment.id
            )));
        }
        // A graph-free segment (from a `PqScanOnly` index) carries an empty
        // graph triple: empty path, empty checksum, zero size. Accept that
        // consistent "no graph" tuple, but reject any partially-populated mix so
        // a genuinely corrupt row is still caught.
        let graph_path_empty = segment.graph_path.trim().is_empty();
        let graph_checksum_empty = segment.graph_checksum.is_empty();
        let graph_absent =
            graph_path_empty && graph_checksum_empty && segment.graph_size_bytes == 0;
        if !graph_absent {
            if graph_path_empty {
                return Err(BorsukError::InvalidStorage(format!(
                    "routing graph path must be present when a graph is stored; segment `{}`",
                    segment.id
                )));
            }
            validate_routing_checksum(
                "routing graph checksum",
                &segment.id,
                &segment.graph_checksum,
            )?;
            if segment.graph_size_bytes == 0 {
                return Err(BorsukError::InvalidStorage(format!(
                    "routing graph size_bytes must be greater than zero; segment `{}`",
                    segment.id
                )));
            }
        }
        validate_routing_vector_signature_bloom(&segment.id, &segment.vector_signature_bloom)?;
        validate_routing_bounds(
            &segment.id,
            segment.dimensions,
            &segment.bounds_min,
            &segment.bounds_max,
        )?;
    }

    Ok(())
}

fn validate_routing_checksum(field: &str, segment_id: &str, checksum: &str) -> Result<()> {
    if is_blake3_hex_checksum(checksum) {
        return Ok(());
    }

    Err(BorsukError::InvalidStorage(format!(
        "{field} must be {BLAKE3_HEX_CHECKSUM_LEN} lowercase hex characters; segment `{segment_id}`"
    )))
}

fn validate_hex_checksum(field: &str, checksum: &str) -> Result<()> {
    if is_blake3_hex_checksum(checksum) {
        return Ok(());
    }

    Err(BorsukError::InvalidStorage(format!(
        "{field} checksum must be {BLAKE3_HEX_CHECKSUM_LEN} lowercase hex characters"
    )))
}

fn is_blake3_hex_checksum(checksum: &str) -> bool {
    checksum.len() == BLAKE3_HEX_CHECKSUM_LEN
        && checksum
            .bytes()
            .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
}

fn validate_routing_centroid_dimensions(
    segment_id: &str,
    expected: usize,
    actual: usize,
) -> Result<()> {
    validate_stored_vector_dimensions("routing centroid", segment_id, expected, actual)
}

fn validate_routing_centroid_values(segment_id: &str, centroid: &[f32]) -> Result<()> {
    if let Some((coordinate_index, value)) = non_finite_coordinate(centroid) {
        return Err(BorsukError::InvalidStorage(format!(
            "routing centroids must contain only finite f32 values; segment `{segment_id}` coordinate {coordinate_index} was {value}"
        )));
    }

    Ok(())
}

fn validate_routing_radius(segment_id: &str, radius: f32) -> Result<()> {
    if !radius.is_finite() {
        return Err(BorsukError::InvalidStorage(format!(
            "routing radii must contain only finite f32 values; segment `{segment_id}` was {radius}"
        )));
    }

    Ok(())
}

fn validate_routing_bounds(
    segment_id: &str,
    dimensions: usize,
    bounds_min: &[f32],
    bounds_max: &[f32],
) -> Result<()> {
    if bounds_min.is_empty() && bounds_max.is_empty() {
        return Ok(());
    }
    validate_stored_vector_dimensions(
        "routing bounds_min",
        segment_id,
        dimensions,
        bounds_min.len(),
    )?;
    validate_stored_vector_dimensions(
        "routing bounds_max",
        segment_id,
        dimensions,
        bounds_max.len(),
    )?;
    for (coordinate_index, (min, max)) in bounds_min.iter().zip(bounds_max).enumerate() {
        if !min.is_finite() {
            return Err(BorsukError::InvalidStorage(format!(
                "routing bounds_min must contain only finite f32 values; segment `{segment_id}` coordinate {coordinate_index} was {min}"
            )));
        }
        if !max.is_finite() {
            return Err(BorsukError::InvalidStorage(format!(
                "routing bounds_max must contain only finite f32 values; segment `{segment_id}` coordinate {coordinate_index} was {max}"
            )));
        }
        if min > max {
            return Err(BorsukError::InvalidStorage(format!(
                "routing bounds must satisfy min <= max; segment `{segment_id}` coordinate {coordinate_index} had {min} > {max}"
            )));
        }
    }

    Ok(())
}

fn validate_routing_id_bloom(segment_id: &str, id_bloom: &[u8]) -> Result<()> {
    if id_bloom.is_empty() || id_bloom.len() == SEGMENT_ID_BLOOM_BYTES {
        return Ok(());
    }

    Err(BorsukError::InvalidStorage(format!(
        "routing segment `{segment_id}` id_bloom must be {SEGMENT_ID_BLOOM_BYTES} bytes when present, got {}",
        id_bloom.len()
    )))
}

fn validate_routing_vector_signature_bloom(
    segment_id: &str,
    vector_signature_bloom: &[u8],
) -> Result<()> {
    if vector_signature_bloom.is_empty()
        || vector_signature_bloom.len() == SEGMENT_VECTOR_SIGNATURE_BLOOM_BYTES
    {
        return Ok(());
    }

    Err(BorsukError::InvalidStorage(format!(
        "routing segment `{segment_id}` vector_signature_bloom must be {SEGMENT_VECTOR_SIGNATURE_BLOOM_BYTES} bytes when present, got {}",
        vector_signature_bloom.len()
    )))
}

fn routing_leaf_mode(batch: &RecordBatch, row: usize) -> Result<LeafMode> {
    let Ok(column_index) = batch.schema().index_of("leaf_mode") else {
        return Ok(LeafMode::Graph);
    };
    routing_leaf_mode_at_column(batch, row, column_index)
}

fn routing_leaf_mode_at_column(
    batch: &RecordBatch,
    row: usize,
    column_index: usize,
) -> Result<LeafMode> {
    let value = string_value(batch, column_index, row, "leaf_mode")?;
    value.parse::<LeafMode>().map_err(|_| {
        BorsukError::InvalidStorage(format!(
            "routing leaf_mode `{value}` is not a supported leaf mode"
        ))
    })
}

fn validate_routing_layer_page_field(field: &str, expected: u64, actual: u64) -> Result<()> {
    if actual == expected {
        return Ok(());
    }

    Err(BorsukError::InvalidStorage(format!(
        "routing layer page {field} {actual} does not match expected {expected}"
    )))
}

fn validate_routing_layer_page_refs(page_refs: &[RoutingLayerPageRef]) -> Result<()> {
    let mut seen_ordinals = HashSet::with_capacity(page_refs.len());
    for page_ref in page_refs {
        if !seen_ordinals.insert(page_ref.page_ordinal) {
            return Err(BorsukError::InvalidStorage(format!(
                "duplicate routing layer page ordinal {}",
                page_ref.page_ordinal
            )));
        }
        if page_ref.path.trim().is_empty() {
            return Err(BorsukError::InvalidStorage(
                "routing layer page index contains an empty page path".to_string(),
            ));
        }
        if !page_ref.path.starts_with("routing/pages/") {
            return Err(BorsukError::InvalidStorage(format!(
                "routing layer page `{}` is outside routing/pages",
                page_ref.path
            )));
        }
        validate_hex_checksum("routing layer page", &page_ref.checksum)?;
        if page_ref.page_segments == 0 {
            return Err(BorsukError::InvalidStorage(
                "routing layer page index must not reference empty pages".to_string(),
            ));
        }
        if page_ref.leaf_segments == 0 {
            return Err(BorsukError::InvalidStorage(
                "routing layer page index must not reference empty leaf ranges".to_string(),
            ));
        }
        if page_ref.leaf_pages == 0 || page_ref.routing_pages == 0 {
            if page_ref.leaf_pages != 0 || page_ref.routing_pages != 0 {
                return Err(BorsukError::InvalidStorage(
                    "routing layer page index leaf_pages and routing_pages must both be present or both be legacy-zero".to_string(),
                ));
            }
        } else if page_ref.routing_pages < page_ref.leaf_pages {
            return Err(BorsukError::InvalidStorage(
                "routing layer page index routing_pages must be at least leaf_pages".to_string(),
            ));
        }
        if !page_ref.id_bloom.is_empty() {
            validate_routing_id_bloom("routing-layer-page", &page_ref.id_bloom)?;
        }
        if !page_ref.vector_signature_bloom.is_empty() {
            validate_routing_vector_signature_bloom(
                "routing-layer-page",
                &page_ref.vector_signature_bloom,
            )?;
        }
        if page_ref.level_mask == 0 {
            return Err(BorsukError::InvalidStorage(
                "routing layer page index level_mask must not be zero".to_string(),
            ));
        }
        if page_ref.dimensions == 0 && page_ref.centroid.is_empty() && page_ref.radius.is_infinite()
        {
            continue;
        }
        if page_ref.dimensions == 0 {
            return Err(BorsukError::InvalidStorage(
                "routing layer page index dimensions must be greater than zero".to_string(),
            ));
        }
        validate_routing_centroid_dimensions(
            "routing-layer-page",
            page_ref.dimensions,
            page_ref.centroid.len(),
        )?;
        validate_routing_centroid_values("routing-layer-page", &page_ref.centroid)?;
        validate_routing_radius("routing-layer-page", page_ref.radius)?;
        validate_routing_bounds(
            "routing-layer-page",
            page_ref.dimensions,
            &page_ref.bounds_min,
            &page_ref.bounds_max,
        )?;
    }

    Ok(())
}

fn routing_page_ref_leaf_segments(
    batch: &RecordBatch,
    row: usize,
    page_segments: usize,
) -> Result<usize> {
    let Ok(column_index) = batch.schema().index_of("leaf_segments") else {
        return Ok(page_segments);
    };
    usize_from_u64(primitive_value::<UInt64Type>(
        batch,
        column_index,
        row,
        "leaf_segments",
    )?)
}

fn routing_page_ref_leaf_pages(batch: &RecordBatch, row: usize) -> Result<usize> {
    let Ok(column_index) = batch.schema().index_of("leaf_pages") else {
        return Ok(0);
    };
    usize_from_u64(primitive_value::<UInt64Type>(
        batch,
        column_index,
        row,
        "leaf_pages",
    )?)
}

fn routing_page_ref_routing_pages(batch: &RecordBatch, row: usize) -> Result<usize> {
    let Ok(column_index) = batch.schema().index_of("routing_pages") else {
        return Ok(0);
    };
    usize_from_u64(primitive_value::<UInt64Type>(
        batch,
        column_index,
        row,
        "routing_pages",
    )?)
}

fn routing_page_ref_dimensions(batch: &RecordBatch, row: usize) -> Result<usize> {
    let Ok(column_index) = batch.schema().index_of("dimensions") else {
        return Ok(0);
    };
    usize_from_u64(primitive_value::<UInt64Type>(
        batch,
        column_index,
        row,
        "dimensions",
    )?)
}

fn routing_page_ref_centroid(batch: &RecordBatch, row: usize) -> Result<Vec<f32>> {
    let Ok(column_index) = batch.schema().index_of("centroid") else {
        return Ok(Vec::new());
    };
    fixed_f32_value(batch, column_index, row, "centroid")
}

fn routing_page_ref_radius(batch: &RecordBatch, row: usize) -> Result<f32> {
    let Ok(column_index) = batch.schema().index_of("radius") else {
        return Ok(f32::INFINITY);
    };
    primitive_value::<Float32Type>(batch, column_index, row, "radius")
}

fn routing_page_ref_id_bloom(batch: &RecordBatch, row: usize) -> Result<Vec<u8>> {
    let Ok(column_index) = batch.schema().index_of("id_bloom") else {
        return Ok(Vec::new());
    };
    Ok(binary_value(batch, column_index, row, "id_bloom")?.to_vec())
}

fn routing_page_ref_bounds(batch: &RecordBatch, row: usize, column_name: &str) -> Result<Vec<f32>> {
    let Ok(column_index) = batch.schema().index_of(column_name) else {
        return Ok(Vec::new());
    };
    fixed_f32_value(batch, column_index, row, column_name)
}

fn routing_page_ref_vector_signature_bloom(batch: &RecordBatch, row: usize) -> Result<Vec<u8>> {
    let Ok(column_index) = batch.schema().index_of("vector_signature_bloom") else {
        return Ok(Vec::new());
    };
    let bloom = binary_value(batch, column_index, row, "vector_signature_bloom")?.to_vec();
    validate_routing_vector_signature_bloom("routing-layer-page", &bloom)?;
    Ok(bloom)
}

fn routing_page_ref_level_mask(batch: &RecordBatch, row: usize) -> Result<u64> {
    let Ok(column_index) = batch.schema().index_of("level_mask") else {
        return Ok(u64::MAX);
    };
    primitive_value::<UInt64Type>(batch, column_index, row, "level_mask")
}

fn routing_page_ref_page_records(batch: &RecordBatch, row: usize) -> Result<usize> {
    let Ok(column_index) = batch.schema().index_of("page_records") else {
        return Ok(0);
    };
    usize_from_u64(primitive_value::<UInt64Type>(
        batch,
        column_index,
        row,
        "page_records",
    )?)
}

fn routing_page_ref_page_segment_bytes(batch: &RecordBatch, row: usize) -> Result<u64> {
    let Ok(column_index) = batch.schema().index_of("page_segment_bytes") else {
        return Ok(0);
    };
    primitive_value::<UInt64Type>(batch, column_index, row, "page_segment_bytes")
}

fn routing_page_ref_page_vector_bytes(batch: &RecordBatch, row: usize) -> Result<u64> {
    let Ok(column_index) = batch.schema().index_of("page_vector_bytes") else {
        return Ok(0);
    };
    primitive_value::<UInt64Type>(batch, column_index, row, "page_vector_bytes")
}

fn routing_page_ref_page_graph_bytes(batch: &RecordBatch, row: usize) -> Result<u64> {
    let Ok(column_index) = batch.schema().index_of("page_graph_bytes") else {
        return Ok(0);
    };
    primitive_value::<UInt64Type>(batch, column_index, row, "page_graph_bytes")
}

fn routing_page_ref_page_sparse_encoded_vectors(batch: &RecordBatch, row: usize) -> Result<usize> {
    let Ok(column_index) = batch.schema().index_of("page_sparse_encoded_vectors") else {
        return Ok(0);
    };
    usize_from_u64(primitive_value::<UInt64Type>(
        batch,
        column_index,
        row,
        "page_sparse_encoded_vectors",
    )?)
}

fn routing_page_ref_page_dense_encoded_vectors(batch: &RecordBatch, row: usize) -> Result<usize> {
    let Ok(column_index) = batch.schema().index_of("page_dense_encoded_vectors") else {
        return Ok(0);
    };
    usize_from_u64(primitive_value::<UInt64Type>(
        batch,
        column_index,
        row,
        "page_dense_encoded_vectors",
    )?)
}

fn routing_vector_signature_bloom(
    batch: &RecordBatch,
    row: usize,
    segment_id: &str,
) -> Result<Vec<u8>> {
    let Ok(column_index) = batch.schema().index_of("vector_signature_bloom") else {
        return Ok(Vec::new());
    };
    let bloom = binary_value(batch, column_index, row, "vector_signature_bloom")?.to_vec();
    validate_routing_vector_signature_bloom(segment_id, &bloom)?;
    Ok(bloom)
}

fn routing_bounds(
    batch: &RecordBatch,
    row: usize,
    column_name: &str,
    segment_id: &str,
) -> Result<Vec<f32>> {
    let Ok(column_index) = batch.schema().index_of(column_name) else {
        return Ok(Vec::new());
    };
    let bounds = fixed_f32_value(batch, column_index, row, column_name)?;
    if let Some((coordinate_index, value)) = non_finite_coordinate(&bounds) {
        return Err(BorsukError::InvalidStorage(format!(
            "routing {column_name} must contain only finite f32 values; segment `{segment_id}` coordinate {coordinate_index} was {value}"
        )));
    }
    Ok(bounds)
}

/// Read a segment's persisted metadata pruning stats, defaulting to empty when
/// the column is absent.
fn routing_metadata_stats(batch: &RecordBatch, row: usize) -> Result<crate::MetadataStats> {
    if batch.schema().field_with_name("metadata_stats").is_ok() {
        crate::MetadataStats::from_bytes(binary_value_by_name(batch, row, "metadata_stats")?)
    } else {
        Ok(crate::MetadataStats::default())
    }
}

fn routing_text_doc_count(batch: &RecordBatch, row: usize) -> Result<u32> {
    if batch.schema().field_with_name("text_doc_count").is_ok() {
        primitive_value_by_name::<UInt32Type>(batch, row, "text_doc_count")
    } else {
        Ok(0)
    }
}

fn routing_text_total_doc_length(batch: &RecordBatch, row: usize) -> Result<u64> {
    if batch
        .schema()
        .field_with_name("text_total_doc_length")
        .is_ok()
    {
        primitive_value_by_name::<UInt64Type>(batch, row, "text_total_doc_length")
    } else {
        Ok(0)
    }
}

fn routing_u64_or_zero(batch: &RecordBatch, row: usize, name: &str) -> Result<u64> {
    if batch.schema().field_with_name(name).is_ok() {
        primitive_value_by_name::<UInt64Type>(batch, row, name)
    } else {
        Ok(0)
    }
}

fn routing_lexical_shards(
    batch: &RecordBatch,
    row: usize,
) -> Result<Vec<crate::manifest::SegmentLexicalShardRef>> {
    let column = batch
        .schema()
        .index_of("lexical_shards_json")
        .map_err(|_| {
            BorsukError::InvalidStorage(
            "routing table is missing required lexical_shards_json; rebuild the unreleased index"
                .to_string(),
        )
        })?;
    serde_json::from_str(string_value(batch, column, row, "lexical_shards_json")?).map_err(|err| {
        BorsukError::InvalidStorage(format!("failed to parse segment lexical shard refs: {err}"))
    })
}

fn routing_sparse_encoded(batch: &RecordBatch, row: usize) -> Result<usize> {
    if batch.schema().field_with_name("sparse_encoded").is_ok() {
        usize_from_u64(primitive_value_by_name::<UInt64Type>(
            batch,
            row,
            "sparse_encoded",
        )?)
    } else {
        Ok(0)
    }
}

fn routing_dense_encoded(batch: &RecordBatch, row: usize) -> Result<usize> {
    if batch.schema().field_with_name("dense_encoded").is_ok() {
        usize_from_u64(primitive_value_by_name::<UInt64Type>(
            batch,
            row,
            "dense_encoded",
        )?)
    } else {
        Ok(0)
    }
}

pub(crate) fn routing_from_parquet(
    bytes: &[u8],
    expected_manifest_version: u64,
) -> Result<Vec<SegmentSummary>> {
    let mut summaries = Vec::new();
    for batch in read_batches(bytes)? {
        for row in 0..batch.num_rows() {
            let format_version =
                primitive_value_by_name::<UInt16Type>(&batch, row, "format_version")?;
            if format_version != CURRENT_VERSION {
                return Err(BorsukError::InvalidStorage(format!(
                    "unsupported routing table version {format_version}"
                )));
            }
            validate_table_manifest_version(
                "routing table",
                expected_manifest_version,
                primitive_value_by_name::<UInt64Type>(&batch, row, "manifest_version")?,
            )?;

            let id = string_value_by_name(&batch, row, "id")?.to_string();
            let centroid = fixed_f32_value_by_name(&batch, row, "centroid")?;
            let radius = primitive_value_by_name::<Float32Type>(&batch, row, "radius")?;
            let dimensions = usize_from_u64(primitive_value_by_name::<UInt64Type>(
                &batch,
                row,
                "dimensions",
            )?)?;
            validate_routing_centroid_dimensions(&id, dimensions, centroid.len())?;
            validate_routing_centroid_values(&id, &centroid)?;
            validate_routing_radius(&id, radius)?;
            let id_bloom = if batch.schema().field_with_name("id_bloom").is_ok() {
                let id_bloom = binary_value_by_name(&batch, row, "id_bloom")?.to_vec();
                validate_routing_id_bloom(&id, &id_bloom)?;
                id_bloom
            } else {
                Vec::new()
            };
            let leaf_mode = routing_leaf_mode(&batch, row)?;
            let vector_signature_bloom = routing_vector_signature_bloom(&batch, row, &id)?;
            let bounds_min = routing_bounds(&batch, row, "bounds_min", &id)?;
            let bounds_max = routing_bounds(&batch, row, "bounds_max", &id)?;

            summaries.push(SegmentSummary {
                id,
                level: primitive_value_by_name::<UInt8Type>(&batch, row, "level")?,
                path: string_value_by_name(&batch, row, "path")?.to_string(),
                layout: serde_json::from_str(string_value_by_name(&batch, row, "layout_json")?)
                    .map_err(|error| {
                        BorsukError::InvalidStorage(format!(
                            "segment physical layout reference is invalid: {error}"
                        ))
                    })?,
                object_count: usize_from_u64(primitive_value_by_name::<UInt64Type>(
                    &batch,
                    row,
                    "object_count",
                )?)?,
                dimensions,
                centroid,
                radius,
                bounds_min,
                bounds_max,
                checksum: string_value_by_name(&batch, row, "checksum")?.to_string(),
                size_bytes: primitive_value_by_name::<UInt64Type>(&batch, row, "size_bytes")?,
                vector_size_bytes: routing_u64_or_zero(&batch, row, "vector_size_bytes")?,
                graph_path: string_value_by_name(&batch, row, "graph_path")?.to_string(),
                graph_checksum: string_value_by_name(&batch, row, "graph_checksum")?.to_string(),
                graph_size_bytes: primitive_value_by_name::<UInt64Type>(
                    &batch,
                    row,
                    "graph_size_bytes",
                )?,
                leaf_mode,
                id_bloom,
                vector_signature_bloom,
                metadata_stats: routing_metadata_stats(&batch, row)?,
                sparse_encoded: routing_sparse_encoded(&batch, row)?,
                dense_encoded: routing_dense_encoded(&batch, row)?,
                text_doc_count: routing_text_doc_count(&batch, row)?,
                text_total_doc_length: routing_text_total_doc_length(&batch, row)?,
                text_lexical_decoded_bytes: routing_u64_or_zero(
                    &batch,
                    row,
                    "text_lexical_decoded_bytes",
                )?,
                sparse_lexical_max_decoded_bytes: routing_u64_or_zero(
                    &batch,
                    row,
                    "sparse_lexical_max_decoded_bytes",
                )?,
                lexical_shards: routing_lexical_shards(&batch, row)?,
                created_at: datetime_from_millis(primitive_value_by_name::<Int64Type>(
                    &batch,
                    row,
                    "created_at_ms",
                )?)?,
            });
        }
    }

    validate_routing_segment_ids(&summaries)?;
    validate_routing_segment_paths(&summaries)?;
    validate_routing_segment_summary_metadata(&summaries)?;

    Ok(summaries)
}

pub(crate) fn segment_to_parquet(segment: &Segment) -> Result<Vec<u8>> {
    segment_to_parquet_impl(segment, false, VectorElementType::Float32)
}

pub(crate) type PositionedWalRecord = (VectorRecord, u64, u32);

pub(crate) fn positioned_wal_records_to_table(
    records: &[PositionedWalRecord],
    dimensions: usize,
    element_type: VectorElementType,
    format: crate::PhysicalFormat,
) -> Result<Vec<u8>> {
    let logical_records = records
        .iter()
        .map(|(record, _, _)| record)
        .collect::<Vec<_>>();
    let batch = wal_records_to_batch(&logical_records, dimensions, element_type)?;
    let mut fields = batch
        .schema()
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect::<Vec<_>>();
    fields.push(Field::new("routing_epoch", DataType::UInt64, false));
    fields.push(Field::new("cell_ordinal", DataType::UInt32, false));
    let mut columns = batch.columns().to_vec();
    columns.push(array(UInt64Array::from_iter_values(
        records.iter().map(|(_, epoch, _)| *epoch),
    )));
    columns.push(array(UInt32Array::from_iter_values(
        records.iter().map(|(_, _, ordinal)| *ordinal),
    )));
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)?;
    match format {
        crate::PhysicalFormat::Parquet => {
            write_batch_with_row_groups(batch, Some(POSITIONED_WAL_ROW_GROUP_ROWS))
        }
        other => Err(BorsukError::InvalidStorage(format!(
            "positioned WAL records cannot use physical format `{other}`"
        ))),
    }
}

fn wal_records_to_batch(
    records: &[&VectorRecord],
    dimensions: usize,
    vector_element_type: VectorElementType,
) -> Result<RecordBatch> {
    if records.is_empty() {
        return Err(BorsukError::InvalidStorage(
            "WAL record object requires at least one record".to_string(),
        ));
    }
    let stored_dimensions = u32::try_from(dimensions)
        .map_err(|_| BorsukError::InvalidStorage("WAL vector dimensions exceed u32".to_string()))?;
    validate_segment_record_ids(records)?;
    for record in records {
        validate_segment_record_dimensions(&record.id, dimensions, record.vector.len())?;
        validate_segment_record_vector_values(&record.id, &record.vector)?;
        validate_segment_record_text_terms(record)?;
    }

    let mut sparse_indices = Vec::<Option<Vec<u32>>>::with_capacity(records.len());
    let mut sparse_values = Vec::<Option<Vec<f32>>>::with_capacity(records.len());
    let mut include_sparse = false;
    for record in records {
        match record.storage.resolve_for_vector(&record.vector) {
            StorageEncoding::Dense => {
                sparse_indices.push(None);
                sparse_values.push(None);
            }
            StorageEncoding::Sparse => {
                include_sparse = true;
                let (indices, values) = sparse_parts_from_dense(&record.id, &record.vector)?;
                sparse_indices.push(Some(indices));
                sparse_values.push(Some(values));
            }
            StorageEncoding::Auto => unreachable!("storage encoding should be resolved"),
        }
    }
    let include_text = records
        .iter()
        .any(|record| !record.text_term_ids.is_empty());
    let include_generation = records.iter().any(|record| record.generation != 0);
    let include_mutation_stamp = mutation_stamps_present(records)?;
    let schema = wal_records_schema(
        dimensions,
        include_sparse,
        include_text,
        include_generation,
        include_mutation_stamp,
        vector_element_type,
    )?;
    let mut columns = vec![
        array(BinaryArray::from_iter_values(
            records.iter().map(|record| record.id.as_bytes()),
        )),
        array(BinaryArray::from_iter_values(
            records
                .iter()
                .map(|record| crate::metadata::encode(&record.metadata)),
        )),
    ];
    if include_sparse {
        columns.push(array(optional_u32_list_array(
            sparse_indices.iter().map(|indices| indices.as_deref()),
        )));
        columns.push(array(optional_f32_list_array(
            sparse_values.iter().map(|values| values.as_deref()),
        )));
    }
    if include_text {
        columns.push(array(sparse_u32_list_array(
            records.iter().map(|record| record.text_term_ids.as_slice()),
        )));
        columns.push(array(sparse_u32_list_array(
            records
                .iter()
                .map(|record| record.text_term_freqs.as_slice()),
        )));
    }
    if include_generation {
        columns.push(array(UInt64Array::from_iter_values(
            records.iter().map(|record| record.generation),
        )));
    }
    if include_mutation_stamp {
        columns.extend(mutation_stamp_arrays(records)?);
    }
    columns.push(optional_typed_vector_array(
        records,
        &sparse_indices,
        dimensions,
        vector_element_type,
    )?);
    let extras = records
        .iter()
        .map(|record| {
            serde_json::to_vec(&WalRecordExtras {
                extra_vectors: record.extra_vectors.clone(),
                extra_sparse: record.extra_sparse.clone(),
                extra_multi_vectors: record.extra_multi_vectors.clone(),
                storage: record.storage,
            })
            .map_err(|error| {
                BorsukError::InvalidStorage(format!(
                    "failed to serialize WAL record extras: {error}"
                ))
            })
        })
        .collect::<Result<Vec<_>>>()?;
    columns.push(array(BinaryArray::from_iter_values(
        extras.iter().map(Vec::as_slice),
    )));
    columns.push(array(UInt8Array::from_iter_values(
        records
            .iter()
            .map(|_| wal_vector_element_type_code(vector_element_type)),
    )));
    columns.push(array(UInt32Array::from_iter_values(
        records.iter().map(|_| stored_dimensions),
    )));

    Ok(RecordBatch::try_new(schema, columns)?)
}

/// Decode a WAL object back into its records, reconstructing each row's primary
/// vector from the dedicated record-only table.
pub(crate) fn wal_records_from_table(bytes: Vec<u8>, path: &str) -> Result<Vec<VectorRecord>> {
    let batches = if path.ends_with(".parquet") {
        read_batches(&bytes)?
    } else {
        return Err(BorsukError::InvalidStorage(format!(
            "WAL records object `{path}` has no supported table extension"
        )));
    };
    wal_records_from_batches(batches)
}

pub(crate) fn positioned_wal_records_from_table(
    bytes: Vec<u8>,
    path: &str,
) -> Result<Vec<PositionedWalRecord>> {
    if !path.ends_with(".parquet") {
        return Err(BorsukError::InvalidStorage(format!(
            "positioned WAL records object `{path}` has no supported table extension"
        )));
    }
    let batches = read_batches(&bytes)?;
    let mut owners = Vec::new();
    for batch in &batches {
        let epoch = batch.schema().index_of("routing_epoch").map_err(|_| {
            BorsukError::InvalidStorage(
                "positioned WAL table is missing required `routing_epoch`".to_string(),
            )
        })?;
        let ordinal = batch.schema().index_of("cell_ordinal").map_err(|_| {
            BorsukError::InvalidStorage(
                "positioned WAL table is missing required `cell_ordinal`".to_string(),
            )
        })?;
        for row in 0..batch.num_rows() {
            let epoch = primitive_value::<UInt64Type>(batch, epoch, row, "routing_epoch")?;
            let ordinal = primitive_value::<UInt32Type>(batch, ordinal, row, "cell_ordinal")?;
            if epoch == 0 {
                return Err(BorsukError::InvalidStorage(
                    "positioned WAL routing epoch must be positive".to_string(),
                ));
            }
            owners.push((epoch, ordinal));
        }
    }
    let records = wal_records_from_batches(batches)?;
    if records.len() != owners.len() {
        return Err(BorsukError::InvalidStorage(
            "positioned WAL record and owner cardinalities differ".to_string(),
        ));
    }
    Ok(records
        .into_iter()
        .zip(owners)
        .map(|(record, (epoch, ordinal))| (record, epoch, ordinal))
        .collect())
}

#[derive(serde::Serialize, serde::Deserialize)]
struct WalRecordExtras {
    extra_vectors: BTreeMap<String, Vec<f32>>,
    extra_sparse: BTreeMap<String, crate::SparseVector>,
    extra_multi_vectors: BTreeMap<String, crate::LateInteractionVector>,
    storage: crate::StorageEncoding,
}

type MutationStampColumns = (usize, usize, usize);

fn mutation_stamp_columns(schema: &Schema) -> Result<Option<MutationStampColumns>> {
    let columns = [
        schema.index_of("mutation_hlc").ok(),
        schema.index_of("mutation_writer").ok(),
        schema.index_of("mutation_digest").ok(),
    ];
    match columns {
        [None, None, None] => Ok(None),
        [Some(hlc), Some(writer), Some(digest)] => Ok(Some((hlc, writer, digest))),
        _ => Err(BorsukError::InvalidStorage(
            "record table must contain mutation_hlc, mutation_writer, and mutation_digest together"
                .to_string(),
        )),
    }
}

fn mutation_stamp_value(
    batch: &RecordBatch,
    columns: Option<MutationStampColumns>,
    row: usize,
) -> Result<Option<MutationStamp>> {
    let Some((hlc_column, writer_column, digest_column)) = columns else {
        return Ok(None);
    };
    let hlc = primitive_value::<UInt64Type>(batch, hlc_column, row, "mutation_hlc")?;
    let writer = fixed_size_binary_value::<16>(batch, writer_column, row, "mutation_writer")?;
    let digest = fixed_size_binary_value::<32>(batch, digest_column, row, "mutation_digest")?;
    Ok(Some(MutationStamp::new(
        MutationVersion::from_parts(hlc, writer),
        digest,
    )))
}

fn fixed_size_binary_value<const N: usize>(
    batch: &RecordBatch,
    column: usize,
    row: usize,
    name: &str,
) -> Result<[u8; N]> {
    let column = batch.column(column);
    if let Some(array) = column.as_any().downcast_ref::<FixedSizeBinaryArray>() {
        if array.is_null(row) {
            return Err(BorsukError::InvalidStorage(format!(
                "column `{name}` contains a null value"
            )));
        }
        return array.value(row).try_into().map_err(|_| {
            BorsukError::InvalidStorage(format!("column `{name}` must contain exactly {N} bytes"))
        });
    }
    let array = column
        .as_any()
        .downcast_ref::<FixedSizeListArray>()
        .filter(|array| array.value_length() == N as i32)
        .ok_or_else(|| {
            BorsukError::InvalidStorage(format!("column `{name}` has wrong physical type"))
        })?;
    if array.is_null(row) {
        return Err(BorsukError::InvalidStorage(format!(
            "column `{name}` contains a null value"
        )));
    }
    let values = array.value(row);
    let values = values
        .as_any()
        .downcast_ref::<UInt8Array>()
        .ok_or_else(|| {
            BorsukError::InvalidStorage(format!("column `{name}` list values are not UInt8"))
        })?;
    let mut decoded = [0_u8; N];
    for (position, value) in decoded.iter_mut().enumerate() {
        if values.is_null(position) {
            return Err(BorsukError::InvalidStorage(format!(
                "column `{name}` contains a null byte"
            )));
        }
        *value = values.value(position);
    }
    Ok(decoded)
}

fn wal_records_from_batches(batches: Vec<RecordBatch>) -> Result<Vec<VectorRecord>> {
    let mut records = Vec::new();
    let mut expected_element_type = None;
    let mut expected_dimensions = None;
    for batch in batches {
        let schema = batch.schema();
        let record_id_column = schema.index_of("record_id").map_err(|_| {
            BorsukError::InvalidStorage("WAL table is missing `record_id`".to_string())
        })?;
        let metadata_column = schema.index_of("metadata").map_err(|_| {
            BorsukError::InvalidStorage("WAL table is missing `metadata`".to_string())
        })?;
        let vector_column = schema.index_of("vector").map_err(|_| {
            BorsukError::InvalidStorage("WAL table is missing `vector`".to_string())
        })?;
        let extras_column = schema.index_of("wal_record_extras").map_err(|_| {
            BorsukError::InvalidStorage("WAL table is missing `wal_record_extras`".to_string())
        })?;
        let element_type_column = schema.index_of("wal_vector_element_type").map_err(|_| {
            BorsukError::InvalidStorage(
                "WAL table is missing `wal_vector_element_type`".to_string(),
            )
        })?;
        let dimensions_column = schema.index_of("wal_vector_dimensions").map_err(|_| {
            BorsukError::InvalidStorage("WAL table is missing `wal_vector_dimensions`".to_string())
        })?;
        let sparse_indices_column = schema.index_of("sparse_indices").ok();
        let sparse_values_column = schema.index_of("sparse_values").ok();
        if sparse_indices_column.is_some() != sparse_values_column.is_some() {
            return Err(BorsukError::InvalidStorage(
                "WAL table must contain both sparse_indices and sparse_values columns".to_string(),
            ));
        }
        let text_term_ids_column = schema.index_of("text_term_ids").ok();
        let text_term_freqs_column = schema.index_of("text_term_freqs").ok();
        if text_term_ids_column.is_some() != text_term_freqs_column.is_some() {
            return Err(BorsukError::InvalidStorage(
                "WAL table must contain both text_term_ids and text_term_freqs columns".to_string(),
            ));
        }
        let generation_column = schema.index_of("generation").ok();
        let mutation_stamp_columns = mutation_stamp_columns(&schema)?;

        for row in 0..batch.num_rows() {
            let element_type = wal_vector_element_type_from_code(primitive_value::<UInt8Type>(
                &batch,
                element_type_column,
                row,
                "wal_vector_element_type",
            )?)?;
            if expected_element_type.is_some_and(|expected| expected != element_type) {
                return Err(BorsukError::InvalidStorage(
                    "WAL table has inconsistent `wal_vector_element_type` values".to_string(),
                ));
            }
            expected_element_type = Some(element_type);

            let dimensions = usize::try_from(primitive_value::<UInt32Type>(
                &batch,
                dimensions_column,
                row,
                "wal_vector_dimensions",
            )?)
            .map_err(|_| {
                BorsukError::InvalidStorage(
                    "WAL table `wal_vector_dimensions` exceeds usize".to_string(),
                )
            })?;
            if dimensions == 0 {
                return Err(BorsukError::InvalidStorage(
                    "WAL table `wal_vector_dimensions` must be positive".to_string(),
                ));
            }
            if expected_dimensions.is_some_and(|expected| expected != dimensions) {
                return Err(BorsukError::InvalidStorage(
                    "WAL table has inconsistent `wal_vector_dimensions` values".to_string(),
                ));
            }
            expected_dimensions = Some(dimensions);

            let id = record_id_value(&batch, record_id_column, row, "record_id")?;
            let metadata =
                crate::metadata::decode(binary_value(&batch, metadata_column, row, "metadata")?)?;
            let (vector, _encoding) = decode_segment_vector(
                &batch,
                row,
                &id,
                dimensions,
                Some(vector_column),
                sparse_indices_column,
                sparse_values_column,
                element_type,
            )?;
            let (text_term_ids, text_term_freqs) =
                match (text_term_ids_column, text_term_freqs_column) {
                    (Some(ids_column), Some(freqs_column)) => {
                        let ids = primitive_list_optional_value::<UInt32Type>(
                            &batch,
                            ids_column,
                            row,
                            "text_term_ids",
                        )?
                        .unwrap_or_default();
                        let freqs = primitive_list_optional_value::<UInt32Type>(
                            &batch,
                            freqs_column,
                            row,
                            "text_term_freqs",
                        )?
                        .unwrap_or_default();
                        validate_text_terms(&id, &ids, &freqs)?;
                        (ids, freqs)
                    }
                    (None, None) => (Vec::new(), Vec::new()),
                    _ => unreachable!("text term column presence checked above"),
                };
            let generation = match generation_column {
                Some(column) => primitive_value::<UInt64Type>(&batch, column, row, "generation")?,
                None => 0,
            };
            let mutation_stamp = mutation_stamp_value(&batch, mutation_stamp_columns, row)?;
            let extras: WalRecordExtras = serde_json::from_slice(binary_value(
                &batch,
                extras_column,
                row,
                "wal_record_extras",
            )?)
            .map_err(|error| {
                BorsukError::InvalidStorage(format!("failed to decode WAL record extras: {error}"))
            })?;
            records.push(VectorRecord {
                id,
                vector,
                extra_vectors: extras.extra_vectors,
                extra_sparse: extras.extra_sparse,
                extra_multi_vectors: extras.extra_multi_vectors,
                storage: extras.storage,
                text: None,
                text_term_ids,
                text_term_freqs,
                metadata,
                generation,
                mutation_stamp,
            });
        }
    }
    if records.is_empty() {
        return Err(BorsukError::InvalidStorage(
            "WAL table must contain at least one row".to_string(),
        ));
    }
    validate_segment_record_ids(&records)?;
    Ok(records)
}

fn segment_to_parquet_impl(
    segment: &Segment,
    include_vectors: bool,
    vector_element_type: VectorElementType,
) -> Result<Vec<u8>> {
    let batch = segment_to_batch_impl(segment, include_vectors, vector_element_type)?;
    write_batch_with_row_groups(batch, Some(SEGMENT_ROW_GROUP_ROWS))
}

fn segment_to_batch_impl(
    segment: &Segment,
    include_vectors: bool,
    vector_element_type: VectorElementType,
) -> Result<RecordBatch> {
    validate_segment_centroid_dimensions(&segment.id, segment.dimensions, segment.centroid.len())?;
    validate_segment_centroid_values(&segment.id, &segment.centroid)?;
    validate_segment_radius(&segment.id, segment.radius)?;
    validate_segment_routing_code_count(
        &segment.id,
        segment.records.len(),
        segment.routing_codes.len(),
    )?;
    validate_segment_pq_code_count(&segment.id, segment.records.len(), segment.pq_codes.len())?;
    validate_segment_record_ids(&segment.records)?;
    // Coarse codes are `dimensions`-wide for ScalarBounds but SRHT-padded (next
    // power of two) for TurboQuant, so validate every code against the segment's
    // own coarse-code width rather than the raw dimensionality. With TurboQuant's
    // stage-2 QJL residual, each code also carries a fixed self-describing tail, so
    // the `pq_code` column width (`pq_code_len`) can exceed the bounds width
    // (`coarse_code_len`, used for `pq_min`/`pq_max`).
    let pq_code_dimensions = segment.pq_code_len();
    for ((record, routing_code), pq_code) in segment
        .records
        .iter()
        .zip(&segment.routing_codes)
        .zip(&segment.pq_codes)
    {
        validate_segment_record_dimensions(&record.id, segment.dimensions, record.vector.len())?;
        validate_segment_routing_code(&record.id, *routing_code)?;
        validate_segment_pq_code_dimensions(&record.id, pq_code_dimensions, pq_code.len())?;
        validate_segment_record_vector_values(&record.id, &record.vector)?;
        validate_segment_record_text_terms(record)?;
    }

    let records = &segment.records;
    let mut sparse_indices = Vec::<Option<Vec<u32>>>::with_capacity(records.len());
    let mut sparse_values = Vec::<Option<Vec<f32>>>::with_capacity(records.len());
    let mut include_sparse = false;
    for record in records {
        match record.storage.resolve_for_vector(&record.vector) {
            StorageEncoding::Dense => {
                // Dense vectors live only in the Arrow IPC sidecar now, so the
                // Parquet segment carries no dense-vector column. A dense row is
                // simply one with no sparse encoding.
                sparse_indices.push(None);
                sparse_values.push(None);
            }
            StorageEncoding::Sparse => {
                include_sparse = true;
                let (indices, values) = sparse_parts_from_dense(&record.id, &record.vector)?;
                sparse_indices.push(Some(indices));
                sparse_values.push(Some(values));
            }
            StorageEncoding::Auto => unreachable!("storage encoding should be resolved"),
        }
    }
    let include_text = records
        .iter()
        .any(|record| !record.text_term_ids.is_empty());
    let include_generation = records.iter().any(|record| record.generation != 0);
    let include_mutation_stamp = mutation_stamps_present(records)?;
    let schema = segment_schema(
        segment.dimensions,
        pq_code_dimensions,
        include_sparse,
        include_text,
        include_generation,
        include_mutation_stamp,
        include_vectors,
        vector_element_type,
    );
    let header = encode_segment_header(segment)?;
    let mut columns = vec![
        array(BinaryArray::from_iter(
            (0..records.len()).map(|row| (row == 0).then_some(header.as_slice())),
        )),
        array(Float32Array::from_iter_values(
            segment.routing_codes.iter().copied(),
        )),
        array(fixed_u8_array(
            segment.pq_codes.iter().map(Vec::as_slice),
            pq_code_dimensions,
        )),
        array(BinaryArray::from_iter_values(
            records.iter().map(|record| record.id.as_bytes()),
        )),
        array(BinaryArray::from_iter_values(
            records
                .iter()
                .map(|record| crate::metadata::encode(&record.metadata)),
        )),
    ];
    if include_sparse {
        columns.push(array(optional_u32_list_array(
            sparse_indices.iter().map(|indices| indices.as_deref()),
        )));
        columns.push(array(optional_f32_list_array(
            sparse_values.iter().map(|values| values.as_deref()),
        )));
    }
    if include_text {
        columns.push(array(sparse_u32_list_array(
            records.iter().map(|record| record.text_term_ids.as_slice()),
        )));
        columns.push(array(sparse_u32_list_array(
            records
                .iter()
                .map(|record| record.text_term_freqs.as_slice()),
        )));
    }
    if include_generation {
        columns.push(array(UInt64Array::from_iter_values(
            records.iter().map(|record| record.generation),
        )));
    }
    if include_mutation_stamp {
        columns.extend(mutation_stamp_arrays(records)?);
    }
    if include_vectors {
        // WAL objects inline the dense vector so the un-flushed tail is fully
        // searchable without a sidecar. A row is dense exactly when it has no
        // sparse encoding: dense rows carry their full vector, sparse rows write
        // null here (their vector is reconstructed from the sparse columns) so a
        // row never carries both a dense and a sparse encoding.
        columns.push(optional_typed_vector_array(
            records,
            &sparse_indices,
            segment.dimensions,
            vector_element_type,
        )?);
        let extras = records
            .iter()
            .map(|record| {
                serde_json::to_vec(&WalRecordExtras {
                    extra_vectors: record.extra_vectors.clone(),
                    extra_sparse: record.extra_sparse.clone(),
                    extra_multi_vectors: record.extra_multi_vectors.clone(),
                    storage: record.storage,
                })
                .map_err(|error| {
                    BorsukError::InvalidStorage(format!(
                        "failed to serialize WAL record extras: {error}"
                    ))
                })
            })
            .collect::<Result<Vec<_>>>()?;
        columns.push(array(BinaryArray::from_iter_values(
            extras.iter().map(Vec::as_slice),
        )));
        columns.push(array(UInt8Array::from_iter_values(
            records
                .iter()
                .map(|_| wal_vector_element_type_code(vector_element_type)),
        )));
    }
    let batch = RecordBatch::try_new(Arc::clone(&schema), columns)?;

    Ok(batch)
}

pub(crate) fn segment_from_parquet(bytes: &[u8]) -> Result<Segment> {
    segment_from_parquet_impl(bytes, false)
}

/// True when the segment carries persisted PQ bounds, so it can be decoded
/// lean (without the vector column) and still quantize queries.
#[cfg(test)]
pub(crate) fn segment_has_persisted_pq_bounds(bytes: &[u8]) -> Result<bool> {
    let builder = ParquetRecordBatchReaderBuilder::try_new(Bytes::copy_from_slice(bytes))?;
    let fields = builder.schema().fields();
    Ok(fields.iter().any(|field| field.name() == "segment_header"))
}

/// Decode a segment for candidate selection without materializing the `vector`
/// column: records carry ids, routing codes, and PQ codes but empty vectors,
/// and the persisted PQ bounds let queries be quantized. Chosen candidates'
/// vectors are fetched with [`segment_vectors_for_rows`].
pub(crate) fn lean_segment_from_parquet(bytes: &[u8]) -> Result<Segment> {
    let header = read_lean_segment_header(bytes)?;
    let batches = read_lean_segment_row_batches(bytes)?;
    segment_from_batches_with_header(batches, true, Some(header))
}

fn segment_from_parquet_impl(bytes: &[u8], lean: bool) -> Result<Segment> {
    if lean {
        lean_segment_from_parquet(bytes)
    } else {
        segment_from_batches(read_batches_projected(bytes, false, None)?, false)
    }
}

fn read_lean_segment_header(bytes: &[u8]) -> Result<LeanSegmentHeader> {
    let builder = ParquetRecordBatchReaderBuilder::try_new(Bytes::copy_from_slice(bytes))?;
    let total_rows =
        usize::try_from(builder.metadata().file_metadata().num_rows()).map_err(|_| {
            BorsukError::InvalidStorage("segment row count does not fit usize".to_string())
        })?;
    if total_rows == 0 {
        return Err(BorsukError::InvalidStorage(
            "segment table must contain at least one row".to_string(),
        ));
    }
    let selection = row_selection_for_rows(&[0], total_rows);
    let batches =
        read_batches_projected_columns(bytes, LEAN_SEGMENT_HEADER_COLUMNS, Some(selection))?;
    let batch = batches.first().ok_or_else(|| {
        BorsukError::InvalidStorage("segment table must contain at least one row".to_string())
    })?;
    lean_segment_header_from_batch(batch)
}

fn lean_segment_header_from_batch(batch: &RecordBatch) -> Result<LeanSegmentHeader> {
    if batch.num_rows() == 0 {
        return Err(BorsukError::InvalidStorage(
            "segment table must contain at least one row".to_string(),
        ));
    }
    let column = column_index(batch, "segment_header")?;
    let array = batch
        .column(column)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .ok_or_else(|| {
            BorsukError::InvalidStorage("column `segment_header` has wrong type".to_string())
        })?;
    if array.is_null(0) {
        return Err(BorsukError::InvalidStorage(
            "segment table row zero is missing its packed header".to_string(),
        ));
    }
    decode_segment_header(array.value(0))
}

fn read_lean_segment_row_batches(bytes: &[u8]) -> Result<Vec<RecordBatch>> {
    read_batches_projected_columns(bytes, LEAN_SEGMENT_ROW_COLUMNS, None)
}

fn packed_segment_header(segment: &Segment) -> LeanSegmentHeader {
    LeanSegmentHeader {
        format_version: CURRENT_VERSION,
        metadata: SegmentMetadata {
            id: segment.id.clone(),
            level: segment.level,
            metric: segment.metric.clone(),
            dimensions: segment.dimensions,
            centroid: segment.centroid.clone(),
            radius: segment.radius,
            created_at: segment.created_at,
        },
        pq_bounds: (segment.pq_min.clone(), segment.pq_max.clone()),
    }
}

fn encode_segment_header(segment: &Segment) -> Result<Vec<u8>> {
    let header = packed_segment_header(segment);
    encode_packed_segment_header(&header)
}

fn encode_packed_segment_header(header: &LeanSegmentHeader) -> Result<Vec<u8>> {
    validate_packed_segment_header(header)?;
    encode_packed_segment_header_unchecked(header)
}

fn encode_packed_segment_header_unchecked(header: &LeanSegmentHeader) -> Result<Vec<u8>> {
    let metadata = &header.metadata;
    let id = metadata.id.as_bytes();
    let metric = metadata.metric.to_string();
    let metric = metric.as_bytes();
    let dimensions = u32::try_from(metadata.dimensions).map_err(|_| {
        BorsukError::InvalidStorage(format!(
            "segment `{}` dimensions exceed packed-header limits",
            metadata.id
        ))
    })?;
    let centroid_len = u32::try_from(metadata.centroid.len()).map_err(|_| {
        BorsukError::InvalidStorage(format!(
            "segment `{}` centroid exceeds packed-header limits",
            metadata.id
        ))
    })?;
    let pq_len = u32::try_from(header.pq_bounds.0.len()).map_err(|_| {
        BorsukError::InvalidStorage(format!(
            "segment `{}` PQ bounds exceed packed-header limits",
            metadata.id
        ))
    })?;
    let id_len = u32::try_from(id.len()).map_err(|_| {
        BorsukError::InvalidStorage("segment id exceeds packed-header limits".to_string())
    })?;
    let metric_len = u32::try_from(metric.len()).map_err(|_| {
        BorsukError::InvalidStorage("segment metric exceeds packed-header limits".to_string())
    })?;

    let mut bytes = Vec::new();
    bytes.extend_from_slice(SEGMENT_HEADER_MAGIC);
    bytes.push(SEGMENT_HEADER_CODEC_VERSION);
    bytes.extend_from_slice(&header.format_version.to_le_bytes());
    bytes.push(metadata.level);
    bytes.extend_from_slice(&dimensions.to_le_bytes());
    bytes.extend_from_slice(&centroid_len.to_le_bytes());
    bytes.extend_from_slice(&pq_len.to_le_bytes());
    bytes.extend_from_slice(&id_len.to_le_bytes());
    bytes.extend_from_slice(&metric_len.to_le_bytes());
    bytes.extend_from_slice(&metadata.created_at.timestamp().to_le_bytes());
    bytes.extend_from_slice(&metadata.created_at.timestamp_subsec_nanos().to_le_bytes());
    bytes.extend_from_slice(&metadata.radius.to_bits().to_le_bytes());
    bytes.extend_from_slice(id);
    bytes.extend_from_slice(metric);
    for value in &metadata.centroid {
        bytes.extend_from_slice(&value.to_bits().to_le_bytes());
    }
    for value in &header.pq_bounds.0 {
        bytes.extend_from_slice(&value.to_bits().to_le_bytes());
    }
    for value in &header.pq_bounds.1 {
        bytes.extend_from_slice(&value.to_bits().to_le_bytes());
    }
    let checksum = blake3::hash(&bytes);
    bytes.extend_from_slice(checksum.as_bytes());
    Ok(bytes)
}

fn decode_segment_header(bytes: &[u8]) -> Result<LeanSegmentHeader> {
    if bytes.len() < 44 + SEGMENT_HEADER_CHECKSUM_LEN {
        return Err(BorsukError::InvalidStorage(
            "packed segment header is truncated".to_string(),
        ));
    }
    let payload_len = bytes.len() - SEGMENT_HEADER_CHECKSUM_LEN;
    let (payload, stored_checksum) = bytes.split_at(payload_len);
    let expected_checksum = blake3::hash(payload);
    if stored_checksum != expected_checksum.as_bytes() {
        return Err(BorsukError::InvalidStorage(
            "packed segment header checksum mismatch".to_string(),
        ));
    }

    let mut cursor = 0;
    if take_segment_header_bytes(payload, &mut cursor, SEGMENT_HEADER_MAGIC.len())?
        != SEGMENT_HEADER_MAGIC
    {
        return Err(BorsukError::InvalidStorage(
            "packed segment header magic is invalid".to_string(),
        ));
    }
    let codec_version = read_segment_header_u8(payload, &mut cursor)?;
    if codec_version != SEGMENT_HEADER_CODEC_VERSION {
        return Err(BorsukError::InvalidStorage(format!(
            "unsupported packed segment header codec version {codec_version}"
        )));
    }
    let format_version = read_segment_header_u16(payload, &mut cursor)?;
    let level = read_segment_header_u8(payload, &mut cursor)?;
    let dimensions = read_segment_header_u32(payload, &mut cursor)? as usize;
    let centroid_len = read_segment_header_u32(payload, &mut cursor)? as usize;
    let pq_len = read_segment_header_u32(payload, &mut cursor)? as usize;
    let id_len = read_segment_header_u32(payload, &mut cursor)? as usize;
    let metric_len = read_segment_header_u32(payload, &mut cursor)? as usize;
    let created_at_seconds = read_segment_header_i64(payload, &mut cursor)?;
    let created_at_nanos = read_segment_header_u32(payload, &mut cursor)?;
    let radius = f32::from_bits(read_segment_header_u32(payload, &mut cursor)?);

    let float_values = centroid_len
        .checked_add(
            pq_len
                .checked_mul(2)
                .ok_or_else(segment_header_length_overflow)?,
        )
        .ok_or_else(segment_header_length_overflow)?;
    let expected_remaining = id_len
        .checked_add(metric_len)
        .and_then(|size| {
            float_values
                .checked_mul(4)
                .and_then(|tail| size.checked_add(tail))
        })
        .ok_or_else(segment_header_length_overflow)?;
    if payload.len().saturating_sub(cursor) != expected_remaining {
        return Err(BorsukError::InvalidStorage(
            "packed segment header lengths do not match its payload".to_string(),
        ));
    }

    let id = std::str::from_utf8(take_segment_header_bytes(payload, &mut cursor, id_len)?)
        .map_err(|_| {
            BorsukError::InvalidStorage("packed segment id is not valid UTF-8".to_string())
        })?
        .to_string();
    let metric_name =
        std::str::from_utf8(take_segment_header_bytes(payload, &mut cursor, metric_len)?).map_err(
            |_| BorsukError::InvalidStorage("packed segment metric is not valid UTF-8".to_string()),
        )?;
    let metric = VectorMetric::from_str(metric_name).map_err(|error| {
        BorsukError::InvalidStorage(format!("packed segment metric is invalid: {error}"))
    })?;
    let centroid = read_segment_header_f32s(payload, &mut cursor, centroid_len)?;
    let pq_min = read_segment_header_f32s(payload, &mut cursor, pq_len)?;
    let pq_max = read_segment_header_f32s(payload, &mut cursor, pq_len)?;
    if cursor != payload.len() {
        return Err(BorsukError::InvalidStorage(
            "packed segment header contains trailing payload".to_string(),
        ));
    }
    let created_at = DateTime::<Utc>::from_timestamp(created_at_seconds, created_at_nanos)
        .ok_or_else(|| {
            BorsukError::InvalidStorage("packed segment timestamp is out of range".to_string())
        })?;
    let header = LeanSegmentHeader {
        format_version,
        metadata: SegmentMetadata {
            id,
            level,
            metric,
            dimensions,
            centroid,
            radius,
            created_at,
        },
        pq_bounds: (pq_min, pq_max),
    };
    validate_packed_segment_header(&header)?;
    Ok(header)
}

fn segment_header_length_overflow() -> BorsukError {
    BorsukError::InvalidStorage("packed segment header length overflows usize".to_string())
}

fn take_segment_header_bytes<'a>(
    bytes: &'a [u8],
    cursor: &mut usize,
    length: usize,
) -> Result<&'a [u8]> {
    let end = cursor
        .checked_add(length)
        .ok_or_else(segment_header_length_overflow)?;
    let value = bytes.get(*cursor..end).ok_or_else(|| {
        BorsukError::InvalidStorage("packed segment header is truncated".to_string())
    })?;
    *cursor = end;
    Ok(value)
}

fn read_segment_header_u8(bytes: &[u8], cursor: &mut usize) -> Result<u8> {
    Ok(take_segment_header_bytes(bytes, cursor, 1)?[0])
}

fn read_segment_header_u16(bytes: &[u8], cursor: &mut usize) -> Result<u16> {
    let bytes: [u8; 2] = take_segment_header_bytes(bytes, cursor, 2)?
        .try_into()
        .expect("segment header helper returned two bytes");
    Ok(u16::from_le_bytes(bytes))
}

fn read_segment_header_u32(bytes: &[u8], cursor: &mut usize) -> Result<u32> {
    let bytes: [u8; 4] = take_segment_header_bytes(bytes, cursor, 4)?
        .try_into()
        .expect("segment header helper returned four bytes");
    Ok(u32::from_le_bytes(bytes))
}

fn read_segment_header_i64(bytes: &[u8], cursor: &mut usize) -> Result<i64> {
    let bytes: [u8; 8] = take_segment_header_bytes(bytes, cursor, 8)?
        .try_into()
        .expect("segment header helper returned eight bytes");
    Ok(i64::from_le_bytes(bytes))
}

fn read_segment_header_f32s(bytes: &[u8], cursor: &mut usize, length: usize) -> Result<Vec<f32>> {
    let mut values = Vec::with_capacity(length);
    for _ in 0..length {
        values.push(f32::from_bits(read_segment_header_u32(bytes, cursor)?));
    }
    Ok(values)
}

fn validate_packed_segment_header(header: &LeanSegmentHeader) -> Result<()> {
    if header.format_version != CURRENT_VERSION {
        return Err(BorsukError::InvalidStorage(format!(
            "unsupported segment table version {}",
            header.format_version
        )));
    }
    let metadata = &header.metadata;
    validate_segment_centroid_dimensions(
        &metadata.id,
        metadata.dimensions,
        metadata.centroid.len(),
    )?;
    validate_segment_centroid_values(&metadata.id, &metadata.centroid)?;
    validate_segment_radius(&metadata.id, metadata.radius)?;
    let (pq_min, pq_max) = &header.pq_bounds;
    if pq_min.is_empty() || pq_min.len() != pq_max.len() {
        return Err(BorsukError::InvalidStorage(format!(
            "segment `{}` PQ bounds must be non-empty and have equal lengths",
            metadata.id
        )));
    }
    for (coordinate, (&min, &max)) in pq_min.iter().zip(pq_max).enumerate() {
        if !min.is_finite() || !max.is_finite() || min > max {
            return Err(BorsukError::InvalidStorage(format!(
                "segment `{}` has invalid PQ bounds at coordinate {coordinate}",
                metadata.id
            )));
        }
    }
    Ok(())
}

/// Decode a lean segment (records carry ids, routing/PQ codes, and empty
/// vectors) from Parquet batches that were already fetched with a ranged,
/// vector-excluding projection — the object-store-native scoring read's decode
/// half. Chosen candidates' vectors are then range-read separately.
#[allow(dead_code)]
pub(crate) fn lean_segment_from_batches(batches: Vec<RecordBatch>) -> Result<Segment> {
    segment_from_batches(batches, true)
}

/// Decode a lean segment from a separately ranged header row and projected
/// record batches. Keeping the header out of the row projection lets object
/// storage fetch only the compact routing/PQ columns needed for candidate
/// selection.
pub(crate) fn lean_segment_from_header_and_batches(
    header: &RecordBatch,
    batches: Vec<RecordBatch>,
) -> Result<Segment> {
    let header = lean_segment_header_from_batch(header)?;
    segment_from_batches_with_header(batches, true, Some(header))
}

fn segment_from_batches(batches: Vec<RecordBatch>, lean: bool) -> Result<Segment> {
    segment_from_batches_with_header(batches, lean, None)
}

fn segment_from_batches_with_header(
    batches: Vec<RecordBatch>,
    lean: bool,
    header: Option<LeanSegmentHeader>,
) -> Result<Segment> {
    let mut records = Vec::new();
    let mut routing_codes = Vec::new();
    let mut pq_codes = Vec::new();
    let (mut metadata, mut pq_bounds) = match header {
        Some(header) => {
            if header.format_version != CURRENT_VERSION {
                return Err(BorsukError::InvalidStorage(format!(
                    "unsupported segment table version {}",
                    header.format_version
                )));
            }
            (Some(header.metadata), Some(header.pq_bounds))
        }
        None => (None, None),
    };

    for batch in batches {
        let segment_header_column = batch.schema().index_of("segment_header").ok();
        let routing_code_column = batch.schema().index_of("routing_code").map_err(|_| {
            BorsukError::InvalidStorage("segment table missing `routing_code` column".to_string())
        })?;
        let pq_code_column = batch.schema().index_of("pq_code").ok();
        let record_id_column = batch.schema().index_of("record_id").map_err(|_| {
            BorsukError::InvalidStorage("segment table missing `record_id` column".to_string())
        })?;
        let metadata_column = batch.schema().index_of("metadata").ok();
        let sparse_indices_column = batch.schema().index_of("sparse_indices").ok();
        let sparse_values_column = batch.schema().index_of("sparse_values").ok();
        if sparse_indices_column.is_some() != sparse_values_column.is_some() {
            return Err(BorsukError::InvalidStorage(
                "segment table must contain both sparse_indices and sparse_values columns"
                    .to_string(),
            ));
        }
        let text_term_ids_column = batch.schema().index_of("text_term_ids").ok();
        let text_term_freqs_column = batch.schema().index_of("text_term_freqs").ok();
        if text_term_ids_column.is_some() != text_term_freqs_column.is_some() {
            return Err(BorsukError::InvalidStorage(
                "segment table must contain both text_term_ids and text_term_freqs columns"
                    .to_string(),
            ));
        }
        let generation_column = batch.schema().index_of("generation").ok();
        let mutation_stamp_columns = mutation_stamp_columns(&batch.schema())?;
        // Normal Parquet segments carry no dense-vector column — their dense
        // vectors live in the per-segment Arrow IPC sidecar and are
        // reconstructed at the read boundary, so decode yields empty dense
        // vectors (sparse rows are still detected from the sparse columns).
        // Legacy segment-compatible inline-vector objects can carry a `vector`
        // column; current WAL runs use the dedicated record-only decoder.
        let vector_column = batch.schema().index_of("vector").ok();
        let wal_vector_element_type_column = if vector_column.is_some() {
            Some(
                batch
                    .schema()
                    .index_of("wal_vector_element_type")
                    .map_err(|_| {
                        BorsukError::InvalidStorage(
                            "WAL table is missing `wal_vector_element_type`".to_string(),
                        )
                    })?,
            )
        } else {
            None
        };
        let wal_record_extras_column = batch.schema().index_of("wal_record_extras").ok();
        if metadata.is_none() && batch.num_rows() > 0 {
            let column = segment_header_column.ok_or_else(|| {
                BorsukError::InvalidStorage(
                    "segment table is missing its packed header column".to_string(),
                )
            })?;
            let array = batch
                .column(column)
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| {
                    BorsukError::InvalidStorage(
                        "column `segment_header` has wrong type".to_string(),
                    )
                })?;
            if array.is_null(0) {
                return Err(BorsukError::InvalidStorage(
                    "segment table row zero is missing its packed header".to_string(),
                ));
            }
            let header = decode_segment_header(array.value(0))?;
            metadata = Some(header.metadata);
            pq_bounds = Some(header.pq_bounds);
        }
        for row in 0..batch.num_rows() {
            let row_dimensions = metadata
                .as_ref()
                .ok_or_else(|| {
                    BorsukError::InvalidStorage(
                        "segment table is missing its segment header".to_string(),
                    )
                })?
                .dimensions;

            let id = record_id_value(&batch, record_id_column, row, "record_id")?;
            let routing_code =
                primitive_value::<Float32Type>(&batch, routing_code_column, row, "routing_code")?;
            validate_segment_routing_code(&id, routing_code)?;
            if let Some(pq_code_column) = pq_code_column {
                let pq_code = fixed_u8_value(&batch, pq_code_column, row, "pq_code")?;
                // Coarse codes are `dimensions`-wide for ScalarBounds but SRHT-padded
                // (next power of two) for TurboQuant, and TurboQuant's stage-2 QJL
                // residual appends a fixed self-describing tail, so a code can be
                // WIDER than the persisted bounds. Require at least the bounds width
                // (so the scalar prefix is always present) rather than an exact
                // match; the fixed-list column already guarantees a uniform width
                // across rows. Fall back to the raw dimensionality for
                // legacy/bounds-less segments.
                let expected_coarse = pq_bounds
                    .as_ref()
                    .map(|(mins, _)| mins.len())
                    .unwrap_or(row_dimensions);
                validate_segment_pq_code_min_dimensions(&id, expected_coarse, pq_code.len())?;
                pq_codes.push(pq_code);
            }
            let metadata = match metadata_column {
                Some(column) => {
                    crate::metadata::decode(binary_value(&batch, column, row, "metadata")?)?
                }
                None => crate::Metadata::new(),
            };
            let vector_element_type = match wal_vector_element_type_column {
                Some(column) => wal_vector_element_type_from_code(primitive_value::<UInt8Type>(
                    &batch,
                    column,
                    row,
                    "wal_vector_element_type",
                )?)?,
                None => VectorElementType::Float32,
            };
            // The second element is the on-disk encoding; the record's `storage`
            // is a write-time hint, not persisted state, so a decoded record
            // round-trips as `Auto` (equal to how it was originally built).
            let (vector, _encoding) = decode_segment_vector(
                &batch,
                row,
                &id,
                row_dimensions,
                vector_column,
                sparse_indices_column,
                sparse_values_column,
                vector_element_type,
            )?;
            let (text_term_ids, text_term_freqs) =
                match (text_term_ids_column, text_term_freqs_column) {
                    (Some(ids_column), Some(freqs_column)) => {
                        let ids = primitive_list_optional_value::<UInt32Type>(
                            &batch,
                            ids_column,
                            row,
                            "text_term_ids",
                        )?
                        .unwrap_or_default();
                        let freqs = primitive_list_optional_value::<UInt32Type>(
                            &batch,
                            freqs_column,
                            row,
                            "text_term_freqs",
                        )?
                        .unwrap_or_default();
                        if ids.is_empty() && freqs.is_empty() {
                            (Vec::new(), Vec::new())
                        } else {
                            validate_text_terms(&id, &ids, &freqs)?;
                            (ids, freqs)
                        }
                    }
                    (None, None) => (Vec::new(), Vec::new()),
                    _ => unreachable!("text term column presence checked above"),
                };
            let generation = match generation_column {
                Some(column) => primitive_value::<UInt64Type>(&batch, column, row, "generation")?,
                None => 0,
            };
            let mutation_stamp = mutation_stamp_value(&batch, mutation_stamp_columns, row)?;
            let (extra_vectors, extra_sparse, extra_multi_vectors, storage) = if let Some(column) =
                wal_record_extras_column
            {
                let extras: WalRecordExtras =
                    serde_json::from_slice(binary_value(&batch, column, row, "wal_record_extras")?)
                        .map_err(|error| {
                            BorsukError::InvalidStorage(format!(
                                "failed to decode WAL record extras: {error}"
                            ))
                        })?;
                (
                    extras.extra_vectors,
                    extras.extra_sparse,
                    extras.extra_multi_vectors,
                    extras.storage,
                )
            } else {
                (
                    BTreeMap::new(),
                    BTreeMap::new(),
                    BTreeMap::new(),
                    crate::StorageEncoding::Auto,
                )
            };
            records.push(VectorRecord {
                id,
                vector,
                extra_vectors,
                extra_sparse,
                extra_multi_vectors,
                storage,
                text: None,
                text_term_ids,
                text_term_freqs,
                metadata,
                generation,
                mutation_stamp,
            });
            routing_codes.push(routing_code);
        }
    }

    let metadata = metadata.ok_or_else(|| {
        BorsukError::InvalidStorage("segment table must contain at least one row".to_string())
    })?;
    validate_segment_record_ids(&records)?;
    if pq_codes.is_empty() {
        if lean {
            return Err(BorsukError::InvalidStorage(
                "lean segment decode requires stored `pq_code` values".to_string(),
            ));
        }
        pq_codes = crate::segment::pq_codes_for_records(&records, metadata.dimensions)?;
    }
    validate_segment_pq_code_count(&metadata.id, records.len(), pq_codes.len())?;

    let (pq_min, pq_max) = match pq_bounds {
        Some(bounds) => bounds,
        None => {
            if lean {
                return Err(BorsukError::InvalidStorage(
                    "lean segment decode requires persisted PQ bounds".to_string(),
                ));
            }
            crate::segment::pq_bounds_for_records(&records, metadata.dimensions)?
        }
    };

    Ok(Segment {
        id: metadata.id,
        level: metadata.level,
        metric: metadata.metric,
        dimensions: metadata.dimensions,
        centroid: metadata.centroid,
        radius: metadata.radius,
        records,
        routing_codes,
        pq_codes,
        pq_min,
        pq_max,
        created_at: metadata.created_at,
    })
}

pub(crate) fn lexical_root_to_parquet(root: &LexicalRoot) -> Result<Vec<u8>> {
    root.validate()?;
    if root.pages.is_empty() {
        return Err(BorsukError::InvalidStorage(
            "lexical root must contain at least one term page".to_string(),
        ));
    }
    let schema = lexical_root_schema();
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            array(StringArray::from_iter_values(
                root.pages.iter().map(|_| root.kind.as_str()),
            )),
            array(UInt32Array::from_iter_values(
                root.pages.iter().map(|_| root.dimensions),
            )),
            array(UInt64Array::from_iter_values(
                root.pages.iter().map(|_| root.document_count),
            )),
            array(UInt64Array::from_iter_values(
                root.pages.iter().map(|_| root.total_document_length),
            )),
            array(UInt32Array::from_iter_values(
                root.pages.iter().map(|page| page.first_term),
            )),
            array(UInt32Array::from_iter_values(
                root.pages.iter().map(|page| page.last_term),
            )),
            array(StringArray::from_iter_values(
                root.pages.iter().map(|page| page.path.as_str()),
            )),
            array(StringArray::from_iter_values(
                root.pages.iter().map(|page| page.checksum.as_str()),
            )),
            array(StringArray::from_iter_values(
                root.pages.iter().map(|page| page.content_checksum.as_str()),
            )),
            array(UInt64Array::from_iter_values(
                root.pages.iter().map(|page| page.encoded_bytes),
            )),
            array(UInt32Array::from_iter_values(
                root.pages.iter().map(|page| page.term_count),
            )),
        ],
    )?;
    write_batch(batch)
}

pub(crate) fn lexical_root_from_parquet(bytes: &[u8]) -> Result<LexicalRoot> {
    let mut metadata = None;
    let mut pages = Vec::new();
    for batch in read_batches(bytes)? {
        for row in 0..batch.num_rows() {
            let row_metadata = (
                LexicalKind::from_str(string_value_by_name(&batch, row, "kind")?)?,
                primitive_value_by_name::<UInt32Type>(&batch, row, "dimensions")?,
                primitive_value_by_name::<UInt64Type>(&batch, row, "document_count")?,
                primitive_value_by_name::<UInt64Type>(&batch, row, "total_document_length")?,
            );
            if metadata
                .as_ref()
                .is_some_and(|value| value != &row_metadata)
            {
                return Err(BorsukError::InvalidStorage(
                    "lexical root metadata differs between rows".to_string(),
                ));
            }
            metadata.get_or_insert(row_metadata);
            pages.push(LexicalTermPageRef {
                first_term: primitive_value_by_name::<UInt32Type>(&batch, row, "first_term")?,
                last_term: primitive_value_by_name::<UInt32Type>(&batch, row, "last_term")?,
                path: string_value_by_name(&batch, row, "page_path")?.to_string(),
                checksum: string_value_by_name(&batch, row, "page_checksum")?.to_string(),
                content_checksum: string_value_by_name(&batch, row, "page_content_checksum")?
                    .to_string(),
                encoded_bytes: primitive_value_by_name::<UInt64Type>(
                    &batch,
                    row,
                    "page_encoded_bytes",
                )?,
                term_count: primitive_value_by_name::<UInt32Type>(&batch, row, "page_term_count")?,
            });
        }
    }
    let (kind, dimensions, document_count, total_document_length) = metadata.ok_or_else(|| {
        BorsukError::InvalidStorage("lexical root Parquet table is empty".to_string())
    })?;
    let root = LexicalRoot {
        kind,
        dimensions,
        document_count,
        total_document_length,
        pages,
    };
    root.validate()?;
    Ok(root)
}

pub(crate) fn lexical_term_page_to_parquet(
    root: &LexicalRoot,
    page: &LexicalTermPage,
) -> Result<Vec<u8>> {
    page.validate(root)?;
    if page.entries.is_empty() {
        return Err(BorsukError::InvalidStorage(
            "lexical term page must contain at least one block".to_string(),
        ));
    }
    let schema = lexical_term_page_schema();
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            array(StringArray::from_iter_values(
                page.entries.iter().map(|_| page.kind.as_str()),
            )),
            array(UInt32Array::from_iter_values(
                page.entries.iter().map(|entry| entry.term),
            )),
            array(UInt64Array::from_iter_values(
                page.entries.iter().map(|entry| entry.document_frequency),
            )),
            array(StringArray::from_iter_values(
                page.entries
                    .iter()
                    .map(|entry| entry.run.segment_key.as_str()),
            )),
            array(UInt32Array::from_iter_values(
                page.entries.iter().map(|entry| entry.run.row_start),
            )),
            array(UInt32Array::from_iter_values(
                page.entries.iter().map(|entry| entry.run.row_count),
            )),
            array(UInt64Array::from_iter_values(
                page.entries.iter().map(|entry| entry.run.decoded_bytes),
            )),
            array(StringArray::from_iter_values(
                page.entries
                    .iter()
                    .map(|entry| entry.run.postings_path.as_str()),
            )),
            array(StringArray::from_iter_values(
                page.entries
                    .iter()
                    .map(|entry| entry.run.postings_checksum.as_str()),
            )),
            array(UInt64Array::from_iter_values(
                page.entries.iter().map(|entry| entry.run.postings_bytes),
            )),
            array(UInt32Array::from_iter_values(
                page.entries
                    .iter()
                    .map(|entry| entry.run.postings_row_group),
            )),
            array(StringArray::from_iter_values(
                page.entries
                    .iter()
                    .map(|entry| entry.run.postings_group_checksum.as_str()),
            )),
            array(StringArray::from_iter_values(
                page.entries
                    .iter()
                    .map(|entry| entry.run.metadata_path.as_str()),
            )),
            array(StringArray::from_iter_values(
                page.entries
                    .iter()
                    .map(|entry| entry.run.metadata_checksum.as_str()),
            )),
            array(UInt64Array::from_iter_values(
                page.entries.iter().map(|entry| entry.run.metadata_bytes),
            )),
            array(UInt32Array::from_iter_values(
                page.entries
                    .iter()
                    .map(|entry| entry.run.metadata_row_group),
            )),
            array(StringArray::from_iter_values(
                page.entries
                    .iter()
                    .map(|entry| entry.run.metadata_group_checksum.as_str()),
            )),
            array(UInt32Array::from_iter_values(
                page.entries.iter().map(|entry| entry.posting_count),
            )),
            array(Float32Array::from_iter_values(
                page.entries.iter().map(|entry| entry.min_value),
            )),
            array(Float32Array::from_iter_values(
                page.entries.iter().map(|entry| entry.max_value),
            )),
            array(UInt32Array::from_iter_values(
                page.entries.iter().map(|entry| entry.min_doc_length),
            )),
        ],
    )?;
    write_batch(batch)
}

pub(crate) fn lexical_term_page_from_parquet(
    root: &LexicalRoot,
    bytes: &[u8],
) -> Result<LexicalTermPage> {
    lexical_term_page_from_batches(root, &read_batches(bytes)?)
}

pub(crate) fn lexical_term_page_from_batches(
    root: &LexicalRoot,
    batches: &[RecordBatch],
) -> Result<LexicalTermPage> {
    let mut kind = None;
    let mut entries = Vec::new();
    for batch in batches {
        for row in 0..batch.num_rows() {
            let row_kind = LexicalKind::from_str(string_value_by_name(batch, row, "kind")?)?;
            if kind.is_some_and(|value| value != row_kind) {
                return Err(BorsukError::InvalidStorage(
                    "lexical term-page kind differs between rows".to_string(),
                ));
            }
            kind.get_or_insert(row_kind);
            entries.push(LexicalTermBlock {
                term: primitive_value_by_name::<UInt32Type>(batch, row, "term")?,
                document_frequency: primitive_value_by_name::<UInt64Type>(
                    batch,
                    row,
                    "document_frequency",
                )?,
                run: LexicalRunRef {
                    segment_key: string_value_by_name(batch, row, "segment_key")?.to_string(),
                    row_start: primitive_value_by_name::<UInt32Type>(batch, row, "row_start")?,
                    row_count: primitive_value_by_name::<UInt32Type>(batch, row, "row_count")?,
                    decoded_bytes: primitive_value_by_name::<UInt64Type>(
                        batch,
                        row,
                        "decoded_bytes",
                    )?,
                    postings_path: string_value_by_name(batch, row, "postings_path")?.to_string(),
                    postings_checksum: string_value_by_name(batch, row, "postings_checksum")?
                        .to_string(),
                    postings_bytes: primitive_value_by_name::<UInt64Type>(
                        batch,
                        row,
                        "postings_bytes",
                    )?,
                    postings_row_group: primitive_value_by_name::<UInt32Type>(
                        batch,
                        row,
                        "postings_row_group",
                    )?,
                    postings_group_checksum: string_value_by_name(
                        batch,
                        row,
                        "postings_group_checksum",
                    )?
                    .to_string(),
                    metadata_path: string_value_by_name(batch, row, "metadata_path")?.to_string(),
                    metadata_checksum: string_value_by_name(batch, row, "metadata_checksum")?
                        .to_string(),
                    metadata_bytes: primitive_value_by_name::<UInt64Type>(
                        batch,
                        row,
                        "metadata_bytes",
                    )?,
                    metadata_row_group: primitive_value_by_name::<UInt32Type>(
                        batch,
                        row,
                        "metadata_row_group",
                    )?,
                    metadata_group_checksum: string_value_by_name(
                        batch,
                        row,
                        "metadata_group_checksum",
                    )?
                    .to_string(),
                },
                posting_count: primitive_value_by_name::<UInt32Type>(batch, row, "posting_count")?,
                min_value: primitive_value_by_name::<Float32Type>(batch, row, "min_value")?,
                max_value: primitive_value_by_name::<Float32Type>(batch, row, "max_value")?,
                min_doc_length: primitive_value_by_name::<UInt32Type>(
                    batch,
                    row,
                    "min_doc_length",
                )?,
            });
        }
    }
    let page = LexicalTermPage {
        kind: kind.ok_or_else(|| {
            BorsukError::InvalidStorage("lexical term-page Parquet table is empty".to_string())
        })?,
        entries,
    };
    page.validate(root)?;
    Ok(page)
}

#[cfg(test)]
pub(crate) fn bm25_postings_to_parquet(
    postings: &[Bm25Posting],
    row_count: u32,
) -> Result<Vec<u8>> {
    bm25_posting_blocks_to_parquet(&[(postings.to_vec(), row_count)])
}

pub(crate) fn bm25_posting_blocks_to_parquet(
    blocks: &[(Vec<Bm25Posting>, u32)],
) -> Result<Vec<u8>> {
    if blocks.is_empty() {
        return Err(BorsukError::InvalidStorage(
            "BM25 Parquet pack must contain a row group".to_string(),
        ));
    }
    let schema = bm25_postings_schema();
    let mut batches = Vec::with_capacity(blocks.len());
    for (postings, row_count) in blocks {
        validate_bm25_postings(postings, *row_count)?;
        batches.push(RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                array(UInt32Array::from_iter_values(
                    postings.iter().map(|posting| posting.term),
                )),
                array(UInt32Array::from_iter_values(
                    postings.iter().map(|posting| posting.row),
                )),
                array(UInt32Array::from_iter_values(
                    postings.iter().map(|posting| posting.term_frequency),
                )),
            ],
        )?);
    }
    write_batches_as_row_groups(Arc::clone(&schema), &batches)
}

#[cfg(test)]
pub(crate) fn bm25_postings_from_parquet(bytes: &[u8], row_count: u32) -> Result<Vec<Bm25Posting>> {
    bm25_postings_from_batches(&read_batches(bytes)?, row_count)
}

pub(crate) fn bm25_postings_from_batches(
    batches: &[RecordBatch],
    row_count: u32,
) -> Result<Vec<Bm25Posting>> {
    let mut postings = Vec::new();
    for batch in batches {
        for row in 0..batch.num_rows() {
            postings.push(Bm25Posting {
                term: primitive_value_by_name::<UInt32Type>(batch, row, "term")?,
                row: primitive_value_by_name::<UInt32Type>(batch, row, "row")?,
                term_frequency: primitive_value_by_name::<UInt32Type>(
                    batch,
                    row,
                    "term_frequency",
                )?,
            });
        }
    }
    validate_bm25_postings(&postings, row_count)?;
    Ok(postings)
}

#[cfg(test)]
pub(crate) fn sparse_postings_to_parquet(
    postings: &[SparsePosting],
    row_count: u32,
) -> Result<Vec<u8>> {
    sparse_postings_to_parquet_typed(postings, row_count, VectorElementType::Float32)
}

#[cfg(test)]
pub(crate) fn sparse_postings_to_parquet_typed(
    postings: &[SparsePosting],
    row_count: u32,
    element_type: VectorElementType,
) -> Result<Vec<u8>> {
    sparse_posting_blocks_to_parquet_typed(&[(postings.to_vec(), row_count)], element_type)
}

pub(crate) fn sparse_posting_blocks_to_parquet_typed(
    blocks: &[(Vec<SparsePosting>, u32)],
    element_type: VectorElementType,
) -> Result<Vec<u8>> {
    if blocks.is_empty() {
        return Err(BorsukError::InvalidStorage(
            "sparse Parquet pack must contain a row group".to_string(),
        ));
    }
    let schema = sparse_postings_schema(element_type)?;
    let mut batches = Vec::with_capacity(blocks.len());
    for (postings, row_count) in blocks {
        validate_sparse_postings(postings, *row_count)?;
        let values: ArrayRef = match element_type {
            VectorElementType::Float32 => array(Float32Array::from_iter_values(
                postings.iter().map(|posting| posting.value),
            )),
            VectorElementType::Float16 => array(Float16Array::from_iter_values(
                postings
                    .iter()
                    .map(|posting| half::f16::from_f32(posting.value)),
            )),
            _ => {
                return Err(BorsukError::InvalidStorage(format!(
                    "sparse postings support float32 or float16 values, got {element_type}"
                )));
            }
        };
        batches.push(RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                array(UInt32Array::from_iter_values(
                    postings.iter().map(|posting| posting.term),
                )),
                array(UInt32Array::from_iter_values(
                    postings.iter().map(|posting| posting.row),
                )),
                values,
            ],
        )?);
    }
    write_batches_as_row_groups(Arc::clone(&schema), &batches)
}

#[cfg(test)]
pub(crate) fn sparse_postings_from_parquet(
    bytes: &[u8],
    row_count: u32,
) -> Result<Vec<SparsePosting>> {
    sparse_postings_from_batches(&read_batches(bytes)?, row_count)
}

pub(crate) fn sparse_postings_from_batches(
    batches: &[RecordBatch],
    row_count: u32,
) -> Result<Vec<SparsePosting>> {
    let mut postings = Vec::new();
    for batch in batches {
        for row in 0..batch.num_rows() {
            let value_column = column_index(batch, "value")?;
            let value = match batch.schema().field(value_column).data_type() {
                DataType::Float32 => {
                    primitive_value::<Float32Type>(batch, value_column, row, "value")?
                }
                DataType::Float16 => f32::from(primitive_value::<Float16Type>(
                    batch,
                    value_column,
                    row,
                    "value",
                )?),
                data_type => {
                    return Err(BorsukError::InvalidStorage(format!(
                        "sparse posting value column must be Float32 or Float16, got {data_type:?}"
                    )));
                }
            };
            postings.push(SparsePosting {
                term: primitive_value_by_name::<UInt32Type>(batch, row, "term")?,
                row: primitive_value_by_name::<UInt32Type>(batch, row, "row")?,
                value,
            });
        }
    }
    validate_sparse_postings(&postings, row_count)?;
    Ok(postings)
}

#[cfg(test)]
pub(crate) fn lexical_row_metadata_to_parquet(
    kind: LexicalKind,
    rows: &[LexicalRowMetadata],
) -> Result<Vec<u8>> {
    lexical_row_metadata_blocks_to_parquet(kind, &[rows.to_vec()])
}

pub(crate) fn lexical_row_metadata_blocks_to_parquet(
    kind: LexicalKind,
    blocks: &[Vec<LexicalRowMetadata>],
) -> Result<Vec<u8>> {
    if blocks.is_empty() || blocks.iter().any(Vec::is_empty) {
        return Err(BorsukError::InvalidStorage(
            "lexical metadata Parquet pack must contain non-empty row groups".to_string(),
        ));
    }
    let stamped = blocks
        .iter()
        .flatten()
        .filter(|row| row.mutation_stamp.is_some())
        .count();
    let row_count = blocks.iter().map(Vec::len).sum::<usize>();
    if stamped != 0 && stamped != row_count {
        return Err(BorsukError::InvalidStorage(
            "lexical metadata cannot mix stamped and unstamped mutations".to_string(),
        ));
    }
    let schema = lexical_row_metadata_schema(stamped != 0);
    let mut batches = Vec::with_capacity(blocks.len());
    for rows in blocks {
        validate_lexical_rows(kind, rows)?;
        let mut columns = vec![
            array(UInt32Array::from_iter_values(
                rows.iter().map(|row| row.row),
            )),
            array(BinaryArray::from_iter_values(
                rows.iter().map(|row| row.record_id.as_slice()),
            )),
            array(UInt64Array::from_iter_values(
                rows.iter().map(|row| row.generation),
            )),
        ];
        if stamped != 0 {
            columns.extend([
                array(UInt64Array::from_iter_values(rows.iter().map(|row| {
                    row.mutation_stamp
                        .expect("all lexical stamps checked before encoding")
                        .version()
                        .hlc()
                }))),
                array(FixedSizeBinaryArray::try_from_iter(rows.iter().map(
                    |row| {
                        row.mutation_stamp
                            .expect("all lexical stamps checked before encoding")
                            .version()
                            .writer()
                    },
                ))?),
                array(FixedSizeBinaryArray::try_from_iter(rows.iter().map(
                    |row| {
                        row.mutation_stamp
                            .expect("all lexical stamps checked before encoding")
                            .digest()
                    },
                ))?),
            ]);
        }
        columns.push(array(UInt32Array::from_iter_values(
            rows.iter().map(|row| row.document_length),
        )));
        batches.push(RecordBatch::try_new(Arc::clone(&schema), columns)?);
    }
    write_batches_as_row_groups(Arc::clone(&schema), &batches)
}

#[cfg(test)]
pub(crate) fn lexical_row_metadata_from_parquet(
    kind: LexicalKind,
    bytes: &[u8],
) -> Result<Vec<LexicalRowMetadata>> {
    lexical_row_metadata_from_batches(kind, &read_batches(bytes)?)
}

pub(crate) fn lexical_row_metadata_from_batches(
    kind: LexicalKind,
    batches: &[RecordBatch],
) -> Result<Vec<LexicalRowMetadata>> {
    let mut rows = Vec::new();
    for batch in batches {
        let mutation_stamp_columns = mutation_stamp_columns(&batch.schema())?;
        for row in 0..batch.num_rows() {
            rows.push(LexicalRowMetadata {
                row: primitive_value_by_name::<UInt32Type>(batch, row, "row")?,
                record_id: binary_value_by_name(batch, row, "record_id")?.to_vec(),
                generation: primitive_value_by_name::<UInt64Type>(batch, row, "generation")?,
                mutation_stamp: mutation_stamp_value(batch, mutation_stamp_columns, row)?,
                document_length: primitive_value_by_name::<UInt32Type>(
                    batch,
                    row,
                    "document_length",
                )?,
            });
        }
    }
    validate_lexical_rows(kind, &rows)?;
    Ok(rows)
}

fn validate_bm25_postings(postings: &[Bm25Posting], row_count: u32) -> Result<()> {
    let mut previous = None;
    for posting in postings {
        let key = (posting.term, posting.row);
        if posting.row >= row_count
            || posting.term_frequency == 0
            || previous.is_some_and(|prior| prior >= key)
        {
            return Err(BorsukError::InvalidStorage(
                "BM25 Parquet postings are invalid or unsorted".to_string(),
            ));
        }
        previous = Some(key);
    }
    Ok(())
}

fn validate_sparse_postings(postings: &[SparsePosting], row_count: u32) -> Result<()> {
    let mut previous = None;
    for posting in postings {
        let key = (posting.term, posting.row);
        if posting.row >= row_count
            || !posting.value.is_finite()
            || posting.value == 0.0
            || previous.is_some_and(|prior| prior >= key)
        {
            return Err(BorsukError::InvalidStorage(
                "sparse Parquet postings are invalid or unsorted".to_string(),
            ));
        }
        previous = Some(key);
    }
    Ok(())
}

fn validate_lexical_rows(kind: LexicalKind, rows: &[LexicalRowMetadata]) -> Result<()> {
    for (expected, row) in rows.iter().enumerate() {
        if usize::try_from(row.row).ok() != Some(expected)
            || row.record_id.is_empty()
            || (kind == LexicalKind::Bm25 && row.document_length == 0)
            || (kind == LexicalKind::Sparse && row.document_length != 0)
        {
            return Err(BorsukError::InvalidStorage(
                "lexical Parquet row metadata is invalid or non-contiguous".to_string(),
            ));
        }
    }
    Ok(())
}

pub(crate) fn graph_to_parquet(graph: &SegmentGraph) -> Result<Vec<u8>> {
    for edge in &graph.edges {
        validate_graph_edge_distance(
            edge.source_record_index,
            edge.neighbor_record_index,
            edge.distance,
        )?;
    }

    let schema = graph_schema();
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            array(UInt16Array::from_iter_values(
                graph.edges.iter().map(|_| CURRENT_VERSION),
            )),
            array(StringArray::from_iter_values(
                graph.edges.iter().map(|_| graph.segment_id.as_str()),
            )),
            array(UInt8Array::from_iter_values(
                graph.edges.iter().map(|_| graph.level),
            )),
            array(Int64Array::from_iter_values(
                graph
                    .edges
                    .iter()
                    .map(|_| graph.created_at.timestamp_millis()),
            )),
            array(UInt64Array::from_iter_values(
                graph
                    .edges
                    .iter()
                    .map(|edge| edge.source_record_index as u64),
            )),
            array(UInt64Array::from_iter_values(
                graph
                    .edges
                    .iter()
                    .map(|edge| edge.neighbor_record_index as u64),
            )),
            array(Float32Array::from_iter_values(
                graph.edges.iter().map(|edge| edge.distance),
            )),
        ],
    )?;

    write_batch(batch)
}

pub(crate) fn graph_from_parquet(
    bytes: &[u8],
    expected_segment_id: &str,
    expected_level: u8,
    records: &[VectorRecord],
) -> Result<SegmentGraph> {
    let mut edges = Vec::new();
    let mut metadata = None::<GraphMetadata>;
    let record_index_by_id = records
        .iter()
        .enumerate()
        .map(|(index, record)| (record.id.as_bytes(), index))
        .collect::<HashMap<_, _>>();

    for batch in read_batches(bytes)? {
        let source_record_index_column = batch.schema().index_of("source_record_index").ok();
        let neighbor_record_index_column = batch.schema().index_of("neighbor_record_index").ok();
        let source_record_id_column = batch.schema().index_of("source_record_id").ok();
        let neighbor_record_id_column = batch.schema().index_of("neighbor_record_id").ok();
        for row in 0..batch.num_rows() {
            let format_version = primitive_value::<UInt16Type>(&batch, 0, row, "format_version")?;
            if format_version != CURRENT_VERSION {
                return Err(BorsukError::InvalidStorage(format!(
                    "unsupported graph table version {format_version}"
                )));
            }

            let row_metadata = GraphMetadata {
                segment_id: string_value(&batch, 1, row, "segment_id")?.to_string(),
                level: primitive_value::<UInt8Type>(&batch, 2, row, "level")?,
                created_at: datetime_from_millis(primitive_value::<Int64Type>(
                    &batch,
                    3,
                    row,
                    "created_at_ms",
                )?)?,
            };

            if row_metadata.segment_id != expected_segment_id {
                return Err(BorsukError::InvalidStorage(format!(
                    "graph table segment id `{}` does not match expected segment `{expected_segment_id}`",
                    row_metadata.segment_id
                )));
            }
            if row_metadata.level != expected_level {
                return Err(BorsukError::InvalidStorage(format!(
                    "graph table level {} does not match expected level {expected_level}",
                    row_metadata.level
                )));
            }

            if let Some(metadata) = &metadata {
                if metadata != &row_metadata {
                    return Err(BorsukError::InvalidStorage(
                        "graph metadata differs between rows".to_string(),
                    ));
                }
            } else {
                metadata = Some(row_metadata);
            }

            let (source_record_index, neighbor_record_index) = match (
                source_record_index_column,
                neighbor_record_index_column,
                source_record_id_column,
                neighbor_record_id_column,
            ) {
                (Some(source_column), Some(neighbor_column), _, _) => {
                    let source_record_index = usize_from_u64(primitive_value::<UInt64Type>(
                        &batch,
                        source_column,
                        row,
                        "source_record_index",
                    )?)?;
                    let neighbor_record_index = usize_from_u64(primitive_value::<UInt64Type>(
                        &batch,
                        neighbor_column,
                        row,
                        "neighbor_record_index",
                    )?)?;
                    validate_graph_record_index(
                        expected_segment_id,
                        "source",
                        source_record_index,
                        records.len(),
                    )?;
                    validate_graph_record_index(
                        expected_segment_id,
                        "neighbor",
                        neighbor_record_index,
                        records.len(),
                    )?;
                    (source_record_index, neighbor_record_index)
                }
                (_, _, Some(source_column), Some(neighbor_column)) => {
                    let source_record_id =
                        string_value(&batch, source_column, row, "source_record_id")?;
                    let neighbor_record_id =
                        string_value(&batch, neighbor_column, row, "neighbor_record_id")?;
                    (
                        graph_record_index_from_id(
                            expected_segment_id,
                            "source",
                            source_record_id,
                            &record_index_by_id,
                        )?,
                        graph_record_index_from_id(
                            expected_segment_id,
                            "neighbor",
                            neighbor_record_id,
                            &record_index_by_id,
                        )?,
                    )
                }
                _ => {
                    return Err(BorsukError::InvalidStorage(
                        "graph table missing record reference columns".to_string(),
                    ));
                }
            };
            let distance = primitive_value::<Float32Type>(&batch, 6, row, "neighbor_distance")?;
            validate_graph_edge_distance(source_record_index, neighbor_record_index, distance)?;

            edges.push(GraphEdge {
                source_record_index,
                neighbor_record_index,
                distance,
            });
        }
    }

    let metadata = match metadata {
        Some(metadata) => metadata,
        None => GraphMetadata {
            segment_id: expected_segment_id.to_string(),
            level: expected_level,
            created_at: datetime_from_millis(0)?,
        },
    };

    let mut graph = SegmentGraph {
        segment_id: metadata.segment_id,
        level: metadata.level,
        edges,
        adjacency_offsets: Vec::new(),
        created_at: metadata.created_at,
    };
    graph.prepare_adjacency(records.len());
    Ok(graph)
}

#[cfg(test)]
fn manifest_schema() -> Arc<Schema> {
    manifest_schema_with_named_vectors_and_wal(false, false, false, false)
}

fn manifest_schema_with_named_vectors_and_wal(
    include_named_vectors: bool,
    include_wal: bool,
    include_build_config: bool,
    include_quantizer_ref: bool,
) -> Arc<Schema> {
    let mut fields = vec![
        Field::new("format_version", DataType::UInt16, false),
        Field::new("version", DataType::UInt64, false),
        Field::new("uri", DataType::Utf8, false),
        Field::new("metric", DataType::Utf8, false),
        Field::new("dimensions", DataType::UInt64, false),
        Field::new("segment_max_vectors", DataType::UInt64, false),
        Field::new("created_at_ms", DataType::Int64, false),
        Field::new("ram_budget_bytes", DataType::UInt64, true),
        Field::new("text_enabled", DataType::Boolean, false),
        Field::new("text_tokenizer", DataType::Utf8, true),
        Field::new("next_generated_id", DataType::UInt64, false),
        Field::new("routing_max_level", DataType::UInt8, false),
        Field::new("routing_page_fanout", DataType::UInt64, false),
        Field::new("graph_neighbors", DataType::UInt64, false),
        Field::new("leaf_capability", DataType::Utf8, true),
        Field::new("logical_cell_routing_strategy_json", DataType::Utf8, false),
        Field::new("tombstone_path", DataType::Utf8, true),
        Field::new("tombstone_checksum", DataType::Utf8, true),
        Field::new("tombstone_count", DataType::UInt64, true),
        Field::new("tombstone_id_bloom", DataType::Binary, true),
        Field::new("tombstone_created_at_ms", DataType::Int64, true),
    ];
    if include_named_vectors {
        fields.push(Field::new("named_vectors_json", DataType::Utf8, true));
    }
    if include_wal {
        fields.push(Field::new("wal_json", DataType::Utf8, true));
    }
    if include_build_config {
        fields.push(Field::new("build_config_json", DataType::Utf8, true));
    }
    if include_quantizer_ref {
        fields.push(Field::new("quantizer_ref_json", DataType::Utf8, true));
    }
    fields.push(Field::new("global_ann_ref_json", DataType::Utf8, true));
    fields.push(Field::new(
        "global_cell_card_ann_ref_json",
        DataType::Utf8,
        true,
    ));
    fields.push(Field::new("lexical_roots_json", DataType::Utf8, false));
    fields.push(Field::new("bm25_stats_delta_json", DataType::Utf8, true));
    Arc::new(Schema::new(fields))
}

fn routing_schema(dimensions: usize) -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("format_version", DataType::UInt16, false),
        Field::new("manifest_version", DataType::UInt64, false),
        Field::new("id", DataType::Utf8, false),
        Field::new("level", DataType::UInt8, false),
        Field::new("path", DataType::Utf8, false),
        Field::new("layout_json", DataType::Utf8, false),
        Field::new("object_count", DataType::UInt64, false),
        Field::new("dimensions", DataType::UInt64, false),
        fixed_f32_field("centroid", dimensions),
        Field::new("radius", DataType::Float32, false),
        Field::new("checksum", DataType::Utf8, false),
        Field::new("size_bytes", DataType::UInt64, false),
        Field::new("vector_size_bytes", DataType::UInt64, false),
        Field::new("graph_path", DataType::Utf8, false),
        Field::new("graph_checksum", DataType::Utf8, false),
        Field::new("graph_size_bytes", DataType::UInt64, false),
        Field::new("created_at_ms", DataType::Int64, false),
        Field::new("id_bloom", DataType::Binary, false),
        Field::new("leaf_mode", DataType::Utf8, false),
        Field::new("vector_signature_bloom", DataType::Binary, false),
        fixed_f32_field("bounds_min", dimensions),
        fixed_f32_field("bounds_max", dimensions),
        Field::new("metadata_stats", DataType::Binary, false),
        Field::new("text_doc_count", DataType::UInt32, false),
        Field::new("text_total_doc_length", DataType::UInt64, false),
        Field::new("text_lexical_decoded_bytes", DataType::UInt64, false),
        Field::new("sparse_lexical_max_decoded_bytes", DataType::UInt64, false),
        Field::new("lexical_shards_json", DataType::Utf8, false),
        Field::new("sparse_encoded", DataType::UInt64, false),
        Field::new("dense_encoded", DataType::UInt64, false),
    ]))
}

fn routing_layer_page_schema(dimensions: usize) -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("format_version", DataType::UInt16, false),
        Field::new("manifest_version", DataType::UInt64, false),
        Field::new("routing_level", DataType::UInt8, false),
        Field::new("page_ordinal", DataType::UInt64, false),
        Field::new("page_segments", DataType::UInt64, false),
        Field::new("segment_ordinal", DataType::UInt64, false),
        Field::new("segment_id", DataType::Utf8, false),
        Field::new("segment_level", DataType::UInt8, false),
        Field::new("object_count", DataType::UInt64, false),
        Field::new("dimensions", DataType::UInt64, false),
        fixed_f32_field("centroid", dimensions),
        Field::new("radius", DataType::Float32, false),
        Field::new("segment_path", DataType::Utf8, false),
        Field::new("segment_layout_json", DataType::Utf8, false),
        Field::new("segment_checksum", DataType::Utf8, false),
        Field::new("segment_size_bytes", DataType::UInt64, false),
        Field::new("vector_size_bytes", DataType::UInt64, false),
        Field::new("graph_path", DataType::Utf8, false),
        Field::new("graph_checksum", DataType::Utf8, false),
        Field::new("graph_size_bytes", DataType::UInt64, false),
        Field::new("id_bloom", DataType::Binary, false),
        Field::new("leaf_mode", DataType::Utf8, false),
        Field::new("vector_signature_bloom", DataType::Binary, false),
        Field::new("created_at_ms", DataType::Int64, false),
        fixed_f32_field("bounds_min", dimensions),
        fixed_f32_field("bounds_max", dimensions),
        Field::new("metadata_stats", DataType::Binary, false),
        Field::new("text_doc_count", DataType::UInt32, false),
        Field::new("text_total_doc_length", DataType::UInt64, false),
        Field::new("text_lexical_decoded_bytes", DataType::UInt64, false),
        Field::new("sparse_lexical_max_decoded_bytes", DataType::UInt64, false),
        Field::new("lexical_shards_json", DataType::Utf8, false),
        Field::new("sparse_encoded", DataType::UInt64, false),
        Field::new("dense_encoded", DataType::UInt64, false),
    ]))
}

fn routing_layer_page_index_schema(dimensions: usize) -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("format_version", DataType::UInt16, false),
        Field::new("manifest_version", DataType::UInt64, false),
        Field::new("routing_level", DataType::UInt8, false),
        Field::new("page_ordinal", DataType::UInt64, false),
        Field::new("page_path", DataType::Utf8, false),
        Field::new("page_checksum", DataType::Utf8, false),
        Field::new("page_segments", DataType::UInt64, false),
        Field::new("leaf_segments", DataType::UInt64, false),
        Field::new("leaf_pages", DataType::UInt64, false),
        Field::new("routing_pages", DataType::UInt64, false),
        Field::new("dimensions", DataType::UInt64, false),
        Field::new(
            "centroid",
            DataType::FixedSizeList(
                Arc::new(Field::new("item", DataType::Float32, true)),
                dimensions as i32,
            ),
            false,
        ),
        Field::new("radius", DataType::Float32, false),
        Field::new("id_bloom", DataType::Binary, false),
        Field::new("vector_signature_bloom", DataType::Binary, false),
        Field::new("level_mask", DataType::UInt64, false),
        Field::new("page_records", DataType::UInt64, false),
        Field::new("page_segment_bytes", DataType::UInt64, false),
        Field::new("page_vector_bytes", DataType::UInt64, false),
        Field::new("page_graph_bytes", DataType::UInt64, false),
        Field::new("page_sparse_encoded_vectors", DataType::UInt64, false),
        Field::new("page_dense_encoded_vectors", DataType::UInt64, false),
        fixed_f32_field("bounds_min", dimensions),
        fixed_f32_field("bounds_max", dimensions),
    ]))
}

fn pivots_schema(dimensions: usize) -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("format_version", DataType::UInt16, false),
        Field::new("manifest_version", DataType::UInt64, false),
        Field::new("ordinal", DataType::UInt64, false),
        Field::new("pivot_id", DataType::Utf8, false),
        fixed_f32_field("vector", dimensions),
    ]))
}

trait VectorRecordView {
    fn as_vector_record(&self) -> &VectorRecord;
}

impl VectorRecordView for VectorRecord {
    fn as_vector_record(&self) -> &VectorRecord {
        self
    }
}

impl VectorRecordView for &VectorRecord {
    fn as_vector_record(&self) -> &VectorRecord {
        self
    }
}

fn mutation_stamps_present<R: VectorRecordView>(records: &[R]) -> Result<bool> {
    let stamped = records
        .iter()
        .filter(|record| record.as_vector_record().mutation_stamp().is_some())
        .count();
    if stamped != 0 && stamped != records.len() {
        return Err(BorsukError::InvalidStorage(
            "record table cannot mix stamped and unstamped mutations".to_string(),
        ));
    }
    Ok(stamped == records.len())
}

fn mutation_stamp_fields() -> [Field; 3] {
    [
        Field::new("mutation_hlc", DataType::UInt64, false),
        Field::new("mutation_writer", DataType::FixedSizeBinary(16), false),
        Field::new("mutation_digest", DataType::FixedSizeBinary(32), false),
    ]
}

fn mutation_stamp_arrays<R: VectorRecordView>(records: &[R]) -> Result<[ArrayRef; 3]> {
    let stamps = records
        .iter()
        .map(|record| {
            record.as_vector_record().mutation_stamp().ok_or_else(|| {
                BorsukError::InvalidStorage(
                    "record table mutation stamp disappeared during encoding".to_string(),
                )
            })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok([
        array(UInt64Array::from_iter_values(
            stamps.iter().map(|stamp| stamp.version().hlc()),
        )),
        array(FixedSizeBinaryArray::try_from_iter(
            stamps.iter().map(|stamp| stamp.version().writer()),
        )?),
        array(FixedSizeBinaryArray::try_from_iter(
            stamps.iter().map(|stamp| stamp.digest()),
        )?),
    ])
}

#[allow(clippy::too_many_arguments)]
fn segment_schema(
    dimensions: usize,
    pq_code_dimensions: usize,
    include_sparse: bool,
    include_text: bool,
    include_generation: bool,
    include_mutation_stamp: bool,
    include_vectors: bool,
    vector_element_type: VectorElementType,
) -> Arc<Schema> {
    // `pq_code` is sized to the quantizer's actual code length, NOT
    // `dimensions`: TurboQuant's SRHT rotation pads to the next power of two,
    // and the optional QJL correction adds a fixed tail. Per-segment constants
    // and coarse bounds are encoded once in `segment_header`.
    let mut fields = vec![
        Field::new("segment_header", DataType::Binary, true),
        Field::new("routing_code", DataType::Float32, false),
        fixed_u8_field("pq_code", pq_code_dimensions),
        Field::new("record_id", DataType::Binary, false),
        Field::new("metadata", DataType::Binary, false),
    ];
    if include_sparse {
        fields.push(sparse_u32_field("sparse_indices"));
        fields.push(sparse_f32_field("sparse_values"));
    }
    if include_text {
        fields.push(sparse_u32_field("text_term_ids"));
        fields.push(sparse_u32_field("text_term_freqs"));
    }
    if include_generation {
        fields.push(Field::new("generation", DataType::UInt64, false));
    }
    if include_mutation_stamp {
        fields.extend(mutation_stamp_fields());
    }
    if include_vectors {
        // Nullable: sparse rows carry no dense vector. Appended last so all
        // positional column indices of the base segment layout are unchanged.
        fields.push(Field::new(
            "vector",
            wal_vector_data_type(vector_element_type, dimensions)
                .expect("validated WAL vector dimensions must have an Arrow type"),
            true,
        ));
        fields.push(Field::new("wal_record_extras", DataType::Binary, false));
        // Persist the declared primary-vector type as a tiny constant column;
        // Parquet compresses this repeated byte to a negligible footprint.
        fields.push(Field::new(
            "wal_vector_element_type",
            DataType::UInt8,
            false,
        ));
    }
    Arc::new(Schema::new(fields))
}

fn wal_records_schema(
    dimensions: usize,
    include_sparse: bool,
    include_text: bool,
    include_generation: bool,
    include_mutation_stamp: bool,
    vector_element_type: VectorElementType,
) -> Result<Arc<Schema>> {
    let mut fields = vec![
        Field::new("record_id", DataType::Binary, false),
        Field::new("metadata", DataType::Binary, false),
    ];
    if include_sparse {
        fields.push(sparse_u32_field("sparse_indices"));
        fields.push(sparse_f32_field("sparse_values"));
    }
    if include_text {
        fields.push(sparse_u32_field("text_term_ids"));
        fields.push(sparse_u32_field("text_term_freqs"));
    }
    if include_generation {
        fields.push(Field::new("generation", DataType::UInt64, false));
    }
    if include_mutation_stamp {
        fields.extend(mutation_stamp_fields());
    }
    fields.push(Field::new(
        "vector",
        wal_vector_data_type(vector_element_type, dimensions)?,
        true,
    ));
    fields.push(Field::new("wal_record_extras", DataType::Binary, false));
    fields.push(Field::new(
        "wal_vector_element_type",
        DataType::UInt8,
        false,
    ));
    fields.push(Field::new("wal_vector_dimensions", DataType::UInt32, false));
    Ok(Arc::new(Schema::new(fields)))
}

fn vector_records_schema(dimensions: usize) -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("format_version", DataType::UInt16, false),
        Field::new("dimensions", DataType::UInt64, false),
        Field::new("record_id", DataType::Binary, false),
        fixed_f32_field("vector", dimensions),
        Field::new("metadata", DataType::Binary, false),
    ]))
}

fn graph_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("format_version", DataType::UInt16, false),
        Field::new("segment_id", DataType::Utf8, false),
        Field::new("level", DataType::UInt8, false),
        Field::new("created_at_ms", DataType::Int64, false),
        Field::new("source_record_index", DataType::UInt64, false),
        Field::new("neighbor_record_index", DataType::UInt64, false),
        Field::new("neighbor_distance", DataType::Float32, false),
    ]))
}

fn lexical_root_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("kind", DataType::Utf8, false),
        Field::new("dimensions", DataType::UInt32, false),
        Field::new("document_count", DataType::UInt64, false),
        Field::new("total_document_length", DataType::UInt64, false),
        Field::new("first_term", DataType::UInt32, false),
        Field::new("last_term", DataType::UInt32, false),
        Field::new("page_path", DataType::Utf8, false),
        Field::new("page_checksum", DataType::Utf8, false),
        Field::new("page_content_checksum", DataType::Utf8, false),
        Field::new("page_encoded_bytes", DataType::UInt64, false),
        Field::new("page_term_count", DataType::UInt32, false),
    ]))
}

fn lexical_term_page_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("kind", DataType::Utf8, false),
        Field::new("term", DataType::UInt32, false),
        Field::new("document_frequency", DataType::UInt64, false),
        Field::new("segment_key", DataType::Utf8, false),
        Field::new("row_start", DataType::UInt32, false),
        Field::new("row_count", DataType::UInt32, false),
        Field::new("decoded_bytes", DataType::UInt64, false),
        Field::new("postings_path", DataType::Utf8, false),
        Field::new("postings_checksum", DataType::Utf8, false),
        Field::new("postings_bytes", DataType::UInt64, false),
        Field::new("postings_row_group", DataType::UInt32, false),
        Field::new("postings_group_checksum", DataType::Utf8, false),
        Field::new("metadata_path", DataType::Utf8, false),
        Field::new("metadata_checksum", DataType::Utf8, false),
        Field::new("metadata_bytes", DataType::UInt64, false),
        Field::new("metadata_row_group", DataType::UInt32, false),
        Field::new("metadata_group_checksum", DataType::Utf8, false),
        Field::new("posting_count", DataType::UInt32, false),
        Field::new("min_value", DataType::Float32, false),
        Field::new("max_value", DataType::Float32, false),
        Field::new("min_doc_length", DataType::UInt32, false),
    ]))
}

fn bm25_postings_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("term", DataType::UInt32, false),
        Field::new("row", DataType::UInt32, false),
        Field::new("term_frequency", DataType::UInt32, false),
    ]))
}

fn sparse_postings_schema(element_type: VectorElementType) -> Result<Arc<Schema>> {
    let value_type = match element_type {
        VectorElementType::Float32 => DataType::Float32,
        VectorElementType::Float16 => DataType::Float16,
        _ => {
            return Err(BorsukError::InvalidStorage(format!(
                "sparse postings support float32 or float16 values, got {element_type}"
            )));
        }
    };
    Ok(Arc::new(Schema::new(vec![
        Field::new("term", DataType::UInt32, false),
        Field::new("row", DataType::UInt32, false),
        Field::new("value", value_type, false),
    ])))
}

fn lexical_row_metadata_schema(include_mutation_stamp: bool) -> Arc<Schema> {
    let mut fields = vec![
        Field::new("row", DataType::UInt32, false),
        Field::new("record_id", DataType::Binary, false),
        Field::new("generation", DataType::UInt64, false),
    ];
    if include_mutation_stamp {
        fields.extend(mutation_stamp_fields());
    }
    fields.push(Field::new("document_length", DataType::UInt32, false));
    Arc::new(Schema::new(fields))
}

fn fixed_f32_field(name: &str, dimensions: usize) -> Field {
    Field::new(
        name,
        DataType::FixedSizeList(
            Arc::new(Field::new_list_field(DataType::Float32, true)),
            dimensions as i32,
        ),
        false,
    )
}

fn fixed_u8_field(name: &str, dimensions: usize) -> Field {
    Field::new(
        name,
        DataType::FixedSizeList(
            Arc::new(Field::new_list_field(DataType::UInt8, true)),
            dimensions as i32,
        ),
        false,
    )
}

fn sparse_u32_field(name: &str) -> Field {
    Field::new(
        name,
        DataType::List(Arc::new(Field::new_list_field(DataType::UInt32, true))),
        true,
    )
}

fn sparse_f32_field(name: &str) -> Field {
    Field::new(
        name,
        DataType::List(Arc::new(Field::new_list_field(DataType::Float32, true))),
        true,
    )
}

fn fixed_f32_array<'a>(
    values: impl IntoIterator<Item = &'a [f32]>,
    dimensions: usize,
) -> FixedSizeListArray {
    let values = values
        .into_iter()
        .map(|vector| Some(vector.iter().copied().map(Some).collect::<Vec<_>>()))
        .collect::<Vec<_>>();
    FixedSizeListArray::from_iter_primitive::<Float32Type, _, _>(values, dimensions as i32)
}

fn optional_typed_vector_array<R: VectorRecordView>(
    records: &[R],
    sparse_indices: &[Option<Vec<u32>>],
    dimensions: usize,
    element_type: VectorElementType,
) -> Result<ArrayRef> {
    let canonical = records
        .iter()
        .zip(sparse_indices)
        .map(|(record, sparse)| {
            let record = record.as_vector_record();
            if sparse.is_some() {
                Ok(None)
            } else {
                element_type.canonicalize(&record.vector).map(Some)
            }
        })
        .collect::<Result<Vec<_>>>()?;
    let list_size = i32::try_from(dimensions)
        .map_err(|_| BorsukError::InvalidStorage("WAL vector dimensions exceed i32".to_string()))?;
    macro_rules! primitive {
        ($type:ty, $convert:expr) => {{
            let rows = canonical
                .iter()
                .map(|row| {
                    row.as_ref().map(|row| {
                        row.iter()
                            .copied()
                            .map(|value| Some(($convert)(value)))
                            .collect::<Vec<_>>()
                    })
                })
                .collect::<Vec<_>>();
            array(FixedSizeListArray::from_iter_primitive::<$type, _, _>(
                rows, list_size,
            ))
        }};
    }
    Ok(match element_type {
        VectorElementType::Float32 => primitive!(Float32Type, |value| value),
        VectorElementType::Float16 => primitive!(Float16Type, half::f16::from_f32),
        VectorElementType::BFloat16 => {
            primitive!(UInt16Type, |value| half::bf16::from_f32(value).to_bits())
        }
        VectorElementType::Float8E4M3Fn => {
            primitive!(UInt8Type, crate::float8::encode_e4m3fn)
        }
        VectorElementType::Float8E5M2 => primitive!(UInt8Type, crate::float8::encode_e5m2),
        VectorElementType::Int8 => primitive!(Int8Type, |value| value as i8),
        VectorElementType::Binary => {
            let packed = canonical
                .iter()
                .map(|row| {
                    row.as_ref().map(|row| {
                        let mut bytes = vec![0_u8; dimensions.div_ceil(8)];
                        for (dimension, value) in row.iter().copied().enumerate() {
                            if value != 0.0 {
                                bytes[dimension / 8] |= 1 << (dimension % 8);
                            }
                        }
                        bytes
                    })
                })
                .collect::<Vec<_>>();
            let rows = packed
                .iter()
                .map(|row| {
                    row.as_ref()
                        .map(|row| row.iter().copied().map(Some).collect::<Vec<Option<u8>>>())
                })
                .collect::<Vec<_>>();
            array(FixedSizeListArray::from_iter_primitive::<UInt8Type, _, _>(
                rows,
                i32::try_from(dimensions.div_ceil(8)).map_err(|_| {
                    BorsukError::InvalidStorage("WAL binary vector width exceeds i32".to_string())
                })?,
            ))
        }
    })
}

fn wal_vector_data_type(element_type: VectorElementType, dimensions: usize) -> Result<DataType> {
    if element_type == VectorElementType::Binary {
        let packed_bytes = i32::try_from(dimensions.div_ceil(8)).map_err(|_| {
            BorsukError::InvalidStorage("WAL binary vector width exceeds i32".to_string())
        })?;
        return Ok(DataType::FixedSizeList(
            Arc::new(Field::new_list_field(DataType::UInt8, true)),
            packed_bytes,
        ));
    }
    crate::arrow_vector_sidecar::vector_data_type(element_type, dimensions)
}

fn wal_vector_element_type_code(element_type: VectorElementType) -> u8 {
    match element_type {
        VectorElementType::Float32 => 0,
        VectorElementType::Float16 => 1,
        VectorElementType::BFloat16 => 2,
        VectorElementType::Float8E4M3Fn => 3,
        VectorElementType::Float8E5M2 => 4,
        VectorElementType::Int8 => 5,
        VectorElementType::Binary => 6,
    }
}

fn wal_vector_element_type_from_code(code: u8) -> Result<VectorElementType> {
    match code {
        0 => Ok(VectorElementType::Float32),
        1 => Ok(VectorElementType::Float16),
        2 => Ok(VectorElementType::BFloat16),
        3 => Ok(VectorElementType::Float8E4M3Fn),
        4 => Ok(VectorElementType::Float8E5M2),
        5 => Ok(VectorElementType::Int8),
        6 => Ok(VectorElementType::Binary),
        other => Err(BorsukError::InvalidStorage(format!(
            "WAL vector element type code {other} is unsupported"
        ))),
    }
}

fn fixed_u8_array<'a>(
    values: impl IntoIterator<Item = &'a [u8]>,
    dimensions: usize,
) -> FixedSizeListArray {
    let values = values
        .into_iter()
        .map(|code| Some(code.iter().copied().map(Some).collect::<Vec<_>>()))
        .collect::<Vec<_>>();
    FixedSizeListArray::from_iter_primitive::<UInt8Type, _, _>(values, dimensions as i32)
}

fn sparse_u32_list_array<'a>(values: impl IntoIterator<Item = &'a [u32]>) -> ListArray {
    let values = values
        .into_iter()
        .map(|indices| {
            (!indices.is_empty()).then(|| indices.iter().copied().map(Some).collect::<Vec<_>>())
        })
        .collect::<Vec<_>>();
    ListArray::from_iter_primitive::<UInt32Type, _, _>(values)
}

fn optional_u32_list_array<'a>(values: impl IntoIterator<Item = Option<&'a [u32]>>) -> ListArray {
    let values = values
        .into_iter()
        .map(|indices| indices.map(|indices| indices.iter().copied().map(Some).collect::<Vec<_>>()))
        .collect::<Vec<_>>();
    ListArray::from_iter_primitive::<UInt32Type, _, _>(values)
}

fn optional_f32_list_array<'a>(values: impl IntoIterator<Item = Option<&'a [f32]>>) -> ListArray {
    let values = values
        .into_iter()
        .map(|weights| weights.map(|weights| weights.iter().copied().map(Some).collect::<Vec<_>>()))
        .collect::<Vec<_>>();
    ListArray::from_iter_primitive::<Float32Type, _, _>(values)
}

/// Encode one sorted convergent record-state run as stock-readable Parquet.
/// Both winning puts and deletes are retained so independent writers merge by
/// the complete `(HLC, writer)` order without a collection-wide counter.
pub(crate) fn tombstone_ids_to_parquet(entries: &[(Vec<u8>, MutationState)]) -> Result<Vec<u8>> {
    validate_mutation_state_entries(entries)?;
    let schema = Arc::new(Schema::new(vec![
        Field::new("record_id", DataType::Binary, false),
        Field::new("mutation_hlc", DataType::UInt64, false),
        Field::new("mutation_writer", DataType::FixedSizeBinary(16), false),
        Field::new("mutation_digest", DataType::FixedSizeBinary(32), false),
        Field::new("deleted", DataType::Boolean, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            array(BinaryArray::from_iter_values(
                entries.iter().map(|(id, _)| id.as_slice()),
            )),
            array(UInt64Array::from_iter_values(
                entries
                    .iter()
                    .map(|(_, state)| state.stamp().version().hlc()),
            )),
            array(FixedSizeBinaryArray::try_from_iter(
                entries
                    .iter()
                    .map(|(_, state)| state.stamp().version().writer()),
            )?),
            array(FixedSizeBinaryArray::try_from_iter(
                entries.iter().map(|(_, state)| state.stamp().digest()),
            )?),
            array(BooleanArray::from_iter(
                entries.iter().map(|(_, state)| Some(state.is_deleted())),
            )),
        ],
    )?;
    write_batch(batch)
}

/// Decode a convergent record-state run. Experimental generation-only tables
/// are deliberately rejected instead of acquiring a compatibility fallback.
pub(crate) fn tombstone_ids_from_parquet(bytes: &[u8]) -> Result<Vec<(Vec<u8>, MutationState)>> {
    let mut entries = Vec::new();
    for batch in read_batches(bytes)? {
        let stamp_columns = mutation_stamp_columns(&batch.schema())?.ok_or_else(|| {
            BorsukError::InvalidStorage(
                "tombstone table is missing canonical mutation columns".to_string(),
            )
        })?;
        let deleted_column = column_index(&batch, "deleted")?;
        for row in 0..batch.num_rows() {
            let stamp =
                mutation_stamp_value(&batch, Some(stamp_columns), row)?.ok_or_else(|| {
                    BorsukError::InvalidStorage(
                        "tombstone mutation stamp disappeared during decoding".to_string(),
                    )
                })?;
            let operation = if boolean_value(&batch, deleted_column, row, "deleted")? {
                MutationOperation::Delete
            } else {
                MutationOperation::Put
            };
            entries.push((
                binary_value_by_name(&batch, row, "record_id")?.to_vec(),
                MutationState::new(stamp, operation),
            ));
        }
    }
    validate_mutation_state_entries(&entries)?;
    Ok(entries)
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PositionedTombstoneRow {
    pub(crate) modality: String,
    pub(crate) record_id: Vec<u8>,
    pub(crate) state: MutationState,
    pub(crate) created_at_seconds: i64,
    pub(crate) created_at_nanos: u32,
}

fn positioned_tombstone_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("modality", DataType::Utf8, false),
        Field::new("record_id", DataType::Binary, false),
        Field::new("mutation_hlc", DataType::UInt64, false),
        Field::new("mutation_writer", DataType::FixedSizeBinary(16), false),
        Field::new("mutation_digest", DataType::FixedSizeBinary(32), false),
        Field::new("deleted", DataType::Boolean, false),
        Field::new("created_at_seconds", DataType::Int64, false),
        Field::new("created_at_nanos", DataType::UInt32, false),
    ]))
}

pub(crate) fn positioned_tombstones_to_parquet(rows: &[PositionedTombstoneRow]) -> Result<Vec<u8>> {
    if rows.is_empty()
        || rows
            .iter()
            .any(|row| row.modality.is_empty() || row.record_id.is_empty())
        || rows.windows(2).any(|pair| {
            (&pair[0].modality, pair[0].record_id.as_slice())
                >= (&pair[1].modality, pair[1].record_id.as_slice())
        })
    {
        return Err(BorsukError::InvalidStorage(
            "positioned tombstones must be non-empty and strictly sorted by modality and id"
                .to_string(),
        ));
    }
    let batch = RecordBatch::try_new(
        positioned_tombstone_schema(),
        vec![
            array(StringArray::from_iter_values(
                rows.iter().map(|row| row.modality.as_str()),
            )),
            array(BinaryArray::from_iter_values(
                rows.iter().map(|row| row.record_id.as_slice()),
            )),
            array(UInt64Array::from_iter_values(
                rows.iter().map(|row| row.state.stamp().version().hlc()),
            )),
            array(FixedSizeBinaryArray::try_from_iter(
                rows.iter().map(|row| row.state.stamp().version().writer()),
            )?),
            array(FixedSizeBinaryArray::try_from_iter(
                rows.iter().map(|row| row.state.stamp().digest()),
            )?),
            array(BooleanArray::from_iter(
                rows.iter().map(|row| Some(row.state.is_deleted())),
            )),
            array(Int64Array::from_iter_values(
                rows.iter().map(|row| row.created_at_seconds),
            )),
            array(UInt32Array::from_iter_values(
                rows.iter().map(|row| row.created_at_nanos),
            )),
        ],
    )?;
    write_batch(batch)
}

pub(crate) fn positioned_tombstones_from_parquet(
    bytes: &[u8],
) -> Result<Vec<PositionedTombstoneRow>> {
    let mut rows = Vec::new();
    for batch in read_batches(bytes)? {
        if batch.schema().as_ref() != positioned_tombstone_schema().as_ref() {
            return Err(BorsukError::InvalidStorage(
                "positioned tombstone schema is not exact".to_string(),
            ));
        }
        let stamp_columns = mutation_stamp_columns(batch.schema().as_ref())?.expect("exact schema");
        for row in 0..batch.num_rows() {
            let stamp =
                mutation_stamp_value(&batch, Some(stamp_columns), row)?.expect("exact schema");
            rows.push(PositionedTombstoneRow {
                modality: string_value_by_name(&batch, row, "modality")?.to_string(),
                record_id: binary_value_by_name(&batch, row, "record_id")?.to_vec(),
                state: MutationState::new(
                    stamp,
                    if boolean_value(&batch, column_index(&batch, "deleted")?, row, "deleted")? {
                        MutationOperation::Delete
                    } else {
                        MutationOperation::Put
                    },
                ),
                created_at_seconds: primitive_value_by_name::<Int64Type>(
                    &batch,
                    row,
                    "created_at_seconds",
                )?,
                created_at_nanos: primitive_value_by_name::<UInt32Type>(
                    &batch,
                    row,
                    "created_at_nanos",
                )?,
            });
        }
    }
    if rows.is_empty()
        || rows.windows(2).any(|pair| {
            (&pair[0].modality, pair[0].record_id.as_slice())
                >= (&pair[1].modality, pair[1].record_id.as_slice())
        })
    {
        return Err(BorsukError::InvalidStorage(
            "positioned tombstones are empty or not canonically sorted".to_string(),
        ));
    }
    Ok(rows)
}

fn validate_mutation_state_entries(entries: &[(Vec<u8>, MutationState)]) -> Result<()> {
    if entries.iter().any(|(id, _)| id.is_empty())
        || entries
            .windows(2)
            .any(|pair| pair[0].0.as_slice() >= pair[1].0.as_slice())
    {
        return Err(BorsukError::InvalidStorage(
            "tombstone mutation states must have non-empty, strictly sorted ids".to_string(),
        ));
    }
    Ok(())
}

pub(crate) type IdDirectoryStateRow = (Vec<u8>, u64, u32, MutationState);

pub(crate) fn id_directory_states_to_parquet(entries: &[IdDirectoryStateRow]) -> Result<Vec<u8>> {
    if entries.iter().any(|(id, _, _, _)| id.is_empty())
        || entries
            .windows(2)
            .any(|pair| pair[0].0.as_slice() >= pair[1].0.as_slice())
    {
        return Err(BorsukError::InvalidStorage(
            "ID-directory rows must have non-empty, strictly sorted ids".to_string(),
        ));
    }
    let schema = Arc::new(Schema::new(vec![
        Field::new("record_id", DataType::Binary, false),
        Field::new("routing_epoch", DataType::UInt64, false),
        Field::new("cell_ordinal", DataType::UInt32, false),
        Field::new("mutation_hlc", DataType::UInt64, false),
        Field::new("mutation_writer", DataType::FixedSizeBinary(16), false),
        Field::new("mutation_digest", DataType::FixedSizeBinary(32), false),
        Field::new("deleted", DataType::Boolean, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            array(BinaryArray::from_iter_values(
                entries.iter().map(|(id, _, _, _)| id.as_slice()),
            )),
            array(UInt64Array::from_iter_values(
                entries.iter().map(|(_, epoch, _, _)| *epoch),
            )),
            array(UInt32Array::from_iter_values(
                entries.iter().map(|(_, _, ordinal, _)| *ordinal),
            )),
            array(UInt64Array::from_iter_values(
                entries
                    .iter()
                    .map(|(_, _, _, state)| state.stamp().version().hlc()),
            )),
            array(FixedSizeBinaryArray::try_from_iter(
                entries
                    .iter()
                    .map(|(_, _, _, state)| state.stamp().version().writer()),
            )?),
            array(FixedSizeBinaryArray::try_from_iter(
                entries
                    .iter()
                    .map(|(_, _, _, state)| state.stamp().digest()),
            )?),
            array(BooleanArray::from_iter(
                entries
                    .iter()
                    .map(|(_, _, _, state)| Some(state.is_deleted())),
            )),
        ],
    )?;
    write_batch(batch)
}

pub(crate) fn id_directory_states_from_parquet(bytes: &[u8]) -> Result<Vec<IdDirectoryStateRow>> {
    let mut entries = Vec::new();
    for batch in read_batches(bytes)? {
        let stamp_columns = mutation_stamp_columns(&batch.schema())?.ok_or_else(|| {
            BorsukError::InvalidStorage(
                "ID-directory table is missing canonical mutation columns".to_string(),
            )
        })?;
        let deleted_column = column_index(&batch, "deleted")?;
        for row in 0..batch.num_rows() {
            let stamp =
                mutation_stamp_value(&batch, Some(stamp_columns), row)?.ok_or_else(|| {
                    BorsukError::InvalidStorage(
                        "ID-directory mutation stamp disappeared during decoding".to_string(),
                    )
                })?;
            let operation = if boolean_value(&batch, deleted_column, row, "deleted")? {
                MutationOperation::Delete
            } else {
                MutationOperation::Put
            };
            entries.push((
                binary_value_by_name(&batch, row, "record_id")?.to_vec(),
                primitive_value_by_name::<UInt64Type>(&batch, row, "routing_epoch")?,
                primitive_value_by_name::<UInt32Type>(&batch, row, "cell_ordinal")?,
                MutationState::new(stamp, operation),
            ));
        }
    }
    if entries.iter().any(|(id, _, _, _)| id.is_empty())
        || entries
            .windows(2)
            .any(|pair| pair[0].0.as_slice() >= pair[1].0.as_slice())
    {
        return Err(BorsukError::InvalidStorage(
            "ID-directory rows must have non-empty, strictly sorted ids".to_string(),
        ));
    }
    Ok(entries)
}

/// Encode one bounded, sorted page of BM25 document-frequency corrections.
pub(crate) fn bm25_stats_delta_page_to_parquet(entries: &[(u32, i64)]) -> Result<Vec<u8>> {
    validate_bm25_stats_delta_entries(entries)?;
    let schema = Arc::new(Schema::new(vec![
        Field::new("term", DataType::UInt32, false),
        Field::new("document_frequency_delta", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            array(UInt32Array::from_iter_values(
                entries.iter().map(|(term, _)| *term),
            )),
            array(Int64Array::from_iter_values(
                entries.iter().map(|(_, delta)| *delta),
            )),
        ],
    )?;
    write_batch(batch)
}

/// Decode and validate one bounded BM25 statistics-delta page.
pub(crate) fn bm25_stats_delta_page_from_parquet(bytes: &[u8]) -> Result<Vec<(u32, i64)>> {
    let mut entries = Vec::new();
    for batch in read_batches(bytes)? {
        let terms = batch
            .column_by_name("term")
            .and_then(|column| column.as_any().downcast_ref::<UInt32Array>())
            .ok_or_else(|| {
                BorsukError::InvalidStorage(
                    "BM25 statistics-delta page is missing u32 term".to_string(),
                )
            })?;
        let deltas = batch
            .column_by_name("document_frequency_delta")
            .and_then(|column| column.as_any().downcast_ref::<Int64Array>())
            .ok_or_else(|| {
                BorsukError::InvalidStorage(
                    "BM25 statistics-delta page is missing i64 document_frequency_delta"
                        .to_string(),
                )
            })?;
        if terms.len() != deltas.len() {
            return Err(BorsukError::InvalidStorage(
                "BM25 statistics-delta page columns differ in length".to_string(),
            ));
        }
        entries.extend((0..terms.len()).map(|row| (terms.value(row), deltas.value(row))));
    }
    validate_bm25_stats_delta_entries(&entries)?;
    Ok(entries)
}

fn validate_bm25_stats_delta_entries(entries: &[(u32, i64)]) -> Result<()> {
    if entries.is_empty() {
        return Err(BorsukError::InvalidStorage(
            "BM25 statistics-delta page must not be empty".to_string(),
        ));
    }
    for (index, (term, delta)) in entries.iter().enumerate() {
        if *delta == 0 {
            return Err(BorsukError::InvalidStorage(format!(
                "BM25 statistics-delta term {term} has a zero correction"
            )));
        }
        if index > 0 && entries[index - 1].0 >= *term {
            return Err(BorsukError::InvalidStorage(
                "BM25 statistics-delta terms must be strictly increasing".to_string(),
            ));
        }
    }
    Ok(())
}

pub(crate) struct PositionedPayloadMetadata {
    pub(crate) rows: u64,
    pub(crate) min_stamp: PositionedMutationStamp,
    pub(crate) max_stamp: PositionedMutationStamp,
    pub(crate) version_digests: BTreeMap<(u64, [u8; 16]), [u8; 32]>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum PositionedRouteAssignmentKind {
    Catalog,
    Analyzer,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum PositionedRouteProjectionKind {
    Primary,
    Dense,
    Sparse,
    Text,
    LateInteraction,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PositionedRouteAssignment {
    pub(crate) kind: PositionedRouteAssignmentKind,
    pub(crate) checksum: [u8; 32],
    pub(crate) routing_epoch: Option<u64>,
}

impl PositionedRouteAssignment {
    pub(crate) fn catalog(checksum: [u8; 32], routing_epoch: u64) -> Result<Self> {
        if checksum == [0; 32] {
            return Err(BorsukError::InvalidStorage(
                "positioned route catalog checksum must be nonzero".to_string(),
            ));
        }
        if routing_epoch == 0 {
            return Err(BorsukError::InvalidStorage(
                "positioned route catalog epoch must be positive".to_string(),
            ));
        }
        Ok(Self {
            kind: PositionedRouteAssignmentKind::Catalog,
            checksum,
            routing_epoch: Some(routing_epoch),
        })
    }

    pub(crate) fn analyzer(checksum: [u8; 32]) -> Result<Self> {
        if checksum == [0; 32] {
            return Err(BorsukError::InvalidStorage(
                "positioned route analyzer checksum must be nonzero".to_string(),
            ));
        }
        Ok(Self {
            kind: PositionedRouteAssignmentKind::Analyzer,
            checksum,
            routing_epoch: None,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PositionedRoutePlanRow {
    pub(crate) record_id: Option<Vec<u8>>,
    pub(crate) modality: String,
    pub(crate) projection_kind: PositionedRouteProjectionKind,
    pub(crate) projected_ordinal: Option<u32>,
    pub(crate) assignment: PositionedRouteAssignment,
    pub(crate) cell_ordinal: Option<u32>,
    pub(crate) logical_row_count: u64,
    pub(crate) stamp: MutationStamp,
}

impl PositionedRoutePlanRow {
    pub(crate) fn summary(
        modality: &str,
        projection_kind: PositionedRouteProjectionKind,
        assignment: PositionedRouteAssignment,
        logical_row_count: u64,
        stamp: MutationStamp,
    ) -> Result<Self> {
        let row = Self {
            record_id: None,
            modality: modality.to_string(),
            projection_kind,
            projected_ordinal: None,
            assignment,
            cell_ordinal: None,
            logical_row_count,
            stamp,
        };
        validate_positioned_route_plan_row(&row)?;
        Ok(row)
    }

    pub(crate) fn routed(
        record_id: Vec<u8>,
        modality: &str,
        projection_kind: PositionedRouteProjectionKind,
        projected_ordinal: u32,
        assignment: PositionedRouteAssignment,
        cell_ordinal: u32,
        stamp: MutationStamp,
    ) -> Result<Self> {
        let row = Self {
            record_id: Some(record_id),
            modality: modality.to_string(),
            projection_kind,
            projected_ordinal: Some(projected_ordinal),
            assignment,
            cell_ordinal: Some(cell_ordinal),
            logical_row_count: 0,
            stamp,
        };
        validate_positioned_route_plan_row(&row)?;
        Ok(row)
    }

    pub(crate) fn term_partitioned(
        record_id: Vec<u8>,
        modality: &str,
        projection_kind: PositionedRouteProjectionKind,
        projected_ordinal: u32,
        assignment: PositionedRouteAssignment,
        stamp: MutationStamp,
    ) -> Result<Self> {
        let row = Self {
            record_id: Some(record_id),
            modality: modality.to_string(),
            projection_kind,
            projected_ordinal: Some(projected_ordinal),
            assignment,
            cell_ordinal: None,
            logical_row_count: 0,
            stamp,
        };
        validate_positioned_route_plan_row(&row)?;
        Ok(row)
    }

    fn is_summary(&self) -> bool {
        self.record_id.is_none()
    }
}

fn validate_positioned_route_plan_row(row: &PositionedRoutePlanRow) -> Result<()> {
    if row.modality.is_empty() || row.modality.len() > 256 {
        return Err(BorsukError::InvalidStorage(
            "positioned route modality length is outside 1..=256".to_string(),
        ));
    }
    if row.assignment.checksum == [0; 32] {
        return Err(BorsukError::InvalidStorage(
            "positioned route assignment checksum must be nonzero".to_string(),
        ));
    }
    if (row.projection_kind == PositionedRouteProjectionKind::Primary)
        != (row.modality == "primary")
    {
        return Err(BorsukError::InvalidStorage(
            "positioned primary route must use exactly the `primary` modality".to_string(),
        ));
    }
    let expected_assignment = match row.projection_kind {
        PositionedRouteProjectionKind::Primary
        | PositionedRouteProjectionKind::Dense
        | PositionedRouteProjectionKind::LateInteraction => PositionedRouteAssignmentKind::Catalog,
        PositionedRouteProjectionKind::Sparse | PositionedRouteProjectionKind::Text => {
            PositionedRouteAssignmentKind::Analyzer
        }
    };
    if row.assignment.kind != expected_assignment {
        return Err(BorsukError::InvalidStorage(
            "positioned route projection uses the wrong assignment kind".to_string(),
        ));
    }
    match row.assignment.kind {
        PositionedRouteAssignmentKind::Catalog => {
            if row.assignment.routing_epoch.is_none_or(|epoch| epoch == 0) {
                return Err(BorsukError::InvalidStorage(
                    "positioned catalog route requires a positive epoch".to_string(),
                ));
            }
            if !row.is_summary() && row.cell_ordinal.is_none() {
                return Err(BorsukError::InvalidStorage(
                    "positioned catalog route requires a cell ordinal".to_string(),
                ));
            }
        }
        PositionedRouteAssignmentKind::Analyzer => {
            if row.assignment.routing_epoch.is_some() || row.cell_ordinal.is_some() {
                return Err(BorsukError::InvalidStorage(
                    "positioned analyzer route cannot carry an epoch or cell".to_string(),
                ));
            }
        }
    }
    match (&row.record_id, row.projected_ordinal, row.cell_ordinal) {
        (None, None, None) => {}
        (Some(id), Some(_), _) if !id.is_empty() && row.logical_row_count == 0 => {}
        _ => {
            return Err(BorsukError::InvalidStorage(
                "positioned route row has an invalid summary/data shape".to_string(),
            ));
        }
    }
    Ok(())
}

pub(crate) fn validate_positioned_route_plan(rows: &[PositionedRoutePlanRow]) -> Result<()> {
    if rows.is_empty() {
        return Err(BorsukError::InvalidStorage(
            "positioned route plan must contain at least one summary".to_string(),
        ));
    }
    for row in rows {
        validate_positioned_route_plan_row(row)?;
    }
    for pair in rows.windows(2) {
        let left = &pair[0];
        let right = &pair[1];
        if (
            left.modality.as_str(),
            u8::from(!left.is_summary()),
            left.record_id.as_deref(),
            left.projected_ordinal,
        ) >= (
            right.modality.as_str(),
            u8::from(!right.is_summary()),
            right.record_id.as_deref(),
            right.projected_ordinal,
        ) {
            return Err(BorsukError::InvalidStorage(
                "positioned route plan rows are not canonical".to_string(),
            ));
        }
    }
    let mut summaries = BTreeMap::<
        &str,
        (
            PositionedRouteProjectionKind,
            &PositionedRouteAssignment,
            u64,
            MutationStamp,
        ),
    >::new();
    let mut observed = BTreeMap::<&str, u64>::new();
    let mut late_ordinals = BTreeMap::<(&str, &[u8]), u32>::new();
    let mut version_digests = BTreeMap::<MutationVersion, [u8; 32]>::new();
    let mut minimum_data_stamp = None::<MutationStamp>;
    for row in rows {
        if let Some(previous) = version_digests.insert(row.stamp.version(), row.stamp.digest())
            && previous != row.stamp.digest()
        {
            return Err(BorsukError::InvalidStorage(
                "positioned route plan has conflicting digests for one mutation version"
                    .to_string(),
            ));
        }
        if row.is_summary() {
            if summaries
                .insert(
                    row.modality.as_str(),
                    (
                        row.projection_kind,
                        &row.assignment,
                        row.logical_row_count,
                        row.stamp,
                    ),
                )
                .is_some()
            {
                return Err(BorsukError::InvalidStorage(
                    "positioned route plan repeats a modality summary".to_string(),
                ));
            }
            continue;
        }
        let Some((projection_kind, assignment, _, _)) = summaries.get(row.modality.as_str()) else {
            return Err(BorsukError::InvalidStorage(
                "positioned route row precedes its modality summary".to_string(),
            ));
        };
        if *projection_kind != row.projection_kind || *assignment != &row.assignment {
            return Err(BorsukError::InvalidStorage(
                "positioned route row disagrees with its modality summary".to_string(),
            ));
        }
        let count = observed.entry(row.modality.as_str()).or_default();
        *count = count.checked_add(1).ok_or_else(|| {
            BorsukError::InvalidStorage("positioned route row count overflow".to_string())
        })?;
        minimum_data_stamp = match minimum_data_stamp {
            None => Some(row.stamp),
            Some(current) if row.stamp.version() < current.version() => Some(row.stamp),
            Some(current) => Some(current),
        };
        let ordinal = row.projected_ordinal.expect("data row shape was validated");
        if row.projection_kind == PositionedRouteProjectionKind::LateInteraction {
            let id = row
                .record_id
                .as_deref()
                .expect("data row shape was validated");
            let expected = late_ordinals
                .entry((row.modality.as_str(), id))
                .or_default();
            if ordinal != *expected {
                return Err(BorsukError::InvalidStorage(
                    "positioned late-interaction token ordinals are not contiguous".to_string(),
                ));
            }
            *expected = expected.checked_add(1).ok_or_else(|| {
                BorsukError::InvalidStorage(
                    "positioned late-interaction token ordinal overflow".to_string(),
                )
            })?;
        } else if ordinal != 0 {
            return Err(BorsukError::InvalidStorage(
                "positioned entity-level route ordinal must be zero".to_string(),
            ));
        }
    }
    let expected_summary_stamp = minimum_data_stamp.unwrap_or(rows[0].stamp);
    for (modality, (_, _, expected, summary_stamp)) in summaries {
        if observed.get(modality).copied().unwrap_or(0) != expected {
            return Err(BorsukError::InvalidStorage(format!(
                "positioned route modality `{modality}` row count does not match its summary"
            )));
        }
        if summary_stamp != expected_summary_stamp {
            return Err(BorsukError::InvalidStorage(
                "positioned route summaries must use the transaction minimum stamp".to_string(),
            ));
        }
    }
    Ok(())
}

fn positioned_route_plan_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("format_version", DataType::UInt16, false),
        Field::new("row_kind", DataType::UInt8, false),
        Field::new("record_id", DataType::Binary, true),
        Field::new("modality", DataType::Utf8, false),
        Field::new("projection_kind", DataType::UInt8, false),
        Field::new("projected_ordinal", DataType::UInt32, true),
        Field::new("assignment_kind", DataType::UInt8, false),
        Field::new("assignment_checksum", DataType::FixedSizeBinary(32), false),
        Field::new("routing_epoch", DataType::UInt64, true),
        Field::new("cell_ordinal", DataType::UInt32, true),
        Field::new("logical_row_count", DataType::UInt64, false),
        Field::new("mutation_hlc", DataType::UInt64, false),
        Field::new("mutation_writer", DataType::FixedSizeBinary(16), false),
        Field::new("mutation_digest", DataType::FixedSizeBinary(32), false),
    ]))
}

pub(crate) fn positioned_route_plan_to_parquet(rows: &[PositionedRoutePlanRow]) -> Result<Vec<u8>> {
    validate_positioned_route_plan(rows)?;
    let batch = RecordBatch::try_new(
        positioned_route_plan_schema(),
        vec![
            array(UInt16Array::from_iter_values(rows.iter().map(|_| 1))),
            array(UInt8Array::from_iter_values(
                rows.iter().map(|row| u8::from(!row.is_summary())),
            )),
            array(BinaryArray::from_iter(
                rows.iter().map(|row| row.record_id.as_deref()),
            )),
            array(StringArray::from_iter_values(
                rows.iter().map(|row| row.modality.as_str()),
            )),
            array(UInt8Array::from_iter_values(rows.iter().map(
                |row| match row.projection_kind {
                    PositionedRouteProjectionKind::Primary => 0,
                    PositionedRouteProjectionKind::Dense => 1,
                    PositionedRouteProjectionKind::Sparse => 2,
                    PositionedRouteProjectionKind::Text => 3,
                    PositionedRouteProjectionKind::LateInteraction => 4,
                },
            ))),
            array(UInt32Array::from_iter(
                rows.iter().map(|row| row.projected_ordinal),
            )),
            array(UInt8Array::from_iter_values(rows.iter().map(
                |row| match row.assignment.kind {
                    PositionedRouteAssignmentKind::Catalog => 0,
                    PositionedRouteAssignmentKind::Analyzer => 1,
                },
            ))),
            array(FixedSizeBinaryArray::try_from_iter(
                rows.iter().map(|row| row.assignment.checksum),
            )?),
            array(UInt64Array::from_iter(
                rows.iter().map(|row| row.assignment.routing_epoch),
            )),
            array(UInt32Array::from_iter(
                rows.iter().map(|row| row.cell_ordinal),
            )),
            array(UInt64Array::from_iter_values(
                rows.iter().map(|row| row.logical_row_count),
            )),
            array(UInt64Array::from_iter_values(
                rows.iter().map(|row| row.stamp.version().hlc()),
            )),
            array(FixedSizeBinaryArray::try_from_iter(
                rows.iter().map(|row| row.stamp.version().writer()),
            )?),
            array(FixedSizeBinaryArray::try_from_iter(
                rows.iter().map(|row| row.stamp.digest()),
            )?),
        ],
    )?;
    write_batch(batch)
}

pub(crate) fn positioned_route_plan_from_parquet(
    bytes: &[u8],
) -> Result<Vec<PositionedRoutePlanRow>> {
    catch_parquet_decode_panic(|| positioned_route_plan_from_parquet_inner(bytes))
}

fn positioned_route_plan_from_parquet_inner(bytes: &[u8]) -> Result<Vec<PositionedRoutePlanRow>> {
    let encoded_bytes = u64::try_from(bytes.len()).map_err(|_| {
        BorsukError::InvalidStorage("positioned route plan byte count exceeds u64".to_string())
    })?;
    if encoded_bytes > crate::positioned_log::MAX_APPEND_ENCODED_BYTES {
        return Err(BorsukError::InvalidStorage(
            "positioned route plan exceeds the append byte bound".to_string(),
        ));
    }
    let builder = ParquetRecordBatchReaderBuilder::try_new(Bytes::copy_from_slice(bytes))?;
    let physical_rows =
        u64::try_from(builder.metadata().file_metadata().num_rows()).map_err(|_| {
            BorsukError::InvalidStorage("positioned route plan row count exceeds u64".to_string())
        })?;
    if physical_rows == 0 || physical_rows > crate::positioned_log::MAX_APPEND_ROWS {
        return Err(BorsukError::InvalidStorage(
            "positioned route plan row count is outside the append bound".to_string(),
        ));
    }
    drop(builder);
    let mut rows = Vec::new();
    for batch in read_batches(bytes)? {
        if batch.schema().as_ref() != positioned_route_plan_schema().as_ref() {
            return Err(BorsukError::InvalidStorage(
                "positioned route plan schema is not exact".to_string(),
            ));
        }
        let record_ids = batch
            .column(column_index(&batch, "record_id")?)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .expect("exact schema");
        let modality_column = column_index(&batch, "modality")?;
        for row in 0..batch.num_rows() {
            if required_primitive_value::<UInt16Type>(
                &batch,
                column_index(&batch, "format_version")?,
                row,
                "format_version",
            )? != 1
            {
                return Err(BorsukError::InvalidStorage(
                    "positioned route plan format version is not 1".to_string(),
                ));
            }
            let row_kind = required_primitive_value::<UInt8Type>(
                &batch,
                column_index(&batch, "row_kind")?,
                row,
                "row_kind",
            )?;
            let record_id = (!record_ids.is_null(row)).then(|| record_ids.value(row).to_vec());
            let assignment_kind = match required_primitive_value::<UInt8Type>(
                &batch,
                column_index(&batch, "assignment_kind")?,
                row,
                "assignment_kind",
            )? {
                0 => PositionedRouteAssignmentKind::Catalog,
                1 => PositionedRouteAssignmentKind::Analyzer,
                _ => {
                    return Err(BorsukError::InvalidStorage(
                        "positioned route assignment kind is invalid".to_string(),
                    ));
                }
            };
            let assignment = PositionedRouteAssignment {
                kind: assignment_kind,
                checksum: fixed_size_binary_value::<32>(
                    &batch,
                    column_index(&batch, "assignment_checksum")?,
                    row,
                    "assignment_checksum",
                )?,
                routing_epoch: primitive_optional_value_by_name::<UInt64Type>(
                    &batch,
                    row,
                    "routing_epoch",
                )?,
            };
            let projection_kind = match required_primitive_value::<UInt8Type>(
                &batch,
                column_index(&batch, "projection_kind")?,
                row,
                "projection_kind",
            )? {
                0 => PositionedRouteProjectionKind::Primary,
                1 => PositionedRouteProjectionKind::Dense,
                2 => PositionedRouteProjectionKind::Sparse,
                3 => PositionedRouteProjectionKind::Text,
                4 => PositionedRouteProjectionKind::LateInteraction,
                _ => {
                    return Err(BorsukError::InvalidStorage(
                        "positioned route projection kind is invalid".to_string(),
                    ));
                }
            };
            if batch.column(modality_column).is_null(row) {
                return Err(BorsukError::InvalidStorage(
                    "positioned route modality contains a null value".to_string(),
                ));
            }
            let stamp = MutationStamp::new(
                MutationVersion::from_parts(
                    required_primitive_value::<UInt64Type>(
                        &batch,
                        column_index(&batch, "mutation_hlc")?,
                        row,
                        "mutation_hlc",
                    )?,
                    fixed_size_binary_value::<16>(
                        &batch,
                        column_index(&batch, "mutation_writer")?,
                        row,
                        "mutation_writer",
                    )?,
                ),
                fixed_size_binary_value::<32>(
                    &batch,
                    column_index(&batch, "mutation_digest")?,
                    row,
                    "mutation_digest",
                )?,
            );
            let decoded = PositionedRoutePlanRow {
                record_id,
                modality: string_value(&batch, modality_column, row, "modality")?.to_string(),
                projection_kind,
                projected_ordinal: primitive_optional_value_by_name::<UInt32Type>(
                    &batch,
                    row,
                    "projected_ordinal",
                )?,
                assignment,
                cell_ordinal: primitive_optional_value_by_name::<UInt32Type>(
                    &batch,
                    row,
                    "cell_ordinal",
                )?,
                logical_row_count: required_primitive_value::<UInt64Type>(
                    &batch,
                    column_index(&batch, "logical_row_count")?,
                    row,
                    "logical_row_count",
                )?,
                stamp,
            };
            if row_kind != u8::from(!decoded.is_summary()) {
                return Err(BorsukError::InvalidStorage(
                    "positioned route row kind disagrees with its shape".to_string(),
                ));
            }
            rows.push(decoded);
        }
    }
    validate_positioned_route_plan(&rows)?;
    Ok(rows)
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PositionedTransactionMetadataRow {
    pub(crate) modality: String,
    pub(crate) logical_record_count: u64,
    pub(crate) next_generated_id_floor: u64,
    pub(crate) new_tombstone_ids: u64,
    pub(crate) document_count_delta: i64,
    pub(crate) total_document_length_delta: i64,
    pub(crate) term: Option<u32>,
    pub(crate) document_frequency_delta: i64,
    pub(crate) stamp: MutationStamp,
}

fn positioned_transaction_metadata_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("modality", DataType::Utf8, false),
        Field::new("logical_record_count", DataType::UInt64, false),
        Field::new("next_generated_id_floor", DataType::UInt64, false),
        Field::new("new_tombstone_ids", DataType::UInt64, false),
        Field::new("document_count_delta", DataType::Int64, false),
        Field::new("total_document_length_delta", DataType::Int64, false),
        Field::new("term", DataType::UInt32, true),
        Field::new("document_frequency_delta", DataType::Int64, false),
        Field::new("mutation_hlc", DataType::UInt64, false),
        Field::new("mutation_writer", DataType::FixedSizeBinary(16), false),
        Field::new("mutation_digest", DataType::FixedSizeBinary(32), false),
    ]))
}

pub(crate) fn positioned_transaction_metadata_to_parquet(
    rows: &[PositionedTransactionMetadataRow],
) -> Result<Vec<u8>> {
    if rows.is_empty() {
        return Err(BorsukError::InvalidStorage(
            "positioned transaction metadata must contain at least one row".to_string(),
        ));
    }
    let schema = positioned_transaction_metadata_schema();
    let batch = RecordBatch::try_new(
        schema,
        vec![
            array(StringArray::from_iter_values(
                rows.iter().map(|row| row.modality.as_str()),
            )),
            array(UInt64Array::from_iter_values(
                rows.iter().map(|row| row.logical_record_count),
            )),
            array(UInt64Array::from_iter_values(
                rows.iter().map(|row| row.next_generated_id_floor),
            )),
            array(UInt64Array::from_iter_values(
                rows.iter().map(|row| row.new_tombstone_ids),
            )),
            array(Int64Array::from_iter_values(
                rows.iter().map(|row| row.document_count_delta),
            )),
            array(Int64Array::from_iter_values(
                rows.iter().map(|row| row.total_document_length_delta),
            )),
            array(UInt32Array::from_iter(rows.iter().map(|row| row.term))),
            array(Int64Array::from_iter_values(
                rows.iter().map(|row| row.document_frequency_delta),
            )),
            array(UInt64Array::from_iter_values(
                rows.iter().map(|row| row.stamp.version().hlc()),
            )),
            array(FixedSizeBinaryArray::try_from_iter(
                rows.iter().map(|row| row.stamp.version().writer()),
            )?),
            array(FixedSizeBinaryArray::try_from_iter(
                rows.iter().map(|row| row.stamp.digest()),
            )?),
        ],
    )?;
    write_batch(batch)
}

pub(crate) fn positioned_transaction_metadata_from_parquet(
    bytes: &[u8],
) -> Result<Vec<PositionedTransactionMetadataRow>> {
    let mut rows = Vec::new();
    for batch in read_batches(bytes)? {
        if batch.schema().as_ref() != positioned_transaction_metadata_schema().as_ref() {
            return Err(BorsukError::InvalidStorage(
                "positioned transaction metadata schema is not exact".to_string(),
            ));
        }
        let stamp_columns = mutation_stamp_columns(batch.schema().as_ref())?.expect("exact schema");
        for row in 0..batch.num_rows() {
            rows.push(PositionedTransactionMetadataRow {
                modality: string_value_by_name(&batch, row, "modality")?.to_string(),
                logical_record_count: primitive_value_by_name::<UInt64Type>(
                    &batch,
                    row,
                    "logical_record_count",
                )?,
                next_generated_id_floor: primitive_value_by_name::<UInt64Type>(
                    &batch,
                    row,
                    "next_generated_id_floor",
                )?,
                new_tombstone_ids: primitive_value_by_name::<UInt64Type>(
                    &batch,
                    row,
                    "new_tombstone_ids",
                )?,
                document_count_delta: primitive_value_by_name::<Int64Type>(
                    &batch,
                    row,
                    "document_count_delta",
                )?,
                total_document_length_delta: primitive_value_by_name::<Int64Type>(
                    &batch,
                    row,
                    "total_document_length_delta",
                )?,
                term: primitive_optional_value_by_name::<UInt32Type>(&batch, row, "term")?,
                document_frequency_delta: primitive_value_by_name::<Int64Type>(
                    &batch,
                    row,
                    "document_frequency_delta",
                )?,
                stamp: mutation_stamp_value(&batch, Some(stamp_columns), row)?
                    .expect("exact schema"),
            });
        }
    }
    if rows.is_empty() {
        return Err(BorsukError::InvalidStorage(
            "positioned transaction metadata is empty".to_string(),
        ));
    }
    Ok(rows)
}

pub(crate) fn positioned_payload_metadata(
    bytes: &[u8],
    format: PositionedPayloadFormat,
    row_limit: u64,
) -> Result<PositionedPayloadMetadata> {
    if format == PositionedPayloadFormat::Parquet {
        return catch_parquet_decode_panic(|| {
            positioned_payload_metadata_inner(bytes, format, row_limit)
        });
    }
    positioned_payload_metadata_inner(bytes, format, row_limit)
}

fn positioned_payload_metadata_inner(
    bytes: &[u8],
    format: PositionedPayloadFormat,
    row_limit: u64,
) -> Result<PositionedPayloadMetadata> {
    match format {
        PositionedPayloadFormat::ArrowIpc => {
            reject_compressed_positioned_arrow_stream(bytes)?;
            let schema_reader = StreamReader::try_new(Cursor::new(bytes), None)?;
            let columns =
                mutation_stamp_columns(schema_reader.schema().as_ref())?.ok_or_else(|| {
                    BorsukError::InvalidStorage(
                        "positioned payload must contain typed mutation stamp columns".to_owned(),
                    )
                })?;
            drop(schema_reader);
            let reader = StreamReader::try_new(
                Cursor::new(bytes),
                Some(vec![columns.0, columns.1, columns.2]),
            )?;
            summarize_positioned_payload_batches(
                reader.map(|batch| batch.map_err(BorsukError::from)),
                row_limit,
            )
        }
        PositionedPayloadFormat::Parquet => {
            let mut builder =
                ParquetRecordBatchReaderBuilder::try_new(Bytes::copy_from_slice(bytes))?;
            let columns = mutation_stamp_columns(builder.schema().as_ref())?.ok_or_else(|| {
                BorsukError::InvalidStorage(
                    "positioned payload must contain typed mutation stamp columns".to_owned(),
                )
            })?;
            let physical_rows = u64::try_from(builder.metadata().file_metadata().num_rows())
                .map_err(|_| {
                    BorsukError::InvalidStorage(
                        "positioned payload row count exceeds u64".to_owned(),
                    )
                })?;
            if physical_rows > row_limit {
                return Err(BorsukError::InvalidStorage(
                    "positioned payload exceeds its declared row bound".to_owned(),
                ));
            }
            let projection =
                ProjectionMask::roots(builder.parquet_schema(), [columns.0, columns.1, columns.2]);
            builder = builder.with_projection(projection).with_batch_size(1024);
            summarize_positioned_payload_batches(
                builder
                    .build()?
                    .map(|batch| batch.map_err(BorsukError::from)),
                row_limit,
            )
        }
    }
}

fn reject_compressed_positioned_arrow_stream(bytes: &[u8]) -> Result<()> {
    let mut offset = 0_usize;
    loop {
        let prefix = bytes.get(offset..offset.saturating_add(4)).ok_or_else(|| {
            BorsukError::InvalidStorage("positioned Arrow stream has a truncated prefix".to_owned())
        })?;
        offset += 4;
        let mut metadata_len = u32::from_le_bytes(prefix.try_into().expect("four-byte prefix"));
        if metadata_len == u32::MAX {
            let length = bytes.get(offset..offset.saturating_add(4)).ok_or_else(|| {
                BorsukError::InvalidStorage(
                    "positioned Arrow stream has a truncated continuation".to_owned(),
                )
            })?;
            offset += 4;
            metadata_len = u32::from_le_bytes(length.try_into().expect("four-byte length"));
        }
        if metadata_len == 0 {
            return Ok(());
        }
        let metadata_len = usize::try_from(metadata_len).map_err(|_| {
            BorsukError::InvalidStorage("positioned Arrow metadata length exceeds usize".to_owned())
        })?;
        let metadata = bytes
            .get(
                offset..offset.checked_add(metadata_len).ok_or_else(|| {
                    BorsukError::InvalidStorage(
                        "positioned Arrow metadata offset overflow".to_owned(),
                    )
                })?,
            )
            .ok_or_else(|| {
                BorsukError::InvalidStorage(
                    "positioned Arrow stream has truncated metadata".to_owned(),
                )
            })?;
        offset += metadata_len;
        let message = arrow_ipc::root_as_message(metadata).map_err(|error| {
            BorsukError::InvalidStorage(format!(
                "positioned Arrow message metadata is invalid: {error}"
            ))
        })?;
        let compressed = message
            .header_as_record_batch()
            .is_some_and(|batch| batch.compression().is_some())
            || message
                .header_as_dictionary_batch()
                .and_then(|batch| batch.data())
                .is_some_and(|batch| batch.compression().is_some());
        if compressed {
            return Err(BorsukError::InvalidStorage(
                "compressed positioned Arrow IPC is outside the bounded validation profile"
                    .to_owned(),
            ));
        }
        let body_len = usize::try_from(message.bodyLength()).map_err(|_| {
            BorsukError::InvalidStorage("positioned Arrow body length is invalid".to_owned())
        })?;
        offset = offset.checked_add(body_len).ok_or_else(|| {
            BorsukError::InvalidStorage("positioned Arrow body offset overflow".to_owned())
        })?;
        if offset > bytes.len() {
            return Err(BorsukError::InvalidStorage(
                "positioned Arrow stream has a truncated body".to_owned(),
            ));
        }
    }
}

fn summarize_positioned_payload_batches(
    batches: impl Iterator<Item = Result<RecordBatch>>,
    row_limit: u64,
) -> Result<PositionedPayloadMetadata> {
    let mut rows = 0_u64;
    let mut min_stamp = None::<PositionedMutationStamp>;
    let mut max_stamp = None::<PositionedMutationStamp>;
    let mut version_digests = BTreeMap::new();
    for batch in batches {
        let batch = batch?;
        let columns = mutation_stamp_columns(batch.schema().as_ref())?.ok_or_else(|| {
            BorsukError::InvalidStorage(
                "positioned payload must contain typed mutation stamp columns".to_owned(),
            )
        })?;
        rows = rows
            .checked_add(u64::try_from(batch.num_rows()).map_err(|_| {
                BorsukError::InvalidStorage("positioned payload row count exceeds u64".to_owned())
            })?)
            .ok_or_else(|| {
                BorsukError::InvalidStorage("positioned payload row total overflow".to_owned())
            })?;
        if rows > row_limit {
            return Err(BorsukError::InvalidStorage(
                "positioned payload exceeds its declared row bound".to_owned(),
            ));
        }
        for row in 0..batch.num_rows() {
            let stamp = PositionedMutationStamp {
                hlc: required_primitive_value::<UInt64Type>(
                    &batch,
                    columns.0,
                    row,
                    "mutation_hlc",
                )?,
                writer: fixed_size_binary_value::<16>(&batch, columns.1, row, "mutation_writer")?,
                digest: fixed_size_binary_value::<32>(&batch, columns.2, row, "mutation_digest")?,
            };
            if let Some(existing) = version_digests.insert((stamp.hlc, stamp.writer), stamp.digest)
                && existing != stamp.digest
            {
                return Err(BorsukError::InvalidStorage(
                    "equal mutation version has unequal canonical digests".to_owned(),
                ));
            }
            min_stamp = Some(min_stamp.map_or(stamp, |existing| existing.min(stamp)));
            max_stamp = Some(max_stamp.map_or(stamp, |existing| existing.max(stamp)));
        }
    }
    let min_stamp = min_stamp.ok_or_else(|| {
        BorsukError::InvalidStorage(
            "positioned payload typed container must contain at least one row".to_owned(),
        )
    })?;
    Ok(PositionedPayloadMetadata {
        rows,
        min_stamp,
        max_stamp: max_stamp.expect("a minimum stamp implies a maximum stamp"),
        version_digests,
    })
}

pub(crate) struct DecodedPositionedEnvelope {
    pub(crate) envelope: PositionedMutationEnvelope,
    pub(crate) transaction_digest: String,
    pub(crate) request_digest: String,
}

const MAX_POSITIONED_ENVELOPE_BYTES: usize = 1024 * 1024;
const POSITIONED_ENVELOPE_LAYOUT: u16 = 15;

fn positioned_envelope_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("layout", DataType::UInt16, false),
        Field::new("transaction_id", DataType::Utf8, false),
        Field::new("transaction_digest", DataType::Utf8, false),
        Field::new("request_digest", DataType::Utf8, false),
        Field::new("source_epoch", DataType::UInt64, false),
        Field::new("shard", DataType::UInt8, false),
        Field::new("sequence", DataType::UInt64, false),
        Field::new("schema_fingerprint", DataType::Utf8, false),
        Field::new("min_mutation_hlc", DataType::UInt64, false),
        Field::new("min_mutation_writer", DataType::FixedSizeBinary(16), false),
        Field::new("min_mutation_digest", DataType::FixedSizeBinary(32), false),
        Field::new("max_mutation_hlc", DataType::UInt64, false),
        Field::new("max_mutation_writer", DataType::FixedSizeBinary(16), false),
        Field::new("max_mutation_digest", DataType::FixedSizeBinary(32), false),
        Field::new("payload_ordinal", DataType::UInt32, false),
        Field::new("payload_modality", DataType::Utf8, false),
        Field::new("payload_role", DataType::Utf8, false),
        Field::new("payload_id_bloom", DataType::Binary, false),
        Field::new("payload_format", DataType::Utf8, false),
        Field::new("payload_path", DataType::Utf8, false),
        Field::new("payload_checksum", DataType::Utf8, false),
        Field::new("payload_rows", DataType::UInt64, false),
        Field::new("payload_bytes", DataType::UInt64, false),
    ]))
}

fn validate_positioned_envelope_batch(batch: &RecordBatch) -> Result<()> {
    validate_positioned_envelope_schema_and_columns(batch.schema().as_ref(), batch.columns())
}

fn validate_positioned_envelope_schema_and_columns(
    schema: &Schema,
    columns: &[ArrayRef],
) -> Result<()> {
    if schema != positioned_envelope_schema().as_ref() {
        return Err(BorsukError::InvalidStorage(
            "positioned envelope schema is not the exact V14 schema".to_owned(),
        ));
    }
    if columns.iter().any(|column| column.null_count() != 0) {
        return Err(BorsukError::InvalidStorage(
            "positioned envelope contains a null value".to_owned(),
        ));
    }
    Ok(())
}

pub(crate) fn positioned_envelope_to_parquet(
    envelope: &PositionedMutationEnvelope,
    transaction_digest: &str,
    request_digest: &str,
) -> Result<Vec<u8>> {
    envelope.validate()?;
    let schema = positioned_envelope_schema();
    let payloads = &envelope.payloads;
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            array(UInt16Array::from_iter_values(
                payloads.iter().map(|_| POSITIONED_ENVELOPE_LAYOUT),
            )),
            array(StringArray::from_iter_values(
                payloads.iter().map(|_| envelope.transaction_id.as_str()),
            )),
            array(StringArray::from_iter_values(
                payloads.iter().map(|_| transaction_digest),
            )),
            array(StringArray::from_iter_values(
                payloads.iter().map(|_| request_digest),
            )),
            array(UInt64Array::from_iter_values(
                payloads.iter().map(|_| envelope.position.source_epoch),
            )),
            array(UInt8Array::from_iter_values(
                payloads.iter().map(|_| envelope.position.shard),
            )),
            array(UInt64Array::from_iter_values(
                payloads.iter().map(|_| envelope.position.sequence),
            )),
            array(StringArray::from_iter_values(
                payloads
                    .iter()
                    .map(|_| envelope.schema_fingerprint.as_str()),
            )),
            array(UInt64Array::from_iter_values(
                payloads.iter().map(|_| envelope.min_stamp.hlc),
            )),
            array(FixedSizeBinaryArray::try_from_iter(
                payloads.iter().map(|_| envelope.min_stamp.writer),
            )?),
            array(FixedSizeBinaryArray::try_from_iter(
                payloads.iter().map(|_| envelope.min_stamp.digest),
            )?),
            array(UInt64Array::from_iter_values(
                payloads.iter().map(|_| envelope.max_stamp.hlc),
            )),
            array(FixedSizeBinaryArray::try_from_iter(
                payloads.iter().map(|_| envelope.max_stamp.writer),
            )?),
            array(FixedSizeBinaryArray::try_from_iter(
                payloads.iter().map(|_| envelope.max_stamp.digest),
            )?),
            array(UInt32Array::from_iter_values(
                (0..payloads.len()).map(|ordinal| u32::try_from(ordinal).unwrap_or(u32::MAX)),
            )),
            array(StringArray::from_iter_values(
                payloads.iter().map(|payload| payload.modality.as_str()),
            )),
            array(StringArray::from_iter_values(
                payloads.iter().map(|payload| payload.role.as_str()),
            )),
            array(BinaryArray::from_iter_values(
                payloads.iter().map(|payload| payload.id_bloom.as_slice()),
            )),
            array(StringArray::from_iter_values(
                payloads.iter().map(|payload| payload.format.as_str()),
            )),
            array(StringArray::from_iter_values(
                payloads.iter().map(|payload| payload.path.as_str()),
            )),
            array(StringArray::from_iter_values(
                payloads.iter().map(|payload| payload.checksum.as_str()),
            )),
            array(UInt64Array::from_iter_values(
                payloads.iter().map(|payload| payload.rows),
            )),
            array(UInt64Array::from_iter_values(
                payloads.iter().map(|payload| payload.encoded_bytes),
            )),
        ],
    )?;
    write_batch(batch)
}

pub(crate) fn positioned_envelope_from_parquet(bytes: &[u8]) -> Result<DecodedPositionedEnvelope> {
    catch_parquet_decode_panic(|| positioned_envelope_from_parquet_inner(bytes))
}

fn positioned_envelope_from_parquet_inner(bytes: &[u8]) -> Result<DecodedPositionedEnvelope> {
    if bytes.len() > MAX_POSITIONED_ENVELOPE_BYTES {
        return Err(BorsukError::InvalidStorage(
            "positioned envelope exceeds its encoded byte bound".to_owned(),
        ));
    }
    let builder = ParquetRecordBatchReaderBuilder::try_new(Bytes::copy_from_slice(bytes))?;
    let row_count = builder.metadata().file_metadata().num_rows();
    if row_count <= 0
        || row_count
            > i64::try_from(crate::positioned_log::MAX_PAYLOADS_PER_TRANSACTION)
                .expect("positioned payload bound fits i64")
    {
        return Err(BorsukError::InvalidStorage(
            "positioned envelope row count is outside 1..=64".to_owned(),
        ));
    }
    if builder.schema().as_ref() != positioned_envelope_schema().as_ref() {
        return Err(BorsukError::InvalidStorage(
            "positioned envelope schema is not the exact V14 schema".to_owned(),
        ));
    }
    let mut batches = builder
        .with_batch_size(crate::positioned_log::MAX_PAYLOADS_PER_TRANSACTION)
        .build()?;
    let mut first = None::<PositionedEnvelopeRow>;
    let mut payloads = Vec::new();
    let mut ordinal = 0_u32;
    for batch in &mut batches {
        let batch = batch?;
        validate_positioned_envelope_batch(&batch)?;
        for row in 0..batch.num_rows() {
            let decoded = decode_positioned_envelope_row(&batch, row)?;
            if let Some(first) = first.as_ref() {
                if decoded.layout != POSITIONED_ENVELOPE_LAYOUT
                    || decoded.transaction_id != first.transaction_id
                    || decoded.transaction_digest != first.transaction_digest
                    || decoded.request_digest != first.request_digest
                    || decoded.position != first.position
                    || decoded.schema_fingerprint != first.schema_fingerprint
                    || decoded.min_stamp != first.min_stamp
                    || decoded.max_stamp != first.max_stamp
                {
                    return Err(BorsukError::InvalidStorage(
                        "positioned envelope repeated transaction columns disagree".to_owned(),
                    ));
                }
            } else if decoded.layout != POSITIONED_ENVELOPE_LAYOUT {
                return Err(BorsukError::InvalidStorage(
                    "positioned envelope layout marker is unsupported".to_owned(),
                ));
            }
            if decoded.ordinal != ordinal {
                return Err(BorsukError::InvalidStorage(
                    "positioned envelope payload ordinals are not contiguous".to_owned(),
                ));
            }
            ordinal = ordinal.checked_add(1).ok_or_else(|| {
                BorsukError::InvalidStorage(
                    "positioned envelope payload ordinal overflow".to_owned(),
                )
            })?;
            if payloads.len() == crate::positioned_log::MAX_PAYLOADS_PER_TRANSACTION {
                return Err(BorsukError::InvalidStorage(
                    "positioned envelope row count is outside 1..=64".to_owned(),
                ));
            }
            payloads.push(decoded.payload.clone());
            first.get_or_insert(decoded);
        }
    }
    let first = first.ok_or_else(|| {
        BorsukError::InvalidStorage("positioned envelope contains no rows".to_owned())
    })?;
    let envelope = PositionedMutationEnvelope {
        transaction_id: first.transaction_id,
        schema_fingerprint: first.schema_fingerprint,
        position: first.position,
        min_stamp: first.min_stamp,
        max_stamp: first.max_stamp,
        payloads,
    };
    envelope.validate()?;
    Ok(DecodedPositionedEnvelope {
        envelope,
        transaction_digest: first.transaction_digest,
        request_digest: first.request_digest,
    })
}

struct PositionedEnvelopeRow {
    layout: u16,
    transaction_id: String,
    transaction_digest: String,
    request_digest: String,
    position: crate::positioned_log::CommitSourcePosition,
    schema_fingerprint: String,
    min_stamp: PositionedMutationStamp,
    max_stamp: PositionedMutationStamp,
    ordinal: u32,
    payload: PositionedMutationPayloadRef,
}

fn decode_positioned_envelope_row(
    batch: &RecordBatch,
    row: usize,
) -> Result<PositionedEnvelopeRow> {
    let value = |name: &str| string_value_by_name(batch, row, name).map(str::to_owned);
    Ok(PositionedEnvelopeRow {
        layout: primitive_value_by_name::<UInt16Type>(batch, row, "layout")?,
        transaction_id: value("transaction_id")?,
        transaction_digest: value("transaction_digest")?,
        request_digest: value("request_digest")?,
        position: crate::positioned_log::CommitSourcePosition::new(
            primitive_value_by_name::<UInt64Type>(batch, row, "source_epoch")?,
            primitive_value_by_name::<UInt8Type>(batch, row, "shard")?,
            primitive_value_by_name::<UInt64Type>(batch, row, "sequence")?,
        )?,
        schema_fingerprint: value("schema_fingerprint")?,
        min_stamp: PositionedMutationStamp {
            hlc: primitive_value_by_name::<UInt64Type>(batch, row, "min_mutation_hlc")?,
            writer: fixed_size_binary_value(
                batch,
                column_index(batch, "min_mutation_writer")?,
                row,
                "min_mutation_writer",
            )?,
            digest: fixed_size_binary_value(
                batch,
                column_index(batch, "min_mutation_digest")?,
                row,
                "min_mutation_digest",
            )?,
        },
        max_stamp: PositionedMutationStamp {
            hlc: primitive_value_by_name::<UInt64Type>(batch, row, "max_mutation_hlc")?,
            writer: fixed_size_binary_value(
                batch,
                column_index(batch, "max_mutation_writer")?,
                row,
                "max_mutation_writer",
            )?,
            digest: fixed_size_binary_value(
                batch,
                column_index(batch, "max_mutation_digest")?,
                row,
                "max_mutation_digest",
            )?,
        },
        ordinal: primitive_value_by_name::<UInt32Type>(batch, row, "payload_ordinal")?,
        payload: PositionedMutationPayloadRef {
            modality: crate::positioned_log::PositionedMutationModality::parse(
                string_value_by_name(batch, row, "payload_modality")?,
            )?,
            role: value("payload_role")?,
            id_bloom: binary_value_by_name(batch, row, "payload_id_bloom")?.to_vec(),
            format: PositionedPayloadFormat::parse(string_value_by_name(
                batch,
                row,
                "payload_format",
            )?)?,
            path: value("payload_path")?,
            checksum: value("payload_checksum")?,
            rows: primitive_value_by_name::<UInt64Type>(batch, row, "payload_rows")?,
            encoded_bytes: primitive_value_by_name::<UInt64Type>(batch, row, "payload_bytes")?,
        },
    })
}

fn write_batch(batch: RecordBatch) -> Result<Vec<u8>> {
    write_batch_with_row_groups(batch, None)
}

fn write_batches_as_row_groups(schema: Arc<Schema>, batches: &[RecordBatch]) -> Result<Vec<u8>> {
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();
    let mut bytes = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut bytes, schema, Some(props))?;
    for batch in batches {
        writer.write(batch)?;
        // A lexical block is the exact pruning/read unit. Flushing after every
        // batch makes its row-group ordinal a stable range-read address.
        writer.flush()?;
    }
    writer.close()?;
    Ok(bytes)
}

/// Segment rows per Parquet row group. Small groups let a row-selective rerank
/// read fetch only the vector column chunks of the groups holding the chosen
/// rows, instead of the whole column — the object-store byte win. Traded against
/// a slightly larger footer.
pub(crate) const SEGMENT_ROW_GROUP_ROWS: usize = 32;

/// Positioned WAL payloads are validated and replayed as complete transaction
/// objects; they are not the row-selective immutable segment page format. Keep
/// their Parquet footer and encode/decode cost bounded instead of inheriting
/// the 32-row rerank groups used by segments.
const POSITIONED_WAL_ROW_GROUP_ROWS: usize = 4_096;

/// Parquet hard-caps a file at 32767 row groups. A bulk-load L0 segment can hold
/// the whole corpus (millions of rows) before compaction splits it into cells;
/// at [`SEGMENT_ROW_GROUP_ROWS`] that overflows the cap (e.g. 1.18M rows / 32 =
/// 36 875 groups > 32767). Stay a safe margin under the cap and grow the group
/// size only when a file is large enough to need it — normal ≤`segment_max`
/// segments keep the 32-row groups and their row-selective-rerank win.
const MAX_PARQUET_ROW_GROUPS: usize = 30_000;

fn effective_row_group_rows(total_rows: usize, requested_rows: usize) -> usize {
    requested_rows
        .max(total_rows.div_ceil(MAX_PARQUET_ROW_GROUPS))
        .max(1)
}

fn write_batch_with_row_groups(
    batch: RecordBatch,
    max_row_group_rows: Option<usize>,
) -> Result<Vec<u8>> {
    let mut properties = WriterProperties::builder().set_compression(Compression::SNAPPY);
    if let Some(rows) = max_row_group_rows {
        // Never let the group *count* exceed Parquet's 32767 limit: for a file
        // large enough that `rows`-sized groups would overflow, widen the groups
        // just enough to stay under the cap.
        let effective = effective_row_group_rows(batch.num_rows(), rows);
        properties = properties.set_max_row_group_row_count(Some(effective));
    }
    let props = properties.build();
    let mut bytes = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut bytes, batch.schema(), Some(props))?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(bytes)
}

fn read_batches(bytes: &[u8]) -> Result<Vec<RecordBatch>> {
    read_batches_projected(bytes, false, None)
}

/// Read a segment's Parquet batches, optionally projecting out the `vector`
/// column (so it is never decompressed) and/or restricting to a set of rows.
fn read_batches_projected(
    bytes: &[u8],
    project_out_vector: bool,
    row_selection: Option<RowSelection>,
) -> Result<Vec<RecordBatch>> {
    catch_parquet_decode_panic(|| {
        read_batches_projected_inner(bytes, project_out_vector, row_selection)
    })
}

fn read_batches_projected_inner(
    bytes: &[u8],
    project_out_vector: bool,
    row_selection: Option<RowSelection>,
) -> Result<Vec<RecordBatch>> {
    let mut builder = ParquetRecordBatchReaderBuilder::try_new(Bytes::copy_from_slice(bytes))?;
    let schema_metadata = builder.schema().metadata().clone();
    if project_out_vector {
        let vector_root = vector_root_index(builder.parquet_schema());
        let roots = (0..builder.parquet_schema().root_schema().get_fields().len())
            .filter(|index| Some(*index) != vector_root);
        let mask = ProjectionMask::roots(builder.parquet_schema(), roots);
        builder = builder.with_projection(mask);
    }
    if let Some(selection) = row_selection {
        builder = builder.with_row_selection(selection);
    }
    let batches = builder
        .build()?
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(BorsukError::from)?;
    restore_projected_schema_metadata(batches, &schema_metadata)
}

fn read_batches_projected_columns(
    bytes: &[u8],
    column_names: &[&str],
    row_selection: Option<RowSelection>,
) -> Result<Vec<RecordBatch>> {
    catch_parquet_decode_panic(|| {
        read_batches_projected_columns_inner(bytes, column_names, row_selection)
    })
}

fn read_batches_projected_columns_inner(
    bytes: &[u8],
    column_names: &[&str],
    row_selection: Option<RowSelection>,
) -> Result<Vec<RecordBatch>> {
    let mut builder = ParquetRecordBatchReaderBuilder::try_new(Bytes::copy_from_slice(bytes))?;
    let schema_metadata = builder.schema().metadata().clone();
    let roots = builder
        .parquet_schema()
        .root_schema()
        .get_fields()
        .iter()
        .enumerate()
        .filter_map(|(index, field)| column_names.contains(&field.name()).then_some(index));
    let mask = ProjectionMask::roots(builder.parquet_schema(), roots);
    builder = builder.with_projection(mask);
    if let Some(selection) = row_selection {
        builder = builder.with_row_selection(selection);
    }
    let batches = builder
        .build()?
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(BorsukError::from)?;
    restore_projected_schema_metadata(batches, &schema_metadata)
}

fn catch_parquet_decode_panic<T>(decode: impl FnOnce() -> Result<T>) -> Result<T> {
    catch_unwind(AssertUnwindSafe(decode)).map_err(|_| {
        BorsukError::InvalidStorage(
            "Parquet decoder rejected corrupt embedded Arrow metadata".to_string(),
        )
    })?
}

fn restore_projected_schema_metadata(
    batches: Vec<RecordBatch>,
    metadata: &HashMap<String, String>,
) -> Result<Vec<RecordBatch>> {
    if metadata.is_empty() {
        return Ok(batches);
    }
    batches
        .into_iter()
        .map(|batch| {
            let schema = Arc::new(Schema::new_with_metadata(
                batch.schema().fields().clone(),
                metadata.clone(),
            ));
            RecordBatch::try_new(schema, batch.columns().to_vec()).map_err(Into::into)
        })
        .collect()
}

fn vector_root_index(schema: &parquet::schema::types::SchemaDescriptor) -> Option<usize> {
    schema
        .root_schema()
        .get_fields()
        .iter()
        .position(|field| field.name() == "vector")
}

#[allow(dead_code)]
pub(crate) fn row_selection_for_rows(sorted_rows: &[usize], total_rows: usize) -> RowSelection {
    let mut selectors = Vec::new();
    let mut cursor = 0_usize;
    for &row in sorted_rows {
        if row > cursor {
            selectors.push(RowSelector::skip(row - cursor));
        }
        selectors.push(RowSelector::select(1));
        cursor = row + 1;
    }
    if total_rows > cursor {
        selectors.push(RowSelector::skip(total_rows - cursor));
    }
    RowSelection::from(selectors)
}

fn first_batch(bytes: &[u8], name: &str) -> Result<RecordBatch> {
    read_batches(bytes)?
        .into_iter()
        .next()
        .ok_or_else(|| BorsukError::InvalidStorage(format!("{name} table must contain one row")))
}

fn array(value: impl Array + 'static) -> ArrayRef {
    Arc::new(value) as ArrayRef
}

fn primitive_value<T>(
    batch: &RecordBatch,
    column: usize,
    row: usize,
    name: &str,
) -> Result<T::Native>
where
    T: arrow_array::ArrowPrimitiveType,
{
    batch
        .column(column)
        .as_any()
        .downcast_ref::<arrow_array::PrimitiveArray<T>>()
        .map(|array| array.value(row))
        .ok_or_else(|| BorsukError::InvalidStorage(format!("column `{name}` has wrong type")))
}

fn required_primitive_value<T>(
    batch: &RecordBatch,
    column: usize,
    row: usize,
    name: &str,
) -> Result<T::Native>
where
    T: arrow_array::ArrowPrimitiveType,
{
    let array = batch
        .column(column)
        .as_any()
        .downcast_ref::<arrow_array::PrimitiveArray<T>>()
        .ok_or_else(|| BorsukError::InvalidStorage(format!("column `{name}` has wrong type")))?;
    if array.is_null(row) {
        return Err(BorsukError::InvalidStorage(format!(
            "column `{name}` contains a null value"
        )));
    }
    Ok(array.value(row))
}

fn column_index(batch: &RecordBatch, name: &str) -> Result<usize> {
    batch
        .schema()
        .index_of(name)
        .map_err(|_| BorsukError::InvalidStorage(format!("missing column `{name}`")))
}

fn primitive_value_by_name<T>(batch: &RecordBatch, row: usize, name: &str) -> Result<T::Native>
where
    T: arrow_array::ArrowPrimitiveType,
{
    primitive_value::<T>(batch, column_index(batch, name)?, row, name)
}

fn primitive_optional_value<T>(
    batch: &RecordBatch,
    column: usize,
    row: usize,
    name: &str,
) -> Result<Option<T::Native>>
where
    T: arrow_array::ArrowPrimitiveType,
{
    let array = batch
        .column(column)
        .as_any()
        .downcast_ref::<arrow_array::PrimitiveArray<T>>()
        .ok_or_else(|| BorsukError::InvalidStorage(format!("column `{name}` has wrong type")))?;
    if array.is_null(row) {
        Ok(None)
    } else {
        Ok(Some(array.value(row)))
    }
}

fn primitive_optional_value_by_name<T>(
    batch: &RecordBatch,
    row: usize,
    name: &str,
) -> Result<Option<T::Native>>
where
    T: arrow_array::ArrowPrimitiveType,
{
    primitive_optional_value::<T>(batch, column_index(batch, name)?, row, name)
}

fn boolean_value(batch: &RecordBatch, column: usize, row: usize, name: &str) -> Result<bool> {
    batch
        .column(column)
        .as_any()
        .downcast_ref::<BooleanArray>()
        .map(|array| array.value(row))
        .ok_or_else(|| BorsukError::InvalidStorage(format!("column `{name}` has wrong type")))
}

fn string_value<'a>(
    batch: &'a RecordBatch,
    column: usize,
    row: usize,
    name: &str,
) -> Result<&'a str> {
    batch
        .column(column)
        .as_any()
        .downcast_ref::<StringArray>()
        .map(|array| array.value(row))
        .ok_or_else(|| BorsukError::InvalidStorage(format!("column `{name}` has wrong type")))
}

fn string_value_by_name<'a>(batch: &'a RecordBatch, row: usize, name: &str) -> Result<&'a str> {
    string_value(batch, column_index(batch, name)?, row, name)
}

fn record_id_value(batch: &RecordBatch, column: usize, row: usize, name: &str) -> Result<RecordId> {
    if let Some(array) = batch.column(column).as_any().downcast_ref::<BinaryArray>() {
        return Ok(RecordId::from_bytes(array.value(row).to_vec()));
    }

    if let Some(array) = batch.column(column).as_any().downcast_ref::<StringArray>() {
        return Ok(RecordId::from(array.value(row)));
    }

    Err(BorsukError::InvalidStorage(format!(
        "column `{name}` has wrong type"
    )))
}

fn binary_value<'a>(
    batch: &'a RecordBatch,
    column: usize,
    row: usize,
    name: &str,
) -> Result<&'a [u8]> {
    batch
        .column(column)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .map(|array| array.value(row))
        .ok_or_else(|| BorsukError::InvalidStorage(format!("column `{name}` has wrong type")))
}

fn binary_value_by_name<'a>(batch: &'a RecordBatch, row: usize, name: &str) -> Result<&'a [u8]> {
    binary_value(batch, column_index(batch, name)?, row, name)
}

fn fixed_f32_value(batch: &RecordBatch, column: usize, row: usize, name: &str) -> Result<Vec<f32>> {
    let list = batch
        .column(column)
        .as_any()
        .downcast_ref::<FixedSizeListArray>()
        .ok_or_else(|| BorsukError::InvalidStorage(format!("column `{name}` has wrong type")))?;
    let values = list.value(row);
    let values = values
        .as_any()
        .downcast_ref::<Float32Array>()
        .ok_or_else(|| BorsukError::InvalidStorage(format!("column `{name}` has wrong type")))?;
    Ok((0..values.len()).map(|index| values.value(index)).collect())
}

fn fixed_f32_value_by_name(batch: &RecordBatch, row: usize, name: &str) -> Result<Vec<f32>> {
    fixed_f32_value(batch, column_index(batch, name)?, row, name)
}

fn fixed_u8_value(batch: &RecordBatch, column: usize, row: usize, name: &str) -> Result<Vec<u8>> {
    let list = batch
        .column(column)
        .as_any()
        .downcast_ref::<FixedSizeListArray>()
        .ok_or_else(|| BorsukError::InvalidStorage(format!("column `{name}` has wrong type")))?;
    let values = list.value(row);
    let values = values
        .as_any()
        .downcast_ref::<UInt8Array>()
        .ok_or_else(|| BorsukError::InvalidStorage(format!("column `{name}` has wrong type")))?;
    Ok((0..values.len()).map(|index| values.value(index)).collect())
}

fn primitive_list_optional_value<T>(
    batch: &RecordBatch,
    column: usize,
    row: usize,
    name: &str,
) -> Result<Option<Vec<T::Native>>>
where
    T: arrow_array::ArrowPrimitiveType,
{
    let list = batch
        .column(column)
        .as_any()
        .downcast_ref::<ListArray>()
        .ok_or_else(|| BorsukError::InvalidStorage(format!("column `{name}` has wrong type")))?;
    if list.is_null(row) {
        return Ok(None);
    }

    let values = list.value(row);
    let values = values
        .as_any()
        .downcast_ref::<arrow_array::PrimitiveArray<T>>()
        .ok_or_else(|| BorsukError::InvalidStorage(format!("column `{name}` has wrong type")))?;
    let mut out = Vec::with_capacity(values.len());
    for index in 0..values.len() {
        if values.is_null(index) {
            return Err(BorsukError::InvalidStorage(format!(
                "column `{name}` contains a null sparse value"
            )));
        }
        out.push(values.value(index));
    }
    Ok(Some(out))
}

#[allow(clippy::too_many_arguments)]
fn decode_segment_vector(
    batch: &RecordBatch,
    row: usize,
    id: &RecordId,
    dimensions: usize,
    vector_column: Option<usize>,
    sparse_indices_column: Option<usize>,
    sparse_values_column: Option<usize>,
    vector_element_type: VectorElementType,
) -> Result<(Vec<f32>, StorageEncoding)> {
    if vector_column.is_none() {
        if let (Some(indices_column), Some(values_column)) =
            (sparse_indices_column, sparse_values_column)
        {
            let sparse_present = !batch.column(indices_column).is_null(row)
                || !batch.column(values_column).is_null(row);
            if sparse_present {
                let indices = primitive_list_optional_value::<UInt32Type>(
                    batch,
                    indices_column,
                    row,
                    "sparse_indices",
                )?
                .unwrap_or_default();
                let values = primitive_list_optional_value::<Float32Type>(
                    batch,
                    values_column,
                    row,
                    "sparse_values",
                )?
                .unwrap_or_default();
                validate_sparse_encoding(id, dimensions, indices, values)?;
                return Ok((Vec::new(), StorageEncoding::Sparse));
            }
        }
        return Ok((Vec::new(), StorageEncoding::Dense));
    }

    let dense = if batch.column(vector_column.unwrap()).is_null(row) {
        None
    } else {
        Some(crate::arrow_vector_sidecar::decode_vector(
            batch.column(vector_column.unwrap()).as_ref(),
            row,
            dimensions,
            vector_element_type,
        )?)
    };
    let sparse = match (sparse_indices_column, sparse_values_column) {
        (Some(indices_column), Some(values_column)) => {
            let indices_present = !batch.column(indices_column).is_null(row);
            let values_present = !batch.column(values_column).is_null(row);
            if indices_present != values_present {
                return Err(BorsukError::InvalidStorage(format!(
                    "segment record `{id}` must store both sparse_indices and sparse_values or neither"
                )));
            }
            if indices_present {
                Some((
                    primitive_list_optional_value::<UInt32Type>(
                        batch,
                        indices_column,
                        row,
                        "sparse_indices",
                    )?
                    .unwrap_or_default(),
                    primitive_list_optional_value::<Float32Type>(
                        batch,
                        values_column,
                        row,
                        "sparse_values",
                    )?
                    .unwrap_or_default(),
                ))
            } else {
                None
            }
        }
        (None, None) => None,
        _ => {
            return Err(BorsukError::InvalidStorage(
                "segment table must contain both sparse_indices and sparse_values columns"
                    .to_string(),
            ));
        }
    };

    match (dense, sparse) {
        (Some(vector), None) => {
            validate_segment_record_dimensions(id, dimensions, vector.len())?;
            validate_segment_record_vector_values(id, &vector)?;
            Ok((vector, StorageEncoding::Dense))
        }
        (None, Some((indices, values))) => {
            let vector = validate_sparse_encoding(id, dimensions, indices, values)?;
            Ok((vector, StorageEncoding::Sparse))
        }
        (Some(_), Some(_)) => Err(BorsukError::InvalidStorage(format!(
            "segment record `{id}` stores both dense and sparse vector encodings"
        ))),
        (None, None) => Err(BorsukError::InvalidStorage(format!(
            "segment record `{id}` stores neither dense nor sparse vector encoding"
        ))),
    }
}

fn validate_sparse_encoding(
    id: &RecordId,
    dimensions: usize,
    indices: Vec<u32>,
    values: Vec<f32>,
) -> Result<Vec<f32>> {
    let sparse = crate::SparseVector::new(indices, values)?;
    let mut vector = vec![0.0; dimensions];
    for (&index, &value) in sparse.indices().iter().zip(sparse.values()) {
        let position = usize::try_from(index).map_err(|_| {
            BorsukError::InvalidStorage(format!(
                "segment record `{id}` sparse index {index} does not fit usize"
            ))
        })?;
        if position >= dimensions {
            return Err(BorsukError::InvalidStorage(format!(
                "segment record `{id}` sparse index {index} is outside {dimensions} dimensions"
            )));
        }
        vector[position] = value;
    }
    validate_segment_record_vector_values(id, &vector)?;
    Ok(vector)
}

fn sparse_parts_from_dense(id: &RecordId, vector: &[f32]) -> Result<(Vec<u32>, Vec<f32>)> {
    let mut indices = Vec::new();
    let mut values = Vec::new();
    for (position, value) in vector.iter().copied().enumerate() {
        if value == 0.0 {
            continue;
        }
        let index = u32::try_from(position).map_err(|_| {
            BorsukError::InvalidRecordInput(format!(
                "record `{id}` sparse storage requires vector dimensions to fit u32"
            ))
        })?;
        indices.push(index);
        values.push(value);
    }
    Ok((indices, values))
}

fn usize_from_u64(value: u64) -> Result<usize> {
    usize::try_from(value).map_err(|_| {
        BorsukError::InvalidStorage(format!("stored value {value} does not fit usize"))
    })
}

fn datetime_from_millis(value: i64) -> Result<DateTime<Utc>> {
    DateTime::<Utc>::from_timestamp_millis(value).ok_or_else(|| {
        BorsukError::InvalidStorage(format!("stored timestamp {value} is out of range"))
    })
}

fn validate_segment_record_vector_values(record_id: &RecordId, vector: &[f32]) -> Result<()> {
    if let Some((coordinate_index, value)) = vector
        .iter()
        .copied()
        .enumerate()
        .find(|(_, value)| !value.is_finite())
    {
        return Err(BorsukError::InvalidStorage(format!(
            "segment record vectors must contain only finite f32 values; record `{record_id}` coordinate {coordinate_index} was {value}"
        )));
    }

    Ok(())
}

fn validate_segment_record_text_terms(record: &VectorRecord) -> Result<()> {
    validate_text_terms(&record.id, &record.text_term_ids, &record.text_term_freqs)
}

fn validate_text_terms(record_id: &RecordId, term_ids: &[u32], term_freqs: &[u32]) -> Result<()> {
    if term_ids.is_empty() && term_freqs.is_empty() {
        return Ok(());
    }
    if term_ids.len() != term_freqs.len() {
        return Err(BorsukError::InvalidStorage(format!(
            "segment record `{record_id}` text_term_ids length {} must match text_term_freqs length {}",
            term_ids.len(),
            term_freqs.len()
        )));
    }
    if let Some(position) = term_freqs.iter().position(|freq| *freq == 0) {
        return Err(BorsukError::InvalidStorage(format!(
            "segment record `{record_id}` text_term_freqs value at position {position} must be greater than zero"
        )));
    }
    if let Some(position) = term_ids
        .windows(2)
        .position(|window| window[0] >= window[1])
    {
        return Err(BorsukError::InvalidStorage(format!(
            "segment record `{record_id}` text_term_ids must be strictly increasing; positions {position} and {} are out of order",
            position + 1
        )));
    }
    Ok(())
}

fn validate_segment_record_ids<R: VectorRecordView>(records: &[R]) -> Result<()> {
    let mut ids = HashSet::with_capacity(records.len());
    for record in records {
        let record = record.as_vector_record();
        if record.id.is_empty() {
            return Err(BorsukError::InvalidStorage(
                "record ids must not be empty".to_string(),
            ));
        }
        if !ids.insert(record.id.as_bytes()) {
            return Err(BorsukError::InvalidStorage(format!(
                "duplicate record id `{}` in segment table",
                record.id
            )));
        }
    }

    Ok(())
}

fn validate_segment_centroid_dimensions(
    segment_id: &str,
    expected: usize,
    actual: usize,
) -> Result<()> {
    validate_stored_vector_dimensions("segment centroid", segment_id, expected, actual)
}

fn validate_segment_centroid_values(segment_id: &str, centroid: &[f32]) -> Result<()> {
    if let Some((coordinate_index, value)) = non_finite_coordinate(centroid) {
        return Err(BorsukError::InvalidStorage(format!(
            "segment centroids must contain only finite f32 values; segment `{segment_id}` coordinate {coordinate_index} was {value}"
        )));
    }

    Ok(())
}

fn validate_segment_radius(segment_id: &str, radius: f32) -> Result<()> {
    if !radius.is_finite() {
        return Err(BorsukError::InvalidStorage(format!(
            "segment radii must contain only finite f32 values; segment `{segment_id}` was {radius}"
        )));
    }

    Ok(())
}

fn validate_segment_record_dimensions(
    record_id: &RecordId,
    expected: usize,
    actual: usize,
) -> Result<()> {
    validate_stored_vector_dimensions(
        "segment record vector",
        &record_id.to_string(),
        expected,
        actual,
    )
}

fn validate_segment_routing_code(record_id: &RecordId, routing_code: f32) -> Result<()> {
    if !routing_code.is_finite() {
        return Err(BorsukError::InvalidStorage(format!(
            "segment routing codes must contain only finite f32 values; record `{record_id}` was {routing_code}"
        )));
    }

    Ok(())
}

fn validate_segment_routing_code_count(
    segment_id: &str,
    record_count: usize,
    routing_code_count: usize,
) -> Result<()> {
    if routing_code_count != record_count {
        return Err(BorsukError::InvalidStorage(format!(
            "segment `{segment_id}` routing code count {routing_code_count} must match record count {record_count}"
        )));
    }

    Ok(())
}

fn validate_segment_pq_code_count(
    segment_id: &str,
    record_count: usize,
    pq_code_count: usize,
) -> Result<()> {
    if pq_code_count != record_count {
        return Err(BorsukError::InvalidStorage(format!(
            "segment `{segment_id}` pq code count {pq_code_count} must match record count {record_count}"
        )));
    }

    Ok(())
}

fn validate_segment_pq_code_dimensions(
    record_id: &RecordId,
    expected: usize,
    actual: usize,
) -> Result<()> {
    if actual != expected {
        return Err(BorsukError::InvalidStorage(format!(
            "segment PQ codes must match vector dimensions; record `{record_id}` had {actual}, expected {expected}"
        )));
    }

    Ok(())
}

/// Like [`validate_segment_pq_code_dimensions`] but requires only that the code is
/// AT LEAST `min_expected` wide. TurboQuant's stage-2 QJL residual appends a fixed
/// self-describing tail past the scalar-code prefix, so a code may exceed the
/// bounds width; the scalar prefix (needed to score) must still be present.
fn validate_segment_pq_code_min_dimensions(
    record_id: &RecordId,
    min_expected: usize,
    actual: usize,
) -> Result<()> {
    if actual < min_expected {
        return Err(BorsukError::InvalidStorage(format!(
            "segment PQ codes must cover at least the coarse bounds width; record `{record_id}` had {actual}, expected >= {min_expected}"
        )));
    }

    Ok(())
}

fn validate_graph_edge_distance(
    source_record_index: usize,
    neighbor_record_index: usize,
    distance: f32,
) -> Result<()> {
    if !distance.is_finite() {
        return Err(BorsukError::InvalidStorage(format!(
            "segment graph distances must contain only finite f32 values; edge {source_record_index} -> {neighbor_record_index} was {distance}"
        )));
    }

    Ok(())
}

fn validate_graph_record_index(
    segment_id: &str,
    role: &str,
    record_index: usize,
    record_count: usize,
) -> Result<()> {
    if record_index < record_count {
        return Ok(());
    }

    Err(BorsukError::InvalidStorage(format!(
        "graph table segment `{segment_id}` {role} record index {record_index} is outside record count {record_count}"
    )))
}

fn graph_record_index_from_id(
    segment_id: &str,
    role: &str,
    record_id: &str,
    record_index_by_id: &HashMap<&[u8], usize>,
) -> Result<usize> {
    record_index_by_id
        .get(record_id.as_bytes())
        .copied()
        .ok_or_else(|| {
            BorsukError::InvalidStorage(format!(
                "graph edge references missing segment record in legacy graph table segment `{segment_id}`: {role} record id `{record_id}`"
            ))
        })
}

fn validate_stored_vector_dimensions(
    field: &str,
    id: &str,
    expected: usize,
    actual: usize,
) -> Result<()> {
    if actual != expected {
        return Err(BorsukError::InvalidStorage(format!(
            "{field} `{id}` has {actual} dimensions, expected {expected}"
        )));
    }

    Ok(())
}

fn non_finite_coordinate(vector: &[f32]) -> Option<(usize, f32)> {
    vector
        .iter()
        .copied()
        .enumerate()
        .find(|(_, value)| !value.is_finite())
}

#[derive(Debug, PartialEq)]
struct SegmentMetadata {
    id: String,
    level: u8,
    metric: VectorMetric,
    dimensions: usize,
    centroid: Vec<f32>,
    radius: f32,
    created_at: DateTime<Utc>,
}

#[derive(Debug, PartialEq)]
struct LeanSegmentHeader {
    format_version: u16,
    metadata: SegmentMetadata,
    pq_bounds: (Vec<f32>, Vec<f32>),
}

#[derive(Debug, PartialEq)]
struct GraphMetadata {
    segment_id: String,
    level: u8,
    created_at: DateTime<Utc>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn wal_records_to_table(
        records: &[VectorRecord],
        dimensions: usize,
        element_type: VectorElementType,
        format: crate::PhysicalFormat,
    ) -> Result<Vec<u8>> {
        let records = records.iter().collect::<Vec<_>>();
        let batch = wal_records_to_batch(&records, dimensions, element_type)?;
        match format {
            crate::PhysicalFormat::Parquet => {
                write_batch_with_row_groups(batch, Some(SEGMENT_ROW_GROUP_ROWS))
            }
            other => Err(BorsukError::InvalidStorage(format!(
                "WAL records cannot use physical format `{other}`"
            ))),
        }
    }

    #[test]
    fn positioned_wal_encoder_borrows_records_without_cloning_payloads() {
        let mut record = valid_segment().records.remove(0);
        record
            .extra_vectors
            .insert("dense".to_string(), vec![0.25, -0.75]);
        record.extra_sparse.insert(
            "sparse".to_string(),
            crate::SparseVector::new(vec![3, 17], vec![1.5, 0.5]).unwrap(),
        );
        record.extra_multi_vectors.insert(
            "tokens".to_string(),
            crate::LateInteractionVector::new(
                vec![vec![0.25, 0.5], vec![-0.75, 1.0]],
                VectorElementType::Float16,
            )
            .unwrap(),
        );
        record.text_term_ids = vec![7, 11];
        record.text_term_freqs = vec![2, 1];
        let records = vec![(record, 1, 0)];

        let (encoded, clone_count) = crate::record::count_vector_record_clones(|| {
            positioned_wal_records_to_table(
                &records,
                2,
                VectorElementType::Float32,
                crate::PhysicalFormat::Parquet,
            )
        });

        encoded.unwrap();
        assert_eq!(
            clone_count, 0,
            "positioned WAL encoding must borrow staged record payloads"
        );
    }

    #[test]
    fn positioned_wal_uses_dedicated_coarse_row_groups() {
        let template = valid_segment().records.remove(0);
        let records = (0..4_097)
            .map(|ordinal| {
                let mut record = template.clone();
                record.id = format!("record-{ordinal}").into();
                (record, 1, 0)
            })
            .collect::<Vec<_>>();

        let bytes = positioned_wal_records_to_table(
            &records,
            2,
            VectorElementType::Float32,
            crate::PhysicalFormat::Parquet,
        )
        .unwrap();
        let reader = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(bytes)).unwrap();

        const { assert!(POSITIONED_WAL_ROW_GROUP_ROWS > SEGMENT_ROW_GROUP_ROWS) };
        assert_eq!(reader.metadata().num_row_groups(), 2);
        assert_eq!(reader.metadata().row_group(0).num_rows(), 4_096);
    }

    const VALID_SEGMENT_CHECKSUM: &str =
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const VALID_GRAPH_CHECKSUM: &str =
        "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

    fn positioned_envelope_fixture() -> PositionedMutationEnvelope {
        let checksum = "b".repeat(64);
        PositionedMutationEnvelope {
            transaction_id: "decoder-fixture".to_owned(),
            schema_fingerprint: "a".repeat(64),
            position: crate::positioned_log::CommitSourcePosition::new(7, 3, 1).unwrap(),
            min_stamp: PositionedMutationStamp {
                hlc: 11,
                writer: [1; 16],
                digest: [2; 32],
            },
            max_stamp: PositionedMutationStamp {
                hlc: 12,
                writer: [3; 16],
                digest: [4; 32],
            },
            payloads: vec![PositionedMutationPayloadRef {
                modality: crate::positioned_log::PositionedMutationModality::PrimaryDense,
                role: "primary".to_owned(),
                id_bloom: Vec::new(),
                format: PositionedPayloadFormat::ArrowIpc,
                path: format!("positioned-log/payloads/arrow-ipc/bb/{checksum}.arrow"),
                checksum,
                rows: 1,
                encoded_bytes: 128,
            }],
        }
    }

    fn positioned_envelope_batch() -> RecordBatch {
        let bytes = positioned_envelope_to_parquet(
            &positioned_envelope_fixture(),
            &"c".repeat(64),
            &"d".repeat(64),
        )
        .unwrap();
        read_batches(&bytes).unwrap().into_iter().next().unwrap()
    }

    #[test]
    fn positioned_envelope_v15_round_trips_authenticated_id_bloom() {
        let mut expected = positioned_envelope_fixture();
        expected.payloads[0].id_bloom = vec![0x5a; 128];
        let bytes =
            positioned_envelope_to_parquet(&expected, &"c".repeat(64), &"d".repeat(64)).unwrap();
        let decoded = positioned_envelope_from_parquet(&bytes).unwrap();
        assert_eq!(decoded.envelope, expected);
    }

    #[test]
    fn positioned_envelope_decoder_rejects_v14_layout_marker() {
        let batch = positioned_envelope_batch();
        let mut columns = batch.columns().to_vec();
        columns[batch.schema().index_of("layout").unwrap()] =
            array(UInt16Array::from_iter_values([14]));
        let old = write_batch(RecordBatch::try_new(batch.schema(), columns).unwrap()).unwrap();

        let error = positioned_envelope_from_parquet(&old)
            .err()
            .expect("v14 envelope must be rejected")
            .to_string();
        assert!(error.contains("layout marker is unsupported"), "{error}");
    }

    #[test]
    fn positioned_envelope_decoder_rejects_extra_and_nullable_schema_fields() {
        assert!(positioned_envelope_from_parquet(b"PAR1corrupt").is_err());
        let batch = positioned_envelope_batch();
        let mut fields = batch.schema().fields().to_vec();
        fields.push(Arc::new(Field::new("extra", DataType::UInt8, false)));
        let mut columns = batch.columns().to_vec();
        columns.push(array(UInt8Array::from_iter_values([1])));
        let extra = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap();
        assert!(positioned_envelope_from_parquet(&write_batch(extra).unwrap()).is_err());

        let batch = positioned_envelope_batch();
        let mut fields = batch.schema().fields().to_vec();
        fields[0] = Arc::new(Field::new("layout", DataType::UInt16, true));
        let nullable =
            RecordBatch::try_new(Arc::new(Schema::new(fields)), batch.columns().to_vec()).unwrap();
        assert!(positioned_envelope_from_parquet(&write_batch(nullable).unwrap()).is_err());

        let batch = positioned_envelope_batch();
        let mut fields = batch.schema().fields().to_vec();
        fields[9] = Arc::new(Field::new(
            "min_mutation_writer",
            DataType::FixedSizeList(Arc::new(Field::new_list_field(DataType::UInt8, true)), 16),
            false,
        ));
        let mut columns = batch.columns().to_vec();
        columns[9] = array(FixedSizeListArray::from_iter_primitive::<UInt8Type, _, _>(
            [Some(vec![Some(1_u8); 16])],
            16,
        ));
        let fixed_list = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap();
        assert!(positioned_envelope_from_parquet(&write_batch(fixed_list).unwrap()).is_err());
    }

    #[test]
    fn positioned_envelope_decoder_rejects_nulls_in_exact_typed_columns() {
        let batch = positioned_envelope_batch();
        let mut columns = batch.columns().to_vec();
        columns[1] = array(StringArray::from(vec![None::<&str>]));
        assert!(
            validate_positioned_envelope_schema_and_columns(batch.schema().as_ref(), &columns)
                .is_err()
        );
    }

    fn positioned_payload_with_null_hlc_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("mutation_hlc", DataType::UInt64, true),
            Field::new("mutation_writer", DataType::FixedSizeBinary(16), false),
            Field::new("mutation_digest", DataType::FixedSizeBinary(32), false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                array(UInt64Array::from(vec![None])),
                array(FixedSizeBinaryArray::try_from_iter([[7_u8; 16]].into_iter()).unwrap()),
                array(FixedSizeBinaryArray::try_from_iter([[9_u8; 32]].into_iter()).unwrap()),
            ],
        )
        .unwrap()
    }

    #[test]
    fn positioned_arrow_payload_decoder_rejects_null_mutation_hlc() {
        let batch = positioned_payload_with_null_hlc_batch();
        let mut bytes = Vec::new();
        let mut writer =
            arrow_ipc::writer::StreamWriter::try_new(&mut bytes, batch.schema().as_ref()).unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();
        drop(writer);
        assert!(positioned_payload_metadata(&bytes, PositionedPayloadFormat::ArrowIpc, 1).is_err());
    }

    #[test]
    fn positioned_parquet_payload_decoder_rejects_null_mutation_hlc() {
        let bytes = write_batch(positioned_payload_with_null_hlc_batch()).unwrap();
        assert!(positioned_payload_metadata(&bytes, PositionedPayloadFormat::Parquet, 1).is_err());
    }

    #[test]
    fn positioned_envelope_rejects_compressed_parquet_above_sixty_four_rows_from_metadata() {
        let one = positioned_envelope_batch();
        let rows = crate::positioned_log::MAX_PAYLOADS_PER_TRANSACTION + 1;
        let columns = one
            .columns()
            .iter()
            .enumerate()
            .map(|(index, column)| match column.data_type() {
                DataType::UInt8 => {
                    let values = column.as_any().downcast_ref::<UInt8Array>().unwrap();
                    array(UInt8Array::from_iter_values(
                        (0..rows).map(|_| values.value(0)),
                    ))
                }
                DataType::UInt16 => {
                    let values = column.as_any().downcast_ref::<UInt16Array>().unwrap();
                    array(UInt16Array::from_iter_values(
                        (0..rows).map(|_| values.value(0)),
                    ))
                }
                DataType::UInt32 => {
                    let values = column.as_any().downcast_ref::<UInt32Array>().unwrap();
                    if index == 14 {
                        array(UInt32Array::from_iter_values(
                            (0..rows).map(|ordinal| u32::try_from(ordinal).unwrap()),
                        ))
                    } else {
                        array(UInt32Array::from_iter_values(
                            (0..rows).map(|_| values.value(0)),
                        ))
                    }
                }
                DataType::UInt64 => {
                    let values = column.as_any().downcast_ref::<UInt64Array>().unwrap();
                    array(UInt64Array::from_iter_values(
                        (0..rows).map(|_| values.value(0)),
                    ))
                }
                DataType::Utf8 => {
                    let values = column.as_any().downcast_ref::<StringArray>().unwrap();
                    array(StringArray::from_iter_values(
                        (0..rows).map(|_| values.value(0)),
                    ))
                }
                DataType::Binary => {
                    let values = column.as_any().downcast_ref::<BinaryArray>().unwrap();
                    array(BinaryArray::from_iter_values(
                        (0..rows).map(|_| values.value(0)),
                    ))
                }
                DataType::FixedSizeBinary(_) => {
                    let values = column
                        .as_any()
                        .downcast_ref::<FixedSizeBinaryArray>()
                        .unwrap();
                    array(
                        FixedSizeBinaryArray::try_from_iter((0..rows).map(|_| values.value(0)))
                            .unwrap(),
                    )
                }
                data_type => panic!("unexpected positioned envelope type {data_type:?}"),
            })
            .collect::<Vec<_>>();
        let batch = RecordBatch::try_new(one.schema(), columns).unwrap();
        let bytes = write_batch(batch).unwrap();
        assert!(bytes.len() < MAX_POSITIONED_ENVELOPE_BYTES);
        let error = match positioned_envelope_from_parquet(&bytes) {
            Ok(_) => panic!("oversized positioned envelope unexpectedly decoded"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("row count"), "{error}");
    }

    fn lexical_run_fixture() -> LexicalRunRef {
        LexicalRunRef {
            segment_key: "segment-2".to_string(),
            row_start: 0,
            row_count: 4,
            decoded_bytes: 400,
            postings_path: "lexical/postings/run.parquet".to_string(),
            postings_checksum: VALID_SEGMENT_CHECKSUM.to_string(),
            postings_bytes: 700,
            postings_row_group: 0,
            postings_group_checksum: VALID_GRAPH_CHECKSUM.to_string(),
            metadata_path: "lexical/rows/run.parquet".to_string(),
            metadata_checksum: VALID_GRAPH_CHECKSUM.to_string(),
            metadata_bytes: 500,
            metadata_row_group: 0,
            metadata_group_checksum: VALID_SEGMENT_CHECKSUM.to_string(),
        }
    }

    #[test]
    fn lexical_hierarchy_round_trips_as_parquet_without_private_container() {
        let root = LexicalRoot {
            kind: LexicalKind::Bm25,
            dimensions: 0,
            document_count: 4,
            total_document_length: 20,
            pages: vec![LexicalTermPageRef {
                first_term: 7,
                last_term: 7,
                path: "lexical/terms/7.parquet".to_string(),
                checksum: VALID_SEGMENT_CHECKSUM.to_string(),
                content_checksum: VALID_GRAPH_CHECKSUM.to_string(),
                encoded_bytes: 900,
                term_count: 1,
            }],
        };
        let root_bytes = lexical_root_to_parquet(&root).unwrap();
        assert_eq!(&root_bytes[..4], b"PAR1");
        assert_eq!(lexical_root_from_parquet(&root_bytes).unwrap(), root);

        let page = LexicalTermPage {
            kind: LexicalKind::Bm25,
            entries: vec![LexicalTermBlock {
                term: 7,
                document_frequency: 2,
                run: lexical_run_fixture(),
                posting_count: 2,
                min_value: 1.0,
                max_value: 3.0,
                min_doc_length: 2,
            }],
        };
        let page_bytes = lexical_term_page_to_parquet(&root, &page).unwrap();
        assert_eq!(&page_bytes[..4], b"PAR1");
        assert_eq!(
            lexical_term_page_from_parquet(&root, &page_bytes).unwrap(),
            page
        );
    }

    #[test]
    fn lexical_postings_and_rows_round_trip_as_typed_parquet() {
        let bm25 = vec![
            Bm25Posting {
                term: 2,
                row: 0,
                term_frequency: 1,
            },
            Bm25Posting {
                term: 2,
                row: 3,
                term_frequency: 4,
            },
        ];
        let bytes = bm25_postings_to_parquet(&bm25, 4).unwrap();
        assert_eq!(&bytes[..4], b"PAR1");
        assert_eq!(bm25_postings_from_parquet(&bytes, 4).unwrap(), bm25);

        let sparse = vec![
            SparsePosting {
                term: 4,
                row: 0,
                value: -0.5,
            },
            SparsePosting {
                term: 9,
                row: 2,
                value: 1.25,
            },
        ];
        let bytes = sparse_postings_to_parquet(&sparse, 4).unwrap();
        assert_eq!(sparse_postings_from_parquet(&bytes, 4).unwrap(), sparse);

        let rows = (0..4)
            .map(|row| LexicalRowMetadata {
                row,
                record_id: format!("doc-{row}").into_bytes(),
                generation: u64::from(row),
                mutation_stamp: Some(MutationStamp::new(
                    MutationVersion::from_parts(7_000, [row as u8; 16]),
                    [0x80 + row as u8; 32],
                )),
                document_length: row + 1,
            })
            .collect::<Vec<_>>();
        let bytes = lexical_row_metadata_to_parquet(LexicalKind::Bm25, &rows).unwrap();
        let batches = read_batches(&bytes).unwrap();
        let schema = batches[0].schema();
        assert_eq!(
            schema.field_with_name("mutation_hlc").unwrap().data_type(),
            &DataType::UInt64
        );
        assert_eq!(
            schema
                .field_with_name("mutation_writer")
                .unwrap()
                .data_type(),
            &DataType::FixedSizeBinary(16)
        );
        assert_eq!(
            schema
                .field_with_name("mutation_digest")
                .unwrap()
                .data_type(),
            &DataType::FixedSizeBinary(32)
        );
        assert_eq!(
            lexical_row_metadata_from_parquet(LexicalKind::Bm25, &bytes).unwrap(),
            rows
        );
    }

    #[test]
    fn lexical_rows_reject_mixed_mutation_stamps() {
        let rows = vec![
            LexicalRowMetadata {
                row: 0,
                record_id: b"stamped".to_vec(),
                generation: 1,
                mutation_stamp: Some(MutationStamp::new(
                    MutationVersion::from_parts(11, [1; 16]),
                    [2; 32],
                )),
                document_length: 1,
            },
            LexicalRowMetadata {
                row: 1,
                record_id: b"unstamped".to_vec(),
                generation: 2,
                mutation_stamp: None,
                document_length: 1,
            },
        ];

        let error = lexical_row_metadata_to_parquet(LexicalKind::Bm25, &rows).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("cannot mix stamped and unstamped mutations")
        );
    }

    #[test]
    fn tombstone_state_round_trips_as_typed_parquet_without_legacy_fallback() {
        let entries = vec![
            (
                b"delete".to_vec(),
                MutationState::new(
                    MutationStamp::new(MutationVersion::from_parts(33, [1; 16]), [4; 32]),
                    MutationOperation::Delete,
                ),
            ),
            (
                b"put".to_vec(),
                MutationState::new(
                    MutationStamp::new(MutationVersion::from_parts(33, [2; 16]), [5; 32]),
                    MutationOperation::Put,
                ),
            ),
        ];

        let bytes = tombstone_ids_to_parquet(&entries).unwrap();
        let batches = read_batches(&bytes).unwrap();
        let schema = batches[0].schema();
        assert_eq!(
            schema.field_with_name("mutation_hlc").unwrap().data_type(),
            &DataType::UInt64
        );
        assert_eq!(
            schema
                .field_with_name("mutation_writer")
                .unwrap()
                .data_type(),
            &DataType::FixedSizeBinary(16)
        );
        assert_eq!(
            schema
                .field_with_name("mutation_digest")
                .unwrap()
                .data_type(),
            &DataType::FixedSizeBinary(32)
        );
        assert_eq!(
            schema.field_with_name("deleted").unwrap().data_type(),
            &DataType::Boolean
        );
        assert!(schema.field_with_name("min_visible_generation").is_err());
        assert_eq!(tombstone_ids_from_parquet(&bytes).unwrap(), entries);
    }

    #[test]
    fn positioned_transaction_metadata_round_trips_exact_header_and_terms() {
        let stamp = MutationStamp::new(MutationVersion::from_parts(7, [3; 16]), [9; 32]);
        let rows = vec![
            PositionedTransactionMetadataRow {
                modality: "primary".to_string(),
                logical_record_count: 3,
                next_generated_id_floor: 11,
                new_tombstone_ids: 2,
                document_count_delta: -1,
                total_document_length_delta: -4,
                term: None,
                document_frequency_delta: 0,
                stamp,
            },
            PositionedTransactionMetadataRow {
                modality: "primary".to_string(),
                logical_record_count: 0,
                next_generated_id_floor: 0,
                new_tombstone_ids: 0,
                document_count_delta: 0,
                total_document_length_delta: 0,
                term: Some(5),
                document_frequency_delta: -1,
                stamp,
            },
        ];
        let bytes = positioned_transaction_metadata_to_parquet(&rows).unwrap();
        assert_eq!(
            positioned_transaction_metadata_from_parquet(&bytes).unwrap(),
            rows
        );
    }

    #[test]
    fn positioned_route_plan_round_trips_catalog_analyzer_and_omitted_modalities() {
        let stamp = MutationStamp::new(MutationVersion::from_parts(17, [4; 16]), [8; 32]);
        let later_stamp = MutationStamp::new(MutationVersion::from_parts(18, [4; 16]), [9; 32]);
        let rows = vec![
            PositionedRoutePlanRow::summary(
                "omitted",
                PositionedRouteProjectionKind::Dense,
                PositionedRouteAssignment::catalog([1; 32], 7).unwrap(),
                0,
                stamp,
            )
            .unwrap(),
            PositionedRoutePlanRow::summary(
                "primary",
                PositionedRouteProjectionKind::Primary,
                PositionedRouteAssignment::catalog([2; 32], 7).unwrap(),
                1,
                stamp,
            )
            .unwrap(),
            PositionedRoutePlanRow::routed(
                b"row-a".to_vec(),
                "primary",
                PositionedRouteProjectionKind::Primary,
                0,
                PositionedRouteAssignment::catalog([2; 32], 7).unwrap(),
                19,
                stamp,
            )
            .unwrap(),
            PositionedRoutePlanRow::summary(
                "text",
                PositionedRouteProjectionKind::Text,
                PositionedRouteAssignment::analyzer([3; 32]).unwrap(),
                1,
                stamp,
            )
            .unwrap(),
            PositionedRoutePlanRow::term_partitioned(
                b"row-a".to_vec(),
                "text",
                PositionedRouteProjectionKind::Text,
                0,
                PositionedRouteAssignment::analyzer([3; 32]).unwrap(),
                stamp,
            )
            .unwrap(),
            PositionedRoutePlanRow::summary(
                "tokens",
                PositionedRouteProjectionKind::LateInteraction,
                PositionedRouteAssignment::catalog([4; 32], 11).unwrap(),
                2,
                stamp,
            )
            .unwrap(),
            PositionedRoutePlanRow::routed(
                b"row-a".to_vec(),
                "tokens",
                PositionedRouteProjectionKind::LateInteraction,
                0,
                PositionedRouteAssignment::catalog([4; 32], 11).unwrap(),
                5,
                stamp,
            )
            .unwrap(),
            PositionedRoutePlanRow::routed(
                b"row-a".to_vec(),
                "tokens",
                PositionedRouteProjectionKind::LateInteraction,
                1,
                PositionedRouteAssignment::catalog([4; 32], 11).unwrap(),
                8,
                later_stamp,
            )
            .unwrap(),
        ];

        let bytes = positioned_route_plan_to_parquet(&rows).unwrap();
        assert_eq!(positioned_route_plan_from_parquet(&bytes).unwrap(), rows);
    }

    #[test]
    fn positioned_route_plan_rejects_noncanonical_rows_and_count_drift() {
        let stamp = MutationStamp::new(MutationVersion::from_parts(17, [4; 16]), [8; 32]);
        let assignment = PositionedRouteAssignment::catalog([2; 32], 7).unwrap();
        let summary = PositionedRoutePlanRow::summary(
            "primary",
            PositionedRouteProjectionKind::Primary,
            assignment.clone(),
            2,
            stamp,
        )
        .unwrap();
        let routed = PositionedRoutePlanRow::routed(
            b"row-a".to_vec(),
            "primary",
            PositionedRouteProjectionKind::Primary,
            0,
            assignment,
            19,
            stamp,
        )
        .unwrap();

        assert!(positioned_route_plan_to_parquet(&[routed.clone(), summary.clone()]).is_err());
        assert!(positioned_route_plan_to_parquet(&[summary, routed]).is_err());

        let duplicate_summary = PositionedRoutePlanRow::summary(
            "primary",
            PositionedRouteProjectionKind::Primary,
            PositionedRouteAssignment::catalog([2; 32], 7).unwrap(),
            2,
            stamp,
        )
        .unwrap();
        let duplicate_row = PositionedRoutePlanRow::routed(
            b"row-a".to_vec(),
            "primary",
            PositionedRouteProjectionKind::Primary,
            0,
            PositionedRouteAssignment::catalog([2; 32], 7).unwrap(),
            19,
            stamp,
        )
        .unwrap();
        assert!(
            positioned_route_plan_to_parquet(&[
                duplicate_summary,
                duplicate_row.clone(),
                duplicate_row,
            ])
            .is_err()
        );
    }

    #[test]
    fn positioned_route_plan_rejects_zero_identity_and_mixed_assignment_kinds() {
        assert!(PositionedRouteAssignment::catalog([0; 32], 7).is_err());
        assert!(PositionedRouteAssignment::catalog([2; 32], 0).is_err());
        assert!(PositionedRouteAssignment::analyzer([0; 32]).is_err());

        let stamp = MutationStamp::new(MutationVersion::from_parts(17, [4; 16]), [8; 32]);
        assert!(
            PositionedRoutePlanRow::term_partitioned(
                b"row-a".to_vec(),
                "primary",
                PositionedRouteProjectionKind::Primary,
                0,
                PositionedRouteAssignment::analyzer([2; 32]).unwrap(),
                stamp,
            )
            .is_err()
        );
    }

    #[test]
    fn positioned_route_plan_rejects_bad_entity_and_token_ordinals() {
        let stamp = MutationStamp::new(MutationVersion::from_parts(17, [4; 16]), [8; 32]);
        let primary = PositionedRouteAssignment::catalog([2; 32], 7).unwrap();
        let bad_entity = vec![
            PositionedRoutePlanRow::summary(
                "primary",
                PositionedRouteProjectionKind::Primary,
                primary.clone(),
                1,
                stamp,
            )
            .unwrap(),
            PositionedRoutePlanRow::routed(
                b"row-a".to_vec(),
                "primary",
                PositionedRouteProjectionKind::Primary,
                1,
                primary,
                3,
                stamp,
            )
            .unwrap(),
        ];
        assert!(positioned_route_plan_to_parquet(&bad_entity).is_err());

        let tokens = PositionedRouteAssignment::catalog([3; 32], 9).unwrap();
        let token_gap = vec![
            PositionedRoutePlanRow::summary(
                "tokens",
                PositionedRouteProjectionKind::LateInteraction,
                tokens.clone(),
                2,
                stamp,
            )
            .unwrap(),
            PositionedRoutePlanRow::routed(
                b"row-a".to_vec(),
                "tokens",
                PositionedRouteProjectionKind::LateInteraction,
                0,
                tokens.clone(),
                3,
                stamp,
            )
            .unwrap(),
            PositionedRoutePlanRow::routed(
                b"row-a".to_vec(),
                "tokens",
                PositionedRouteProjectionKind::LateInteraction,
                2,
                tokens,
                4,
                stamp,
            )
            .unwrap(),
        ];
        assert!(positioned_route_plan_to_parquet(&token_gap).is_err());
    }

    #[test]
    fn positioned_route_plan_rejects_old_marker_and_conflicting_summary_stamp() {
        let stamp = MutationStamp::new(MutationVersion::from_parts(17, [4; 16]), [8; 32]);
        let later_stamp = MutationStamp::new(MutationVersion::from_parts(18, [4; 16]), [9; 32]);
        let assignment = PositionedRouteAssignment::catalog([2; 32], 7).unwrap();
        let rows = vec![
            PositionedRoutePlanRow::summary(
                "primary",
                PositionedRouteProjectionKind::Primary,
                assignment.clone(),
                1,
                later_stamp,
            )
            .unwrap(),
            PositionedRoutePlanRow::routed(
                b"row-a".to_vec(),
                "primary",
                PositionedRouteProjectionKind::Primary,
                0,
                assignment,
                3,
                stamp,
            )
            .unwrap(),
        ];
        assert!(positioned_route_plan_to_parquet(&rows).is_err());

        let valid = vec![
            PositionedRoutePlanRow::summary(
                "primary",
                PositionedRouteProjectionKind::Primary,
                PositionedRouteAssignment::catalog([2; 32], 7).unwrap(),
                1,
                stamp,
            )
            .unwrap(),
            PositionedRoutePlanRow::routed(
                b"row-a".to_vec(),
                "primary",
                PositionedRouteProjectionKind::Primary,
                0,
                PositionedRouteAssignment::catalog([2; 32], 7).unwrap(),
                3,
                stamp,
            )
            .unwrap(),
        ];
        let bytes = positioned_route_plan_to_parquet(&valid).unwrap();
        let batch = read_batches(&bytes).unwrap().remove(0);
        let mut columns = batch.columns().to_vec();
        columns[batch.schema().index_of("format_version").unwrap()] = array(
            UInt16Array::from_iter_values((0..batch.num_rows()).map(|_| 0)),
        );
        let old = write_batch(RecordBatch::try_new(batch.schema(), columns).unwrap()).unwrap();
        assert!(positioned_route_plan_from_parquet(&old).is_err());
    }

    #[test]
    fn positioned_route_plan_decoder_rejects_null_and_invalid_discriminants() {
        let stamp = MutationStamp::new(MutationVersion::from_parts(17, [4; 16]), [8; 32]);
        let rows = vec![
            PositionedRoutePlanRow::summary(
                "primary",
                PositionedRouteProjectionKind::Primary,
                PositionedRouteAssignment::catalog([2; 32], 7).unwrap(),
                1,
                stamp,
            )
            .unwrap(),
            PositionedRoutePlanRow::routed(
                b"row-a".to_vec(),
                "primary",
                PositionedRouteProjectionKind::Primary,
                0,
                PositionedRouteAssignment::catalog([2; 32], 7).unwrap(),
                3,
                stamp,
            )
            .unwrap(),
        ];
        let bytes = positioned_route_plan_to_parquet(&rows).unwrap();
        let batch = read_batches(&bytes).unwrap().remove(0);

        let corrupt = |name: &str, values: ArrayRef| {
            let mut columns = batch.columns().to_vec();
            columns[batch.schema().index_of(name).unwrap()] = values;
            write_batch(RecordBatch::try_new(batch.schema(), columns).unwrap()).unwrap()
        };
        let corrupt_nullable = |name: &str, values: ArrayRef| {
            let nullable_column = batch.schema().index_of(name).unwrap();
            let mut columns = batch.columns().to_vec();
            columns[nullable_column] = values;
            let schema = Arc::new(Schema::new(
                batch
                    .schema()
                    .fields()
                    .iter()
                    .enumerate()
                    .map(|(index, field)| {
                        if index == nullable_column {
                            Field::new(name, field.data_type().clone(), true)
                        } else {
                            field.as_ref().clone()
                        }
                    })
                    .collect::<Vec<_>>(),
            ));
            write_batch(RecordBatch::try_new(schema, columns).unwrap()).unwrap()
        };
        let null_version = corrupt_nullable(
            "format_version",
            array(UInt16Array::from(vec![None, Some(1)])),
        );
        assert!(positioned_route_plan_from_parquet(&null_version).is_err());

        let null_hlc = corrupt_nullable(
            "mutation_hlc",
            array(UInt64Array::from(vec![None, Some(17)])),
        );
        assert!(positioned_route_plan_from_parquet(&null_hlc).is_err());

        let null_projection = corrupt_nullable(
            "projection_kind",
            array(UInt8Array::from(vec![None, Some(0)])),
        );
        assert!(positioned_route_plan_from_parquet(&null_projection).is_err());

        let invalid_assignment = corrupt(
            "assignment_kind",
            array(UInt8Array::from_iter_values([0, 9])),
        );
        assert!(positioned_route_plan_from_parquet(&invalid_assignment).is_err());

        let invalid_projection = corrupt(
            "projection_kind",
            array(UInt8Array::from_iter_values([0, 9])),
        );
        assert!(positioned_route_plan_from_parquet(&invalid_projection).is_err());

        let invalid_row_kind = corrupt("row_kind", array(UInt8Array::from_iter_values([1, 1])));
        assert!(positioned_route_plan_from_parquet(&invalid_row_kind).is_err());
    }

    #[test]
    fn positioned_route_plan_allows_summary_only_and_rejects_digest_conflict() {
        let stamp = MutationStamp::new(MutationVersion::from_parts(17, [4; 16]), [8; 32]);
        let summaries = vec![
            PositionedRoutePlanRow::summary(
                "omitted",
                PositionedRouteProjectionKind::Dense,
                PositionedRouteAssignment::catalog([2; 32], 7).unwrap(),
                0,
                stamp,
            )
            .unwrap(),
            PositionedRoutePlanRow::summary(
                "primary",
                PositionedRouteProjectionKind::Primary,
                PositionedRouteAssignment::catalog([3; 32], 7).unwrap(),
                0,
                stamp,
            )
            .unwrap(),
        ];
        let bytes = positioned_route_plan_to_parquet(&summaries).unwrap();
        assert_eq!(
            positioned_route_plan_from_parquet(&bytes).unwrap(),
            summaries
        );

        let conflicting = vec![
            PositionedRoutePlanRow::summary(
                "primary",
                PositionedRouteProjectionKind::Primary,
                PositionedRouteAssignment::catalog([3; 32], 7).unwrap(),
                1,
                stamp,
            )
            .unwrap(),
            PositionedRoutePlanRow::routed(
                b"row-a".to_vec(),
                "primary",
                PositionedRouteProjectionKind::Primary,
                0,
                PositionedRouteAssignment::catalog([3; 32], 7).unwrap(),
                3,
                MutationStamp::new(stamp.version(), [9; 32]),
            )
            .unwrap(),
        ];
        assert!(positioned_route_plan_to_parquet(&conflicting).is_err());
    }

    #[test]
    fn sparse_float16_postings_are_physically_float16_and_decode_canonically() {
        let sparse = vec![
            SparsePosting {
                term: 4,
                row: 0,
                value: 0.333_3,
            },
            SparsePosting {
                term: 9,
                row: 2,
                value: -1.000_1,
            },
        ];
        let bytes = sparse_postings_to_parquet_typed(&sparse, 4, crate::VectorElementType::Float16)
            .unwrap();
        let batches = read_batches(&bytes).unwrap();
        assert_eq!(
            batches[0]
                .schema()
                .field_with_name("value")
                .unwrap()
                .data_type(),
            &DataType::Float16
        );
        let decoded = sparse_postings_from_parquet(&bytes, 4).unwrap();
        let expected = sparse
            .into_iter()
            .map(|mut posting| {
                posting.value = f32::from(half::f16::from_f32(posting.value));
                posting
            })
            .collect::<Vec<_>>();
        assert_eq!(decoded, expected);
    }

    #[test]
    fn graph_from_parquet_rejects_non_finite_edge_distances() {
        let bytes = external_graph_parquet(f32::NAN);

        let records = vec![
            VectorRecord::new("source", vec![0.0, 0.0]),
            VectorRecord::new("neighbor", vec![1.0, 0.0]),
        ];
        let err = graph_from_parquet(&bytes, "seg", 0, &records).unwrap_err();

        assert!(err.to_string().contains("finite f32 values"), "{err}");
    }

    #[test]
    fn routing_from_parquet_rejects_non_finite_centroids() {
        let bytes = external_routing_parquet([f32::NAN, 0.0], 1.0);

        let err = routing_from_parquet(&bytes, 1).unwrap_err();

        assert!(err.to_string().contains("finite f32 values"), "{err}");
    }

    #[test]
    fn routing_from_parquet_rejects_non_finite_radii() {
        let bytes = external_routing_parquet([0.0, 0.0], f32::INFINITY);

        let err = routing_from_parquet(&bytes, 1).unwrap_err();

        assert!(err.to_string().contains("finite f32 values"), "{err}");
    }

    #[test]
    fn pivots_from_parquet_rejects_non_finite_vectors() {
        let bytes = external_pivots_parquet([f32::NAN, 0.0]);

        let err = pivots_from_parquet(&bytes, 2, 1).unwrap_err();

        assert!(err.to_string().contains("finite f32 values"), "{err}");
    }

    #[test]
    fn pivots_from_parquet_rejects_empty_pivot_ids() {
        let bytes = external_pivots_parquet_with_ids([""]);

        let err = pivots_from_parquet(&bytes, 2, 1).unwrap_err();

        assert!(
            err.to_string().contains("pivot ids must not be empty"),
            "{err}"
        );
    }

    #[test]
    fn write_batch_row_groups_stay_under_parquet_cap_for_huge_files() {
        // A bulk-load L0 segment can hold the whole corpus before compaction
        // splits it. At 32 rows/group, > ~1M rows would exceed Parquet's 32767
        // hard limit. Check the production sizing calculation directly instead
        // of making a debug test construct ~30k Arrow row groups (which itself
        // overflows the library's recursive drop stack on some platforms).
        let huge_rows = MAX_PARQUET_ROW_GROUPS * SEGMENT_ROW_GROUP_ROWS + 1;
        let effective = effective_row_group_rows(huge_rows, SEGMENT_ROW_GROUP_ROWS);
        assert_eq!(effective, SEGMENT_ROW_GROUP_ROWS + 1);
        assert!(huge_rows.div_ceil(effective) <= MAX_PARQUET_ROW_GROUPS);

        // Retain an actual writer/reader round trip at a representative size.
        let rows = SEGMENT_ROW_GROUP_ROWS * 4 + 1;
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![array(Int64Array::from_iter_values(0..rows as i64))],
        )
        .unwrap();
        let bytes = write_batch_with_row_groups(batch, Some(SEGMENT_ROW_GROUP_ROWS)).unwrap();
        let total: usize = read_batches(&bytes)
            .unwrap()
            .iter()
            .map(RecordBatch::num_rows)
            .sum();
        assert_eq!(total, rows);
    }

    #[test]
    fn pivots_from_parquet_rejects_duplicate_pivot_ids() {
        let bytes = external_pivots_parquet_with_ids(["pivot", "pivot"]);

        let err = pivots_from_parquet(&bytes, 2, 1).unwrap_err();

        assert!(err.to_string().contains("duplicate pivot id"), "{err}");
    }

    #[test]
    fn segment_from_parquet_rejects_non_finite_centroids() {
        let bytes = external_segment_parquet([0.0, 0.0], [f32::NAN, 0.0], 0.0, 0.0);

        let err = segment_from_parquet(&bytes).unwrap_err();

        assert!(err.to_string().contains("centroids"), "{err}");
    }

    #[test]
    fn segment_from_parquet_rejects_non_finite_radii() {
        let bytes = external_segment_parquet([0.0, 0.0], [0.0, 0.0], f32::INFINITY, 0.0);

        let err = segment_from_parquet(&bytes).unwrap_err();

        assert!(err.to_string().contains("radii"), "{err}");
    }

    #[test]
    fn segment_from_parquet_rejects_non_finite_routing_codes() {
        let bytes = external_segment_parquet([0.0, 0.0], [0.0, 0.0], 0.0, f32::NAN);

        let err = segment_from_parquet(&bytes).unwrap_err();

        assert!(err.to_string().contains("finite f32 values"), "{err}");
    }

    #[test]
    fn segment_to_parquet_round_trips_pq_codes() {
        let mut segment = valid_segment();
        segment.pq_codes = vec![vec![7, 249]];

        let bytes = segment_to_parquet(&segment).unwrap();
        let batch = first_batch(&bytes, "segment").unwrap();

        assert!(batch.schema().field_with_name("pq_code").is_ok());
        assert_eq!(
            segment_from_parquet(&bytes).unwrap().pq_codes,
            segment.pq_codes
        );
    }

    #[test]
    fn segment_to_parquet_round_trips_padded_coarse_codes() {
        // TurboQuant's SRHT rotation pads a non-power-of-two dimensionality up to
        // the next power of two, so its coarse-code triplet (pq_code/pq_min/pq_max)
        // is WIDER than `dimensions`. Persist a dim=3, coarse-width=4 segment and
        // confirm the padded codes and bounds round-trip byte-for-byte.
        let mut segment = valid_segment();
        segment.dimensions = 3;
        segment.centroid = vec![0.1, 0.2, 0.3];
        segment.records[0].vector = vec![0.1, 0.2, 0.3];
        // Coarse columns at padded length 4 (> dimensions 3).
        segment.pq_codes = vec![vec![1, 2, 3, 4]];
        segment.pq_min = vec![-1.0, -2.0, -3.0, -4.0];
        segment.pq_max = vec![1.0, 2.0, 3.0, 4.0];

        let bytes = segment_to_parquet(&segment).unwrap();
        let batch = first_batch(&bytes, "segment").unwrap();

        // The stored coarse columns are 4 wide, the dense/centroid columns 3 wide.
        let pq_code_field = batch.schema().field_with_name("pq_code").unwrap().clone();
        assert_eq!(
            pq_code_field.data_type(),
            &DataType::FixedSizeList(Arc::new(Field::new_list_field(DataType::UInt8, true)), 4,)
        );

        let decoded = segment_from_parquet(&bytes).unwrap();
        assert_eq!(decoded.dimensions, 3);
        assert_eq!(decoded.pq_codes, segment.pq_codes);
        assert_eq!(decoded.pq_min, segment.pq_min);
        assert_eq!(decoded.pq_max, segment.pq_max);
    }

    #[test]
    fn segment_to_parquet_writes_binary_record_ids() {
        let segment = valid_segment();

        let bytes = segment_to_parquet(&segment).unwrap();
        let batch = first_batch(&bytes, "segment").unwrap();

        assert_eq!(
            batch
                .schema()
                .field_with_name("record_id")
                .unwrap()
                .data_type(),
            &DataType::Binary
        );
    }

    #[test]
    fn segment_to_parquet_omits_sparse_and_text_columns_for_dense_plain_segment() {
        let mut segment = valid_segment();
        segment.records[0].storage = crate::StorageEncoding::Dense;

        let bytes = segment_to_parquet(&segment).unwrap();
        let batch = first_batch(&bytes, "segment").unwrap();
        let schema = batch.schema();

        // The dense `vector` column is gone: dense vectors live only in the
        // Arrow IPC sidecar now.
        assert!(schema.field_with_name("vector").is_err());
        assert!(schema.field_with_name("sparse_indices").is_err());
        assert!(schema.field_with_name("sparse_values").is_err());
        assert!(schema.field_with_name("text_term_ids").is_err());
        assert!(schema.field_with_name("text_term_freqs").is_err());
        let decoded = segment_from_parquet(&bytes).unwrap();
        assert_eq!(decoded.records[0].id, segment.records[0].id);
        // Parquet decode yields empty dense vectors; reconstruction from the
        // sidecar happens at the index read boundary.
        assert!(decoded.records[0].vector.is_empty());
        assert_eq!(decoded.records[0].metadata, segment.records[0].metadata);
    }

    #[test]
    fn segment_to_parquet_includes_sparse_columns_when_any_record_is_sparse() {
        let mut segment = valid_segment();
        segment.records[0].vector = vec![0.0, 1.5];
        segment.records[0].storage = crate::StorageEncoding::Sparse;

        let bytes = segment_to_parquet(&segment).unwrap();
        let batch = first_batch(&bytes, "segment").unwrap();
        let schema = batch.schema();

        assert!(schema.field_with_name("sparse_indices").is_ok());
        assert!(schema.field_with_name("sparse_values").is_ok());
        assert!(schema.field_with_name("text_term_ids").is_err());
        assert!(schema.field_with_name("text_term_freqs").is_err());
        // Parquet decode of a sparse record yields an empty dense vector; the
        // densified vector is reconstructed from the sidecar at the read
        // boundary, so the format-level decode leaves it empty.
        assert!(
            segment_from_parquet(&bytes).unwrap().records[0]
                .vector
                .is_empty()
        );
    }

    #[test]
    fn segment_to_parquet_includes_text_columns_when_any_record_has_terms() {
        let mut segment = valid_segment();
        segment.records[0].storage = crate::StorageEncoding::Dense;
        segment.records[0].text_term_ids = vec![7, 11];
        segment.records[0].text_term_freqs = vec![2, 1];

        let bytes = segment_to_parquet(&segment).unwrap();
        let batch = first_batch(&bytes, "segment").unwrap();
        let schema = batch.schema();

        assert!(schema.field_with_name("sparse_indices").is_err());
        assert!(schema.field_with_name("sparse_values").is_err());
        assert!(schema.field_with_name("text_term_ids").is_ok());
        assert!(schema.field_with_name("text_term_freqs").is_ok());
        assert_eq!(
            segment_from_parquet(&bytes).unwrap().records[0].text_term_ids,
            segment.records[0].text_term_ids
        );
        assert_eq!(
            segment_from_parquet(&bytes).unwrap().records[0].text_term_freqs,
            segment.records[0].text_term_freqs
        );
    }

    #[test]
    fn segment_parquet_round_trips_non_utf8_record_ids() {
        let mut segment = valid_segment();
        segment.records[0] = VectorRecord::new_bytes(vec![0, 159, 255, 7], vec![0.25, -0.75]);

        let bytes = segment_to_parquet(&segment).unwrap();

        let decoded = segment_from_parquet(&bytes).unwrap();
        // The dense vector is no longer stored in Parquet, so it decodes empty
        // (reconstruction from the sidecar happens at the index read boundary);
        // the non-UTF-8 record id must still round-trip byte-for-byte.
        assert_eq!(decoded.records[0].id, segment.records[0].id);
        assert!(decoded.records[0].vector.is_empty());
        assert_eq!(decoded.routing_codes[0], segment.routing_codes[0]);
        assert_eq!(decoded.pq_codes[0], segment.pq_codes[0]);
    }

    #[test]
    fn segment_from_parquet_requires_persisted_pq_codes() {
        // Dense vectors no longer live in the Parquet segment, so a legacy
        // segment that lacks a persisted `pq_code` column can no longer have its
        // PQ codes reconstructed from resident vectors. Every segment this
        // codebase writes persists `pq_code`/`pq_min`/`pq_max`, so such a table
        // is malformed and decode rejects it rather than fabricating codes from
        // absent vectors.
        let bytes = external_segment_parquet([0.25, -0.75], [0.0, 0.0], 1.0, 1.0);

        let err = segment_from_parquet(&bytes).unwrap_err();

        assert!(
            matches!(err, BorsukError::DimensionMismatch { .. }),
            "{err}"
        );
    }

    #[test]
    fn routing_from_parquet_rejects_centroids_with_wrong_dimensions() {
        let bytes = external_routing_parquet_with_dimensions([0.0, 0.0], 1.0, 3);

        let err = routing_from_parquet(&bytes, 1).unwrap_err();

        assert!(err.to_string().contains("dimensions"), "{err}");
    }

    #[test]
    fn routing_from_parquet_rejects_malformed_id_bloom() {
        let bytes = external_routing_parquet_with_id_bloom(vec![0_u8; 3]);

        let err = routing_from_parquet(&bytes, 1).unwrap_err();

        assert!(err.to_string().contains("id_bloom"), "{err}");
    }

    #[test]
    fn routing_from_parquet_rejects_malformed_vector_signature_bloom() {
        let bytes = external_routing_parquet_with_vector_signature_bloom(vec![0_u8; 3]);

        let err = routing_from_parquet(&bytes, 1).unwrap_err();

        assert!(err.to_string().contains("vector_signature_bloom"), "{err}");
    }

    #[test]
    fn routing_from_parquet_rejects_unknown_leaf_mode() {
        let bytes = external_routing_parquet_with_leaf_mode("unknown-leaf");

        let err = routing_from_parquet(&bytes, 1).unwrap_err();

        assert!(err.to_string().contains("routing leaf_mode"), "{err}");
    }

    #[test]
    fn routing_from_parquet_rejects_empty_segment_ids() {
        let bytes = external_routing_parquet_with_segment_ids([""]);

        let err = routing_from_parquet(&bytes, 1).unwrap_err();

        assert!(
            err.to_string()
                .contains("routing segment ids must not be empty"),
            "{err}"
        );
    }

    #[test]
    fn routing_from_parquet_rejects_duplicate_segment_ids() {
        let bytes = external_routing_parquet_with_segment_ids(["seg", "seg"]);

        let err = routing_from_parquet(&bytes, 1).unwrap_err();

        assert!(
            err.to_string().contains("duplicate routing segment id"),
            "{err}"
        );
    }

    #[test]
    fn routing_from_parquet_rejects_empty_segment_paths() {
        let bytes = external_routing_parquet_with_paths([""], ["segments/seg.graph.parquet"]);

        let err = routing_from_parquet(&bytes, 1).unwrap_err();

        assert!(
            err.to_string()
                .contains("routing segment paths must not be empty"),
            "{err}"
        );
    }

    #[test]
    fn routing_from_parquet_rejects_duplicate_segment_paths() {
        let bytes = external_routing_parquet_with_paths(
            ["segments/seg.parquet", "segments/seg.parquet"],
            ["segments/a.graph.parquet", "segments/b.graph.parquet"],
        );

        let err = routing_from_parquet(&bytes, 1).unwrap_err();

        assert!(
            err.to_string().contains("duplicate routing segment path"),
            "{err}"
        );
    }

    #[test]
    fn routing_from_parquet_rejects_partial_empty_graph_paths() {
        // An empty graph path paired with a populated checksum/size is an
        // inconsistent (corrupt) triple and is still rejected. A fully-empty
        // triple (the graph-free `PqScanOnly` case) is accepted; see
        // `routing_from_parquet_accepts_absent_graph`.
        let bytes = external_routing_parquet_with_paths(["segments/seg.parquet"], [""]);

        let err = routing_from_parquet(&bytes, 1).unwrap_err();

        assert!(
            err.to_string()
                .contains("routing graph path must be present when a graph is stored"),
            "{err}"
        );
    }

    #[test]
    fn routing_from_parquet_accepts_absent_graph() {
        // A graph-free segment carries an empty graph triple: empty path, empty
        // checksum, zero size. Round-trips without error.
        let mut row = valid_external_routing_summary_metadata();
        row.graph_checksum = "";
        row.graph_size_bytes = 0;
        let bytes = external_routing_parquet_with_rows_and_summary_metadata(
            &["seg"],
            &["segments/seg.parquet"],
            &[""],
            &[row],
        );

        let segments = routing_from_parquet(&bytes, 1).unwrap();
        assert_eq!(segments.len(), 1);
        assert!(segments[0].graph_path.is_empty());
        assert!(segments[0].graph_checksum.is_empty());
        assert_eq!(segments[0].graph_size_bytes, 0);
    }

    #[test]
    fn routing_from_parquet_rejects_duplicate_graph_paths() {
        let bytes = external_routing_parquet_with_paths(
            ["segments/a.parquet", "segments/b.parquet"],
            ["segments/seg.graph.parquet", "segments/seg.graph.parquet"],
        );

        let err = routing_from_parquet(&bytes, 1).unwrap_err();

        assert!(
            err.to_string().contains("duplicate routing graph path"),
            "{err}"
        );
    }

    #[test]
    fn routing_from_parquet_rejects_malformed_segment_checksums() {
        let bytes = external_routing_parquet_with_summary_metadata(
            1,
            "not-a-blake3-checksum",
            123,
            VALID_GRAPH_CHECKSUM,
            45,
        );

        let err = routing_from_parquet(&bytes, 1).unwrap_err();

        assert!(
            err.to_string()
                .contains("routing segment checksum must be 64 lowercase hex characters"),
            "{err}"
        );
    }

    #[test]
    fn routing_from_parquet_rejects_malformed_graph_checksums() {
        let bytes = external_routing_parquet_with_summary_metadata(
            1,
            VALID_SEGMENT_CHECKSUM,
            123,
            "not-a-blake3-checksum",
            45,
        );

        let err = routing_from_parquet(&bytes, 1).unwrap_err();

        assert!(
            err.to_string()
                .contains("routing graph checksum must be 64 lowercase hex characters"),
            "{err}"
        );
    }

    #[test]
    fn routing_from_parquet_rejects_empty_segment_summaries() {
        let bytes = external_routing_parquet_with_summary_metadata(
            0,
            VALID_SEGMENT_CHECKSUM,
            123,
            VALID_GRAPH_CHECKSUM,
            45,
        );

        let err = routing_from_parquet(&bytes, 1).unwrap_err();

        assert!(
            err.to_string()
                .contains("routing segment object_count must be greater than zero"),
            "{err}"
        );
    }

    #[test]
    fn routing_from_parquet_rejects_zero_segment_sizes() {
        let bytes = external_routing_parquet_with_summary_metadata(
            1,
            VALID_SEGMENT_CHECKSUM,
            0,
            VALID_GRAPH_CHECKSUM,
            45,
        );

        let err = routing_from_parquet(&bytes, 1).unwrap_err();

        assert!(
            err.to_string()
                .contains("routing segment size_bytes must be greater than zero"),
            "{err}"
        );
    }

    #[test]
    fn routing_from_parquet_rejects_zero_graph_sizes() {
        let bytes = external_routing_parquet_with_summary_metadata(
            1,
            VALID_SEGMENT_CHECKSUM,
            123,
            VALID_GRAPH_CHECKSUM,
            0,
        );

        let err = routing_from_parquet(&bytes, 1).unwrap_err();

        assert!(
            err.to_string()
                .contains("routing graph size_bytes must be greater than zero"),
            "{err}"
        );
    }

    #[test]
    fn segment_from_parquet_rejects_centroids_with_wrong_dimensions() {
        let bytes =
            external_segment_parquet_with_dimensions(vec![0.0, 0.0], vec![0.0, 0.0], 0.0, 0.0, 3);

        let err = segment_from_parquet(&bytes).unwrap_err();

        assert!(err.to_string().contains("dimensions"), "{err}");
    }

    #[test]
    fn segment_from_parquet_rejects_empty_record_ids() {
        let bytes = external_segment_parquet_with_records([("", [0.0, 0.0])]);

        let err = segment_from_parquet(&bytes).unwrap_err();

        assert!(
            err.to_string().contains("record ids must not be empty"),
            "{err}"
        );
    }

    #[test]
    fn segment_from_parquet_rejects_duplicate_record_ids() {
        let bytes =
            external_segment_parquet_with_records([("dup", [0.0, 0.0]), ("dup", [1.0, 0.0])]);

        let err = segment_from_parquet(&bytes).unwrap_err();

        assert!(err.to_string().contains("duplicate record id"), "{err}");
    }

    #[test]
    fn manifest_from_parquet_rejects_segment_dimension_mismatch() {
        let manifest_bytes = manifest_to_parquet(&valid_manifest()).unwrap();
        let routing_bytes = external_routing_parquet_with_vector(vec![0.0, 0.0, 0.0], 1.0, 3);

        let err = manifest_from_parquet(&manifest_bytes, &routing_bytes).unwrap_err();

        assert!(err.to_string().contains("dimensions"), "{err}");
    }

    #[test]
    fn manifest_from_parquet_rejects_routing_manifest_version_mismatch() {
        let manifest_bytes = manifest_to_parquet(&valid_manifest()).unwrap();
        let routing_bytes = external_routing_parquet_with_manifest_version(2);

        let err = manifest_from_parquet(&manifest_bytes, &routing_bytes).unwrap_err();

        assert!(
            err.to_string().contains("routing table manifest_version"),
            "{err}"
        );
    }

    #[test]
    fn manifest_persists_exact_catalog_strategy_and_rejects_old_markers() {
        let strategy = crate::centroid_hnsw::CatalogRoutingStrategy::hnsw(8, 24, 48, 31).unwrap();
        let mut manifest = valid_manifest();
        manifest.logical_cell_routing_strategy = strategy;
        let bytes = manifest_to_parquet(&manifest).unwrap();
        let routing = routing_to_parquet(&manifest).unwrap();
        let decoded = manifest_from_parquet(&bytes, &routing).unwrap();
        assert_eq!(decoded.logical_cell_routing_strategy, strategy);

        let bytes = manifest_to_parquet(&valid_manifest()).unwrap();
        let batch = first_batch(&bytes, "manifest").unwrap();
        let mut columns = batch.columns().to_vec();
        for old_version in [35_u16, 37] {
            columns[batch.schema().index_of("format_version").unwrap()] =
                array(UInt16Array::from_iter_values([old_version]));
            let old = write_batch(RecordBatch::try_new(batch.schema(), columns.clone()).unwrap())
                .unwrap();
            let routing = routing_to_parquet(&valid_manifest()).unwrap();

            let error = manifest_from_parquet(&old, &routing)
                .unwrap_err()
                .to_string();
            assert!(
                error.contains(&format!("unsupported manifest table version {old_version}")),
                "{error}"
            );
        }
    }

    #[test]
    fn manifest_from_parquet_rejects_invalid_config_dimensions() {
        let manifest_bytes = external_manifest_parquet(0, 100);
        let routing_bytes = routing_to_parquet(&valid_manifest()).unwrap();

        let err = manifest_from_parquet(&manifest_bytes, &routing_bytes).unwrap_err();

        assert!(
            err.to_string()
                .contains("manifest dimensions must be greater than zero"),
            "{err}"
        );
    }

    #[test]
    fn manifest_from_parquet_rejects_invalid_segment_max_vectors() {
        let manifest_bytes = external_manifest_parquet(2, 0);
        let routing_bytes = routing_to_parquet(&valid_manifest()).unwrap();

        let err = manifest_from_parquet(&manifest_bytes, &routing_bytes).unwrap_err();

        assert!(
            err.to_string()
                .contains("manifest segment_max_vectors must be greater than zero"),
            "{err}"
        );
    }

    #[test]
    fn pre_global_ann_manifest_is_rejected_instead_of_silently_upgraded() {
        let manifest_bytes = legacy_external_manifest_parquet_without_routing_page_fanout(2, 100);
        let routing_bytes = routing_to_parquet(&valid_manifest()).unwrap();

        let error = manifest_from_parquet(&manifest_bytes, &routing_bytes).unwrap_err();

        assert!(
            error
                .to_string()
                .contains("missing required global_ann_ref_json column"),
            "{error}"
        );
    }

    #[test]
    fn manifest_from_parquet_ignores_unknown_columns() {
        let mut expected = valid_manifest();
        expected.config.ram_budget_bytes = Some(4096);
        expected.next_generated_id = 17;
        expected.routing_max_level = 2;
        expected.routing_page_fanout = 64;
        expected.created_at = datetime_from_millis(1234).unwrap();
        let manifest_bytes =
            parquet_with_unknown_column_after_first(&manifest_to_parquet(&expected).unwrap());
        let routing_bytes = routing_to_parquet(&expected).unwrap();

        let manifest = manifest_from_parquet(&manifest_bytes, &routing_bytes).unwrap();

        assert_eq!(manifest.version, expected.version);
        assert_eq!(manifest.config.uri, expected.config.uri);
        assert_eq!(manifest.config.metric, expected.config.metric);
        assert_eq!(manifest.config.dimensions, expected.config.dimensions);
        assert_eq!(
            manifest.config.segment_max_vectors,
            expected.config.segment_max_vectors
        );
        assert_eq!(
            manifest.config.ram_budget_bytes,
            expected.config.ram_budget_bytes
        );
        assert_eq!(manifest.next_generated_id, expected.next_generated_id);
        assert_eq!(manifest.routing_max_level, expected.routing_max_level);
        assert_eq!(manifest.routing_page_fanout, expected.routing_page_fanout);
        assert_eq!(manifest.created_at, expected.created_at);
    }

    #[test]
    fn manifest_metadata_from_parquet_ignores_unknown_columns() {
        let mut expected = valid_manifest();
        expected.config.ram_budget_bytes = Some(4096);
        expected.next_generated_id = 17;
        expected.routing_max_level = 2;
        expected.routing_page_fanout = 64;
        expected.created_at = datetime_from_millis(1234).unwrap();
        let manifest_bytes =
            parquet_with_unknown_column_after_first(&manifest_to_parquet(&expected).unwrap());

        let manifest = manifest_metadata_from_parquet(&manifest_bytes).unwrap();

        assert_eq!(manifest.version, expected.version);
        assert_eq!(manifest.config.uri, expected.config.uri);
        assert_eq!(manifest.config.metric, expected.config.metric);
        assert_eq!(manifest.config.dimensions, expected.config.dimensions);
        assert_eq!(
            manifest.config.segment_max_vectors,
            expected.config.segment_max_vectors
        );
        assert_eq!(
            manifest.config.ram_budget_bytes,
            expected.config.ram_budget_bytes
        );
        assert_eq!(manifest.next_generated_id, expected.next_generated_id);
        assert_eq!(manifest.routing_max_level, expected.routing_max_level);
        assert_eq!(manifest.routing_page_fanout, expected.routing_page_fanout);
        assert_eq!(manifest.created_at, expected.created_at);
    }

    #[test]
    fn manifest_rejects_unversioned_global_ann_reference() {
        let error = decode_global_ann_ref_json(r#"{}"#).unwrap_err().to_string();
        assert!(error.contains("layout_version"), "{error}");
        assert!(error.contains("rebuild the unreleased index"), "{error}");
    }

    #[test]
    fn manifest_round_trips_paged_bm25_statistics_delta() {
        let mut expected = valid_manifest();
        expected.bm25_stats_delta = Some(crate::manifest::Bm25StatsDeltaRef {
            document_count_delta: -3,
            total_document_length_delta: -27,
            pages: vec![crate::manifest::Bm25StatsDeltaPageRef {
                first_term: 11,
                last_term: 99,
                path: "lexical/stats-delta/ab/stats.parquet".to_string(),
                checksum: "ab".repeat(32),
                encoded_bytes: 1234,
                term_count: 2,
            }],
        });

        let manifest_bytes = manifest_to_parquet(&expected).unwrap();
        let decoded = manifest_metadata_from_parquet(&manifest_bytes).unwrap();

        assert_eq!(decoded.bm25_stats_delta, expected.bm25_stats_delta);
    }

    #[test]
    fn manifest_round_trips_distributed_mutation_wal_frontiers() {
        let mut expected = valid_manifest();
        expected.tombstone_frontier = vec![crate::manifest::TombstoneSummary {
            path: "tombstones/ab/delta.parquet".to_string(),
            checksum: "ab".repeat(32),
            count: 2,
            id_bloom: crate::manifest::segment_id_bloom(["a", "b"]),
            created_at: datetime_from_millis(1234).unwrap(),
        }];
        expected.tombstone_id_count = 2;
        expected.tombstone_pages = vec![crate::manifest::TombstonePageRef {
            bucket: 17,
            path: "tombstones/cd/page.parquet".to_string(),
            checksum: "cd".repeat(32),
            count: 9,
            id_bloom: crate::manifest::tombstone_id_bloom(["page-id"]),
            created_at: datetime_from_millis(1235).unwrap(),
        }];
        expected.bm25_stats_delta_frontier = vec![crate::manifest::Bm25StatsDeltaRef {
            document_count_delta: -1,
            total_document_length_delta: -3,
            pages: vec![crate::manifest::Bm25StatsDeltaPageRef {
                first_term: 7,
                last_term: 7,
                path: "lexical/stats-delta/cd/delta.parquet".to_string(),
                checksum: "cd".repeat(32),
                encoded_bytes: 456,
                term_count: 1,
            }],
        }];

        let manifest_bytes = manifest_to_parquet(&expected).unwrap();
        let decoded = manifest_metadata_from_parquet(&manifest_bytes).unwrap();

        assert_eq!(decoded.tombstone_frontier, expected.tombstone_frontier);
        assert_eq!(decoded.tombstone_id_count, expected.tombstone_id_count);
        assert_eq!(decoded.tombstone_pages, expected.tombstone_pages);
        assert_eq!(
            decoded.bm25_stats_delta_frontier,
            expected.bm25_stats_delta_frontier
        );
    }

    #[test]
    fn bm25_statistics_delta_page_round_trips_signed_terms() {
        let expected = vec![(7, -1), (19, -4), (u32::MAX, -2)];
        let bytes = bm25_stats_delta_page_to_parquet(&expected).unwrap();
        assert_eq!(
            bm25_stats_delta_page_from_parquet(&bytes).unwrap(),
            expected
        );
    }

    #[test]
    fn manifest_to_parquet_rejects_invalid_config_dimensions() {
        let mut manifest = valid_manifest();
        manifest.config.dimensions = 0;

        let err = manifest_to_parquet(&manifest).unwrap_err();

        assert!(
            err.to_string()
                .contains("manifest dimensions must be greater than zero"),
            "{err}"
        );
    }

    #[test]
    fn manifest_to_parquet_rejects_invalid_segment_max_vectors() {
        let mut manifest = valid_manifest();
        manifest.config.segment_max_vectors = 0;

        let err = manifest_to_parquet(&manifest).unwrap_err();

        assert!(
            err.to_string()
                .contains("manifest segment_max_vectors must be greater than zero"),
            "{err}"
        );
    }

    #[test]
    fn pivots_to_parquet_rejects_non_finite_vectors() {
        let mut manifest = valid_manifest();
        manifest.pivots = vec![PivotSummary {
            id: "pivot".to_string(),
            ordinal: 0,
            vector: vec![f32::NAN, 0.0],
        }];

        let err = pivots_to_parquet(&manifest).unwrap_err();

        assert!(err.to_string().contains("finite f32 values"), "{err}");
    }

    #[test]
    fn pivots_to_parquet_rejects_vectors_with_wrong_dimensions() {
        let mut manifest = valid_manifest();
        manifest.pivots = vec![PivotSummary {
            id: "pivot".to_string(),
            ordinal: 0,
            vector: vec![0.0],
        }];

        let err = pivots_to_parquet(&manifest).unwrap_err();

        assert!(err.to_string().contains("dimension"), "{err}");
    }

    #[test]
    fn pivots_to_parquet_rejects_empty_pivot_ids() {
        let mut manifest = valid_manifest();
        manifest.pivots = vec![PivotSummary {
            id: String::new(),
            ordinal: 0,
            vector: vec![0.0, 0.0],
        }];

        let err = pivots_to_parquet(&manifest).unwrap_err();

        assert!(
            err.to_string().contains("pivot ids must not be empty"),
            "{err}"
        );
    }

    #[test]
    fn pivots_to_parquet_rejects_duplicate_pivot_ids() {
        let mut manifest = valid_manifest();
        manifest.pivots = vec![
            PivotSummary {
                id: "pivot".to_string(),
                ordinal: 0,
                vector: vec![0.0, 0.0],
            },
            PivotSummary {
                id: "pivot".to_string(),
                ordinal: 1,
                vector: vec![1.0, 0.0],
            },
        ];

        let err = pivots_to_parquet(&manifest).unwrap_err();

        assert!(err.to_string().contains("duplicate pivot id"), "{err}");
    }

    #[test]
    fn pivots_from_parquet_ignores_unknown_columns() {
        let mut manifest = valid_manifest();
        manifest.pivots = vec![PivotSummary {
            id: "pivot".to_string(),
            ordinal: 7,
            vector: vec![1.0, -1.0],
        }];
        let bytes = parquet_with_unknown_column_after_first(&pivots_to_parquet(&manifest).unwrap());

        let pivots = pivots_from_parquet(&bytes, 2, manifest.version).unwrap();

        assert_eq!(pivots.len(), 1);
        assert_eq!(pivots[0].id, "pivot");
        assert_eq!(pivots[0].ordinal, 7);
        assert_eq!(pivots[0].vector, vec![1.0, -1.0]);
    }

    #[test]
    fn routing_to_parquet_rejects_non_finite_centroids() {
        let mut segment = valid_segment_summary();
        segment.centroid = vec![f32::NAN, 0.0];
        let manifest = manifest_with_segment(segment);

        let err = routing_to_parquet(&manifest).unwrap_err();

        assert!(err.to_string().contains("finite f32 values"), "{err}");
    }

    #[test]
    fn routing_to_parquet_rejects_non_finite_radii() {
        let mut segment = valid_segment_summary();
        segment.radius = f32::INFINITY;
        let manifest = manifest_with_segment(segment);

        let err = routing_to_parquet(&manifest).unwrap_err();

        assert!(err.to_string().contains("finite f32 values"), "{err}");
    }

    #[test]
    fn routing_to_parquet_rejects_centroids_with_wrong_dimensions() {
        let mut segment = valid_segment_summary();
        segment.centroid = vec![0.0];
        let manifest = manifest_with_segment(segment);

        let err = routing_to_parquet(&manifest).unwrap_err();

        assert!(err.to_string().contains("dimension"), "{err}");
    }

    #[test]
    fn routing_to_parquet_rejects_segment_dimension_mismatch() {
        let mut segment = valid_segment_summary();
        segment.dimensions = 3;
        let manifest = manifest_with_segment(segment);

        let err = routing_to_parquet(&manifest).unwrap_err();

        assert!(err.to_string().contains("dimension"), "{err}");
    }

    #[test]
    fn routing_to_parquet_rejects_malformed_id_bloom() {
        let mut segment = valid_segment_summary();
        segment.id_bloom = vec![0_u8; 3];
        let manifest = manifest_with_segment(segment);

        let err = routing_to_parquet(&manifest).unwrap_err();

        assert!(err.to_string().contains("id_bloom"), "{err}");
    }

    #[test]
    fn routing_to_parquet_rejects_malformed_vector_signature_bloom() {
        let mut segment = valid_segment_summary();
        segment.vector_signature_bloom = vec![0_u8; 3];
        let manifest = manifest_with_segment(segment);

        let err = routing_to_parquet(&manifest).unwrap_err();

        assert!(err.to_string().contains("vector_signature_bloom"), "{err}");
    }

    #[test]
    fn routing_to_parquet_round_trips_leaf_mode() {
        let mut segment = valid_segment_summary();
        segment.leaf_mode = LeafMode::VamanaPq;
        let manifest = manifest_with_segment(segment);

        let bytes = routing_to_parquet(&manifest).unwrap();
        let summaries = routing_from_parquet(&bytes, manifest.version).unwrap();

        assert_eq!(summaries[0].leaf_mode, LeafMode::VamanaPq);
    }

    #[test]
    fn routing_to_parquet_round_trips_required_segment_layout() {
        let segment = valid_segment_summary();
        let manifest = manifest_with_segment(segment);

        let bytes = routing_to_parquet(&manifest).unwrap();
        let summaries = routing_from_parquet(&bytes, manifest.version).unwrap();

        assert_eq!(
            summaries[0].layout.physical_format,
            crate::PhysicalFormat::Parquet
        );

        let page = routing_layer_page_to_parquet(&manifest, 0, 0, 0, &manifest.segments).unwrap();
        let page_summaries =
            routing_layer_page_from_parquet(&page, manifest.version, 0, 0, 2).unwrap();
        assert_eq!(
            page_summaries[0].layout.physical_format,
            crate::PhysicalFormat::Parquet
        );
    }

    #[test]
    fn routing_rejects_segment_extension_that_disagrees_with_required_format() {
        let mut segment = valid_segment_summary();
        segment.path = "segments/L0/seg.arrow".to_string();
        let manifest = manifest_with_segment(segment);

        let error = routing_to_parquet(&manifest).unwrap_err();

        assert!(error.to_string().contains(".parquet"), "{error}");
    }

    #[test]
    fn routing_to_parquet_round_trips_vector_signature_bloom() {
        let segment = valid_segment_summary();
        let expected = segment.vector_signature_bloom.clone();
        let manifest = manifest_with_segment(segment);

        let bytes = routing_to_parquet(&manifest).unwrap();
        let summaries = routing_from_parquet(&bytes, manifest.version).unwrap();

        assert_eq!(summaries[0].vector_signature_bloom, expected);
    }

    #[test]
    fn routing_to_parquet_round_trips_vector_bounds() {
        let mut segment = valid_segment_summary();
        segment.bounds_min = vec![-1.0, -2.0];
        segment.bounds_max = vec![3.0, 4.0];
        let expected_min = segment.bounds_min.clone();
        let expected_max = segment.bounds_max.clone();
        let manifest = manifest_with_segment(segment);

        let bytes = routing_to_parquet(&manifest).unwrap();
        let summaries = routing_from_parquet(&bytes, manifest.version).unwrap();

        assert_eq!(summaries[0].bounds_min, expected_min);
        assert_eq!(summaries[0].bounds_max, expected_max);
    }

    #[test]
    fn routing_from_parquet_ignores_unknown_columns() {
        let mut segment = valid_segment_summary();
        segment.created_at = datetime_from_millis(1234).unwrap();
        let manifest = manifest_with_segment(segment.clone());
        let bytes =
            parquet_with_unknown_column_after_first(&routing_to_parquet(&manifest).unwrap());

        let summaries = routing_from_parquet(&bytes, manifest.version).unwrap();

        assert_eq!(summaries, vec![segment]);
    }

    #[test]
    fn routing_layer_page_from_parquet_ignores_unknown_columns() {
        let mut segment = valid_segment_summary();
        segment.created_at = datetime_from_millis(1234).unwrap();
        let manifest = manifest_with_segment(segment.clone());
        let bytes = parquet_with_unknown_column_after_first(
            &routing_layer_page_to_parquet(&manifest, 0, 0, 0, &manifest.segments).unwrap(),
        );

        let summaries = routing_layer_page_from_parquet(&bytes, manifest.version, 0, 0, 2).unwrap();

        assert_eq!(summaries, vec![segment]);
    }

    #[test]
    fn routing_layer_page_index_from_parquet_ignores_unknown_columns() {
        let manifest = valid_manifest();
        let page_ref = valid_routing_layer_page_ref();
        let bytes = parquet_with_unknown_column_after_first(
            &routing_layer_page_index_to_parquet(&manifest, 0, std::slice::from_ref(&page_ref))
                .unwrap(),
        );

        let page_refs = routing_layer_page_index_from_parquet(&bytes, manifest.version, 0).unwrap();

        assert_eq!(page_refs, vec![page_ref]);
    }

    #[test]
    fn routing_to_parquet_rejects_invalid_vector_bounds() {
        let mut segment = valid_segment_summary();
        segment.bounds_min = vec![1.0, 0.0];
        segment.bounds_max = vec![0.0, 0.0];
        let manifest = manifest_with_segment(segment);

        let err = routing_to_parquet(&manifest).unwrap_err();

        assert!(err.to_string().contains("min <= max"), "{err}");
    }

    #[test]
    fn routing_to_parquet_rejects_empty_segment_ids() {
        let mut segment = valid_segment_summary();
        segment.id.clear();
        let manifest = manifest_with_segment(segment);

        let err = routing_to_parquet(&manifest).unwrap_err();

        assert!(
            err.to_string()
                .contains("routing segment ids must not be empty"),
            "{err}"
        );
    }

    #[test]
    fn routing_to_parquet_rejects_duplicate_segment_ids() {
        let mut manifest = valid_manifest();
        manifest.segments = vec![valid_segment_summary(), valid_segment_summary()];

        let err = routing_to_parquet(&manifest).unwrap_err();

        assert!(
            err.to_string().contains("duplicate routing segment id"),
            "{err}"
        );
    }

    #[test]
    fn routing_to_parquet_rejects_empty_segment_paths() {
        let mut segment = valid_segment_summary();
        segment.path.clear();
        let manifest = manifest_with_segment(segment);

        let err = routing_to_parquet(&manifest).unwrap_err();

        assert!(
            err.to_string()
                .contains("routing segment paths must not be empty"),
            "{err}"
        );
    }

    #[test]
    fn routing_to_parquet_rejects_duplicate_segment_paths() {
        let mut first = valid_segment_summary();
        let mut second = valid_segment_summary();
        second.id = "seg-b".to_string();
        second.path = first.path.clone();
        second.graph_path = "graphs/L0/seg-b.parquet".to_string();
        first.graph_path = "graphs/L0/seg-a.parquet".to_string();
        let mut manifest = valid_manifest();
        manifest.segments = vec![first, second];

        let err = routing_to_parquet(&manifest).unwrap_err();

        assert!(
            err.to_string().contains("duplicate routing segment path"),
            "{err}"
        );
    }

    #[test]
    fn routing_to_parquet_rejects_partial_empty_graph_paths() {
        // Clearing only the path (leaving a populated checksum/size) is an
        // inconsistent triple and is rejected on write.
        let mut segment = valid_segment_summary();
        segment.graph_path.clear();
        let manifest = manifest_with_segment(segment);

        let err = routing_to_parquet(&manifest).unwrap_err();

        assert!(
            err.to_string()
                .contains("routing graph path must be present when a graph is stored"),
            "{err}"
        );
    }

    #[test]
    fn routing_to_parquet_accepts_absent_graph() {
        // A fully-empty graph triple (graph-free `PqScanOnly` segment) writes
        // without error.
        let mut segment = valid_segment_summary();
        segment.graph_path.clear();
        segment.graph_checksum.clear();
        segment.graph_size_bytes = 0;
        let manifest = manifest_with_segment(segment);

        routing_to_parquet(&manifest).unwrap();
    }

    #[test]
    fn routing_to_parquet_rejects_duplicate_graph_paths() {
        let first = valid_segment_summary();
        let mut second = valid_segment_summary();
        second.id = "seg-b".to_string();
        second.path = "segments/L0/seg-b.parquet".to_string();
        second.graph_path = first.graph_path.clone();
        let mut manifest = valid_manifest();
        manifest.segments = vec![first, second];

        let err = routing_to_parquet(&manifest).unwrap_err();

        assert!(
            err.to_string().contains("duplicate routing graph path"),
            "{err}"
        );
    }

    #[test]
    fn routing_to_parquet_rejects_malformed_segment_checksums() {
        let mut segment = valid_segment_summary();
        segment.checksum = "not-a-blake3-checksum".to_string();
        let manifest = manifest_with_segment(segment);

        let err = routing_to_parquet(&manifest).unwrap_err();

        assert!(
            err.to_string()
                .contains("routing segment checksum must be 64 lowercase hex characters"),
            "{err}"
        );
    }

    #[test]
    fn routing_to_parquet_rejects_malformed_graph_checksums() {
        let mut segment = valid_segment_summary();
        segment.graph_checksum = "not-a-blake3-checksum".to_string();
        let manifest = manifest_with_segment(segment);

        let err = routing_to_parquet(&manifest).unwrap_err();

        assert!(
            err.to_string()
                .contains("routing graph checksum must be 64 lowercase hex characters"),
            "{err}"
        );
    }

    #[test]
    fn routing_to_parquet_rejects_empty_segment_summaries() {
        let mut segment = valid_segment_summary();
        segment.object_count = 0;
        let manifest = manifest_with_segment(segment);

        let err = routing_to_parquet(&manifest).unwrap_err();

        assert!(
            err.to_string()
                .contains("routing segment object_count must be greater than zero"),
            "{err}"
        );
    }

    #[test]
    fn routing_to_parquet_rejects_zero_segment_sizes() {
        let mut segment = valid_segment_summary();
        segment.size_bytes = 0;
        let manifest = manifest_with_segment(segment);

        let err = routing_to_parquet(&manifest).unwrap_err();

        assert!(
            err.to_string()
                .contains("routing segment size_bytes must be greater than zero"),
            "{err}"
        );
    }

    #[test]
    fn routing_to_parquet_rejects_zero_graph_sizes() {
        let mut segment = valid_segment_summary();
        segment.graph_size_bytes = 0;
        let manifest = manifest_with_segment(segment);

        let err = routing_to_parquet(&manifest).unwrap_err();

        assert!(
            err.to_string()
                .contains("routing graph size_bytes must be greater than zero"),
            "{err}"
        );
    }

    #[test]
    fn segment_to_parquet_rejects_non_finite_record_vectors() {
        let mut segment = valid_segment();
        segment.records[0].vector = vec![f32::NAN, 0.0];

        let err = segment_to_parquet(&segment).unwrap_err();

        assert!(err.to_string().contains("finite f32 values"), "{err}");
    }

    #[test]
    fn segment_to_parquet_rejects_centroids_with_wrong_dimensions() {
        let mut segment = valid_segment();
        segment.centroid = vec![0.0];

        let err = segment_to_parquet(&segment).unwrap_err();

        assert!(err.to_string().contains("dimension"), "{err}");
    }

    #[test]
    fn segment_to_parquet_rejects_record_vectors_with_wrong_dimensions() {
        let mut segment = valid_segment();
        segment.records[0].vector = vec![0.0];

        let err = segment_to_parquet(&segment).unwrap_err();

        assert!(err.to_string().contains("dimension"), "{err}");
    }

    #[test]
    fn segment_to_parquet_rejects_empty_record_ids() {
        let mut segment = valid_segment();
        segment.records[0].id.clear();

        let err = segment_to_parquet(&segment).unwrap_err();

        assert!(
            err.to_string().contains("record ids must not be empty"),
            "{err}"
        );
    }

    #[test]
    fn segment_to_parquet_rejects_duplicate_record_ids() {
        let mut segment = valid_segment();
        segment.records.push(VectorRecord {
            id: "record".into(),
            vector: vec![1.0, 0.0],
            extra_vectors: BTreeMap::new(),
            extra_sparse: BTreeMap::new(),
            extra_multi_vectors: BTreeMap::new(),
            storage: crate::StorageEncoding::Auto,
            text: None,
            text_term_ids: Vec::new(),
            text_term_freqs: Vec::new(),
            metadata: crate::Metadata::new(),
            generation: 0,
            mutation_stamp: None,
        });
        segment.routing_codes.push(1.0);
        segment.pq_codes.push(vec![255, 128]);

        let err = segment_to_parquet(&segment).unwrap_err();

        assert!(err.to_string().contains("duplicate record id"), "{err}");
    }

    #[test]
    fn segment_to_parquet_rejects_routing_code_count_mismatch() {
        let mut segment = valid_segment();
        segment.routing_codes.push(1.0);

        let err = segment_to_parquet(&segment).unwrap_err();

        assert!(err.to_string().contains("routing code count"), "{err}");
    }

    #[test]
    fn segment_to_parquet_rejects_non_finite_centroids() {
        let mut segment = valid_segment();
        segment.centroid = vec![f32::NAN, 0.0];

        let err = segment_to_parquet(&segment).unwrap_err();

        assert!(err.to_string().contains("finite f32 values"), "{err}");
    }

    #[test]
    fn segment_to_parquet_rejects_non_finite_radii() {
        let mut segment = valid_segment();
        segment.radius = f32::INFINITY;

        let err = segment_to_parquet(&segment).unwrap_err();

        assert!(err.to_string().contains("finite f32 values"), "{err}");
    }

    #[test]
    fn segment_to_parquet_rejects_non_finite_routing_codes() {
        let mut segment = valid_segment();
        segment.routing_codes[0] = f32::NAN;

        let err = segment_to_parquet(&segment).unwrap_err();

        assert!(err.to_string().contains("finite f32 values"), "{err}");
    }

    #[test]
    fn graph_to_parquet_rejects_non_finite_edge_distances() {
        let graph = SegmentGraph {
            segment_id: "seg".to_string(),
            level: 0,
            edges: vec![GraphEdge {
                source_record_index: 0,
                neighbor_record_index: 1,
                distance: f32::NAN,
            }],
            adjacency_offsets: Vec::new(),
            created_at: Utc::now(),
        };

        let err = graph_to_parquet(&graph).unwrap_err();

        assert!(err.to_string().contains("finite f32 values"), "{err}");
    }

    #[test]
    fn graph_to_parquet_writes_numeric_record_indices() {
        let segment = Segment::from_records(
            "seg".to_string(),
            0,
            VectorMetric::Euclidean,
            2,
            vec![
                VectorRecord::new("long-user-id-a", vec![0.0, 0.0]),
                VectorRecord::new("long-user-id-b", vec![1.0, 0.0]),
            ],
        )
        .unwrap();
        let graph = SegmentGraph::from_segment(&segment, 1).unwrap();

        let bytes = graph_to_parquet(&graph).unwrap();
        let batch = first_batch(&bytes, "graph").unwrap();
        let schema = batch.schema();

        assert_eq!(
            schema
                .field_with_name("source_record_index")
                .unwrap()
                .data_type(),
            &DataType::UInt64
        );
        assert_eq!(
            schema
                .field_with_name("neighbor_record_index")
                .unwrap()
                .data_type(),
            &DataType::UInt64
        );
        assert!(
            schema.field_with_name("source_record_id").is_err(),
            "new graph blocks must not repeat external ids per edge"
        );
        assert!(
            schema.field_with_name("neighbor_record_id").is_err(),
            "new graph blocks must not repeat external ids per edge"
        );
    }

    fn valid_manifest() -> Manifest {
        Manifest {
            version: 1,
            config: IndexConfig {
                uri: "file:///tmp/borsuk-test".to_string(),
                metric: VectorMetric::Euclidean,
                dimensions: 2,
                segment_max_vectors: 100,
                ram_budget_bytes: None,
                text: false,
                named_vectors: Default::default(),
            },
            text_tokenizer: None,
            segments: Vec::new(),
            pivots: Vec::new(),
            next_generated_id: 0,
            routing_max_level: 0,
            routing_page_fanout: DEFAULT_ROUTING_PAGE_FANOUT,
            graph_neighbors: DEFAULT_GRAPH_NEIGHBORS,
            leaf_capability: crate::LeafCapability::default(),
            build_config: crate::BuildConfig::default(),
            tombstone: None,
            tombstone_frontier: Vec::new(),
            tombstone_pages: Vec::new(),
            tombstone_id_count: 0,
            wal_config: crate::manifest::WalConfig::default(),
            routing_epoch: 1,
            cell_wal_config: crate::CellWalConfig::default(),
            logical_cell_catalog_ref: None,
            logical_cell_routing_strategy: crate::centroid_hnsw::CatalogRoutingStrategy::Flat,
            logical_cell_catalog: None,
            logical_cell_router: None,
            cell_wal_consumed_runs: BTreeSet::new(),
            cell_wal_visible_runs: 0,
            cell_wal_visible_tombstone_runs: 0,
            quantizer_ref: None,
            global_ann_ref: None,
            global_cell_card_ann_ref: None,
            lexical_roots: Vec::new(),
            bm25_stats_delta: None,
            bm25_stats_delta_frontier: Vec::new(),
            created_at: Utc::now(),
        }
    }

    #[test]
    fn manifest_and_logical_cell_catalog_versions_are_independently_pinned() {
        let bytes = manifest_to_parquet(&valid_manifest()).unwrap();
        let batch = first_batch(&bytes, "manifest").unwrap();

        assert_eq!(
            primitive_value_by_name::<UInt16Type>(&batch, 0, "format_version").unwrap(),
            CURRENT_VERSION
        );
        assert_eq!(
            crate::logical_cell_catalog::LOGICAL_CELL_CATALOG_FORMAT_VERSION,
            34
        );
    }

    #[test]
    fn manifest_persists_only_the_bounded_logical_cell_catalog_reference() {
        let mut manifest = valid_manifest();
        let catalog = Arc::new(
            crate::logical_cell_catalog::LogicalCellCatalog::from_centroids(
                1,
                2,
                VectorMetric::Euclidean,
                vec![vec![1.0, 2.0], vec![3.0, 4.0]],
            )
            .unwrap(),
        );
        let catalog_bytes =
            crate::logical_cell_catalog::logical_cell_catalog_to_parquet(&catalog).unwrap();
        let reference = crate::logical_cell_catalog::LogicalCellCatalogRef::new(
            blake3::hash(&catalog_bytes).to_hex().to_string(),
            1,
            2,
            2,
            catalog_bytes.len(),
        )
        .unwrap();
        manifest.logical_cell_catalog_ref = Some(reference.clone());
        manifest.logical_cell_catalog = Some(catalog);

        let manifest_bytes = manifest_to_parquet(&manifest).unwrap();
        let routing_bytes = routing_to_parquet(&manifest).unwrap();
        let decoded = manifest_from_parquet(&manifest_bytes, &routing_bytes).unwrap();
        let batch = first_batch(&manifest_bytes, "manifest").unwrap();
        let wal_json = string_value_by_name(&batch, 0, "wal_json").unwrap();

        assert_eq!(decoded.logical_cell_catalog_ref, Some(reference));
        assert!(decoded.logical_cell_catalog.is_none());
        assert!(!wal_json.contains("logical_cells"), "{wal_json}");
        assert!(!wal_json.contains("logical_cell_centroids"), "{wal_json}");
        assert!(wal_json.contains("logical_cell_catalog_ref"), "{wal_json}");
    }

    fn manifest_with_segment(segment: SegmentSummary) -> Manifest {
        let mut manifest = valid_manifest();
        manifest.segments = vec![segment];
        manifest
    }

    #[test]
    fn metadata_round_trips_through_vector_records() {
        use crate::metadata::MetaValue;
        let meta = crate::Metadata::from([
            ("year".to_string(), MetaValue::Int(2021)),
            ("genre".to_string(), MetaValue::Str("comedy".to_string())),
            (
                "tags".to_string(),
                MetaValue::List(vec![MetaValue::Str("a".to_string())]),
            ),
        ]);
        let records = vec![
            VectorRecord::new("a", vec![1.0, 0.0]).with_metadata(meta.clone()),
            VectorRecord::new("b", vec![0.0, 1.0]),
        ];
        let bytes = vector_records_to_parquet(&records, 2).unwrap();
        let decoded = vector_records_from_parquet(&bytes, 2).unwrap();
        assert_eq!(decoded[0].metadata, meta);
        assert!(decoded[1].metadata.is_empty());
    }

    #[test]
    fn metadata_round_trips_through_segment() {
        use crate::metadata::MetaValue;
        let mut segment = valid_segment();
        segment.records[0].metadata = crate::Metadata::from([("k".to_string(), MetaValue::Int(7))]);
        let bytes = segment_to_parquet(&segment).unwrap();
        let decoded = segment_from_parquet(&bytes).unwrap();
        assert_eq!(
            decoded.records[0].metadata,
            crate::Metadata::from([("k".to_string(), MetaValue::Int(7))])
        );
    }

    #[test]
    fn wal_round_trips_named_payloads_and_forced_storage() {
        let mut segment = valid_segment();
        segment.records[0]
            .extra_vectors
            .insert("title".to_string(), vec![0.25, 0.75]);
        segment.records[0].extra_sparse.insert(
            "terms".to_string(),
            crate::SparseVector::new(vec![2, 9], vec![1.5, 0.5]).unwrap(),
        );
        segment.records[0].storage = crate::StorageEncoding::Dense;

        let bytes = wal_records_to_table(
            &segment.records,
            segment.dimensions,
            VectorElementType::Float32,
            crate::PhysicalFormat::Parquet,
        )
        .unwrap();
        let decoded = wal_records_from_table(bytes, "wal/run.parquet").unwrap();

        assert_eq!(decoded[0].extra_vectors, segment.records[0].extra_vectors);
        assert_eq!(decoded[0].extra_sparse, segment.records[0].extra_sparse);
        assert_eq!(decoded[0].storage, crate::StorageEncoding::Dense);
    }

    #[test]
    fn wal_record_codec_does_not_require_segment_derivatives() {
        let records = vec![VectorRecord::new("record", vec![0.25, 0.75])];
        let bytes = wal_records_to_table(
            &records,
            2,
            VectorElementType::Float32,
            crate::PhysicalFormat::Parquet,
        )
        .unwrap();
        let decoded = wal_records_from_table(bytes, "wal/run.parquet").unwrap();
        assert_eq!(decoded, records);
    }

    #[test]
    fn mutation_stamp_round_trips_through_wal_and_materialized_segment_tables() {
        let version = crate::mutation::MutationVersion::from_parts(0x1234_5678, [0x5a; 16]);
        let stamped = crate::mutation::CanonicalMutation::put(
            version,
            VectorRecord::new("stamped", vec![0.25, 0.75]),
        )
        .unwrap()
        .record()
        .unwrap()
        .clone();
        let expected_stamp = stamped.mutation_stamp().unwrap();

        let bytes = wal_records_to_table(
            std::slice::from_ref(&stamped),
            2,
            VectorElementType::Float32,
            crate::PhysicalFormat::Parquet,
        )
        .unwrap();
        let decoded = wal_records_from_table(bytes, "wal/run.parquet").unwrap();
        assert_eq!(decoded[0].mutation_stamp(), Some(expected_stamp));

        let mut segment = valid_segment();
        segment.records[0] = stamped;
        let decoded = segment_from_parquet(&segment_to_parquet(&segment).unwrap()).unwrap();
        assert_eq!(decoded.records[0].mutation_stamp(), Some(expected_stamp));
    }

    #[test]
    fn parquet_wal_uses_record_only_schema() {
        let mut segment = valid_segment();
        segment.records[0].storage = crate::StorageEncoding::Dense;
        let bytes = wal_records_to_table(
            &segment.records,
            segment.dimensions,
            VectorElementType::Float32,
            crate::PhysicalFormat::Parquet,
        )
        .unwrap();
        let batch = first_batch(&bytes, "WAL").unwrap();
        let schema = batch.schema();
        let names = schema
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect::<Vec<_>>();
        assert_eq!(
            names,
            [
                "record_id",
                "metadata",
                "vector",
                "wal_record_extras",
                "wal_vector_element_type",
                "wal_vector_dimensions",
            ]
        );
        assert!(!names.contains(&"segment_header"));
        assert!(!names.contains(&"routing_code"));
        assert!(!names.contains(&"pq_code"));
    }

    #[test]
    fn parquet_wal_round_trips_every_primary_type_and_payload() {
        use crate::metadata::MetaValue;

        let format = crate::PhysicalFormat::Parquet;
        for element_type in [
            VectorElementType::Float32,
            VectorElementType::Float16,
            VectorElementType::BFloat16,
            VectorElementType::Float8E4M3Fn,
            VectorElementType::Float8E5M2,
            VectorElementType::Int8,
            VectorElementType::Binary,
        ] {
            let mut segment = valid_segment();
            let record = &mut segment.records[0];
            record.vector = vec![1.0, 0.0];
            record
                .extra_vectors
                .insert("dense".to_string(), vec![0.25, -0.75]);
            record.extra_sparse.insert(
                "sparse".to_string(),
                crate::SparseVector::new(vec![3, 17], vec![1.5, -0.5]).unwrap(),
            );
            record.extra_multi_vectors.insert(
                "tokens".to_string(),
                crate::LateInteractionVector::new(
                    vec![vec![0.25, 0.5], vec![-0.75, 1.0]],
                    VectorElementType::Float16,
                )
                .unwrap(),
            );
            record.storage = crate::StorageEncoding::Dense;
            record.text_term_ids = vec![7, 11];
            record.text_term_freqs = vec![2, 1];
            record.metadata = crate::Metadata::from([
                ("tenant".to_string(), MetaValue::Str("alpha".to_string())),
                ("rank".to_string(), MetaValue::Int(42)),
            ]);
            record.generation = 9;

            let bytes =
                wal_records_to_table(&segment.records, segment.dimensions, element_type, format)
                    .unwrap();
            let path = format!("wal/run.{}", format.extension());
            let decoded = wal_records_from_table(bytes, &path)
                .unwrap_or_else(|error| panic!("{format}/{element_type}: {error}"));
            let actual = &decoded[0];
            let expected = &segment.records[0];

            assert_eq!(
                actual.vector, expected.vector,
                "{format}/{element_type} primary vector"
            );
            assert_eq!(
                actual.extra_vectors, expected.extra_vectors,
                "{format}/{element_type} named dense"
            );
            assert_eq!(
                actual.extra_sparse, expected.extra_sparse,
                "{format}/{element_type} named sparse"
            );
            assert_eq!(
                actual.extra_multi_vectors, expected.extra_multi_vectors,
                "{format}/{element_type} late interaction"
            );
            assert_eq!(
                actual.storage, expected.storage,
                "{format}/{element_type} storage"
            );
            assert_eq!(
                actual.text_term_ids, expected.text_term_ids,
                "{format}/{element_type} text term ids"
            );
            assert_eq!(
                actual.text_term_freqs, expected.text_term_freqs,
                "{format}/{element_type} text term frequencies"
            );
            assert_eq!(
                actual.metadata, expected.metadata,
                "{format}/{element_type} metadata"
            );
            assert_eq!(
                actual.generation, expected.generation,
                "{format}/{element_type} generation"
            );
        }
    }

    #[test]
    fn parquet_binary_wal_preserves_non_byte_aligned_dimensions() {
        let segment = Segment::from_records(
            "wal".to_string(),
            0,
            VectorMetric::Hamming,
            10,
            vec![VectorRecord::new(
                "record",
                vec![1.0, 0.0, 1.0, 0.0, 1.0, 0.0, 1.0, 0.0, 1.0, 0.0],
            )],
        )
        .unwrap();
        let bytes = wal_records_to_table(
            &segment.records,
            segment.dimensions,
            VectorElementType::Binary,
            crate::PhysicalFormat::Parquet,
        )
        .unwrap();
        let decoded = wal_records_from_table(bytes, "wal/run.parquet").unwrap();
        assert_eq!(decoded[0].vector, segment.records[0].vector);
    }

    #[test]
    fn wal_primary_vector_uses_the_declared_arrow_physical_type() {
        for (element_type, expected) in [
            (
                VectorElementType::Float32,
                DataType::FixedSizeList(
                    Arc::new(Field::new_list_field(DataType::Float32, true)),
                    2,
                ),
            ),
            (
                VectorElementType::Float16,
                DataType::FixedSizeList(
                    Arc::new(Field::new_list_field(DataType::Float16, true)),
                    2,
                ),
            ),
            (
                VectorElementType::BFloat16,
                DataType::FixedSizeList(Arc::new(Field::new_list_field(DataType::UInt16, true)), 2),
            ),
            (
                VectorElementType::Float8E4M3Fn,
                DataType::FixedSizeList(Arc::new(Field::new_list_field(DataType::UInt8, true)), 2),
            ),
            (
                VectorElementType::Float8E5M2,
                DataType::FixedSizeList(Arc::new(Field::new_list_field(DataType::UInt8, true)), 2),
            ),
            (
                VectorElementType::Int8,
                DataType::FixedSizeList(Arc::new(Field::new_list_field(DataType::Int8, true)), 2),
            ),
            (
                VectorElementType::Binary,
                DataType::FixedSizeList(Arc::new(Field::new_list_field(DataType::UInt8, true)), 1),
            ),
        ] {
            let segment = valid_segment();
            let bytes = wal_records_to_table(
                &segment.records,
                segment.dimensions,
                element_type,
                crate::PhysicalFormat::Parquet,
            )
            .unwrap();
            let builder =
                ParquetRecordBatchReaderBuilder::try_new(Bytes::copy_from_slice(&bytes)).unwrap();
            assert_eq!(
                builder
                    .schema()
                    .field_with_name("vector")
                    .unwrap()
                    .data_type(),
                &expected
            );
            let decoded = wal_records_from_table(bytes, "wal/run.parquet").unwrap();
            assert_eq!(decoded[0].vector, segment.records[0].vector);
        }
    }

    #[test]
    fn wal_reader_rejects_the_pre_type_column_schema() {
        let segment = valid_segment();
        let bytes = wal_records_to_table(
            &segment.records,
            segment.dimensions,
            VectorElementType::Float16,
            crate::PhysicalFormat::Parquet,
        )
        .unwrap();
        let batch = first_batch(&bytes, "WAL").unwrap();
        let type_column = batch.schema().index_of("wal_vector_element_type").unwrap();
        let fields = batch
            .schema()
            .fields()
            .iter()
            .enumerate()
            .filter(|(index, _)| *index != type_column)
            .map(|(_, field)| field.as_ref().clone())
            .collect::<Vec<_>>();
        let columns = batch
            .columns()
            .iter()
            .enumerate()
            .filter(|(index, _)| *index != type_column)
            .map(|(_, column)| Arc::clone(column))
            .collect::<Vec<_>>();
        let legacy = RecordBatch::try_new(
            Arc::new(Schema::new_with_metadata(
                fields,
                batch.schema().metadata().clone(),
            )),
            columns,
        )
        .unwrap();
        let legacy = write_batch(legacy).unwrap();

        let error = wal_records_from_table(legacy, "wal/run.parquet").unwrap_err();
        assert!(
            error
                .to_string()
                .contains("missing `wal_vector_element_type`"),
            "{error}"
        );
    }

    #[test]
    fn wal_reader_rejects_a_missing_dimensions_column() {
        let segment = valid_segment();
        let bytes = wal_records_to_table(
            &segment.records,
            segment.dimensions,
            VectorElementType::Float32,
            crate::PhysicalFormat::Parquet,
        )
        .unwrap();
        let batch = first_batch(&bytes, "WAL").unwrap();
        let dimensions_column = batch.schema().index_of("wal_vector_dimensions").unwrap();
        let fields = batch
            .schema()
            .fields()
            .iter()
            .enumerate()
            .filter(|(index, _)| *index != dimensions_column)
            .map(|(_, field)| field.as_ref().clone())
            .collect::<Vec<_>>();
        let columns = batch
            .columns()
            .iter()
            .enumerate()
            .filter(|(index, _)| *index != dimensions_column)
            .map(|(_, column)| Arc::clone(column))
            .collect::<Vec<_>>();
        let malformed = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap();

        let error =
            wal_records_from_table(write_batch(malformed).unwrap(), "wal/run.parquet").unwrap_err();
        assert!(
            error.to_string().contains("`wal_vector_dimensions`"),
            "{error}"
        );
    }

    #[test]
    fn wal_reader_rejects_inconsistent_dimensions() {
        let segment = Segment::from_records(
            "wal".to_string(),
            0,
            VectorMetric::Euclidean,
            2,
            vec![
                VectorRecord::new("a", vec![1.0, 0.0]),
                VectorRecord::new("b", vec![0.0, 1.0]),
            ],
        )
        .unwrap();
        let bytes = wal_records_to_table(
            &segment.records,
            segment.dimensions,
            VectorElementType::Float32,
            crate::PhysicalFormat::Parquet,
        )
        .unwrap();
        let batch = first_batch(&bytes, "WAL").unwrap();
        let dimensions_column = batch.schema().index_of("wal_vector_dimensions").unwrap();
        let mut columns = batch.columns().to_vec();
        columns[dimensions_column] = array(UInt32Array::from_iter_values([2, 3]));
        let malformed = RecordBatch::try_new(batch.schema(), columns).unwrap();

        let error =
            wal_records_from_table(write_batch(malformed).unwrap(), "wal/run.parquet").unwrap_err();
        assert!(
            error
                .to_string()
                .contains("inconsistent `wal_vector_dimensions`"),
            "{error}"
        );
    }

    #[test]
    fn wal_reader_rejects_an_unpaired_sparse_column() {
        let segment = valid_segment();
        let bytes = wal_records_to_table(
            &segment.records,
            segment.dimensions,
            VectorElementType::Float32,
            crate::PhysicalFormat::Parquet,
        )
        .unwrap();
        let batch = first_batch(&bytes, "WAL").unwrap();
        let sparse_values_column = batch.schema().index_of("sparse_values").unwrap();
        let fields = batch
            .schema()
            .fields()
            .iter()
            .enumerate()
            .filter(|(index, _)| *index != sparse_values_column)
            .map(|(_, field)| field.as_ref().clone())
            .collect::<Vec<_>>();
        let columns = batch
            .columns()
            .iter()
            .enumerate()
            .filter(|(index, _)| *index != sparse_values_column)
            .map(|(_, column)| Arc::clone(column))
            .collect::<Vec<_>>();
        let malformed = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap();

        let error =
            wal_records_from_table(write_batch(malformed).unwrap(), "wal/run.parquet").unwrap_err();
        assert!(
            error
                .to_string()
                .contains("both sparse_indices and sparse_values"),
            "{error}"
        );
    }

    fn valid_segment_summary() -> SegmentSummary {
        SegmentSummary {
            id: "seg".to_string(),
            level: 0,
            path: "segments/L0/seg.parquet".to_string(),
            layout: crate::PhysicalLayoutRef {
                object_role: crate::PhysicalObjectRole::NormalSegment,
                physical_format: crate::PhysicalFormat::Parquet,
                layout_policy_version: crate::CURRENT_LAYOUT_POLICY_VERSION,
            },
            object_count: 1,
            dimensions: 2,
            centroid: vec![0.0, 0.0],
            radius: 0.0,
            bounds_min: vec![0.0, 0.0],
            bounds_max: vec![0.0, 0.0],
            checksum: VALID_SEGMENT_CHECKSUM.to_string(),
            size_bytes: 123,
            vector_size_bytes: 67,
            graph_path: "graphs/L0/seg.parquet".to_string(),
            graph_checksum: VALID_GRAPH_CHECKSUM.to_string(),
            graph_size_bytes: 45,
            leaf_mode: LeafMode::Graph,
            id_bloom: crate::manifest::segment_id_bloom(["record"]),
            vector_signature_bloom: valid_vector_signature_bloom(),
            metadata_stats: crate::MetadataStats::default(),
            sparse_encoded: 0,
            dense_encoded: 1,
            text_doc_count: 0,
            text_total_doc_length: 0,
            text_lexical_decoded_bytes: 0,
            sparse_lexical_max_decoded_bytes: 0,
            lexical_shards: Vec::new(),
            created_at: Utc::now(),
        }
    }

    fn valid_vector_signature_bloom() -> Vec<u8> {
        let vector = [0.0_f32, 0.0_f32];
        crate::manifest::segment_vector_signature_bloom([vector.as_slice()])
    }

    fn valid_routing_layer_page_ref() -> RoutingLayerPageRef {
        RoutingLayerPageRef {
            routing_level: 0,
            page_ordinal: 0,
            path: format!("routing/pages/L0/00/page-{VALID_SEGMENT_CHECKSUM}.parquet"),
            checksum: VALID_SEGMENT_CHECKSUM.to_string(),
            page_segments: 1,
            leaf_segments: 1,
            leaf_pages: 1,
            routing_pages: 1,
            dimensions: 2,
            centroid: vec![0.0, 0.0],
            radius: 0.0,
            bounds_min: vec![0.0, 0.0],
            bounds_max: vec![0.0, 0.0],
            id_bloom: crate::manifest::segment_id_bloom(["record"]),
            vector_signature_bloom: valid_vector_signature_bloom(),
            level_mask: 1,
            page_records: 1,
            page_segment_bytes: 123,
            page_vector_bytes: 67,
            page_graph_bytes: 45,
            page_sparse_encoded_vectors: 0,
            page_dense_encoded_vectors: 1,
        }
    }

    fn parquet_with_unknown_column_after_first(bytes: &[u8]) -> Vec<u8> {
        let batch = first_batch(bytes, "table").unwrap();
        let mut fields = batch
            .schema()
            .fields()
            .iter()
            .map(|field| field.as_ref().clone())
            .collect::<Vec<_>>();
        fields.insert(1, Field::new("future_column", DataType::Utf8, false));
        let mut columns = batch.columns().to_vec();
        columns.insert(
            1,
            array(StringArray::from_iter_values(
                (0..batch.num_rows()).map(|_| "ignored"),
            )),
        );
        let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap();
        write_batch(batch).unwrap()
    }

    fn valid_segment() -> Segment {
        Segment {
            id: "seg".to_string(),
            level: 0,
            metric: VectorMetric::Euclidean,
            dimensions: 2,
            centroid: vec![0.0, 0.0],
            radius: 0.0,
            records: vec![VectorRecord {
                id: "record".into(),
                vector: vec![0.0, 0.0],
                extra_vectors: BTreeMap::new(),
                extra_sparse: BTreeMap::new(),
                extra_multi_vectors: BTreeMap::new(),
                storage: crate::StorageEncoding::Auto,
                text: None,
                text_term_ids: Vec::new(),
                text_term_freqs: Vec::new(),
                metadata: crate::Metadata::new(),
                generation: 0,
                mutation_stamp: None,
            }],
            routing_codes: vec![0.0],
            pq_codes: vec![vec![128, 128]],
            pq_min: vec![0.0, 0.0],
            pq_max: vec![0.0, 0.0],
            created_at: Utc::now(),
        }
    }

    fn external_manifest_parquet(dimensions: u64, segment_max_vectors: u64) -> Vec<u8> {
        let schema = manifest_schema();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                array(UInt16Array::from_iter_values([CURRENT_VERSION])),
                array(UInt64Array::from_iter_values([1])),
                array(StringArray::from_iter_values(["file:///tmp/borsuk-test"])),
                array(StringArray::from_iter_values(["euclidean"])),
                array(UInt64Array::from_iter_values([dimensions])),
                array(UInt64Array::from_iter_values([segment_max_vectors])),
                array(Int64Array::from_iter_values([0])),
                array(UInt64Array::from_iter([None::<u64>])),
                array(BooleanArray::from_iter([false])),
                array(StringArray::from_iter([None::<String>])),
                array(UInt64Array::from_iter_values([0])),
                array(UInt8Array::from_iter_values([0])),
                array(UInt64Array::from_iter_values([
                    DEFAULT_ROUTING_PAGE_FANOUT as u64
                ])),
                array(UInt64Array::from_iter_values([
                    DEFAULT_GRAPH_NEIGHBORS as u64
                ])),
                array(StringArray::from_iter([None::<String>])),
                array(StringArray::from_iter_values([serde_json::to_string(
                    &crate::centroid_hnsw::CatalogRoutingStrategy::Flat,
                )
                .unwrap()])),
                array(StringArray::from_iter([None::<String>])),
                array(StringArray::from_iter([None::<String>])),
                array(UInt64Array::from_iter([None::<u64>])),
                array(BinaryArray::from_iter([None::<&[u8]>])),
                array(Int64Array::from_iter([None::<i64>])),
                array(StringArray::from_iter([None::<String>])),
                array(StringArray::from_iter([None::<String>])),
                array(StringArray::from_iter_values(["[]"])),
                array(StringArray::from_iter([None::<String>])),
            ],
        )
        .unwrap();

        write_batch(batch).unwrap()
    }

    fn legacy_external_manifest_parquet_without_routing_page_fanout(
        dimensions: u64,
        segment_max_vectors: u64,
    ) -> Vec<u8> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("format_version", DataType::UInt16, false),
            Field::new("version", DataType::UInt64, false),
            Field::new("uri", DataType::Utf8, false),
            Field::new("metric", DataType::Utf8, false),
            Field::new("dimensions", DataType::UInt64, false),
            Field::new("segment_max_vectors", DataType::UInt64, false),
            Field::new("created_at_ms", DataType::Int64, false),
            Field::new("ram_budget_bytes", DataType::UInt64, true),
            Field::new("next_generated_id", DataType::UInt64, false),
            Field::new("routing_max_level", DataType::UInt8, false),
            Field::new("logical_cell_routing_strategy_json", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                array(UInt16Array::from_iter_values([CURRENT_VERSION])),
                array(UInt64Array::from_iter_values([1])),
                array(StringArray::from_iter_values(["file:///tmp/borsuk-test"])),
                array(StringArray::from_iter_values(["euclidean"])),
                array(UInt64Array::from_iter_values([dimensions])),
                array(UInt64Array::from_iter_values([segment_max_vectors])),
                array(Int64Array::from_iter_values([0])),
                array(UInt64Array::from_iter([None::<u64>])),
                array(UInt64Array::from_iter_values([0])),
                array(UInt8Array::from_iter_values([0])),
                array(StringArray::from_iter_values([serde_json::to_string(
                    &crate::centroid_hnsw::CatalogRoutingStrategy::Flat,
                )
                .unwrap()])),
            ],
        )
        .unwrap();

        write_batch(batch).unwrap()
    }

    fn external_routing_parquet(centroid: [f32; 2], radius: f32) -> Vec<u8> {
        external_routing_parquet_with_dimensions(centroid, radius, 2)
    }

    fn external_routing_parquet_with_dimensions(
        centroid: [f32; 2],
        radius: f32,
        stored_dimensions: u64,
    ) -> Vec<u8> {
        external_routing_parquet_with_vector(centroid.to_vec(), radius, stored_dimensions)
    }

    fn external_routing_parquet_with_vector(
        centroid: Vec<f32>,
        radius: f32,
        stored_dimensions: u64,
    ) -> Vec<u8> {
        external_routing_parquet_with_vector_and_id_bloom(
            centroid,
            radius,
            stored_dimensions,
            crate::manifest::segment_id_bloom(["record"]),
        )
    }

    fn external_routing_parquet_with_id_bloom(id_bloom: Vec<u8>) -> Vec<u8> {
        external_routing_parquet_with_vector_and_id_bloom(vec![0.0, 0.0], 0.0, 2, id_bloom)
    }

    fn external_routing_parquet_with_vector_signature_bloom(
        vector_signature_bloom: Vec<u8>,
    ) -> Vec<u8> {
        let mut metadata = valid_external_routing_summary_metadata();
        metadata.vector_signature_bloom = &vector_signature_bloom;
        external_routing_parquet_with_rows_and_summary_metadata(
            &["seg"],
            &["segments/seg.parquet"],
            &["segments/seg.graph.parquet"],
            &[metadata],
        )
    }

    fn external_routing_parquet_with_leaf_mode(leaf_mode: &str) -> Vec<u8> {
        let mut metadata = valid_external_routing_summary_metadata();
        metadata.leaf_mode = leaf_mode;
        external_routing_parquet_with_rows_and_summary_metadata(
            &["seg"],
            &["segments/seg.parquet"],
            &["segments/seg.graph.parquet"],
            &[metadata],
        )
    }

    fn external_routing_parquet_with_segment_ids<const N: usize>(ids: [&str; N]) -> Vec<u8> {
        let paths = vec!["segments/seg.parquet"; N];
        let graph_paths = vec!["segments/seg.graph.parquet"; N];
        external_routing_parquet_with_rows(&ids, &paths, &graph_paths)
    }

    fn external_routing_parquet_with_paths<const N: usize>(
        paths: [&str; N],
        graph_paths: [&str; N],
    ) -> Vec<u8> {
        let ids = (0..N)
            .map(|index| format!("seg-{index}"))
            .collect::<Vec<_>>();
        let ids = ids.iter().map(String::as_str).collect::<Vec<_>>();
        external_routing_parquet_with_rows(&ids, &paths, &graph_paths)
    }

    fn external_routing_parquet_with_rows(
        ids: &[&str],
        paths: &[&str],
        graph_paths: &[&str],
    ) -> Vec<u8> {
        external_routing_parquet_with_rows_and_summary_metadata(
            ids,
            paths,
            graph_paths,
            &vec![valid_external_routing_summary_metadata(); ids.len()],
        )
    }

    fn external_routing_parquet_with_summary_metadata(
        object_count: u64,
        checksum: &str,
        size_bytes: u64,
        graph_checksum: &str,
        graph_size_bytes: u64,
    ) -> Vec<u8> {
        let mut row = valid_external_routing_summary_metadata();
        row.object_count = object_count;
        row.checksum = checksum;
        row.size_bytes = size_bytes;
        row.graph_checksum = graph_checksum;
        row.graph_size_bytes = graph_size_bytes;
        let metadata = [row];
        external_routing_parquet_with_rows_and_summary_metadata(
            &["seg"],
            &["segments/seg.parquet"],
            &["segments/seg.graph.parquet"],
            &metadata,
        )
    }

    #[derive(Clone, Copy)]
    struct ExternalRoutingSummaryMetadata<'a> {
        object_count: u64,
        checksum: &'a str,
        size_bytes: u64,
        graph_checksum: &'a str,
        graph_size_bytes: u64,
        leaf_mode: &'a str,
        vector_signature_bloom: &'a [u8],
    }

    fn valid_external_routing_summary_metadata() -> ExternalRoutingSummaryMetadata<'static> {
        static VECTOR_SIGNATURE_BLOOM: [u8; crate::manifest::SEGMENT_VECTOR_SIGNATURE_BLOOM_BYTES] =
            [0_u8; crate::manifest::SEGMENT_VECTOR_SIGNATURE_BLOOM_BYTES];
        ExternalRoutingSummaryMetadata {
            object_count: 1,
            checksum: VALID_SEGMENT_CHECKSUM,
            size_bytes: 123,
            graph_checksum: VALID_GRAPH_CHECKSUM,
            graph_size_bytes: 45,
            leaf_mode: "graph",
            vector_signature_bloom: &VECTOR_SIGNATURE_BLOOM,
        }
    }

    fn valid_external_segment_layout_json() -> String {
        serde_json::to_string(&crate::PhysicalLayoutRef {
            object_role: crate::PhysicalObjectRole::NormalSegment,
            physical_format: crate::PhysicalFormat::Parquet,
            layout_policy_version: crate::CURRENT_LAYOUT_POLICY_VERSION,
        })
        .unwrap()
    }

    fn external_routing_parquet_with_rows_and_summary_metadata(
        ids: &[&str],
        paths: &[&str],
        graph_paths: &[&str],
        metadata: &[ExternalRoutingSummaryMetadata<'_>],
    ) -> Vec<u8> {
        assert_eq!(ids.len(), paths.len());
        assert_eq!(ids.len(), graph_paths.len());
        assert_eq!(ids.len(), metadata.len());
        let schema = routing_schema(2);
        let centroids = vec![vec![0.0_f32, 0.0]; ids.len()];
        let id_bloom = crate::manifest::segment_id_bloom(["record"]);
        let layout_json = valid_external_segment_layout_json();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                array(UInt16Array::from_iter_values(
                    ids.iter().map(|_| CURRENT_VERSION),
                )),
                array(UInt64Array::from_iter_values(ids.iter().map(|_| 1))),
                array(StringArray::from_iter_values(ids.iter().copied())),
                array(UInt8Array::from_iter_values(ids.iter().map(|_| 0))),
                array(StringArray::from_iter_values(paths.iter().copied())),
                array(StringArray::from_iter_values(
                    ids.iter().map(|_| layout_json.as_str()),
                )),
                array(UInt64Array::from_iter_values(
                    metadata.iter().map(|row| row.object_count),
                )),
                array(UInt64Array::from_iter_values(ids.iter().map(|_| 2))),
                array(fixed_f32_array(centroids.iter().map(Vec::as_slice), 2)),
                array(Float32Array::from_iter_values(ids.iter().map(|_| 0.0))),
                array(StringArray::from_iter_values(
                    metadata.iter().map(|row| row.checksum),
                )),
                array(UInt64Array::from_iter_values(
                    metadata.iter().map(|row| row.size_bytes),
                )),
                array(UInt64Array::from_iter_values(ids.iter().map(|_| 0))),
                array(StringArray::from_iter_values(graph_paths.iter().copied())),
                array(StringArray::from_iter_values(
                    metadata.iter().map(|row| row.graph_checksum),
                )),
                array(UInt64Array::from_iter_values(
                    metadata.iter().map(|row| row.graph_size_bytes),
                )),
                array(Int64Array::from_iter_values(ids.iter().map(|_| 0))),
                array(BinaryArray::from_iter_values(
                    ids.iter().map(|_| id_bloom.as_slice()),
                )),
                array(StringArray::from_iter_values(
                    metadata.iter().map(|row| row.leaf_mode),
                )),
                array(BinaryArray::from_iter_values(
                    metadata.iter().map(|row| row.vector_signature_bloom),
                )),
                array(fixed_f32_array(centroids.iter().map(Vec::as_slice), 2)),
                array(fixed_f32_array(centroids.iter().map(Vec::as_slice), 2)),
                array(BinaryArray::from_iter_values(
                    ids.iter().map(|_| Vec::<u8>::new()),
                )),
                array(UInt32Array::from_iter_values(ids.iter().map(|_| 0))),
                array(UInt64Array::from_iter_values(ids.iter().map(|_| 0))),
                array(UInt64Array::from_iter_values(ids.iter().map(|_| 0))),
                array(UInt64Array::from_iter_values(ids.iter().map(|_| 0))),
                array(StringArray::from_iter_values(ids.iter().map(|_| "[]"))),
                array(UInt64Array::from_iter_values(ids.iter().map(|_| 0))),
                array(UInt64Array::from_iter_values(ids.iter().map(|_| 0))),
            ],
        )
        .unwrap();

        write_batch(batch).unwrap()
    }

    fn external_routing_parquet_with_manifest_version(manifest_version: u64) -> Vec<u8> {
        external_routing_parquet_with_vector_id_bloom_and_manifest_version(
            vec![0.0, 0.0],
            0.0,
            2,
            crate::manifest::segment_id_bloom(["record"]),
            manifest_version,
        )
    }

    fn external_routing_parquet_with_vector_and_id_bloom(
        centroid: Vec<f32>,
        radius: f32,
        stored_dimensions: u64,
        id_bloom: Vec<u8>,
    ) -> Vec<u8> {
        external_routing_parquet_with_vector_id_bloom_and_manifest_version(
            centroid,
            radius,
            stored_dimensions,
            id_bloom,
            1,
        )
    }

    fn external_routing_parquet_with_vector_id_bloom_and_manifest_version(
        centroid: Vec<f32>,
        radius: f32,
        stored_dimensions: u64,
        id_bloom: Vec<u8>,
        manifest_version: u64,
    ) -> Vec<u8> {
        let schema_dimensions = centroid.len();
        let schema = routing_schema(schema_dimensions);
        let vector_signature_bloom = valid_vector_signature_bloom();
        let layout_json = valid_external_segment_layout_json();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                array(UInt16Array::from_iter_values([CURRENT_VERSION])),
                array(UInt64Array::from_iter_values([manifest_version])),
                array(StringArray::from_iter_values(["seg"])),
                array(UInt8Array::from_iter_values([0])),
                array(StringArray::from_iter_values(["segments/seg.parquet"])),
                array(StringArray::from_iter_values([layout_json.as_str()])),
                array(UInt64Array::from_iter_values([1])),
                array(UInt64Array::from_iter_values([stored_dimensions])),
                array(fixed_f32_array([centroid.as_slice()], schema_dimensions)),
                array(Float32Array::from_iter_values([radius])),
                array(StringArray::from_iter_values([VALID_SEGMENT_CHECKSUM])),
                array(UInt64Array::from_iter_values([123])),
                array(UInt64Array::from_iter_values([0])),
                array(StringArray::from_iter_values([
                    "segments/seg.graph.parquet",
                ])),
                array(StringArray::from_iter_values([VALID_GRAPH_CHECKSUM])),
                array(UInt64Array::from_iter_values([45])),
                array(Int64Array::from_iter_values([0])),
                array(BinaryArray::from_iter_values([id_bloom.as_slice()])),
                array(StringArray::from_iter_values(["graph"])),
                array(BinaryArray::from_iter_values([
                    vector_signature_bloom.as_slice()
                ])),
                array(fixed_f32_array([centroid.as_slice()], schema_dimensions)),
                array(fixed_f32_array([centroid.as_slice()], schema_dimensions)),
                array(BinaryArray::from_iter_values([Vec::<u8>::new()])),
                array(UInt32Array::from_iter_values([0])),
                array(UInt64Array::from_iter_values([0])),
                array(UInt64Array::from_iter_values([0])),
                array(UInt64Array::from_iter_values([0])),
                array(StringArray::from_iter_values(["[]"])),
                array(UInt64Array::from_iter_values([0])),
                array(UInt64Array::from_iter_values([0])),
            ],
        )
        .unwrap();

        write_batch(batch).unwrap()
    }

    fn external_pivots_parquet(vector: [f32; 2]) -> Vec<u8> {
        external_pivots_parquet_with_rows(vec![("pivot", 0, vector)])
    }

    fn external_pivots_parquet_with_ids<const N: usize>(ids: [&str; N]) -> Vec<u8> {
        external_pivots_parquet_with_rows(
            ids.iter()
                .enumerate()
                .map(|(ordinal, id)| (*id, ordinal as u64, [0.0, 0.0]))
                .collect(),
        )
    }

    fn external_pivots_parquet_with_rows(rows: Vec<(&str, u64, [f32; 2])>) -> Vec<u8> {
        let schema = pivots_schema(2);
        let vectors = rows.iter().map(|(_, _, vector)| vector.as_slice());
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                array(UInt16Array::from_iter_values(
                    rows.iter().map(|_| CURRENT_VERSION),
                )),
                array(UInt64Array::from_iter_values(rows.iter().map(|_| 1))),
                array(UInt64Array::from_iter_values(
                    rows.iter().map(|(_, ordinal, _)| *ordinal),
                )),
                array(StringArray::from_iter_values(
                    rows.iter().map(|(id, _, _)| *id),
                )),
                array(fixed_f32_array(vectors, 2)),
            ],
        )
        .unwrap();

        write_batch(batch).unwrap()
    }

    fn external_segment_parquet(
        vector: [f32; 2],
        centroid: [f32; 2],
        radius: f32,
        routing_code: f32,
    ) -> Vec<u8> {
        external_segment_parquet_with_dimensions(
            vector.to_vec(),
            centroid.to_vec(),
            radius,
            routing_code,
            2,
        )
    }

    #[test]
    fn lean_and_full_decode_carry_persisted_codes_and_empty_vectors() {
        let segment = Segment::from_records(
            "seg".to_string(),
            0,
            VectorMetric::Euclidean,
            2,
            vec![
                VectorRecord::new("r0", vec![0.0, 0.0]),
                VectorRecord::new("r1", vec![1.0, 0.0]),
                VectorRecord::new("r2", vec![0.0, 1.0]),
                VectorRecord::new("r3", vec![1.0, 1.0]),
            ],
        )
        .unwrap();
        let bytes = segment_to_parquet(&segment).unwrap();
        assert!(segment_has_persisted_pq_bounds(&bytes).unwrap());

        let full = segment_from_parquet(&bytes).unwrap();
        let lean = lean_segment_from_parquet(&bytes).unwrap();

        // Dense vectors now live only in the Arrow IPC sidecar, so BOTH decodes
        // yield empty dense vectors; they still carry ids, PQ codes, and the
        // persisted PQ bounds. Full-vector reconstruction from the sidecar is an
        // index-level read-boundary concern, exercised by the integration tests.
        assert_eq!(lean.pq_codes, full.pq_codes);
        assert_eq!(lean.pq_min, full.pq_min);
        assert_eq!(lean.pq_max, full.pq_max);
        for (lean_record, full_record) in lean.records.iter().zip(&full.records) {
            assert_eq!(lean_record.id, full_record.id);
            assert!(lean_record.vector.is_empty());
            assert!(full_record.vector.is_empty());
        }

        // The query quantizes identically from persisted bounds (the fix).
        let query = vec![0.4, 0.7];
        assert_eq!(
            crate::segment::pq_code_for_query(&lean, &query).unwrap(),
            crate::segment::pq_code_for_query(&full, &query).unwrap(),
        );
    }

    #[test]
    fn lean_row_decode_does_not_materialize_repeated_segment_vectors() {
        let records = (0..128)
            .map(|row| VectorRecord::new(format!("r{row}"), vec![row as f32; 960]))
            .collect();
        let segment = Segment::from_records_with_quantizer(
            "wide-segment".to_string(),
            0,
            VectorMetric::Euclidean,
            960,
            records,
            crate::QuantizerKind::TurboQuant {
                seed: 7,
                bits: 4,
                qjl_bits: 0,
                shards: 1,
            },
        )
        .unwrap();
        let bytes = segment_to_parquet(&segment).unwrap();

        let batches = read_lean_segment_row_batches(&bytes).unwrap();
        let schema = batches[0].schema();

        // These segment constants live once in the packed row-zero header.
        // The serving row projection must not materialize them per candidate.
        for excluded in [
            "segment_id",
            "metric",
            "dimensions",
            "centroid",
            "radius",
            "created_at_ms",
            "pq_min",
            "pq_max",
        ] {
            assert!(
                schema.index_of(excluded).is_err(),
                "materialized {excluded}"
            );
        }
        for required in ["routing_code", "pq_code", "record_id", "metadata"] {
            assert!(schema.index_of(required).is_ok(), "missing {required}");
        }

        let lean = lean_segment_from_parquet(&bytes).unwrap();
        assert_eq!(lean.records.len(), segment.records.len());
        assert_eq!(lean.pq_codes, segment.pq_codes);
        assert_eq!(lean.pq_min, segment.pq_min);
        assert_eq!(lean.pq_max, segment.pq_max);
    }

    #[test]
    fn segment_constants_are_packed_once_instead_of_repeated_per_row() {
        let records = (0..128)
            .map(|row| VectorRecord::new(format!("r{row}"), vec![row as f32; 960]))
            .collect();
        let segment = Segment::from_records_with_quantizer(
            "wide-segment".to_string(),
            0,
            VectorMetric::Euclidean,
            960,
            records,
            crate::QuantizerKind::TurboQuant {
                seed: 7,
                bits: 4,
                qjl_bits: 0,
                shards: 1,
            },
        )
        .unwrap();
        let bytes = segment_to_parquet(&segment).unwrap();
        let batches = read_batches(&bytes).unwrap();
        let batch = &batches[0];
        let schema = batch.schema();

        let header_column = schema
            .index_of("segment_header")
            .expect("normal segment must carry one packed header column");
        let header = batch
            .column(header_column)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        assert!(header.is_valid(0));
        assert!(
            header.value(0).starts_with(b"BSH1"),
            "segment header must use the versioned packed binary codec"
        );
        assert_eq!(header.null_count(), segment.records.len() - 1);
        assert_eq!(
            decode_segment_header(header.value(0)).unwrap(),
            packed_segment_header(&segment)
        );

        let mut corrupted = header.value(0).to_vec();
        let payload_byte = corrupted.len() - SEGMENT_HEADER_CHECKSUM_LEN - 1;
        corrupted[payload_byte] ^= 1;
        let error = decode_segment_header(&corrupted).unwrap_err();
        assert!(
            error.to_string().contains("checksum mismatch"),
            "unexpected corruption error: {error}"
        );

        for repeated in [
            "format_version",
            "segment_id",
            "level",
            "metric",
            "dimensions",
            "centroid",
            "radius",
            "created_at_ms",
            "pq_min",
            "pq_max",
        ] {
            assert!(
                schema.index_of(repeated).is_err(),
                "segment constant `{repeated}` must not be repeated per row"
            );
        }
    }

    fn external_segment_parquet_with_records<const N: usize>(
        records: [(&str, [f32; 2]); N],
    ) -> Vec<u8> {
        let schema = segment_schema(
            2,
            2,
            false,
            false,
            false,
            false,
            false,
            VectorElementType::Float32,
        );
        let header = external_packed_segment_header(vec![0.0, 0.0], 0.0, 2);
        let pq_code = [128_u8, 128_u8];
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                array(BinaryArray::from_iter(
                    (0..records.len()).map(|row| (row == 0).then_some(header.as_slice())),
                )),
                array(Float32Array::from_iter_values(records.iter().map(|_| 0.0))),
                array(fixed_u8_array(
                    records.iter().map(|_| pq_code.as_slice()),
                    2,
                )),
                array(BinaryArray::from_iter_values(
                    records.iter().map(|(id, _)| id.as_bytes()),
                )),
                array(BinaryArray::from_iter_values(
                    records.iter().map(|_| Vec::<u8>::new()),
                )),
            ],
        )
        .unwrap();

        write_batch(batch).unwrap()
    }

    fn external_packed_segment_header(
        centroid: Vec<f32>,
        radius: f32,
        stored_dimensions: usize,
    ) -> Vec<u8> {
        encode_packed_segment_header_unchecked(&LeanSegmentHeader {
            format_version: CURRENT_VERSION,
            metadata: SegmentMetadata {
                id: "seg".to_string(),
                level: 0,
                metric: VectorMetric::Euclidean,
                dimensions: stored_dimensions,
                centroid,
                radius,
                created_at: DateTime::from_timestamp_millis(0).unwrap(),
            },
            pq_bounds: (vec![0.0; stored_dimensions], vec![0.0; stored_dimensions]),
        })
        .unwrap()
    }

    /// Build a legacy-style segment table WITHOUT a `pq_code` column (dense
    /// vectors no longer live in Parquet; they are stored in the Arrow IPC
    /// sidecar). Used to exercise decode-time validation of the non-vector
    /// columns (centroid/radius/routing_code) that Parquet still carries.
    fn external_segment_parquet_with_dimensions(
        _vector: Vec<f32>,
        centroid: Vec<f32>,
        radius: f32,
        routing_code: f32,
        stored_dimensions: u64,
    ) -> Vec<u8> {
        let stored_dimensions = usize::try_from(stored_dimensions).unwrap();
        let header = external_packed_segment_header(centroid, radius, stored_dimensions);
        let schema = Arc::new(Schema::new(vec![
            Field::new("segment_header", DataType::Binary, true),
            Field::new("routing_code", DataType::Float32, false),
            Field::new("record_id", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                array(BinaryArray::from_iter_values([header.as_slice()])),
                array(Float32Array::from_iter_values([routing_code])),
                array(StringArray::from_iter_values(["bad"])),
            ],
        )
        .unwrap();

        write_batch(batch).unwrap()
    }

    fn external_graph_parquet(distance: f32) -> Vec<u8> {
        let schema = graph_schema();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                array(UInt16Array::from_iter_values([CURRENT_VERSION])),
                array(StringArray::from_iter_values(["seg"])),
                array(UInt8Array::from_iter_values([0])),
                array(Int64Array::from_iter_values([0])),
                array(UInt64Array::from_iter_values([0])),
                array(UInt64Array::from_iter_values([1])),
                array(Float32Array::from_iter_values([distance])),
            ],
        )
        .unwrap();

        write_batch(batch).unwrap()
    }
}
