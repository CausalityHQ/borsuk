//! Native Node/TypeScript bindings for BORSUK.
#![allow(missing_docs)]

use std::{path::PathBuf, sync::Mutex, time::Duration};

use borsuk::{
    BorsukIndex, CompactionOptions, DEFAULT_COMPACTION_MAX_SEGMENTS, Fusion,
    GarbageCollectionOptions, HybridOptions, HybridQuery, IndexConfig, LeafMode, OpenOptions,
    RebuildOptions, SearchMode, SearchOptions, VectorMetric, VectorRecord,
};
use napi::{
    Error, Result, Status,
    bindgen_prelude::{Float32Array, Uint8Array},
};
use napi_derive::napi;

#[napi(object)]
pub struct CreateOptions {
    pub uri: String,
    pub metric: String,
    pub dim: Option<u32>,
    pub dimensions: Option<u32>,
    pub segment_size: Option<u32>,
    pub segment_max_vectors: Option<u32>,
    pub routing_page_fanout: Option<u32>,
    pub graph_neighbors: Option<u32>,
    pub ram_budget: Option<String>,
    pub cache_dir: Option<String>,
    pub text: Option<bool>,
}

#[napi(object)]
#[derive(Default)]
pub struct OpenOptionsJs {
    pub cache_dir: Option<String>,
    pub ram_budget: Option<String>,
    pub resident_routing: Option<bool>,
    pub cache_max_bytes: Option<String>,
}

#[napi(object)]
#[derive(Default)]
pub struct SearchOptionsJs {
    pub k: Option<u32>,
    pub mode: Option<String>,
    pub leaf_mode: Option<String>,
    pub eps: Option<f64>,
    pub max_segments: Option<u32>,
    pub max_bytes: Option<f64>,
    pub max_bytes_text: Option<String>,
    pub max_latency_ms: Option<u32>,
    pub routing_page_overfetch: Option<u32>,
    pub max_candidates_per_segment: Option<u32>,
    pub guaranteed_recall: Option<bool>,
    pub prefetch_depth: Option<u32>,
    pub filter: Option<serde_json::Value>,
    pub include_metadata: Option<bool>,
}

#[napi(object)]
pub struct SparseVectorJs {
    pub indices: Vec<u32>,
    pub values: Vec<f64>,
}

#[napi(object)]
#[derive(Default)]
pub struct KSearchOptionsJs {
    pub k: Option<u32>,
}

#[napi(object)]
pub struct HybridQueryJs {
    pub dense: Option<Vec<f64>>,
    pub text: Option<String>,
}

#[napi(object)]
#[derive(Default)]
pub struct HybridOptionsJs {
    pub k: Option<u32>,
    pub fusion: Option<String>,
    pub rrf_k: Option<u32>,
    pub weights: Option<Vec<f64>>,
}

#[napi(object)]
pub struct Hit {
    pub id: String,
    pub id_bytes: Uint8Array,
    pub distance: f64,
    pub metadata: Option<serde_json::Value>,
}

#[napi(object)]
pub struct GetRecordJs {
    pub vector: Vec<f64>,
    pub metadata: serde_json::Value,
}

#[napi(object)]
pub struct IndexStatsJs {
    pub metric: String,
    pub dimensions: u32,
    pub segment_max_vectors: u32,
    pub ram_budget_bytes: Option<f64>,
    pub text: bool,
    pub sparse_encoded_vectors: u32,
    pub dense_encoded_vectors: u32,
    pub manifest_version: f64,
    pub routing_max_level: u32,
    pub routing_page_fanout: u32,
    pub routing_leaf_pages: u32,
    pub routing_pages: u32,
    pub segments: u32,
    pub records: u32,
    pub segment_bytes: f64,
    pub graph_bytes: f64,
    pub resident_bytes_estimate: f64,
}

#[napi(object)]
pub struct SearchReportJs {
    pub hits: Vec<Hit>,
    pub leaf_mode: String,
    pub termination_reason: String,
    pub recall_guarantee: String,
    pub segments_total: u32,
    pub segments_searched: u32,
    pub segments_skipped: u32,
    pub routing_page_indexes_read: u32,
    pub routing_pages_read: u32,
    pub bytes_read: f64,
    pub prefetched_bytes_unused: f64,
    pub graph_bytes_read: f64,
    pub object_cache_hits: u32,
    pub object_cache_misses: u32,
    pub cache_repairs: u32,
    pub records_considered: u32,
    pub records_scored: u32,
    pub graph_candidates_added: u32,
    pub resident_bytes_estimate: f64,
    pub elapsed_ms: u32,
    pub requests: RequestCountsJs,
    pub rows_evaluated: u32,
    pub rows_passed_filter: u32,
    pub segments_pruned_by_filter: u32,
}

#[napi(object)]
pub struct RequestCountsJs {
    pub gets: f64,
    pub puts: f64,
    pub deletes: f64,
    pub heads: f64,
    pub lists: f64,
    pub total: f64,
}

#[napi(object)]
pub struct AddReportJs {
    pub segments_written: u32,
    pub graph_payloads_written: u32,
    pub manifest_tables_written: u32,
    pub routing_pages_written: u32,
    pub total_bytes_written: f64,
    pub bytes_per_vector: f64,
    pub requests: RequestCountsJs,
}

#[napi(object)]
pub struct AddWithReportResultJs {
    pub ids: Vec<String>,
    pub report: AddReportJs,
}

#[napi(object)]
#[derive(Default)]
pub struct CompactionOptionsJs {
    pub source_level: Option<u32>,
    pub target_level: Option<u32>,
    pub max_segments: Option<u32>,
    pub all_matching: Option<bool>,
    pub min_segments: Option<u32>,
    pub target_segment_max_vectors: Option<u32>,
    pub target_segment_max_radius: Option<f64>,
}

#[napi(object)]
pub struct CompactionReportJs {
    pub compacted: bool,
    pub source_level: u32,
    pub target_level: u32,
    pub segments_read: u32,
    pub segments_written: u32,
    pub records_rewritten: u32,
    pub routing_page_indexes_read: u32,
    pub routing_pages_read: u32,
    pub routing_page_indexes_written: u32,
    pub routing_pages_written: u32,
    pub graph_payloads_read: u32,
    pub graph_bytes_read: f64,
    pub bytes_read: f64,
    pub bytes_written: f64,
    pub object_cache_hits: u32,
    pub object_cache_misses: u32,
    pub manifest_version: f64,
}

#[napi(object)]
#[derive(Default)]
pub struct GarbageCollectionOptionsJs {
    pub dry_run: Option<bool>,
    pub min_age_ms: Option<f64>,
}

#[napi(object)]
pub struct GarbageCollectionReportJs {
    pub dry_run: bool,
    pub objects_scanned: u32,
    pub objects_deleted: u32,
    pub routing_objects_deleted: u32,
    pub tables_deleted: u32,
    pub routing_page_indexes_read: u32,
    pub routing_pages_read: u32,
    pub bytes_read: f64,
    pub bytes_reclaimable: f64,
    pub bytes_reclaimed: f64,
    pub object_cache_hits: u32,
    pub object_cache_misses: u32,
    pub candidates: Vec<String>,
}

#[napi(object)]
#[derive(Default)]
pub struct RebuildOptionsJs {
    pub source_level: Option<u32>,
    pub target_level: Option<u32>,
    pub min_segments: Option<u32>,
    pub target_segment_max_vectors: Option<u32>,
    pub delete_obsolete: Option<bool>,
}

#[napi(object)]
pub struct RebuildReportJs {
    pub compaction: CompactionReportJs,
    pub garbage_collection: GarbageCollectionReportJs,
}

#[napi(object)]
pub struct DeleteReportJs {
    pub deleted: u32,
    pub total_tombstoned: u32,
    pub published: bool,
    pub requests: RequestCountsJs,
}

#[napi(object)]
pub struct PurgeReportJs {
    pub segments_rewritten: u32,
    pub records_purged: u32,
    pub tombstones_cleared: u32,
    pub published: bool,
    pub requests: RequestCountsJs,
}

#[napi(object)]
pub struct IncrementalReportJs {
    pub splits: u32,
    pub merges: u32,
    pub segments_created: u32,
    pub segments_removed: u32,
    pub records_moved: u32,
    pub published: bool,
    pub requests: RequestCountsJs,
}

#[napi(object)]
#[derive(Default)]
pub struct IncrementalOptionsJs {
    pub max_segment_vectors: Option<u32>,
    pub max_segment_radius: Option<f64>,
    pub min_segment_vectors: Option<u32>,
    pub max_operations: Option<u32>,
}

#[napi(js_name = "Index")]
pub struct JsIndex {
    inner: Mutex<BorsukIndex>,
}

#[napi]
impl JsIndex {
    #[napi(constructor)]
    pub fn new(uri: String) -> Result<Self> {
        open(uri, None, None, true, None)
    }

    #[napi]
    pub fn add(
        &self,
        vectors: Vec<Vec<f64>>,
        ids: Option<Vec<String>>,
        metadata: Option<Vec<serde_json::Value>>,
        sparse: Option<Vec<Option<SparseVectorJs>>>,
        text: Option<Vec<Option<String>>>,
    ) -> Result<Vec<String>> {
        let mut index = self
            .inner
            .lock()
            .map_err(|_| Error::new(Status::GenericFailure, "index lock poisoned"))?;
        let dimensions = index.manifest().config.dimensions;
        let vectors = vectors
            .into_iter()
            .map(|vector| vector.into_iter().map(f64_to_f32).collect())
            .collect::<Vec<Vec<f32>>>();
        let metadata = match metadata {
            Some(rows) => {
                if rows.len() != vectors.len() {
                    return Err(Error::new(
                        Status::InvalidArg,
                        "metadata length must match vectors length",
                    ));
                }
                Some(
                    rows.iter()
                        .map(borsuk::metadata_from_json)
                        .collect::<std::result::Result<Vec<_>, _>>()
                        .map_err(to_js_error)?,
                )
            }
            None => None,
        };
        validate_optional_rows_len(&sparse, "sparse", vectors.len())?;
        validate_optional_rows_len(&text, "text", vectors.len())?;
        match ids {
            Some(ids) => {
                let ids = ids_for_vectors(Some(ids), vectors.len(), &index)?;
                let records = records_from_vectors(
                    &ids,
                    vectors,
                    dimensions,
                    metadata.as_deref(),
                    sparse.as_deref(),
                    text.as_deref(),
                )?;

                index.add(records).map_err(to_js_error)?;
                Ok(ids)
            }
            None => {
                if metadata.is_some() {
                    return Err(Error::new(
                        Status::InvalidArg,
                        "metadata requires explicit ids",
                    ));
                }
                if sparse.is_some() || text.is_some() {
                    let ids = ids_for_vectors(None, vectors.len(), &index)?;
                    let records = records_from_vectors(
                        &ids,
                        vectors,
                        dimensions,
                        None,
                        sparse.as_deref(),
                        text.as_deref(),
                    )?;

                    index.add(records).map_err(to_js_error)?;
                    return Ok(ids);
                }
                index.add_vectors(vectors).map_err(to_js_error)
            }
        }
    }

    #[napi(js_name = "getRecord")]
    pub fn get_record(&self, id: String) -> Result<Option<GetRecordJs>> {
        let record = self
            .inner
            .lock()
            .map_err(|_| Error::new(Status::GenericFailure, "index lock poisoned"))?
            .get_record(&id)
            .map_err(to_js_error)?;
        Ok(record.map(|(vector, metadata)| GetRecordJs {
            vector: vector.into_iter().map(f64::from).collect(),
            metadata: borsuk::metadata_to_json(&metadata),
        }))
    }

    #[napi(js_name = "addWithReport")]
    pub fn add_with_report(
        &self,
        vectors: Vec<Vec<f64>>,
        ids: Option<Vec<String>>,
    ) -> Result<AddWithReportResultJs> {
        let mut index = self
            .inner
            .lock()
            .map_err(|_| Error::new(Status::GenericFailure, "index lock poisoned"))?;
        let vectors = vectors
            .into_iter()
            .map(|vector| vector.into_iter().map(f64_to_f32).collect())
            .collect::<Vec<Vec<f32>>>();
        let (ids, report) = index.add_with_report(vectors, ids).map_err(to_js_error)?;
        Ok(AddWithReportResultJs {
            ids,
            report: add_report_to_js(report)?,
        })
    }

    #[napi(js_name = "addIdBytes")]
    pub fn add_id_bytes(
        &self,
        vectors: Vec<Vec<f64>>,
        ids: Vec<Uint8Array>,
    ) -> Result<Vec<Uint8Array>> {
        let mut index = self
            .inner
            .lock()
            .map_err(|_| Error::new(Status::GenericFailure, "index lock poisoned"))?;
        let ids = id_bytes_for_vectors(ids, vectors.len())?;
        let vectors = vectors
            .into_iter()
            .map(|vector| vector.into_iter().map(f64_to_f32).collect())
            .collect::<Vec<Vec<f32>>>();
        let records = ids
            .iter()
            .cloned()
            .zip(vectors)
            .map(|(id, vector)| VectorRecord::new_bytes(id, vector))
            .collect::<Vec<_>>();

        index.add(records).map_err(to_js_error)?;
        Ok(id_bytes_to_js(ids))
    }

    #[napi(js_name = "addBuffer")]
    pub fn add_buffer(
        &self,
        vectors: Float32Array,
        ids: Option<Vec<String>>,
    ) -> Result<Vec<String>> {
        let mut index = self
            .inner
            .lock()
            .map_err(|_| Error::new(Status::GenericFailure, "index lock poisoned"))?;
        let dimensions = index.manifest().config.dimensions;
        let row_count = flat_vector_row_count(vectors.as_ref(), dimensions)?;
        match ids {
            Some(ids) => {
                let ids = ids_for_vectors(Some(ids), row_count, &index)?;
                let records = records_from_flat_vectors(ids, vectors.as_ref(), dimensions)?;
                let ids = records
                    .iter()
                    .map(|record| record.id.to_utf8_string().map_err(to_js_error))
                    .collect::<Result<Vec<_>>>()?;

                index.add(records).map_err(to_js_error)?;
                Ok(ids)
            }
            None => index
                .add_vectors(vectors_from_flat_rows(
                    vectors.as_ref(),
                    dimensions,
                    "vector buffer",
                )?)
                .map_err(to_js_error),
        }
    }

    #[napi(js_name = "addBufferIdBytes")]
    pub fn add_buffer_id_bytes(
        &self,
        vectors: Float32Array,
        ids: Vec<Uint8Array>,
    ) -> Result<Vec<Uint8Array>> {
        let mut index = self
            .inner
            .lock()
            .map_err(|_| Error::new(Status::GenericFailure, "index lock poisoned"))?;
        let dimensions = index.manifest().config.dimensions;
        let row_count = flat_vector_row_count(vectors.as_ref(), dimensions)?;
        let ids = id_bytes_for_vectors(ids, row_count)?;
        let records =
            records_from_flat_vectors_with_id_bytes(ids.clone(), vectors.as_ref(), dimensions)?;

        index.add(records).map_err(to_js_error)?;
        Ok(id_bytes_to_js(ids))
    }

    #[napi]
    pub fn stats(&self) -> Result<IndexStatsJs> {
        let stats = self
            .inner
            .lock()
            .map_err(|_| Error::new(Status::GenericFailure, "index lock poisoned"))?
            .try_stats()
            .map_err(to_js_error)?;

        index_stats_to_js(stats)
    }

    #[napi(js_name = "searchIds")]
    pub fn search_ids(
        &self,
        query: Vec<f64>,
        options: Option<SearchOptionsJs>,
    ) -> Result<Vec<String>> {
        let options = options.unwrap_or_default();
        let mode = parse_mode(&options)?;
        let query = query.into_iter().map(f64_to_f32).collect::<Vec<_>>();
        self.inner
            .lock()
            .map_err(|_| Error::new(Status::GenericFailure, "index lock poisoned"))?
            .search_ids(&query, search_options_from_js(&options, mode)?)
            .map_err(to_js_error)
    }

    #[napi(js_name = "searchIdBytes")]
    pub fn search_id_bytes(
        &self,
        query: Vec<f64>,
        options: Option<SearchOptionsJs>,
    ) -> Result<Vec<Uint8Array>> {
        let options = options.unwrap_or_default();
        let mode = parse_mode(&options)?;
        let query = query.into_iter().map(f64_to_f32).collect::<Vec<_>>();
        let ids = self
            .inner
            .lock()
            .map_err(|_| Error::new(Status::GenericFailure, "index lock poisoned"))?
            .search_id_bytes(&query, search_options_from_js(&options, mode)?)
            .map_err(to_js_error)?;
        Ok(id_bytes_to_js(ids))
    }

    #[napi(js_name = "searchVectors")]
    pub fn search_vectors(
        &self,
        query: Vec<f64>,
        options: Option<SearchOptionsJs>,
    ) -> Result<Vec<Vec<f64>>> {
        let options = options.unwrap_or_default();
        let mode = parse_mode(&options)?;
        let query = query.into_iter().map(f64_to_f32).collect::<Vec<_>>();
        let vectors = self
            .inner
            .lock()
            .map_err(|_| Error::new(Status::GenericFailure, "index lock poisoned"))?
            .search_vectors(&query, search_options_from_js(&options, mode)?)
            .map_err(to_js_error)?;
        Ok(vectors
            .into_iter()
            .map(|vector| vector.into_iter().map(f64::from).collect())
            .collect())
    }

    #[napi(js_name = "getVector")]
    pub fn get_vector(&self, id: String) -> Result<Option<Vec<f64>>> {
        self.inner
            .lock()
            .map_err(|_| Error::new(Status::GenericFailure, "index lock poisoned"))?
            .get_vector(&id)
            .map(|vector| vector.map(|values| values.into_iter().map(f64::from).collect()))
            .map_err(to_js_error)
    }

    #[napi(js_name = "getVectorById")]
    pub fn get_vector_by_id(&self, id: Uint8Array) -> Result<Option<Vec<f64>>> {
        self.inner
            .lock()
            .map_err(|_| Error::new(Status::GenericFailure, "index lock poisoned"))?
            .get_vector_by_id(id.as_ref())
            .map(|vector| vector.map(|values| values.into_iter().map(f64::from).collect()))
            .map_err(to_js_error)
    }

    #[napi(js_name = "searchIdsBuffer")]
    pub fn search_ids_buffer(
        &self,
        query: Float32Array,
        options: Option<SearchOptionsJs>,
    ) -> Result<Vec<String>> {
        let options = options.unwrap_or_default();
        let mode = parse_mode(&options)?;
        let index = self
            .inner
            .lock()
            .map_err(|_| Error::new(Status::GenericFailure, "index lock poisoned"))?;
        let dimensions = index.manifest().config.dimensions;
        let query = query_from_flat_vector(query.as_ref(), dimensions, "query buffer")?;
        index
            .search_ids(&query, search_options_from_js(&options, mode)?)
            .map_err(to_js_error)
    }

    #[napi(js_name = "searchIdBytesBuffer")]
    pub fn search_id_bytes_buffer(
        &self,
        query: Float32Array,
        options: Option<SearchOptionsJs>,
    ) -> Result<Vec<Uint8Array>> {
        let options = options.unwrap_or_default();
        let mode = parse_mode(&options)?;
        let index = self
            .inner
            .lock()
            .map_err(|_| Error::new(Status::GenericFailure, "index lock poisoned"))?;
        let dimensions = index.manifest().config.dimensions;
        let query = query_from_flat_vector(query.as_ref(), dimensions, "query buffer")?;
        let ids = index
            .search_id_bytes(&query, search_options_from_js(&options, mode)?)
            .map_err(to_js_error)?;
        Ok(id_bytes_to_js(ids))
    }

    #[napi(js_name = "searchVectorsBuffer")]
    pub fn search_vectors_buffer(
        &self,
        query: Float32Array,
        options: Option<SearchOptionsJs>,
    ) -> Result<Vec<Vec<f64>>> {
        let options = options.unwrap_or_default();
        let mode = parse_mode(&options)?;
        let index = self
            .inner
            .lock()
            .map_err(|_| Error::new(Status::GenericFailure, "index lock poisoned"))?;
        let dimensions = index.manifest().config.dimensions;
        let query = query_from_flat_vector(query.as_ref(), dimensions, "query buffer")?;
        let vectors = index
            .search_vectors(&query, search_options_from_js(&options, mode)?)
            .map_err(to_js_error)?;
        Ok(vectors
            .into_iter()
            .map(|vector| vector.into_iter().map(f64::from).collect())
            .collect())
    }

    #[napi(js_name = "searchWithReportBuffer")]
    pub fn search_with_report_buffer(
        &self,
        query: Float32Array,
        options: Option<SearchOptionsJs>,
    ) -> Result<SearchReportJs> {
        let options = options.unwrap_or_default();
        let mode = parse_mode(&options)?;
        let index = self
            .inner
            .lock()
            .map_err(|_| Error::new(Status::GenericFailure, "index lock poisoned"))?;
        let dimensions = index.manifest().config.dimensions;
        let query = query_from_flat_vector(query.as_ref(), dimensions, "query buffer")?;
        let report = index
            .search_with_report(&query, search_options_from_js(&options, mode)?)
            .map_err(to_js_error)?;

        search_report_to_js(report)
    }

    #[napi(js_name = "searchIdsBatch")]
    pub fn search_ids_batch(
        &self,
        queries: Vec<Vec<f64>>,
        options: Option<SearchOptionsJs>,
    ) -> Result<Vec<Vec<String>>> {
        let options = options.unwrap_or_default();
        let mode = parse_mode(&options)?;
        let queries = queries
            .into_iter()
            .map(|query| query.into_iter().map(f64_to_f32).collect::<Vec<_>>())
            .collect::<Vec<_>>();
        self.inner
            .lock()
            .map_err(|_| Error::new(Status::GenericFailure, "index lock poisoned"))?
            .search_ids_batch(&queries, search_options_from_js(&options, mode)?)
            .map_err(to_js_error)
    }

    #[napi(js_name = "searchIdBytesBatch")]
    pub fn search_id_bytes_batch(
        &self,
        queries: Vec<Vec<f64>>,
        options: Option<SearchOptionsJs>,
    ) -> Result<Vec<Vec<Uint8Array>>> {
        let options = options.unwrap_or_default();
        let mode = parse_mode(&options)?;
        let queries = queries
            .into_iter()
            .map(|query| query.into_iter().map(f64_to_f32).collect::<Vec<_>>())
            .collect::<Vec<_>>();
        let ids = self
            .inner
            .lock()
            .map_err(|_| Error::new(Status::GenericFailure, "index lock poisoned"))?
            .search_id_bytes_batch(&queries, search_options_from_js(&options, mode)?)
            .map_err(to_js_error)?;
        Ok(id_bytes_batch_to_js(ids))
    }

    #[napi(js_name = "searchVectorsBatch")]
    pub fn search_vectors_batch(
        &self,
        queries: Vec<Vec<f64>>,
        options: Option<SearchOptionsJs>,
    ) -> Result<Vec<Vec<Vec<f64>>>> {
        let options = options.unwrap_or_default();
        let mode = parse_mode(&options)?;
        let queries = queries
            .into_iter()
            .map(|query| query.into_iter().map(f64_to_f32).collect::<Vec<_>>())
            .collect::<Vec<_>>();
        let vectors = self
            .inner
            .lock()
            .map_err(|_| Error::new(Status::GenericFailure, "index lock poisoned"))?
            .search_vectors_batch(&queries, search_options_from_js(&options, mode)?)
            .map_err(to_js_error)?;
        Ok(vectors
            .into_iter()
            .map(|hits| {
                hits.into_iter()
                    .map(|vector| vector.into_iter().map(f64::from).collect())
                    .collect()
            })
            .collect())
    }

    #[napi(js_name = "searchIdsBatchBuffer")]
    pub fn search_ids_batch_buffer(
        &self,
        queries: Float32Array,
        options: Option<SearchOptionsJs>,
    ) -> Result<Vec<Vec<String>>> {
        let options = options.unwrap_or_default();
        let mode = parse_mode(&options)?;
        let index = self
            .inner
            .lock()
            .map_err(|_| Error::new(Status::GenericFailure, "index lock poisoned"))?;
        let dimensions = index.manifest().config.dimensions;
        let queries = vectors_from_flat_rows(queries.as_ref(), dimensions, "query buffer")?;
        index
            .search_ids_batch(&queries, search_options_from_js(&options, mode)?)
            .map_err(to_js_error)
    }

    #[napi(js_name = "searchIdBytesBatchBuffer")]
    pub fn search_id_bytes_batch_buffer(
        &self,
        queries: Float32Array,
        options: Option<SearchOptionsJs>,
    ) -> Result<Vec<Vec<Uint8Array>>> {
        let options = options.unwrap_or_default();
        let mode = parse_mode(&options)?;
        let index = self
            .inner
            .lock()
            .map_err(|_| Error::new(Status::GenericFailure, "index lock poisoned"))?;
        let dimensions = index.manifest().config.dimensions;
        let queries = vectors_from_flat_rows(queries.as_ref(), dimensions, "query buffer")?;
        let ids = index
            .search_id_bytes_batch(&queries, search_options_from_js(&options, mode)?)
            .map_err(to_js_error)?;
        Ok(id_bytes_batch_to_js(ids))
    }

    #[napi(js_name = "searchVectorsBatchBuffer")]
    pub fn search_vectors_batch_buffer(
        &self,
        queries: Float32Array,
        options: Option<SearchOptionsJs>,
    ) -> Result<Vec<Vec<Vec<f64>>>> {
        let options = options.unwrap_or_default();
        let mode = parse_mode(&options)?;
        let index = self
            .inner
            .lock()
            .map_err(|_| Error::new(Status::GenericFailure, "index lock poisoned"))?;
        let dimensions = index.manifest().config.dimensions;
        let queries = vectors_from_flat_rows(queries.as_ref(), dimensions, "query buffer")?;
        let vectors = index
            .search_vectors_batch(&queries, search_options_from_js(&options, mode)?)
            .map_err(to_js_error)?;
        Ok(vectors
            .into_iter()
            .map(|hits| {
                hits.into_iter()
                    .map(|vector| vector.into_iter().map(f64::from).collect())
                    .collect()
            })
            .collect())
    }

    #[napi]
    pub fn search_with_report(
        &self,
        query: Vec<f64>,
        options: Option<SearchOptionsJs>,
    ) -> Result<SearchReportJs> {
        let options = options.unwrap_or_default();
        let mode = parse_mode(&options)?;
        let query = query.into_iter().map(f64_to_f32).collect::<Vec<_>>();
        let report = self
            .inner
            .lock()
            .map_err(|_| Error::new(Status::GenericFailure, "index lock poisoned"))?
            .search_with_report(&query, search_options_from_js(&options, mode)?)
            .map_err(to_js_error)?;

        search_report_to_js(report)
    }

    #[napi(js_name = "searchText")]
    pub fn search_text(
        &self,
        text: String,
        options: Option<KSearchOptionsJs>,
    ) -> Result<Vec<String>> {
        let report = self
            .inner
            .lock()
            .map_err(|_| Error::new(Status::GenericFailure, "index lock poisoned"))?
            .search_text(&text, k_from_js(options))
            .map_err(to_js_error)?;

        search_report_ids(report)
    }

    #[napi(js_name = "searchTextWithReport")]
    pub fn search_text_with_report(
        &self,
        text: String,
        options: Option<KSearchOptionsJs>,
    ) -> Result<SearchReportJs> {
        let report = self
            .inner
            .lock()
            .map_err(|_| Error::new(Status::GenericFailure, "index lock poisoned"))?
            .search_text(&text, k_from_js(options))
            .map_err(to_js_error)?;

        search_report_to_js(report)
    }

    #[napi(js_name = "searchHybrid")]
    pub fn search_hybrid(
        &self,
        query: HybridQueryJs,
        options: Option<HybridOptionsJs>,
    ) -> Result<Vec<String>> {
        let query = hybrid_query_from_js(query)?;
        let options = hybrid_options_from_js(options)?;
        let report = self
            .inner
            .lock()
            .map_err(|_| Error::new(Status::GenericFailure, "index lock poisoned"))?
            .search_hybrid(&query, options)
            .map_err(to_js_error)?;

        search_report_ids(report)
    }

    #[napi(js_name = "searchHybridWithReport")]
    pub fn search_hybrid_with_report(
        &self,
        query: HybridQueryJs,
        options: Option<HybridOptionsJs>,
    ) -> Result<SearchReportJs> {
        let query = hybrid_query_from_js(query)?;
        let options = hybrid_options_from_js(options)?;
        let report = self
            .inner
            .lock()
            .map_err(|_| Error::new(Status::GenericFailure, "index lock poisoned"))?
            .search_hybrid(&query, options)
            .map_err(to_js_error)?;

        search_report_to_js(report)
    }

    #[napi]
    pub fn search_batch_with_report(
        &self,
        queries: Vec<Vec<f64>>,
        options: Option<SearchOptionsJs>,
    ) -> Result<Vec<SearchReportJs>> {
        let options = options.unwrap_or_default();
        let mode = parse_mode(&options)?;
        let queries = queries
            .into_iter()
            .map(|query| query.into_iter().map(f64_to_f32).collect::<Vec<_>>())
            .collect::<Vec<_>>();
        let reports = self
            .inner
            .lock()
            .map_err(|_| Error::new(Status::GenericFailure, "index lock poisoned"))?
            .search_batch_with_report(&queries, search_options_from_js(&options, mode)?)
            .map_err(to_js_error)?;

        reports.into_iter().map(search_report_to_js).collect()
    }

    #[napi(js_name = "searchBatchWithReportBuffer")]
    pub fn search_batch_with_report_buffer(
        &self,
        queries: Float32Array,
        options: Option<SearchOptionsJs>,
    ) -> Result<Vec<SearchReportJs>> {
        let options = options.unwrap_or_default();
        let mode = parse_mode(&options)?;
        let index = self
            .inner
            .lock()
            .map_err(|_| Error::new(Status::GenericFailure, "index lock poisoned"))?;
        let dimensions = index.manifest().config.dimensions;
        let queries = vectors_from_flat_rows(queries.as_ref(), dimensions, "query buffer")?;
        let reports = index
            .search_batch_with_report(&queries, search_options_from_js(&options, mode)?)
            .map_err(to_js_error)?;

        reports.into_iter().map(search_report_to_js).collect()
    }

    #[napi]
    pub fn compact(&self, options: Option<CompactionOptionsJs>) -> Result<CompactionReportJs> {
        let options = options.unwrap_or_default();
        let all_matching = options.all_matching.unwrap_or(false);
        if all_matching && options.max_segments.is_some() {
            return Err(Error::new(
                Status::InvalidArg,
                "allMatching cannot be combined with maxSegments",
            ));
        }
        let max_segments = if all_matching {
            None
        } else {
            Some(options.max_segments.unwrap_or(
                u32::try_from(DEFAULT_COMPACTION_MAX_SEGMENTS).map_err(|_| {
                    Error::new(
                        Status::GenericFailure,
                        "default compaction batch is too large",
                    )
                })?,
            ) as usize)
        };
        let report = self
            .inner
            .lock()
            .map_err(|_| Error::new(Status::GenericFailure, "index lock poisoned"))?
            .compact(CompactionOptions {
                source_level: option_u32_to_u8(options.source_level, 0, "sourceLevel")?,
                target_level: option_u32_to_u8(options.target_level, 1, "targetLevel")?,
                max_segments,
                min_segments: options.min_segments.unwrap_or(2) as usize,
                target_segment_max_vectors: options
                    .target_segment_max_vectors
                    .map(|value| value as usize),
                target_segment_max_radius: options
                    .target_segment_max_radius
                    .map(|value| value as f32),
            })
            .map_err(to_js_error)?;

        compaction_report_to_js(report)
    }

    #[napi]
    pub fn delete(&self, ids: Vec<String>) -> Result<DeleteReportJs> {
        let report = self
            .inner
            .lock()
            .map_err(|_| Error::new(Status::GenericFailure, "index lock poisoned"))?
            .delete_with_report(ids)
            .map_err(to_js_error)?;
        delete_report_to_js(report)
    }

    #[napi]
    pub fn purge(&self) -> Result<PurgeReportJs> {
        let report = self
            .inner
            .lock()
            .map_err(|_| Error::new(Status::GenericFailure, "index lock poisoned"))?
            .purge_with_report()
            .map_err(to_js_error)?;
        purge_report_to_js(report)
    }

    #[napi]
    pub fn maintain(&self, options: Option<IncrementalOptionsJs>) -> Result<IncrementalReportJs> {
        let options = options.unwrap_or_default();
        let defaults = borsuk::IncrementalMaintenanceOptions::default();
        let report = self
            .inner
            .lock()
            .map_err(|_| Error::new(Status::GenericFailure, "index lock poisoned"))?
            .run_incremental_maintenance(borsuk::IncrementalMaintenanceOptions {
                max_segment_vectors: options
                    .max_segment_vectors
                    .map_or(defaults.max_segment_vectors, |value| value as usize),
                max_segment_radius: options.max_segment_radius.map(|value| value as f32),
                min_segment_vectors: options
                    .min_segment_vectors
                    .map_or(defaults.min_segment_vectors, |value| value as usize),
                max_operations: options
                    .max_operations
                    .map_or(defaults.max_operations, |value| value as usize),
            })
            .map_err(to_js_error)?;
        incremental_report_to_js(report)
    }

    #[napi]
    pub fn rebuild(&self, options: Option<RebuildOptionsJs>) -> Result<RebuildReportJs> {
        let options = options.unwrap_or_default();
        let report = self
            .inner
            .lock()
            .map_err(|_| Error::new(Status::GenericFailure, "index lock poisoned"))?
            .rebuild(RebuildOptions {
                source_level: option_u32_to_u8(options.source_level, 0, "sourceLevel")?,
                target_level: option_u32_to_u8(options.target_level, 1, "targetLevel")?,
                min_segments: options.min_segments.unwrap_or(1) as usize,
                target_segment_max_vectors: options
                    .target_segment_max_vectors
                    .map(|value| value as usize),
                delete_obsolete: options.delete_obsolete.unwrap_or(false),
            })
            .map_err(to_js_error)?;

        Ok(RebuildReportJs {
            compaction: compaction_report_to_js(report.compaction)?,
            garbage_collection: garbage_collection_report_to_js(report.garbage_collection)?,
        })
    }

    #[napi]
    pub fn gc_obsolete_segments(
        &self,
        options: Option<GarbageCollectionOptionsJs>,
    ) -> Result<GarbageCollectionReportJs> {
        let options = options.unwrap_or_default();
        let report = self
            .inner
            .lock()
            .map_err(|_| Error::new(Status::GenericFailure, "index lock poisoned"))?
            .gc_obsolete_segments(GarbageCollectionOptions {
                dry_run: options.dry_run.unwrap_or(true),
                min_age: duration_from_optional_millis(options.min_age_ms, "min_age_ms")?,
            })
            .map_err(to_js_error)?;

        garbage_collection_report_to_js(report)
    }
}

#[napi(js_name = "vectorDistance")]
pub fn vector_distance(metric: String, left: Vec<f64>, right: Vec<f64>) -> Result<f64> {
    let metric = metric.parse::<VectorMetric>().map_err(to_js_error)?;
    let left = left.into_iter().map(f64_to_f32).collect::<Vec<_>>();
    let right = right.into_iter().map(f64_to_f32).collect::<Vec<_>>();
    metric
        .distance(&left, &right)
        .map(f64::from)
        .map_err(to_js_error)
}

#[napi(js_name = "vectorMetricNames")]
pub fn vector_metric_names() -> Vec<String> {
    borsuk::vector_metric_names()
        .iter()
        .map(|name| (*name).to_string())
        .collect()
}

#[napi(js_name = "leafModeNames")]
pub fn leaf_mode_names() -> Vec<String> {
    borsuk::leaf_mode_names()
        .iter()
        .map(|name| (*name).to_string())
        .collect()
}

#[napi(js_name = "recallAtK")]
pub fn recall_at_k(exact_ids: Vec<String>, actual_ids: Vec<String>, k: u32) -> Result<f64> {
    borsuk::recall_at_k(&exact_ids, &actual_ids, k as usize)
        .map(f64::from)
        .map_err(to_js_error)
}

#[napi(js_name = "tieAwareRecallAtK")]
pub fn tie_aware_recall_at_k(
    exact_distances: Vec<f64>,
    actual_distances: Vec<f64>,
    k: u32,
) -> Result<f64> {
    let exact_distances = exact_distances
        .into_iter()
        .map(f64_to_f32)
        .collect::<Vec<_>>();
    let actual_distances = actual_distances
        .into_iter()
        .map(f64_to_f32)
        .collect::<Vec<_>>();
    borsuk::tie_aware_recall_at_k(&exact_distances, &actual_distances, k as usize)
        .map(f64::from)
        .map_err(to_js_error)
}

#[napi]
pub fn create(options: CreateOptions) -> Result<JsIndex> {
    let ram_budget_bytes = options
        .ram_budget
        .as_deref()
        .map(borsuk::parse_ram_budget)
        .transpose()
        .map_err(to_js_error)?;
    let dimensions = resolve_dimensions(options.dim, options.dimensions)?;
    let segment_max_vectors =
        resolve_segment_max_vectors(options.segment_size, options.segment_max_vectors)?;
    let metric = options
        .metric
        .parse::<VectorMetric>()
        .map_err(to_js_error)?;
    let index = BorsukIndex::create_with_cache_routing_page_fanout_and_graph_neighbors(
        IndexConfig {
            uri: options.uri,
            metric,
            dimensions,
            segment_max_vectors,
            ram_budget_bytes,
            text: options.text.unwrap_or(false),
            named_vectors: Default::default(),
        },
        options.cache_dir.map(PathBuf::from),
        options
            .routing_page_fanout
            .map(|value| value as usize)
            .unwrap_or(borsuk::DEFAULT_ROUTING_PAGE_FANOUT),
        options
            .graph_neighbors
            .map(|value| value as usize)
            .unwrap_or(borsuk::DEFAULT_GRAPH_NEIGHBORS),
    )
    .map_err(to_js_error)?;

    Ok(JsIndex {
        inner: Mutex::new(index),
    })
}

#[napi(js_name = "open")]
pub fn open_index(uri: String, options: Option<OpenOptionsJs>) -> Result<JsIndex> {
    let options = options.unwrap_or_default();
    open(
        uri,
        options.cache_dir,
        options.ram_budget,
        options.resident_routing.unwrap_or(false),
        options.cache_max_bytes,
    )
}

fn open(
    uri: String,
    cache_dir: Option<String>,
    ram_budget: Option<String>,
    resident_routing: bool,
    cache_max_bytes: Option<String>,
) -> Result<JsIndex> {
    let ram_budget_bytes = ram_budget
        .as_deref()
        .map(borsuk::parse_ram_budget)
        .transpose()
        .map_err(to_js_error)?;
    let cache_max_bytes = cache_max_bytes
        .as_deref()
        .map(|value| borsuk::parse_byte_size(value, "cache_max_bytes"))
        .transpose()
        .map_err(to_js_error)?;
    let index = BorsukIndex::open_with_options(
        &uri,
        OpenOptions {
            cache_dir: cache_dir.map(PathBuf::from),
            cache_max_bytes,
            ram_budget_bytes,
            resident_routing,
            ..OpenOptions::default()
        },
    )
    .map_err(to_js_error)?;
    Ok(JsIndex {
        inner: Mutex::new(index),
    })
}

fn duration_from_optional_millis(value: Option<f64>, field: &str) -> Result<Duration> {
    match value {
        Some(value) if value.is_finite() && value >= 0.0 => {
            Ok(Duration::from_secs_f64(value / 1_000.0))
        }
        Some(_) => Err(Error::new(
            Status::InvalidArg,
            format!("{field} must be a non-negative finite number when set"),
        )),
        None => Ok(borsuk::DEFAULT_GARBAGE_COLLECTION_MIN_AGE),
    }
}

fn resolve_dimensions(dim: Option<u32>, dimensions: Option<u32>) -> Result<usize> {
    match (dim, dimensions) {
        (Some(left), Some(right)) if left != right => Err(Error::new(
            Status::InvalidArg,
            "dim and dimensions disagree",
        )),
        (Some(value), _) | (_, Some(value)) => Ok(value as usize),
        (None, None) => Err(Error::new(
            Status::InvalidArg,
            "dim or dimensions is required",
        )),
    }
}

fn resolve_segment_max_vectors(
    segment_size: Option<u32>,
    segment_max_vectors: Option<u32>,
) -> Result<usize> {
    match (segment_size, segment_max_vectors) {
        (Some(left), Some(right)) if left != right => Err(Error::new(
            Status::InvalidArg,
            "segment_size and segment_max_vectors disagree",
        )),
        (Some(value), _) | (_, Some(value)) => Ok(value as usize),
        (None, None) => Ok(4096),
    }
}

fn parse_mode(options: &SearchOptionsJs) -> Result<SearchMode> {
    match options.mode.as_deref().unwrap_or("exact") {
        "exact" => Ok(SearchMode::Exact),
        "approx" => Ok(SearchMode::Approx {
            leaf_mode: options
                .leaf_mode
                .as_deref()
                .unwrap_or("graph")
                .parse::<LeafMode>()
                .map_err(to_js_error)?,
            eps: options.eps.map(f64_to_f32),
            max_segments: options.max_segments.map(|value| value as usize),
            max_bytes: option_byte_size_to_u64(
                options.max_bytes,
                options.max_bytes_text.as_deref(),
                "maxBytes",
            )?,
            max_latency_ms: options.max_latency_ms.map(u64::from),
            routing_page_overfetch: options.routing_page_overfetch.map(|value| value as usize),
            max_candidates_per_segment: options
                .max_candidates_per_segment
                .map(|value| value as usize),
        }),
        other => Err(Error::new(
            Status::InvalidArg,
            format!("unknown search mode `{other}`"),
        )),
    }
}

fn search_options_from_js(options: &SearchOptionsJs, mode: SearchMode) -> Result<SearchOptions> {
    let filter = options
        .filter
        .as_ref()
        .map(borsuk::Filter::from_json)
        .transpose()
        .map_err(to_js_error)?;
    Ok(SearchOptions {
        k: options.k.unwrap_or(10) as usize,
        mode,
        guaranteed_recall: options.guaranteed_recall.unwrap_or(false),
        prefetch_depth: options
            .prefetch_depth
            .map(|value| value as usize)
            .unwrap_or(borsuk::DEFAULT_SEARCH_PREFETCH_DEPTH),
        filter,
        include_metadata: options.include_metadata.unwrap_or(false),
        vector_name: String::new(),
    })
}

fn k_from_js(options: Option<KSearchOptionsJs>) -> usize {
    options.and_then(|options| options.k).unwrap_or(10) as usize
}

fn hybrid_query_from_js(query: HybridQueryJs) -> Result<HybridQuery> {
    let mut out = HybridQuery::new();
    if let Some(dense) = query.dense {
        out = out.with_dense(dense.into_iter().map(f64_to_f32).collect());
    }
    if let Some(text) = query.text {
        out = out.with_text(text);
    }
    Ok(out)
}

fn hybrid_options_from_js(options: Option<HybridOptionsJs>) -> Result<HybridOptions> {
    let options = options.unwrap_or_default();
    let mut out = HybridOptions::new(options.k.unwrap_or(10) as usize);
    let fusion = options.fusion.unwrap_or_else(|| "rrf".to_string());
    out.fusion = match fusion.as_str() {
        "rrf" => Fusion::Rrf {
            k: options.rrf_k.unwrap_or(60) as usize,
        },
        "weighted" => {
            let weights = options.weights.unwrap_or_else(|| vec![1.0, 1.0]);
            if weights.len() != 2 {
                return Err(Error::new(
                    Status::InvalidArg,
                    "weights must contain exactly two values",
                ));
            }
            Fusion::Weighted {
                dense: f64_to_f32(weights[0]),
                text: f64_to_f32(weights[1]),
            }
        }
        other => {
            return Err(Error::new(
                Status::InvalidArg,
                format!("unknown hybrid fusion `{other}`; expected 'rrf' or 'weighted'"),
            ));
        }
    };
    Ok(out)
}

fn f64_to_f32(value: f64) -> f32 {
    value as f32
}

fn option_f64_to_u64(value: Option<f64>, field: &str) -> Result<Option<u64>> {
    value.map_or(Ok(None), |actual| {
        if actual.is_finite() && actual >= 0.0 && actual.fract() == 0.0 && actual <= u64::MAX as f64
        {
            Ok(Some(actual as u64))
        } else {
            Err(Error::new(
                Status::InvalidArg,
                format!("{field} must be a non-negative integer"),
            ))
        }
    })
}

fn option_byte_size_to_u64(
    numeric: Option<f64>,
    text: Option<&str>,
    field: &str,
) -> Result<Option<u64>> {
    match (numeric, text) {
        (Some(_), Some(_)) => Err(Error::new(
            Status::InvalidArg,
            format!("{field} must be provided as either a number or a byte-size string"),
        )),
        (Some(value), None) => option_f64_to_u64(Some(value), field),
        (None, Some(value)) => borsuk::parse_byte_size(value, field)
            .map(Some)
            .map_err(to_js_error),
        (None, None) => Ok(None),
    }
}

fn usize_to_u32(value: usize) -> Result<u32> {
    u32::try_from(value).map_err(|_| Error::new(Status::GenericFailure, "value exceeds u32"))
}

fn index_stats_to_js(stats: borsuk::IndexStats) -> Result<IndexStatsJs> {
    Ok(IndexStatsJs {
        metric: stats.metric,
        dimensions: usize_to_u32(stats.dimensions)?,
        segment_max_vectors: usize_to_u32(stats.segment_max_vectors)?,
        ram_budget_bytes: stats.ram_budget_bytes.map(|value| value as f64),
        text: stats.text,
        sparse_encoded_vectors: usize_to_u32(stats.sparse_encoded_vectors)?,
        dense_encoded_vectors: usize_to_u32(stats.dense_encoded_vectors)?,
        manifest_version: stats.manifest_version as f64,
        routing_max_level: u32::from(stats.routing_max_level),
        routing_page_fanout: usize_to_u32(stats.routing_page_fanout)?,
        routing_leaf_pages: usize_to_u32(stats.routing_leaf_pages)?,
        routing_pages: usize_to_u32(stats.routing_pages)?,
        segments: usize_to_u32(stats.segments)?,
        records: usize_to_u32(stats.records)?,
        segment_bytes: stats.segment_bytes as f64,
        graph_bytes: stats.graph_bytes as f64,
        resident_bytes_estimate: stats.resident_bytes_estimate as f64,
    })
}

fn search_report_to_js(report: borsuk::SearchReport) -> Result<SearchReportJs> {
    let hits = report
        .hits
        .into_iter()
        .map(hit_to_js)
        .collect::<Result<Vec<_>>>()?;
    Ok(SearchReportJs {
        hits,
        leaf_mode: report.leaf_mode,
        termination_reason: report.termination_reason.to_string(),
        recall_guarantee: report.recall_guarantee.to_string(),
        segments_total: usize_to_u32(report.segments_total)?,
        segments_searched: usize_to_u32(report.segments_searched)?,
        segments_skipped: usize_to_u32(report.segments_skipped)?,
        routing_page_indexes_read: usize_to_u32(report.routing_page_indexes_read)?,
        routing_pages_read: usize_to_u32(report.routing_pages_read)?,
        bytes_read: report.bytes_read as f64,
        prefetched_bytes_unused: report.prefetched_bytes_unused as f64,
        graph_bytes_read: report.graph_bytes_read as f64,
        object_cache_hits: usize_to_u32(report.object_cache_hits)?,
        object_cache_misses: usize_to_u32(report.object_cache_misses)?,
        cache_repairs: usize_to_u32(report.cache_repairs)?,
        records_considered: usize_to_u32(report.records_considered)?,
        records_scored: usize_to_u32(report.records_scored)?,
        graph_candidates_added: usize_to_u32(report.graph_candidates_added)?,
        resident_bytes_estimate: report.resident_bytes_estimate as f64,
        elapsed_ms: u64_to_u32(report.elapsed_ms)?,
        requests: request_counts_to_js(report.requests),
        rows_evaluated: usize_to_u32(report.rows_evaluated)?,
        rows_passed_filter: usize_to_u32(report.rows_passed_filter)?,
        segments_pruned_by_filter: usize_to_u32(report.segments_pruned_by_filter)?,
    })
}

fn search_report_ids(report: borsuk::SearchReport) -> Result<Vec<String>> {
    report
        .hits
        .into_iter()
        .map(|hit| hit.id.to_utf8_string().map_err(to_js_error))
        .collect()
}

fn request_counts_to_js(counts: borsuk::RequestCounts) -> RequestCountsJs {
    RequestCountsJs {
        gets: counts.gets as f64,
        puts: counts.puts as f64,
        deletes: counts.deletes as f64,
        heads: counts.heads as f64,
        lists: counts.lists as f64,
        total: counts.total() as f64,
    }
}

fn add_report_to_js(report: borsuk::AddReport) -> Result<AddReportJs> {
    Ok(AddReportJs {
        segments_written: usize_to_u32(report.segments_written)?,
        graph_payloads_written: usize_to_u32(report.graph_payloads_written)?,
        manifest_tables_written: usize_to_u32(report.manifest_tables_written)?,
        routing_pages_written: usize_to_u32(report.routing_pages_written)?,
        total_bytes_written: report.total_bytes_written as f64,
        bytes_per_vector: report.bytes_per_vector,
        requests: request_counts_to_js(report.requests),
    })
}

fn delete_report_to_js(report: borsuk::DeleteReport) -> Result<DeleteReportJs> {
    Ok(DeleteReportJs {
        deleted: usize_to_u32(report.deleted)?,
        total_tombstoned: usize_to_u32(report.total_tombstoned)?,
        published: report.published,
        requests: request_counts_to_js(report.requests),
    })
}

fn purge_report_to_js(report: borsuk::PurgeReport) -> Result<PurgeReportJs> {
    Ok(PurgeReportJs {
        segments_rewritten: usize_to_u32(report.segments_rewritten)?,
        records_purged: usize_to_u32(report.records_purged)?,
        tombstones_cleared: usize_to_u32(report.tombstones_cleared)?,
        published: report.published,
        requests: request_counts_to_js(report.requests),
    })
}

fn incremental_report_to_js(report: borsuk::IncrementalReport) -> Result<IncrementalReportJs> {
    Ok(IncrementalReportJs {
        splits: usize_to_u32(report.splits)?,
        merges: usize_to_u32(report.merges)?,
        segments_created: usize_to_u32(report.segments_created)?,
        segments_removed: usize_to_u32(report.segments_removed)?,
        records_moved: usize_to_u32(report.records_moved)?,
        published: report.published,
        requests: request_counts_to_js(report.requests),
    })
}

fn compaction_report_to_js(report: borsuk::CompactionReport) -> Result<CompactionReportJs> {
    Ok(CompactionReportJs {
        compacted: report.compacted,
        source_level: u32::from(report.source_level),
        target_level: u32::from(report.target_level),
        segments_read: usize_to_u32(report.segments_read)?,
        segments_written: usize_to_u32(report.segments_written)?,
        records_rewritten: usize_to_u32(report.records_rewritten)?,
        routing_page_indexes_read: usize_to_u32(report.routing_page_indexes_read)?,
        routing_pages_read: usize_to_u32(report.routing_pages_read)?,
        routing_page_indexes_written: usize_to_u32(report.routing_page_indexes_written)?,
        routing_pages_written: usize_to_u32(report.routing_pages_written)?,
        graph_payloads_read: usize_to_u32(report.graph_payloads_read)?,
        graph_bytes_read: report.graph_bytes_read as f64,
        bytes_read: report.bytes_read as f64,
        bytes_written: report.bytes_written as f64,
        object_cache_hits: usize_to_u32(report.object_cache_hits)?,
        object_cache_misses: usize_to_u32(report.object_cache_misses)?,
        manifest_version: report.manifest_version as f64,
    })
}

fn garbage_collection_report_to_js(
    report: borsuk::GarbageCollectionReport,
) -> Result<GarbageCollectionReportJs> {
    Ok(GarbageCollectionReportJs {
        dry_run: report.dry_run,
        objects_scanned: usize_to_u32(report.objects_scanned)?,
        objects_deleted: usize_to_u32(report.objects_deleted)?,
        routing_objects_deleted: usize_to_u32(report.routing_objects_deleted)?,
        tables_deleted: usize_to_u32(report.tables_deleted)?,
        routing_page_indexes_read: usize_to_u32(report.routing_page_indexes_read)?,
        routing_pages_read: usize_to_u32(report.routing_pages_read)?,
        bytes_read: report.bytes_read as f64,
        bytes_reclaimable: report.bytes_reclaimable as f64,
        bytes_reclaimed: report.bytes_reclaimed as f64,
        object_cache_hits: usize_to_u32(report.object_cache_hits)?,
        object_cache_misses: usize_to_u32(report.object_cache_misses)?,
        candidates: report.candidates,
    })
}

fn ids_for_vectors(
    ids: Option<Vec<String>>,
    expected_len: usize,
    index: &BorsukIndex,
) -> Result<Vec<String>> {
    match ids {
        Some(ids) if ids.len() != expected_len => Err(Error::new(
            Status::InvalidArg,
            "ids must have the same length as vectors",
        )),
        Some(ids) => Ok(ids),
        None => index.generate_ids(expected_len).map_err(to_js_error),
    }
}

fn validate_optional_rows_len<T>(
    rows: &Option<Vec<T>>,
    field: &str,
    expected_len: usize,
) -> Result<()> {
    if let Some(rows) = rows
        && rows.len() != expected_len
    {
        return Err(Error::new(
            Status::InvalidArg,
            format!(
                "{field} length {} must match vectors length {expected_len}",
                rows.len()
            ),
        ));
    }
    Ok(())
}

fn id_bytes_for_vectors(ids: Vec<Uint8Array>, expected_len: usize) -> Result<Vec<Vec<u8>>> {
    if ids.len() != expected_len {
        return Err(Error::new(
            Status::InvalidArg,
            "ids must have the same length as vectors",
        ));
    }
    Ok(ids.into_iter().map(|id| id.as_ref().to_vec()).collect())
}

fn records_from_vectors(
    ids: &[String],
    vectors: Vec<Vec<f32>>,
    dimensions: usize,
    metadata: Option<&[borsuk::Metadata]>,
    sparse: Option<&[Option<SparseVectorJs>]>,
    text: Option<&[Option<String>]>,
) -> Result<Vec<VectorRecord>> {
    ids.iter()
        .cloned()
        .zip(vectors)
        .enumerate()
        .map(|(row, (id, vector))| {
            let mut record = if let Some(rows) = sparse
                && let Some(sparse) = &rows[row]
            {
                VectorRecord::from_sparse(
                    id,
                    sparse.indices.clone(),
                    sparse.values.iter().copied().map(f64_to_f32).collect(),
                    dimensions,
                )
                .map_err(to_js_error)?
            } else {
                VectorRecord::new(id, vector)
            };
            if let Some(rows) = metadata {
                record = record.with_metadata(rows[row].clone());
            }
            if let Some(rows) = text
                && let Some(text) = &rows[row]
            {
                record = record.with_text(text.clone());
            }
            Ok(record)
        })
        .collect()
}

fn records_from_flat_vectors(
    ids: Vec<String>,
    vectors: &[f32],
    dimensions: usize,
) -> Result<Vec<VectorRecord>> {
    if !vectors.len().is_multiple_of(dimensions) {
        return Err(Error::new(
            Status::InvalidArg,
            format!(
                "flat vector buffer length must be a multiple of index dimensions (dimensions {dimensions}, got {} float32 values)",
                vectors.len()
            ),
        ));
    }

    let expected_values = ids.len().checked_mul(dimensions).ok_or_else(|| {
        Error::new(
            Status::InvalidArg,
            "flat vector buffer length exceeds usize",
        )
    })?;
    if vectors.len() != expected_values {
        return Err(Error::new(
            Status::InvalidArg,
            format!(
                "flat vector buffer length must equal ids length * index dimensions (expected {expected_values} float32 values, got {})",
                vectors.len()
            ),
        ));
    }

    Ok(ids
        .into_iter()
        .zip(vectors.chunks_exact(dimensions))
        .map(|(id, vector)| VectorRecord::new(id, vector.to_vec()))
        .collect())
}

fn records_from_flat_vectors_with_id_bytes(
    ids: Vec<Vec<u8>>,
    vectors: &[f32],
    dimensions: usize,
) -> Result<Vec<VectorRecord>> {
    if !vectors.len().is_multiple_of(dimensions) {
        return Err(Error::new(
            Status::InvalidArg,
            format!(
                "flat vector buffer length must be a multiple of index dimensions (dimensions {dimensions}, got {} float32 values)",
                vectors.len()
            ),
        ));
    }

    let expected_values = ids.len().checked_mul(dimensions).ok_or_else(|| {
        Error::new(
            Status::InvalidArg,
            "flat vector buffer length exceeds usize",
        )
    })?;
    if vectors.len() != expected_values {
        return Err(Error::new(
            Status::InvalidArg,
            format!(
                "flat vector buffer length must equal ids length * index dimensions (expected {expected_values} float32 values, got {})",
                vectors.len()
            ),
        ));
    }

    Ok(ids
        .into_iter()
        .zip(vectors.chunks_exact(dimensions))
        .map(|(id, vector)| VectorRecord::new_bytes(id, vector.to_vec()))
        .collect())
}

fn id_bytes_to_js(ids: Vec<Vec<u8>>) -> Vec<Uint8Array> {
    ids.into_iter().map(Uint8Array::from).collect()
}

fn id_bytes_batch_to_js(id_batches: Vec<Vec<Vec<u8>>>) -> Vec<Vec<Uint8Array>> {
    id_batches.into_iter().map(id_bytes_to_js).collect()
}

fn flat_vector_row_count(vectors: &[f32], dimensions: usize) -> Result<usize> {
    if !vectors.len().is_multiple_of(dimensions) {
        return Err(Error::new(
            Status::InvalidArg,
            format!(
                "flat vector buffer length must be a multiple of index dimensions (dimensions {dimensions}, got {} float32 values)",
                vectors.len()
            ),
        ));
    }

    Ok(vectors.len() / dimensions)
}

fn vectors_from_flat_rows(
    vectors: &[f32],
    dimensions: usize,
    label: &str,
) -> Result<Vec<Vec<f32>>> {
    if dimensions == 0 {
        return Err(Error::new(
            Status::InvalidArg,
            "index dimensions must be greater than zero",
        ));
    }
    if !vectors.len().is_multiple_of(dimensions) {
        return Err(Error::new(
            Status::InvalidArg,
            format!(
                "flat {label} length must be a multiple of index dimensions ({dimensions}); got {} float32 values",
                vectors.len()
            ),
        ));
    }

    Ok(vectors
        .chunks_exact(dimensions)
        .map(<[f32]>::to_vec)
        .collect())
}

fn query_from_flat_vector(query: &[f32], dimensions: usize, label: &str) -> Result<Vec<f32>> {
    if query.len() != dimensions {
        return Err(Error::new(
            Status::InvalidArg,
            format!(
                "flat {label} length must equal index dimensions ({dimensions}); got {} float32 values",
                query.len()
            ),
        ));
    }

    Ok(query.to_vec())
}

fn hit_to_js(hit: borsuk::SearchHit) -> Result<Hit> {
    let id = hit
        .id
        .to_utf8_string()
        .unwrap_or_else(|_| hit.id.to_string());
    let id_bytes = Uint8Array::from(hit.id.as_bytes().to_vec());
    Ok(Hit {
        id,
        id_bytes,
        distance: f64::from(hit.distance),
        metadata: hit.metadata.as_ref().map(borsuk::metadata_to_json),
    })
}

fn option_u32_to_u8(value: Option<u32>, default: u8, field: &str) -> Result<u8> {
    value.map_or(Ok(default), |actual| {
        u8::try_from(actual).map_err(|_| {
            Error::new(
                Status::InvalidArg,
                format!("{field} must be between 0 and 255"),
            )
        })
    })
}

fn u64_to_u32(value: u64) -> Result<u32> {
    u32::try_from(value).map_err(|_| Error::new(Status::GenericFailure, "value exceeds u32"))
}

fn to_js_error(error: borsuk::BorsukError) -> Error {
    Error::new(
        Status::GenericFailure,
        format!("[borsuk:{}] {error}", error.code()),
    )
}
