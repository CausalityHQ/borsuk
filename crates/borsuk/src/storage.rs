use std::{
    env, fmt, fs, io,
    ops::Range,
    path::{Path, PathBuf},
    sync::Arc,
};

use bytes::Bytes;
use futures_util::TryStreamExt;
use object_store::{
    ObjectStore, ObjectStoreExt, PutPayload, parse_url_opts, path::Path as ObjectPath,
};
use tokio::runtime::{Builder, Runtime};
use url::Url;

use crate::{
    error::{BorsukError, Result},
    format::{
        CurrentPointer, current_metadata_checksum, current_table_checksum, decode_current,
        encode_current, manifest_from_parquet, manifest_has_next_generated_id,
        manifest_metadata_from_parquet, manifest_to_parquet, pivots_from_parquet,
        pivots_to_parquet, routing_layer_page_index_from_parquet,
        routing_layer_page_index_to_parquet, routing_layer_page_to_parquet, routing_to_parquet,
        segment_from_parquet,
    },
    manifest::{Manifest, ROUTING_PAGE_FANOUT, RoutingLayerPageRef, SegmentSummary},
};

const CURRENT: &str = "CURRENT";

#[derive(Clone)]
pub(crate) struct Storage {
    uri: String,
    store: Arc<dyn ObjectStore>,
    prefix: ObjectPath,
    cache_dir: Option<PathBuf>,
    runtime: Arc<Runtime>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StoredObject {
    pub path: String,
    pub size: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ReadBytes {
    pub bytes: Vec<u8>,
    pub cache_hit: bool,
}

#[derive(Debug, Clone)]
pub(crate) struct RoutingLayerPageIndexRead {
    pub page_refs: Vec<RoutingLayerPageRef>,
    pub bytes_read: u64,
    pub page_indexes_read: usize,
    pub object_cache_hits: usize,
    pub object_cache_misses: usize,
}

impl fmt::Debug for Storage {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Storage")
            .field("uri", &self.uri)
            .field("prefix", &self.prefix)
            .field("cache_dir", &self.cache_dir)
            .finish_non_exhaustive()
    }
}

impl Storage {
    pub(crate) fn from_uri(uri: &str) -> Result<Self> {
        Self::from_uri_with_cache(uri, None)
    }

    pub(crate) fn from_uri_with_cache(uri: &str, cache_dir: Option<PathBuf>) -> Result<Self> {
        let (store, prefix) = store_from_uri(uri)?;
        let runtime = Builder::new_multi_thread()
            .enable_all()
            .build()
            .map_err(|err| {
                BorsukError::InvalidStorage(format!("failed to create storage runtime: {err}"))
            })?;

        Ok(Self {
            uri: uri.to_string(),
            store,
            prefix,
            cache_dir,
            runtime: Arc::new(runtime),
        })
    }

    pub(crate) fn create_layout(&self) -> Result<()> {
        Ok(())
    }

    pub(crate) fn publish_manifest(&self, manifest: &Manifest) -> Result<Manifest> {
        self.publish_manifest_reusing_routing_pages(manifest, None)
    }

    pub(crate) fn publish_manifest_reusing_routing_pages(
        &self,
        manifest: &Manifest,
        previous: Option<&Manifest>,
    ) -> Result<Manifest> {
        let page_refs = self.routing_layer_page_refs(manifest, previous, 0)?;
        self.publish_manifest_with_routing_page_refs(manifest, &page_refs)
    }

    pub(crate) fn publish_manifest_with_routing_page_refs(
        &self,
        manifest: &Manifest,
        page_refs: &[RoutingLayerPageRef],
    ) -> Result<Manifest> {
        let mut manifest = manifest.clone();
        manifest.set_routing_max_level_for_leaf_pages(page_refs.len())?;
        self.publish_manifest_metadata(&manifest)?;
        self.write_routing_layer_page_indexes(&manifest, page_refs)?;
        Ok(manifest)
    }

    pub(crate) fn publish_manifest_with_top_routing_page_refs(
        &self,
        manifest: &Manifest,
        routing_level: u8,
        page_refs: &[RoutingLayerPageRef],
    ) -> Result<Manifest> {
        let mut manifest = manifest.clone();
        manifest.routing_max_level = routing_level;
        self.publish_manifest_metadata(&manifest)?;
        let page_index_bytes =
            routing_layer_page_index_to_parquet(&manifest, routing_level, page_refs)?;
        self.write_bytes(
            &Manifest::routing_layer_page_index_file_name(manifest.version, routing_level),
            &page_index_bytes,
        )?;
        Ok(manifest)
    }

    fn publish_manifest_metadata(&self, manifest: &Manifest) -> Result<()> {
        let manifest_bytes = manifest_to_parquet(manifest)?;
        let routing_bytes = routing_to_parquet(manifest)?;
        let pivots_bytes = pivots_to_parquet(manifest)?;
        let manifest_checksum = current_table_checksum(&manifest_bytes);
        let routing_checksum = current_table_checksum(&routing_bytes);
        let pivots_checksum = current_table_checksum(&pivots_bytes);

        self.write_bytes(&manifest.file_name(), &manifest_bytes)?;
        self.write_bytes(&manifest.routing_file_name(), &routing_bytes)?;
        self.write_bytes(&manifest.pivots_file_name(), &pivots_bytes)?;
        self.write_bytes(
            CURRENT,
            &encode_current(
                manifest.version,
                manifest_checksum,
                routing_checksum,
                pivots_checksum,
            ),
        )?;
        Ok(())
    }

    fn write_routing_layer_page_indexes(
        &self,
        manifest: &Manifest,
        leaf_page_refs: &[RoutingLayerPageRef],
    ) -> Result<()> {
        let mut routing_level = 0_u8;
        let mut page_refs = leaf_page_refs.to_vec();
        loop {
            let page_index_bytes =
                routing_layer_page_index_to_parquet(manifest, routing_level, &page_refs)?;
            self.write_bytes(
                &Manifest::routing_layer_page_index_file_name(manifest.version, routing_level),
                &page_index_bytes,
            )?;

            if page_refs.len() <= 1 {
                break;
            }

            routing_level = routing_level.checked_add(1).ok_or_else(|| {
                BorsukError::InvalidStorage("routing layer depth exceeds u8".to_string())
            })?;
            page_refs = self.parent_routing_layer_page_refs(manifest, routing_level, &page_refs)?;
        }

        Ok(())
    }

    pub(crate) fn write_routing_layer_page(
        &self,
        manifest: &Manifest,
        routing_level: u8,
        page_ordinal: usize,
        segments: &[SegmentSummary],
    ) -> Result<RoutingLayerPageRef> {
        let bytes = routing_layer_page_to_parquet(
            manifest,
            routing_level,
            page_ordinal,
            page_ordinal * ROUTING_PAGE_FANOUT,
            segments,
        )?;
        let checksum = blake3::hash(&bytes).to_hex().to_string();
        let path = Manifest::routing_layer_page_content_file_name(routing_level, &checksum);
        if !self.exists(&path)? {
            self.write_bytes(&path, &bytes)?;
        }
        Ok(RoutingLayerPageRef {
            routing_level,
            page_ordinal,
            path,
            checksum,
            page_segments: segments.len(),
            leaf_segments: segments.len(),
            dimensions: manifest.config.dimensions,
            centroid: routing_layer_page_centroid(manifest.config.dimensions, segments),
            radius: routing_layer_page_radius(manifest, segments)?,
            bounds_min: routing_layer_page_bounds_min(manifest.config.dimensions, segments),
            bounds_max: routing_layer_page_bounds_max(manifest.config.dimensions, segments),
            id_bloom: routing_layer_page_id_bloom(segments),
            vector_signature_bloom: routing_layer_page_vector_signature_bloom(segments),
            level_mask: routing_layer_page_level_mask(segments),
            page_records: routing_layer_page_record_count(segments),
            page_segment_bytes: routing_layer_page_segment_bytes(segments),
            page_graph_bytes: routing_layer_page_graph_bytes(segments),
        })
    }

    fn routing_layer_page_refs(
        &self,
        manifest: &Manifest,
        previous: Option<&Manifest>,
        routing_level: u8,
    ) -> Result<Vec<RoutingLayerPageRef>> {
        let previous_refs = previous
            .map(|previous| self.read_routing_layer_page_index(previous.version, routing_level))
            .transpose()?
            .unwrap_or_default();
        let mut page_refs = Vec::new();

        for (page_ordinal, segments) in manifest.segments.chunks(ROUTING_PAGE_FANOUT).enumerate() {
            if let Some(previous_manifest) = previous
                && routing_layer_page_unchanged(previous_manifest, page_ordinal, segments)
                && let Some(page_ref) = previous_refs.get(page_ordinal)
            {
                page_refs.push(page_ref.clone());
                continue;
            }

            page_refs.push(self.write_routing_layer_page(
                manifest,
                routing_level,
                page_ordinal,
                segments,
            )?);
        }

        Ok(page_refs)
    }

    fn parent_routing_layer_page_refs(
        &self,
        manifest: &Manifest,
        routing_level: u8,
        child_refs: &[RoutingLayerPageRef],
    ) -> Result<Vec<RoutingLayerPageRef>> {
        child_refs
            .chunks(ROUTING_PAGE_FANOUT)
            .enumerate()
            .map(|(page_ordinal, children)| {
                self.write_parent_routing_layer_page(
                    manifest,
                    routing_level,
                    page_ordinal,
                    children,
                )
            })
            .collect()
    }

    pub(crate) fn write_parent_routing_layer_page(
        &self,
        manifest: &Manifest,
        routing_level: u8,
        page_ordinal: usize,
        child_refs: &[RoutingLayerPageRef],
    ) -> Result<RoutingLayerPageRef> {
        let child_routing_level = routing_level.checked_sub(1).ok_or_else(|| {
            BorsukError::InvalidStorage("parent routing layer must be above L0".to_string())
        })?;
        let bytes = routing_layer_page_index_to_parquet(manifest, child_routing_level, child_refs)?;
        let checksum = blake3::hash(&bytes).to_hex().to_string();
        let path = Manifest::routing_layer_page_content_file_name(routing_level, &checksum);
        if !self.exists(&path)? {
            self.write_bytes(&path, &bytes)?;
        }

        Ok(RoutingLayerPageRef {
            routing_level,
            page_ordinal,
            path,
            checksum,
            page_segments: child_refs.len(),
            leaf_segments: routing_page_refs_leaf_segments(child_refs),
            dimensions: manifest.config.dimensions,
            centroid: routing_page_refs_centroid(manifest.config.dimensions, child_refs),
            radius: routing_page_refs_radius(manifest, child_refs)?,
            bounds_min: routing_page_refs_bounds_min(manifest.config.dimensions, child_refs),
            bounds_max: routing_page_refs_bounds_max(manifest.config.dimensions, child_refs),
            id_bloom: routing_page_refs_id_bloom(child_refs),
            vector_signature_bloom: routing_page_refs_vector_signature_bloom(child_refs),
            level_mask: routing_page_refs_level_mask(child_refs),
            page_records: routing_page_refs_record_count(child_refs),
            page_segment_bytes: routing_page_refs_segment_bytes(child_refs),
            page_graph_bytes: routing_page_refs_graph_bytes(child_refs),
        })
    }

    pub(crate) fn read_routing_layer_page_index(
        &self,
        version: u64,
        routing_level: u8,
    ) -> Result<Vec<RoutingLayerPageRef>> {
        Ok(self
            .read_routing_layer_page_index_with_status(version, routing_level)?
            .page_refs)
    }

    pub(crate) fn read_routing_layer_page_index_with_status(
        &self,
        version: u64,
        routing_level: u8,
    ) -> Result<RoutingLayerPageIndexRead> {
        let path = Manifest::routing_layer_page_index_file_name(version, routing_level);
        match self.read_bytes_with_cache_status(&path) {
            Ok(read) => Ok(RoutingLayerPageIndexRead {
                page_refs: routing_layer_page_index_from_parquet(
                    &read.bytes,
                    version,
                    routing_level,
                )?,
                bytes_read: read.bytes.len() as u64,
                page_indexes_read: 1,
                object_cache_hits: usize::from(read.cache_hit),
                object_cache_misses: usize::from(!read.cache_hit),
            }),
            Err(BorsukError::ObjectStore(object_store::Error::NotFound { .. })) => {
                Ok(RoutingLayerPageIndexRead {
                    page_refs: Vec::new(),
                    bytes_read: 0,
                    page_indexes_read: 0,
                    object_cache_hits: 0,
                    object_cache_misses: 0,
                })
            }
            Err(err) => Err(err),
        }
    }

    pub(crate) fn load_current_manifest(&self) -> Result<Manifest> {
        if !self.exists(CURRENT)? {
            return Err(BorsukError::IndexNotFound(self.uri.clone()));
        }

        let pointer = decode_current(&self.read_bytes_uncached(CURRENT)?)?;
        let manifest_bytes = self.read_current_metadata_table(
            &Manifest::file_name_for_version(pointer.version),
            pointer.version,
            "manifest",
            pointer.manifest_checksum,
        )?;
        let routing_bytes = self.read_current_metadata_table(
            &Manifest::routing_file_name_for_version(pointer.version),
            pointer.version,
            "routing",
            pointer.routing_checksum,
        )?;
        let pivots_bytes = self.read_current_metadata_table(
            &Manifest::pivots_file_name_for_version(pointer.version),
            pointer.version,
            "pivots",
            pointer.pivots_checksum,
        )?;
        validate_current_metadata(
            &pointer,
            &manifest_bytes,
            Some(&routing_bytes),
            Some(&pivots_bytes),
        )?;

        let manifest_stores_next_generated_id = manifest_has_next_generated_id(&manifest_bytes)?;
        let mut manifest = manifest_from_parquet(&manifest_bytes, &routing_bytes)?;
        if manifest.version != pointer.version {
            return Err(BorsukError::InvalidStorage(format!(
                "CURRENT points to manifest version {}, but manifest table contains version {}",
                pointer.version, manifest.version
            )));
        }
        if !manifest_stores_next_generated_id {
            manifest.next_generated_id =
                self.derive_legacy_next_generated_id_from_segments(&manifest)?;
        }
        manifest.pivots =
            pivots_from_parquet(&pivots_bytes, manifest.config.dimensions, manifest.version)?;
        Ok(manifest)
    }

    pub(crate) fn load_current_manifest_metadata(&self) -> Result<Manifest> {
        if !self.exists(CURRENT)? {
            return Err(BorsukError::IndexNotFound(self.uri.clone()));
        }

        let pointer = decode_current(&self.read_bytes_uncached(CURRENT)?)?;
        let manifest_bytes = self.read_current_metadata_table(
            &Manifest::file_name_for_version(pointer.version),
            pointer.version,
            "manifest",
            pointer.manifest_checksum,
        )?;
        if pointer.manifest_checksum.is_some() {
            validate_current_metadata(&pointer, &manifest_bytes, None, None)?;
        } else {
            let routing_bytes = self.read_current_metadata_table(
                &Manifest::routing_file_name_for_version(pointer.version),
                pointer.version,
                "routing",
                pointer.routing_checksum,
            )?;
            let pivots_bytes = self.read_current_metadata_table(
                &Manifest::pivots_file_name_for_version(pointer.version),
                pointer.version,
                "pivots",
                pointer.pivots_checksum,
            )?;
            validate_current_metadata(
                &pointer,
                &manifest_bytes,
                Some(&routing_bytes),
                Some(&pivots_bytes),
            )?;
        }

        if !manifest_has_next_generated_id(&manifest_bytes)? {
            let mut manifest = self.load_current_manifest()?;
            manifest.segments.clear();
            manifest.pivots.clear();
            return Ok(manifest);
        }

        let manifest = manifest_metadata_from_parquet(&manifest_bytes)?;
        if manifest.version != pointer.version {
            return Err(BorsukError::InvalidStorage(format!(
                "CURRENT points to manifest version {}, but manifest table contains version {}",
                pointer.version, manifest.version
            )));
        }
        Ok(manifest)
    }

    fn derive_legacy_next_generated_id_from_segments(&self, manifest: &Manifest) -> Result<u64> {
        let mut next_generated_id = manifest.next_generated_id;
        for summary in &manifest.segments {
            let bytes = self.read_bytes(&summary.path)?;
            let checksum = blake3::hash(&bytes).to_hex().to_string();
            if checksum != summary.checksum {
                return Err(BorsukError::ChecksumMismatch {
                    path: summary.path.clone(),
                    expected: summary.checksum.clone(),
                    actual: checksum,
                });
            }

            let segment = segment_from_parquet(&bytes)?;
            for record in segment.records {
                if let Ok(id) = record.id.parse::<u64>() {
                    next_generated_id = next_generated_id.max(id.saturating_add(1));
                }
            }
        }
        Ok(next_generated_id)
    }

    fn read_current_metadata_table(
        &self,
        relative: &str,
        version: u64,
        table_name: &str,
        expected_checksum: Option<[u8; 32]>,
    ) -> Result<Vec<u8>> {
        let read = self.read_bytes_with_cache_status(relative)?;
        let Some(expected_checksum) = expected_checksum else {
            return Ok(read.bytes);
        };
        if current_table_checksum(&read.bytes) == expected_checksum {
            return Ok(read.bytes);
        }
        if !read.cache_hit {
            validate_current_table_checksum(version, table_name, &read.bytes, expected_checksum)?;
            return Ok(read.bytes);
        }

        self.delete_cache_file(relative)?;
        let fresh_bytes = self.read_bytes_uncached(relative)?;
        validate_current_table_checksum(version, table_name, &fresh_bytes, expected_checksum)?;
        Ok(fresh_bytes)
    }

    pub(crate) fn write_bytes(&self, relative: &str, bytes: &[u8]) -> Result<()> {
        let location = self.resolve(relative)?;
        let payload = PutPayload::from(Bytes::copy_from_slice(bytes));
        self.runtime
            .block_on(async { self.store.put(&location, payload).await })?;
        self.write_cache_file(relative, bytes)?;
        Ok(())
    }

    pub(crate) fn read_bytes(&self, relative: &str) -> Result<Vec<u8>> {
        Ok(self.read_bytes_with_cache_status(relative)?.bytes)
    }

    fn read_bytes_uncached(&self, relative: &str) -> Result<Vec<u8>> {
        let size = self.object_size(relative)?;
        let location = self.resolve(relative)?;
        let bytes = self
            .runtime
            .block_on(async { self.store.get_range(&location, 0..size).await })?
            .to_vec();
        self.write_cache_file(relative, &bytes)?;
        Ok(bytes)
    }

    pub(crate) fn read_bytes_with_cache_status(&self, relative: &str) -> Result<ReadBytes> {
        if let Some(bytes) = self.read_cache_file(relative)? {
            return Ok(ReadBytes {
                bytes,
                cache_hit: true,
            });
        }

        let size = self.object_size(relative)?;
        let bytes = self.read_range(relative, 0..size)?;
        self.write_cache_file(relative, &bytes)?;
        Ok(ReadBytes {
            bytes,
            cache_hit: false,
        })
    }

    pub(crate) fn read_range(&self, relative: &str, range: Range<u64>) -> Result<Vec<u8>> {
        if let Some(bytes) = self.read_cache_file(relative)? {
            let start = usize::try_from(range.start).map_err(|_| {
                BorsukError::InvalidStorage(format!(
                    "range start {} does not fit usize",
                    range.start
                ))
            })?;
            let end = usize::try_from(range.end).map_err(|_| {
                BorsukError::InvalidStorage(format!("range end {} does not fit usize", range.end))
            })?;
            if end > bytes.len() || start > end {
                return Err(BorsukError::InvalidStorage(format!(
                    "range {}..{} is outside cached object `{relative}` of {} bytes",
                    range.start,
                    range.end,
                    bytes.len()
                )));
            }
            return Ok(bytes[start..end].to_vec());
        }

        let location = self.resolve(relative)?;
        let bytes = self
            .runtime
            .block_on(async { self.store.get_range(&location, range).await })?;
        Ok(bytes.to_vec())
    }

    pub(crate) fn list_objects(&self, relative_prefix: &str) -> Result<Vec<StoredObject>> {
        let prefix = self.resolve(relative_prefix)?;
        let metas = self
            .runtime
            .block_on(async { self.store.list(Some(&prefix)).try_collect::<Vec<_>>().await })?;
        let mut objects = metas
            .into_iter()
            .map(|meta| {
                Ok(StoredObject {
                    path: self.relative_path(&meta.location)?,
                    size: meta.size,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        objects.sort_by(|left, right| left.path.cmp(&right.path));
        Ok(objects)
    }

    pub(crate) fn delete_object(&self, relative: &str) -> Result<bool> {
        let location = self.resolve(relative)?;
        match self
            .runtime
            .block_on(async { self.store.delete(&location).await })
        {
            Ok(()) => {
                self.delete_cache_file(relative)?;
                Ok(true)
            }
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(err) => Err(err.into()),
        }
    }

    fn object_size(&self, relative: &str) -> Result<u64> {
        let location = self.resolve(relative)?;
        let meta = self
            .runtime
            .block_on(async { self.store.head(&location).await })?;
        Ok(meta.size)
    }

    fn exists(&self, relative: &str) -> Result<bool> {
        let location = self.resolve(relative)?;
        match self
            .runtime
            .block_on(async { self.store.head(&location).await })
        {
            Ok(_) => Ok(true),
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(err) => Err(err.into()),
        }
    }

    fn resolve(&self, relative: &str) -> Result<ObjectPath> {
        let relative = relative.trim_matches('/');
        let path = if self.prefix.as_ref().is_empty() {
            relative.to_string()
        } else if relative.is_empty() {
            self.prefix.as_ref().to_string()
        } else {
            format!("{}/{relative}", self.prefix.as_ref())
        };

        ObjectPath::parse(path).map_err(|err| {
            BorsukError::InvalidStorage(format!("invalid object path `{relative}`: {err}"))
        })
    }

    fn relative_path(&self, location: &ObjectPath) -> Result<String> {
        let path = location.as_ref();
        let prefix = self.prefix.as_ref();
        if prefix.is_empty() {
            return Ok(path.to_string());
        }

        path.strip_prefix(prefix)
            .and_then(|value| value.strip_prefix('/'))
            .map(ToString::to_string)
            .ok_or_else(|| {
                BorsukError::InvalidStorage(format!(
                    "listed object `{path}` is outside index prefix `{prefix}`"
                ))
            })
    }

    fn cache_path(&self, relative: &str) -> Option<PathBuf> {
        let cache_dir = self.cache_dir.as_ref()?;
        let mut path = cache_dir.clone();
        for component in Path::new(relative.trim_matches('/')).components() {
            if let std::path::Component::Normal(value) = component {
                path.push(value);
            }
        }
        Some(path)
    }

    fn read_cache_file(&self, relative: &str) -> Result<Option<Vec<u8>>> {
        let Some(path) = self.cache_path(relative) else {
            return Ok(None);
        };

        match fs::read(&path) {
            Ok(bytes) => Ok(Some(bytes)),
            Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(err) => Err(BorsukError::InvalidStorage(format!(
                "failed to read cache file `{}`: {err}",
                path.display()
            ))),
        }
    }

    fn write_cache_file(&self, relative: &str, bytes: &[u8]) -> Result<()> {
        let Some(path) = self.cache_path(relative) else {
            return Ok(());
        };

        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|err| {
                BorsukError::InvalidStorage(format!(
                    "failed to create cache directory `{}`: {err}",
                    parent.display()
                ))
            })?;
        }
        fs::write(&path, bytes).map_err(|err| {
            BorsukError::InvalidStorage(format!(
                "failed to write cache file `{}`: {err}",
                path.display()
            ))
        })
    }

    fn delete_cache_file(&self, relative: &str) -> Result<()> {
        let Some(path) = self.cache_path(relative) else {
            return Ok(());
        };

        match fs::remove_file(&path) {
            Ok(()) => Ok(()),
            Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(err) => Err(BorsukError::InvalidStorage(format!(
                "failed to delete cache file `{}`: {err}",
                path.display()
            ))),
        }
    }
}

fn validate_current_metadata(
    pointer: &CurrentPointer,
    manifest_bytes: &[u8],
    routing_bytes: Option<&[u8]>,
    pivots_bytes: Option<&[u8]>,
) -> Result<()> {
    if let Some(manifest_checksum) = pointer.manifest_checksum {
        validate_current_table_checksum(
            pointer.version,
            "manifest",
            manifest_bytes,
            manifest_checksum,
        )?;
        if let (Some(routing_checksum), Some(routing_bytes)) =
            (pointer.routing_checksum, routing_bytes)
        {
            validate_current_table_checksum(
                pointer.version,
                "routing",
                routing_bytes,
                routing_checksum,
            )?;
        }
        if let (Some(pivots_checksum), Some(pivots_bytes)) = (pointer.pivots_checksum, pivots_bytes)
        {
            validate_current_table_checksum(
                pointer.version,
                "pivots",
                pivots_bytes,
                pivots_checksum,
            )?;
        }
        return Ok(());
    }

    let Some(routing_bytes) = routing_bytes else {
        return Err(BorsukError::InvalidStorage(
            "CURRENT v1 metadata validation requires routing bytes".to_string(),
        ));
    };
    let Some(pivots_bytes) = pivots_bytes else {
        return Err(BorsukError::InvalidStorage(
            "CURRENT v1 metadata validation requires pivot bytes".to_string(),
        ));
    };
    let actual_checksum = current_metadata_checksum(manifest_bytes, routing_bytes, pivots_bytes);
    if actual_checksum != pointer.metadata_checksum {
        return Err(BorsukError::InvalidStorage(format!(
            "CURRENT metadata checksum mismatch for manifest version {}",
            pointer.version
        )));
    }
    Ok(())
}

fn validate_current_table_checksum(
    version: u64,
    table_name: &str,
    bytes: &[u8],
    expected_checksum: [u8; 32],
) -> Result<()> {
    let actual_checksum = current_table_checksum(bytes);
    if actual_checksum != expected_checksum {
        return Err(BorsukError::InvalidStorage(format!(
            "CURRENT metadata checksum mismatch for manifest version {version} ({table_name} table)"
        )));
    }
    Ok(())
}

fn store_from_uri(uri: &str) -> Result<(Arc<dyn ObjectStore>, ObjectPath)> {
    if has_uri_scheme(uri) {
        let url = Url::parse(uri).map_err(|err| {
            BorsukError::InvalidStorage(format!("invalid storage URI `{uri}`: {err}"))
        })?;
        let (store, prefix) = parse_url_opts(&url, env::vars())?;
        return Ok((store.into(), prefix));
    }

    let path = Path::new(uri);
    fs::create_dir_all(path).map_err(|source| BorsukError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    Ok((
        Arc::new(object_store::local::LocalFileSystem::new_with_prefix(path)?),
        ObjectPath::parse("").map_err(|err| {
            BorsukError::InvalidStorage(format!("invalid local storage root `{uri}`: {err}"))
        })?,
    ))
}

fn has_uri_scheme(uri: &str) -> bool {
    if looks_like_windows_drive_path(uri) {
        return false;
    }

    uri.split_once(':').is_some_and(|(scheme, _)| {
        !scheme.is_empty()
            && scheme
                .chars()
                .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '+' | '-' | '.'))
    })
}

fn routing_layer_page_unchanged(
    previous: &Manifest,
    page_ordinal: usize,
    segments: &[SegmentSummary],
) -> bool {
    let start = page_ordinal * ROUTING_PAGE_FANOUT;
    let end = start + segments.len();
    previous
        .segments
        .get(start..end)
        .is_some_and(|previous_segments| previous_segments == segments)
}

fn routing_layer_page_centroid(dimensions: usize, segments: &[SegmentSummary]) -> Vec<f32> {
    let total_objects = segments
        .iter()
        .map(|segment| segment.object_count)
        .sum::<usize>()
        .max(1);
    let mut centroid = vec![0.0_f32; dimensions];
    for segment in segments {
        let weight = segment.object_count as f32 / total_objects as f32;
        for (coordinate, value) in centroid.iter_mut().zip(&segment.centroid) {
            *coordinate += value * weight;
        }
    }
    centroid
}

fn routing_layer_page_radius(manifest: &Manifest, segments: &[SegmentSummary]) -> Result<f32> {
    let centroid = routing_layer_page_centroid(manifest.config.dimensions, segments);
    segments.iter().try_fold(0.0_f32, |radius, segment| {
        let center_distance = manifest
            .config
            .metric
            .distance(&centroid, &segment.centroid)?;
        Ok(radius.max(center_distance + segment.radius))
    })
}

fn routing_layer_page_bounds_min(dimensions: usize, segments: &[SegmentSummary]) -> Vec<f32> {
    let mut bounds = vec![f32::INFINITY; dimensions];
    for segment in segments {
        if segment.bounds_min.len() != dimensions {
            return Vec::new();
        }
        for (target, source) in bounds.iter_mut().zip(&segment.bounds_min) {
            *target = target.min(*source);
        }
    }
    bounds
}

fn routing_layer_page_bounds_max(dimensions: usize, segments: &[SegmentSummary]) -> Vec<f32> {
    let mut bounds = vec![f32::NEG_INFINITY; dimensions];
    for segment in segments {
        if segment.bounds_max.len() != dimensions {
            return Vec::new();
        }
        for (target, source) in bounds.iter_mut().zip(&segment.bounds_max) {
            *target = target.max(*source);
        }
    }
    bounds
}

fn routing_layer_page_id_bloom(segments: &[SegmentSummary]) -> Vec<u8> {
    let mut bloom = vec![0_u8; crate::manifest::SEGMENT_ID_BLOOM_BYTES];
    for segment in segments {
        if segment.id_bloom.len() != bloom.len() {
            return Vec::new();
        }
        for (target, source) in bloom.iter_mut().zip(&segment.id_bloom) {
            *target |= source;
        }
    }
    bloom
}

fn routing_layer_page_vector_signature_bloom(segments: &[SegmentSummary]) -> Vec<u8> {
    let mut bloom = vec![0_u8; crate::manifest::SEGMENT_VECTOR_SIGNATURE_BLOOM_BYTES];
    for segment in segments {
        if segment.vector_signature_bloom.len() != bloom.len() {
            return Vec::new();
        }
        for (target, source) in bloom.iter_mut().zip(&segment.vector_signature_bloom) {
            *target |= source;
        }
    }
    bloom
}

fn routing_layer_page_level_mask(segments: &[SegmentSummary]) -> u64 {
    let mut mask = 0_u64;
    for segment in segments {
        if segment.level >= u64::BITS as u8 {
            return u64::MAX;
        }
        mask |= 1_u64 << segment.level;
    }
    mask
}

fn routing_layer_page_record_count(segments: &[SegmentSummary]) -> usize {
    segments.iter().map(|segment| segment.object_count).sum()
}

fn routing_layer_page_segment_bytes(segments: &[SegmentSummary]) -> u64 {
    segments.iter().map(|segment| segment.size_bytes).sum()
}

fn routing_layer_page_graph_bytes(segments: &[SegmentSummary]) -> u64 {
    segments
        .iter()
        .map(|segment| segment.graph_size_bytes)
        .sum()
}

fn routing_page_refs_centroid(dimensions: usize, page_refs: &[RoutingLayerPageRef]) -> Vec<f32> {
    let total_records = page_refs
        .iter()
        .map(|page_ref| page_ref.page_records)
        .sum::<usize>()
        .max(1);
    let mut centroid = vec![0.0_f32; dimensions];
    for page_ref in page_refs {
        let weight = page_ref.page_records as f32 / total_records as f32;
        for (coordinate, value) in centroid.iter_mut().zip(&page_ref.centroid) {
            *coordinate += value * weight;
        }
    }
    centroid
}

fn routing_page_refs_radius(manifest: &Manifest, page_refs: &[RoutingLayerPageRef]) -> Result<f32> {
    let centroid = routing_page_refs_centroid(manifest.config.dimensions, page_refs);
    page_refs.iter().try_fold(0.0_f32, |radius, page_ref| {
        let center_distance = manifest
            .config
            .metric
            .distance(&centroid, &page_ref.centroid)?;
        Ok(radius.max(center_distance + page_ref.radius))
    })
}

fn routing_page_refs_bounds_min(dimensions: usize, page_refs: &[RoutingLayerPageRef]) -> Vec<f32> {
    let mut bounds = vec![f32::INFINITY; dimensions];
    for page_ref in page_refs {
        if page_ref.bounds_min.len() != dimensions {
            return Vec::new();
        }
        for (target, source) in bounds.iter_mut().zip(&page_ref.bounds_min) {
            *target = target.min(*source);
        }
    }
    bounds
}

fn routing_page_refs_bounds_max(dimensions: usize, page_refs: &[RoutingLayerPageRef]) -> Vec<f32> {
    let mut bounds = vec![f32::NEG_INFINITY; dimensions];
    for page_ref in page_refs {
        if page_ref.bounds_max.len() != dimensions {
            return Vec::new();
        }
        for (target, source) in bounds.iter_mut().zip(&page_ref.bounds_max) {
            *target = target.max(*source);
        }
    }
    bounds
}

fn routing_page_refs_id_bloom(page_refs: &[RoutingLayerPageRef]) -> Vec<u8> {
    let mut bloom = vec![0_u8; crate::manifest::SEGMENT_ID_BLOOM_BYTES];
    for page_ref in page_refs {
        if page_ref.id_bloom.len() != bloom.len() {
            return Vec::new();
        }
        for (target, source) in bloom.iter_mut().zip(&page_ref.id_bloom) {
            *target |= source;
        }
    }
    bloom
}

fn routing_page_refs_vector_signature_bloom(page_refs: &[RoutingLayerPageRef]) -> Vec<u8> {
    let mut bloom = vec![0_u8; crate::manifest::SEGMENT_VECTOR_SIGNATURE_BLOOM_BYTES];
    for page_ref in page_refs {
        if page_ref.vector_signature_bloom.len() != bloom.len() {
            return Vec::new();
        }
        for (target, source) in bloom.iter_mut().zip(&page_ref.vector_signature_bloom) {
            *target |= source;
        }
    }
    bloom
}

fn routing_page_refs_level_mask(page_refs: &[RoutingLayerPageRef]) -> u64 {
    let mut mask = 0_u64;
    for page_ref in page_refs {
        if page_ref.level_mask == u64::MAX {
            return u64::MAX;
        }
        mask |= page_ref.level_mask;
    }
    mask
}

fn routing_page_refs_record_count(page_refs: &[RoutingLayerPageRef]) -> usize {
    page_refs.iter().map(|page_ref| page_ref.page_records).sum()
}

fn routing_page_refs_leaf_segments(page_refs: &[RoutingLayerPageRef]) -> usize {
    page_refs
        .iter()
        .map(|page_ref| page_ref.leaf_segments)
        .sum()
}

fn routing_page_refs_segment_bytes(page_refs: &[RoutingLayerPageRef]) -> u64 {
    page_refs
        .iter()
        .map(|page_ref| page_ref.page_segment_bytes)
        .sum()
}

fn routing_page_refs_graph_bytes(page_refs: &[RoutingLayerPageRef]) -> u64 {
    page_refs
        .iter()
        .map(|page_ref| page_ref.page_graph_bytes)
        .sum()
}

fn looks_like_windows_drive_path(uri: &str) -> bool {
    let bytes = uri.as_bytes();
    bytes.len() >= 3
        && bytes[0].is_ascii_alphabetic()
        && bytes[1] == b':'
        && matches!(bytes[2], b'/' | b'\\')
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::Storage;
    use url::Url;

    #[test]
    fn accepts_s3_compatible_uri() {
        let storage = Storage::from_uri("s3://vectors/indexes/docs-index");

        assert!(
            storage.is_ok(),
            "S3-compatible URIs must be supported by the storage layer: {storage:?}"
        );
    }

    #[test]
    fn windows_drive_paths_are_local_paths_not_uri_schemes() {
        assert!(!super::has_uri_scheme("C:\\Users\\borsuk\\index"));
        assert!(!super::has_uri_scheme("D:/data/borsuk-index"));
    }

    #[test]
    fn reads_byte_ranges_without_fetching_whole_object() {
        let dir = tempfile::tempdir().unwrap();
        let uri = file_uri(dir.path());
        let storage = Storage::from_uri(&uri).unwrap();

        storage
            .write_bytes("segments/L0/aa/test.bin", b"0123456789")
            .unwrap();

        let range = storage.read_range("segments/L0/aa/test.bin", 2..6).unwrap();

        assert_eq!(range, b"2345");
    }

    #[test]
    fn lists_and_deletes_objects_relative_to_index_root() {
        let dir = tempfile::tempdir().unwrap();
        let uri = file_uri(dir.path());
        let storage = Storage::from_uri(&uri).unwrap();

        storage.write_bytes("segments/L0/aa/a.bin", b"aaa").unwrap();
        storage
            .write_bytes("segments/L1/bb/b.bin", b"bbbb")
            .unwrap();

        let listed = storage.list_objects("segments").unwrap();

        assert_eq!(
            listed
                .iter()
                .map(|object| (object.path.as_str(), object.size))
                .collect::<Vec<_>>(),
            vec![("segments/L0/aa/a.bin", 3), ("segments/L1/bb/b.bin", 4)]
        );
        assert!(storage.delete_object("segments/L0/aa/a.bin").unwrap());
        assert!(!storage.delete_object("segments/L0/aa/a.bin").unwrap());
        assert_eq!(
            storage
                .list_objects("segments")
                .unwrap()
                .iter()
                .map(|object| object.path.as_str())
                .collect::<Vec<_>>(),
            vec!["segments/L1/bb/b.bin"]
        );
    }

    fn file_uri(path: &Path) -> String {
        Url::from_directory_path(path).unwrap().to_string()
    }
}
