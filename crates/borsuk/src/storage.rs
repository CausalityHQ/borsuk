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
        decode_current, encode_current, manifest_from_parquet, manifest_to_parquet,
        routing_to_parquet,
    },
    manifest::Manifest,
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

    pub(crate) fn publish_manifest(&self, manifest: &Manifest) -> Result<()> {
        self.write_bytes(&manifest.file_name(), &manifest_to_parquet(manifest)?)?;
        self.write_bytes(
            &manifest.routing_file_name(),
            &routing_to_parquet(manifest)?,
        )?;
        self.write_bytes(CURRENT, &encode_current(manifest.version))
    }

    pub(crate) fn load_current_manifest(&self) -> Result<Manifest> {
        if !self.exists(CURRENT)? {
            return Err(BorsukError::IndexNotFound(self.uri.clone()));
        }

        let version = decode_current(&self.read_bytes(CURRENT)?)?;
        let manifest_bytes = self.read_bytes(&Manifest::file_name_for_version(version))?;
        let routing_bytes = self.read_bytes(&Manifest::routing_file_name_for_version(version))?;
        manifest_from_parquet(&manifest_bytes, &routing_bytes)
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
        if let Some(bytes) = self.read_cache_file(relative)? {
            return Ok(bytes);
        }

        let size = self.object_size(relative)?;
        let bytes = self.read_range(relative, 0..size)?;
        self.write_cache_file(relative, &bytes)?;
        Ok(bytes)
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
            Ok(()) => Ok(true),
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
}

fn store_from_uri(uri: &str) -> Result<(Arc<dyn ObjectStore>, ObjectPath)> {
    if has_uri_scheme(uri) {
        let url = Url::parse(uri).map_err(|err| {
            BorsukError::InvalidStorage(format!("invalid storage URI `{uri}`: {err}"))
        })?;
        let (store, prefix) = parse_url_opts(&url, env::vars())?;
        return Ok((store.into(), prefix));
    }

    Ok((
        Arc::new(object_store::local::LocalFileSystem::new()),
        ObjectPath::parse(uri).map_err(|err| {
            BorsukError::InvalidStorage(format!("invalid local storage path `{uri}`: {err}"))
        })?,
    ))
}

fn has_uri_scheme(uri: &str) -> bool {
    uri.split_once(':').is_some_and(|(scheme, _)| {
        !scheme.is_empty()
            && scheme
                .chars()
                .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '+' | '-' | '.'))
    })
}

#[cfg(test)]
mod tests {
    use super::Storage;

    #[test]
    fn accepts_s3_compatible_uri() {
        let storage = Storage::from_uri("s3://vectors/indexes/docs.borsuk");

        assert!(
            storage.is_ok(),
            "S3-compatible URIs must be supported by the storage layer: {storage:?}"
        );
    }

    #[test]
    fn reads_byte_ranges_without_fetching_whole_object() {
        let dir = tempfile::tempdir().unwrap();
        let uri = format!("file://{}", dir.path().display());
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
        let uri = format!("file://{}", dir.path().display());
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
}
