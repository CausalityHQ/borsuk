//! One trusted root for the resident cosine graph's immutable local artifacts.
//! Object-store hydration must pin immutable objects before calling this loader.

use std::{
    fs,
    io::{self, Read},
    path::Path,
    sync::{Arc, RwLock},
};

use serde::Deserialize;
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::{
    pq64_nominee::{Pq64CosineView, Pq64Router},
    resident_fp16_tier::{ResidentFp16Error, ResidentFp16Tier, resident_plane_bytes},
    resident_vector_graph::{ResidentPqCosineGraph, ResidentVectorGraph},
};

pub(crate) const SCHEMA: &str = "borsuk-resident-graph-generation-v1";
pub(crate) const MAX_ROOT_BYTES: usize = 16 * 1024;

#[derive(Debug, Error)]
pub enum ResidentGraphGenerationError {
    #[error("resident graph generation I/O: {0}")]
    Io(#[from] io::Error),
    #[error("resident graph generation is invalid: {0}")]
    Invalid(&'static str),
    #[error("resident graph generation FP16/graph: {0}")]
    Tier(#[from] ResidentFp16Error),
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Artifact {
    pub(crate) bytes: u64,
    pub(crate) sha256: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Root {
    pub(crate) schema: String,
    pub(crate) generation: u64,
    pub(crate) source_sha256: String,
    pub(crate) rows: u64,
    pub(crate) dimensions: usize,
    pub(crate) plane: Artifact,
    pub(crate) graph: Artifact,
    pub(crate) map: Artifact,
    pub(crate) books: Artifact,
    pub(crate) codes: Artifact,
}

/// Owned, authenticated files for one immutable graph generation. Every
/// serving worker creates its own cosine view and graph workspace.
pub struct ResidentGraphGeneration {
    plane: ResidentFp16Tier,
    graph: ResidentVectorGraph,
    pq: Pq64Router,
    old_for_new: Vec<usize>,
}

/// Readers retain their authenticated generation while a replacement becomes
/// current. Callers must budget RAM for all generations still pinned by readers.
pub struct ResidentGraphSlot {
    current: RwLock<Arc<ResidentGraphGeneration>>,
}

impl ResidentGraphSlot {
    pub fn new(initial: Arc<ResidentGraphGeneration>) -> Self {
        Self {
            current: RwLock::new(initial),
        }
    }

    pub fn pin(&self) -> Arc<ResidentGraphGeneration> {
        self.current
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .clone()
    }

    /// Swap only after publication and hydration of the new head succeed.
    /// The returned old generation can be used to delay cache reclamation.
    pub fn replace(
        &self,
        next: Arc<ResidentGraphGeneration>,
    ) -> Result<Arc<ResidentGraphGeneration>, ResidentGraphGenerationError> {
        let mut current = self
            .current
            .write()
            .unwrap_or_else(|error| error.into_inner());
        if next.generation() <= current.generation() {
            return Err(ResidentGraphGenerationError::Invalid("generation order"));
        }
        Ok(std::mem::replace(&mut *current, next))
    }
}

pub(crate) fn valid_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

pub(crate) fn parse_authenticated_root(
    root_bytes: &[u8],
    trusted_root_sha256: &str,
) -> Result<Root, ResidentGraphGenerationError> {
    if root_bytes.len() > MAX_ROOT_BYTES
        || !valid_sha256(trusted_root_sha256)
        || format!("{:x}", Sha256::digest(root_bytes)) != trusted_root_sha256
    {
        return Err(ResidentGraphGenerationError::Invalid(
            "trusted root SHA-256",
        ));
    }
    let root: Root = serde_json::from_slice(root_bytes)
        .map_err(|_| ResidentGraphGenerationError::Invalid("root JSON"))?;
    if root.schema != SCHEMA
        || root.generation == 0
        || root.rows < 2
        || root.rows > u32::MAX as u64
        || root.dimensions == 0
        || !valid_sha256(&root.source_sha256)
        || [
            &root.plane,
            &root.graph,
            &root.map,
            &root.books,
            &root.codes,
        ]
        .iter()
        .any(|artifact| artifact.bytes == 0 || !valid_sha256(&artifact.sha256))
    {
        return Err(ResidentGraphGenerationError::Invalid("root identity"));
    }
    Ok(root)
}

#[cfg(test)]
fn digest_file(path: &Path) -> Result<String, io::Error> {
    let mut input = fs::File::open(path)?;
    let mut digest = Sha256::new();
    let mut block = [0_u8; 1024 * 1024];
    loop {
        let n = input.read(&mut block)?;
        if n == 0 {
            break;
        }
        digest.update(&block[..n]);
    }
    Ok(format!("{:x}", digest.finalize()))
}

fn read_authenticated(
    path: &Path,
    artifact: &Artifact,
) -> Result<Vec<u8>, ResidentGraphGenerationError> {
    if fs::metadata(path)?.len() != artifact.bytes {
        return Err(ResidentGraphGenerationError::Invalid("artifact length"));
    }
    let mut bytes = Vec::new();
    fs::File::open(path)?
        .take(artifact.bytes.saturating_add(1))
        .read_to_end(&mut bytes)?;
    if bytes.len() as u64 != artifact.bytes
        || format!("{:x}", Sha256::digest(&bytes)) != artifact.sha256
    {
        return Err(ResidentGraphGenerationError::Invalid(
            "artifact hash or length",
        ));
    }
    Ok(bytes)
}

pub(crate) fn preflight_root(
    root: &Root,
    max_resident_bytes: usize,
    active_workers: usize,
) -> Result<usize, ResidentGraphGenerationError> {
    let rows = usize::try_from(root.rows)
        .map_err(|_| ResidentGraphGenerationError::Invalid("row count"))?;
    if active_workers == 0 {
        return Err(ResidentGraphGenerationError::Invalid("worker count"));
    }
    let width = root.dimensions.div_ceil(64);
    let plane_bytes = resident_plane_bytes(root.rows, root.dimensions)?;
    let map_bytes = rows
        .checked_mul(4)
        .ok_or(ResidentGraphGenerationError::Invalid("map size"))?;
    let code_bytes = rows
        .checked_mul(64)
        .ok_or(ResidentGraphGenerationError::Invalid("code size"))?;
    let book_bytes = 64usize
        .checked_mul(256)
        .and_then(|n| n.checked_mul(width))
        .and_then(|n| n.checked_mul(4))
        .ok_or(ResidentGraphGenerationError::Invalid("book size"))?;
    let plane_file_bytes = plane_bytes
        .checked_add(64)
        .ok_or(ResidentGraphGenerationError::Invalid("plane size"))?;
    if root.plane.bytes != plane_file_bytes as u64
        || root.map.bytes != map_bytes as u64
        || root.codes.bytes != code_bytes as u64
        || root.books.bytes != book_bytes as u64
        || root.graph.bytes == 0
    {
        return Err(ResidentGraphGenerationError::Invalid("artifact geometry"));
    }
    let graph_bytes = usize::try_from(root.graph.bytes)
        .map_err(|_| ResidentGraphGenerationError::Invalid("graph size"))?;
    let map_resident_bytes = rows
        .checked_mul(std::mem::size_of::<usize>())
        .ok_or(ResidentGraphGenerationError::Invalid("map resident size"))?;
    let worker_bytes = rows
        .checked_mul(8)
        .and_then(|n| n.checked_mul(active_workers))
        .ok_or(ResidentGraphGenerationError::Invalid("worker size"))?;
    let floor = plane_bytes
        .checked_add(map_resident_bytes)
        .and_then(|n| n.checked_add(code_bytes))
        .and_then(|n| n.checked_add(book_bytes))
        .and_then(|n| n.checked_add(graph_bytes))
        .and_then(|n| n.checked_add(worker_bytes))
        .ok_or(ResidentGraphGenerationError::Invalid("resident size"))?;
    if floor > max_resident_bytes {
        return Err(ResidentGraphGenerationError::Invalid("resident cap"));
    }
    Ok(worker_bytes)
}

impl ResidentGraphGeneration {
    /// Open fixed artifact names from a fully hydrated directory. The digest
    /// must come from a separately trusted, conditional generation pointer.
    /// The cap includes modeled generation data and the declared workers'
    /// cosine views and visit workspaces, but not temporary decoder RSS.
    pub fn open_local_authenticated(
        root_bytes: &[u8],
        trusted_root_sha256: &str,
        directory: &Path,
        max_resident_bytes: usize,
        active_workers: usize,
    ) -> Result<Self, ResidentGraphGenerationError> {
        let root = parse_authenticated_root(root_bytes, trusted_root_sha256)?;
        let worker_bytes = preflight_root(&root, max_resident_bytes, active_workers)?;
        let rows = root.rows as usize;
        let dimensions = root.dimensions;
        let code_bytes = rows
            .checked_mul(64)
            .ok_or(ResidentGraphGenerationError::Invalid("code size"))?;
        let book_bytes = 64usize
            .checked_mul(256)
            .and_then(|n| n.checked_mul(dimensions.div_ceil(64)))
            .and_then(|n| n.checked_mul(4))
            .ok_or(ResidentGraphGenerationError::Invalid("book size"))?;
        let path = |name| directory.join(name);
        if fs::metadata(path("graph.bin"))?.len() != root.graph.bytes {
            return Err(ResidentGraphGenerationError::Invalid("graph length"));
        }
        let mapping = read_authenticated(&path("map.u32"), &root.map)?;
        let raw_books = read_authenticated(&path("books.bin"), &root.books)?;
        let codes = read_authenticated(&path("codes.bin"), &root.codes)?;
        let plane = ResidentFp16Tier::open_authenticated(
            &path("plane.bin"),
            &root.plane.sha256,
            &root.source_sha256,
            root.rows,
            dimensions,
            root.generation,
            max_resident_bytes,
        )?;
        let graph = ResidentVectorGraph::open_authenticated(
            &path("graph.bin"),
            &root.graph.sha256,
            &plane,
        )?;
        let old_for_new = mapping
            .chunks_exact(4)
            .map(|word| u32::from_le_bytes(word.try_into().unwrap()) as usize)
            .collect::<Vec<_>>();
        let books = raw_books
            .chunks_exact(4)
            .map(|word| f32::from_le_bytes(word.try_into().unwrap()))
            .collect::<Vec<_>>();
        let pq = Pq64Router::new(
            rows,
            dimensions,
            rows,
            1,
            vec![0.0; dimensions],
            books,
            codes,
        )
        .map_err(|_| ResidentGraphGenerationError::Invalid("PQ arrays"))?;
        // Bind once during loading so a bad permutation never reaches traffic.
        let view = pq
            .cosine_view()
            .map_err(|_| ResidentGraphGenerationError::Invalid("PQ cosine norms"))?;
        ResidentPqCosineGraph::bind(&graph, &plane, &view, &old_for_new)?;
        let map_allocation = old_for_new
            .capacity()
            .checked_mul(std::mem::size_of::<usize>())
            .ok_or(ResidentGraphGenerationError::Invalid("map resident size"))?;
        let actual = plane
            .resident_bytes()
            .checked_add(graph.heap_bytes())
            .and_then(|n| n.checked_add(map_allocation))
            .and_then(|n| n.checked_add(code_bytes))
            .and_then(|n| n.checked_add(book_bytes))
            .and_then(|n| n.checked_add(dimensions.checked_mul(4)?))
            .and_then(|n| n.checked_add(worker_bytes))
            .ok_or(ResidentGraphGenerationError::Invalid("resident size"))?;
        if actual > max_resident_bytes {
            return Err(ResidentGraphGenerationError::Invalid("resident cap"));
        }
        Ok(Self {
            plane,
            graph,
            pq,
            old_for_new,
        })
    }

    pub fn rows(&self) -> usize {
        self.plane.rows()
    }
    pub fn dimensions(&self) -> usize {
        self.plane.dimensions()
    }
    pub fn generation(&self) -> u64 {
        self.plane.generation()
    }
    pub(crate) fn source_id(&self, ordinal: usize) -> Result<u64, ResidentGraphGenerationError> {
        Ok(self.plane.source_id(ordinal)?)
    }
    pub fn cosine_view(&self) -> Result<Pq64CosineView<'_>, ResidentGraphGenerationError> {
        self.pq
            .cosine_view()
            .map_err(|_| ResidentGraphGenerationError::Invalid("PQ cosine norms"))
    }
    pub fn bind<'a, 'b>(
        &'a self,
        view: &'a Pq64CosineView<'b>,
    ) -> Result<ResidentPqCosineGraph<'a, 'b>, ResidentGraphGenerationError> {
        if !std::ptr::eq(view.router(), &self.pq) {
            return Err(ResidentGraphGenerationError::Invalid("foreign PQ view"));
        }
        Ok(ResidentPqCosineGraph::bind(
            &self.graph,
            &self.plane,
            view,
            &self.old_for_new,
        )?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::resident_fp16_tier::write_resident_fp16_tier;
    use crate::resident_graph_store::{
        hydrate_graph_generation, publish_graph_generation, read_graph_head,
    };
    use crate::resident_graph_overlay::{ResidentGraphOverlay, ResidentMutation};
    use crate::resident_vector_graph::GraphSearchWorkspace;
    use object_store::{memory::InMemory, path::Path as ObjectPath};

    #[test]
    fn trusted_root_loads_and_corrupt_artifact_fails_closed() {
        let dir = tempfile::tempdir().unwrap();
        let source = "11".repeat(32);
        let vectors = vec![
            vec![1.0, 0.0],
            vec![0.9, 0.1],
            vec![0.0, 1.0],
            vec![-1.0, 0.0],
        ];
        let path = dir.path().join("plane.bin");
        let plane_sha = write_resident_fp16_tier(
            &path,
            4,
            2,
            7,
            &source,
            [42, 7, 19, 33].into_iter().zip(vectors.iter().cloned()),
        )
        .unwrap();
        let plane =
            ResidentFp16Tier::open_authenticated(&path, &plane_sha, &source, 4, 2, 7, 48).unwrap();
        let graph = ResidentVectorGraph::build(&vectors, &plane, 4, 4, 8).unwrap();
        graph
            .write_authenticated(&dir.path().join("graph.bin"))
            .unwrap();
        fs::write(
            dir.path().join("map.u32"),
            (0..4_u32).flat_map(u32::to_le_bytes).collect::<Vec<_>>(),
        )
        .unwrap();
        let mut books = vec![0.0_f32; 64 * 256];
        books[31 * 256 + 1] = 1.0;
        books[63 * 256] = 0.1;
        fs::write(
            dir.path().join("books.bin"),
            books
                .iter()
                .flat_map(|x| x.to_le_bytes())
                .collect::<Vec<_>>(),
        )
        .unwrap();
        let mut codes = vec![0_u8; 4 * 64];
        codes[31] = 1;
        codes[64 + 31] = 1;
        fs::write(dir.path().join("codes.bin"), codes).unwrap();
        let artifact = |name: &str| {
            let file = dir.path().join(name);
            serde_json::json!({"bytes":fs::metadata(&file).unwrap().len(),
                "sha256":digest_file(&file).unwrap()})
        };
        let root = serde_json::json!({
            "schema":SCHEMA,"generation":7,"source_sha256":source,
            "rows":4,"dimensions":2,"plane":artifact("plane.bin"),
            "graph":artifact("graph.bin"),"map":artifact("map.u32"),
            "books":artifact("books.bin"),"codes":artifact("codes.bin")
        })
        .to_string();
        let trusted = format!("{:x}", Sha256::digest(root.as_bytes()));
        let loaded = ResidentGraphGeneration::open_local_authenticated(
            root.as_bytes(),
            &trusted,
            dir.path(),
            100_000,
            1,
        )
        .unwrap();
        assert_eq!(
            (loaded.rows(), loaded.dimensions(), loaded.generation()),
            (4, 2, 7)
        );
        let view = loaded.cosine_view().unwrap();
        let bound = loaded.bind(&view).unwrap();
        let foreign = Pq64Router::new(
            4,
            2,
            4,
            1,
            vec![0.0; 2],
            vec![1.0; 64 * 256],
            vec![0; 4 * 64],
        )
        .unwrap();
        assert!(loaded.bind(&foreign.cosine_view().unwrap()).is_err());
        let mut workspace = GraphSearchWorkspace::new(4).unwrap();
        assert_eq!(
            bound
                .search(&[1.0, 0.0], 2, 4, 2, &mut workspace)
                .unwrap()
                .0,
            vec![42, 7]
        );
        let store = InMemory::new();
        let prefix = ObjectPath::from("graph");
        let cache = tempfile::tempdir().unwrap();
        let original_head = tokio::runtime::Runtime::new().unwrap().block_on(async {
            publish_graph_generation(&store, &prefix, root.as_bytes(), dir.path(), None)
                .await
                .unwrap();
            let head = read_graph_head(&store, &prefix).await.unwrap().unwrap();
            assert!(
                hydrate_graph_generation(&store, &prefix, &head, cache.path(), 100, 1)
                    .await
                    .is_err()
            );
            assert_eq!(fs::read_dir(cache.path()).unwrap().count(), 0);
            let (hydrated, cold) =
                hydrate_graph_generation(&store, &prefix, &head, cache.path(), 100_000, 1)
                    .await
                    .unwrap();
            assert_eq!(hydrated.generation(), 7);
            assert_eq!(cold.object_gets, 5);
            let (_, warm) =
                hydrate_graph_generation(&store, &prefix, &head, cache.path(), 100_000, 1)
                    .await
                    .unwrap();
            assert_eq!(warm.object_gets, 0);
            head
        });
        drop(bound);
        drop(view);
        let pinned = Arc::new(loaded);
        let slot = ResidentGraphSlot::new(Arc::clone(&pinned));
        let next_dir = tempfile::tempdir().unwrap();
        let next_source = "22".repeat(32);
        let next_vectors = vec![
            vec![0.0, 1.0],
            vec![0.9, 0.1],
            vec![0.0, 1.0],
            vec![-1.0, 0.0],
        ];
        let next_plane_path = next_dir.path().join("plane.bin");
        let next_plane_sha = write_resident_fp16_tier(
            &next_plane_path,
            4,
            2,
            8,
            &next_source,
            [99, 7, 19, 33]
                .into_iter()
                .zip(next_vectors.iter().cloned()),
        )
        .unwrap();
        let next_plane = ResidentFp16Tier::open_authenticated(
            &next_plane_path,
            &next_plane_sha,
            &next_source,
            4,
            2,
            8,
            48,
        )
        .unwrap();
        ResidentVectorGraph::build(&next_vectors, &next_plane, 4, 4, 8)
            .unwrap()
            .write_authenticated(&next_dir.path().join("graph.bin"))
            .unwrap();
        for name in ["map.u32", "books.bin", "codes.bin"] {
            fs::copy(dir.path().join(name), next_dir.path().join(name)).unwrap();
        }
        let next_artifact = |name: &str| {
            let file = next_dir.path().join(name);
            serde_json::json!({"bytes":fs::metadata(&file).unwrap().len(),
                "sha256":digest_file(&file).unwrap()})
        };
        let next_root = serde_json::json!({
            "schema":SCHEMA,"generation":8,"source_sha256":next_source,
            "rows":4,"dimensions":2,"plane":next_artifact("plane.bin"),
            "graph":next_artifact("graph.bin"),"map":next_artifact("map.u32"),
            "books":next_artifact("books.bin"),"codes":next_artifact("codes.bin")
        })
        .to_string();
        let next = Arc::new(tokio::runtime::Runtime::new().unwrap().block_on(async {
            let published = publish_graph_generation(
                &store,
                &prefix,
                next_root.as_bytes(),
                next_dir.path(),
                Some(&original_head),
            )
            .await
            .unwrap();
            let readback = read_graph_head(&store, &prefix).await.unwrap().unwrap();
            assert_eq!(published.root_sha256, readback.root_sha256);
            hydrate_graph_generation(&store, &prefix, &readback, cache.path(), 100_000, 1)
                .await
                .unwrap()
                .0
        }));
        let held_reader = slot.pin();
        let retiring = slot.replace(Arc::clone(&next)).unwrap();
        assert!(slot.replace(Arc::clone(&retiring)).is_err());
        assert_eq!(slot.pin().generation(), 8);
        assert_eq!(held_reader.generation(), 7);
        assert!(Arc::ptr_eq(&held_reader, &retiring));
        let old_view = held_reader.cosine_view().unwrap();
        let old_graph = held_reader.bind(&old_view).unwrap();
        let new_view = next.cosine_view().unwrap();
        let new_graph = next.bind(&new_view).unwrap();
        let mut old_workspace = GraphSearchWorkspace::new(4).unwrap();
        let mut new_workspace = GraphSearchWorkspace::new(4).unwrap();
        assert!(
            old_graph
                .search(&[1.0, 0.0], 4, 4, 4, &mut old_workspace)
                .unwrap()
                .0
                .contains(&42)
        );
        let new_ids = new_graph
            .search(&[1.0, 0.0], 4, 4, 4, &mut new_workspace)
            .unwrap()
            .0;
        assert!(new_ids.contains(&99) && !new_ids.contains(&42));
        let empty_overlay = ResidentGraphOverlay::new(Arc::clone(&held_reader), vec![], 0).unwrap();
        let empty_bound = empty_overlay.bind(&old_view).unwrap();
        assert_eq!(
            empty_bound
                .search(&[1.0, 0.0], 2, 4, 2, &mut old_workspace)
                .unwrap()
                .0,
            old_graph
                .search(&[1.0, 0.0], 2, 4, 2, &mut old_workspace)
                .unwrap()
                .0
        );
        let mutations = || vec![
            ResidentMutation { id: 42, vector: Some(vec![0.0, 1.0]) },
            ResidentMutation { id: 7, vector: None },
            ResidentMutation { id: 99, vector: Some(vec![1.0, 0.0]) },
        ];
        assert!(ResidentGraphOverlay::new(Arc::clone(&held_reader), mutations(), 39).is_err());
        let overlay = ResidentGraphOverlay::new(Arc::clone(&held_reader), mutations(), 40).unwrap();
        let overlay_bound = overlay.bind(&old_view).unwrap();
        let (ids, stats) = overlay_bound
            .search(&[1.0, 0.0], 4, 4, 4, &mut old_workspace)
            .unwrap();
        assert_eq!(ids, vec![99, 19, 42, 33]);
        assert_eq!(stats.delta_rows_scanned, 2);
        assert_eq!(stats.masked_shortlist_rows, 2);
        assert!(ResidentGraphOverlay::new(
            Arc::clone(&held_reader),
            vec![ResidentMutation { id: 42, vector: None }, ResidentMutation { id: 42, vector: None }],
            0,
        ).is_err());
        assert!(
            ResidentGraphGeneration::open_local_authenticated(
                root.as_bytes(),
                &"00".repeat(32),
                dir.path(),
                100_000,
                1
            )
            .is_err()
        );
        assert!(
            ResidentGraphGeneration::open_local_authenticated(
                root.as_bytes(),
                &trusted,
                dir.path(),
                100,
                1
            )
            .is_err()
        );
        fs::write(dir.path().join("map.u32"), [0_u8; 16]).unwrap();
        assert!(
            ResidentGraphGeneration::open_local_authenticated(
                root.as_bytes(),
                &trusted,
                dir.path(),
                100_000,
                1
            )
            .is_err()
        );
    }
}
