# BORSUK — Named vectors, each dense-or-sparse

**Status:** approved model (2026-07-09); implementation in progress.
**NOT a "v2":** this is a direct breaking change to the existing library (unreleased) — modify in place, no v1/v2 split, no migration/compat readers, no parallel structures. `format_version` stays 1.

## Goal

A record has one or more **named vectors** (default single unnamed vector = today's simple API), plus **optional metadata** and **optional text** (BM25 lexical). Each named vector lives in its own D-dim space + metric. A vector is stored **dense OR sparse purely to minimize bytes** (same space; sparse omits zeros; dot/distance/centroid identical). One search path — routing → leaves → match — handles a dense-or-sparse query identically. Hybrid fuses across named vectors + text (RRF default / weighted). Shape matches Qdrant/Milvus/Pinecone so drop-in adapters are lossless.

## Architecture decision — least-invasive coordinator (default vector UNCHANGED + named sub-indexes)

CRITICAL for safe autonomous execution: **the single/default vector keeps today's exact on-disk layout at `<root>/` — byte-identical, zero test churn.** Named vectors are ADDITIVE sub-indexes; nothing about the common single-vector path changes.

- **Default (primary) vector**: stays `VectorRecord.vector` (name `""`), stored in the current top-level index at `<root>/` using the EXISTING engine completely unchanged. `IndexConfig.metric`/`dimensions` describe it. This is why nothing breaks.
- **Additional named vectors**: `VectorRecord.extra_vectors: BTreeMap<String, Vector>` (default empty). Declared at create via `IndexConfig.named_vectors: BTreeMap<String, VectorSpec{dimensions, metric}>` (default empty). Each declared name gets its OWN sub-index at `<root>/vectors/<name>/`, reusing the whole current engine instance per name (routing/segments/compaction/GC/fidx filtering). Each vector (default or named) is dense-or-sparse encoded via the Phase C `StorageEncoding` (already shipped).
- **Coordinator** = `BorsukIndex` becomes a thin wrapper: create sets up the primary index + a sub-index per declared name; `add` writes each record's primary vector to the primary index and its `extra_vectors[name]` to sub-index `name` (records lacking a given name simply aren't added to that sub-index — nullable per record); `search(query, vector=name, ...)` routes to the primary index (`""`) or sub-index `name`; compaction/GC/maintain/stats fan out to all.
- **Metadata**: duplicated into each sub-index a record touches, so per-name filtered search prunes via the existing fidx path. Small (metadata << vectors); single-vector case unaffected.
- **Text/BM25**: a dedicated text sub-index at `<root>/text/` keyed by record id (reuse text.rs/bm25.rs), independent of the vector names.
- **Publish**: for a first version each sub-index publishes INDEPENDENTLY (its own `<root>/vectors/<name>/CURRENT`), primary last. Per-name search is always self-consistent; cross-name (hybrid) tolerates a brief lag. A top-level atomic `{name→version}` CURRENT is a later hardening, not required for correctness of per-name search.

## Sub-stage plan (each a gated, pushed-green Codex stage)
- **NV1 Rust core**: `VectorRecord.extra_vectors` + `IndexConfig.named_vectors` + coordinator (create/add/search-by-name/compact/gc/stats fan-out to per-name sub-indexes; metadata duplicated). Default path unchanged. Tests: create with a named vector, add primary+named, search each name in isolation, correctness + isolation.
- **NV2 hybrid over names**: extend `search_hybrid` to fuse across multiple named vectors + text (RRF/weighted). `HybridQuery { vectors: BTreeMap<name, Vector>, text }`.
- **NV3 bindings**: Py/TS/CLI named-vector API — `create(named_vectors=...)`, `add(..., vectors={name: dense|sparse})`, `search(vector=name)`, hybrid across names.
- **NV4 drop-in adapters**: Pinecone `values`→default + `sparse_values`→named `"sparse"`; Qdrant/Milvus named vectors→named vectors. Update python/TS compat adapters + parity.
- **NV5 docs**: api.md/README/architecture/web named-vectors + drop-in + landscape.

## Record & API model

- `Record { id, vectors: Map<name, Vector>, metadata?: Metadata, text?: String }`. `Vector` = dense `Vec<f32>` OR sparse `{indices, values}`, same D per name.
- Default name (e.g. `""`) so `add(id, vec)` / `search(query, k)` keep working unchanged (single unnamed vector).
- Create schema: `vectors: Map<name, {dimensions, metric}>` (default: one unnamed `{dimensions, metric}` = today). Optional `text: TextConfig` (tokenizer default UnicodeWordLowercase, code-side per [[borsuk-sparse-bm25]]).
- **Storage heuristic:** store a vector sparse iff `nnz*2 < dimensions` (sparse costs index+value per nnz), else dense; per-add override `Encoding::{Auto, Dense, Sparse}`. Always exact.

## Search

- `search(query, k, { vector: name = default, filter?, ... }) -> hits`. `query` is dense or sparse; routes through name's tree; leaf match uses representation-agnostic dot/distance. Results identical to the other encoding.
- Hybrid: `search_hybrid({ vectors: {name → query}, text? }, { k, fusion })` fuses per-name ranked lists + text via RRF (default)/weighted (reuse Stage 3a).

## Phase C core change (the hard part)

Generalise the vector operations to a representation-agnostic `Vector` (dense slice OR sparse pairs): `dot`, `distance` (all metrics), `norm`, centroid accumulation, radius, routing/PQ code derivation, and leaf scoring must accept either encoding and agree numerically. Segment vector column becomes: an encoding tag + either the fixed dense array or the (indices,values) lists (reuse Stage 1b sparse columns). The separate `sidx`/`search_sparse` retrieval is REMOVED — sparse rides the routing tree. Caveat: very high-D sparse degrades centroid/PQ routing vs a true inverted index — correctness holds; optional inverted-index leaf acceleration later.

## Phased plan

- **A. Model + coordinator skeleton.** New `Record`/create-schema types; coordinator that, for a single default named vector, delegates to one sub-index (behaviour-identical to today). Reuse existing engine as the sub-index. Migrate existing single-vector API onto the default name. Gate green.
- **B. Multiple named vectors.** Coordinator manages N sub-indexes + top-level CURRENT atomic publish; add routes named vectors; search by name; per-name compaction/GC/maintenance. Metadata co-located per name.
- **C. Dense-or-sparse encoding on one path.** Representation-agnostic vector ops + segment encoding tag + size heuristic + override; remove `sidx`/`search_sparse`.
- **D. Text + hybrid.** Text sub-index; `search_hybrid` fusing named vectors + text.
- **E. Bindings + adapters + docs.** Py/TS/CLI named-vector API; drop-in adapters map Pinecone `values`/`sparse_values`, Qdrant/Milvus named vectors; docs + landscape.

Each phase: Codex implements from a self-contained spec → I gate (fmt, clippy --all-targets, tests, bindings, policy, docs-web) → review → commit → push. Every pushed commit green.
