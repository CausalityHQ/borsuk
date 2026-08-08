# Standard Durable Formats Migration Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make every production BORSUK object independently readable by stock Parquet, Arrow IPC, or JSON tooling, with no bespoke persistent framing, while retaining efficient typed vector access.

**Architecture:** Foreground mutation extents and fixed-width vector, quantizer, candidate, graph, and late-interaction arrays use Arrow IPC. Materialized scan-oriented immutable tables use Parquet. Small conditional control-plane records use versioned UTF-8 JSON. Schema metadata carries format role/version and semantic invariants; S3 object metadata and existing content digests provide integrity. Experimental custom layouts are rejected without compatibility readers.

**Tech Stack:** Rust, Apache Arrow, Apache Parquet, Serde JSON, `object_store`, S3 conditional writes.

## Global constraints

- Do not hide a custom byte stream inside a standard container. PQ codes are typed fixed-size `UInt8` lists and graph adjacency is a typed Arrow list array.
- No magic prefix, custom outer frame, packed row file, hand-written binary control, or opaque custom graph payload may remain on a production persistence path.
- Standard logical schemas remain self-describing and readable without BORSUK code. BORSUK schema metadata may explain semantics but may not be required to parse physical fields.
- This is a pre-release atomic format break. Reject all old experimental formats and delete their readers/writers once replacement tests pass.
- Benchmark uncached performance after migration; do not retain a custom format merely because it was faster without proving the standard representation cannot meet the production gate.

### Task 1: Lock the format policy with executable inventory tests

**Files:**
- Create: `crates/borsuk/tests/standard_storage_formats.rs`
- Modify: `crates/borsuk/src/storage.rs`

- [ ] Write a RED structural test that creates a representative multimodal collection, enumerates every reachable object, classifies its role, and opens each payload with the official Parquet reader, Arrow IPC reader, or `serde_json`. Unknown roles and undecodable payloads fail closed.
- [ ] Add a source-policy test that rejects production persistence magic constants and direct packed-file writers. Maintain no permanent allowlist; temporary failures identify the next migration slice.
- [ ] Record a typed object-role/schema registry used by tests and observability, not a custom file wrapper.
- [ ] Commit: `test: require standard durable object formats`

### Task 2: Replace mutation, segment, and ID-state layouts

**Files:**
- Modify: `crates/borsuk/src/{format,lane_log,index,cell_wal}.rs`
- Test: `crates/borsuk/tests/{standard_storage_formats,group_commit,consistency}.rs`

- [ ] Use Arrow IPC typed columns for foreground mutation extents and Parquet typed columns for materialized records, tombstones, and ID deltas, including mutation HLC/writer/digest and operation.
- [ ] Use versioned JSON for lane/cell heads, claims, descriptors, active directories, checkpoints, and commit controls.
- [ ] Remove custom magic/version codecs and reject their objects as incompatible.
- [ ] Commit: `storage: standardize mutation and segment artifacts`

### Task 3: Replace vector, routing, graph, and lexical sidecars

**Files:**
- Modify: `crates/borsuk/src/{arrow_vector_sidecar,global_pq_sidecar,late_interaction_sidecar,bm25,lexical_build,lexical_root,metadata,format}.rs`
- Test: `crates/borsuk/tests/{standard_storage_formats,vector_encoding,named_vectors,sparse_named_vectors,text_storage,late_interaction_index}.rs`

- [ ] Store exact vectors as Arrow fixed-size lists with their declared scalar type; store PQ codes as fixed-size lists of `UInt8` and codebooks as typed floating arrays.
- [ ] Store routing and graph adjacency as typed Arrow structs/list arrays; store scan-oriented sparse/lexical tables as Parquet.
- [ ] Store small manifests/roots/schema descriptors as versioned JSON and remove packed metadata/header blobs.
- [ ] Prove all modalities reopen with exact metadata and version/digest preservation through stock readers.
- [ ] Commit: `storage: standardize vector and search sidecars`

The 2026-08-08 mutation checkpoint makes global-PQ identity, mutation, row
integrity, and exact-vector columns stock-readable and converts convergent
tombstone/ID-directory state to Parquet. The subsequent v31 cutover replaces
the lane extent outer record plus nested custom block/Parquet payload with one
stock-readable Arrow IPC mutation table while preserving the two-PUT
acknowledgement boundary. The global-PQ scan code/location field remains
packed, so Tasks 2-3 are not complete and the inventory gate must continue to
reject a production-ready claim.

### Task 4: Close the inventory and qualify performance

- [ ] Require the structural inventory to cover every reachable object and report zero unknown/custom payloads.
- [ ] Run focused format/modality/fault suites, strict all-feature Clippy, and one full repository gate.
- [ ] Run local uncached 768D structural qualification and compare request count, bytes, CPU, RSS, ingest throughput, latency, and recall against the frozen pre-cutover evidence without treating them as one frozen architecture.
- [ ] Only after the direct-ingest and bounded-delta gates are green, freeze one revision for the preregistered AWS campaign.
- [ ] Commit: `storage: enforce portable production artifacts`
