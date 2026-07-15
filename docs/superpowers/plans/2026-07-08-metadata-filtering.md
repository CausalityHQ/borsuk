# Metadata & Filtered Search Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give every vector optional typed metadata and let searches filter kNN on it, with segment-level pruning, across the Rust engine and the Python/TypeScript/CLI bindings, plus docs, web demo, and a benchmark.

**Architecture:** A new `metadata` module owns the value type, a compact binary codec, filter evaluation, and per-segment stats. Segment payloads gain one binary metadata column; `SegmentSummary` gains metadata stats used to prune segments before fetch. Search prunes candidate segments by stats, then pre-filters rows before top-k (fill-to-k within budget), then exact-reranks. Bindings accept a Pinecone-style filter dict. Unreleased library → no backward-compatibility.

**Tech Stack:** Rust (arrow-array/arrow-schema/parquet 59, blake3, object_store), PyO3 (Python), N-API/napi-rs (TypeScript), criterion.

**Spec:** `docs/superpowers/specs/2026-07-08-metadata-filtering-design.md`

**Conventions (verify against the repo before each stage):**
- Gate every commit: `cargo fmt --all`, `cargo clippy --locked --workspace --all-targets -- -D warnings`, `cargo test --locked -p borsuk` (relevant tests), and for web/docs `python scripts/check_repo_policy.py` + `node scripts/test_docs_web.mjs`.
- Commit + push after each task once the gate passes (repo pattern). Use `rtk proxy cargo …` when the rtk hook filters output.
- Follow existing patterns: `VectorRecord`/`SearchOptions`/`SearchReport` in `record.rs`; segment schema in `format.rs`; `SegmentSummary` in `manifest.rs`; add/compact/search in `index.rs`; segment build/scan in `segment.rs`.

---

## File Structure

**Create:**
- `crates/borsuk/src/metadata.rs` — `MetaValue`, `Metadata`, binary codec (encode/decode), leaf-path flattening, `MetadataStats` (compute + `can_match` prune predicate), `Filter` tree + Pinecone-dict parser + row `matches`. Unit tests inline (`#[cfg(test)]`).
- `docs/web/assets/benchmarks/filtering.csv` — benchmark artifact.

**Modify (engine):**
- `crates/borsuk/src/lib.rs` — export metadata types.
- `crates/borsuk/src/record.rs` — `VectorRecord.metadata`; `VectorRecord::with_metadata`; `SearchOptions` gains `filter`, `include_metadata`; `SearchHit` gains optional `metadata`; `SearchReport` gains `rows_evaluated`, `rows_passed_filter`, `segments_pruned_by_filter`; `GetRecord` shape.
- `crates/borsuk/src/format.rs` — segment payload schema `metadata` Binary column (read/write); manifest schema columns for segment metadata stats.
- `crates/borsuk/src/manifest.rs` — `SegmentSummary.metadata_stats`; resident-bytes accounting.
- `crates/borsuk/src/segment.rs` — compute `MetadataStats` at build; expose row metadata for scan/filter.
- `crates/borsuk/src/index.rs` — `add` stores metadata; compaction carries metadata + recomputes stats; `search*` prune + pre-filter fill-to-k; `get_record`; report counters.

**Modify (bindings):**
- `crates/borsuk-python/src/lib.rs`, `python/src/borsuk/__init__.py`, `python/src/borsuk/__init__.pyi`.
- `crates/borsuk-node/src/lib.rs`, `packages/borsuk/src/index.ts`, `packages/borsuk/native.d.ts`.
- `crates/borsuk-cli/src/main.rs`.

**Modify (docs/web/bench/policy):**
- `docs/api.md`, `docs/architecture.md`, `docs/storage-format.md`, `README.md`.
- `docs/web/docs.html`, `docs/web/index.html`, `docs/web/app.js`, `docs/web/viz3d.js`, `docs/web/styles.css`.
- `crates/borsuk/examples/benchmark_report.rs`, `scripts/check_repo_policy.py`, `scripts/test_check_repo_policy.py`, `scripts/test_docs_web.mjs`.

**Tests:**
- Inline in `metadata.rs`; `crates/borsuk/tests/local_index.rs`; `crates/borsuk/tests/performance_smoke.rs`; `python/tests/test_api.py`; `packages/borsuk/test/api.test.ts`.

---

## Stage 1 — `metadata.rs`: types, codec, filter, stats (isolated)

Pure module, no engine wiring yet. TDD each piece with inline tests.

### Task 1.1: MetaValue type + binary codec

**Files:** Create `crates/borsuk/src/metadata.rs`; Modify `crates/borsuk/src/lib.rs`.

- [ ] **Step 1 — failing test.** In `metadata.rs` add `#[cfg(test)]` module with `roundtrip_all_kinds`: build a `Metadata` map containing every kind (Null, Bool, Int(-5, i64::MAX), Float, Str, Timestamp, List of mixed, nested Map), `encode` → `decode`, assert equal.
```rust
#[test]
fn roundtrip_all_kinds() {
    let m = Metadata::from([
        ("b".into(), MetaValue::Bool(true)),
        ("i".into(), MetaValue::Int(i64::MAX)),
        ("f".into(), MetaValue::Float(-1.5)),
        ("s".into(), MetaValue::Str("x".into())),
        ("t".into(), MetaValue::Timestamp(1_700_000_000_000)),
        ("l".into(), MetaValue::List(vec![MetaValue::Str("a".into()), MetaValue::Int(2)])),
        ("nested".into(), MetaValue::Map(Metadata::from([("k".into(), MetaValue::Bool(false))]))),
    ]);
    assert_eq!(decode(&encode(&m)).unwrap(), m);
}
```
- [ ] **Step 2 — run, expect fail** (`rtk proxy cargo test -p borsuk --lib metadata::`): compile error / not defined.
- [ ] **Step 3 — implement** `MetaValue` enum (Null/Bool/Int(i64)/Float(f64)/Str(String)/Timestamp(i64)/List(Vec<MetaValue>)/Map(Metadata)), `type Metadata = BTreeMap<String, MetaValue>`, `encode(&Metadata)->Vec<u8>` and `decode(&[u8])->Result<Metadata>`: 1 tag byte per value + varint-length-prefixed payload; maps/lists recurse; varint counts. Derive PartialEq (Float via bit compare or store as f64 with total order for eq — use `f64::to_bits` for Eq/Hash). Add module to `lib.rs`: `pub mod metadata; pub use metadata::{MetaValue, Metadata};`
- [ ] **Step 4 — run, expect pass.**
- [ ] **Step 5 — commit** `feat(metadata): MetaValue type + binary codec`.

### Task 1.2: leaf-path flattening

**Files:** Modify `metadata.rs`.
- [ ] **Step 1 — failing test** `flatten_leaf_paths`: nested map `{a:{b:1}, tags:[x,y]}` flattens to `[("a.b", Int 1), ("tags", Str x), ("tags", Str y)]` (list elements share the parent path; nested maps dot-join).
- [ ] **Step 2 — run, fail.**
- [ ] **Step 3 — implement** `fn for_each_leaf(meta, |path:&str, &MetaValue|)` walking maps (dot-join) and lists (elements at parent path).
- [ ] **Step 4 — pass.** **Step 5 — commit.**

### Task 1.3: Filter tree + Pinecone-dict parser + row matching

**Files:** Modify `metadata.rs`.
- [ ] **Step 1 — failing tests** covering the exact spec semantics: each op (Eq/Ne/Gt/Gte/Lt/Lte/In/Nin/Contains), And/Or/Not, dotted path, **missing path** (positive ops false; `Exists{present:false}` true), **negation** (`Ne` matches missing; `Ne≡Not(Eq)`), **cross-type** compare false, numeric coercion (Float operand vs stored Int), `Eq` on list ≠ element match, `Contains` matches element. Include a `parse_dict` test for `{"year":{"$gte":2020},"genre":{"$in":["c"]},"a.b":{"$gt":2},"tags":{"$contains":"x"}}`.
- [ ] **Step 2 — run, fail.**
- [ ] **Step 3 — implement** `Filter` enum (And/Or/Not/Cmp{path,op,value}/Exists{path,present}), `Op`, `fn matches(&self, &Metadata) -> bool` (total, per spec rules), and `fn parse_dict(&serde_json::Value|map) -> Result<Filter>` mapping `$eq/$ne/$gt/$gte/$lt/$lte/$in/$nin/$contains/$exists/$and/$or/$not`. Get value at dotted path via `for_each_leaf`/direct walk. Numeric coercion helper over Int/Float/Timestamp.
- [ ] **Step 4 — pass.** **Step 5 — commit.**

### Task 1.4: MetadataStats + prune predicate

**Files:** Modify `metadata.rs`.
- [ ] **Step 1 — failing tests** `stats_prune_soundness`: build stats from a set of metadata rows; assert (a) for a filter that no row matches and only positive ops, `stats.can_match(&filter) == false`; (b) for any filter some row matches, `can_match == true`; (c) negated predicates (`Ne`/`Nin`/`Not`/`Exists{false}`) always `can_match == true` (cannot prune). Add a property-style loop over random rows/filters asserting: if `can_match==false` then no row matches.
- [ ] **Step 2 — run, fail.**
- [ ] **Step 3 — implement** `MetadataStats` { per leaf-path numeric min/max; per string/tag leaf-path a small fixed-size presence bloom (reuse the existing bloom in `format.rs`/`manifest.rs` if suitable, else a compact `u64`-word bitset hashed with blake3) + key-present set }, `fn from_rows(&[&Metadata]) -> MetadataStats`, `fn can_match(&self, &Filter) -> bool` (sound: only prunes positive ops; And = all children can_match; Or = any; Not/negated = true). Encode/decode for persistence. **Bound stat size** (spec risk): cap the number of tracked leaf paths and traversal depth (const `MAX_STAT_PATHS`, `MAX_STAT_DEPTH`); paths beyond the cap simply aren't pruned (still sound — `can_match` returns true for unknown paths). Add a test that an over-wide metadata map still round-trips and searches correctly with capped stats, and document the cap in Task 7.1 (storage-format.md).
- [ ] **Step 4 — pass.** **Step 5 — commit.** Run full `cargo clippy -D warnings` here.

---

## Stage 2 — storage: metadata column + compaction carry-through

### Task 2.1: VectorRecord.metadata + with_metadata

**Files:** Modify `record.rs`.
- [ ] **Steps:** failing test `record_with_metadata` (build record, assert `.metadata`); add `pub metadata: Metadata` to `VectorRecord` (default empty via `Default`/`new`), `fn with_metadata(mut self, Metadata) -> Self`; keep `new(id, vector)` → empty metadata. Update all in-repo `VectorRecord { … }` literals/constructions to compile (grep `VectorRecord`), and `VectorRecord::new` call sites are unaffected. Commit `feat(record): metadata field on VectorRecord`.

### Task 2.2: segment payload metadata column (write + read)

**Files:** Modify `format.rs`, `segment.rs`.
- [ ] **Steps:** failing test in `crates/borsuk/tests/local_index.rs` `metadata_round_trips_through_segment` (create index, `add` a record with metadata, reopen, `get_record` returns the metadata — get_record lands in Stage 5, so for Stage 2 assert at the `format.rs` level with a unit test that writes a batch incl. metadata column and reads it back). Add a `metadata` Binary column to the segment payload Arrow schema (encode each row via `metadata::encode`; empty map → empty bytes/null). Update `segment_to_parquet`/`records_from_segment` (or equivalents) to write/read + decode. Update the `external_manifest_parquet`-style schema tests that hard-code column counts. Commit `feat(storage): metadata column on segment payloads`.

### Task 2.3: compaction carries metadata

**Files:** Modify `index.rs`, `segment.rs`.
- [ ] **Steps:** failing test `compaction_preserves_metadata` (add records with metadata across ≥2 L0 segments, compact L0→L1, reopen, metadata intact). Ensure the compaction read→rewrite path threads `record.metadata` through unchanged. Commit.

---

## Stage 3 — segment stats on SegmentSummary + pruning

### Task 3.1: SegmentSummary.metadata_stats persisted

**Files:** Modify `manifest.rs`, `format.rs`, `segment.rs`.
- [ ] **Steps:** failing test `segment_summary_carries_metadata_stats` (build a segment, publish, reopen manifest, assert stats present + `can_match` behaves). Add `metadata_stats: MetadataStats` to `SegmentSummary`; compute it in the segment build (`segment.rs`, from the batch's metadata via `MetadataStats::from_rows`); persist via new manifest-table columns (encode stats bytes) in `format.rs` (`manifest_to_parquet`/`manifest_from_parquet` + the 17→N field schema — update the format test that builds a manifest batch). Add resident-bytes accounting in `manifest.rs` (`SegmentSummary::resident_bytes_estimate`). Commit `feat(routing): per-segment metadata stats`.

---

## Stage 4 — query flow: prune + pre-filter fill-to-k + report

### Task 4.1: SearchOptions.filter + include_metadata; report counters

**Files:** Modify `record.rs`.
- [ ] **Steps:** failing test (compile-level) constructing `SearchOptions::approx(k, mode).with_filter(filter).with_include_metadata(true)` and reading `report.rows_evaluated` etc. Add `filter: Option<Filter>`, `include_metadata: bool` to `SearchOptions` (+ builders); add `rows_evaluated`, `rows_passed_filter`, `segments_pruned_by_filter: usize` to `SearchReport` (default 0); optional `metadata: Option<Metadata>` on `SearchHit`. Update the `synthetic_report(...)` test helper + any `SearchReport { … }` literal. Commit.

### Task 4.2: segment pruning by stats

**Files:** Modify `index.rs`.
- [ ] **Steps:** failing test `filter_prunes_segments` (build index where a metadata value only exists in some segments; a selective filter → `report.segments_pruned_by_filter > 0` and correct hits). In the routing/candidate-selection path, when `filter` is set, drop candidate segments whose `summary.metadata_stats.can_match(filter) == false`; count them. Commit.

### Task 4.3: pre-filter fill-to-k in the scan

**Files:** Modify `index.rs`, `segment.rs`.
- [ ] **Steps:** failing tests: `filtered_search_returns_k_matches` (enough matches → exactly k, all satisfy filter, correct nearest order vs exact oracle); `filtered_search_degrades_on_budget` (tight `max_segments`/candidate budget stopping before k → fewer results + `recall_guarantee == Degraded` + a `max-*` termination reason). Apply `filter.matches(row_meta)` to each candidate row **before** it enters the top-k heap; keep scanning/expanding within budget until k matches or budget stop; increment `rows_evaluated`/`rows_passed_filter`. Ensure exact rerank runs on the matched candidates only. Commit.

### Task 4.4: include_metadata on hits

**Files:** Modify `index.rs`.
- [ ] **Steps:** failing test `search_returns_metadata_when_requested` (include_metadata true → hits carry stored metadata; default false → none). Decode metadata for the returned top-k rows (already fetched) when requested. Commit. Run full workspace clippy/tests here.

---

## Stage 5 — Rust public API surface

### Task 5.1: get_record

**Files:** Modify `index.rs`, `record.rs`, `lib.rs`.
- [ ] **Steps:** failing test `get_record_returns_vector_and_metadata`. Add `pub fn get_record(&self, id) -> Result<Option<(Vec<f32>, Metadata)>>` (extend the `get_vector` path to also decode metadata). Export needed types from `lib.rs`. Commit.

### Task 5.2: local_index integration tests + docs-example ladder rung

**Files:** Modify `crates/borsuk/tests/local_index.rs`, `crates/borsuk/examples/local_index.rs`.
- [ ] **Steps:** end-to-end test: add records with rich metadata (nested/timestamp/list), filtered searches for each operator, `include_metadata`, `get_record`; assert vs an in-test exact oracle. Add a short metadata+filter snippet to the local example. Commit. **Full gate** (fmt, clippy -D warnings, `cargo test -p borsuk`).

---

## Stage 6 — bindings (Python → TypeScript → CLI)

### Task 6.1: Python

**Files:** Modify `crates/borsuk-python/src/lib.rs`, `python/src/borsuk/__init__.py`, `python/src/borsuk/__init__.pyi`, `python/tests/test_api.py`.
- [ ] **Steps:** failing pytest `test_metadata_filtered_search` (build venv per repo GOTCHA: `uv venv --python 3.14`, `maturin develop`, then `rm python/src/borsuk/_borsuk*.so` before policy). `add(vectors, ids=, metadata=[{...}])`; `search_ids(q, k=, filter={...}, include_metadata=True)`; **`search_with_report(q, filter=..., include_metadata=True)`** exposing `report.rows_evaluated/rows_passed_filter/segments_pruned_by_filter`; `get_record(id)`. Convert Python `int/float/bool/str/datetime/list/dict` ↔ `MetaValue`; **timestamps accept `datetime` OR ISO-8601 string OR epoch int** (test all three); parse the operator dict → `Filter`. Update `.pyi`. Commit.

### Task 6.2: TypeScript

**Files:** Modify `crates/borsuk-node/src/lib.rs`, `packages/borsuk/src/index.ts`, `packages/borsuk/native.d.ts`, `packages/borsuk/test/api.test.ts`.
- [ ] **Steps:** failing node test `metadata filtered search` (`npm run build:native` then `npm test`). `add(vectors, { ids, metadata })`; `searchIds(q, { k, filter, includeMetadata })`; **`searchWithReport(q, { filter, includeMetadata })`** exposing `rowsEvaluated/rowsPassedFilter/segmentsPrunedByFilter`; `getRecord(id)`. Map `bigint/number/boolean/string/Date/Array/object` ↔ `MetaValue`; **timestamps accept `Date` OR ISO string OR epoch number** (test all three). Update `native.d.ts` + hand TS types. Commit.

### Task 6.3: cross-language parity fixture

**Files:** Modify `python/tests/test_api.py`, `packages/borsuk/test/api.test.ts` (shared fixture values inline in each).
- [ ] **Steps:** define one shared fixture (same vectors, ids, metadata, query, filter) in both the Python and TS suites; assert each returns the **identical hit ids in order and the identical returned metadata** (with `include_metadata`). This is the spec's cross-language parity requirement — per-language tests alone don't compare across languages. Commit.

### Task 6.4: CLI

**Files:** Modify `crates/borsuk-cli/src/main.rs`.
- [ ] **Steps:** failing CLI test/roundtrip: `borsuk add --metadata <jsonl>`; `borsuk search --filter '<json>' --include-metadata`. Parse JSON → Metadata/Filter (reuse `parse_dict`). Commit. **Full cross-language gate** (Rust + wheel unittest + node test + policy).

---

## Stage 7 — docs, web, 3D-demo pruning, policy

### Task 7.1: markdown docs

**Files:** Modify `docs/api.md`, `docs/architecture.md`, `docs/storage-format.md`, `README.md`.
- [ ] **Steps:** api.md "Metadata & filtering" section (value model, operator table, dict examples, `include_metadata`, `get_record`, report counters) + update Add/Read/Search; architecture.md query flow (prune → pre-filter); storage-format.md binary encoding + stats; README record shape + filtered quickstart. Commit.

### Task 7.2: web docs + glossary + policy

**Files:** Modify `docs/web/docs.html`, `docs/web/index.html`, `scripts/check_repo_policy.py`, `scripts/test_check_repo_policy.py`, `scripts/test_docs_web.mjs`.
- [ ] **Steps:** docs.html Filtering section + glossary entries (`metadata`, `filter`, segment pruning); index.html feature line (native metadata filtering). Add any required policy anchors + update the policy self-tests and `test_docs_web.mjs` assertions. Run `node scripts/test_docs_web.mjs` + `python scripts/check_repo_policy.py`. Commit.

### Task 7.3: 3D demo shows metadata pruning

**Files:** Modify `docs/web/viz3d.js`, `docs/web/docs.html`, `docs/web/styles.css`.
- [ ] **Steps:** add an optional "filter" toggle to the query demo: give each demo point a metadata tag (color already ~ segment); when a filter is active, segments whose stats can't match are visibly greyed/skipped **before** the read step, and rows failing the filter are dimmed during read. Bump `?v=` on the module. Verify in Chrome. Keep out of the app.js test path. Commit.

---

## Stage 8 — benchmark

### Task 8.1: filtering benchmark in the report harness

**Files:** Modify `crates/borsuk/examples/benchmark_report.rs`; Create `docs/web/assets/benchmarks/filtering.csv`.
- [ ] **Steps:** add a filtering sweep to `benchmark_report`: synthetic dataset with generated metadata (a numeric field + a categorical/tag field), run queries at selectivities 100/25/5/1% and an unfiltered baseline; emit `filtering.csv` columns: selectivity, mode, p50_ms, p95_ms, tie_aware_recall_at_10 (vs exact filtered oracle), avg_bytes_read, segments_pruned, rows_evaluated, rows_passed, requests_total. Regenerate the artifact. Commit.

### Task 8.2: web chart + criterion micro-bench + smoke

**Files:** Modify `docs/web/app.js`, `docs/web/docs.html`, `scripts/test_docs_web.mjs`, `crates/borsuk/benches/local_search.rs`, `crates/borsuk/tests/performance_smoke.rs`.
- [ ] **Steps:** render `filtering.csv` on docs.html (new `data-filtering-root` chart, following the existing chart wiring in app.js) + update `test_docs_web.mjs` fixtures/assertions + expected CSV list. Add a criterion bench `local_filtered_search_10k` (filtered vs unfiltered). Add `performance_smoke` test `filtered_search_stays_bounded_and_exact` (filtered top-k equals the exact filtered oracle; latency bound). Commit. **Final full gate** across all suites.

---

## Definition of Done

- Every stage's tests pass; final full gate green (fmt, clippy -D warnings, workspace tests, Python wheel unittest, node test, policy + self-tests, web docs test, example drift).
- Metadata round-trips through all languages; filtered kNN returns exactly the matching top-k with correct nearest order; selective filters demonstrably prune segments (benchmark shows fewer reads); docs/web/glossary/3D-demo/benchmark all updated.
- Phase-2 adapters remain a separate, later spec.
