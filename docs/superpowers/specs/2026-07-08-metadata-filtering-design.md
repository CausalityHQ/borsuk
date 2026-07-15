# Metadata & Filtered Search — Phase 1 (Engine) Design

Status: approved design, pre-implementation
Date: 2026-07-08
Scope: BORSUK core engine + all bindings + docs/web + benchmark
Follow-up (separate spec): Phase 2 drop-in adapters for Pinecone / turbopuffer / S3 Vectors

## Goal

Give every vector optional, schemaless, typed **metadata**, and let searches
**filter** on it. This is the foundation for Phase-2 drop-in adapters: those
services all revolve around per-vector metadata and filter predicates, so BORSUK
must store metadata, return it, and filter kNN by it — cheaply, on object
storage.

The library is unreleased, so metadata is added as a first-class part of the
record schema and API. There is **no** backward-compatibility layer, migration
path, or "optional add-on" framing.

### Non-goals (Phase 1)

- Drop-in SDK adapters (Phase 2, separate spec).
- Full-text / keyword search over metadata.
- Metadata mutation in place (metadata changes follow the existing
  delete + re-add / rebuild model).
- Secondary metadata-only indexes beyond the per-segment pruning stats below.

## Value model

Metadata is a schemaless, recursive, typed map on each record:

```
MetaValue =
  | Null
  | Bool(bool)
  | Int(i64)            // exact integers
  | Float(f64)          // reals
  | Str(String)
  | Timestamp(i64)      // UTC epoch milliseconds
  | List(Vec<MetaValue>)
  | Map(BTreeMap<String, MetaValue>)   // nested objects

Metadata = BTreeMap<String, MetaValue>
```

`VectorRecord` becomes `{ id, vector, metadata: Metadata }` (empty map = no
metadata). Keys and value types vary per record.

Binding type mapping:

| Native | Python | TypeScript |
|---|---|---|
| Int | `int` | `bigint` |
| Float | `float` | `number` |
| Bool | `bool` | `boolean` |
| Str | `str` | `string` |
| Timestamp | `datetime` (aware→UTC) or ISO str or epoch int | `Date` or ISO string or epoch number |
| List | `list` | `Array` |
| Map | `dict` | object |

Ambiguity rules: a Python `int` is `Int`; `float` is `Float`; a bare epoch number
is only a `Timestamp` when the caller uses a timestamp type (`datetime`/`Date`) —
plain numbers stay numeric. Comparisons coerce `Int`/`Float`/`Timestamp` into a
common numeric order; cross-type compares between non-numeric kinds are `false`
(never an error). A filter operand's numeric kind need not match the stored
kind: a `Float` operand (e.g. a TypeScript `number` literal `2020`) compares
correctly against a stored `Int`, and vice versa.

## Storage

- One new **`metadata`** column on the segment payload Parquet schema, holding a
  **compact self-describing typed binary encoding** of each record's map
  (per value: 1 type-tag byte, then a typed/length-prefixed payload; maps/lists
  recurse; varint counts and lengths). This is *not* JSON — it matches BORSUK's
  "compact binary, no JSON in the index format" storage rule and encodes exact
  types so filter semantics and stats are unambiguous.
- Metadata rides through compaction unchanged (rewritten verbatim; stats
  recomputed on the new segment).
- `format.rs` owns the encode/decode + the Arrow/Parquet column; a dedicated
  `metadata.rs` module owns the `MetaValue`/`Metadata` types, the binary codec,
  flattening, and stat computation, so `format.rs` does not grow another
  concern.

## Segment stats & pruning

Each `SegmentSummary` gains a compact **`MetadataStats`** computed at write time
from the segment's rows, keyed by **flattened leaf dotted-path** (nested maps
flatten to `a.b.c`; list elements contribute their leaf path):

- numeric/timestamp leaf path → `min`, `max`
- string / tag leaf path → a small presence bloom of values + the set of keys
  present in the segment
- key-presence set across all leaf paths (for `Exists`)

Stats are resident routing data (small, bounded per segment), so a filtered query
can **skip an entire segment** whose stats prove no row can match — fewer object
reads (directly cheaper on S3). Pruning is sound (never skips a segment that
could match) but not complete (some non-matching segments are still read); the
bloom's false positives only cost extra reads, never wrong results.

`IndexStats` / resident-byte accounting includes the stats so their RAM cost is
visible and bounded by `ram_budget`.

## Filter representation & semantics

Native typed filter tree:

```
Filter =
  | And(Vec<Filter>) | Or(Vec<Filter>) | Not(Box<Filter>)
  | Cmp { path: String, op: Op, value: MetaValue }
  | Exists { path: String, present: bool }

Op = Eq | Ne | Gt | Gte | Lt | Lte | In | Nin | Contains
```

- `path` is a dotted path into nested maps.
- `In`/`Nin`: membership of a scalar in a provided list value.
- `Contains`: the record's list-valued field contains the given scalar (tag
  match).
- Numeric ops span `Int`/`Float`/`Timestamp`; string ops on `Str`; `Eq`/`Ne`
  defined for all scalar kinds.
- A top-level binding dict is implicit `And` of its keys.

### Missing fields, negation, and cross-type semantics (exact rules)

These corners are load-bearing (they drive row evaluation, which predicates may
prune, and Phase-2 SDK parity), so they are pinned exactly:

- **Evaluation is total** — every predicate yields `true`/`false`, never an
  error. Cross-kind comparison where the op's type rule does not apply (e.g.
  `Gt` between `Str` and `Int`, or `Eq` between `Int` and `Str`) yields `false`.
- **Missing path** — for the *positive* comparison ops (`Eq`, `Gt`, `Gte`, `Lt`,
  `Lte`, `In`, `Contains`) an absent path yields `false`.
- **Negation is purely logical** — `Ne(p, v) ≡ Not(Eq(p, v))` and
  `Nin(p, v) ≡ Not(In(p, v))`, and `Not(f)` is the boolean negation of `f`.
  Consequently a record **missing** the path *does* satisfy `Ne`/`Nin` (because
  `Eq`/`In` were `false` there) — the MongoDB/Pinecone-style behavior — and there
  is exactly one interpretation of `Not(Eq)` vs `Ne`: they are identical.
- **`Exists{present:false}`** is `true` exactly when the path is absent.
- **`Eq` on a list-valued field** matches only when the whole stored value equals
  the operand (a list equals a list); element membership is `Contains` (and `In`
  on a scalar field). Stats flatten list elements into the leaf path so `Contains`
  can prune, but that does not make `Eq(p, scalar)` match a list element.

### Pruning of negated predicates

Segment pruning is only ever applied to the *positive* ops via min/max and
presence blooms. `Ne`, `Nin`, `Not(...)`, and `Exists{present:false}`
conservatively report **cannot-prune** (the segment must be read), because a
presence bloom can prove "value may be present" but never "value is absent from
every row." (A sound `Ne` optimization — prune only when stats prove every row
equals the operand — is out of scope for v1.)

Bindings accept the widely-known **Pinecone-style operator dict** as ergonomic
input and translate to the native tree:

```python
filter = {"year": {"$gte": 2020}, "genre": {"$in": ["comedy", "drama"]},
          "author.rank": {"$gt": 2}, "tags": {"$contains": "award"}}
```

Rust callers can build the `Filter` tree directly; a parser also accepts the
same dict shape for parity.

## Query flow (correct filtered kNN)

1. Route to candidate segments (unchanged).
2. **Prune** candidate segments whose `MetadataStats` prove no row can satisfy
   the filter.
3. Scan surviving segments; apply the filter to each row **before it competes
   for top-k** (pre-filter). Keep pulling/expanding candidates within the query
   budget until `k` matches are found or a budget stops the query.
4. Exact-rerank the matching candidates on full vectors.
5. If `include_metadata`, attach each returned hit's decoded metadata (rows are
   already fetched for scoring, so this is a decode, not an extra object read).

Correctness: the filter is a **pre-filter**, so a filtered query returns up to
`k` matching rows, never fewer-than-k because filtering happened after top-k.
Honesty: if a budget stops the query before `k` matches, the report's
`recall_guarantee` is `degraded` and `termination_reason` explains the stop —
never a silent short result presented as complete.

## API surface (backward compat NOT required)

Rust:
```rust
VectorRecord::new(id, vector).with_metadata(map)
SearchOptions::approx(k, mode)
    .with_filter(filter)          // Filter tree or parsed dict
    .with_include_metadata(true)
index.get_record(id) -> Option<(Vec<f32>, Metadata)>
```
Reports: `SearchReport` gains `rows_evaluated`, `rows_passed_filter`,
`segments_pruned_by_filter`; hits optionally carry `metadata`.

Python:
```python
index.add(vectors, ids=[...], metadata=[{...}, ...])
index.search_ids(q, k=10, filter={...}, include_metadata=True)
index.search_with_report(q, k=10, filter={...}, include_metadata=True)
index.get_record(id) -> (vector, metadata)
```

TypeScript:
```ts
index.add(vectors, { ids, metadata })
index.searchIds(q, { k, filter, includeMetadata })
index.searchWithReport(q, { k, filter, includeMetadata })
index.getRecord(id)
```

CLI: `borsuk add --metadata <json|jsonl>`; `borsuk search --filter '<json>'
--include-metadata`.

## Docs & web (required)

- `docs/api.md`: new "Metadata & filtering" section (value model, filter DSL +
  operator table, `include_metadata`, `get_record`, new report counters);
  update Add/Read and Search sections.
- `docs/architecture.md`: metadata column + segment stats + prune-then-pre-filter
  query flow.
- `docs/storage-format.md`: the binary metadata encoding + segment `MetadataStats`.
- `README.md`: metadata in the record shape + a filtered-search quickstart snippet.
- `docs/web/docs.html`: a "Filtering" section; extend the **3D query demo** to
  show metadata pruning — segments a filter prunes are visibly skipped before the
  read step — and a note in the glossary (`metadata`, `filter`, `pruning`).
- `docs/web/index.html`: a line in features that BORSUK does metadata filtering
  natively (feeds the Phase-2 drop-in story).
- Repo policy (`check_repo_policy.py`) + its self-tests updated for any new
  required doc anchors.

## Benchmark (required)

New benchmark artifact `docs/web/assets/benchmarks/filtering.csv` from the
benchmark harness, rendered on the docs page. It sweeps **filter selectivity**
(fraction of rows matching, e.g. 100% / 25% / 5% / 1%) on a synthetic dataset
with generated metadata and reports, per selectivity: p50/p95 latency, tie-aware
recall@10 (vs an exact filtered oracle), bytes read, **segments pruned**, rows
evaluated vs passed, and object-store requests/query — with an unfiltered
baseline row. The headline it should demonstrate: on a selective filter, segment
pruning reads far fewer objects than a full scan. A criterion micro-bench covers
filtered-vs-unfiltered `search` latency, and a `performance_smoke`-style test
asserts filtered search returns exactly the matching top-k and stays bounded.

## Testing

- Codec round-trip for every value kind incl. nested map, list, timestamp,
  negative/large ints.
- Store → search → return: `include_metadata` returns exactly the stored map;
  default omits it.
- Each operator + dotted paths + `Contains`/`In`/`Exists`; type-coercion and
  cross-type-compare rules.
- **Fill-to-k**: with enough matches, k results all satisfy the filter; when a
  budget stops early, fewer results + `degraded`.
- **Pruning soundness**: a fuzz/property test that a pruned segment truly held no
  matching row; a counter test proving a selective filter reads fewer
  segments/objects than the unfiltered query.
- Compaction preserves metadata and recomputes stats.
- Cross-language parity: same dataset + filter yields the same hits and metadata
  in Rust, Python, and TypeScript.

## Implementation stages (for the plan)

1. `metadata.rs`: `MetaValue`/`Metadata`, binary codec, flattening + stats,
   filter tree + dict parser + row evaluation. Unit-tested in isolation.
2. Storage: metadata column in the segment schema (`format.rs`); write/read;
   compaction carry-through. Round-trip tests.
3. Segment stats on `SegmentSummary` + resident accounting + prune predicate.
4. Query flow: pre-filter fill-to-k + segment prune; report counters.
5. Rust public API (`with_metadata`, `with_filter`, `include_metadata`,
   `get_record`).
6. Bindings: Python, then TypeScript, then CLI (+ .pyi/.d.ts, parity tests).
7. Docs + web + glossary + 3D-demo pruning + policy anchors.
8. Benchmark harness + `filtering.csv` + web chart.

## Risks / open items

- Encoding/stat cost on write for very large or deeply nested metadata — bound
  stat depth/width and document it.
- Bloom sizing vs false-positive rate for tag/string pruning — pick a small fixed
  budget per key; measured in the benchmark.
- Timestamp input ergonomics across languages — accept ISO string + epoch, store
  epoch-ms UTC.
