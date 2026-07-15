# Drop-in Adapter Libraries — Design Spec

**Goal:** Let users of Pinecone, turbopuffer, and Amazon S3 Vectors swap in BORSUK
with near-zero code changes, by shipping thin client-shim modules whose surface
mimics each target SDK and whose backend is BORSUK's existing add/search/get/delete.

**Status:** APPROVED direction (2026-07-08). Namespaces = separate BORSUK index
root per namespace (no engine change). Smart-filtering engine (bitmaps + partition
key) is a SEPARATE later spec. See memory `borsuk-dropin-adapters-and-smart-filtering`.

## Scope

Phase 2a only: the adapter shims. Python first (`borsuk.compat.*`), then
TypeScript (`borsuk/compat/*`). Three adapters: `pinecone`, `s3vectors`,
`turbopuffer`. Build order: Pinecone (native `$`-dict, simplest) → S3 Vectors
(native dialect + `key` rename + bucket/index nouns) → turbopuffer (tuple-filter
translator).

Out of scope: emulating server-side control-plane semantics beyond what a local
BORSUK index needs (billing, replication, pod types), streaming/import APIs,
sparse vectors, BM25/hybrid ranking. Adapters target the data-plane core:
upsert, query, fetch/get, delete, and minimal create/describe.

## Namespace model

A namespace (Pinecone/turbopuffer) or index-within-bucket (S3 Vectors) maps to a
distinct BORSUK index rooted at `<base_uri>/<sanitized-namespace>`. The adapter
client holds a `base_uri` plus create-time defaults (metric, dimensions) and
lazily `open`s or `create`s the per-namespace index on first use, caching handles.
Sanitization: percent-encode path-unsafe characters so arbitrary namespace
strings are safe URI path segments. Empty/default namespace → a reserved segment
(`__default__` for Pinecone).

## Shared translation layer (`borsuk.compat._common`)

- **Filter translation.** Pinecone and S3 Vectors already use the `$`-operator
  dict BORSUK accepts natively → pass through, with a validation pass that coerces
  bare `{"k": v}` → `{"k": {"$eq": v}}` is unnecessary (BORSUK handles bare values)
  but S3/Pinecone forbid `$contains`/`$not`, so the adapters do not advertise them.
  turbopuffer uses tuple syntax `(attr, "Op", value)` and logical `("And", (...))`;
  a `turbopuffer_filter_to_borsuk()` converts tuple → `$`-dict (Eq→$eq, NotEq→$ne,
  Gt→$gt, Gte→$gte, Lt→$lt, Lte→$lte, In→$in, NotIn→$nin, Contains→$contains,
  And→$and, Or→$or, Not→$not). Unsupported ops raise a clear error.
- **Metric mapping.** Target metric name → BORSUK `VectorMetricName`:
  Pinecone `cosine`/`euclidean`/`dotproduct` → `cosine`/`euclidean`/`inner-product`;
  turbopuffer `cosine_distance`/`euclidean_squared` → `cosine`/`squared-euclidean`;
  S3 Vectors `cosine`/`euclidean` → `cosine`/`euclidean`.
- **Distance/score.** Return BORSUK's `distance` verbatim as the service's
  distance/score field. (No attempt to reconstruct Pinecone's similarity score;
  documented as distance semantics.)

## Adapter surfaces

### Pinecone (`borsuk.compat.pinecone`)
- `Pinecone(api_key=None, *, base_uri, dimension, metric="cosine")` — api_key is
  accepted and ignored (local backend); `base_uri` is the BORSUK storage root.
- `pc.create_index(name, dimension=None, metric=None, spec=None, **_)` — creates
  the namespace root eagerly is unnecessary; records index-level defaults.
- `pc.Index(name=None, host=None)` → `_Index` handle bound to a namespace root.
- `_Index.upsert(vectors, namespace="__default__")` — accepts `(id, values,
  metadata)` tuples or `{"id","values","metadata"}` dicts → `index.add`.
  Returns `{"upserted_count": n}`.
- `_Index.query(vector=None, id=None, top_k=10, filter=None,
  include_values=False, include_metadata=False, namespace="__default__")` →
  `search_with_report`. Returns `{"matches": [{"id","score","values"?,
  "metadata"?}], "namespace": ns}`. `id=` queries fetch that vector then search.
- `_Index.fetch(ids, namespace)` → `{"vectors": {id: {"id","values","metadata"}},
  "namespace": ns}` via `get_record`.
- `_Index.delete(ids=None, delete_all=False, filter=None, namespace)` →
  `index.delete`. `filter`/`delete_all` iterate matching ids (search/scan) — MVP
  supports `ids` and `delete_all`; `filter`-delete raises NotImplemented for now.
- `_Index.describe_index_stats()` → dimension + per-namespace vector counts.

### S3 Vectors (`borsuk.compat.s3vectors`)
- `client("s3vectors", **_)` factory returns an `S3VectorsClient` (mirrors
  `boto3.client`). Config carries `base_uri`.
- `create_vector_bucket(vectorBucketName)`, `create_index(vectorBucketName,
  indexName, dimension, distanceMetric="cosine", dataType="float32",
  metadataConfiguration=None)`.
- `put_vectors(vectorBucketName, indexName, vectors=[{"key","data":{"float32":[]},
  "metadata"}])` → `add`. `key`↔BORSUK id.
- `query_vectors(vectorBucketName, indexName, queryVector={"float32":[]}, topK,
  filter=None, returnMetadata=False, returnDistance=False)` → search. Returns
  `{"vectors":[{"key","distance"?,"metadata"?}]}`.
- `get_vectors(vectorBucketName, indexName, keys, returnData=False,
  returnMetadata=False)`, `delete_vectors(..., keys)`, `list_indexes`,
  `list_vector_buckets`.
- Namespace root = `<base>/<bucket>/<index>`.

### turbopuffer (`borsuk.compat.turbopuffer`)
- `Turbopuffer(api_key=None, *, base_uri, dimension, region=None)`.
- `tpuf.namespace(name)` → `Namespace` handle.
- `ns.write(upsert_rows=None, upsert_columns=None, deletes=None,
  delete_by_filter=None, distance_metric="cosine_distance", **_)` → `add`/`delete`.
  Rows are `{"id","vector", **attrs}`; attrs become metadata (minus id/vector).
- `ns.query(rank_by=("vector","ANN",vec), top_k=10, filters=None,
  include_attributes=None)` → search; `filters` is tuple syntax → translated.
  Returns list of `{"id","dist", **included_attrs}`.

## Testing

Each adapter gets a test module that drives the emulated surface end to end
against a temp `base_uri`: create → upsert with metadata → filtered query →
fetch/get → delete, plus a filter-translation unit test (turbopuffer) and a
metric-mapping unit test. Cross-check that the same logical data returns the same
BORSUK ids through each adapter (reuse the metadata parity fixture where natural).

## Docs + web

- `docs/api.md` (or a new `docs/drop-in.md`) section per adapter with a
  before/after snippet (their SDK vs the one-line import swap).
- README feature bullet + a "Drop-in replacement" quick start.
- Web docs.html: a "Drop-in replacements" section with the three before/after
  swaps and the filter-dialect note; TOC link; glossary entry for "Namespace".
- policy anchors + test_docs_web if needed.

## Non-goals / honest limits (document these)

- Local, embedded backend — not a network service; no auth, replication, or
  server-side consistency guarantees beyond BORSUK's own publish semantics.
- Score semantics = BORSUK distance, not the service's exact similarity score.
- Filter-delete and metadata-fetch-by-filter are MVP-limited.
- turbopuffer BM25/hybrid, Pinecone sparse vectors, S3 non-filterable-key
  size classes are not emulated.
