# Drop-in Replacements

BORSUK ships thin client adapters that emulate the data-plane surface of
**Pinecone**, **turbopuffer**, and **Amazon S3 Vectors** (Python + TypeScript),
plus **Chroma** and **Qdrant** (Python). Existing code switches backend by
changing the import and pointing at a BORSUK storage root — no rewrite of your
upsert/query/delete calls.

Each adapter maps a **namespace** (Pinecone/turbopuffer) — or an **index inside a
bucket** (S3 Vectors) — to its own BORSUK index under a shared base URI. Isolation
is physical: `<base_uri>/<namespace>`. Metadata and filtered search work exactly
as they do natively; the filter dialects are handled per service.

## Honest limits

These are **local, embedded** backends over object storage, not network
services. They do not emulate the vendors' auth, replication, billing, pod types,
or exact server-side consistency — they give BORSUK's own publish semantics. A
hit's `score`/`distance` is BORSUK's distance under the index metric, not the
vendor's similarity score. Metric names are mapped to BORSUK's
(`cosine`, `euclidean`, `squared-euclidean`, `inner-product`).

## Compatibility matrix

The data-plane operations each adapter implements. Everything here is exercised
by `python/tests/test_compat.py`. Unsupported calls raise a clear error rather
than silently misbehaving.

| Capability | Pinecone | Qdrant | S3 Vectors | turbopuffer | Chroma |
|---|:--:|:--:|:--:|:--:|:--:|
| create index/collection | ✅ | ✅ | ✅ | ✅ | ✅ |
| upsert (overwrite by id) | ✅ | ✅ | ✅ | ✅ | ✅ |
| vector query | ✅ | ✅ | ✅ | ✅ | ✅ |
| metadata / payload filtering | ✅ | ✅ | ✅ | ✅ | ✅ |
| fetch / get by id | ✅ | ✅ (`retrieve`) | ✅ | — | ✅ |
| delete by ids | ✅ | ✅ | ✅ | ✅ | ✅ |
| list / scroll records | ✅ (`list`) | ✅ (`scroll`) | — | — | ✅ (`get`) |
| named (dense) vectors | — | ✅ | — | — | — |
| sparse vectors | ✅ (`sparse_values`) | ✅ (`sparse_vectors_config`) | — | — | — |
| count / stats | ✅ (`describe_index_stats`) | ✅ (`count`) | — | — | ✅ (`count`) |

Record enumeration is emulated where the row shows a method: Pinecone
`list`/`list_paginated`, Qdrant `scroll`, and Chroma `get` walk stored records
(S3 Vectors lists *indexes/buckets*, not vectors, so it has none). Sparse-vector
query scores are RRF-fused ranks, not raw dot products. `count` /
`describe_index_stats` report **live** records, but an in-place upsert leaves the
superseded copy on disk until the next compaction reclaims it, so the count can
transiently over-report after overwrites (queries, `retrieve`/`fetch`, and
`scroll` always see exactly one live copy — only the raw count lags). **Not
emulated by any adapter:** control-plane operations (auth, billing, replication,
pod/index provisioning), async clients, server-side consistency/visibility flags,
and integrated embedding. Filter-based delete is ids-only.

## Pinecone

The `$`-operator filter dialect is used as-is.

```python
# before
# from pinecone import Pinecone
# pc = Pinecone(api_key="…")
from borsuk.compat.pinecone import Pinecone
pc = Pinecone(base_uri="file:///data/vectors", dimension=768, metric="cosine")

index = pc.Index("products")
index.upsert(
    [("a", [0.1, 0.2, ...], {"genre": "rock"})],
    namespace="store-1",
)
res = index.query(
    vector=[0.1, 0.2, ...], top_k=10,
    filter={"genre": {"$eq": "rock"}, "year": {"$gte": 1990}},
    include_metadata=True, namespace="store-1",
)
```

```typescript
// before: import { Pinecone } from "@pinecone-database/pinecone";
import { Pinecone } from "borsuk/compat/pinecone";
const pc = new Pinecone({ baseUri: "file:///data/vectors", dimension: 768, metric: "cosine" });

const index = pc.Index("products");
await index.upsert([{ id: "a", values: [/*…*/], metadata: { genre: "rock" } }], "store-1");
const res = await index.query({
  vector: [/*…*/], topK: 10,
  filter: { genre: { $eq: "rock" } }, includeMetadata: true, namespace: "store-1"
});
```

Supported: `create_index`, `Index`, `upsert`, `query` (by `vector` or `id`),
`fetch`, `delete` (by ids), `list` / `list_paginated` (id enumeration with a
`prefix` and an opaque cursor), `describe_index_stats`. `delete` by filter /
`delete_all` raise `NotImplementedError` for now.

## Amazon S3 Vectors

A vector's id is `key`; payloads nest as `{"float32": [...]}`. The `$`-operator
dialect is used as-is (bare `{"k": "v"}` means `$eq`).

```python
# before: import boto3; s3v = boto3.client("s3vectors")
from borsuk.compat.s3vectors import client
s3v = client("s3vectors", base_uri="file:///data/vectors")

s3v.create_vector_bucket(vectorBucketName="media")
s3v.create_index(vectorBucketName="media", indexName="movies",
                 dimension=768, distanceMetric="cosine")
s3v.put_vectors(vectorBucketName="media", indexName="movies",
                vectors=[{"key": "star-wars", "data": {"float32": emb},
                          "metadata": {"genre": "scifi"}}])
res = s3v.query_vectors(vectorBucketName="media", indexName="movies",
                        queryVector={"float32": emb}, topK=3,
                        filter={"genre": "scifi"}, returnMetadata=True)
```

Supported: `create_vector_bucket`, `create_index`, `list_indexes`, `get_index`,
`put_vectors`, `query_vectors`, `get_vectors`, `delete_vectors`,
`list_vector_buckets`. The TypeScript adapter mirrors these as camelCase methods
on `client("s3vectors", { baseUri })`.

## turbopuffer

turbopuffer stores the vector inline as `vector` and every other row key as a
filterable attribute. Its tuple filter syntax is translated to BORSUK's operator
dict (`Eq`→`$eq`, `NotEq`→`$ne`, `In`→`$in`, `And`/`Or`/`Not`, …).

```python
# before: import turbopuffer; tpuf = turbopuffer.Turbopuffer(region="…")
from borsuk.compat.turbopuffer import Turbopuffer
tpuf = Turbopuffer(base_uri="file:///data/vectors", dimension=768)

ns = tpuf.namespace("products")
ns.write(upsert_rows=[{"id": "1", "vector": [0.1, 0.2, ...], "category": "animal"}],
         distance_metric="cosine_distance")
rows = ns.query(rank_by=("vector", "ANN", [0.1, 0.2, ...]), top_k=10,
                filters=("And", (("category", "Eq", "animal"),)),
                include_attributes=["category"])
```

Supported: `namespace`, `write` (upsert rows / deletes), `query` (with tuple
`filters` and `include_attributes`). `delete_by_filter` raises
`NotImplementedError` for now. turbopuffer's own BM25/full-text ranking is not
yet mapped through *this adapter* — BORSUK itself does full-text (BM25), sparse,
and hybrid search natively (see [`docs/api.md`](api.md#named-vectors)); use the
native API for those until the turbopuffer adapter wires them through.

## Chroma

Chroma's `where` filter already uses a Mongo-style operator dict, so it maps to
BORSUK directly. Documents ride in a reserved metadata key.

```python
# before: import chromadb; client = chromadb.PersistentClient(path="…")
from borsuk.compat.chroma import Client
client = Client(base_uri="file:///data/vectors", dimensions=768)

col = client.get_or_create_collection("docs")
col.add(ids=["a"], embeddings=[embedding], metadatas=[{"genre": "rock"}],
        documents=["hello"])
col.query(query_embeddings=[embedding], n_results=10,
          where={"genre": "rock"}, include=["metadatas", "documents", "distances"])
```

Supported: `create_collection`, `get_or_create_collection`, `get_collection`,
`delete_collection`; `add`/`upsert`, `query`, `get` (by ids or `where`), `peek`,
`delete` (by ids), `count`. `get`/`peek`/`scroll`-style listing is backed by the
`list_records` scan. `delete` by `where` is not supported yet.

## Qdrant

Qdrant's structured `Filter` (`must` / `should` / `must_not` with `FieldCondition`
+ `match` / `range`) is accepted in its plain-dict form and translated to
BORSUK's operator dict. Payloads map to metadata.

```python
# before: from qdrant_client import QdrantClient; c = QdrantClient(path="…")
from borsuk.compat.qdrant import QdrantClient
c = QdrantClient(base_uri="file:///data/vectors")

c.create_collection("docs", vectors_config={"size": 768, "distance": "Cosine"})
c.upsert("docs", points=[{"id": "a", "vector": embedding,
                          "payload": {"genre": "rock"}}])
c.search("docs", query_vector=embedding, limit=10, with_payload=True,
         query_filter={"must": [{"key": "genre", "match": {"value": "rock"}}]})
```

Supported: `create_collection`/`recreate_collection`, `collection_exists`;
`upsert`, `search`, `retrieve` (by ids), `scroll`, `delete` (by ids), `count`.
Filter `match.value`/`match.any`/`match.except` and `range` (`gt`/`gte`/`lt`/`lte`)
translate; other condition types are not yet supported.

## Metric name mapping

| Service | Service metric | BORSUK metric |
|---|---|---|
| Pinecone | `cosine` / `euclidean` / `dotproduct` | `cosine` / `euclidean` / `inner-product` |
| turbopuffer | `cosine_distance` / `euclidean_squared` | `cosine` / `squared-euclidean` |
| S3 Vectors | `cosine` / `euclidean` | `cosine` / `euclidean` |
| Chroma | `l2` / `cosine` / `ip` | `euclidean` / `cosine` / `inner-product` |
| Qdrant | `Cosine` / `Euclid` / `Dot` | `cosine` / `euclidean` / `inner-product` |
