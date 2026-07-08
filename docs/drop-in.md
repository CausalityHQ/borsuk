# Drop-in Replacements

BORSUK ships thin client adapters that emulate the data-plane surface of
**Pinecone**, **turbopuffer**, and **Amazon S3 Vectors**. Existing code switches
backend by changing the import and pointing at a BORSUK storage root — no rewrite
of your upsert/query/delete calls.

Each adapter maps a **namespace** (Pinecone/turbopuffer) — or an **index inside a
bucket** (S3 Vectors) — to its own BORSUK index under a shared base URI. Isolation
is physical: `<base_uri>/<namespace>`. Metadata and filtered search work exactly
as they do natively; the filter dialects are handled per service.

## Honest limits

These are **local, embedded** backends over object storage, not network
services. They do not emulate the vendors' auth, replication, billing, pod types,
or exact server-side consistency — they give BORSUK's own publish semantics. A
hit's `score`/`distance` is BORSUK's distance under the index metric, not the
vendor's similarity score. Filter-based delete, sparse vectors, and BM25/hybrid
ranking are not emulated. Metric names are mapped to BORSUK's
(`cosine`, `euclidean`, `squared-euclidean`, `inner-product`).

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
`fetch`, `delete` (by ids), `describe_index_stats`. `delete` by filter / `delete_all`
raise `NotImplementedError` for now.

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
`NotImplementedError` for now; BM25/hybrid ranking is out of scope.

## Metric name mapping

| Service | Service metric | BORSUK metric |
|---|---|---|
| Pinecone | `cosine` / `euclidean` / `dotproduct` | `cosine` / `euclidean` / `inner-product` |
| turbopuffer | `cosine_distance` / `euclidean_squared` | `cosine` / `squared-euclidean` |
| S3 Vectors | `cosine` / `euclidean` | `cosine` / `euclidean` |
