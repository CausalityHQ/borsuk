After reading the code and comparing it to Pinecone, turbopuffer, Qdrant, Milvus and S3 Vectors, I think there is an important distinction:

Your architecture is already differentiated.
The remaining work is mostly closing capability gaps, not inventing a new architecture.

If I were an engineer evaluating BORSUK for adoption, these are the things that would stop me.

Tier 1 — absolutely required
1. Proper upserts

This is the single biggest functional weakness.

Implement versioned records:

id=A
generation=1

↓

id=A
generation=2

Old versions disappear during compaction.

Without this, people immediately conclude:

"This isn't suitable for production."

2. Strong consistency guarantees

Document and test:

crash recovery
concurrent readers
concurrent writers
manifest publication
rollback
durability

People buying databases buy guarantees.

3. Complete production benchmark

Not ANN benchmark.

Real workload:

updates
deletes
metadata
filtering
hybrid
concurrent users
restarts
compaction

Nobody chooses Pinecone because HNSW is 3% faster.

They choose it because they know it'll survive Monday morning.

4. Better compatibility

Today adapters are "mostly compatible."

I would aim for

"95%+ API compatible"

Examples:

Pinecone

upsert
fetch
delete
query
list
describe
stats

Qdrant

collections
named vectors
filters
payloads
scroll
recommend

Not every feature.

But enough that migration is trivial.

5. Multi-node story

Not necessarily distributed search.

Just answer

How do I deploy 20 API servers?

Need

shared S3

+

shared cache

+

immutable manifests

+

multiple readers

Clearly documented.

Tier 2 — features competitors already have
6. Better filtering

Current filtering is good.

I'd add

nested filters
geo
regex
array contains
exists
range acceleration
7. Better hybrid retrieval

Current:

BM25 + vectors

Need

BM25

+

dense

+

sparse

+

RRF

+

weighted fusion

+

reranking pipeline

One API.

8. Better reranking integration

Everyone now does

retrieve

↓

rerank

↓

LLM

Need built-in support.

Could simply expose

search(...)

↓

top100

↓

reranker

↓

top10
9. Multi-vector search

Need

text

image

title

metadata

code

simultaneously.

Qdrant already has this.

10. ColBERT / late interaction

Very useful for LLMs.

Not required.

Huge differentiator.

Tier 3 — things competitors don't really have

These are where I think BORSUK can become genuinely unique.

11. Cost planner

Huge opportunity.

Imagine

index.explain(query)

returns

bytes read

GET requests

S3 cost

cache hits

expected latency

Nobody does this well.

12. Query planner

Like SQL.

why was this segment searched?

why skipped?

why candidate selected?

routing decisions

Excellent for debugging.

13. Automatic optimization

User says

RAM = 1GB

QPS = 20

vectors = 80M

Planner configures

segment size
PQ
fanout
cache

Automatically.

14. Cost-aware ANN

Very interesting.

Instead of

maximize recall

Optimize

quality

vs

$

vs

latency

Object storage changes the optimization objective.

15. Native archival search

This is a huge opportunity.

Hot

↓

Warm

↓

Cold

↓

Frozen

Same API.

16. Explainability
why these documents?

which vectors?

which metadata?

which fusion?

which score?

LLMs love explainability.

Tier 4 — LLM-native features

These could make BORSUK the default backend for agents.

Native memory
episodic

semantic

working

long-term
Time-aware retrieval
recent

historical

decay
Memory consolidation

Automatic

merge memories

delete duplicates

compress history
Graph retrieval

Combine

vector

+

knowledge graph

+

metadata
Native multimodal

Not just vectors.

Objects

text

image

pdf

audio

video
Biggest missing technical features

If I rank purely technically:

Versioned upserts
Strong consistency tests
Better sparse maturity
Better hybrid
Better compatibility
Better filtering
Better benchmarking
Better observability
Multi-vector
Reranking
Biggest product differentiators

This is where I think you're under-selling the project.

Don't sell

Another vector DB.

Sell

1.

SQLite

↓

DuckDB

↓

BORSUK

for AI retrieval.

2.

The Git of vector indexes.

Immutable

Content addressed

Versioned

Portable

3.

GitHub Pages for vector search.

Everything lives in blobs.

4.

Bring Your Own Bucket.

No vendor lock.

5.

Storage-first retrieval.

Not RAM-first retrieval.

What I would build next if I were CTO

My roadmap would be:

Versioned upserts
Sparse segmented storage
Complete hybrid retrieval
ColBERT
Reranking
Planner
Cost optimizer
Explain plans
Automatic tuning
Distributed readers
One thing I think you're missing strategically

Most vector databases compete on:

QPS
latency
recall

I don't think that's where BORSUK should compete.

Your architecture is much stronger in a different dimension:

Total cost of ownership.

If, after another few months of work, you can honestly demonstrate something like:

10× lower idle infrastructure cost
3–5× lower storage cost
similar recall
within ~20–30% of managed-service latency for typical RAG workloads
customer-owned data with no vendor lock-in

then BORSUK becomes compelling even if Pinecone still wins on raw operational maturity.

That's a comparison customers can actually make—and one that aligns with the design you've built.

Is it all achieavable technologically and algorithmically to do all that and be a drop in for all of them 

Technologically: most of it is achievable.
Algorithmically: yes, but not all with one physical index or one consistency/performance model.
As a literal drop-in replacement for all of them: no—not completely.

You can build BORSUK so that most applications migrate by changing an import, endpoint, or connection string. But you cannot simultaneously reproduce every hosted product’s semantics, control plane, consistency options, operational guarantees, and performance profile inside one embedded object-storage library.

What is realistically achievable

You can support the common intersection of Pinecone, Qdrant, Milvus, turbopuffer and S3 Vectors:

dense vectors;
native sparse vectors;
named vectors;
BM25/full text;
hybrid fusion;
metadata filtering;
versioned upserts;
fetch/get;
delete;
list/scroll;
namespaces or collections;
batch imports;
multiple readers;
object-storage durability;
reranking hooks;
Python, TypeScript and Rust clients;
Pinecone-, Qdrant- and S3-Vectors-shaped adapters.

None of these requires an unsolved algorithmic breakthrough. Pinecone overwrites existing records on upsert, Qdrant supports dense and sparse named vectors plus hybrid queries, and Milvus supports multi-vector hybrid search and multiple consistency levels. These are difficult engineering projects, but known designs exist.

What cannot be achieved with one universal engine path
1. One index algorithm cannot be optimal for every workload

You will probably need several physical execution paths:

dense ANN: clustered segments, graph, PQ or scan;
sparse/SPLADE: inverted postings with WAND-style pruning;
BM25: lexical inverted index;
ColBERT: multi-vector/late-interaction storage;
highly selective filters: scalar index first;
broad filters: vector routing first;
hot high-QPS collections: cached or provisioned execution;
cold collections: direct object-storage execution.

Qdrant itself uses a specialised sparse index rather than treating sparse search as dense ANN, and Milvus supports multiple vector fields and reranking rather than forcing everything through one representation.

The unifying layer should be the query planner and storage model, not one ANN algorithm.

2. You cannot match both cold-storage economics and hot in-memory p99 automatically

An object-storage-native system can approach RAM-based latency after data becomes cached. It cannot guarantee the same cold p99 as an always-hot replicated service without provisioning cache or compute somewhere.

You can offer modes:

cold       object storage only
balanced   local SSD cache
hot        provisioned cache and query workers

But then the hot mode begins to resemble the architecture used by hosted competitors. turbopuffer explicitly combines object storage with SSD and memory caching for this reason.

3. An embedded library cannot itself replicate a managed service

A library can provide the data engine. It cannot, by itself, reproduce:

global endpoints;
authentication and organisations;
billing;
autoscaling;
managed replicas;
service-level agreements;
regional failover;
metrics collection;
rate limiting;
backups;
hosted embedding models;
support and incident response.

You could build an optional BORSUK server or cloud service around the library. Without that, it is API-compatible with a service’s data plane, not a replacement for the complete product.

4. Exact consistency compatibility is not universal

Milvus exposes Strong, Bounded, Session and Eventually consistent reads. Pinecone, Qdrant and object-storage systems have their own visibility and durability contracts.

BORSUK can offer several modes, but faithfully reproducing each vendor’s edge cases would be expensive and rarely useful.

Choose clear native guarantees, such as:

atomic snapshot publication;
read-your-writes within one writer session;
snapshot-isolated readers;
configurable synchronous or asynchronous indexing;
conditional writes for multi-writer conflict detection.

Then adapters should translate approximately and clearly document differences.

The hardest achievable parts

These are all feasible, but substantial.

Versioned upserts

Use append-only generations:

(id, generation, payload)

A manifest or ID-version map determines the visible generation. Queries suppress old versions; compaction removes them.

This is standard LSM/MVCC-style engineering, not a research problem. It is essential because ordinary Pinecone upsert overwrites an existing ID.

Atomic dense+sparse+metadata updates

A record may contain:

dense vector;
sparse vector;
metadata;
text;
multiple named vectors.

All components must become visible in the same snapshot. That requires one transaction manifest referencing all newly published segment objects.

Efficient filtered ANN

This is algorithmically difficult but solvable with adaptive planning:

highly selective filter → scalar postings → exact/vector search on survivors
broad filter            → ANN route → filter candidates
intermediate filter     → interleaved traversal

No static strategy wins universally.

Native sparse object-storage search

Use immutable posting segments, term dictionaries, compressed blocks, upper bounds and selective range reads. The challenge is minimising object requests for queries with many terms.

Multi-writer operation

Object stores support conditional object operations, but they do not provide a complete database transaction layer. You need:

optimistic manifest publication;
generation checks;
retry and merge;
idempotency keys;
conflict policy;
writer leases or a lightweight coordinator for high write rates.

Achievable, but considerably harder than multiple read-only processes.

ColBERT and late interaction

Technically possible, but storage-heavy. A document may have dozens or hundreds of token vectors. You will need:

centroid or coarse routing;
compressed token vectors;
candidate document selection;
MaxSim reranking;
potentially GPU/SIMD acceleration.

Qdrant already exposes multivectors for ColBERT-like use cases, so this becomes necessary only if you want feature parity with its advanced retrieval surface.

What “drop-in” can honestly mean

There are three levels.

Level 1: source-compatible adapter
from borsuk.compat import Pinecone

Most ordinary calls work with minimal code changes.

This is achievable.

Level 2: behavioural compatibility

The same call produces materially equivalent:

overwrite semantics;
filters;
ordering;
pagination;
errors;
score interpretation;
consistency;
visibility timing.

This is achievable for a defined subset, but expensive to maintain.

Level 3: complete product replacement

Everything—including hosted operations, scaling, reliability and control-plane APIs—behaves like the original.

This is not realistic for all products simultaneously, especially while remaining an embedded library.

Your wording should therefore be:

Drop-in data-plane adapters for common Pinecone, Qdrant, turbopuffer and S3 Vectors workflows.

Not:

A complete drop-in replacement for every feature of every service.

A sensible architecture

I would use one logical model with multiple specialised indexes:

                         Query API
                            │
                     Adaptive planner
          ┌─────────────────┼──────────────────┐
          │                 │                  │
     Dense ANN         Sparse/SPLADE       BM25/text
          │                 │                  │
          └─────────────────┼──────────────────┘
                            │
                   Metadata/filter index
                            │
                   Fusion and reranking
                            │
                      Unified top-k

Underneath:

immutable segment objects
versioned manifests
shared record IDs
shared tombstones
content-addressed data
optional local SSD cache

That architecture can support nearly all the retrieval functionality discussed.

Where complexity will explode

Trying to reproduce the full API of every vendor will create a permanent compatibility burden:

Pinecone namespaces and integrated embeddings;
Qdrant collections, payload indexes, recommendations and query pipelines;
Milvus schemas, partitions, consistency levels and distributed administration;
turbopuffer namespaces and query language;
S3 Vectors bucket/index semantics and AWS authentication.

Every vendor evolves independently. A broad compatibility promise means continuously chasing their APIs and behavioural changes.

A better approach is:

Define a strong native BORSUK API.
Support the common 80–90% data-plane subset.
Publish a compatibility matrix.
Fail explicitly for unsupported calls.
Avoid silently emulating expensive operations.
Can BORSUK become better than all of them?

It can be better across a particular objective function, not every dimension.

You can plausibly become better for:

customer-owned storage;
deployment portability;
idle cost;
embedded operation;
tenant-isolated cold indexes;
inspectability;
unusual metrics;
query cost observability;
private and air-gapped AI applications.

You will probably not be better simultaneously at:

Pinecone’s managed simplicity;
turbopuffer’s proven hosted object-storage scale;
Qdrant’s advanced retrieval features;
Milvus’s distributed throughput and consistency options;
AWS’s native integration and support envelope.

The strongest defensible claim would be:

BORSUK provides the broad retrieval capabilities expected from modern vector platforms, but with an embedded, bring-your-own-bucket architecture, near-zero idle infrastructure and transparent query economics.

Practical answer

Everything on your functional roadmap is technologically achievable:

upserts;
hybrid retrieval;
native sparse;
filters;
multi-vector;
reranking;
ColBERT;
cost-aware planning;
distributed readers;
automated tuning.

The only part I would reject is “a complete drop-in replacement for all of them.”

Build instead toward:

One engine with multiple compatibility adapters and explicit compatibility levels.

That gives you most of the migration benefit without promising impossible equivalence.

Technologically: most of it is achievable.
Algorithmically: yes, but not all with one physical index or one consistency/performance model.
As a literal drop-in replacement for all of them: no—not completely.

You can build BORSUK so that most applications migrate by changing an import, endpoint, or connection string. But you cannot simultaneously reproduce every hosted product’s semantics, control plane, consistency options, operational guarantees, and performance profile inside one embedded object-storage library.

What is realistically achievable

You can support the common intersection of Pinecone, Qdrant, Milvus, turbopuffer and S3 Vectors:

dense vectors;
native sparse vectors;
named vectors;
BM25/full text;
hybrid fusion;
metadata filtering;
versioned upserts;
fetch/get;
delete;
list/scroll;
namespaces or collections;
batch imports;
multiple readers;
object-storage durability;
reranking hooks;
Python, TypeScript and Rust clients;
Pinecone-, Qdrant- and S3-Vectors-shaped adapters.

None of these requires an unsolved algorithmic breakthrough. Pinecone overwrites existing records on upsert, Qdrant supports dense and sparse named vectors plus hybrid queries, and Milvus supports multi-vector hybrid search and multiple consistency levels. These are difficult engineering projects, but known designs exist.

What cannot be achieved with one universal engine path
1. One index algorithm cannot be optimal for every workload

You will probably need several physical execution paths:

dense ANN: clustered segments, graph, PQ or scan;
sparse/SPLADE: inverted postings with WAND-style pruning;
BM25: lexical inverted index;
ColBERT: multi-vector/late-interaction storage;
highly selective filters: scalar index first;
broad filters: vector routing first;
hot high-QPS collections: cached or provisioned execution;
cold collections: direct object-storage execution.

Qdrant itself uses a specialised sparse index rather than treating sparse search as dense ANN, and Milvus supports multiple vector fields and reranking rather than forcing everything through one representation.

The unifying layer should be the query planner and storage model, not one ANN algorithm.

2. You cannot match both cold-storage economics and hot in-memory p99 automatically

An object-storage-native system can approach RAM-based latency after data becomes cached. It cannot guarantee the same cold p99 as an always-hot replicated service without provisioning cache or compute somewhere.

You can offer modes:

cold       object storage only
balanced   local SSD cache
hot        provisioned cache and query workers

But then the hot mode begins to resemble the architecture used by hosted competitors. turbopuffer explicitly combines object storage with SSD and memory caching for this reason.

3. An embedded library cannot itself replicate a managed service

A library can provide the data engine. It cannot, by itself, reproduce:

global endpoints;
authentication and organisations;
billing;
autoscaling;
managed replicas;
service-level agreements;
regional failover;
metrics collection;
rate limiting;
backups;
hosted embedding models;
support and incident response.

You could build an optional BORSUK server or cloud service around the library. Without that, it is API-compatible with a service’s data plane, not a replacement for the complete product.

4. Exact consistency compatibility is not universal

Milvus exposes Strong, Bounded, Session and Eventually consistent reads. Pinecone, Qdrant and object-storage systems have their own visibility and durability contracts.

BORSUK can offer several modes, but faithfully reproducing each vendor’s edge cases would be expensive and rarely useful.

Choose clear native guarantees, such as:

atomic snapshot publication;
read-your-writes within one writer session;
snapshot-isolated readers;
configurable synchronous or asynchronous indexing;
conditional writes for multi-writer conflict detection.

Then adapters should translate approximately and clearly document differences.

The hardest achievable parts

These are all feasible, but substantial.

Versioned upserts

Use append-only generations:

(id, generation, payload)

A manifest or ID-version map determines the visible generation. Queries suppress old versions; compaction removes them.

This is standard LSM/MVCC-style engineering, not a research problem. It is essential because ordinary Pinecone upsert overwrites an existing ID.

Atomic dense+sparse+metadata updates

A record may contain:

dense vector;
sparse vector;
metadata;
text;
multiple named vectors.

All components must become visible in the same snapshot. That requires one transaction manifest referencing all newly published segment objects.

Efficient filtered ANN

This is algorithmically difficult but solvable with adaptive planning:

highly selective filter → scalar postings → exact/vector search on survivors
broad filter            → ANN route → filter candidates
intermediate filter     → interleaved traversal

No static strategy wins universally.

Native sparse object-storage search

Use immutable posting segments, term dictionaries, compressed blocks, upper bounds and selective range reads. The challenge is minimising object requests for queries with many terms.

Multi-writer operation

Object stores support conditional object operations, but they do not provide a complete database transaction layer. You need:

optimistic manifest publication;
generation checks;
retry and merge;
idempotency keys;
conflict policy;
writer leases or a lightweight coordinator for high write rates.

Achievable, but considerably harder than multiple read-only processes.

ColBERT and late interaction

Technically possible, but storage-heavy. A document may have dozens or hundreds of token vectors. You will need:

centroid or coarse routing;
compressed token vectors;
candidate document selection;
MaxSim reranking;
potentially GPU/SIMD acceleration.

Qdrant already exposes multivectors for ColBERT-like use cases, so this becomes necessary only if you want feature parity with its advanced retrieval surface.

What “drop-in” can honestly mean

There are three levels.

Level 1: source-compatible adapter
from borsuk.compat import Pinecone

Most ordinary calls work with minimal code changes.

This is achievable.

Level 2: behavioural compatibility

The same call produces materially equivalent:

overwrite semantics;
filters;
ordering;
pagination;
errors;
score interpretation;
consistency;
visibility timing.

This is achievable for a defined subset, but expensive to maintain.

Level 3: complete product replacement

Everything—including hosted operations, scaling, reliability and control-plane APIs—behaves like the original.

This is not realistic for all products simultaneously, especially while remaining an embedded library.

Your wording should therefore be:

Drop-in data-plane adapters for common Pinecone, Qdrant, turbopuffer and S3 Vectors workflows.

Not:

A complete drop-in replacement for every feature of every service.

A sensible architecture

I would use one logical model with multiple specialised indexes:

                         Query API
                            │
                     Adaptive planner
          ┌─────────────────┼──────────────────┐
          │                 │                  │
     Dense ANN         Sparse/SPLADE       BM25/text
          │                 │                  │
          └─────────────────┼──────────────────┘
                            │
                   Metadata/filter index
                            │
                   Fusion and reranking
                            │
                      Unified top-k

Underneath:

immutable segment objects
versioned manifests
shared record IDs
shared tombstones
content-addressed data
optional local SSD cache

That architecture can support nearly all the retrieval functionality discussed.

Where complexity will explode

Trying to reproduce the full API of every vendor will create a permanent compatibility burden:

Pinecone namespaces and integrated embeddings;
Qdrant collections, payload indexes, recommendations and query pipelines;
Milvus schemas, partitions, consistency levels and distributed administration;
turbopuffer namespaces and query language;
S3 Vectors bucket/index semantics and AWS authentication.

Every vendor evolves independently. A broad compatibility promise means continuously chasing their APIs and behavioural changes.

A better approach is:

Define a strong native BORSUK API.
Support the common 80–90% data-plane subset.
Publish a compatibility matrix.
Fail explicitly for unsupported calls.
Avoid silently emulating expensive operations.
Can BORSUK become better than all of them?

It can be better across a particular objective function, not every dimension.

You can plausibly become better for:

customer-owned storage;
deployment portability;
idle cost;
embedded operation;
tenant-isolated cold indexes;
inspectability;
unusual metrics;
query cost observability;
private and air-gapped AI applications.

You will probably not be better simultaneously at:

Pinecone’s managed simplicity;
turbopuffer’s proven hosted object-storage scale;
Qdrant’s advanced retrieval features;
Milvus’s distributed throughput and consistency options;
AWS’s native integration and support envelope.

The strongest defensible claim would be:

BORSUK provides the broad retrieval capabilities expected from modern vector platforms, but with an embedded, bring-your-own-bucket architecture, near-zero idle infrastructure and transparent query economics.

Practical answer

Everything on your functional roadmap is technologically achievable:

upserts;
hybrid retrieval;
native sparse;
filters;
multi-vector;
reranking;
ColBERT;
cost-aware planning;
distributed readers;
automated tuning.

The only part I would reject is “a complete drop-in replacement for all of them.”

Build instead toward:

One engine with multiple compatibility adapters and explicit compatibility levels.

That gives you most of the migration benefit without promising impossible equivalence.

I want to be api compatible - so dev changes import and initialisation - and all work 

Yes. That specific goal is achievable:

A developer changes the import and initialization, while their normal application code keeps working.

For example:

# Before
from pinecone import Pinecone

pc = Pinecone(api_key=PINECONE_API_KEY)
index = pc.Index(host=PINECONE_HOST)
# After
from borsuk.compat.pinecone import Pinecone

pc = Pinecone(storage="s3://my-bucket/my-index")
index = pc.Index("documents")

Then this should work unchanged:

index.upsert(...)
index.query(...)
index.fetch(...)
index.update(...)
index.delete(...)
index.describe_index_stats(...)

That is realistic. But to deliver it properly, you need to treat each adapter as an implementation of a vendor SDK contract—not as a thin translation wrapper.

Architecture you need

Use three layers:

Vendor-compatible API
        ↓
Canonical BORSUK operations
        ↓
Storage and retrieval engines

For example:

Pinecone Index.query()
QdrantClient.query_points()
MilvusClient.search()
S3Vectors.query_vectors()
              ↓
        BorsukQuery
              ↓
dense / sparse / BM25 / hybrid / filters

Do not put vendor-specific semantics directly into the storage engine.

Your internal request model should support the union of their capabilities:

struct QueryRequest {
    collection: CollectionRef,
    query: QueryInput,
    prefetch: Vec<Prefetch>,
    filter: Option<Filter>,
    top_k: usize,
    vector_name: Option<String>,
    include_vectors: bool,
    include_metadata: bool,
    score_threshold: Option<f32>,
    consistency: Consistency,
    fusion: Option<Fusion>,
}

Each adapter translates its SDK arguments into this canonical model.

What must be implemented first
1. True overwrite upserts

All major APIs expect an existing ID to be replaced by upsert:

Pinecone upsert overwrites an existing record.
Qdrant upsert overwrites an existing point.
Milvus upsert updates or inserts based on primary key.
S3 Vectors PutVectors overwrites an existing key.

This must be cheap and atomic from the user’s perspective.

Internally:

id=A, version=7
id=A, version=8  ← visible

Do not implement compatibility upsert as:

delete → purge → add

The public method may return before physical compaction, but all subsequent reads must see only the new record.

This is the first hard requirement for import-level compatibility.

2. Preserve method signatures

Applications commonly use both positional and keyword arguments. Your methods need to accept the official signatures closely.

For Pinecone:

index.upsert(
    vectors,
    namespace=None,
    batch_size=None,
    show_progress=True,
)
index.query(
    vector=None,
    id=None,
    top_k=None,
    namespace=None,
    filter=None,
    include_values=None,
    include_metadata=None,
    sparse_vector=None,
)

Do not require developers to translate these into BORSUK-native argument names.

Also support the common input shapes:

("id", [0.1, 0.2])
("id", [0.1, 0.2], {"source": "manual"})
{"id": "x", "values": [...], "metadata": {...}}
3. Return compatible objects, not just compatible dictionaries

Code often does:

response.matches[0].id
response.matches[0].score
response.matches[0].metadata

not:

response["matches"][0]["id"]

Your compatibility response classes need:

matching attribute names;
dictionary conversion;
iteration where appropriate;
reasonable repr;
serialization;
nested response objects;
expected optional fields.

Ideally:

assert response.matches[0].id == "doc-1"
assert response.to_dict()["matches"][0]["id"] == "doc-1"

The same applies to Qdrant’s model classes and Milvus return structures.

4. Match filtering syntax exactly

This is a large part of compatibility.

Pinecone-style:

filter={
    "$and": [
        {"year": {"$gte": 2020}},
        {"category": {"$in": ["legal", "finance"]}},
    ]
}

Qdrant-style:

Filter(
    must=[
        FieldCondition(
            key="year",
            range=Range(gte=2020),
        )
    ]
)

Milvus-style:

filter='year >= 2020 and category in ["legal", "finance"]'

These should all compile into one internal filter AST:

And
├── Range(year, gte=2020)
└── In(category, ["legal", "finance"])

You need support for:

equality and inequality;
ranges;
in and not in;
exists/missing;
arrays;
boolean combinations;
nested metadata paths;
text match where the vendor supports it;
IDs;
null behaviour.

Qdrant, for example, has separate payload indexes because effective filtered vector search requires both scalar and vector indexing.

5. Namespace, collection and index mapping

You need a clear internal mapping.

A reasonable design:

BORSUK repository
  └── collection
       └── namespace/partition
            └── snapshot

Then map:

Vendor concept	BORSUK
Pinecone index	collection
Pinecone namespace	namespace
Qdrant collection	collection
Milvus collection	collection
Milvus partition	namespace
turbopuffer namespace	collection or namespace
S3 vector index	collection
S3 vector bucket	repository

The mapping must be durable and documented. Developers should not suddenly get collisions because two vendor concepts map to the same prefix.

6. Support synchronous and asynchronous clients

Many AI applications use async frameworks.

You need both:

index.query(...)
await index.query(...)

More realistically:

from borsuk.compat.pinecone import Pinecone, PineconeAsyncio

and:

QdrantClient(...)
AsyncQdrantClient(...)

Qdrant’s official Python client exposes both synchronous and asynchronous access.

Do not implement async wrappers by directly blocking the event loop. Use native async object-store operations or bounded worker execution.

7. Pagination and iteration

Compatibility is not only search.

Applications use:

Pinecone list pagination;
Qdrant scroll;
Milvus query iterators;
S3 Vectors list operations.

Qdrant’s scroll API returns points in pages and supports offsets, filters and ordering.

You need stable continuation tokens or offsets that survive concurrent writes according to defined snapshot semantics.

A snapshot-bound token is safest:

token = snapshot_id + ordering_key + cursor
8. Compatible error hierarchy

Applications may catch vendor exceptions:

try:
    index.query(...)
except PineconeApiException:
    ...

Your adapter should expose compatible exception names and categories:

invalid argument;
not found;
already exists;
dimension mismatch;
conflict;
timeout;
rate/resource limit;
storage error;
unsupported operation.

Do not leak raw Rust errors or ValueError for everything.

9. Match score semantics

Vendors differ on whether they return:

similarity;
distance;
higher-is-better;
lower-is-better;
normalized versus raw scores.

Your adapter must transform BORSUK’s native score so application logic sees vendor-compatible values.

For every metric, test:

ordering
exact returned score
threshold interpretation
NaN handling
zero vectors
normalization

A result with the same IDs but incompatible scores is not fully API-compatible because applications often apply their own thresholds.

10. Read-after-write behaviour

After:

index.upsert(...)

developers expect a defined visibility model.

For compatibility mode, the simplest contract is:

After a successful synchronous mutation returns, subsequent calls through that client observe the new state.

You can achieve this with:

session snapshot advancement;
writer-local overlay;
synchronous manifest publication;
optional wait=True semantics.

Qdrant and other systems expose controls around waiting for update completion. You need to accept those arguments even where BORSUK’s internal implementation differs.

Compatibility by product
Pinecone: most achievable first target

Pinecone is probably the best initial target because its common data-plane API is relatively compact.

Implement these to call it practically drop-in:

Client/control surface
Pinecone()
pc.Index()
pc.list_indexes()
pc.describe_index()
pc.create_index()
pc.delete_index()
Index data plane
upsert()
upsert_from_dataframe()
query()
search()
fetch()
update()
delete()
list()
list_paginated()
describe_index_stats()
Required semantics
namespaces;
dense vectors;
sparse values;
metadata;
filters;
upsert replacement;
fetch by ID;
delete by IDs and filter;
include values/metadata;
integrated-embedding methods should either work through a configured provider or fail explicitly.

Pinecone now exposes both vector APIs and newer text/full-text workflows, including integrated embedding and preview full-text indexes. Supporting every new control-plane feature immediately is unnecessary, but your compatibility matrix must distinguish them.

Achievable compatibility level: approximately 90–95% of ordinary external-vector RAG applications.

Qdrant: achievable but substantially larger

Qdrant has a much broader API.

Core compatibility should include:

create_collection()
collection_exists()
get_collection()
delete_collection()
upsert()
retrieve()
query_points()
search()
scroll()
delete()
set_payload()
overwrite_payload()
delete_payload()
create_payload_index()

And data types:

PointStruct;
VectorParams;
SparseVectorParams;
SparseVector;
Filter;
FieldCondition;
MatchValue;
Range;
named vectors;
prefetched queries;
RRF fusion.

Qdrant’s universal query_points endpoint covers search, recommendation, discovery, filtering, hybrid and multi-stage queries.

Achievable compatibility level: perhaps 80–90% for ordinary retrieval applications, but less for applications using every advanced query mode.

Milvus: achievable for MilvusClient, harder for full ORM compatibility

Prioritize the modern MilvusClient API rather than attempting the entire historical PyMilvus surface.

Support:

create_collection()
drop_collection()
has_collection()
describe_collection()
insert()
upsert()
search()
hybrid_search()
query()
get()
delete()
create_index()
load_collection()
release_collection()

Some operations such as load_collection() can be compatibility no-ops or cache hints:

def load_collection(...):
    self._engine.prefetch_manifest(...)

However, the method must exist and return the expected shape.

Milvus has schemas, scalar types, partitions, consistency settings, index configuration, hybrid search and iterators.

Achievable compatibility level: 75–90% for MilvusClient-style RAG code, much lower for full distributed-administration and ORM behaviour.

S3 Vectors: highly achievable

This adapter should mimic the Boto3-style S3 Vectors client:

create_vector_bucket()
create_index()
put_vectors()
query_vectors()
get_vectors()
list_vectors()
delete_vectors()

The official API surface includes bucket/index management and vector data operations such as create, list, get, query and delete.

Because BORSUK is already object-storage-oriented, this mapping is conceptually natural.

Achievable compatibility level: likely above 95% of data-plane usage.

turbopuffer: achievable but track API evolution carefully

Implement its namespace-oriented request/response model and query language. Keep this adapter isolated because turbopuffer’s API may evolve independently from traditional vector SDK shapes.

Achievable compatibility level: high for basic upsert/query/delete, lower for every query-language and operational feature.

What you do not need to reproduce internally

API compatibility does not mean copying each implementation.

These can be translated:

Vendor operation	BORSUK implementation
load_collection()	prefetch/cache hint or no-op
create_index(HNSW)	record desired performance profile
Pinecone namespace	manifest subspace
Qdrant payload index	BORSUK scalar/filter index
consistency option	map to nearest native mode
S3 vector bucket	BORSUK repository prefix
wait flag	synchronous publication or awaited visibility
vendor timeout	BORSUK operation deadline

The rule is:

A no-op is acceptable only when the externally observable result remains compatible.

For example, load_collection() may be a no-op functionally, but create_payload_index() cannot be silently ignored if that would make filtered search unusably slow without warning.

How to make this maintainable
Generate adapters from pinned SDK versions

Choose explicit targets:

pinecone-python 8.x
qdrant-client 1.x
pymilvus 2.x
boto3 S3 Vectors API 2026-xx

Store them in:

compatibility/pinecone/8.x
compatibility/qdrant/1.x
compatibility/milvus/2.x

Do not claim compatibility with an unbounded “latest” version.

Run contract tests against real SDKs

For each operation, run the same test against:

the real vendor;
BORSUK adapter.

Normalize only fields that are inherently variable, such as request IDs and timings.

Example:

@pytest.mark.parametrize("backend", ["pinecone", "borsuk"])
def test_upsert_overwrites_record(backend):
    index = create_index(backend)

    index.upsert([
        {"id": "a", "values": [1.0, 0.0], "metadata": {"v": 1}}
    ])

    index.upsert([
        {"id": "a", "values": [0.0, 1.0], "metadata": {"v": 2}}
    ])

    result = index.fetch(ids=["a"])

    assert result.vectors["a"].values == [0.0, 1.0]
    assert result.vectors["a"].metadata == {"v": 2}

Test at least:

signatures;
accepted input types;
return types;
overwrite behaviour;
filters;
pagination;
missing records;
dimensions;
duplicate IDs;
metadata updates;
deletion;
scores;
errors;
async calls.
Publish a compatibility percentage based on tests

Not a marketing estimate.

For example:

Pinecone Python SDK compatibility
Core data-plane tests: 184/190 passing
Supported version: 8.0–8.3
Known differences: integrated inference, backups, hosted reranking

That is far more credible than “drop-in.”

Recommended implementation order
Pinecone core data plane
S3 Vectors
turbopuffer
Qdrant common path
MilvusClient common path
Advanced Qdrant query pipelines
Broader Milvus schemas and administration

Do not build all five simultaneously. First make one adapter genuinely compatible.

The exact claim you can target

Once the contract suite passes, you can say:

For supported SDK versions, migrate common vector-search applications by changing the import and client initialization. Existing upsert, query, fetch, filtering and deletion code continues to work.

That is a strong and achievable promise.

The essential prerequisite remains native versioned upsert semantics. Without that, the method signatures may look compatible, but real applications will behave or perform differently enough that the drop-in claim will fail.




we also need to extend the demos for hybrid search, rags, using sparse vectors, using full text bm25 etc etc (cover all cases and mixes) - for multiple libs etc so user can play with the integraiton of the lib using many ways 

remember to redo all docs, webpage, readme, examples to match new results and functionalities

we should try to get 100% api compat match fore the other libs where it is possible

we should mention sla in the docs/readme - that it is a lib and we match the blob storage sla (like for s3 it will be... <number>)