# Single-root two-bit generation API

The object-native candidate now has a public Rust metadata opener, planner and
conditional on-demand SQ8 search path. `TwoBitGeneration` uses no PQ router,
full-vector plane or resident SQ8 cache. This is an integration candidate;
full paired quality, HTTP, lifecycle and vendor gates remain required.

```rust
use borsuk::two_bit_generation::{TwoBitGeneration, TwoBitGenerationLimits};

let generation = TwoBitGeneration::open(metadata_dir, trusted_root_sha256,
    TwoBitGenerationLimits {
        max_memory_bytes: 1_073_741_824,
        max_active_queries: 1,
        max_query_bytes: 84 * 256 * 780, // frozen 100k D768 gate
        max_query_gets: 32,
        max_parallel_gets: 32,
        max_query_scratch_bytes: 400_000,
        already_pinned_bytes: 0,
    })?;
let result = generation.search(&s3_reader, query, 100).await?;
// result.plan: physical ranges/bytes; result.ranked: hits and read stats.
```

A trusted `borsuk-two-bit-generation-v1` root manifest binds generation ID,
`plane/manifest.json`, page manifest, centroids, graph, graph resident size,
SQ8 object SHA/key/ETag, and SQ8 low/step calibration. Its digest must come
from the application's authenticated publication authority. All child paths
are fixed. Unknown/incompatible schemas fail; no legacy fallback is attempted.
The completed source-plane manifest is described in `two-bit-source-builder.md`.

The root is currently assembled from source-only preparation artifacts; a public
end-to-end create/publish API is still pending. Remote metadata bootstrap is
available through `TwoBitGeneration::open_remote`.
Creation must bind calibration and centroids from the same immutable SQ8 build.
Do not manufacture a root from untrusted metadata and call its computed digest
trusted. Wrong bindings, corruption and admission failures reject the generation.

Queries use cosine geometry: finite nonzero inputs are normalized with a f64
norm before centroid routing and final SQ8 scoring. Inputs whose squared norm
is within1e-6 of one retain their original f32 values to preserve the frozen
normalized workload. Two-bit nomination retains its existing cosine scoring.
Zero, nonfinite and wrong-width queries fail before routing. Normalization
reuses admitted preparation scratch; it does not increase the payload cap.

The graph uses the frozen V296 one-unit/128-evaluation seed, then158 additional
pages/1272 evaluations. Up to159 candidate pages are scored with the Rust
reconstructed-cosine two-bit codec; the existing budgeted planner selects and
bridges page ranges. Smaller artifacts use their complete page count. The
semaphore spans planning through fetch and final SQ8 ranking. Direct `plan`
uses the same admission semaphore. CPU planning is synchronous within the async
call; HTTP scheduling/throughput must be measured before release qualification.

Nomination metadata remains resident: D768 codes alone are200bytes/row, or
20GB at100M (projection). This is not a qualified100M envelope. Local open
admits three copies of metadata files,64times the source-mean file size
for padded codec storage/temporaries,128KiB root/plane manifest overhead,
graph towers, old pins, and per-query lookup/planning
plus three SQ8 byte buffers. Allocator/runtime/transport/OS file cache are outside
the model; measure them including generation overlap. The existing reader's
stats count submitted GETs, authenticated bytes and failed GETs; failed-response
wire bytes are not yet reported. Total lifecycle cost therefore remains open.

Offline development demo (no S3 reads, no service timing claim):

```sh
cargo run -p borsuk --bin two_bit_plan_demo -- \
  ROOT ROOT_SHA DEVELOPMENT_REQUESTS REQUESTS_SHA NEW_OUTPUT
```

The runner accepts at most1000 authenticated sequentially numbered requests
and uses the frozen D768 byte, GET and scratch limits. Its default is
development0–63; optional FIRST COUNT accepts only0 64,256 64 or256 744.
Output preserves original query ordinals. It cannot certify recall, service latency or vendor
superiority. Complete paired validation and cold HTTP come next once the
intended remote execution target is available. Insert/update/delete, publication,
restart recovery beyond immutable reopen, and in-process compaction/GC remain
release blockers.

## Verified integration increment

The focused Rust test passes: immutable reload and planning, wrong root/SQ8
and generation binding, corrupted graph/centroid/page/code metadata, and small
memory rejection. The API demo reproduces all64 CoHere first100k D768
**development0–63** V296 physical fetched pages, GET counts and byte counts
exactly. The debug replay took14.65seconds with37,796KiB GNU-time peak RSS,
zero swaps and no S3 reads. These numbers describe offline integration
verification, not query percentiles, production RSS/QPS, recall qualification or
a vendor comparison. Receipts are `docs/research/native-two-bit-generation-*`.

## Open metadata directly from object storage

```rust
let generation = TwoBitGeneration::open_remote(
    &store, &metadata_prefix, trusted_root_sha256, limits, scratch_parent,
).await?;
```

Use an application-authorized immutable prefix and a root digest obtained from
its publication authority. The opener streams eight fixed metadata objects to
an owned temporary directory, verifies the root before further downloads, then
calls the same local authenticated opener. JSON manifests are capped at64KiB,
total staged metadata at the memory allowance less older pins; decoded metadata
and concurrent query buffers must still pass local admission. Scratch is removed
when opening succeeds, fails or is cancelled. SQ8/source objects are not fetched.
The two-bit nomination records and centroid/graph metadata become resident.

The caller configures metadata store credentials, retries and transport buffers;
bootstrap transport overhead/retry traffic is outside the payload model and the
query range counters. This API has metadata-only InMemory coverage, not measured
cold S3 bootstrap time, serving latency or cloud cost. Public create/publication,
full paired quality, cold HTTP and incremental maintenance remain required.

The focused remote check passes using metadata-only InMemory storage. It covers
local/remote range equality, cleanup on success/failure, wrong-root, corrupt or
missing code metadata and insufficient cap. This is functional API evidence,
not an S3 benchmark. Source/test receipts are `native-two-bit-remote-check.json`
and `native-two-bit-remote-test.txt` in `docs/research`.

## Publish and reload a prepared generation

```rust
use borsuk::two_bit_store::{publish_two_bit_generation, read_two_bit_head};

let expected = read_two_bit_head(&store, &index_prefix).await?;
let pinned = publish_two_bit_generation(
    &store, &index_prefix, prepared_metadata_dir, trusted_prepared_root_sha256,
    limits, expected.as_ref(),
).await?;
let generation = TwoBitGeneration::open_remote(
    &store, &pinned.metadata_prefix(), pinned.root_sha256(), limits, scratch_parent,
).await?;
```

The application must authorize this store/prefix and head writers. Use a
conditional-put-capable store. The opaque token is bound to the index prefix;
use it with the same store that issued it. New IDs must exceed the expected
head's generation. All fixed metadata files are streamed, length/SHA checked
before multipart completion, under `generations/ROOT_SHA/`. The head changes
last via create-or-CAS. A lost head-write acknowledgement is reconciled against
authenticated readback. Failed staging or stale publication can leave unreachable
metadata; in-process GC is still a release requirement. Root digests authorize
content identity; store/application policy authorizes writers.

SQ8 must already exist at the immutable key/ETag in the trusted prepared root.
Publication checks its HEAD length and ETag and never rewrites/downloads it;
this is not a whole-object rehash. The caller's authenticated immutable SQ8
upload is a construction prerequisite. Query page hashes still fail closed on
corrupt data. Metadata validation uses the serving opener and releases its
resident metadata before upload. Multipart read/copy buffers are admitted
against remaining payload allowance; transport and allocator overhead still
require measurement. Caller-owned preparation files must stay immutable.

This completes **prepared-generation publication**, not raw-source layout/SQ8
creation, incremental mutations, compaction/GC or cold HTTP qualification. The
candidate has no legacy head fallback. Canonical nested object keys are used;
slash-containing strings are split into path segments rather than passed as
one encoded `Path::join` segment.

Publication functional checks pass in the synthetic InMemory fixture: initial
head/reload, generation2 advancement, stale writer and namespace rejection,
invalid metadata leaving the head unchanged, old pinned-root reload and SQ8
ETag preservation. See `docs/research/native-two-bit-publication-check.json`.
Lost-ack reconciliation has not been fault-injected; live S3/CAS/error/cost
qualification remains required. No new recall or performance claim follows.

## Build generation metadata in Rust

```rust
use borsuk::{two_bit_build::TwoBitGenerationBuilder, two_bit_source::TwoBitSource};

let root_sha = TwoBitGenerationBuilder {
    source: TwoBitSource { raw: raw_path, raw_sha256, sq8: sq8_path, sq8_sha256,
        rows, dimensions },
    generation,
    low, step,
    sq8_object_key, sq8_etag,
}.build(new_metadata_dir, max_build_payload_bytes)?;
// Pass new_metadata_dir/root_sha to publish_two_bit_generation.
```

Inputs are approved immutable source/SQ8 snapshots in the frozen physical order.
Raw rows are ordinal f32; the SQ8 IDs are their permutation. Calibration must
come from the same SQ8 encoder; construction does not infer that relationship
from checksums. The builder accepts no query/GT and refits no layout. It streams
the source codec, centroids and page digests, builds the existing centroid graph,
then writes the root manifest last. Repeated output paths are rejected. Failed
builds leave unpublished scratch for caller cleanup. Local staging rename can be
lost on crash; remote conditional publication establishes the durable generation.

Graph build payload is conservatively admitted before allocating:

`262144 + 128*D + 256*(D+12) + 32*ceil(N/256) + ceil(N/32)*(32*D+16384)` bytes.

The source codec also applies its own ID/bitset/buffer admission. These are
payload models, not measured RSS guarantees. Runtime/allocator/OS cache and
already pinned generations are additional caller charges. SQ8/source matrices
are streamed; centroid and adjacency workspace is resident.100M build time,
RSS and cost are unqualified. A single-node graph supports tiny indexes; the
multi-node builder/encoding arithmetic is unchanged. Source/SQ8 zero norms
are rejected for the cosine route.

The synthetic focused test reproduces all eight manually sealed metadata files
byte-for-byte and covers no-overwrite, budget refusal before output, singleton
open/plan and zero-norm failure without a root. The same check retains publication,
remote reload and stale-writer cases. This closes manual metadata/root assembly;
raw-source semantic layout fitting and SQ8 encoding, full paired quality, cold
HTTP, mutation and compaction/GC still require implementation/qualification.

## Cosine input correction

The query boundary now normalizes raw queries before graph routing and SQ8
scoring. The focused integration check passes for extreme finite scales and
invalid inputs. A replay of the original64 CoHere development requests changed
58 physical plans: those requests are unnormalized, and the frozen V296 harness
also fed raw queries to Euclidean graph/SQ8 stages against a normalized corpus.
Historical V296 quality remains evidence for its original arithmetic; it does
not qualify the corrected serving path. Fresh paired development and validation
are required before scale or release. The replay took14.73seconds/37,720KiB
peak RSS in debug mode, with zero S3 reads; these are not service measurements.
See `docs/research/native-two-bit-query-check.json` and preserved raw plans.

The corrected CoHere development0–63 screen subsequently passed: mean R@100
99.00%, p05 97; paired flat SQ8 99.234375%/98, deficit0.234375pp. Maximum
32GET/16,773,120bytes. This is development-only offline evidence. Full paired
validation and serving qualification remain open; see the corrected-method
receipt/decision in `docs/research/native-cosine-development-*`.

The corrected CoHere validation256–319 short screen also passed: returned
99.015625%/p05 98 against paired full SQ8 99.21875%/98, deficit0.203125pp;
32GET/16,773,120bytes maximum. This does not qualify the full744-query split
or the ReLAION dataset. Receipts: `docs/research/native-cosine-validation-*`.
