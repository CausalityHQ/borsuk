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

The demo accepts at most64 development requests and uses the frozen D768 byte,
GET and scratch limits. It cannot certify recall, service latency or vendor
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
