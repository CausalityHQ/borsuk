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

A trusted `borsuk-two-bit-generation-v2` root manifest binds generation ID,
`plane/manifest.json`, page manifest, centroids, graph, graph resident size,
SQ8 object SHA/key/ETag, SQ8 low/step calibration, and a canonical normalized
FP32 source descriptor (geometry/length/SHA/approved key). Its digest must come
from the application's authenticated publication authority. All child paths
are fixed. Unknown/incompatible schemas fail; no legacy fallback is attempted.
The completed source-plane manifest is described in `two-bit-source-builder.md`.

The public source writer, `TwoBitGenerationBuilder`, conditional publisher and
`TwoBitGeneration::open_remote` provide native construction/publication/reload.
Their qualification and lifecycle gates remain open.
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
admission model also charges ranking workspace by maximum fetched row count
(min(source rows, byte cap/(D+12))), plus coordinate weights. A conservative
256B per returned row covers score-vector growth, local/global results,
ordinal rosters, duplicate/membership sets and sorting. Response, ranking and
planner/codec allowances are multiplied by configured active queries. The
older PQ object-native loader uses the same estimator, including one planner
allowance per concurrent query. This is a checked payload model, not a measured
RSS guarantee; allocator/runtime/transport overhead requires separate headroom.
The ranker returns a fresh top-k score buffer; the full fetched-row score
capacity is dropped before returning to the caller. Retained result objects
remain caller-owned memory. The semaphore spans planning through fetch and
final SQ8 ranking. Direct `plan`
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
head's generation. Replacing an existing head requires sealing its mutation head first
(see below), even when there are no pending rows. The canonical source and all fixed metadata files are streamed, length/SHA checked
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

This function publishes a prepared generation; the sections below cover source
construction and incremental mutations. Compaction/GC and cold HTTP qualification
remain open. The
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
Raw rows are ordinal f32; SQ8 IDs can be logical signed64-bit IDs when the explicit source order is supplied. Calibration must
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

The full corrected CoHere validation256–999 split (744 queries) now passes:
99.131720% mean R@100/p05 98 versus paired flat SQ8 99.315860%/98,
deficit0.184140pp; maximum32GET/16,773,120bytes. This qualifies this offline
single-dataset quality gate only. ReLAION, cold HTTP, lifecycle, scale and
vendor gates remain open. Authenticated Spark SSH was revalidated and is
available for heavier ReLAION preparation.

## Command-line metadata assembly

The small build_two_bit_generation binary calls the same public builder:

    build_two_bit_generation CONFIG CONFIG_SHA MAX_MEMORY_BYTES NEW_OUTPUT

The authenticated JSON configuration is capped at64KiB and rejects unknown
fields. It contains raw/raw_sha256, sq8/sq8_sha256, rows, dimensions, generation,
low, step, sq8_object_key and sq8_etag. Paths identify caller-owned immutable
local snapshots; calibration must come from that approved SQ8 build. The
command prints the completed root SHA and refuses output overwrite through the
underlying builder. It does not fit layout/SQ8, read queries/GT, upload data or
authorize the computed root as a serving authority. Use a real immutable SQ8
ETag before remote publication; the offline harness placeholder is never a
production object identity.

## Frozen paired100k offline quality

The corrected library nomination/physical plans now pass full validation256–999
on both datasets (744 queries each): CoHere99.131720%/p05 98 versus paired
SQ8 flat99.315860%/98; ReLAION99.162634%/97 versus99.568548%/99. Both
meet98mean/p05 95/0.5pp deficit/32GET/16MiB. This freezes the candidate
for product integration. Final scoring uses the established sequential-f32
Python mirror; live Rust/S3 serving, raw-source creation, mutations/GC, package/CI,
1M/scale and vendor gates remain open. See paired quality decision/receipts.

## Actual Rust range-serving integration

The four focused range-reader tests pass on Spark. A new two-row D2 fixture
builds/publishes/reads head/reopens through the public API, then searches over
signed conditional HTTP ranges. Expected ranking survives normal/huge/tiny
finite query magnitude; successful queries charge one submitted GET and28
verified bytes. Corrupt bytes, changed ETag and HTTP412 fail with one charged
failed GET and no hidden retry. Existing wrong-range/truncated/500/tail checks
also pass. Metadata publication uses conditional InMemory storage; this is
synthetic protocol integration, not real AWS IAM or benchmark qualification.
Failed-response wire bytes are not covered by verified-byte stats.
The Rust test bundle must include scripts/fixtures, referenced by existing
library tests. See native-two-bit-http-serving-check.json and its source receipt.

## Native SQ8 preparation

`borsuk::sq8_source::build_sq8_source(source, source_sha256, dimensions,
order, output, max_payload_bytes)` writes the existing SQ8 body from immutable
normalized little-endian f32 rows and an approved complete ordinal permutation.
It derives source-only min/max calibration and returns `low`, `step` and the
body SHA for `TwoBitGenerationBuilder`. Upload that body first and pass its
strong ETag to the generation builder/publication path. No query/truth input is
accepted. The actual HTTP integration test now uses this writer before building,
publishing, reopening and searching the generation.

The writer reads one source row at a time, checks the trusted source digest and
unit norms, validates the permutation, and admits caller-owned order plus
bitset/buffers before allocating. Keep the source immutable throughout both
passes. Output uses create-new and sync; failed writes may leave an unpublished
partial body, which must be discarded. Only a successful receipt authorizes
further construction. Runtime/allocator and OS page cache are outside the payload
budget. Source ordering is supplied or fitted separately. Signed-i64 application IDs
use the explicit-order API below; complete nomination and lifecycle qualification
remain open.

Code rounding uses f32 ties-to-even; reconstructed squared norms accumulate
sequential f32. The independent NumPy fixture covers negative minima, rounding
ties and constant coordinates, but does not establish complete dataset byte or
recall parity. Existing paired100k benchmark bodies and qualification remain
immutable; this new writer is not yet the qualified production creation default.

The source writer now passes exact code/ID/calibration parity and the unchanged
paired100k offline validation gates on the approved frozen order. See
[native SQ8 decision](../research/native-sq8-paired-parity-decision.md).
Run the public API example with sealed normalized source and LE u64 order:

```sh
cargo run --locked -p borsuk --example build_sq8_source -- \
  SOURCE SOURCE_SHA DIMENSIONS ORDER_LE_U64 ORDER_SHA MAX_PAYLOAD_BYTES NEW_SQ8
```

It emits calibration/body identity JSON after successful sync; callers feed
those values to the generation builder. This supplies an executable native SQ8
creation path. Native ordering/normalization and lifecycle/package gates remain
open; the paired replay does not prove a new cloud root or service performance.

## Native source normalization

`borsuk::sq8_source::normalize_source(raw, raw_sha, rows, dimensions, output,
max_payload_bytes)` accepts immutable raw LE f32 source, streams one row at a
time through the same f64 cosine normalization used by query planning, and
returns the normalized body SHA. Unit vectors within squared-norm tolerance1e-6
keep their original bytes. Zero/nonfinite inputs fail. Admission is
`16*dimensions +135168` payload bytes, independent of row count; runtime and
OS cache require separate accounting. Source shape/hash are checked before a
synced temporary file is installed without overwrite. On error discard any
unpublished output; an installation/parent-sync failure can leave output present.

```sh
cargo run --locked -p borsuk --example build_sq8_source -- normalize \
  RAW RAW_SHA ROWS DIMENSIONS MAX_PAYLOAD_BYTES NEW_NORMALIZED_SOURCE
```

Pass that output and SHA to the SQ8 writer with the approved physical order.
The source/SQ8 builder and signed-HTTP fixture now start from non-unit raw rows
and normalize/encode/build/publish/reopen/search through public Rust APIs.
Native semantic ordering is still missing. Its requested dual review was held
by Devbox's six-hour cooldown; no override or duplicate consultation was started.
General raw normalization follows the shared Rust f64 policy and is not claimed
to be byte-identical to the historical Python f32 preprocessing.

The two source tests and four HTTP range tests pass on Spark. The extra
`two_bit_generation::tests` filter matched zero and contributes no evidence.
The runnable normalization example preserves all approved normalized f32 bytes
(SHA/307,200,000B) on both first100k D768 cohorts, independently rehashed after
terminal. Existing paired quality evidence therefore remains applicable to
those unchanged source bytes. See native-normalized-source-receipt.json.

## Mutation visibility before top-k

`generation.search_excluding(&reader, &query, k, &excluded_ids).await?` hides
sorted unique signed-i64 logical IDs before selection, for deletes or replacements.
The roster is borrowed from an immutable snapshot; callers authorize it and bind
it to this generation. Include the entire snapshot payload in
`TwoBitGenerationLimits::already_pinned_bytes` at open; the helper rejects a
roster larger than that charge. Allocator/runtime overhead and caller allocations
outside declared snapshots remain separately budgeted.

All fetched rows still undergo page and value/duplicate-ID checks, including
excluded rows. Planning, conditional GETs, byte caps and failure accounting stay
on the same path. No full-base ID roster or vector plane is hydrated. Results
contain **up to** k visible rows; an empty result after masking all fetched rows
is valid. This helper does not perform extra reads to fill k. Report actual
result counts in quality measurements. The existing `search` remains strict
about producing k fetched candidates.

This is a query-side primitive. Durable snapshot publication/recovery, upsert
merging and compaction are not implemented by this method and remain release
requirements. The caller must not infer mutation durability from a borrowed ID
roster. No benchmark of the historical v1 route qualifies this code revision.

## Durable pending mutations

Use `two_bit_mutations::{read_two_bit_mutations, apply_two_bit_mutations}` with
an authenticated `TwoBitHead`, its `dimensions()`, and `TwoBitMutationLimits`.
Recover `Option<TwoBitMutationSnapshot>`, then apply a sorted unique batch of
`TwoBitMutation { id, vector }`: `None` deletes, `Some(vector)` upserts a finite
nonzero vector and uses the existing cosine normalization. The complete latest
pending state is persisted under that immutable base root. Deleted IDs remain
as tombstones until a future compaction incorporates them; they are not dropped.

```rust,ignore
let recovered = read_two_bit_mutations(store, &head, head.dimensions(), caps).await?;
let updated = apply_two_bit_mutations(
    store, &head, head.dimensions(), recovered.as_ref(), &sorted_updates, caps,
).await?;
// On restart, recover with the same authenticated base head and caps.
// Charge updated.resident_payload_bytes() alongside other retained pins.
```

Snapshots use binary `BTMUT001`: root SHA, dimensions, revision, row count, then
signed ID, operation and normalized FP32 put coordinates. Objects are stored at
`generations/<base-root>/mutations/<snapshot-sha>` with conditional create;
`mutation-head.json` uses `borsuk-two-bit-mutation-head-v2`, with required
`sealed` flag, and is conditionally created/updated last. Head v1 is rejected. An opaque recovered
CAS token serializes writers. A stale CAS error requires rereading/reapplying the
batch; a committed write with a lost acknowledgement is accepted only after
exact authenticated revision/digest readback. Store ACLs authorize writers.
Hashes reject altered bytes and malformed or wrong-base/dimension snapshots.

`max_snapshot_bytes` bounds serialized latest state. The conservative helper
payload model charges twelve serialized-body caps, eight times input row/vector
capacity and path payload, and 4096 metadata bytes. It includes simultaneous
old/new state and transport body copies; concurrent publishers, separately
retained snapshots and allocator/runtime/transport overhead need additional
process admission. Cap failure leaves the existing head intact. There is no
unbounded log or full base hydration. Each batch rewrites the bounded delta;
this write amplification must be measured in lifecycle cost.

`search_with_mutations` below merges recovered puts into query results. This
persistence primitive itself does not score queries or compact/GC snapshots.
Using exclusion alone still hides replacements without returning their new rows.
Keep writes pinned to their base: changing the index head without incorporating
its mutations can lose visibility. The publication fence below closes writers,
but replacement corpus construction, compaction and safe old-object reclamation
remain release gates.

## Fence mutations before generation replacement

```rust,ignore
let latest = read_two_bit_mutations(store, &head, head.dimensions(), caps).await?;
let sealed = borsuk::two_bit_mutations::seal_two_bit_mutations(
    store, &head, latest.as_ref(), caps,
).await?;
assert!(sealed.is_sealed());
// Stream canonical base + these sealed latest states into a new generation.
// Only then publish with expected=Some(&head); never discard sealed rows.
```

Sealing is irreversible. It CAS-publishes a readable snapshot, including an empty
snapshot for an absent mutation head. Stale writers fail CAS; writes using an
observed sealed token reject before staging. If a writer wins the CAS first,
sealing fails: reread and retry with its complete latest state. A seal with a lost
acknowledgement requires an authenticated **sealed** revision/SHA readback.
Sealing an already-sealed recovered token is idempotent after readback.

A crash between seal and replacement leaves old-generation queries working but
pauses writes; resume from the recovered sealed state. There is no unseal API.
Low-level generation publication requires a sealed old head before uploading
objects, but cannot prove that prepared target rows incorporate that state.
The full library compactor must perform that merge and recovery workflow.
Neither this fence nor its functional tests qualify full compaction, GC,
cloud latency, scale or vendor superiority.

## Search a recovered mutation snapshot

```rust,ignore
let snapshot = read_two_bit_mutations(store, &head, head.dimensions(), caps)
    .await?.expect("published mutation state");
// Reopen with every retained snapshot payload charged in already_pinned_bytes.
let result = generation.search_with_mutations(&reader, &query, k, &snapshot).await?;
```

The method checks the authenticated base-root/dimension binding and retained
snapshot charge before any GET. One query semaphore permit covers planning,
conditional SQ8 retrieval, pre-top-k exclusion and pending-put scoring/merge.
Deletes never reappear from base pages; replacements use their pending vectors;
new IDs can be returned even when k exceeds base N. The method returns up to k
visible hits, with actual count exposed. It does not fetch extra pages to fill k.

`TwoBitMutationSearchResult` contains logical ID/score hits, the base page plan,
base-query GET/verified-byte/failure stats, delta rows visited/puts scored and the
pinned mutation revision/digest. Pending vectors have no base physical ordinal.
Earlier snapshot recovery I/O must be counted separately in lifecycle/cold-start
cost; these query stats do not claim it was free.

Pending normalized FP32 vectors are scored in normalized squared-L2 units;
base scores remain the existing SQ8 approximation. A bounded std BinaryHeap
selects score/ID order. Extra heap/output payload `(32 * capacity + 4096)` is
charged for every configured active query on top of the loader's complete
payload model. Capacity is bounded by potential fetched base rows plus pending
puts, not a caller's arbitrarily large k. Runtime/allocator/transport overhead
and result buffers retained beyond completed queries require separate admission.

Focused local HTTP mutation/recovery checks prove functional visibility and
one GET per fixture query. They do not measure ANN quality, AWS latency/QPS,
100M RSS, costs or vendor superiority. Generation v2 now owns a durable canonical source binding. Compaction still needs
an atomic base/delta handoff and GC; decoded SQ8 is not its canonical source.

## Canonical source for maintenance

Every v2 build streams `canonical.bin`: physical-order signed-i64 ID followed by
`dimensions` normalized little-endian FP32 coordinates per row. The descriptor
binds rows/dimensions, length, SHA and a content-addressed key in the approved SQ8
object namespace. Source-plane permutation/ID checks precede construction;
canonical construction rechecks the complete SQ8 and raw input digests. Inputs
must remain immutable throughout. The shared normalization preserves already-unit
FP32 rows, avoiding repeated SQ8 reconstruction/quantization during maintenance.

Publication uploads this file through the existing streamed length/hash-checked
multipart uploader before committing the head. `open`, `open_remote`, `plan` and
all search methods **do not fetch or hydrate canonical rows**. The eight query
metadata objects are unchanged. Source storage/build upload is lifecycle cost,
not free cache or query I/O; it adds `(8 + 4D) * N` durable bytes before replication,
retired generations, mutation snapshots and other index artifacts.

Explicit maintenance recovery:

```rust,ignore
let stats = borsuk::canonical_source::recover_two_bit_source(
    store, &head, new_local_source_path, max_source_disk_bytes, max_source_chunk_bytes,
).await?;
```

It authenticates the pinned root, checks descriptor/disk admission, then streams
one source GET to owned temporary disk. Length/SHA must pass before no-clobber
rename. Existing output rejects; authentication/cap/transport failures do not
expose a partial source. The helper holds a bounded source chunk plus root metadata
(up to128KiB payload), not the full source in RAM. Caller transport buffers and
oversized chunks are outside that payload model; oversized delivered chunks fail
rather than silently exceeding the declared source buffer. Query cache budgets
and maintenance concurrency/disk budgets must be admitted separately.

Success and `CanonicalRecoveryFailure` retain submitted SDK GET calls and delivered
root/source bytes. SDK retry attempts and wire bytes are not inferred from these
counters; configure/measure transport separately. V1 generations now reject,
with no migration/legacy reader. Historical v1 benchmark artifacts are unchanged
and do not qualify this revision. Canonical storage and maintenance cost, atomic
compaction/recovery/GC and matched wins over both vendors remain release gates.


## Prepare bounded compaction input

Use `canonical_source::prepare_two_bit_compaction(store, &head, &sealed, output, caps)`
with `TwoBitCompactionLimits`. It requires a sealed snapshot bound to the same
root, dimensions and index namespace. It streams authenticated canonical base
rows, suppresses IDs with any sealed mutation, and appends only sealed puts.
Survivors retain original physical order; appended puts use signed-ID order.
It returns `TwoBitCompactionSource` and installs `source.f32`, `ids.i64`, then
`manifest.json` last in a new directory. Digests and base/mutation identity are
recorded. This output is maintenance input, never a serving generation.

No base vector or ID plane is held in memory. Payload admission charges twice
the retained snapshot,262144 fixed bytes, delivered source chunk cap and three
row widths; allocator/runtime/transport and other concurrent tasks are separate
caller charges. Pending rows are copied into an in-process blocking worker,
so disk streaming does not occupy a query executor thread. Caller owns
maintenance task concurrency. Dropping the async future does not cancel an
already-running blocking worker; retain/await the operation and its maintenance
admission until completion. An abandoned operation requires receipt/file
verification before reuse; it never updates a serving head. Disk admission reserves canonical input plus the
maximum unfiltered merged raw/ID output and64KiB metadata before source GET.
Source-response and failed-call charges remain available on errors. These are
SDK submissions/delivered bytes, not hidden retries or wire accounting.

Local canonical bytes are rehashed while consumed; nonfinite/nonunit coordinates
reject. The authorized original builder validated base ID uniqueness; the
existing `build_sq8_source_with_ids` checks merged IDs again. Use the raw/ID
files with those existing native SQ8 and generation builders, then publish
against the sealed original head. Generation replacement does not independently
prove that an arbitrary prepared corpus includes the delta. A complete library
compactor must own that orchestration and failure/reload path.

An absent completion manifest means unpublished scratch. A failure after a
manifest rename or directory sync has an uncertain durability outcome; reverify
receipt/file hashes before reuse. An all-deleted source returns zero rows and
empty files; the current generation builder rejects zero rows, so use the typed empty publisher and logical index API below for empty serving.
Complete crash-resumable compaction/GC remains OPEN. No sentinel rows or
silently dropped acknowledged mutations.


## One logical index API, including empty bases

`two_bit_index::TwoBitIndex::open_remote(store, head, limits, scratch_parent)`
consumes an authenticated `TwoBitHead`. Populated bases use the existing bounded
metadata loader; empty bases authenticate one small root and load no plane,
graph, SQ8 or source-vector object. `index.head()` supplies the pinned identity
for mutation recovery/publication.

```rust,ignore
let head = read_two_bit_head(store, &prefix).await?.expect("published index");
let snapshot = read_two_bit_mutations(store, &head, head.dimensions(), mutation_caps).await?;
let snapshot_bytes = snapshot.as_ref().map_or(0, |m| m.resident_payload_bytes() as u64);
let limits = TwoBitGenerationLimits {
    already_pinned_bytes: other_pins + snapshot_bytes, ..limits
};
let index = borsuk::two_bit_index::TwoBitIndex::open_remote(
    store, head, limits, scratch_parent,
).await?;
let result = index.search(&reader, &query, k, snapshot.as_ref()).await?;
```

Search returns up to k signed logical IDs and physical base-query statistics.
`None` explicitly selects the immutable base without a delta; callers must
recover/pass the desired snapshot for incremental visibility. This is a pinned
snapshot API, not an implicit latest-read transaction. Without a snapshot,
revision is0 and mutation digest is empty. All snapshots must bind the index
namespace, root and dimensions and be charged in `already_pinned_bytes`.
The shared query slot spans base planning/retrieval and pending scoring.
Additional logical top-k conversion/merge payload is admitted for every active
query; oversized k clamps to possible candidates. Empty base queries score
only pending puts and issue **zero base GETs**. Metadata recovery/open calls
remain lifecycle I/O. Invalid query, namespace, pin charge or memory admission
fails before retrieval. SQ8 base scores remain approximate; pending FP32 puts
use normalized squared-L2. This API does not establish full-corpus ANN quality.

`publish_empty_two_bit_generation(store, prefix, dimensions, generation, expected)`
creates `borsuk-two-bit-empty-generation-v1`: only schema, generation and positive
dimensions. No sentinel rows or empty graph files are stored. Same-dimension
replacement must increase generation and requires the old mutation seal.
Root create is conditional; index head CAS remains last with authenticated
lost-ack readback. As with the low-level populated publisher, the caller must
prove the retired corpus is all deleted. Canonical recovery/preparation from
an empty root authenticates one root GET and emits no source-object GET; new
pending puts supply the replacement source. Nonempty format v2 is unchanged.

This resolves empty **serving/publication**, including resurrection by pending
puts. Native empty->nonempty rebuild uses the existing SQ8 and generation
builders. Automatic/callable crash-resumable compaction and reader-safe GC remain
OPEN. No vendor/100M/latency claim follows from functional small-corpus checks.
