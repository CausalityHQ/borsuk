# Cold object-store read latency design

**Status:** Accepted cold-first direction on 2026-08-27; implementation and
qualification pending.

## Objective

Make a truly cold BORSUK query competitive with first-party object-store vector
search while preserving the frozen ranking methodology and bounded-memory
serving contract. A cold observation opens a fresh index handle for one query,
does not reuse a decoded or code-plane cache populated by another query, and
reports every query-scoped backing request and byte.

The release gate for `deep-image-96` is:

- recall@10 at least 97.41%;
- p50 latency at most 160 ms and p99 latency below 200 ms;
- at most 30 backing GETs and 32 MiB read per query on average;
- no swap, OOM, unbounded task, detached request, or unaccounted retained data.

Third-party superiority is not a claim gate until the same frozen query set and
equivalent disclosed conditions have run through real S3 Vectors and
Turbopuffer adapters.

## Evidence and diagnosis

Source `59e67790e4bd0a8d81f56e54fec038a30474cd34` corrected the cache-state
methodology. Warm preparation explicitly loads complete immutable code planes;
cold preparation leaves them paged and opens a fresh handle for every measured
query. The repaired warm campaign retained 97.41% recall@10 at a median p50 of
16.576 ms and a p99 near 22.5 ms.

The first honest cold repetition retained the same recall but measured 228.573
ms p50, 333.354 ms p99, 58.642 GETs, and 28.481 MB (27.162 MiB) per query. Its two dominant
serial stages were approximately 46.9 code-head requests followed by 11.7
exact-vector requests. Head fetch consumed about 117 ms/query and exact fetch
about 88 ms/query; decode and scoring CPU were secondary.

The second independent cold repetition retained exactly the same 97.41% recall,
58.642 GETs/query, and 27.162 MiB/query while measuring 204.985 ms p50 and
290.203 ms p99. Its head and exact fetch walls were about 103.0 and 77.6
ms/query. The stable request/byte identity across hosts makes scattered backing
I/O the reproduced architectural bottleneck rather than a one-host CPU or
network anomaly.

A third Spot repetition again retained 97.41% recall, exactly 58.642 GETs/query,
and 27.162 MiB/query. It measured 214.763 ms p50 and 303.110 ms p99, with head
and exact fetch walls of about 104.5 and 84.6 ms/query. The subsequent R04
launch was rejected for unavailable matching Spot capacity after its bounded
retries, so it produced no measurement and R05 was not started in that serial
loop. The three terminal repetitions establish request fan-out as the stable
cause while preserving the infrastructure failure separately.

An older 54.316 ms result is not an architectural counterexample. That runner
reused one handle across all queries, so query-populated retained state reduced
the whole run to 9.345 GETs/query. It is retained as historical evidence but is
not an honest cold observation.

The current immutable root contains 80,202 cell cards in 215 group objects,
about 373 cards/group. Each group code plane is about 6.3 MiB. The quantizer
does renumber parent and child centroids for locality, but the current
high-dimensional Morton/Z-order key degenerates into coarse octant bucketing
after SRHT rotation. The selected 416-card head can therefore touch about 47
groups even though a good packing approaches two groups. In addition, a fresh
cold handle currently owns a retained pool; the planner can promote selected
ranges into complete multi-megabyte planes even though that handle is discarded
after one query. Bytes are already inside the 32 MiB gate; request count and
tail service time are the binding constraints.

## Decision

Deliver the repair in two independently measurable layers.

### 1. Cacheless cold execution and exact I/O telemetry

Add an explicit public code-plane retention budget to `OpenOptions`. Zero means
that complete-plane retention and cache-motivated promotion are disabled while
request-local single-flight and authenticated selected-range decoding remain.
The publication cold profile sets this budget to zero; warm uses the bounded
serving budget. Ranking breadth and exact rerank selection remain independent
of cache state, so warm and cold must return identical IDs and recall.

Record bounded per-query distributions for physical head and exact reads:
request count, bytes, service duration, and phase wall time. Queue delay is
derived from task admission timestamps and must be reported separately from
object-store service time. Each physical read returns its own counters to the
query; shared-handle counter deltas are not valid under concurrency. Telemetry
is fixed-size aggregate evidence, not an unbounded vector retained by the
library.

Run a claim-ineligible diagnostic on the existing immutable index with leaf
widths 32 and 64 using a preregistered tuning subset disjoint from the 1,000
publication queries. Select 64 only if it improves p99 without exceeding the
process backing-GET cap or memory gate, and disclose that selection. Width is a
scheduling parameter, not a substitute for reducing requests.

### 2. Locality-packed immutable cell-card groups

Replace the current high-dimensional Morton centroid order with deterministic
nearest-neighbour-chain renumbering where the parent and per-parent child
codebooks are fitted. Each level has at most 256 centroids, so the bounded
all-pairs distance work is at most 256 squared per level and is independent of
corpus rows. Stable centroid-value and prior-ordinal tie breaks make output
bytes deterministic.

This is renumbering, not a read-side permutation: cell IDs remain canonical in
the new physical order, the root remains strictly sorted by `(cell_index,
card_ordinal)`, and its binary-search lookup is unchanged. The codebook layout
marker and checksum authenticate the new order; pre-release readers reject the
old experimental format. Existing amplification, request, byte, and
transient-memory gates remain authoritative. The 65,536-cell format ceiling is
an explicit scaling bound (about 1,526 rows/cell at 100M rows).

The layout must demonstrate, on the frozen query set, that geometrically nearby
routed cells co-occur physically and that the selected-range planner averages
at most 30 total backing GETs/query. A new format/version marker rejects prior
experimental layouts; no compatibility reader or migration layer is retained
before the first release.

### 3. Bounded tail hedge qualification

Request collapse is the first p50 and cost lever, but the p99 gate also needs a
tail lever. Wire the existing optional range-hedge primitive into V20 head and
exact physical reads with a public bounded delay. Every primary and duplicate
attempt consumes the same global backing-GET permit and is charged to the
owning query's attempt/response-byte telemetry. Winner completion cancels or
drains the loser inside the scoped operation; no detached request, permit, or
unreported byte may outlive the query. Primary-error/hedge-success and the
inverse have deterministic error precedence.

Run the already registered control/75/35/20 ms hedge arms only after request
collapse. Select on the same disjoint tuning split, disclose the selected delay,
and rerun the untouched publication queries. If no arm reduces p99 while
respecting the GET/byte/cost gates, ship no hedge and treat the p99 target as
unmet rather than lowering it.

## Verification

The implementation requires RED-to-GREEN tests that prove:

1. a zero code-plane budget never promotes or retains a complete plane;
2. warm and cold plans select the same cards, exact blocks, IDs, and recall;
3. head/exact service and queue telemetry reconcile with storage counters and
   remains bounded on success, error, and cancellation;
4. the packing map is deterministic, bijective, checksum-bound, and rejects
   malformed authority;
5. shuffled neighbouring centroids are renumbered into contiguous cells without exceeding the 2x amplification,
   4 MiB range, request, byte, or transient-memory caps;
6. a format round trip preserves logical identity and search results;
7. focused tests, strict Clippy, formatting, repository assurance, and a
   read-only cross-provider review are green.
8. hedge winner/loser/error/cancellation paths account every attempt and release
   every I/O and memory permit before query return.

After source freeze, build a new immutable index and run five serial cold
repetitions on Spot. Publish only complete terminal artifacts and aggregate the
predeclared recall, latency, request, byte, memory, and resource-health gates.
