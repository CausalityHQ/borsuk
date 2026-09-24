# Memory, recall and scale qualification policy

Status: research policy, 2026-09-23. No production memory default is frozen.
The former 3-GiB research target is not a universal limit, and there is no
vector-count threshold at which BORSUK silently changes quality tier. A
100M-vector collection may need substantially more RAM or local SSD than a
1M-vector collection at the same target quality.

## Decision variables

Qualify a candidate at `(N, D, R, C, G, L)`: vector count `N`, dimension
`D`, requested returned Recall@100 floor `R`, concurrent queries `C`,
simultaneously pinned generations `G`, and a live latency objective `L`.
Record RAM charged to the serving cgroup, local SSD occupancy and I/O,
remote bytes/GETs, throughput, p50/p95/p99 latency and instance cost at
that point. The selected tier is the least-cost measured configuration
that clears **all** quality, tail, cap, recovery and observability gates.
The system must expose the qualified capacity and reject an unsupported
request rather than lower recall or change tier based only on `N`.

Per-generation plane arithmetic is an input to sizing, never a substitute
for a charged-memory measurement. For an `b(D)`-byte row plane, its payload
over pinned generations is `sum_g N_g × b(D)`. Add codebooks, ID/layout
maps, allocation overhead, per-query scratch and any page cache measured
at concurrency `C`. The existing SQ8 record is `D + 12` bytes per row;
at 100M rows and two full pinned generations this is 156 billion payload
bytes for `D=768`, or 21.6 billion for `D=96`. The rejected V113 plane
would have been 3.2 billion payload bytes for two 100M generations at 16
bytes per row, but it failed the 100k quality screen. None of these
figures is a measured 100M RAM peak or a price quote.

RAM and local SSD are separate placements for authenticated local score
data. A disk-backed exact scorer may have a smaller resident set but still
needs measured random-read latency, page-cache behavior and startup time.
An exact resident tier may use much more memory while keeping one remote
data wave. A compressed tier can be selected only after its returned
quality is proven empirically across the frozen gates. Availability of
additional memory at 100M is a capacity choice, not evidence that the
current 100M nomination algorithm will meet latency or QPS.

## Generalization and proof obligations

Corpus-only fitting may adapt codebooks and physical layout to a new
collection. Query and ground-truth rows cannot set code widths, vote
factors, thresholds, or a special branch for ReLAION or deep-image. Freeze
the method and one acceptance rule before untouched splits and the other
embedding family. Use matched same-run controls and the same physical
GET/byte caps. Historical measurements stay tied to their own source and
format and cannot be treated as a paired baseline after an architecture
change.

Lean can check deterministic implications: authenticated score-error
bounds plus a separating top-100 margin preserve the primary set and, for
a fixed route, returned hits; authenticated GET/page counts imply byte
caps; an exact score representation must distinguish SQ8 codes in the
checked basis-query model. A useful certificate also needs a verified
connection from production bytes and floating-point operations to those
premises. Neither Lean nor a source-only model establishes unseen-query
recall, S3 tail latency, charged RAM peaks or prices without workload and
service assumptions. Those remain release measurements.

`formal/FlatRouterScaling.lean` additionally checks the current flat
router's symbolic payload and a conditional latency bound: if every page
summary coordinate is evaluated and a machine can perform at most `F`
coordinate operations per time unit, any budget `L` with `F × L` below the
required coordinate count is impossible for that scan. At 100M rows,
256-row pages and two summaries/page, the proof computes a 2.4-billion-byte
summary plane for D=768, plus 6.4 billion bytes of PQ64 codes, per
generation. Two pinned generations require 17.6 billion payload bytes for
those router planes before overhead. If exact SQ8 is also resident, add
156 billion bytes for two D=768 generations. These are arithmetic bounds,
not observed memory or latency. V116's 19.83 seconds for 1,000 Rust
nomination queries is an offline batch measurement at 1M; it does not split
summary and code-scan time or project linearly to 100M.

A bounded graph walk is a research alternative to the flat scans, with
unproven recall and construction cost. First compare it with the current
flat router in a paired 100k source-frozen Spot gate, then promote only if
quality and compute evidence support it across corpora. An oracle selecting
the best 84 independent D=768 pages under 16 MiB would be only a relaxed
upper bound: it ignores the 32 contiguous-range constraint. An actual
feasible oracle must obey both constraints and is a diagnostic, never a
query-time routing rule.

## Promotion sequence

1. Use a source-frozen 100k gate to reject a representation or routing
   failure cheaply. V113's ReLAION-100k development screen rejected the
   16-byte residual plane: mean/p05 SQ8-primary overlap 74.037/62 versus
   the preregistered 95/90 gate. Do not rescue it through an unregistered
   width sweep or dataset-specific threshold.
2. Promote a corrected method to paired ReLAION-1M development routing;
   record returned GT100 hits, lower-tail hits, GETs, physical bytes and
   cap violations against same-run V109-style and V112-style controls.
   Reproduce the reader and artifacts from one frozen source revision.
3. Freeze acceptance before untouched ReLAION-1M validation and a
   deep-image-96-angular cross-corpus campaign with newly built,
   corpus-only indexes. Apply the same method and acceptance rules.
4. Measure live S3 latency/QPS and the full resource frontier, including
   cold start, mutation, compaction and `G ≥ 2` generation rollover. Scale
   the qualified revision to 10M and 100M; a change in routing, placement
   or representation starts a new source-bound comparison.

The release default is selected only after these gates. A configuration
that passes one development dataset is a research point, not the generic
production method.
