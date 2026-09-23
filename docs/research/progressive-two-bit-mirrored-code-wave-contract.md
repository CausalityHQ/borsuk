# Progressive two-bit mirrored page-wave contract

Status: preregistered, no 1M result. This is one architecture decision after
the rejected page-local 200-byte code-wave projection. Historical artifacts
and their source commits remain immutable.

## Question and fixed candidate

Can the sealed 1M OPQ8 page priority admit enough source truth owners when
the code-cover planner charges **104 bytes per row**, within 32 GETs and
16,777,216 encoded bytes? The previous 200-byte page-local cover retained
98,468 GT100 positions and p05 91, below its frozen 98,651/p05 93 screen.
The 96-byte sign and PQ score representations failed paired 100k fidelity.

Repack the existing 200-byte rotated two-bit record without retraining:

* Plane A: 96 bytes of packed symbol sign bits, the existing float32 scale,
  and the existing float32 norm, exactly 104 bytes per physical row.
* Plane B: 96 bytes of packed symbol magnitude bits per physical row.
* Each plane has separate page-ordered base and delta objects. Page offsets,
  row counts and SHA-256 digests are sealed in an incompatible generation
  manifest. Each HTTP range must be checked for status 206, Content-Range,
  length and page digest before decoding.

The two planes for one page rejoin the original 200-byte record bit for bit.
The first plane alone is **not** a qualified row scorer. Plan the first
plane from the sealed, source-trained OPQ8 page priority using the frozen
minimum-byte merged-range rule. Mirror exactly those physical page intervals
into the second plane. The second plane has the same GET count and at most
96/104 of the row payload bytes. Its GETs can be issued with the first
plane because its page choice depends only on the OPQ8 route, not on the
first plane's returned scores. Score only rows fetched from **both** planes,
then apply the unchanged top-100-row page priority and 32-GET/16-MiB data
range rule.

This architecture spends two code waves, each independently capped at
32 GETs/16 MiB. Concurrency may reduce elapsed time but does not reduce
request count or transferred bytes. The actual-read qualification must
measure both waves, end-to-end tail latency, throughput and request cost.

## Cheapest decisive gate

Use exactly the terminal-closed 1M source/layout, 1,000 development queries,
OPQ8 plans, page priority and truth-owner authority used by the rejected
200-byte projection. First run a **source-only byte and containment
projection**; do not construct the 1M split code objects yet. Project exact
104-byte page lengths from authenticated page row counts and independently
replay the greedy admission, including pages bridged by physical ranges.
Project the mirrored 96-byte ranges from the same included page intervals.
Seal and upload all query-only plans before opening truth, then count GT100,
GT10, p05, sub-90, target and bridged hits, page counts, GETs and bytes.

The only advance rule is **GT100 containment ≥98,651 and p05 GT100 ≥93**,
with both code waves ≤32 GETs and ≤16,777,216 bytes on every query. This
is a coverage screen, not measured recall or latency. If it fails, stop
this layout without constructing a code plane. No width, priority, merge
rule or threshold may be tuned against this reused development cohort.

If it passes, run one frozen 1M source-only paired scoring cell on precisely
the mirrored cover. Compare bit-exact reconstructed two-bit scores against
exact source scores on the same rows, and report the historical 98,920
GT100/p05 94 full-group exact-source diagnostic separately. The final
numerical gate is GT100 ≥98,151, GT10 ≥9,928, p05 GT100 ≥90, at most
49 sub-90 queries, and ≤32 GETs/16 MiB for each code and data wave. A pass
still requires authenticated actual S3 reads and an untouched query cohort
before any serving claim, then a frozen 10M/100M scale gate.

Use Causality Spot, an exact pushed source archive and one immutable attempt
ordinal. Publish a terminal marker and independently read back and replay
the complete result. Restart an interrupted measurement cell under a new
ordinal; terminate the instance promptly after its terminal marker. Do not
inspect incomplete measurement CSVs.

## Formal and empirical boundary

`formal/SourceRangeFidelity.lean` proves symbolic two-bit sign/magnitude
rejoining, 104+96=200 bytes, per-wave row ceilings and the mirrored
payload inequality. `formal/Opq8Planner.lean` proves a conditional latency
ceiling for concurrent code waves followed by the data wave. A production
claim additionally needs byte-level Python refinement, authenticated
page-cover certificates, checked physical range offsets, measured service
times and held-out recall. None is inferred from the formal arithmetic.
