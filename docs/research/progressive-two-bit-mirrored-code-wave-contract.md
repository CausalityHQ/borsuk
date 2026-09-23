# Progressive two-bit mirrored page-wave contract

Status: 1M source-only projection passed; paired scorer cell remains pending.
This is one architecture decision after
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
the mirrored cover. Rejoin the two-bit records bit for bit, then compare
their frozen float32 batched-matrix scores against exact source scores on
the same rows. The batched matrix reduction can round differently from
the historical per-row scorer; this paired cell measures that arithmetic.
Report the historical 98,920
GT100/p05 94 full-group exact-source diagnostic separately. The final
numerical gate is GT100 ≥98,151, GT10 ≥9,928, p05 GT100 ≥90, at most
49 sub-90 queries, and ≤32 GETs/16 MiB for each code and data wave. A pass
also requires net paired GT100 loss from the same-cover exact-source arm
of at most 300 positions; gross lost and recovered positions are reported.
A pass still requires authenticated actual S3 reads and an untouched query cohort
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
The paired cell's independent validator checks all rejoined page payloads,
code-wave/data-wave geometry and truth masks; it does not independently
recompute every floating-point score priority.

## Closed 1M projection decision

The sole attempt used pushed source
`7a06b24b0b5790406ba558d2c0f097493f401de1`, archived in
`s3://borsuk-bench-453182569524-euc1/research/native-one-million-progressive_code_wave-selector/7a06b24b0b5790406ba558d2c0f097493f401de1/source/source.tar.gz`
(11,783,491 bytes, SHA-256
`abfb90075599cd6c2ef944fcae8be9d4b11bb461f738847ae6f73983ea8f25e2`).
The create-only attempt prefix ends in
`runs/relaion-1m-dev1000-a0001/`; its complete terminal SHA-256 is
`a97a9f2deb402d1b7a016906028849e1ee075f6b295b77f389c684d18784138a`.
The original controller exited zero after 798 seconds and read back all 13
terminal artifacts. Separate local receipt closeout authenticated the
reservation, exact source archive, all artifacts, prior frozen plans/page
map and zero-swap resource files. The Spot worker independently replayed
all 1,000 104-byte admissions, 96-byte mirrors and ordered truth masks.
Causality Spot `i-0405f60b5582283ce` (`c7i.8xlarge`) was confirmed
terminated. No local all-query replay was run under host swap pressure.

| Frozen metric | Result | Advance rule |
|---|---:|---:|
| GT100 owners in mirrored cover / 100,000 | **98,986** | ≥98,651 |
| GT10 owners in mirrored cover / 10,000 | 9,956 | reported |
| p05 GT100 | **95** | ≥93 |
| queries below 90 GT100 | 15 | reported |
| maximum sign GETs / bytes | 32 / 16,777,176 | ≤32 / ≤16,777,216 |
| maximum magnitude GETs / bytes | 32 / 15,486,624 | ≤32 / ≤16,777,216 |

The sign and magnitude covers are identical. The 104-byte first plane
recovers 518 more GT100 positions and four p05 points than the rejected
200-byte page-local code wave. It also includes one bridged truth owner
beyond the historical 98,985 selected-group containment. All 1,000 sign
plans use 32 GETs, leaving only 40 bytes under the maximum observed
sign-wave cap. Construct, plan, evaluate and validate maximum RSS were
968,724, 1,034,608, 1,723,160 and 1,720,040 KiB; all four phases
reported zero swaps under the remote 3-GiB cap.

**Decision:** `advance-to-paired-score-cell` under the frozen rule. The
byte cover and truth-owner screen pass. Neither two-bit final recall nor
S3 read latency is established. The two mirrored code planes together
have a per-query upper bound of about 30.8 MiB and 64 GETs from the
separately observed wave maxima,
before the final data wave; request cost and tail latency need actual-read
qualification. Build the split plane artifacts once from source, check
bit-exact rejoining, score only the sealed mirrored cover, and compare
against exact source scores on those same rows with the fixed final gate.
