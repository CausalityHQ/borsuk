# Actual shared core: BOTH-corpus object-native HTTP development GO

The exact compiled shared Rust planner, ordinary native object reads and incoming
HTTP all pass the fixed development gates on BOTH corpora. This permits fresh
qualification. It does not establish superiority over either vendor: candidate
matched p95 is slower and serial QPS is lower on both panels.

Both panels: first100k indexed rows, D768 cosine k100, explicitly consumed
external development query ordinals0–63. Every number below is independently
verified from closed a0002 artifacts. Control is the unchanged flat source/old
planner and HTTP binary measured in THIS cell; exhaustive is the authenticated
immutable per-ID SQ8 score baseline, not a serving baseline.

| Dataset/split | Matched control recall@100 | Actual core recall@100 | Delta | Exhaustive SQ8 recall | Control → candidate p90 / p95 ms | Control → candidate serial QPS |
|---|---:|---:|---:|---:|---|---|
| relaion/development0–63 | 99.421875% (6363/6400) | 99.468750% (6366/6400) | +3 hits / +0.046875 pp | 99.515625% | 107.275025 / 108.357040 → 112.664031 / 117.705166 | 9.415363 → 9.165581 |
| cohere/development0–63 | 99.171875% (6347/6400) | 99.203125% (6349/6400) | +2 hits / +0.031250 pp | 99.234375% | 106.465087 / 107.969737 → 107.898551 / 111.353057 | 9.504608 → 9.344978 |

Candidate p05 recall@100: ReLAION99%, CoHere98%, same as control. Candidate
p50/p99: ReLAION106.717349/140.351054ms;
CoHere105.839312/122.436530ms. Median of two repetition quantiles per arm;
ABBA64 queries/repetition,512 total HTTP requests. Client prepared request bytes
through complete JSON parse, loopback, one serial query slot, CPU0–3. Resident
router reopened per repetition; client has no SQ8 cache. Native live reference
calls precede HTTP; S3 server cache is uncontrolled. No WAN/TLS, sustained QPS,
concurrent QPS or QPS per total dollar is established.

All exact ordered IDs/GT intersections/ranges/physical counters match native
reference calls and cached scorer. Each query <=32GET/16773120B, zero failed GETs.
Per64-query candidate/control GETs and bytes respectively:
ReLAION1671/1663 and1073105280/1072281600B;
CoHere1969/1934 and1072805760/1072955520B. Each HTTP arm repeats those physical
counters twice; these are actual data GET submissions and verified bytes.
HTTP server peak RSS range across repetitions78860–89348KiB; entire screen
cgroup peak3652743168B includes offline construction/controller/client/cache,
not serving RSS. Eight GiB cgroup, zero swap/OOM.

Candidate GT loss decomposition: ReLAION walk pool0, discovery roster2,
nomination3, physical0, quantization/ranking29. CoHere walk pool0, discovery0,
nomination5, physical0, quantization/ranking46. No flat-present final ranking
loss; candidate remains3/2 hits below exhaustive respectively. The nine CoHere
CONTROL nearest-graph walks naturally end at985/1223 units with false work
exhaustion, all others1272. Verifier initially incorrectly required exactly1272
rather than the frozen maximum; corrected metadata-only assertion verifies
bounded distinct units, exhaustion flag implication and every primary unit,
reports actual work. Native algorithm, data, gates and measurements unchanged.
No extra remote test/build/scoring job was launched for this assertion correction.

Independent proof:112 artifacts,394 native files,402 frozen source/config/scorer
files, compiled actual-core source and2693-pass/zero-fail/26-ignore full assurance
(146 Cargo execution targets) reused. Authenticated source/scorer/control/root
parity, ReLAION standalone-to-core exact64 plans, reused CoHere construction,
all512 HTTP records, authority/boundary/disconnect/cleanup gates and actual EC2
termination verified. Failed a0001 remains INVALID; no measurements from it were
combined with a0002.

Original45583 CLOSED0; i-0834d3cb3e90d0b56 terminated after401s. Compute cost
estimate$.0199 from observed quote/wall, excludes EBS/S3, not invoice/lifecycle.
No active compute retained. Source archive:
`cc71bc011fc06a2dc1e8d7143bc6f9323291902d6324bbe92ccec2a2ff296553`.
Terminal:
`fb32ead222b368a159b1f9f4107a0bf0a0e2c009f8f3526c149b5149de47d124`.

## Next decisive gate and remaining product gap

1. Preserve/push these closed measurements; keep the integrated mechanism and
fixed gates. Do not tune on a new cohort or reopen the novelty-rejected seal.
2. Establish a genuinely unused query identity cohort outside the first1M
indexed rows, with complete historical external/source-pseudoquery identity
inventory BEFORE query/GT/quality access. The previously sealed100000–100999
interval is unsuitable: it overlaps consumed identities and lies inside1M.
3. Freeze a ReLAION-first1M build and incoming object-native HTTP confirmation
with exact source/scorer/control parity, build/resource admission, quality and
physical caps, bounded Spot worker and immediate terminal cleanup. Advance
CoHere only after ReLAION survives. Scientific failure ends the exact arm.
4. Only after fresh1M qualification advance10M/100M, generation swap/pin RSS,
mutation/compaction/GC/recovery and matched BOTH-vendor lifecycle economics.
Current measurements do not cover these. S3 Vectors query/create authority is
unverified; Turbopuffer credential is absent. No operator decision is required
for the already-authorized native prerequisites; vendor access is a later
external dependency. The full production goal remains active.
