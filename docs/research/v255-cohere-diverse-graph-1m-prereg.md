# V255 CoHere first 1M: diverse graph transfer and scale gate

**Decision question:** Does the source-only diverse graph that passed
V250's CoHere-100k exact-quality and loaded-latency gates retain those
properties at 1M without a fixed corpus-size memory cutoff or query
work growing linearly with N? This is a new gate. V250's frozen
10,000-score scale threshold failed at 100k and is not retroactively
changed. V253 global PQ is the strongest 100k quality baseline but
scores every PQ row. V254's two-assignment coarse route failed quality;
its closed-artifact bound diagnostic also failed a 20,000-row target.

Use CoHere-large-10M's canonical **first 1,000,000 source rows**, D768
cosine, k100, test query ordinals 0–999 from the authenticated staging
receipt. Development ordinals 0–255 and remaining validation 256–999
were already used; report both separately. Source-only PQ64 books and
codes, FP16 plane, and the V250 diverse builder method are frozen:
M32, M0 64, construction ef128, eight build workers. Select exactly
one arm before exact truth: graph PQ-cosine navigation ef4096, FP16
rerank shortlist4096. Seal returned IDs before computing exact F32
GT100. No query/truth-dependent training, tuning or retry. If this arm
fails, diagnose the responsible layer and return to a materially
different 100k falsifier; do not sweep ef on these used queries.

Pass requires an authenticated fully reachable graph with minimum
in-degree four and maximum degree≤256; each query split mean R@100≥
0.995 and p05 exact hits≥98. Report combined and split counts,
query-wise score work and exact IDs. Loaded eight-worker in-process
p95≤35 ms, p99≤45 ms, throughput≥300 completed QPS, p95 PQ row
scores≤150,000 (≤15% of this N), peak serving RSS≤4 GiB, and zero
vector-body GETs. Build≤1,800 s and peak builder RSS≤10 GiB; record
separate preparation and exact-truth resource peaks. These bounds
permit memory to scale with N and make the query-work fraction explicit;
they do not prove asymptotic sublinearity or any 10M/100M performance.

If the in-process gate passes, use the same revision and immutable
artifacts for a frozen persisted HTTP first/repeat pass with exact ID
parity, client p50/p90/p95/p99, throughput, bytes/GETs, cache state,
RSS and elapsed compute cost. Compare to the strongest authenticated
ReLAION-1M BORSUK/S3 measurements only as cross-dataset context;
do not claim a matched vendor win. Turbopuffer published cold numbers
remain dated unmatched context until direct access exists. Keep the
10M gate closed until a same-dataset 1M end-to-end result passes.

One `causality` c7i.4xlarge Spot attempt at a time, immutable source
archive and S3 attempt prefix, terminal artifact SHA-256 readback,
interruption discard/restart, and immediate compute termination. Never
inspect incomplete measurement files. Record instance identity, region,
Spot quote, elapsed time and estimated compute cost. No active prior
V25x instance exists at preregistration.
