# V141 D96 returned-quality replay preregistration

**Decision:** can V140's frozen β=4 physical range plan produce at least
99.5% **returned exact-source Recall@100** on the already-used D96 100k
cohort, with acceptable per-query tails, while reading less SQ8 data than
the broad candidate? V140 counted 99.738% GT coverage inside fetched SQ8
ranges. That bounds SQ8-only results, while router nominees outside those
ranges can enter the exact-source union. V141 is
the cheapest quality gate before implementing a production Rust planner or
spending on D768 source replay. It does not measure live S3 latency or a
fresh held-out split.

Use the **deep-image-96-angular random100k train subset**, already-used
**publication-test ordinals 9000–9999**, with the exact V122 source, SQ8,
low/step, queries and GT artifacts authenticated by their historical
SHA-256 values in `scripts/launch_v131_source_replay_spot.py`. Authenticate
V140 a0001's completed terminal (SHA-256
`d9e15698b83c6be6456aeaff2864763b91192c063f0bda5dfca0cdc392a1f3c4`)
and its D96 raw file (SHA-256
`e2a1b31161bb2190cb7dc36d0601c50449c6d6aba0cc1a4985c032d713bc54d5`).
The worker receives the committed GT-free V122 route-only derivative,
SHA-256 `388fe94cfd6c6ac59e7153c9947af5cdb07119e8fe5acabef4d5c1b3ffe422d4`.
It must not download GT until all returned IDs are sealed.

For each query, compare three arms with identical scorer and nominee roster:

1. **β=4**: V140 raw `variants["4"].ranges`.
2. **Broad current BORSUK candidate**: V122 sealed candidate ranges.
3. **Fixed capped control**: V122 sealed baseline ranges.

Score every fetched SQ8 row with the existing
`scripts.v114_1m_paired.score_sq8_ranges` arithmetic, retain the best 512
by score and source ID, union their source IDs with all 512 router nominee
IDs, then rank that union by float64 cosine against the original float32
source vectors. Emit ordered SQ8 top-100 and exact-source top-100 IDs for
all three arms and all 1,000 queries **before** downloading truth. After
the replay is sealed, download V122 GT and reduce only. Require the broad
and fixed-control arms to reproduce V131's same-method totals:
**99,942/98,827 exact-source hits** and **99,106/98,203 SQ8 hits**. If
either baseline differs, the V141 candidate is claim-ineligible.

The β=4 arm passes only if exact-source hits are at least **99,500/100,000**,
p05 exact-source hits per query at least **98/100**, no query below 90,
and aggregate hits are no less than the same-run fixed capped control.
Report per-query paired wins/ties/losses against both baselines, SQ8 hits,
fetched-range physical coverage, candidate-union size, planned GET/bytes,
offline SQ8 and source-scoring time, and worker RSS/charged memory. These
offline times exclude router construction, centroid scoring/selection,
network, startup, concurrency and S3; they cannot support a serving latency
claim. If β=4 fails, revise the page ranking or SQ8 representation rather
than adjusting β on this used split. If it passes, run the analogous D768
returned-quality gate on Spot and implement the β=4 planner in production
Rust before fresh matched-layout/live-S3 qualification.

Run one frozen Causality Spot attempt with an immutable source archive,
5,400-second whole-instance deadline, terminal-only observation while
incomplete, authenticated terminal and raw artifacts, Spot interruption
discard, and termination at the terminal. No overlapping heavy execution.
