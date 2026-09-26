# V254 CoHere 100k: bounded coarse PQ candidate routing

**Decision question:** Can source-only coarse lists preserve V253's high
quality while scoring a bounded fraction of its 100,000 PQ codes? V253
global PQ64 cosine top8192 plus FP16 returned 99,922/100,000 exact GT100
hits, loaded p95 19.157 ms and 8,192 FP16 rows, but scanned all100,000
PQ codes per query. V251's graph needed 14,508 FP16 base scores at p95
to pass quality; V252 entry routing did not repair its smaller beam.

Train a deterministic spherical Lloyd coarse quantizer on the same
authenticated source F32 rows only: K=ceil(N/256)=391 for N=100,000,
fixed seed 254 and six iterations. Each source row appears in its two
nearest centroid lists; no query or truth data enters training or
assignment. The new versioned artifacts are `centroids.f32`,
`offsets.u32`, `postings.u32` and `coarse.json`, with source/PQ code
digests, geometry and artifact SHA-256. Reject invalid offsets, ordinals,
duplicate entries or a list longer than eight times the mean. Keep the
V248 FP16 plane, PQ books/codes, map, queries and truth byte-identical;
the V250 graph is only a paired-source guard, not queried.

Freeze one serving arm: score all391 centroids by cosine, probe the best32
lists, deduplicate source ordinals, score their PQ64 reconstructions,
keep the best8,192 by PQ cosine and row-ordinal tie break, then rerank
those8,192 by authenticated FP16 for k100. Fail closed if fewer than
8,192 unique candidates appear. Seal returned IDs before scoring.
Development is query ordinals0–255 and validation256–999, both already
used. Require each split mean R@100≥0.995 and p05 GT100 hits≥98;
selected-arm p95 PQ rows scored≤20,000, exactly8,192 FP16 rows,
eight-worker loaded p95<19.157 ms, peak serving RSS≤256 MiB and zero
vector-body GETs. Record all raw hits, p50/p90/p95/p99, QPS, list sizes,
PQ work, RSS and build/preparation resources. Timings are in-process,
not service or vendor latency. A quality pass without bounded PQ work
or latency is not a scale promotion.

This one-level route is a 100k falsifier. At larger N, K rises with N;
centroid scoring needs a hierarchy or compact centroid index before
100M. The two assignments cap posting storage at 2N ordinals, a
conditional deterministic memory bound. They do not prove recall;
only the frozen exact-truth gate can. If V254 fails, do not tune K,
copies or probes on this used panel; choose a materially different
coarse-index or PQ-code format before another gate. On a pass, promote
to a frozen 1M end-to-end serving/recall/resource comparison under the
same rows-per-cell, two assignments and32 probes, with a separate
scale decision before10M.

One `causality` c7i.4xlarge Spot attempt, immutable reservation/source
archive, narrow tests before measurement, closed terminal with full
artifact SHA-256 readback, interruption discard and immediate termination.
