# V273 one-shot 100k graph-construction falsifier

Status: preregistered before candidate measurement. V272 failed its 10M
quality gate. A read-only diagnostic over all 83 development queries with a
miss found that 171/179 missed ground-truth rows scored above the returned
FP16 cutoff; graph candidate coverage is the dominant observed loss. A
one-hop search expansion on V271's prior-used 100k panel did not change its
9,994/10,000 hits, so that candidate was discarded without a 10M replay.

## Fixed candidate and inputs

The **only** candidate is stronger source graph construction at fixed degree:
M=32, M0=64, efConstruction=256 (V272: 128). Keep each frozen-snapshot
insertion batch capped at 1,024 rows, independently of efConstruction; use
up to 32 available build workers. PQ, FP16 plane, generation layout, and serving
search widths stay unchanged. This changes graph edges, not query memory or
the number of graph objects. No construction or search parameter sweep.

Corpus: CoHere-large-10M canonical train rows 0–99,999, D768 cosine, from
V261's sealed `vectors.raw` (3,072,000,000 bytes, SHA-256
`6c82a340e3e1b4226640e593efa9c4000c6a5962d4b13063093a1dab689a9005`).
The first 100k rows must reproduce source SHA-256
`d638878523cfdd349cb28e214d59010709d6451f3eeca2a70da88c0c9d9ea753`
in V271's authenticated 100k root
`440beefd321dfeeae25ba6277a2e2f1f938c389a0c4a9007177f4bacc4b1d6e3`.
Fresh excluded queries are rows 100,000–100,999 of the same sealed source;
the fixed raw query slice SHA-256 is
`10322f59ee236849e60137c081432c3a8ef55d6c09dc1585356e984b2bfc30c0`.
These rows were not in the 100k index or V271's held-out query panel.
Truth is FAISS `IndexFlatIP` on FP32 unit-normalized source/query vectors,
k100. Authenticate V271's baseline generation against its terminal before
opening it; build one candidate generation from the same source rows.

## Measurement and decision

Use one `causality` c7i.4xlarge Spot host in eu-central-1, with a two-hour
hard stop and one terminal attempt. Open each generation in a separate process
and warm once. Measure the same 1,000 queries sequentially at k100 with one
worker, no query-time GETs, and **diagnostic** widths PQ ef=256, shortlist=256,
exact ef=128. These widths are fixed before measurement to expose graph
coverage at 100k; they are not proposed production defaults. Record raw IDs,
visits and per-query latency, p50/p90/p95/p99 from the same samples, recall,
p05 hits, build time/RSS, search RSS, artifact bytes, instance/quote/cost,
and every input/output SHA-256. Upload artifacts and a terminal marker before
terminating Spot. An interruption discards the cell.

The test is **inconclusive** if the baseline has fewer than 100 misses out of
100,000 possible hits. Otherwise the candidate passes this falsifier only if
it cuts baseline misses by at least 30%, does not lower p05 hits, keeps p95
and p99 within 10% of the paired baseline, and preserves authenticated
open/search and graph reachability. Failure rejects this construction change.
A pass permits one frozen 1M fresh-query gate; it does not establish 10M
quality, HTTP product latency, or vendor superiority. No 10M build follows
directly from this 100k result.
