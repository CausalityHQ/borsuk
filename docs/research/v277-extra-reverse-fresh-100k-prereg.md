# V277 fresh-query validation of bounded extra reverse edges

Status: preregistered before V277 measurement. V276 terminal SHA-256
`6f83e1877ed0e5eaf44ba5514c7588df6b924476f035cea5a77feffc0d8148a1`
is immutable. V276 failed only its impossible absolute max out-degree≤96 gate:
V271's authenticated baseline already has max118, and V276 preserves all
baseline edges. The corrected structural rule below is fixed before a new
query panel is measured. V276 cannot itself authorize promotion.

## Frozen inputs and method

CoHere-large-10M canonical train first100k, D768 cosine, k100. V271 baseline
root `440beefd321dfeeae25ba6277a2e2f1f938c389a0c4a9007177f4bacc4b1d6e3`;
V276 candidate root `56de9f4768271611683193f4fa5d36795cb5926adabb89efb7bd4c851b1e340b`.
Download both generations by their authenticated V271/V276 terminal identities;
do not rebuild or tune either graph. Fresh excluded train query rows
104,000–104,999, raw FP32 SHA-256
`0099cdcd57a80d33437a63a3cd9e9fab4bd333ba27ad4d1cedbc2d32e2a932cb`.
Create FAISS IndexFlatIP FP32 unit-normalized GT100 using the same authenticated
source first100k as V276. Candidate method remains V276's M32/M0=64,
efConstruction128 graph with at most16 appended reverse edges per source
whose original out-degree is <96. No baseline edge is displaced.

One eu-central-1 c7i.4xlarge `causality` Spot host, two-hour hard stop, one
attempt. Load each arm in a separate Rust process with one warmup and run the
same 1,000 queries sequentially at diagnostic PQef256/shortlist256/exactef128
and default PQ4096/shortlist4096/exactef2048. Record query-level IDs, visits,
latency and same-sample p50/p90/p95/p99, recall, p05 hits, search RSS, graph
structure and bytes, instance/quote/cost and artifact SHA-256. Both arms are
local resident, zero query-time GET. Upload terminal artifacts before stopping
compute; discard an interrupted cell. This is in-process evidence, not HTTP
service or vendor evidence.

## Frozen decision

Inconclusive if baseline has fewer than100 GT100 misses in the baseline
in-degree≤8 subgroup. Otherwise promote to a fresh 1M gate only if candidate
reduces that fixed subgroup's misses by ≥30%, total diagnostic misses do not
rise, p05 hits do not fall, mean visits and diagnostic p95/p99 are each ≤120%
of baseline, and search RSS≤115% baseline. All baseline edges must remain;
each source gains ≤16 edges; a source with baseline degree≥96 gains zero;
candidate max out-degree≤max(baseline max,96), graph bytes≤115% baseline,
minimum in-degree≥4, full reachability and authenticated open/search hold.
The all-row share with in-degree≤8 must fall ≥50%. At default widths,
candidate hits may fall by at most10/100,000 and p95/p99≤120% baseline.
Reuse V276's verified build evidence: 241.286 s and 847,204 KiB, each≤150%
of V271's same-class 201.80 s and 840,720 KiB. Any failed gate rejects
promotion. No vendor or 10M claim follows from a pass.
