# V254 coarse PQ route: bounded work, failed quality

Source commit `a7def2219cb4cc402637e8bbfd905d1d300840f2`; one
`causality` c7i.4xlarge Spot attempt `a0001`, instance
`i-0e639dbefc98814a7`, terminated after its complete terminal.
Immutable prefix:
`s3://borsuk-bench-453182569524-euc1/research/v254-cohere-coarse-pq-100k/a7def2219cb4cc402637e8bbfd905d1d300840f2/runs/a0001/`.
Source archive SHA-256
`5dae2b2b18294f74feb6b645f665c8ea58f1c997770b45fffdde8a34b9bd3710`;
terminal SHA-256
`97a91fd937d61f804e17fcce047685e9c11df12c07ffbb2077000ec3519ec16e`.
The launcher replayed every terminal artifact's length and SHA-256, and
the narrow remote PQ test passed 1/1. The source, FP16 plane, PQ
books/codes, map, requests, truth and paired-source graph match V253
byte for byte. The sealed V254 raw IDs SHA-256 is
`3aaa5d79192f6af776ea14c3593c03428c47dd71b455d6495cbea2eda4755341`.

CoHere-large-10M first 100k rows, D768 cosine, k100. Development query
ordinals 0–255 and validation 256–999 were both previously used. V254
trains 391 spherical centroids on source rows, assigns each row to its
two closest lists, probes 32 query-nearest lists, PQ-scores the unique
rows and FP16-reranks the top 8,192. V253 scores all 100,000 PQ rows
and uses the same FP16 rerank. Times are eight-worker loaded in-process
measurements on separate same-class Spot attempts, not service latency.

| Method | Dev exact GT100 hits / 25,600; p05 | Val hits / 74,400; p05 | Combined / 100,000 | Loaded p50/p90/p95/p99 ms | QPS | p95 PQ rows scored |
|---|---:|---:|---:|---:|---:|---:|
| V253 global PQ | 25,575; 99 | 74,347; 99 | 99,922 | 18.560 / 18.946 / 19.157 / 19.505 | 418.9 | 100,000 |
| V254 coarse PQ | 24,568; 85 | 72,029; 89 | 96,597 | 17.142 / 17.381 / 17.448 / 17.603 | 465.2 | 17,254 |

V254 failed the frozen mean R@100≥0.995 and p05≥98 on each split.
Its p95 PQ work≤20,000, exactly 8,192 FP16 rows, loaded p95<19.157 ms,
226,299,904 B peak serving RSS and zero vector-body GETs passed their
separate gates. Cold hydration was 358.086 ms. Coarse preparation took
5.29 s and peaked at 933,528 KiB. These are measurements only at 100k;
they do not establish scale or vendor superiority.

After terminal, independent replay of all 1,000 sealed ID sets against
the authenticated truth reproduced both split counts. Reconstructing
the frozen 32-list union from the authenticated centroids, postings and
request vectors found only **96,609/100,000 exact GT100 IDs inside the
route**. Thus 3,391 exact neighbors were never scored; only 12 were
inside the route but absent from the final results. V253's returned IDs
were inside V254's route in 96,623/100,000 cases. Against V253, V254
lost 3,369 exact hits, gained 44, tied on 280 queries and lost on 720;
it won none. The causal failure is coarse-list coverage, not the PQ
ranking or FP16 rerank after admission. The two-source-assignment/32-list
route is rejected. The preregistered rule forbids tuning its K, copies
or probes on this used panel.

**Decision:** retain V253 as the strongest CoHere 100k quality baseline
and its authenticated PQ/FP16 artifacts. A read-only, non-preregistered
bound diagnostic on closed query ordinals 0–9 evaluated the exact PQ
top8,192 cutoff and an admissible list bound: sum the best query/PQ
codeword dot product seen in each of 64 subspaces, then divide by that
list's minimum reconstructed norm and the query norm. For query 0 the
bound covered every posting's actual PQ score, but retained 172/391
lists and **45,035 unique rows** above the true PQ cutoff. Across the
ten queries it retained 45,035–54,965 unique rows. Computing these
bounds also requires list/codeword work. This simple score-aware bound
does not meet V254's 20,000-row work target; no new benchmark claim
follows from this diagnostic.

The practical next scale candidate is the already measured V250
diverse graph with PQ navigation and FP16 rerank: at 100k its ef4096
arm met exact quality and loaded latency, though its p95 33,637 PQ
scores exceeded that campaign's frozen 10,000-score scale threshold.
That 100k result is not relabeled a pass. Freeze a **new** CoHere-1M
gate around its unchanged source-only graph method, explicit
score-work and memory budgets that scale with N, exact recall and
end-to-end serving latency. The gate will determine whether the graph
work actually scales; it cannot be inferred from either 100k result.
Keep 10M closed until the 1M gate passes.
