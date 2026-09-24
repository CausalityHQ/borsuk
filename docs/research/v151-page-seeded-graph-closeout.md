# V151 page-seeded graph: terminal-closed reject

**Frozen decision:** reject. The primary-seeded, page-diverse graph
restored the preregistered returned-quality gate on the fresh
deep-image-96-angular random100k subset, publication test ordinals
3000–3999, but missed the paired CPU gate. No result here qualifies
1M, 10M or 100M performance.

## Authenticated attempt

- Source commit: `84bfeefc73f4ccca8d785e2c63fb4f642df82ddd`.
  Source tarball SHA-256:
  `9089fda4eecfc371e3f1c5c0593c4eff9bea70a57d96738e840591054d77eca4`.
- One uninterrupted Causality Spot `c7i.8xlarge` attempt,
  `i-0acb76d4ae524793d`, elapsed 492 s, terminated after terminal.
  Terminal: `s3://borsuk-bench-453182569524-euc1/research/v151-page-seeded-graph/84bfeefc73f4ccca8d785e2c63fb4f642df82ddd/runs/v151-20260924T130137Z/a0001/terminal.json`,
  SHA-256 `741b7507b7c9e2653127435fe6aa2db86e01dc677ba776c8c1cb3b29ab533d72`.
  All 33 terminal artifacts matched their recorded sizes and SHA-256
  values in the postterminal recount. Graph SHA-256 was the frozen V150
  `647799e92c32fa724aa764a4af2053377e7267ef7d968936adcbb2cc6de966dc`.
- The worker's five focused graph tests passed. GT-blind returned-ID
  audit recomputed 3,000 ordered SQ8 and source-result lists before GT
  reduction. The postterminal verifier independently replayed all page
  plans, recomputed V151 provisional minima and scored-page distances
  from the authenticated f16 plane, and recounted quality and verdict.
  Its initial double-precision ranking check rejected a float32 tie on
  query 244; after modeling Rust's float32 accumulation exactly, the
  same immutable artifacts passed the full recount. This verifier fix
  changed no worker data, frozen gate or measured result.

## Paired results

All values below are measured on the same 1,000 queries and source
subset. CPU is the single-threaded wall-clock time for page scoring,
graph search where applicable, and planning; it excludes SQ8 range
reads and source reranking. V150 always ran between the alternating
flat and V151 arms, so its timing is diagnostic only.

| Arm | Source Recall@100 | p05 hits | Queries <90 hits | CPU p95 (ms) | Planned bytes total | GETs total |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Flat V146, paired | 99.720% (99,720/100,000) | 98 | 0 | 0.570292 | 2,690,858,880 | 28,267 |
| V150, unchanged | 99.505% (99,505/100,000) | 97 | 1 | 0.517435 | 1,995,667,200 | 25,277 |
| V151 | 99.646% (99,646/100,000) | 98 | 0 | 0.644371 | 2,648,446,848 | 27,982 |

V151 was 74 hits (0.074 percentage point) below paired flat, within
the 100-hit gate; it won 56 queries, tied 846, and lost 98. Its
planned bytes and GETs were lower than flat, all queries retained
their primary pages, every plan stayed within 32 GET and 16 MiB, and
the maximum scored-page difference from flat was zero. However V151
CPU p95 exceeded flat by 0.074079 ms (13.0%), violating the strict
CPU gate. The frozen verdict is therefore **reject**.

V151 p95 graph evaluations were 624 units, exact-page evaluations
1,325, distinct union 1,325, and total computed distances 1,933.
The large gap between total and union shows that the current code
recomputes graph-scored units when it scores candidate pages. The
graph walk seeds all 8P primary-page units, leaving at most 8P new
units under its 16P cap, then scores primary pages again. This generic
duplicate-work mechanism is the leading explanation for the CPU loss;
the timing data do not isolate it causally. V151 had 168 queries with
planner target shortfall, p95 shortfall five pages. Graph build took
218.556 ms, graph artifact was 426,398 B, science maximum RSS was
32,516 KiB, and the isolated science cgroup peak was 62,398,464 B.

The quality screen is limited by 100k geometry: there are only 391
physical pages and the 5P candidate budget can exactly score a large
fraction of them. The entire SQ8 corpus is 10.8 MB, below the 16 MiB
per-query cap. This pass is necessary implementation evidence, while
1M is the first decisive sparse-discovery quality gate.

## Next design decision

V152 should make one evaluated centroid distance reusable by both graph
navigation and exact candidate-page scoring, avoiding the duplicated
work without a corpus-size switch or dataset-specific cap. Keep the
same frozen graph, router, page geometry and transport budget. Measure
the structural CPU change against paired flat and V151 on a declared
development split; use the reserved 4000–4999 ordinals only to confirm
an actual winner. A passing 100k confirmation then advances to a
frozen paired ReLAION-1M gate, where the candidate fraction is small
enough to test discovery quality. The method must still allow RAM to
rise with required recall and latency rather than imposing a vector
count knee.
