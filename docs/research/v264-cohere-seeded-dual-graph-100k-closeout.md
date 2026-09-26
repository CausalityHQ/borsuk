# V264 CoHere 100k: seeded exact navigation fails latency gate

**Decision: stop PQ-seeded exact graph navigation.** The frozen quality gate
passed, but loaded p95 was 52.794 ms versus the 42 ms limit. The change
saved only 0.716 ms at p95 relative to V260, with 1000/1000 ordered ID
lists identical. Its p95 exact FP16 score count was also identical. The
PQ best row led to essentially the same exact search neighborhood as the
global graph route; this is an inference from the paired score counts and
IDs, not a proof for other datasets or scales. No 1M promotion follows.

One `causality` c7i.4xlarge Spot attempt `a0001` ran on
`i-09995aa0538396a4f` in `eu-central-1c`, now terminated. Source commit
`8cb587328cd3cb236499d37877d3c08141eb9286`, archive SHA-256
`7ea16a0a8d4d782fde0bb8d989118db86158231299f3ae3336cb6e7265440264`.
Immutable prefix:
`s3://borsuk-bench-453182569524-euc1/research/v264-cohere-seeded-dual-graph-100k/8cb587328cd3cb236499d37877d3c08141eb9286/runs/a0001/`.
Terminal status `complete`, exit0, SHA-256
`7277bc130af447a3c81e9279b44c3447e43969af579e60c414a80302a9bbdfce`.
The original launcher replayed every artifact size/SHA-256 and confirmed
termination. The remote Rust graph test passed. Source, plane, PQ books,
codes, map, graph, requests and exact truth match V260 byte for byte.

Dataset and split: CoHere-large-10M canonical first100,000 source rows,
D768 cosine, k100, authenticated prior-used test ordinals0–999;
development0–255 and validation256–999. Both methods run PQ-cosine graph
ef4096/FP16 shortlist4096→k100, exact FP16 graph ef2048→k100, and FP16
rerank of the distinct final-list union. V264 changes only the exact
graph start node to the PQ top ID's authenticated physical row.

| Same 100k panel, loaded eight-worker in-process | Dev GT100 / 25,600; p05 | Val GT100 / 74,400; p05 | Combined / 100,000 | p50/p90/p95/p99, ms | QPS |
|---|---:|---:|---:|---:|---:|
| V260 global exact entry | 25,583; 99 | 74,365; 100 | 99,948 | 47.756 / 52.109 / 53.510 / 55.730 | 166.32 |
| V264 PQ-seeded exact entry | 25,583; 99 | 74,365; 100 | 99,948 | 47.191 / 51.560 / 52.794 / 55.509 | 168.09 |

The raw sealed IDs and loaded timing samples independently replayed all
four percentiles and 1000/1000 ordered ID parity. V264 sequential
p50/p90/p95/p99 was 46.873/51.392/52.470/55.032 ms. Paired p95 PQ
scores were33,637 in both cells; p95 exact FP16 scores were21,236 in
both; p95 final distinct union was102 IDs in both. Component percentiles
must not be added. Serving peak RSS was214,601,728 B, under300 MiB;
zero query vector-body GETs and zero remote swap. Graph build took
170.40 s and peaked at523,064 KiB. Source launch17:39:38 UTC,
terminal17:51:12 UTC, elapsed694 s; at the previously recorded
$0.3663/hour Spot quote, compute through terminal was approximately
$0.0706, excluding EBS, S3 and cleanup tail. This is a quote-based
estimate, not an observed bill.

Sealed quality SHA-256
`6604ca258f4610c47849e21f217a97499873efa24de9df05884e597f39e5cc45`,
raw IDs `75cd75cc384c87be6318d3151ede152d276ea768a25684e235e367e31f87a811`,
loaded timing `35b7bf156e26f272f60e2e66fa098888431759e9476ca2b60e051e44ba2bf5e5`,
serving summary `721375bb6b287001f427a2bbc239df1edfae3cd60a1d8fe4f5d0af22d0ea748d`.

The next gate must change a material scoring or index format component
on one 100k falsifier. The resident HTTP V262 and direct S3 Vectors V263
first1M results remain historical evidence from their own frozen source
revisions; V264 planner timings are not a new product or vendor result.
