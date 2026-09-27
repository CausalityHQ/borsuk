# V273 100k construction-quality closeout

Decision: **reject candidate**. The V272 10M RC remains a recall no-go; V273
does not authorize a 1M or 10M rerun. Source commit
`f5cc5a01a7184112adb609c3fff4e59508da5ecb` tested M32/M0=64,
efConstruction256, insertion batch cap1024, and up to32 workers against the
authenticated V271 M32/M0=64, efConstruction128 graph. Both used the same
fixed Rust in-process diagnostic search widths (PQ ef256, shortlist256,
exact ef128), not product defaults or HTTP serving.

CoHere-large-10M canonical train rows0–99,999 (D768 cosine) formed the index;
excluded train rows100,000–100,999 were the 1,000 fresh queries. Exact k100
truth was FAISS `IndexFlatIP` 1.15.1 over FP32 unit-normalized vectors. The
V271 baseline root was
`440beefd321dfeeae25ba6277a2e2f1f938c389a0c4a9007177f4bacc4b1d6e3`;
the candidate root was
`a8e2e2c7c6a14aa8589d35d9a448796cdf41ffb6483dcc40948e16a1def12ee0`.
Both opened with root/blob authentication and used zero query-time object GETs.

| Same 1,000 samples | Baseline | Candidate |
| --- | ---: | ---: |
| Exact GT100 hits / 100,000 | 98,492 | 98,647 |
| Recall@100 | 0.98492 | 0.98647 |
| Misses | 1,508 | 1,353 |
| p05 hits per query | 94 | 95 |
| p50 / p90 / p95 / p99 (ms) | 3.112 / 3.594 / 3.708 / 3.967 | 3.365 / 3.933 / 4.109 / 4.377 |
| Mean graph visits/query | 7,802.662 | 7,905.562 |
| Search process peak RSS | 204,900 KiB | 204,692 KiB |

Candidate misses fell 10.28%, below the preregistered 30% reduction. Its p95
rose 10.81% and p99 10.31%, both above the preregistered 10% limits. Candidate
build took 3m13.16s, peak RSS 847,000 KiB; its six generation files total
188,762,768 bytes. This 100k diagnostic result is not a 10M or vendor
performance measurement. The production builder's candidate construction
changes were reverted after terminal closeout; the authenticated RC format
and V272 artifacts remain intact.

Attempt `a0001` failed at input authentication before compilation or
measurement because its expected raw-vector SHA was the ID-plus-vector source
identity. Its terminal and closeout are retained. Attempt `a0002` corrected
that check without changing the panel, algorithm or decision rule. The one
c7i.4xlarge Spot instance `i-09fccc94d47c26465` was terminated after its
complete terminal. Terminal SHA-256:
`dfac21e16f24f4b0fddb6c2520736cd77758461465c576b68618a9619d3c4885`;
closeout SHA-256:
`c507f5945ed634bd45685980fed87cbc48c702d4bcdfb5f8322cccc7ec8761b2`.
All 24 terminal artifacts were SHA/size replayed from S3 at
`s3://borsuk-bench-453182569524-euc1/research/v273-construction-quality/f5cc5a01a7184112adb609c3fff4e59508da5ecb/runs/a0002`.
Spot quote was $0.3729/hour; estimated compute through terminal was $0.04734,
excluding storage and transfer. Attempt `a0001` estimated compute was $0.00673.

Next gate: one preregistered 100k fresh-query falsifier of bounded exact-graph
neighbor expansion on the same authenticated V271 baseline, using this
diagnostic stress panel where coverage misses are measurable. Promote only if
it recovers enough misses within a fixed tail-latency and memory bound; then
run one fresh 1M gate before another 10M build. Do not claim vendor superiority
from these in-process timings.
