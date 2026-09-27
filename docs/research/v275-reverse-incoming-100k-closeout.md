# V275 reverse-incoming graph closeout

Decision: **reject the capped replacement implementation**. V272's 10M
recall failure remains open. V275 source commit
`50170297e61b3b923f83cae91668a1f7db7e72d0` tested bounded reverse
incoming links while preserving every node's out-degree. The baseline was
V271's authenticated root
`440beefd321dfeeae25ba6277a2e2f1f938c389a0c4a9007177f4bacc4b1d6e3`;
the candidate root was
`20b1890748d2c8686be80076fe60c5610bdfca0211e08e3ccce8e9d144f4c354`.
Both used CoHere-large-10M canonical train rows0–99,999, D768 cosine, with
1,000 excluded fresh train queries rows102,000–102,999 and FAISS `IndexFlatIP`
FP32 unit-normalized GT100 truth.

| Same 1,000 queries, k100 | Baseline | Candidate |
| --- | ---: | ---: |
| Diagnostic PQ ef256 / shortlist256 / exact ef128 GT hits / 100,000 | 98,503 | 98,503 |
| Diagnostic p50 / p90 / p95 / p99 (ms) | 2.766 / 3.186 / 3.293 / 3.593 | 2.823 / 3.259 / 3.355 / 3.725 |
| Diagnostic mean visits/query | 7,740.015 | 7,741.383 |
| Diagnostic p05 hits/query | 94 | 94 |
| Baseline in-degree≤8 GT misses | 298 | 298 |
| Default PQ ef4096 / shortlist4096 / exact ef2048 GT hits / 100,000 | 99,938 | 99,938 |
| Default p95 / p99 (ms) | 27.305 / 28.534 | 26.852 / 28.066 |
| Diagnostic search RSS | 204,504 KiB | 204,664 KiB |
| All-row share in-degree≤8 | 19.592% | 19.575% |

Candidate construction took 293.552 s and peaked at 842,468 KiB RSS; V271's
same-class baseline build receipt was 201.80 s and 840,720 KiB. Per-row
out-degree was unchanged, minimum in-degree stayed four, and all 100,000 rows
remained reachable. The candidate missed the preregistered ≥30% reduction in
low-degree GT misses and ≥50% reduction in all-row low-degree share: both
quality arms returned exactly the same hit counts. A post-terminal authenticated
graph diff found only **437 inserted and 437 displaced edges across 357
source rows**, out of millions of base links. This independently explains why
the no-extra-slot repair barely changed connectivity; it does not falsify
reverse links when additional memory is allowed. The experimental builder was
reverted after closeout.

The one c7i.4xlarge Spot instance `i-0a5d544914a5615db` was terminated after
complete terminal. Terminal SHA-256:
`7f986fd18117e0cd8d42f55e431bef555e3b512eefcbaa1ec34e4678b696eb19`;
closeout SHA-256:
`daaa1911cd03013e97cc8ee78336ac8e7ff75155611da8090b7099100e9ba82e`.
All 30 terminal artifacts were SHA/size replayed from
`s3://borsuk-bench-453182569524-euc1/research/v275-reverse-incoming/50170297e61b3b923f83cae91668a1f7db7e72d0/runs/a0001`.
Spot quote was $0.3729/hour; estimated compute through terminal was $0.06236,
excluding storage and transfer. These are in-process timings, not matched
HTTP or vendor measurements.

Next design decision: allow a bounded extra reverse-edge budget rather than
displacing scarce RNG-backfill edges. This spends graph memory only where
incoming connectivity is weak, consistent with memory scaling to the recall
target. Preregister one fresh 100k structure/quality/resource falsifier before
any 1M or 10M run; preserve V272's 10M per-split recall gate and measure
query tail latency and RSS from the new frozen format.
