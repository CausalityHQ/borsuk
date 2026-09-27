# V274 bounded neighbour-expansion closeout

Decision: **reject candidate**. Source commit
`2cfbb7108eeda0895ae5dc17e072fc75d18725d5` used the same authenticated
V271 first100k graph in both arms. Candidate expanded the baseline FP16-ranked
k100 through up to 32 × k base-layer graph-neighbour ordinals, then reranked
with the same FP16 cosine scorer. Search widths were the fixed diagnostic
PQ ef256, shortlist256, exact ef128. This was Rust in-process resident search,
not HTTP product latency.

CoHere-large-10M canonical train rows0–99,999 (D768 cosine) formed the index;
excluded train rows101,000–101,999 were 1,000 fresh queries. Truth was FAISS
`IndexFlatIP` 1.15.1 on FP32 unit-normalized rows, k100. Root
`440beefd321dfeeae25ba6277a2e2f1f938c389a0c4a9007177f4bacc4b1d6e3`
and all blobs were authenticated for each arm; query-time object GETs were zero.

| Same 1,000 samples | Baseline | Neighbour expansion |
| --- | ---: | ---: |
| Exact GT100 hits / 100,000 | 98,558 | 98,575 |
| Recall@100 | 0.98558 | 0.98575 |
| Misses | 1,442 | 1,425 |
| p05 hits per query | 95 | 95 |
| p50 / p90 / p95 / p99 (ms) | 3.569 / 4.240 / 4.371 / 4.638 | 6.583 / 7.913 / 8.206 / 8.757 |
| Mean graph visits plus unique expanded rows/query | 7,719.581 | 9,065.870 |
| Search process peak RSS | 204,964 KiB | 204,732 KiB |

Misses fell only 1.18%, below the preregistered 30% reduction; p95 rose
87.75% and p99 88.83%, far beyond the 20% limits. The candidate increased
candidate work without reaching nearly all missed true neighbors. This is
evidence against one-hop rescue on this fixed diagnostic panel, not proof
that every local graph expansion fails. The experimental search method was
reverted after terminal closeout. V272's 10M recall no-go remains; no 1M or
10M run is authorized by V274, and no vendor superiority follows from it.

The c7i.4xlarge Spot instance `i-0d57cbcf03ec0dabd` was terminated after a
complete terminal. Terminal SHA-256:
`d0e7487eb5637d60655ce542ad8bde44abf4fffe97100b57ce0cf71a78ce9c40`;
closeout SHA-256:
`9c9d2a878821308a81b4f4378ade8863fa5c62137d01b4eb9539213aeb9eefda`.
All 16 terminal artifacts were SHA/size replayed from
`s3://borsuk-bench-453182569524-euc1/research/v274-neighbour-expand/2cfbb7108eeda0895ae5dc17e072fc75d18725d5/runs/a0001`.
Spot quote was $0.3729/hour; estimated compute through terminal was $0.02828,
excluding storage and transfer.

Next gate: choose one material index/topology redesign that can expose
nonlocal candidates, preregister its 100k fresh-query falsifier and resource
bounds, and promote only a winner to a new 1M gate. No further construction
or one-hop parameter sweep follows these two negative results.
