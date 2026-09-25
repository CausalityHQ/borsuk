# V214 persisted PQ graph 100k closeout

**Decision: reject the frozen V214 gate; retain the persisted graph
format and fresh-process memory improvement.** PQ64 navigation cuts
dense work enough for sub-10-ms in-process p95, but no frozen arm
matches paired V193 tail quality. Do not promote this exact method to
1M. The next single 100k falsifier changes navigation to cosine on
the existing source PQ reconstruction, aligning it with graph build
and final FP16 cosine; this is a metric correction, not a per-dataset
threshold adjustment.

The sole Causality c7i.4xlarge Spot attempt `a0001` ran on
`i-055a324fc787124a5` and is terminated. Source commit
`8f8cbd15852ccab48d80ea3c44d21627727b890f`, archive SHA-256
`e9bd1f98e3895d9aadc1dd2a3947c9f9f1291a9fd885989313b293563e7866da`.
The terminal SHA-256 is
`ad8db261ab30acaedafb491d2d50bab2349c605c7303bcac9374e18c1df08465`
at `s3://borsuk-bench-453182569524-euc1/research/v214-pq-graph-100k/8f8cbd15852ccab48d80ea3c44d21627727b890f/runs/a0001/terminal.json`.
Terminal status is `complete`, exit 0; all 19 artifact lengths and
SHA-256 hashes passed S3 readback. Raw returned IDs were sealed before
truth and V193 downloads.

Dataset/split: **ReLAION-100k D768, already-used development queries
0–255**. Paired V193 full-rank SQ8 returns **25,440/25,600 GT100
hits**. V214 fixed source-only cosine HNSW M=32, M0=64,
ef_construction=128; source-trained PQ64 squared-L2 scores navigated
the graph, and authenticated resident FP16 cosine reranked each frozen
shortlist. All times are whole in-process sequential Rust requests;
they exclude network, concurrent admission, mutation, generation swap
and S3 transport.

| ef / FP16 shortlist | GT100 hits /25,600 | p05 hits/query | p50 / p95 / p99 (ms) | p95 base visits | sequential QPS | Gate |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| 1024 / 1024 | 25,316 | 95 | 3.368 / 4.150 / 4.382 | 15,200 | 294.46 | quality fail |
| 2048 / 1024 | 25,314 | 95 | 4.219 / 5.460 / 5.787 | 23,410 | 236.34 | quality fail |
| 2048 / 2048 | **25,430** | **97** | 6.011 / **7.224** / 7.659 | 23,410 | 165.76 | quality fail |
| 4096 / 1024 | 25,316 | 95 | 5.854 / 7.644 / 8.163 | 35,984 | 169.86 | quality fail |
| 4096 / 2048 | 25,429 | 97 | 7.661 / 9.413 / 9.866 | 35,984 | 130.33 | quality fail |

The frozen gate was hits≥25,440, p05≥98, p95≤10 ms, peak serving
RSS≤320,000,000 bytes, and zero vector-body GETs. Latency, memory
and zero-GET criteria passed in all arms; paired quality failed in
all arms. Increasing ef from 2048 to 4096 with the same 2048 FP16
shortlist did not recover the missing hits, so the observed loss is
consistent with PQ shortlist ordering rather than graph discovery
alone. The squared-L2 PQ score is inconsistent with the cosine graph
and FP16 final metric; whether correcting that score closes the quality
gap is an unverified hypothesis.

The versioned graph artifact was **26,318,786 bytes** and bound to
the source, FP16 plane and generation; complete SHA-256 and edge
validation passed in a separate serving process. The loaded graph
owned **30,889,640 bytes** of heap, the FP16 plane **154,400,000
bytes**, and PQ codes **6,400,000 bytes**. Measured serving peak RSS
was **202,280,960 bytes**, versus V213's same-process 839,872,512 B
build peak and 833,736,704 B post-build RSS. Cold hydration was
0.327 s. The separate source graph build took **189.973 s**, peaked
at **839,892,992 bytes**, and released that heap at process exit.

The instance launched 2026-09-25 09:15:44 UTC; the terminal landed
09:23:20 UTC, 456 s later. EC2 Spot history for eu-central-1c gave
**$0.3644/hour** effective at launch; launch-to-terminal compute-only
cost is **estimated $0.0462**, excluding termination tail, EBS, S3
and taxes. This is not a billing measurement.

V214 is a 100k in-process falsifier. It does not establish end-to-end
product latency, 100M behavior, or a matched comparison with
Turbopuffer or AWS S3 Vectors.
