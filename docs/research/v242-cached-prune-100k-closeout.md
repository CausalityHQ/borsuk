# V242 cached neighbor distances, ReLAION-100k closeout

**Decision: fail the preregistered 25% build-time gate.** V242's one
cached-key sort preserved the exact V241 graph and all returned IDs,
and reduced measured build time by 21.8%. It finished at 138.640 s,
above the frozen 133 s ceiling. This is a useful generic improvement,
but it does not qualify the serial builder for 1M promotion or solve
the 10M/100M time problem. The preregistration called for reverting
after a miss; the production code provisionally retains this one-line,
exact-output improvement because removing it would restore 38.721 s
of measured 100k build work. This is an explicit decision deviation,
not a changed or passed gate. The next gate must test a material
parallel construction design, with its own frozen thresholds.

The sole `causality` c7i.4xlarge Spot cell was `a0001` on
`i-07f45b33c8cad7673` in `eu-central-1c`; it is **terminated**.
Source commit `5dca961dbe8c4e3d8635867a3c0bd55f2e6940b6`, source
archive SHA-256
`0ed30b8329fafbff95aef483362d1002017f195e6fbdc7fd2ca34f997a3c3851`,
terminal SHA-256
`bab59b7ab4ac7024f7f550928ba55ed3c318b3533773728306b73ab0fcdc50be`.
The terminal exited zero; the original launcher replayed all ten
artifact sizes and SHA-256 hashes and confirmed termination. The
sealed raw SHA-256 is
`26e0bd70d76c344cd52660ae33e18a57c1c4b12892e23d1cd4b1668a4b3552ae`.
Evidence is under
`s3://borsuk-bench-453182569524-euc1/research/v242-cached-prune-100k/5dca961dbe8c4e3d8635867a3c0bd55f2e6940b6/runs/a0001/`.

The frozen source is ReLAION-100k D768 cosine k=100, with prior-used
development queries 0–255 and method-held-out queries 256–999, the
same authenticated source, plane, PQ books/codes and requests as V241.
M=32, M0=64, ef_construction=128, PQ ef/FP16 shortlist=2048/2048.
The V242 graph SHA-256 is exactly V241's
`d8b70919243a7cd6ecb9448ce23f776374738476c1882cbc6a651fb34753af2f`
at 26,571,646 B. Both runs have 100,000/100,000 reachable nodes,
minimum in-degree four, zero rows below four in-edges, maximum degree
99 and 6,463,215 directed base edges. The narrow release Rust graph
test passed 1/1.

| Verified build | Rust build time, s | Peak builder RSS, B | Graph SHA-256 |
| --- | ---: | ---: | --- |
| V241 owned F32 | 177.361 | 530,915,328 | `d8b70919…af2f` |
| V242 cached sort key | **138.640** | 531,288,064 | `d8b70919…af2f` |

The 38.721 s, 21.8% difference is descriptive across separate Spot
instances; the same graph bytes prove deterministic output, not the
precise causal fraction of every build stage. V242 met the 600 MB RSS
ceiling, missed the 133 s build ceiling by 5.640 s, and had no swaps.
Its `/usr/bin/time` wall graph build was 2m18.98s.

V241 and V242 raw artifacts were authenticated against their terminal
receipts; V242's sealed raw bytes matched its uploaded raw artifact.
Independent replay found **1,000/1,000 identical ordered PQ result
lists** and **1,000/1,000 identical exact-FP16 diagnostic lists**.
V242 scored 25,537/25,600 development GT100 hits,
74,234/74,400 held-out hits, 99,771/100,000 combined, p05=99,
identical to V241. Its loaded eight-worker in-process
p50/p90/p95/p99 was 5.830/6.778/7.060/7.487 ms,
1,337.9 QPS, peak serving RSS 211,460,096 B and zero vector-body
GETs. These serving values are not a same-host network product win.

The instance ran from 21:15:05 to terminal upload 21:24:58 UTC
(593 s). At the eu-central-1c Spot quote of $0.3676/hour, compute
was approximately **$0.0606**, excluding EBS, S3, termination tail
and billing rounding.

**Next gate:** inspect deterministic distance-call counts in the
serial build, then test one fixed, source-only batch construction at
100k. Its parallel search must use a frozen graph snapshot and
fixed-order edge commits; compare thread-count SHA, reachability,
paired returned quality, build time and RSS against V242. Do not infer
1M/10M/100M build performance or a S3 Vectors/Turbopuffer win from
this 100k result.
