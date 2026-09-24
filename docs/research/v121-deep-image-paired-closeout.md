# V121 deep-image 10M paired quality closeout

The V121 source-only 9,990,000-row D96 index was V120's sealed build. The
first two V121 attempts failed before GT access: a0001 had compose CLI wiring
error; a0002 hit a redundant physical-planner budget lattice. Their immutable
failure records are in `v121-a0001-harness-failure.md` and
`v121-a0002-planner-lattice-failure.md`. Attempt a0003 preserved the candidate,
control, source index and test-first-1000 cohort; it applied exact budget
lattice normalization and completed the paired returned replay.

Attempt a0003 source commit was `79a51449cbeb169851d9c02c173488d0f073d519`.
Its terminal is at
`s3://borsuk-bench-453182569524-euc1/research/v121-deep-image-paired/79a51449cbeb169851d9c02c173488d0f073d519/runs/v121-20260924T014535Z/a0003/terminal.json`,
SHA-256 `01cf70dfc77c285636943b334c21efb3bccbfe33dcfeffee153e4a7cdb484113`.
The `c7i.12xlarge` Spot `i-08a10e7cfab9cf985` in `eu-central-1c` terminated
after its 353-second worker run. The original launcher exited 0. All 27
terminal-listed artifacts were independently downloaded and checked by length
and SHA-256, including the summary SHA-256
`071cfcf3687cdee456175457bd54f77441f247ee15c38f60bc9aba00cc590dcb`.

| Deep-image-96-angular, publication test-first-1000, GT100 | Candidate | Paired V109-style capped control |
| --- | ---: | ---: |
| Returned Recall@100 | 98.034% (98,034/100,000 hits) | 97.791% (97,791/100,000) |
| p05 returned hits/query | 96 | 95 |
| Queries below 90 hits | 12 | 26 |
| GT hits physically covered by fetched ranges | 99,580 | 99,323 |
| p05 physical GT coverage/query | 99 | 96 |
| Maximum GETs/query | 32 | 32 |
| Maximum planned bytes/query | 16,754,688 | 16,754,688 |

The candidate won 78 paired queries, tied 920, and lost two. Both arms stayed
under the 32-GET/16,777,216-byte cap. Independent recount compared all 1,000
sealed replay rows with the publication GT object, SHA-256
`d305fcea7387988941defd2942cca1673693271329f977ba073da888cac3de8d`,
and V120's authenticated layout, SHA-256
`419f9280d2e85f6fa275c115dd342c249ac31ec6af2d19a42f5b96c38ed247c1`.
It matched returned hits, p05, sub-90, GETs and bytes. The 512 physical
nominees contained 99,994/100,000 source-ID GT hits after layout mapping;
exact local SQ8 primary top-100 contained 98,332. Candidate fetched ranges
covered 99,580, but final returned SQ8 top-100 contained only 98,034. Thus
at least 1,546 covered GT hits were displaced by final SQ8 ranking, while
420 were outside candidate ranges. This decomposition identifies final-score
representation/reranking as the dominant miss source; routing and physical
selection are secondary. The p05 physical coverage was 99, so the fixed plan
has room to improve returned quality without increasing its remote cap.

The preregistered ≥99% returned-quality floor **failed**, despite beating the
same-run control. V122's 100k D96 development screen passed at 99.106% because
its entire 10.8-MB SQ8 body fit in one GET; that result did not predict score
crowding at 9.99M rows. Do not tune a deep-image-specific score width or
vote factor to make this cohort pass. The next experiment must change the
generic final-score/rerank representation and use new held-out quality rows
after a source-only development screen. V116 ReLAION-1M and V121 have different
layout fitters, so they cannot be presented as one frozen-method cross-corpus
pair. A matched ReLAION rebuild remains required before generic promotion.

The offline Rust nomination batch took 3:04.76 for 1,000 queries (184.76 ms
per query averaged over that batch), peaking at 697,808 KiB process RSS.
Compose took 34.18 s, and local exact-mirror scoring plus interval planning
and SQ8 replay took 1:09.10, peaking at 2,142,740 KiB. These are worker
measurements, not live S3 latency, throughput or serving cgroup RAM. The
flat-router CPU cost is independently a serious latency concern even if
final-score quality is repaired. No 10M production default is qualified.
