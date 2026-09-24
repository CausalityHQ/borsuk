# V131 production-Rust source replay: 100k development decision

V131 completed one immutable Causality `c7i.8xlarge` Spot cell in
`eu-central-1` at source archive SHA-256
`25c935e2498be9c6fdca92a182dea7c9643ed078194995576c982f37b2b4c4b2`.
Its output prefix is
`s3://borsuk-bench-453182569524-euc1/research/v131-source-replay/56f7071716681fde524432d29a436015584df773/runs/v131-20260924T052935Z/a0001`.
Terminal SHA-256 is
`7a68269ddd56f940c0896b75e42b87d7e498cbd2634d869ee1be4e89d8a21534`:
`complete`, exit 0, 246 seconds, instance `i-06c836aeff127af89`, observed
terminated. All 16 terminal-listed artifacts (83,020,546 bytes total) and
the 4,943,643-byte source archive were independently downloaded or streamed
from S3 and checked against their recorded lengths and SHA-256 values.

The dataset is **deep-image-96-angular random 100k train subset** from the
sealed V122 source. The 1,000 queries are **used publication-test ordinals
9000–9999**, with GT100 recomputed within this subset. This is a development
screen, not untouched publication evidence. Its paired arms use the same
production Rust scorer, 512 router nominees, top-512 SQ8 returned expansion,
source-ID map, exact F32 source-plane coordinates scored as float64 cosine,
65,536-byte authenticated local blocks and V122's fixed physical ranges.
Arm execution order alternates by query. All V122 inputs, query identities
and truth rows were authenticated before scoring.

| Metric, 1,000 queries | Candidate | Paired capped control |
| --- | ---: | ---: |
| Same-run Rust SQ8 Recall@100 | 99.106% (99,106/100,000 hits) | 98.203% (98,203/100,000) |
| Exact-source Recall@100 | **99.942% (99,942/100,000)** | 98.827% (98,827/100,000) |
| Exact-source p05 hits/query | 100 | 94 |
| Exact-source minimum hits/query | 97 | 82 |
| Queries below 90 hits | 0 | 12 |
| Source-rank p50 / p95, warm local ms | 14.224 / 15.034 | 14.129 / 14.678 |
| Offline SQ8+map+source p50 / p95, ms | 30.218 / 32.064 | 16.231 / 18.708 |
| Maximum logical authenticated local bytes/query | 27,721,728 | 25,559,040 |
| Maximum verified local blocks/query | 423 | 390 |

The candidate won 335 paired source-hit queries, tied 665, and lost none.
Exact source hit count equals the V122 physical-range GT coverage on **every
query in both arms**. The 512-plus-512 union therefore reaches this fixed
route's measured capture ceiling at 100k. Candidate source ranking gains
836 GT positions over same-run candidate SQ8; the capped control gains 624.
The candidate's p95 source rank is below the 30-ms preregistered local
headroom gate, and its maximum logical local read is below 32 MiB. The
candidate meets the ≥99.5% source-recall, p05≥98, zero-sub90, same-run
control, and ≥99.0% SQ8 gates. No gate threshold was adjusted after seeing
V130's failure.

Production Rust and sealed V122 NumPy produced identical top-100 **sets**
for all 1,000 queries and both arms, reproducing V122's SQ8 hit totals.
Their within-set ordering differed on nine queries per arm; no set
membership difference occurred. The V130 strict ordered-parity failure is
preserved separately in `v130-source-replay-closeout.md`. This matters for
rank-sensitive metrics, but not the reported Recall@100 set intersection.

The source-plane artifact is 39,200,064 bytes (`BORSST02`); the source-ID
map is 1,600,096 bytes (`BORSMAP1`). Its resident entry payload is 1,600,000
bytes and the source digest table payload is 19,168 bytes. Build plus
authenticated open took 165.780 ms inside the evaluation process; the
evaluation process peaked at 43,432 KiB RSS and ran 46.39 seconds. These
are **not charged serving RAM**: they exclude or cannot separate source
file page cache, compiler/worker cgroup cache and a deployed runtime.
`source-rank` times include local block SHA checks and exact cosine only;
`offline SQ8+map+source` times include in-memory returned SQ8 scoring,
map resolution and source scoring, but exclude router computation, network
GETs, S3 retries, startup and concurrent throughput. The candidate's
10.8 MB SQ8 object was fully fetched in V122's physical plan, so this
100k result does not demonstrate selective reads at larger corpus sizes.

Independent closeout recounted all 1,000 V131 evidence rows against V122's
sealed truth IDs and physical coverage. It checked source and SQ8 unique
IDs, per-query hits, candidate counts, local read statistics, p05/sub90,
p50/p95 times, paired outcomes and all order/set differences against the
authenticated summary. The summary SHA-256 is
`0337d8059566b00df163db9db1d80304df63e36d9e04953e91d6ce981d847262`;
the per-query replay SHA-256 is
`9c687edc8ea66a5ef41b1e83d1d51788022b58cf526f2e9dcdb21a38bb5dd7d9`.

Decision: **promote the candidate to a live 100k S3 serving integration
gate**, while keeping production defaults unfrozen. That gate must use the
authenticated generation, ETag-pinned S3 range reader and hydrated source
plane, and measure complete p50/p95/p99, wire GET attempts/bytes,
throughput and charged serving memory beside the same capped control.
After the live gate, rebuild both deep-image and ReLAION-1M with one generic
layout/router and source policy on fresh splits. The existing V121 and
V126 layouts were fitted differently and cannot qualify cross-dataset
generality. Only a winner from that matched 1M gate proceeds to selective
10M and 100M cost/quality tests. Lean conditional bounds remain useful for
invariants and payload arithmetic; they do not replace these measurements.
