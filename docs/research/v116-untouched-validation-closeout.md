# V116 untouched ReLAION-1M validation closeout

Status: **preregistered offline quality gate passed**. Live S3 performance,
cross-corpus quality and production readiness remain unmeasured.

## Frozen attempt and verification

- Source commit: `5e9b35ad40ea023eab4407aa611d759e1893bb34`;
  archive SHA-256 `a56f17b40b4c747c6361393ca3d03d727dfa5b9de45d4b18599fbe4efec54f30`
  (11,541,885 bytes).
- Causality Spot `c7i.12xlarge` `i-0ee37f04d15d3c16c`, launched
  2026-09-23 23:54:34 UTC and terminated after the terminal marker.
- Immutable attempt:
  `s3://borsuk-bench-453182569524-euc1/research/v116-validation-paired/5e9b35ad40ea023eab4407aa611d759e1893bb34/runs/v116-validation-20260923T235426Z/a0001`.
- Terminal: `complete`, exit 0, 211 seconds. All 21 terminal-listed artifacts
  were independently streamed back and verified against their SHA-256 and
  recorded sizes (69,581,817 bytes total). Summary SHA-256:
  `a0cca3ac72ab914ee8aa450417689b1bd13698b25c2a658d92215cc07759749a`.
- A separate local recount streamed the sealed Rust replay and the pinned
  validation GT, checked the query ordinals, top-100 uniqueness and both
  physical caps, and reproduced every reported quality and paired count.

## Paired result

The dataset is **ReLAION-1M, validation-1000**, with 100 GT neighbors per
query. Both arms use the same source-only router planes and Rust returned
SQ8 scorer. The candidate uses exact-primary weighted interval planning;
the control uses V109 ranked-page admission. Neither arm's plan saw GT.

| Measure | Candidate | Paired capped control |
| --- | ---: | ---: |
| GT100 hits / 100,000 | 99,208 | 98,618 |
| Recall@100 | 99.208% | 98.618% |
| p05 hits / 100 | 97 | 93 |
| Queries below 90 hits | 3 | 25 |
| Maximum planned GETs/query | 32 | 32 |
| Maximum planned bytes/query | 16,773,120 | 16,773,120 |

Candidate had more GT hits on 151 queries, equal hits on 848, and fewer on
one. The candidate clears the preregistered 99.0% floor, paired total and
tail comparisons, and physical caps. This is a verified offline quality
measurement. The Rust router nomination batch took 19.83 seconds for 1,000
queries at 93,544 KiB process RSS; the paired local SQ8 replay took 24.77
seconds at 1,536,228 KiB RSS. Those are batch figures on Spot, **not**
live-query latency, QPS, cgroup memory, or S3 cost.

The historical V75 reader reported 99.272% Recall@100 on ReLAION-1M
validation-1000. V116 is 0.064 percentage points lower, but V75 is not a
matched, same-run control and had a different source-bound reader. Do not
claim V116 improves on the strongest historical reader. The paired gain
against the capped control is 0.590 percentage points under this source
and scorer.

## Decision

Keep the candidate method unchanged for a genuinely different embedding
family. Build deep-image-96-angular from corpus-only training and apply the
same routing and scoring rules and a paired capped control. Investigate any
cross-corpus quality or tail miss by layer, without fitting thresholds to
the ReLAION result. Then qualify a generation-pinned live S3 query path,
including actual GET accounting, latency/QPS, charged RAM and rollover.
The current source-only router scans all page summaries and a substantial
PQ64 row-code fraction at 1M; 10M/100M latency must be measured after a
scalable routing implementation. No vector-count knee or fixed 3-GiB cap
has been inferred.
