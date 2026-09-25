# V185 closed paired PQ score-metric source screen

## Decision

PQ-reconstructed cosine is the stronger generic unit ranker for exact cosine
source truth, but **neither score passes the preregistered 446-unit gate** on
this fresh holdout. Cosine contains 12,738/12,800 truth rows, p05 98, versus
squared-L2 ADC's 12,693/12,800, p05 96. The required threshold was
12,745/12,800 and p05 98. Cosine is no worse at 446 on any holdout query
(12 wins, 116 ties), but remains seven hits short. At 672 units it contains
12,746/12,800, p05 99; that exploratory source-rank point does not override
the 446-unit decision or establish a physical plan.

The width-32 candidate ceiling is 12,751/12,800 on holdout. Of its 49
candidate misses, one query accounts for 28. Only 13 additional truth rows
inside the candidates are below cosine rank 446. This shifts the next generic
question from score metric to candidate reach and tail reliability. Test a
fresh, disjoint candidate-width ladder using the cosine scorer; do not create
a query-ID exception or a fixed vector-count memory knee. Keep budget as an
explicit caller resource/recall tradeoff and require a physical planner and
returned-quality gate before choosing a production default.

## Closed authority

Frozen source commit `fb246786f9380a72c88987c0c4dba65f44278594`, one
Causality Spot `c7i.12xlarge` worker `i-0fd80521dc5df3bb9`, terminated
after the complete terminal. Attempt:
`s3://borsuk-bench-453182569524-euc1/research/v185-pq-metric-source/fb246786f9380a72c88987c0c4dba65f44278594/runs/a0001/`.
Terminal SHA256 `623ab3e46b027d505ac52b16ec0fe2af74c67dc0ff3f05f9b13a184572dbab1f`;
summary SHA256 `dfe56169e59cbb3772723d25df638f2a92ae3a34a964d2dd6f82888b51bc5d07`;
features SHA256 `561e92b6298eba0ff426bc6ec7196021a0b0e8b3f0ce31eecd7cd2773f6482b7`.
The launcher rehashed every terminal artifact and confirmed termination. An
independent closed-artifact replay rehashed the terminal-listed files, the
GT-blind feature seal and source labels; matched all 256 query identities and
candidate sets between score arms; and reproduced every aggregate count.
Both complete GT-blind rankings were sealed to S3 before exact truth opened.

## Measured source-only evidence

Dataset: **ReLAION-1M D768 source pseudoquery SHA ranks 1409–1664**. Fit
is ranks 1409–1536 and holdout 1537–1664, 128 queries each. Exact float64
cosine GT100 excludes each source query row. Counts are true neighbors in
ranked candidate units, **not** returned Recall@100 or S3 query latency.

| Split | Width-32 candidate / 12,800 | Metric | Top 128 | Top 256 | Top 446 | Top 672 | Top 1,344 | p05 at 446 |
|---|---:|---|---:|---:|---:|---:|---:|---:|
| Fit | 12,777 | Squared L2 | 12,432 | 12,691 | 12,754 | 12,771 | 12,777 | 97 |
| Fit | 12,777 | Cosine | 12,627 | 12,756 | 12,775 | 12,777 | 12,777 | 98 |
| Holdout | 12,751 | Squared L2 | 12,303 | 12,618 | 12,693 | 12,724 | 12,743 | 96 |
| Holdout | 12,751 | Cosine | 12,521 | 12,700 | 12,738 | 12,746 | 12,751 | 98 |

The strongest existing BORSUK comparator is **V155 used ReLAION-1M D768
validation-1000** actual returned exact-source Recall@100 99,567/100,000,
p05 98, at 11,134,007,040 planned bytes and 22,126 GETs. It is unpaired
with this source panel. No paired S3 Vectors or Turbopuffer measurement exists
at this revision. V185 measured neither a contiguous GET plan nor charged
serving RAM, and cannot be called a product or competitor win.

Spot preparation took 52.65 seconds and peaked at 9,340,672 KiB RSS;
source-truth evaluation took 31.04 seconds and peaked at 9,511,584 KiB RSS.
These are offline experiment-process measurements, not serving latency or
serving memory. No local full suite ran during devbox swap pressure.
