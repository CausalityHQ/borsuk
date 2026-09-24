# V185 paired PQ score-metric source screen preregistration

V184 proved that V164 whole-unit geometry can meet the source quality and
resource envelope if optional truth-bearing units are known. V182 fit data
showed that squared-L2 PQ ranking needed hundreds of units to find a median
36 truth-bearing units per query. The exact source target is cosine. V185
tests a generic score-representation change: rank the same width-32
candidate units by the best row's **PQ-reconstructed cosine** instead of
the best row's squared-L2 ADC score. No dataset ID, query exception or
vector-count knee is part of either scorer.

Use a new disjoint **ReLAION-1M D768 source pseudoquery SHA ranks
1409–1664** panel, fit ranks 1409–1536 and holdout 1537–1664 (128 queries
each). Freeze V115 nominees, V164 row order, 32-row SQ8 mandatory units,
the width-32 candidate union, and both complete GT-blind unit rankings to
S3 before exact source truth is opened. Exclude the source query row from
both PQ rank minima and exact float64 cosine GT100. Both arms use identical
candidate sets and thus have identical candidate ceilings.

For each arm report exact-source truth contained in top 128, 256, 446, 672
and 1,344 PQ-ranked units and per-query p05, split by fit/holdout; report
candidate ceiling and offline scoring resources. The 446-unit point matches
the complete-unit floor of V155's mean planned bytes per query; 672/1,344
are exploratory 16/32-MiB allowances. These are **unit-rank screens**, not
contiguous GET plans, production memory limits or returned Recall@100.

The metric decision is paired on the fresh holdout. Advance reconstructed
cosine to a physical planner gate only if its top-446 containment is at
least 12,745/12,800 with p05 at least 98 and is no worse than squared L2
at the same allowance. If cosine fails but squared L2 passes, retain squared
L2. If both fail the 446-unit gate, neither raw score alone is selective
enough at that resource point; develop a source-calibrated utility or new
candidate representation rather than fitting a dataset/query exception.
The 12,745 threshold transfers V155's 99,567/100,000 **unpaired used
validation** rate to 128 source queries, rounded up. A pass is only a
source screen and still needs mandatory-cover physical plans, a fresh
sealed panel, paired used ReLAION-1M validation-1000 returned recall, S3
latency and charged serving RAM.

Run one immutable Causality Spot cell, seal both rank lists before truth,
rehash complete artifacts and terminate the instance immediately.
