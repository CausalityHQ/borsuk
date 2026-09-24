# V182 wide candidate PQ rank preregistration

V181 showed that source truth is usually reachable through a wider physical
neighborhood, while V180's width-8 fit/holdout tail varied materially. This
gate asks whether PQ64 minimum-row scores can select truth-bearing units from
a **width-32** neighborhood at finite unit allowances. The width is generic
and fixed before this panel; it is not chosen from the panel's truth.

Use ReLAION-1M D768 source pseudoquery SHA ranks 1153–1408, disjoint from
V177/V178/V180/V181. Fit ranks 1153–1280 and holdout 1281–1408, 128 queries
each. Freeze 512 V115 nominees, V164 row order, SQ8 primary units and the
GT-blind PQ ranking for the full width-32 candidate set to S3 before opening
fit truth. Exclude the query source row from the PQ unit minimum and exact
cosine truth. Fit one rank-to-expected-hits curve using only fit labels;
seal it before holdout labels.

Score the first 672 and 1,344 PQ-ranked units as two exploratory unit
allowances. They correspond to about 16 and 32 MiB of complete SQ8 units,
respectively, but ignore GET contiguity, mandatory primary cover and actual
object bytes. They are **not** production memory limits. Predeclared
source-only holdout screen for each arm: at least 12,745/12,800 exact source
truth positions and p05 at least 98. Select the smaller passing allowance.
Report width-32 candidate ceiling, loss within the candidate set at each
allowance, fit/holdout calibration error and offline process resources. If
neither arm passes, decide from the separated losses whether to revise
candidate generation or PQ scoring, without a query-ID exception. A pass
licenses a later GT-blind mandatory-cover interval planner and paired used
ReLAION-1M validation-1000 returned Recall@100 gate against V155. Source
pseudoqueries are unpaired and may be easier than used validation queries.

Use one immutable Causality Spot cell; seal features, then fit model, then
holdout labels in S3; rehash all terminal artifacts; terminate compute.
