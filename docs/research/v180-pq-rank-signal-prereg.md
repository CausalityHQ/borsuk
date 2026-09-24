# V180 source PQ rank signal preregistration

## Decision

Test whether direct PQ64 ADC minimum-row scores rank optional physical units
well enough to justify a source-calibrated interval planner. The new panel is
ReLAION-1M D768 source pseudoquery SHA ranks 769–896, disjoint from V177/V178.
Fit uses ranks 769–832 (64 queries); holdout uses 833–896 (64 queries). The
source query row is excluded from both exact truth and the PQ unit minimum.
No external validation queries or GT are read in this gate.

The 512 frozen V115 nominees, V164 relaid order, 32-row SQ8 units and width-8
candidate neighborhood are unchanged. Prepare seals each roster and full
GT-blind PQ unit ranking to S3 before fit source labels are computed. Fit
labels train one weighted isotonic rank-to-expected-source-hits curve. Freeze
that curve and its fit labels to S3 before holdout source labels are computed.
The model uses rank and unit geometry only. It has no dataset ID, vector-count
threshold, query exception, or learned memory cap.

The screen takes the first 672 PQ-ranked candidate units, ignoring GET
contiguity and mandatory primary units. This is a **score information** test
at the number of complete 32-row units allowed by the old 16-MiB comparator;
it is not a feasible serving plan or a production memory limit. Predeclared
advance threshold: holdout exact-source truth contained in those units at
least 6,373/6,400 and per-query p05 at least 98/100. That count transfers
V155's 99,567/100,000 used-validation rate to 64 source queries and rounds
up; the panels are unpaired and source pseudoqueries may be easier. Report
candidate-universe coverage, rank loss, fit/holdout calibration error and
ranking resource/time evidence separately. A pass licenses a subsequent
GET/byte-constrained GT-blind source planner gate, then paired used ReLAION-1M
validation-1000 returned Recall@100 against V155. A failure kills this
rank-only PQ utility model and requires a new representation or score feature,
not a fitted special case for the failed query.

Run one Causality Spot cell with an immutable source archive, frozen input
digests, S3 prepare/fit seals, terminal artifact hashes and immediate instance
termination. Interrupted cells are discarded and restarted as a new attempt.
