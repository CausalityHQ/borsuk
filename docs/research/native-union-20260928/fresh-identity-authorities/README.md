# Closed query-population authority for the next freshness audit

Only small metadata receipts were read. The complete V36 receipt authenticates
its freeze/population metadata and the exact registered1M source parquet. The
population's16 consumed shard paths/bytes/SHA/URIs all match the pinned original
ReLAION manifest. All four original external query roles are named:1000development,
1000validation,1000sealed-holdout and10000performance. No query, embedding, GT or
new/old sealed query body was opened by this read.

The population authority is the actual CLOSED receipt, not a guessed subset from
the planned full-corpus authority. It explicitly records length-prefixed path
hashing and source population selection. Native consumed1M source pseudoqueries
must also be excluded; excluding only the external1000-request file is insufficient.

This is NOT a complete historical identity audit or a new cohort selector/seal.
Before quality access: inventory all prior derivative/pseudoquery authorities,
authenticate their mapping to these source identities, collect only relevant
feature-ID metadata, preregister a wholly separate query-quality-blind cohort,
and prove all prospective IDs are outside indexed1M and previous query populations.
No rejected1000-panel salvage, per-query skipping or freshness relabeling.
The active1M worker uses explicitly consumed dev0–63 and is unaffected.
