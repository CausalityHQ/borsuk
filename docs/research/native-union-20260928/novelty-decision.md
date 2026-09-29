# Prospective ReLAION cohort fails query novelty

**Do not use the sealed ReLAION1000 panel as unused qualification evidence.**
At least one prospective query was already a source pseudoquery in a consumed
method holdout. This is a query-identity gate failure, not an ANN quality failure;
no new sealed vector, request or GT body was opened, and no ANN quality ran.
Construction/source parity/exact f64 GT seal remain valid historical evidence.

Verified witness: prospective zero-based ordinal **903**, original source-parquet
row **100903**, source feature ID **83606072**, was V189 source pseudoquery
ordinal **2578** (SHA rank2579, inside consumed holdout ranks2561–2688). Both use
the EXACT same registered ReLAION1M source parquet SHA
`2796b579f37afe99ca4aff57e282335a6a79ad30596645957d26326a0560cf86`,1458450077B.
The prospective100k index excludes this row, while old V189 used1M and excluded
self from source truth; different index sizes/GT do not make its query identity new.

Metadata-only audit authenticated closed V189 terminal
`ec2a845a4471afacf48ec8e78cb16fdafff75bb0852a7323e67d9ea9db2f8605`,
all256 feature records via15327489B/SHA
`7eb4833c76675534cde41330c5939599ab70d7871ac27cb365d62cdb840c3fbe`,
and immutable original source archive
`0e34f57af127f078bf32c76fb818e18421f8e37b650b238a2c03cb24823e834a`.
Read original five source files to verify registered source identity and
parquet-row preservation through `_source_arrays`/`source_arrays`/`_inputs` to
`vectors[source_row]`. Old feature IDs are not treated as new row ordinals.
Saved terminal, prepare seal, source-code hashes and witness in `fresh-novelty/`;
`scripts/audit_native_union_fresh_novelty.py` reproduces this closed metadata check.
It never fetches new `/sealed/` data. No new compute instance or query/scoring job.

This is a **minimum one-overlap witness**, not a complete all-history audit.
Earlier inspected panels' zero overlap is insufficient to establish novelty.
No fresh recall/latency/QPS exists for either corpus. CoHere's prospective query
novelty is still uncertified; its source/GT construction result remains unchanged.

Keep the entire original sealed panel immutable. Do not remove ordinal903 and
silently rename999 queries as fresh, tune on their GT, or use this panel for a
qualification claim. After the existing incomingHTTP development gate closes,
preregister an entirely new cohort and audit query identities (including previous
source pseudoqueries and original external query authorities) BEFORE quality
access. Use generic, query-quality-blind identity selection; no architecture or
threshold change is justified by this metadata failure. Production/source/scorer
and BOTH-vendor requirements remain mandatory. No operator decision is needed.

Current HTTP gate uses explicitly consumed development0–63 and is unaffected.
Full product goal stays active. Future fresh qualification remains a separate gate.
