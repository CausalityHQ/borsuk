# Corrected cosine full CoHere validation

Frozen CoHere first100k, D768 cosine k100; validation256–999,744 queries.
Same immutable artifact/requests/GT/calibration and budgets; no tuning.

| Measured offline layer | Mean GT100 hits / R@100 percent | p05 hits |
| --- | ---: | ---: |
| Fetched coverage |99.737903|98|
| Returned SQ8 |99.131720|98|
| Paired full-plane SQ8 |99.315860|98|

Paired deficit0.184140pp. Maximum32GET/16,773,120bytes. All frozen
CoHere full-split thresholds pass. **GO for this dataset's offline quality;
HOLD paired qualification, release, scale and vendor claims.**

Mean loss before scoring0.262097hits; fetched-to-returned loss0.606183hits.
Nomination and fetch selection cannot be separated by this API output.
Plan original session97487 exit0:170.93s/48,376KiB peak RSS. Score original
session17888 exit0:131.32s/235,284KiB. Zero swaps and zero S3 reads. These
whole debug/offline jobs are not latency percentiles, production RAM or QPS.

Next single quality gate: ReLAION100k on the same corrected method and frozen
source-only layout, with preregistered development/early kill followed by full
validation256–999 at the unchanged thresholds. Authenticated SSH to Spark
now works; preparation can move there rather than remain blocked locally.
Spark offers20 logical CPUs,~120GiB available RAM; no competing listed
Cargo/Python/BORSUK process was found. UV exists; Cargo is not in PATH.
No paid cloud instance was launched. Transfer only pinned inputs/code; preserve
remote owners/jobs and collect each original job's terminal marker/status.
