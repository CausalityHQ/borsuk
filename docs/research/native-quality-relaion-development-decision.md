# ReLAION actual development gate

Frozen root c4c2a37be1fd2065980f2e23f21b167384786a3f2317e848ca6f8a04cbec0506;
ReLAION first100k D768 cosine k100, development0–63. Shared scorer reproduces
all64 prior CoHere development samples/metrics exactly. No dataset-specific
algorithm parameters or scoring are used.

| Measured offline layer | Mean R@100 percent / GT hits | p05 hits |
| --- | ---: | ---: |
| Fetched coverage |99.625000|98|
| Returned SQ8 |99.234375|98|
| Paired full-plane SQ8 |99.515625|99|

Deficit0.28125pp, maximum32GET/16,773,120bytes. **Development GO**;
full paired validation, cold HTTP, release, scale and vendor gates stay open.
Mean loss before scoring0.375hits; fetched-to-returned loss0.390625hits.
API output does not separate nomination from physical selection. This is
actual quality evidence, distinct from the earlier query-informed oracle.

Original Spark jobs both exit0: plans10.67s/51,188KiB; score7.38s/225,872KiB.
Zero swaps and zero S3 data reads. Whole debug/offline diagnostics are not
service latency percentiles, product RSS/QPS or vendor measurements.

Next gate: unchanged corrected method on validation256–319 as early screen,
then744-query validation256–999 if it survives. Same98mean/p05 95/0.5pp
paired-flat deficit/32GET/16MiB limits. No tuning or scale promotion.
