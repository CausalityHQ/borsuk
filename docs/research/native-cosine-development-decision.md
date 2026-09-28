# Corrected cosine development decision

CoHere first100k, D768 cosine, k100; frozen development queries0–63.
Same authenticated SQ8 artifact/layout/budgets; corrected query normalization.
All values below are measured offline against exact GT100, not service metrics.

| Layer | Mean GT100 hits / R@100 percent | p05 hits |
| --- | ---: | ---: |
| Corrected API fetched rows |99.671875|98|
| Corrected API returned SQ8 top100 |99.000000|97|
| Paired full-plane SQ8 top100 |99.234375|98|

Mean deficit to paired flat:0.234375pp. Maximum32GET and16,773,120bytes.
**GO for qualification preparation**, not release or scale. Mean loss before
final scoring is0.328125hits, final SQ8 ordering loses0.671875hits. The current
API plan output cannot separate graph nomination from fetch selection.
Historical V296 used raw query magnitude and is separately versioned evidence;
no cross-method matched quality claim follows.

The scoring diagnostic completed exit0 in12.18s,179,912KiB peak RSS,zero swaps,
zero S3 reads. This is whole offline diagnostic elapsed time, not per-query
latency, RSS/QPS qualification or vendor performance.

Next single quality gate: freeze corrected-method paired ReLAION/CoHere
validation256–999 (mean>=98,p05>=95,paired flat deficit<=0.5pp,32GET/16MiB).
Use a preregistered short development/early kill screen before expensive work;
never certify full qualification from a short screen. Intended remote execution
access is still missing for heavy ReLAION preparation. No paid job started.
