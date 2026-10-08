# Cold membership discovery: verified 100k comparison

**Select resident authenticated membership discovery for the next cold baseline.** Both counterbalanced pairs pass the preregistered ≥5% p90/p95 improvement and serial-QPS nonregression screens. This is a BORSUK mechanism win on this workload, not an overall vendor or lifecycle-cost win.

## Workload and protocol

Cohere Wikipedia multilingual-v3, fixed100k corpus,1000 ordered queries,D1024,cosine,k10, revision `ade45fb52bd549f5e8c065636fe4160a43c2af36`. CPU1,512MiB,swap0,pids256,one active query,width32. Four fresh processes/clients A1(control),B1(membership),B2(membership),A2(control). Application source/payload caches OFF; backend cache state uncontrolled. No guaranteed backend-cold claim.

Source identity and release binaries were independently qualified; real1000-query admission and separate disposable canary passed before the frozen run. Both producers use rustc1.98.0/cargo1.98.0 and the same Cargo.lock. Native source differences are confined to the two membership modules, Rust reducer and scorer fixture.

## Results

| Run | p50 ms | p90 ms | p95 ms | p99 ms | Serial QPS | Recall@10 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| A1 control |128.212219|152.030079|164.467861|187.988557|7.522306|97.23%|
| B1 membership |93.988107|116.434577|123.879512|154.573053|10.087234|97.23%|
| B2 membership |93.252356|118.561257|126.885883|145.677567|10.119562|97.23%|
| A2 control |127.595391|152.508454|164.470284|187.885721|7.518168|97.23%|

All4000queries complete, no underfill or failed GETs. Rust verifies identical ordered IDs/score bits/recall, nomination traces and source/SQ8 charges. Candidate router GET/byte/HEAD and leaf-inflight charges are zero. Each candidate removes16000GETs/1588098204 verified bytes per1000queries. Source27343GETs/8971573248B and SQ824600GETs/16699756416B remain unchanged.

The native screen reports p95 ratios0.753214/0.771482 and QPS ratios1.340976/1.346014. Discovery totals drop from34.60–34.64s to0.414s per1000queries. Source remains40.01–40.10s, SQ8 fetch/rank52.30–52.77s, planning5.91–5.98s. These stage means are not independently additive tail percentiles or network-only costs.

## Saved vendor references

| Reference | Population | Recall@10 | p50 ms | p90 ms | p95 ms | p99 ms | Serial QPS |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| BORSUK membership B1/B2 |100k|97.23%|93.25–93.99|116.43–118.56|123.88–126.89|145.68–154.57|10.09–10.12|
| S3 Vectors saved first pass |100k|96.56%|60.352389|118.479276|149.056020|215.428843|13.754141|
| Turbopuffer published cache-disabled |10M|undisclosed|874|1214|undisclosed|1686|8QPS offered load, not throughput capacity|

S3 uses the same frozen raw corpus/query/truth bytes; the preserved reference is a separate first-pass measurement after idle, not a simultaneously paired service run or proven backend-cold state. Its serial QPS is reciprocal query time; whole-pass QPS12.891559 is separately recorded. BORSUK now has lower p95/p99 and higher recall on these measurements, but higher p50 and lower serial QPS. No overall S3 win. Do not rerun the reference.

Turbopuffer sources: https://turbopuffer.com/ and https://turbopuffer.com/v3 (recorded first-party cold10M Cohere-family numbers). Exact selected IDs/query split and recall are undisclosed; different population/region/client/load conditions prohibit a matched win claim. Scale the same corpus later; do not substitute a new dataset.

## Closure and limits

Original watcher28454 closed0; same instance `i-0858be8fbbafb673a` terminated and waited before collection. All stage/native/reduction exits0, actual caps/pins/scratch and order independently verified. Cgroup peak87719936B,swap/OOM0; candidate process peaks43347968/52310016B. Budget reservation$1.50 is a cap, not actual spend. Billed requests/bytes and total lifecycle dollars remain unknown; serial QPS is not concurrent service capacity.

Archive4839309B SHA256 `99c1efb9922fa53e4dd9ea64e9183ad0ba1e21e0e42dd49171776249a6fd4c06` at `s3://borsuk-bench-453182569524-euc1/research/semantic-router/20261008/cold-membership-abba-a0001/evidence.tar.gz`. Full four JSONL files are preserved there and pinned in artifacts.sha256/native report; no ANN rerun was needed.

The Rust reducer intentionally leaves external provenance/order/resource qualification false; independent-verification.json supplies those root checks. Production physical ranges are not emitted: parity evidence is the actual native exact request/range/ETag fixtures and unchanged source/SQ8 planner, not a runtime range trace. Historical fixture failures and original width16/32 INVALID attempt remain immutable.

## Next bounded work

Cold only. Profile CPU versus I/O inside source fetch/scoring and SQ8 fetch/rank before choosing another Rust change; these are the remaining measured stages. The current target still includes S3 serialQPS/p50, followed by same-corpus10M validation against published Turbopuffer context. Defer caches,warm measurements,new datasets and100B.
