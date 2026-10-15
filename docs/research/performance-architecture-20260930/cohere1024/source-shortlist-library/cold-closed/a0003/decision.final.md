# Closed v9 cold comparison: retain the trade-off, no default adoption

The native campaign is valid and complete. Independent evidence review 3ca0bf7b49854c50 completed0: qualified acceptance of valid cold ABBA and the trade-off, no default adoption. This is not a release or vendor-win claim.

## Frozen comparison

Cohere Wikipedia embed-multilingual-v3, 1,000,000 corpus rows, D1024, cosine, k10, the same frozen 1,000 queries/truth; sequential A1/B1/B2/A2, fresh process per cell, application cache off, fetch width64. Common m6a.large Spot host, CPU0/100%, 2GiB stage cap, swap0, pids128. Server cache eviction is uncontrolled. Native query wall includes discovery, SOURCE, planning and SQ8; startup and client RPC ingress are excluded. Sequential QPS is not concurrent service throughput.

Control uses qualified v8 ELF 59a22eed2e7d3cb8142e35108171cdcd8c27ec83c8968ba307cd9dc092c78083; candidate uses qualified v9 ELF 912f23d34dd175f365aab2797896da2c45306bf9dd4faf7d44723c8bdce50995. Helper ELF 0845bddd426a6e7b9a56317f73ff92fdfa15b14b2f1d109cf62d7a77bbb54fbd validates every complete result. This compares two complete architectures: proof rows256/32, shortlist implementation and SQ8 envelopes differ. It does not isolate one causal change.

## Native aggregate results

| Metric | A1 | B1 | B2 | A2 |
|---|---:|---:|---:|---:|
| Recall hits /10000 |9780|9786|9786|9780|
| Underfilled queries |0|0|0|0|
| p50 ms |177.589|164.823|165.193|179.146|
| p90 ms |206.311|240.420|235.887|202.136|
| p95 ms |268.234|309.778|308.932|228.028|
| p99 ms |360.917|388.889|391.541|349.499|
| Sequential QPS |5.352|5.470|5.510|5.327|

Each candidate cell gains six hits (0.06 percentage points), reduces SQ8 bytes31.2% and total SOURCE+SQ8 bytes11.8%, improves median and sequential QPS2.2–3.4%, and worsens both paired p90/p95. The preregistered tail screen fails. Preserve the candidate as a measured quality/bytes/median trade-off; do not change production defaults. Two repetitions per arm do not establish statistical significance. Control p95 itself varies about15%; no percentile arithmetic is used to infer per-query causality.

SOURCE charges remain107882 GET /27428966400 bytes per cell. SQ8 changes29903 GET /16708608000 bytes to28358 GET /11497411968 bytes. These are logical charges. SDK attempts/payload counters include startup and are separate from wire or billed quantities.

## Next discriminator

Native SQ8 median falls from61–62ms to47ms, but SQ8 p90 rises from68ms to101ms and p95 from74–80ms to108–109ms in both candidate repeats. Discovery/planning are essentially unchanged; SOURCE B1 has an additional p95 spike. CPU throttling counters are zero. These observations narrow the next diagnostic to SQ8 completion/straggler and host timing, but do not identify network, authentication, decoding or scoring as the cause. Use a bounded traced panel before any production rewrite or repeat full campaign; retain current quality/bytes gains as evidence.

## Closure and limits

Original SSM e184d237-e549-4ac5-b4d4-68b3fb400342 succeeded0. All16 native/helper stages closed0,32 ordered START/END rows, both control canaries and real CLI object cleanup passed. Archive30370316B SHA25629f2c977247d76e1b475bb13520fc069da310556a45ff53a3a3aa0f4c5809f5b is retained at s3://borsuk-bench-453182569524-euc1/research/semantic-router/20261015/source-shortlist-v9-cold-a0003/terminal/evidence.tar.gz. Root authenticated705 payload files without locally parsing scientific JSONL. Instance i-05a7e2ec05095f8c4 terminated, volume vol-0fee6b51e73cfda31 absent, external timer inactive.

Historical a0001/a0002 setup failures remain separately closed; neither ran ANN measurements or rejected the algorithm. S3 Vectors and Turbopuffer have not been reproduced under this exact protocol/quality, so no matched competitor win follows. Lifecycle source audit7309f959e7614cf6 additionally records GC progress, profile-transition and reader-coordination release limits; this cold experiment does not qualify those capabilities.
