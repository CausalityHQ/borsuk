# Shared SOURCE authentication: valid latency/CPU Pareto gain
Frozen40-leaf Cohere1M D1024 cosine k10,1000 identical ordered requests, ABBA inline/offload2/offload2/inline. Both arms use exactsame qualifiednative3e86/sourcea046 and strictreaderec0b. No source/index/scoring/routing/read-policy change. Current shared two-worker scheduler, not predecessor2ef. All actual admission/smoke/resource/checkpoint/drain/cleanup/runtime gates0.

All1000 crossarm and samearm returnedIDs/scorebits/physicalordinals/plans/traces/logicalcharges exactly match canonical: recall97.37%,127811GET/34229466368B. Physical process attempts/consumed payload also equal with zero transport/stream failures. These are not billed/wire metrics.

Inline p90=182.354997/185.809003ms,p95=201.896591/204.450042ms,QPS6.941798/6.847351.
Offload2 p90=173.167229/175.589636ms,p95=192.602065/192.047572ms,QPS7.209343/7.141631.
Both pairs: p90-5.04%/-5.50%,p95-4.60%/-6.07%,QPS+3.85%/+4.30%,querywall-3.71%/-4.12%,SOURCEwall-7.50%/-9.07%. CPU+7.26%/+7.66%; peakRSS about90.0–90.8MiB versus80.8–81.3MiB. p99mixed:254.064227/248.955984ms versus244.413122/272.301146; do not claim everytail improved.

Retain existing shared Offload2 as a measured Q1 latency option and inline as the lower-CPU alternative/default. No automatic production/default change. The model reserves exact2380032 extra library bytes under unchanged2GiBcap; external4CPU/8GiB/swap0 envelope verified. RSS is observed, not a concurrency bound. Higher CPU can hurt saturation throughput/cost; concurrentQPS/lifecycle$/query remain unmeasured.

First query included; repeated1000 requests are not2000 independent samples. AppcacheOFF/backendUNKNOWN,fetch64/retry unchanged. S3Vectors96.17%/p95205.998ms is descriptive context only, not matched cache/retry/normalization/timing conditions or total-dollar proof. Turbopuffer remains unmeasured. No vendorwin claim.

Supporting full1000 userCPU profile is instrumented attribution only: SHA33.39%,blockscore19.74%,8700samples/lost0; rawprofile S3research/native-library-check/20261018/source-hotspot-profile-a0001/evidence.tar SHA f1b81f0014b2ec2cf0e9b58698b1b15216c380302d39104007beb73a8fd77ced. It is not an extra normal timing arm.

Next gate should qualify same-process overlapping queries with bounded downloads/compute/bytes and aggregate CPU/transport accounting, preserving per-query outputs/ordered errors/drain. Current serial runner snapshots cannot simply be summed for concurrent queries. Preserve higher-quality48 option and do not infer its Offload2 tail results from this40-leaf screen.
