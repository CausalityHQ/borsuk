# Retained native quality and cost points, 2026-10-20

Retain SOURCE cap125% as the faster point and cap150% as the lower-GET point. A later uniform SQ8 budget32->33 change restores recorded aggregate recall9902/10000. Existing zero-gap canonical coalescing then removes0.528GET/query with identical outputs/bytes. These are research points on one frozen fixture, not production defaults or vendor wins.

Exact candidate18e4da5f1b3a094ece1a0f3f7116b1cb516e027d (cap commitdcddf019e756d832028e77f4af4abcc787976a14) descends fromd542abc5cb83418607a18a55ccbab484da987ae9 and50f76f39335bb2b4605829b67ad4c12a1f698e76; remains unmerged. Frozen native ELFbe339d8efc8aee2acec2be8b7eec25e446dbc8985b9a939e74b3e23ebc5de7cb. SOURCE cap is original+floor(original/4); original logical ranges and bridge bytes remain scored, new gaps authenticate but never rank. Separate commit moves unused fusion state reservation from Inline to flat Offload2; native red/green proved Inline exact-parent admission and Offload2 full-model-minus-one rejection.

Required native gates:20 affected tests(5fusion/3overlap/11auth/1record), workspace Clippy correctness/suspicious, complete workspace test compilation and release all0, original9ccba3c6-7a0d-4c77-8d94-90090f3d9f99. Full workspace tests NOT executed; unchanged10B-vs17B authority fixture failure remains. Full library model617817198B is identical in matched arms. The reporting-only SOURCE-authentication component field omits the moved21504B, but it is not used for admission; preserve and disclose historical fields. Cancellation multi-mask test gap remains before product qualification.

## SOURCE125 vs150 native matched test

Same-host Spot i-0665a9359195a75a9 m7i.2xlarge eu-central-1b;4CPU/8GiB external/2GiB modeled,swap0,activequery1,applicationcache0,transport64,Offload2. A=150%;B=125%. Original14a9d656-e1d9-4e6f-adac-9bc2ffd152bf all4 terminal0 ended18:42:58UTC; terminal cells synced before next, no rerun. Real first8 admission and separatecanary passed before measurement. All4 seals, all1000 IDs/scorebits/plans/logicaltraces/shortlist/nonSOURCEcharges match; recall9901/10000; all native GET deltas successful206 with no extra attempts.

| Arm | mean ms | p95 ms | p99 ms | cohort QPS | process CPU s | peak bytes |
|---|---:|---:|---:|---:|---:|---:|
| A1 | 179.750 | 333.922 | 400.903 | 5.381 | 121.361 | 161607680 |
| B1 | 165.571 | 245.513 | 323.401 | 5.824 | 117.243 | 145387520 |
| B2 | 165.336 | 240.734 | 358.701 | 5.831 | 119.443 | 148430848 |
| A2 | 176.767 | 332.577 | 407.693 | 5.467 | 125.422 | 144777216 |

125% improves mean query time~7.2% and CPU~4%, lowers physical bytes~11.8%, but increases total GET~15.2%. Both neighboring p95 comparisons improve on this same-host ABBA. Unchanged SQ8 means/p95 remain similar across these cells, unlike the earlier drifting source50-vs150 experiment. One ABBA has no provider-cache control or cross-workload generalization. Peak-memory figures vary; no general RSS win is inferred. Source-only requests65.125->82.054; bytes31.9257->26.6449MB. Choose a Pareto point using actual request/transfer/compute prices, not GET count alone.

## Uniform SQ8 budget33 quality gain

Existing native runner validates explicit SQ8 GET budgets1..256. Only sq8_get_budget32->33 changed; all source/byte/concurrency/cache limits stayed fixed. Real first8 plus separatecanary passed (a659ab0e). Original full1000 3844d76d-7efe-43ca-b746-5307d2e87e85 and reducerfa9e1d2e terminal0. Recall9901->9902/10000, only query886 changed; common-IDscorebits and every SOURCE charge remain exact. This was preregistered before results, with no ID exception. It matches recorded incumbent aggregate recall; it is not an incumbent paired timing comparison. Full1000 canonical-f64 truth equivalence beyond prior first8 diagnostic remains unestablished.

{
  "decision": "RETAIN_QUALITY_PARETO",
  "baseline_recall": [
    9901,
    10000
  ],
  "candidate_recall": [
    9902,
    10000
  ],
  "unchanged_common_ID_scorebits": true,
  "changed_output_queries": [
    886
  ],
  "unchanged_SOURCE_charges": true,
  "source_gets_mean": 82.054,
  "source_bytes_mean": 26644893.696,
  "sq8_gets_mean": 31.229,
  "sq8_bytes_mean": 12319714.176,
  "total_gets_mean": 129.283,
  "total_bytes_mean": 39030271.872,
  "process_cpu_s": 114.052160623,
  "process_wall_s": 170.472946119,
  "observed_peak_bytes": 143474688,
  "extra_attempts": 0,
  "scratch_remaining": 0,
  "timing_scope": "single qualityrun, not matched timing win",
  "original_ssm": "3844d76d-7efe-43ca-b746-5307d2e87e85"
}

## Existing canonical zero-gap coalescing

Sealed trace geometry found314 queries with adjacent selected canonical records, predicting0.528 fewer GET/query at the unchanged65664B cap. Only serving.coalesce false->true changed. Native first8 plus separatecanary1287f62a passed. Original full1000 ece58aeb-5ee9-4858-9905-1f40f1a40341 and reducerda190339 terminal0: all1000 IDs/scorebits/ordinals/plans/logicaltraces/shortlist exact, SOURCE/SQ8charges and all bytes unchanged, recall9902. Canonical16->15.472GET/query exactly matches geometry. Retain this measured request reduction. Timing is a single quality/accounting run: do not attach budget32 matched p95 figures to this budget33/coalesced configuration.

{
  "decision": "RETAIN_EXACT_ZERO_GAP_GET_REDUCTION_WITH_9902_RECALL",
  "queries": 1000,
  "recall": [
    9902,
    10000
  ],
  "exact_IDs_scorebits_plans_trace_shortlist": true,
  "unchanged_SOURCE_SQ8_and_allbytes": true,
  "canonical_GET_mean": [
    16,
    15.472
  ],
  "total_GET_mean": [
    129.283,
    128.755
  ],
  "total_bytes_mean": 39030271.872,
  "source_gets_mean": 82.054,
  "sq8_gets_mean": 31.229,
  "process_cpu_s": 110.567921564,
  "process_wall_s": 169.755370549,
  "observed_peak_bytes": 144457728,
  "extra_attempts": 0,
  "scratch_remaining": 0,
  "scope": "single quality/accountingrun; NOT matched latency claim for budget33/coalescetrue",
  "source_commit": "18e4da5f1b3a094ece1a0f3f7116b1cb516e027d",
  "original_ssm": "ece58aeb-5ee9-4858-9905-1f40f1a40341"
}

## Immutable receipts and next hypothesis

Bucket borsuk-bench-453182569524-euc1 prefix research/semantic-router/20261020/:
- actual-source-fusion-cap125-root-a0001/qualification-admission.tar.gz SHAa6ac735d0f12aba50239126c7a3245b26b9b99099477f6c056cfacee6a8828cd includes20test qualification, source/config hashes, exact candidate commit pack, frozenELF and realadmission/canary.
- actual-source-fusion-cap125-root-a0001/full1000-evidence.tar.gz SHA2e2d9e82c83d82e8197bb1a190163e5144dfffd40777d133773e81bd82e65c37.
- actual-source-fusion-cap125-vs150-paired1000-root-a0001/{A1,B1,B2,A2}.terminal.tar.gz and reconciliation.tar.gz SHAc8f4b4efc412e8d482cf286c046c93ee71bb65f5b898219351c1abc7a5c9d3f1. Strict reducer934596cf and modelcheckf144fa3c terminal0.
- centered-sq8-budget33-root-a0001/full1000-evidence.tar.gz SHAeaf9efe23c58b55991ee9549ce563d06077f4afe21196223330bb5ca986ab432.
- canonical-zero-gap-coalesce-root-a0001/full1000-evidence.tar.gz SHA4c37108f9218c5e9f536cdbc08222ed1a726d3ec88fdc0f2a382e0dec59efd7c; reconciliation.tar.gz holds the exact final comparison and geometry.

Consultation7a9746dad2db4a58 completed once via Codex fallback after Fable/Opus limits, capped20USD/900s. Facts-only result proposes mask-preserving physical SQ8fusion, conditional on same-object locality and authority. Its suggested25%GET-saving target is an unmeasured proposal, not an admission gate. Root commissioned a fresh read-only source/eligibility audit, no implementation or paid experiment authorized by the consultation. Local automatic consultation worktree metadata was not experiment source authority.

## Official comparison boundaries

No vendor service was queried here. [AWS S3 pricing](https://aws.amazon.com/s3/pricing/) lists request, processed-index-data and returned-data components; do not use US-East examples as Frankfurt quotes. [S3 Vectors documentation](https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-vectors.html) describes strong writes and managed optimization. [Turbopuffer architecture](https://turbopuffer.com/docs/architecture) distinguishes object-storage cold reads from subsequent NVMe-cached queries and default strong consistency; its published latencies are not matched measurements. [Turbopuffer pricing](https://turbopuffer.com/pricing) includes minimum usage and storage/write/query components. Accessed2026-10-20; equivalent source/query bytes, recall, cache/consistency, region and full lifecycle operations remain required before any vendor comparison.
