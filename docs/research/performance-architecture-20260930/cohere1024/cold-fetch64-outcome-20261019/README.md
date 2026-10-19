# Cold fetch concurrency: useful Pareto point, tail gate fails

Generic Rust library; Cohere Wikipedia 1M D1024 cosine k10 is an evaluation fixture. Same qualified native source/ELFs, leaf64, Q1, Offload2, FP32 shortlist16, coalescing off, source cache0. Only fetch_parallelism changes128 (A) to64 (B), including its coupled configured runtime/SQ8 admission ceilings; SQ8 budget32 makes the latter nonlimiting. Network occupancy was not measured. No Rust code/default change.

## Decisions

| Closed attempt | Recall | Pooled query wall B/A | Query CPU B/A | Paired p95 B/A | Decision |
| --- | --- | --- | --- | --- | --- |
| Fixed68 screen | 674/680 every cell | 0.766427 | 1.018431 | 0.684786 / 0.911761 | Valid negative on3/11 strict gates; retain latency/CPU/RAM Pareto |
| Frozen1000 confirmation | 9902/10000 every cell | 0.858000 | 1.020521 | 0.731639 / 1.074043 | Valid negative on3/11 strict gates; retain Pareto, no default promotion |

Full1000 reduces pooled query wall14.20%, external process wall13.76%, pooledp9017.81%, but queryCPU rises2.05%. Second-pair SOURCEp95 worsens6.05% and end-to-endp95 worsens7.40%; B2p99871.40ms versus A2557.79ms. Never exclude B2. First-pair improvements do not establish repeatable tail gains. All11 preregistered gates remain unchanged; failures are second p95, second SOURCEp95 and queryCPU.

All4020 returned ID/f64 score bits/physical ordinals/plans/trace/shortlists/charges exactly match the frozen own-arm reference. Every measured cell has169024 logical and physical GETs, 51077441536 verified bytes, zero retries/transport/stream failures. Modeled memory471434981->471043301 bytes; measured RSS per cell is recorded separately, not equated with the conservative model. Library2GiB/host8GiB are this campaign's envelopes, not universal product limits.

Same fresh m6i.2xlarge Spot i-04e58bcab75727bd0/Intel8375C for full ABBA. Real8-query admission and separate2-query real-S3 canaries pass BOTH arms before measurement. Application SOURCE cache is off and each cell uses a fresh process; provider cache eviction is unproven. This is frozen1000 regression evidence, not an untouched holdout or generalization claim. Full absolute numbers are in stage-summary.json.

Historical leaf40/SQ8 full1000 favored128 under another pipeline; that result remains valid. Current leaf64/FP32/SOURCE-score-overlap result is conditional. Keep the qualified128 incumbent and expose64 only as an explicit workload Pareto option; do not sweep more concurrency values or repeat this full experiment to find a passing sample.

No S3 Vectors or Turbopuffer win, matched competitor parity, production readiness or lifecycle/scale qualification is claimed. Previous vendor comparisons have metric/normalization/cache/protocol limitations; Turbopuffer remains unmeasured. Generic routing/scoring/storage code is unchanged.

## Verification and delivery

Native pipeline0 and all8 native+reader cells0; original SSMf9d08047-8ecb-41ed-9170-a616d4caf8c1, unit invocationaa47cf4a6e0f41f390bc6a5b08697865. Independent closed verification4e373966-822a-4069-81ce-4931081ec438 passes all4020 rows, metrics/11checks and10 checksumHEADs. Root verification SHAd56b4029e7aa3889c7c77b116965a8b858a01ff138c9b9e073e1441306574205. Initial post-terminal observation guard ee7eff30 failed before reading data because systemd garbage-collected the transient unit's InvocationID; original terminal receipts/journal were independently read, preserved, and verification alone resumed. Native work was not repeated.

Exact qualified crates tree ab73789e2b2ee642f893123d8d2e96a90c26e77c and qualification gates are reused; this increment changes documentation/glue only. Historical unrelated fixture failures remain unclassified in the original qualification; no full-suite-pass claim. See evidence.json and immutable source packet/protocol copies.
