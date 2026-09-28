# Delayed DELETE recovery falsifier

Use one deterministic in-memory storage adversary in Rust, not an S3 timing
claim. Fail a staged mutation final CAS; GC DELETE error leaves its effect
ambiguous. Successful GC retry deletes orphan and releases fence. Reapply the
same batch and acknowledge it. Deliver original orphan DELETE effect afterward.
Required: acknowledged mutation survives recovery. Existing hash encoding has
no reclamation epoch, so causal prediction is missing recovered payload.
One AWS causality Spot red-only correctness test, expected assertion failure101;
verify exact failure, sync logs and terminate.30min/$0.30 max, discard interruption
without relaunch. No broad suite, repeated baseline, benchmark or DGX/local Cargo.
Only after reproduction choose a guard covering all publication/reference paths.

First workeri-0b63704917d6ac0c1 closed: test stopped at test-helper assertion
that failed DELETE was logged. Common fault wrapper records no DELETE operation
entry; remove instrumentation assumption, retain actual GC attempt counter and
object-state assertions. Sequential a0002 reruns the actual falsifier.

## Corrected red retry capacity failure
Attempt a0002 was rejected by EC2 RunInstances with InsufficientInstanceCapacity in eu-central-1c; no instance launched and active-tag query was empty. Preserve its reservation. Sequential a0003 uses public subnet subnet-034528fbd6977848f in eu-central-1a, same VPC/security group, Spot instance type and test/cost caps; no production changes.

Attempt a0003 likewise failed InsufficientInstanceCapacity in eu-central-1a before launch. Sequential a0004 checks eu-central-1b with unchanged Spot/cost/test policy.

Attempt a0004 in eu-central-1b failed Spot capacity before launch. No active workers remained. Sequential a0005 changes only correctness-worker instance size to c7i.2xlarge (8 vCPU/32GiB), same4 Cargo jobs and test/time/cost caps, eu-central-1a. Observed Spot price $0.2128/hour, not an invoice. This is not a performance benchmark.

## Mutation epoch green preregistration
Verified red a0005 reproduced missing acknowledged snapshot. BTMUT002 encodes advanced control epoch in hashed body; retain root/revision/corruption checks and reject old snapshot format. Run10 focused/live-S3 checks (existing9 plus delayed-delete) sequentially on one Spot c7i.2xlarge worker with same caps, terminate on terminal. No full gate or performance claim. Broader generation identity safety remains open.

## Generation sibling red preregistration
After mutation green closes, one sequential a0007 red-only check of delayed-delete integration: existing mutation regression must pass; empty-generation retry after GC is predicted to reuse its orphan root key and lose acknowledged generation under late DELETE. Require exact generation recovery assertion, same Spot c7i.2xlarge/caps, no full suite/performance measurement.

## Epoch-owned generation green a0008
After exact generation red, bind all prepared root/maintenance identities to captured epoch; strengthen claims with epoch in physical namespace; rebuild old jobs after GC epoch change. One sequential Spot green of12 focused/S3 checks plus builder-config validation; no full gate until52 failures reconciled.30min/$0.30 caps unchanged; old formats explicitly rejected.

## Sequential a0009
Prior worker terminal compile failed in test fixture, terminated. Complete missing4 limits, restore before-I/O empty geometry guard with regression, same focused green and builder config. No tests passed at a0008.

## Sequential a0010
Original a0009 closed after6 passing checks and compaction instrumentation assertion. Uploader logs MultipartPut; correct actual operation predicate and assert injected Store failure before identifying staged object. No production change; original sequential focused/S3/config gate unchanged.

## Sequential a0011
Exact Store-error guard exposed intentionally too-low1MB test cap. Use existing4MB fixture generation allowance; production admission unchanged. Original a0010 terminated; same sequential targeted gate.

## Sequential a0012
Seven checks pass, stale replacement fixture must regenerate prepared epoch after sealing; preserve pre-seal/stale-root rejection. No production behavior change; minor comment clarified per completed implementation review. Same sequential14-check/S3/config green.
