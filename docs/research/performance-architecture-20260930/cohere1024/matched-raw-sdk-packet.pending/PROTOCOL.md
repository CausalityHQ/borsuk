# Matched original-raw S3 Vectors packet

Measure S3 Vectors on the same Cohere 1M corpus, D1024, cosine, k10 and fixed ordered 1,000 queries as the Rust leaf-limit-48 reference (9,783/10,000 recall hits). Record either outcome. This packet establishes no service, quality, performance or vendor-win result. Parent owns review, native execution, integration and publication.

Only run.sh is executable. Scope is this directory; no native source, library, fixture, export or existing caller changes. protocol.json, frozen configs, retained metadata and manifest.sha256 bind the experiment. Preserve the packet and execution ledger. Never rerun a consumed mode, resume a partial query stream or rename state to bypass admission. The controls in evidence/s3vectors-full-cell-a0002 are reference only; never execute them.

## Immutable inputs and proof

Parent source is 7a64c62bc253774ae62036977d8282cab07efc14. SDK source matches qualified ab9243bf7174d087061f80dd7852e0afe8ffecec: SHA256 778aad02428e91044368f0d38ac4ac19d85d6bbb2fca89235b58de6a60a2fb1c. Reuse only /mnt/borsuk-workspace/artifacts/s3vectors-original-raw-release-a0003/check_s3_vectors_baseline, ELF SHA256 7240cde309d2995a2f8dcbb5ee2259873c3da1be47784e28f419e04e0714c3da. Source/ELF hashes and ELF magic are checked before execution. Retained qualification records 16 debug tests, 16 release tests, release build, workspace Clippy and unshimmed test-build passing; unchanged native gates are reused.

| Input | Bytes | SHA256 |
|---|---:|---|
| Original corpus, implicit IDs 0..999999 | 4,096,000,000 | 1a491c060ec8b668b4983042dc1ac3971456327f41f28fd6fd2449df24cf3e38 |
| Ordered queries | 4,096,000 | 8460a81ff2f979deff7d82bede874a1301f47dfd3e4589305c9f53e020920d5e |
| Exact truth, u64LE logical IDs | 80,000 | 0e38d943adaf62f197b98fe8b01b2edec2008010df760717ababfa5054693b7f |

Paths/stat identities are in protocol.json. Worker used metadata/stat only: no payload, query or truth contents were read, hydrated or regenerated. Native admit must authenticate every byte, geometry, finite/nonzero vector, truth row and descriptor identity.

Dataset revision: ade45fb52bd549f5e8c065636fe4160a43c2af36. Corpus source intervals: [0,100000) and [101000,1001000); reserved queries: [100000,101000). cohort.json binds original publisher Float32 bits, this split and all artifacts. Exact truth uses sequential f64 dot/norm sums over original f32, cosine distance and ascending logical corpus ordinal ties. Service ties may differ; do not adapt truth or queries.

derivation.json binds original raw to normalized SHA256 31f28fa211f9237f0f7e41f569773cd207cefcdce22d4932ec96aee01ff2c40d, Rust source-order map and SQ8. normalization.json records source-bound native reexecution with the same normalized digest, unchanged input/ELF stamps and independent output hash readback, without queries/truth. rust48-bindings.json binds Rust48 to that normalized source, logical split, query and truth pins.

Normalization uses f64 squared-norm summation; rows within 1e-6 of unit squared norm are unchanged, otherwise f64 division by the norm rounds to f32. Mathematical cosine scale invariance does not imply identical finite-precision scores/ties. Original producer ELF was not recovered; arithmetic was not independently implemented. These are explicit proof limits. No fabricated proof substitutes for native input admission.

## Exact API and stages

Native API: check_s3_vectors_baseline <admit|prepare-scale-canary|publish|measure|reduce|cleanup> CONFIG CONFIG_SHA256.
Config schema: borsuk-s3-vectors-scale-raw-config-v2.
Receipt schema: borsuk-s3-vectors-receipt-v1, with config_sha256, kind and data.
State is create-only except append-only call ledgers and numbered cleanup attempts.

1. run.sh gates: full real-input admission; native canary preparation; exact byte comparison to canary.expected.json; canary admission; live publication and all 257 rows bit-exact readback; two diagnostic queries, native reduction, one cleanup. Canary uses first 257 authenticated raw rows and first two queries, synthetic ID-validation truth. Canary quality/performance claims are forbidden.
2. run.sh verify-canary: independent GetVectorBucket/GetIndex probes after gate unit is terminal. Each requires CLI exit 254 and NotFoundException; timeout/access denial/other errors fail. Emits config-bound absence.json.
3. Parent reviews actual gate statuses/receipts/cleanup/absence. run.sh full enforces them too. Fresh distinct full index: 2,000 paced PutVectors calls of 500 rows; complete key enumeration; 10,000 GetVectors calls of 100 rows; bit-identical readback of every original raw f32.
4. Wait until publication verification plus 3,600 seconds. Run exactly one ordered query1000 pass and qualified reducer. Reduction requires a complete file/directory-fsynced response seal before truth opens. quality-comparison.json records SDK hits, denominator and difference from 9,783, with vendor_win_claim=false. No partial CSV/JSONL reduction.
5. EXIT performs one full cleanup. After terminal completion parent runs verify-full for independent absence.

Retained outputs per phase: argv, UTC start, original PID, stdout, stderr, actual exit. measurement.exit, pipeline.exit, cleanup.exit, drain.exit and runner.exit are separate; NOT_RUN differs from zero. Cleanup success cannot erase measurement failure. Preflight rejection precedes ledger creation; retain enclosing unit/SSM stderr and exit as its blocker evidence.

## Conditions and finite termination

Host i-08f0d6520a3cb73df, region eu-central-1, explicit causality IMDS profile from aws.config, no copied credentials. Single SDK attempt, no retries, reused client, concurrency 1, zero query interval. Operation timeout 30s. First query includes lazy credentials. Latency covers construction/signing/transport/deserialization/validation, excluding staging/receipt writes.

Cache-uncontrolled first pass: backend cache unobservable, one-hour inactivity is not proof of eviction. Report first-query latency, integer nearest-rank percentiles, serial reciprocal QPS and whole-pass QPS; not concurrent service capacity. Rust bindings retain their own memory, fetch parallelism, cache and retry conditions. Input matching alone does not prove resource/latency/cost equivalence. Parent must reconcile conditions and complete measurements before any comparison claim. Billed bytes/requests/cost remain unknown.

GNU timeout sends TERM then KILL after 5s. Wrapper waits the original handle and checks its whole process group is gone. On interruption it signals the same PID/group, escalates after 5s and allows 5s more for disappearance before reaping. Cleanup starts only after successful drain. Surviving group means unresolved cleanup for parent; no overlap.

Full bounds: publish7200s, idle wrapper3610s, measure wrapper1860s, reduce120s, cleanup600s. Native query clock remains1800s; wrapper adds60s for input authentication/startup. Bounds do not guarantee successful import. RuntimeMaxSec=13600 full,2100 gates. TimeoutStopSec=650 both covers10s drain+600s cleanup+escalation/status writes. KillMode=mixed preserves this cleanup window after TERM and ultimately kills the cgroup. A60s stop grace cannot cover600s cleanup and is not used.

Each launch reserves runtime+stop before actual guest horizon 2026-10-18T02:07:56+00:00. Parent must verify the actual shutdown guard is no earlier. Full14250s reservation is checked at launch against the actual execution horizon and MUST fail if it cannot fit. Do not shorten frozen idle, extend timers or bypass guard. This prospective root binding preserves the original preparation packet; no service call occurred before rebinding.

## Parent commands

Execute only through SSM on named host after reviewing/freezing commit and manifest. Retain original SSM handle and unit name; queued launch is not start proof. Runtime units deliberately omit --collect so terminal status survives. Verify actual guest shutdown guard first; worker did not inspect/change it.

~~~bash
packet=/mnt/borsuk-workspace/matched-raw-sdk-packet-source-a0001/docs/research/performance-architecture-20260930/cohere1024/matched-raw-sdk-packet.pending
cd "$packet"
sha256sum --check --strict manifest.sha256
systemd-run --unit=borsuk-matched-raw-gates-a0001 \
  -p CPUQuota=400% -p AllowedCPUs=4-7 -p MemoryMax=8G -p MemorySwapMax=0 -p TasksMax=512 \
  -p RuntimeMaxSec=2100 -p TimeoutStopSec=650 -p KillMode=mixed -p Restart=no \
  /usr/bin/bash "$packet/run.sh" gates
systemctl show borsuk-matched-raw-gates-a0001 -p ActiveState -p SubState -p Result -p ExecMainStatus -p MemoryPeak
~~~

After gate unit is actually terminal and reviewed:

~~~bash
systemd-run --unit=borsuk-matched-raw-verify-canary-a0002 \
  -p CPUQuota=400% -p AllowedCPUs=4-7 -p MemoryMax=8G -p MemorySwapMax=0 -p TasksMax=512 \
  -p RuntimeMaxSec=120 -p TimeoutStopSec=15 -p KillMode=mixed -p Restart=no \
  /usr/bin/bash "$packet/run.sh" verify-canary
~~~

Complete full command for review; frozen horizon check currently BLOCKS it:

~~~bash
systemd-run --unit=borsuk-matched-raw-full-a0001 \
  -p CPUQuota=400% -p AllowedCPUs=4-7 -p MemoryMax=8G -p MemorySwapMax=0 -p TasksMax=512 \
  -p RuntimeMaxSec=13600 -p TimeoutStopSec=650 -p KillMode=mixed -p Restart=no \
  /usr/bin/bash "$packet/run.sh" full
systemctl show borsuk-matched-raw-full-a0001 -p ActiveState -p SubState -p Result -p ExecMainStatus -p MemoryPeak
~~~

After full unit terminal, independently verify:

~~~bash
systemd-run --unit=borsuk-matched-raw-verify-full-a0001 \
  -p CPUQuota=400% -p AllowedCPUs=4-7 -p MemoryMax=8G -p MemorySwapMax=0 -p TasksMax=512 \
  -p RuntimeMaxSec=120 -p TimeoutStopSec=15 -p KillMode=mixed -p Restart=no \
  /usr/bin/bash "$packet/run.sh" verify-full
~~~

SIGKILL/OOM/host loss can bypass EXIT. Parent must verify original cgroup is terminal before external recovery; persistent flock also prevents concurrent modes. One explicit recovery attempt with intact ownership evidence:

~~~bash
systemd-run --unit=borsuk-matched-raw-cleanup-full-a0001 \
  -p CPUQuota=400% -p AllowedCPUs=4-7 -p MemoryMax=8G -p MemorySwapMax=0 -p TasksMax=512 \
  -p RuntimeMaxSec=650 -p TimeoutStopSec=15 -p KillMode=mixed -p Restart=no \
  /usr/bin/bash "$packet/run.sh" cleanup-full
~~~

For canary use unit borsuk-matched-raw-cleanup-canary-a0001 and mode cleanup-canary. Recovery never retries publication/measurement. Native cleanup validates exact owned identities and refuses missing ownership/creation evidence. Reconcile original create intent, returned ARN/request ID and external resource evidence; never synthesize receipts or delete guessed names. Independent verification remains mandatory. If original guest is lost, parent must prepare an authorized cleanup environment with preserved ledger; this worker grants no cloud-control authority.

## Unrun gates

static-validation.json records remote static checks only. Worker never executed wrapper, native tests/builds, SDK/service probes, payload checks or experiments. Parent must verify actual input admission, IMDS/service permissions, runtime timeout/drain, canary bit preservation, memory, full completion, quality and cleanup. Missing/mismatched proof fails admission. No automatic binary admission, payload download, native rebuild or fallback dataset.

## Root prospective execution binding
Upstream3928ab42 is preserved unexecuted. This snapshot binds actual execution host and verified guest poweroff minus60s, with an independently prearmed6h termination timer. 8GiB/4CPU/512tasks is an operational safety envelope, not a product acceptance threshold. Native SDK, raw/queries/truth, source proof, idle3600 and all stage/runtime/cleanup bounds are unchanged. Full state/bucket name remains unused; canary config/namespace is deterministically refreshed from new full config SHA before any input admission or service call.

## Observed verification correction
Original independent verify-canary attempt is preserved with exit1/drain0: actual authenticated CLI returned exit254 and NotFoundException, while the control expected ResourceNotFoundException. Native admission/canary/readback/reduction/cleanup all passed0 and are NOT repeated. Corrected read-only verification writes new verify-canary-a0002; full gate binds exactly that new absence/runner receipt. SDK, configs, ownership, data and scientific conditions are unchanged.
