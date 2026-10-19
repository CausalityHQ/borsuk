# Current BORSUK / S3 Vectors gate — 2026-10-19

Reuse the completed SDK experiment. Do not launch the old pending packet.
The retained native result is 9,902/10,000 hits and the completed S3 Vectors
ENHANCED result is 9,617/10,000 on the same fixed logical cohort, query bytes
and exact-truth bytes: **+285 hits / +2.85 percentage points** for BORSUK.
This is an authenticated, input-linked descriptive comparison of closed runs,
not a current-source matched latency/throughput/lifecycle-cost win.

This document authorizes no run, cloud operation, spend, host extension or
compiler intervention. The root retains those decisions. Current host
i-00ab8447977a3e14d has a 00:30 UTC horizon per the assignment; its timer was
not inspected or changed. Work here was confined to CPU1/512MiB/swap0/tasks128,
120-second static source/metadata commands through causality/eu-central-1 SSM.

## Evidence and exact input linkage

All absolute paths below are on EC2 EBS under /mnt/borsuk-workspace. gate.json
contains full pins, metadata witness hashes, limits and machine-readable gaps.
No corpus, query, truth, gold, response stream or ELF payload was opened or
hashed by this worker. Retained qualification and terminal receipts are evidence;
their byte-level execution checks were not independently rerun.

Cohort receipt e0503d41493157a6a0a48627515e62ec452386d0496827028bb9571d406e3a17
binds Cohere revision ade45fb52bd549f5e8c065636fe4160a43c2af36,
1M rows, D1024, cosine, k10, corpus source intervals [0,100000) and
[101000,1001000), and reserved query interval [100000,101000).
Compact corpus ID i maps to original source i below100000 and i+1000 above;
query q maps to source100000+q and is not a corpus service key.

| Artifact | Bytes | SHA256 |
|---|---:|---|
| Original raw corpus f32LE | 4096000000 | 1a491c060ec8b668b4983042dc1ac3971456327f41f28fd6fd2449df24cf3e38 |
| Ordered queries f32LE | 4096000 | 8460a81ff2f979deff7d82bede874a1301f47dfd3e4589305c9f53e020920d5e |
| Logical-ID truth u64LE | 80000 | 0e38d943adaf62f197b98fe8b01b2edec2008010df760717ababfa5054693b7f |
| Corpus ID metadata | 200873446 | 008191ae3103d78db682276008190cc858a361b19e487f6780c02cea5dd70067 |
| Query ID metadata | 196017 | f0e6e974c3dfe45fae7db3a9a5146891541d1f3c0d605674d115b6c580a87f68 |

SDK keys are six-digit zero-padded logical ordinals, not publisher document
strings or BORSUK physical row positions (current SDK source key()/ordinal(),
lines1036 onward). Truth is an independent block-major exhaustive scan with
sequential f64 dot/norm arithmetic over original f32, ties by ascending compact
corpus ordinal. It is not ANN-generated truth.

Derivation receipt 00e59ee26745c092f1097ba8892b4f205f7c96b48a757311aedc9072fcc3c96e
binds raw corpus to normalized source31f28fa211f9237f0f7e41f569773cd207cefcdce22d4932ec96aee01ff2c40d,
SOURCE order1060c919e34e93cc9a3d88b93eda226a8635bba41bfc60e5ba47caafb711d413
and SQ8 cc4395af5fdbdeffcb596460aceaad0e55bdcc4af074f405e78ab2dd2be002e1,
without query/truth use. Their sizes are4096000000,8000000,1036000000 bytes.
Current A1 config carries these same pins and the same cohort/derivation/query/
truth pins. Physical SOURCE permutation must resolve back to logical IDs.

sq8_source.rs22–37 at b3bcfce uses f64 squared-norm summation, leaves rows
within1e-6 of unit squared norm unchanged, otherwise divides in f64 and rounds
to f32. normalization.json records source-bound reexecution7acbbb82 with
native exit0 and independent output SHA readback matching the normalized pin.
Wrapper exit1 was a wrong-directory final source check, subsequently reconciled.
Original producer ELF is unrecovered; no independent normalization arithmetic
implementation is proved. This supports semantic derivation, not identical
raw/normalized bits or finite-precision tie parity.

Gold ce13a10a30f0cbaafe2a8aaefdeea54a5e1118ed3bf812168d371d690c18f74c,
48684516 bytes, is a historical exact-answer/plan/score reference for1000
ordinals. It is distinct from truth.u64. Its authority links to completed
cold-source-overlap-full1000-a0002/B1.jsonl; current verification also records
the independent f64 oracle. Gold was not read.

## The pending packet and actual original outcome

The requested packet checkout is exact3928ab42e156a7e78fc098736c68beee1869f047.
Its PROTOCOL/run.sh/configs/qualification/cohort/normalization/derivation/
raw-recovery/rust48-bindings were read. Its full config9ad311cf, host
i-01f04c14490040309, Oct17 horizon, CPU1/512MiB/cpu12 and old Rust48
9,783-hit reference describe preparation, not the executed experiment.

Original full argv points instead to matched-raw-sdk-execution-source-a0001
at e8b5653f27b3588c504c15afed93dcb95f662146. Its full config is
31f3ddf6d3b59eaa8ea705012b449f60714b7beaee892b16a9be7060af54feb1,
canary8c24ca40, host i-08f0d6520a3cb73df, external4CPU/8GiB/swap0/tasks512.
The legacy emitted512MiB value is not its enforced external cap.
Do not silently apply these old host/config pins to today's host.

evidence/matched-raw-sdk-closure-a0001/root-verification.json binds original
launch SSM55f98001-5cdc-4ff6-8130-e168e590464f and terminal observation
61c5c5d3-faa1-4f76-b13b-01dbd7a2dac8, invocation96475bf14fcc4a4a93c0d6d414d4a2f6.
The retained journal closes20:27:33–22:44:35 UTC Oct17: all full phase exits0.
Publication verified1M raw rows bit-exact after2000 PUT,2000 LIST and10000 GET
calls. One sealed ordered1000-query pass followed3605561ms observed inactivity.
Provider default and actual index were ENHANCED; STANDARD was not measured.
Response seal SHA37626f0fc4f0ee73843a31d84ee4580871162dbb4b3556226e620e5dc6c661eb,
1759924 bytes, precedes truth opening. Reduction is VALID, no underfilled queries.

Ownership receipts bind exact bucket/index ARNs, config31f3ddf6 and create
request IDs1e974bfb-9a4d-8f71-b73d-c63441a38702 /
17b4bb7e-fd92-81c7-9d09-ff0a3afe41fb. Native cleanup0 and independent
verify-full0 establish both owned resources absent. Independent verification
launch7d49309a-909b-47c3-b7cc-033978644146 and terminal
2d20dd76-35e3-4754-8ad2-37c36c10eddf are preserved. Original canary verifier
failed1 because it expected ResourceNotFoundException; separate
verify-canary-a0002 passed with actual NotFoundException and CLI254.
Execution wrapper has that correction; the3928 wrapper does not.

State is consumed. Never rerun full/gates/reduce, rename state to bypass
create-only checks, synthesize ownership, or delete by guessed names.
Historical service-resource absence is established; the closure's infrastructure
termination field remains PENDING_AFTER_ARCHIVE and is not proof of host
termination. No present cloud-state assertion is made.

## Native/source/ELF validity and comparison limits

Qualified SDK sourceab9243bf / SHA778aad02428e91044368f0d38ac4ac19d85d6bbb2fca89235b58de6a60a2fb1c
pairs with ELF7240cde309d2995a2f8dcbb5ee2259873c3da1be47784e28f419e04e0714c3da.
Retained qualification has16 debug and16 release checks plus release/Clippy/
real test-build0. Current b3bc SDK source hashes
ae4633ecbb7230e7f669dd017ee27f2f319aa5499a6f12bdf4e77b91905f10f2 and adds
publication concurrency/pacing (627 insertions/77 deletions). Reuse the old
qualified source/ELF pair for its established purpose; do not relabel its ELF
as a build of current source. CLI compatibility is not binary qualification.

Native closed evidence is evidence/fp32-concurrent-full1000-root-a0001.
Qualified source0cddd795 / cratesab0730bfc49fe9587705275a464cb54f3813c1d2
pairs with runnerafcbbfbd616d7c35aa29beb71968435f4b0e6142c526f11051796ccb21076648
and readercea18d35d66862cc4723d3c4b25aa8abe5bb2bc87e6577c5f2f9a6941d0fb545.
Its runner source matches b3bc at6b7f6a727ffefec33af2722df72692c93b26dc6a8ef274683bb56e2618848a20.
But b3bc crates tree0f73631f67df77e20f186ffcdeaa27958c5aa3a1 (same71aad360)
differs in three hierarchical/fine-SQ8 source files named in gate.json.
No exact b3bc ELF qualification is established here. Preserve source-bound
historical results; root must reconcile qualified reuse or new compiler evidence.

Current supported native CLI is CONFIG CONFIG_SHA NEW_OUTPUT
(check_cohere_native_baseline.rs3775–3803). A1 is config-v21, result-v22,
Q1/multi_thread2, fetch64/offload2/cache0, leaf64, FP32 shortlist16, flat records,
generation14c8cbba54b3299ad681a40ce0455caac78f46c316dd1714da6b475733c5fde5.
Reader API is --fp32-serving COMPLETED_CONFIG CONFIG_SHA NEW_REPORT
(compare_native_replay.rs9945–9968), completed-config-v4. Rust48 bindings use
old config-v15/result-v16, baseline/inline, leaf48, generation824df22c;
they cannot replace today's FP32 headers or its strict reader inputs.

| Closed observation | Recall hits/10000 | p95 ms | Cohort / whole-pass QPS |
|---|---:|---:|---:|
| SDK ENHANCED, one Q1 pass | 9617 | 205.998414 | 7.223852 |
| BORSUK Q1 A1 / A2 | 9902 | 281.836025 / 257.028206 | 5.041886 / 5.396326 |
| BORSUK Q2 B1 / B2 | 9902 | 411.045252 / 415.029013 | 6.399885 / 6.266288 |

These are descriptive historical operating points, not matched speed ratios.
SDK serial reciprocal7.483474QPS uses a different denominator from whole-pass.
Native Q1/Q2 ABBA is internally matched; cross-vendor runs are not balanced.
Native external8CPU/8GiB differs from SDK4CPU/8GiB. Native2GiB is modeled
admission, not measured RSS. SDK whole-unit25.6M rounded cgroup peak is not
isolated query RSS. Native retry one_retry_100ms differs from SDK one attempt;
first SDK query includes lazy credentials while native startup can warm pools.
Native dispatch-to-completion includes initial runtime queue; SDK timing covers
construction/signing/transport/deserialization/validation and excludes receipt
writes. Application cache0 and one-hour SDK inactivity do not prove provider
cache eviction. Logical/SDK counters are not wire/billed usage. Dollar cost,
lifecycle equality and Turbopuffer results remain absent.

## Smallest valid next comparison and root commands

The cheapest valid step is already possible: reuse the closed metadata above
to report fixed-cohort quality and the separate historical operating points.
No data regeneration, native reduction, import, service call or new framework
is required. Root can independently reproduce the narrow linkage on EC2:

```sh
python3 - <<'CHECK'
import json, pathlib
r=pathlib.Path('/mnt/borsuk-workspace')
p=r/'matched-raw-sdk-execution-source-a0001/docs/research/performance-architecture-20260930/cohere1024/matched-raw-sdk-packet.pending'
e=r/'evidence'
f=json.loads((p/'full.json').read_text())
c=json.loads((e/'fp32-concurrent-full1000-root-a0001/A1-config.json').read_text())
s=json.loads((e/'matched-raw-sdk-closure-a0001/sdk-reduction.json').read_text())['data']
b=json.loads((e/'fp32-concurrent-full1000-root-a0001/result.json').read_text())
for a,z in [('queries','requests'),('truth','truth')]:
    assert f[a]['sha256']==c[z]['sha256'] and f[a]['bytes']==c[z]['bytes']
assert s['status']=='VALID' and s['query_count']==1000 and s['recall_denominator']==10000
assert all(v['queries']==1000 and v['denominator']==10000 and v['gold_exact']
           for v in b['measured'].values())
print({'native_hits':b['measured']['A1']['hits'], 'sdk_hits':s['hits'],
       'difference':b['measured']['A1']['hits']-s['hits'], 'vendor_win':False})
CHECK
```

To obtain a genuinely current matched end-to-end point, root first needs an
exact qualified current native source/ELF pairing, independent expected headers,
a new frozen packet and one full1000 Q1 pass per system under a common declared
client envelope and clock/startup/retry/cache treatment. Use fixed existing
queries/truth without tuning against results. A257-row/two-query canary proves
admission/service plumbing only; a smaller corpus or query subset is not the
same comparison. One pass is a bounded feasibility point; a vendor-win study
also needs preregistered order/repetitions, quality target and lifecycle cost.

Reuse existing qualified machinery. These are exact native argument interfaces
for root's **future frozen** files, not runnable authorization; there are no
guessed new config pins or resource identities:
- SDK: "$SDK_BIN" admit "$SDK_CONFIG" "$SDK_CONFIG_SHA"; then
  prepare-scale-canary, publish, measure, reduce, cleanup with the same
  three-argument interface. Keep wrapper gates, full, verify-canary, verify-full
  sequencing, original-handle drain and independent cleanup closure.
- BORSUK: "$NATIVE_BIN" "$NATIVE_CONFIG" "$NATIVE_CONFIG_SHA" "$NEW_JSONL".
- Strict reader: "$READER_BIN" --fp32-serving "$COMPLETED_CONFIG"
  "$COMPLETED_CONFIG_SHA" "$NEW_REPORT"; then existing full1000 exact-gold and
  independent-f64 oracle verification. Expected headers come from independently
  serialized configuration/qualified authority, not observed output.
- SDK_BIN's established path is artifacts/s3vectors-original-raw-release-a0003/
  check_s3_vectors_baseline. The historical native binaries are in
  evidence/fp32-concurrency-screen-root-a0001/bin/. Verify source/ELF/manifest
  pins and choose a root-qualified pair before use; these paths are not a license
  to call them now.

Root must bind fresh create-only state/output, actual host and safe CPU affinity,
service identity/credentials, horizon, config SHA, source/ELF and allowed
budget. Existing SDK config hard-codes1M/1000, D1024/k10,3600-second idle,
single query concurrency and22000 full-call reservations; changing count/idle
or silently using current publication code is not a cheap valid shortcut.
Original SDK IMDS profile is in packet/aws.config; no credentials were copied.
Native historical runner uses IMDS only. Root must preserve both existing
compiler/native locks and wait for their owning job to finish before any
new native scheduling.

Static recovery ceiling:120s,1CPU/512MiB/swap0/tasks128, zero vendor calls,
zero added vendor or new-host cost. A prospective fresh matched feasibility
cell has a **proposal ceiling**, not authorization:21600s total,4CPU/8GiB/
swap0/tasks512 common client envelope, native model cap2GiB. SDK reserves
2750s gates+stop,14250s full+stop,135s each independent verifier; one native
600s run+5s grace+120s reader+5s grace=730s. Reserve665s for one explicit
cleanup recovery, leaving2935s for binding/collection within six hours.
This cannot fit today's00:30 host. No shutdown change is proposed or made.

Proposed all-in monetary admission ceiling is$10, **not an estimate or grant**.
ENHANCED prices, regional service charges, EC2/EBS/network, import/readback,
queries, storage lifetime and cleanup reserve have not been priced here.
Root must establish a conservative bound below its authorized ceiling before
launch; if that cannot be done, do not launch. Existing measured cost stays null.
No bill or quota is inferred from SDK call counts.

## Simulated objections and falsifiers

These are simulated perspectives, not expert testimony or additional evidence.
Retrieval: raw-to-normalized derivation and logical IDs matter more than a shared
dataset name. Prediction: changing any raw/query/truth/order pin must fail
admission; mathematical scale invariance alone cannot prove f32 score ties.
Receipts and sq8_source.rs verify the premise; independent arithmetic is absent.

Systems: matching CLI syntax cannot qualify a different ELF or equate startup
clocks. Prediction: the3928 wrapper rejects current host/horizon/source hash
before service work; current publication source cannot inherit old binary proof.
Source diff, preflight and original argv support this without executing them.

Statistics:1000 query outcomes are correlated, and one SDK pass is not balanced
against native ABBA. Prediction: retained metadata proves the aggregate285-hit
difference but cannot yield paired query uncertainty or population tail confidence.
Retain useful quality/latency/memory tradeoffs and do not infer a vendor win.

## Verification and delivery

Only decision.md and gate.json are added on exactb3bc in a sparse-before-checkout
worktree. Inherited missing .github/workflows/pages.yml object7130c209 was
preserved without repair/fetch. Metadata inventory, before/after unowned status/
diff audits, two-file commit, thin bundle and handoff hashes are retained in
evidence/matched-s3vectors-current-gate-a0001. Static assertions check input
links, closed outcomes, document scope and hashes. No cargo/rustc/tests,
payload checks, native execution, vendor/API/infra operations, children,
consultations, reviews, push or merge were performed.
