# Autoresearch: measured decisions, 2026-10-18

Frozen native source7c3de4f0f06e29f8e6751a4fd36599c00121e251; runner SHA256
73ab82ac75463f48ef92dc629f99f7f0976f4afd7e10796a2b0c223e679860d9.
Later5537 is operating documentation only. No rebuild between these trials.
Cohere Wikipedia1M/D1024/cosine/k10, same authenticated requests/truth/source/
SQ8/order, leaf40, Offload2. Spot m6in.4xlarge i-0836745310f2e6a98,
CPU0-7/800%,8GiB/noSwap guards; fresh processes/cacheOFF/backend cacheUNKNOWN.
These are regression results, not untouched holdout, proven cold S3,
concurrent service capacity, competitor parity or production readiness.

## Decisions

| Causal change | Quality | Measured effect | Decision |
|---|---|---|---|
| Flat to framed SOURCE, first32 ABBA | Exact314/320 and ordered plans/scorebits | SOURCE-4.67%,sameGETs;wall+4.4/+6.5%;CPU+60.2/+55.1% | Retain byte tradeoff;flat incumbent |
| Leaf40 to48, first32 ABBA |314/320,zero per-query gains/losses|SOURCE+17.67%,moreCPU|Keep40;not a global idea rejection |
| Fetch64 to128, first32 ABBA |Exact314/320 and allquery plans/charges/IDs/scorebits|wall-7.7/-14.5%,sameGETs/bytes|Nominated full1000 |
| Fetch64 to128, full1000 ABBA |Exact9737/10000 in all4,zero underfill,all1000 per-query plans/trace/IDs/scorebits/charges equal|wall-8.9/-12.4%,p95-7.9/-12.9%|Recommend128 for this workload;global library default unchanged |

## Full1000, pair results

| Metric | Control64 A1 | Candidate128 B1 | Candidate128 B2 | Control64 A2 |
|---|---:|---:|---:|---:|
| Recall@10 |97.37%|97.37%|97.37%|97.37%|
| Mean query ms |136.134|123.976|123.732|141.197|
| p90 ms |173.327|158.981|160.422|178.263|
| p95 ms |191.408|176.270|174.799|200.775|
| p99 ms |222.528|224.000|211.841|302.100|
| Sequential queries/s |7.346|8.066|8.082|7.082|
| Query CPU seconds |99.002|97.081|97.105|98.979|
| Process peak RSS MB (decimal) |90.88|113.47|113.86|97.30|

One pair p99 rose0.66%;the other fell29.88%. Both preregistered p90/p95 and
total-wall screens passed. Query bootstrap95% wall-ratio intervals:
0.9024..0.9200 and0.8630..0.8888, conditional on this cohort/host/order.
They do not account for host/time variation or semantic query correlation.
Do not pool these as4000 independent queries or claim generalization.

Each cell has unchanged SOURCE97,797GET/24,255,289,344 verified bytes and
SQ830,014GET/9,974,177,024 verified bytes:127,811 logicalGET and
34,229,466,368 bytes combined. SDK attempts/retries are separate raw native
transport observations;wire/billed counts remain unclaimed.

Recommended config is campaign-bound, not a portable default. It demonstrates
independent tiers: SOURCE downloads128, authentication workers2, activequery1;
SQ8 budget<=32GETs so its effective parallelism stays<=32. Native2GiB model
cap and8GiB host guard differ from observed processRSS. Q2/hardware changes
need separate confirmation.

## Validity and failures

Both new layouts passed real native S3 publication, authenticated input
admission, separate canary and native strict paired validation. Leaf48 and
fetch128 each passed actual input admission/canary and strict completion.
Original128 traced canary failed the diagnostics1MiB cap before payloadI/O.
Preserved as INVALID; a separately recorded traceOFF canary matching measurement
passed, without algorithm/resource-cap changes. No native Rust code was
changed to accommodate these trials.

Four full1000 cells each passed the native completed reader, seal/EOF and
source-bound headers, then create-only S3 checkpoint/hashHEAD. Whole-unit
terminal0 preceded analysis. All files and configSHA values remain in archive.
Native prior qualification includes affected tests, release, workspace
correctness/suspicious Clippy and actual shim-unset test-build on exact7c.

## Recall diagnosis and next gate

An independent structural verifier authenticated all1,036,000,000 SQ8 bytes,
derived source-order SHA and full bijection, reconciled314 hits/six misses,
and cross-checked every returned physical ordinal. Five misses were already
SOURCE-scored/SQ8-selected/fetched;one was outside those sets. This falsifies
the claim that all six were lost before SOURCE scoring. It does NOT identify
quantization alone:ranking,normalization and ties remain possible.
See loss-membership.json and archived verifier/source-audit receipts.

Next bounded native diagnostic: exact native SQ8 replay on the recorded
fetched set, then canonical-f32 ranking of that SAME set. Refuse attribution
on replay/pin/order mismatch. No extra coverage intervention. A contextless
implementation packet owns this separately;root owns actual replay.

Production gaps remain:full lifecycle/maintenance envelope, deployment-safe
GC ownership (current contract is cooperating single-host/same-directory),
fresh untouched confirmation cohort, concurrent capacity and matched S3
Vectors/Turbopuffer runs. These query improvements do not close those gates.

## Evidence

Create-only S3 closure:
s3://borsuk-bench-453182569524-euc1/research/semantic-router/20261018/autoresearch-development-closure-a0001/
Full/raw cells are checkpointed underfetch128-full1000-a0001/cells/;all manifests,
configurations,scripts,failures,native reports and analysis are in evidence.tar.gz.
Exact7c binaries/source qualification are reused from source-frame-serving-
closure-a0001/evidence.tar.gz SHA
a6597318cacfffd7224a10d58edf3c17c9293512a0c3d45531560198bb7041e9.
Persistent EBS retains input payloads;no dataset payloads are added to Git.
