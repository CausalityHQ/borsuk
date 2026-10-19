# Cold breadth48 Pareto packet — HOLD for root

Base: 9902a281e4c3e33d30d249244ff764e828dcb8de. Only this pending directory changes.
Adapted from the old cold-source-overlap-full1000 packet (58e), with its retained
SHA-pinned helper. Both arms reuse old B qualification, runner67055f6d and readerfa16dcca;
source cd8cac6 crates treeab73789 matches the main qualified crates tree. No rebuild.
Qualification retains unclassified historical test failures; no full-suite-pass claim.

Root owns native preparation/execution, lifecycle, scientific decisions, integration
and push. Worker prepared this packet only via EC2 SSM under CPU1/512MiB/swap0/
tasks128/120s. Static checks open controller/config/expected/qualification metadata,
not raw JSONL, query/truth/corpus, Rust source or native binaries. No native, Cargo,
S3, infrastructure changes, children or consultations were run by the worker.

The sole causal difference is semantic_leaf_limit A64/B48. Shared settings: fresh
processes/cache0/Q1/offload2/FP32 shortlist16/coalescefalse, SOURCE128GET/64MiB,
SQ832GET/32MiB, FP3216GET/65664B, library2GiB and unchanged scratch.
Historical source config absence of source_cache_bytes becomes explicit zero.
Separate frozen per-arm expected headers retain each semantic limit and bind the
shared qualified runner/source hashes. They are never copied as common guessed pins.

Parent source-confirmed model derivations and original receipt outcomes are in
model-pins.json. Full A caller7010842/model471434981; full B caller6942450/
model470249621. For n diagnostic queries, A adds524288+8*n, B adds262144+8*n
to BOTH caller and model. Actual root admission checks these exact values.
The old B caller and old B binary hashes are retained as historical evidence only.

Root runs 8-query admission for BOTH arms, then separate 2-query canaries, then
fixed68 A1/B1/B2/A2. All cells use diagnostic_panel, tracefalse and fresh processes.
closed-references.json is SHA8552cbcd...176, copied unchanged from the root authority.
Root runtime reads each complete reference, verifies SHA/bytes/EOF/1000 ordinals,
native+reader0, closed summary and historical headers/generation; each new arm
matches its OWN reference IDs/f64bits/physical ordinals/plan/trace/shortlist/charges.
The independent f64 oracle is retained. Worker never opens the raw references.

This is a quality/latency tradeoff screen, not quality parity. Retain historical
A9902/10000 (99.02%) and B9852/10000 (98.52%); fixed68 is no new full-quality claim.
Mandatory gates: BOTH paired p95 B/A<=.90, pooled query wall<=.90, pooled p90<=1,
query CPU<=1, external wall<=.95, external CPU<=1.05, logical GET<=1 and verified
bytes<=1. Report panel quality delta, latency, GNU-time RSS and native physical
SDK/retry counters separately from logical charges. Valid slow cells/retries stay.
No vendor, readiness, population or generalization claim.

Budget: prelude360 +4*(native150+reader45)=780 +kill160
+20 AWS operations*(15s+2s)=340 +other160 =1800s.
AWS wrapper uses timeout15/kill2, connect5/read10, AWS_MAX_ATTEMPTS=1; native
bounded retry policy is unchanged. Aggregate292 query ceilings; eight cell
PUT+HEAD checkpoints plus archive and terminal PUT+HEAD are create-only.
Evidence/prefix cold-breadth48-pareto-a0001 is fresh-create only under
research/semantic-router/20261019/. No slow-cell retry, overwrite or auto replacement.

Latest outer launch11:55, measurement12:00 UTC, fixed guest shutdown~12:40 and
external75min. Never extend any timer. Root verifies existing Spot instance
i-015199f89c1cf89ea (m6i.2xlarge Intel8375C/8vCPU, eu-central-1).
Exactly once, root may launch the following after its own review:

    sudo systemd-run --unit=borsuk-cold-breadth48-pareto-a0001 --collect \
      -p AllowedCPUs=0-7 -p CPUQuota=800% -p MemoryMax=8G -p MemorySwapMax=0 \
      -p TasksMax=1024 -p RuntimeMaxSec=1800 -p TimeoutStopSec=10 \
      /bin/bash /mnt/borsuk-workspace/cold-breadth48-pareto-packet-a0001/docs/research/performance-architecture-20260930/cohere1024/cold-breadth48-pareto.pending/launch-root.sh

Do not call prepare separately first. Keep the original unit/SSM handle and shared
cold-native-experiment lock. Spot interruption, failed reader, missing cell,
checksum failure or missing successful terminal makes the attempt INVALID.
No incomplete payload inspection; root observes lifecycle and terminal metadata.
Early preparation/reference failure or hard kill can leave only local INVALID
control metadata: never infer success or retry an ambiguous upload.

Run python3 -B run-prepared.py selfcheck for the eight pure-config cases.
The worker's static check is not root runtime admission or a performance result.
Storage/evaluator objections in protocol.json are source-checked simulated
perspectives, not expert testimony or an extra review. Commit/bundle then HOLD.
