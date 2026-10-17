# Leaf-prefix 48 versus 40: fixed ABBA packet

Parent-only execution. This worker prepared and statically checked the packet on Causality EC2 i-00bf80e4d5592efc8; no runtime admission, smoke, native reader, ANN, service queries, or measured cells were executed by the worker.

One causal change: semantic_leaf_limit. A1=48, B1=40, B2=40, A2=48; exactly 1000 identical ordered Cohere1M / D1024 / cosine / k10 requests in each full cell. Full frozen native source, producer authority, generation, requests, truth and order pins are in configs, frozen-headers and header-authority.json. Application source cache OFF, backend cache UNKNOWN. Fetch parallelism 64; existing one_retry_100ms policy unchanged. No guaranteed-cold, concurrent-throughput, production-acceptance, vendor-win, or lifecycle-win claim.

Retained canonical references: evidence/leaf-prefix-falsifier-a0001/full.leaf{40,48}.{jsonl,reader-report.json}. Frozen canonical files record their exact hashes and per-query digests of returned identities/score bits/physical ordinals/plans/traces/charges. Leaf40 must reproduce 9737/10000 hits, 34229466368 logical bytes and 127811 logical GETs; leaf48 must reproduce 9783/10000, 39690200320 bytes and 139913 GETs. Every cell must match its own canonical reference. Repeats A1/A2 and B1/B2 must match output, recall, plan and charge rows. Cross-arm returned parity is deliberately not required.

The full original freeze-headers helper is retained with only destination and immutable Git-revision lookup adaptations. It derives complete expected headers from source a0463ed097296c19539d05529ddad615be760fdd and the retained qualified recipe, before any new query. Delivery base is beeb3c8218698ccf0c569b3980112bf1b62c0184. Native and strict-reader binary hashes are in artifacts.sha256; no compilation or dependency change.

Stage API, run on this EC2 host through the parent's original SSM command handles:
    bash /mnt/borsuk-workspace/evidence/leaf-prefix40-abba-preparation-a0001/launch.sh gates
    bash /mnt/borsuk-workspace/evidence/leaf-prefix40-abba-preparation-a0001/launch.sh run
    bash /mnt/borsuk-workspace/evidence/leaf-prefix40-abba-preparation-a0001/launch.sh collect

Each command is synchronous and create-only, with a distinct systemd unit and *-control-a0001 receipt. Preserve each original SSM CommandId. Collect its terminal invocation result before starting the next stage; a send receipt is not completion. All static preparation already exists on EBS. Never rerun or overwrite an attempt. The parent must confirm no other owned job overlaps this host; the packet additionally locks its own namespace and rejects any running native benchmark/reader.

Gates: both arms execute the real runtime validator with authenticated requests, records and truth using admission ordinal 0, followed by a separate disposable two-query smoke on ordinals 32,161. Both native exits, timeout, drain and scratch-cleanup checks must be zero before measured cells. These gates use real read-only S3 data/IMDS operations; nothing is mocked. Diagnostic gates are not performance evidence. Native internally creates/removes disposable scratch; evidence receipts remain retained.

Runtime resource unit: CPUs 4-7, quota 400%, memory 8GiB, swap 0, pids 512, RuntimeMaxSec 2400, TimeoutStopSec 60, KillMode control-group. Each native full cell has a fixed 450s deadline; strict local reader 90s; admission 60s; smoke 90s. Host guard binds exact instance and guest poweroff USEC=1792284726321116, requiring stage allowance plus 60s before that horizon. The external two-hour timer is already armed and remains parent-owned. Never change guest shutdown, external timer, host resources or scientific thresholds.

Native exits (*.native.exit), timeout/wrapper exits (*.exit and *.timeout.exit), native drain (*.drain.exit), scratch cleanup (*.cleanup.exit), reader exits (*.reader.exit), manager exit and systemd runtime.exit/properties are distinct. Failure/OOM/timeout/missing exit/Spot interruption means execution INVALID, preserving incomplete cell and whole trial. No automatic retries; only the parent may authorize a restart in a new namespace. Preserve all originals, even when execution never becomes valid. Runtime termination can prevent a manager terminal marker; absence never means success.

Read only terminal markers and infrastructure health while measurements run. No timing peeks, adaptive cells, early performance rejection, or post-result threshold changes. Strict reader validates exact frozen headers, count1000, full EOF, ordered query/recall rows and seals. Closed reduction is allowed only after all native/reader/runtime exits are zero and emits canonical and same-arm parity, both temporal pairs (A1/B1 and A2/B2), p50/p90/p95/p99/max, sequential QPS, CPU, RSS, stages, logical charges and physical cumulative process transport. Physical counters include S3+IMDS submissions and consumed payload, not wire/billed bytes. Per-cell time resources and systemd peak resource receipts remain available. Repetitions are not 2000 independent queries.

Report quality/read tradeoff without arbitrary production acceptance. The closed SDK baseline 96.17% recall / p95 206ms is descriptive context only. A lower leaf limit may exchange recall for fewer reads; only these paired cells establish its latency behavior on this host.

Unrun risks: real input/credential/runtime admission, smoke, full-cell timing, Spot survival and strict-reader acceptance remain parent checks. An interrupted or resource-invalid run cannot support a scientific rejection or performance claim.

Exact parent SSM submission (replace STAGE once with gates, run, or collect; do not launch these concurrently):
    aws --profile causality --region eu-central-1 ssm send-command --instance-ids i-00bf80e4d5592efc8 --document-name AWS-RunShellScript --parameters '{"commands":["bash /mnt/borsuk-workspace/evidence/leaf-prefix40-abba-preparation-a0001/launch.sh STAGE"],"executionTimeout":["2600"]}' --comment leaf-prefix40-abba-STAGE-a0001 --query Command.CommandId --output text
    aws --profile causality --region eu-central-1 ssm get-command-invocation --instance-id i-00bf80e4d5592efc8 --command-id ORIGINAL_COMMAND_ID

The systemd-run --wait receipt retains final main exit, runtime and resource statistics even if systemd garbage-collects the unit before a later property read. An unavailable property read is marked separately, never substituted for the original runtime exit. Host job guard checks active research units and live native/build processes; parent ownership reconciliation remains required for jobs with unrelated names.
