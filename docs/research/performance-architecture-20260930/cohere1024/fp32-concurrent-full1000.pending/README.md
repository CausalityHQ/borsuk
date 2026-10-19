# FP32 concurrent Full1000: pending root execution

One matched Q1 (active 1) / Q2 (active 2) experiment on the frozen Cohere
1000-query cohort. Same multi_thread2, fetch64, Offload2, application cache0,
leaf64, FP32 shortlist16. Only active query count changes between arms;
per-cell scratch paths differ for isolation. This is fixture glue only.

Base afab20a8c2f58d326c4502065a80e61f2a9be680; crates tree
ab0730bfc49fe9587705275a464cb54f3813c1d2. Native qualification is retained
from 0cddd79569db2f92bf2079f1d9bbc3b90759a895 (154 checks, Clippy,
real test build and release). No new compilation, review, Rust, schema or
library changes. Immutable E0 binaries and source/config/template hashes are
checked by preparation/runtime; qualification host provenance stays unchanged.

Root API, on i-09f8571d0edf296e9 only:

```sh
P=/mnt/borsuk-workspace/fp32-concurrent-full1000-packet-a0001/docs/research/performance-architecture-20260930/cohere1024/fp32-concurrent-full1000.pending
python3 -B "$P/prepare.py"
bash "$P/launch.sh"
```

Preparation opens metadata only and writes fresh files under
/mnt/borsuk-workspace/evidence/fp32-concurrent-full1000-root-a0001 (E).
The root supplies E/gold-authority.json, E/gold-full1000.json and the
existing E/qualified-native-authority.json bound to the new execution host.
Preparation validates and hashes that authority without overwriting it; E0
retains the original qualification-host provenance.
Do not invoke preparation or launch twice. If any step fails, preserve this
attempt and let the root decide; there is no automatic retry or resume.
The child runs selfcheck.py only, in a CPU1/512MiB/swap0/tasks128/120s unit.

Launch refuses at/after 2026-10-19 21:05 UTC. The guest shutdown timer is
22:20 UTC; external shutdown remains 22:25 UTC under root control. Launch
creates one 4200-second systemd unit: CPUs0-7, CPU800%, memory8GiB, swap0,
tasks1024, both cargo-target and native-benchmark locks, control-group cleanup.
Guest credentials are EC2 instance role only, with no guest --profile.

Order: separate admit-A/B (untraced diagnostic ordinals0..7), then canary-A/B
(untraced ordinals0..1), then measured Full1000 A1/B1/B2/A2 (execution full,
count1000). Every process gets fresh output and every cell fresh scratch.
All four prelude cells must verify before measurements start.

Budget: prelude 4*(native60+5 grace+reader30+5 grace)=400s; measured
4*(native600+5+reader120+5)=2920s; four checkpoints each with a 20s conditional
PUT and 20s checksum HEAD=160s; reserve720s; total4200s. Checkpoints happen
after verified cells, outside native timing, with attempts1/connect5/read10.
Completed cells and checkpoints survive interruption; incomplete/missing
terminal output, timeout, OOM, interruption or malformed output is INVALID,
never an algorithm-negative result. No automatic retry.

Headers come from immutable retained UNBOUND templates plus independently
serialized config and native authority, never measured output. Source mapping:
check_cohere_native_baseline.rs lines139-147 and3543-3662 at the pinned base.
The unchanged strict reader recomputes startup/model admission; no guessed
memory pin. The reader config authenticates completed JSONL bytes and headers.

Every query must match historical gold answers, score bits, physical ordinals,
plans, charges, trace and canonical shortlist, and pass the retained independent
f64 canonical-row oracle. Runtime authenticates requests, truth, manifest and
row digests. Gold is from immutable cold-source-overlap-full1000-a0002/B1.jsonl,
SHA256 69b56a0464d9db152625b32051438fdc644785f6dc18564f68e0980a06e6b8b1.
It is a quality reference only: no cross-source timing comparison. Actual
recall hits/denominator are reported; perfect quality is not assumed.

Report cohort QPS and query p50/p90/p95/p99 on this frozen 1000-query cohort.
Never sum overlapping per-query CPU or transport counters; these must be null.
SDK S3+IMDS submission/payload counts are not wire or billed measurements.
Application cache0 does not prove provider cache eviction. This is not a vendor
win or generalization claim. Keep useful quality/time/memory Pareto outcomes.

Artifacts: per-cell configs, independent expected headers, raw JSONL, resource
and exit receipts, strict-reader reports, exact/oracle verification, four S3
checkpoint receipts, result.json, terminal.json and controller.exit.
Root verifies/integrates/runs; child must not launch native/data/S3 or cloud
controls, push/merge, spawn children, or read requests/truth/gold/canonical data.
