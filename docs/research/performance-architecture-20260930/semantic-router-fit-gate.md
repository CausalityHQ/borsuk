# Local semantic-router construction gate

Preregistered before real corpus fitting. This supplements the frozen coverage
falsifier; it authorizes no query execution until runtime qualification passes.

After exact-source compilation, Clippy, complete test compilation, final
workspace execution and review reconciliation pass, fit the authenticated
FIRST100k D768 historical centroid inputs in order ReLAION, then CoHere.
The input authority JSON and closed terminal body hashes remain mandatory.
Use the exact qualified binary SHA; record source commit and binary bytes/SHA.
No truth or query file is supplied to construction.

Each fit is a separate local systemd user service with MemoryMax=256MiB,
MemorySwapMax=0, CPUQuota=200%, TasksMax=32 and RuntimeMaxSec=900.
Set BORSUK_CPU_THREADS=2 and restrict execution to CPUs 0–1. No other fitting
job overlaps. Record effective limits, memory peak/events, cpu.stat, exit status,
wall time, stdout/stderr and /usr/bin/time maximum RSS. These are construction
cost measurements, not serving latency. The builder's <=128MiB allocation model
is separate from the enforced 256MiB process cgroup budget.

Publish into a new attempt directory without replacing output. Authenticate
manifest and every emitted file after success; record manifest SHA, root bytes,
leaf count/max bytes, total payload and membership bytes. Reject any failed
identity, limit or incomplete output. An OOM/timeout is a construction failure;
report it rather than quietly increasing limits or repeating the same fit.

Only qualified outputs reach the fixed coverage evaluator. Its first nomination
failure ends the arm; no cloud expenditure or scoring run is earned by a
construction success alone. No 1M/100M memory, quality, latency or maintenance
claim follows from this 100k gate.

Before corpus fitting, run one distinct synthetic 100k-row/D768 construction
(3,125 units) under the same cgroup limits. Generate each FP16 coordinate from
`((unit*31+dimension*17)%257-128)/4096`, replacing dimension zero with
`unit/4096`; this is deterministic, finite and gives distinct units. Authenticate
a synthetic v4 root and exact BORSUCP1 header/body. Record measured peak RSS,
manifest size, leaf geometry, construction exit and identities. This tests the
maximum geometry and recursive path; its quality has no dataset meaning.
