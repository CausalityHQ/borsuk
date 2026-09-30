# Bounded metadata range geometry development gate

2026-09-30. Preparation only; no new cloud job or performance result.

## Intervention and authority

Control: exact `1224634b` native source, including the authenticated graph
digest reuse, with 4 concurrent 8 MiB ranges. Candidate differs only in
staging constants: 8 concurrent 4 MiB ranges. Tests follow the declared
concurrency. Both retain a 32 MiB staging payload bound, serial objects,
HEAD admission, exact lengths, root/child authentication, synchronous scratch
writes, and cancellation cleanup. SDK overhead is outside the payload bound.
No new API, environment switch, format, scorer, query cap, or cache.

Freeze and authenticate both full native source identities and compiled
binaries. Build both with the same locked ARM toolchain on one bounded Spot
worker; focused staging/generation/HTTP/source checks precede measurement.
Do not reuse the old qualified binary as matched control: it lacks the graph
digest change. Historical assurance is not a current full-suite claim.

## Protocol and gates

Reuse the closed metadata ranges campaign's exact FIRST 1M D768 cosine
ReLAION and CoHere inputs, previously observed development queries 0–63,
k=10, cold process/namespace protocol, ABBA order, 64 calls per arm/dataset,
CPU affinity, caller memory, no application SQ8 cache and no HTTP retries.
Immutable source/config/inputs and Spot identities are required. A Spot
interruption invalidates its measurement cell; no overlapping replacement.
Owned instance termination precedes artifact collection.

Resource envelope: one eu-central-1 c7g.2xlarge Spot worker under profile
`causality`, maximum bid $0.30/hour, 80 GiB encrypted delete-on-termination
gp3. Machine cap 4200 seconds; the combined two-build qualification phase is
capped at 2400 seconds plus 30 seconds cleanup, and measurement at 1500 plus
30 seconds cleanup. Build: 10 GiB cgroup, zero swap, four CPUs; profile:
8 GiB cgroup, zero swap, 4 GiB address-space limit, 1 GiB caller admission,
client CPUs 4–5/native 0–3. Compute ceiling $0.35 plus $0.15 EBS/S3 allowance
is an estimate, not an invoice. Both binary builds must invalidate changed
crate outputs; source restoration and hashes are checked before each build.

All 256 calls must succeed, preserve ordered IDs and physical query counters,
and each arm/dataset must meet mean recall@10 >=95%. Development GO requires
candidate cold start-to-first-response p90 below matched control on BOTH
datasets. Preserve FAIL otherwise. Report p50/p90/p95/p99, paired differences,
stage/decode/write intervals, logical metadata GET/HEAD counts, bytes, RSS,
cgroup peaks, errors/retries and estimated compute separately from total cost.
Report the contextual published 444 ms cold p90 comparison separately; missing
it is not an invented universal quality/architecture KILL. No vendor win,
8 QPS attainment, R100 or 100M scalability claim follows from this gate.

Before launch: controller must authenticate both frozen epochs, test the
actual lifecycle and verify geometry accounting for each arm. Reuse existing
owned Spot lifecycle and independent record validator. No paid launch for the
graph digest optimization alone and no repeated full-workspace gate.
If geometry survives, the next measurement is the bounded cold offered-load
curve; if it fails, retain the valid prior candidate and identify the measured
transfer/decode bottleneck rather than relabeling the gate.

## Current evidence

Prior 4x8 MiB binary (earlier native epoch): verified closed ABBA p90
2164.242 ms ReLAION, 2202.817 ms CoHere; R10 99.375%/96.875%.
Latest authenticated closed offered records at 1 QPS: p90
2037.488/2016.007 ms and the same R10, 64/64 successes each. That campaign
terminal FAILED its postmeasurement resource assertion; records were verified
separately, not retroactively declared a passing campaign. These are historical
references, not measurements of either new arm. No projected speedup is used.

## Preparation evidence

Controller child `28b8d549` was reviewed and integrated as `5235520b`.
Parent original mock execution 53109 closed with exit 0: source/config/code
rejection, both five-class builds, crate-output invalidation, restoration on
failure, graph-count and toolchain-parity rejection, transient observations,
ACK ownership and termination before collection. No compute was launched by
these checks. Actual preflight binds 395 native files per arm, nine compiled
snapshots per arm, eleven runtime/controller code files and sixty artifacts;
generated user data is 10,527 bytes and passes shell syntax checking.

Config SHA256: `410ce351d764bccccafcfe6e34697dcc961856288c67f8519c451fa4266143c3`.
Control native identity: `6566a30c7ccfbf5ec8a8d4481b2568fc020b67e831e5d186a94f5025ab156ec2`.
Candidate native identity: `4bbc6c332e78e5a068001913597cdc82d6c13bd147acd96e019149c190a39a1c`.
The candidate additionally corrects its buffer documentation to 32 MiB; this
changes no behavior. Local staging execution 29333 passed all four affected
tests before that comment correction. Fresh ARM qualification precedes queries.

Geometry record and binary-authority checks pass, including rejection of a
wrong fresh-control GET count, source identity, binary size, compiled graph
hash, false full-suite claim and oversized concurrency. The legacy default
cold-call cleanup/parity check passes. The independent closeout reader's
reduction was checked synthetically; its full remote path awaits real receipts.
An unchanged baseline repository-policy check fails because the package
metadata test lacks its expected repository URL string. No full native suite
was repeated or claimed, and that unrelated source was not modified.
