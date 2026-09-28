# GO: coordinated single-host reader lifetime

`TwoBitIndex::open_coordinated` locks before recovering the latest head and
holds a shared file lock until index destruction. Callable compaction's owned
worker holds that same gate through completion. Directory identity initialization
is serialized without overwrite. Exclusive reclamation lock contention rejects
new coordinated opens/compaction before remote work; live coordinated readers
prevent exclusivity. Shared readers and compaction coexist.

One original causality eu-central-1 c7i.4xlarge Spot worker:
`i-080741a97072347e8`, terminal exit0/complete, verified terminated,277s.
Missing-API red exit101; green2 integration+4 existing HTTP-layer tests pass.
Frozen source SHA5211a5dad1888171b758ac615bbe7d9d1f7f84bf1f9c7d2ac0db10cfaf8e7b2f.
All three changed Rust files match the immutable archive; terminal and every
artifact length/SHA replay verifies. Compute estimate$0.0279 excludes EBS/S3,
not an invoice. No local Cargo, Spark, duplicate worker or benchmark panel.

The functional fixture is512 source rows,D2 with tiny replacement bases.
No new recall/latency/scale/vendor measurement. The design critique request was
rejected before launch by native six-hour cooldown; no override or manual bypass.

## What this does not prove

Only cooperating processes using the SAME host/filesystem, bucket/index and
maintenance directory participate. Direct low-level APIs and other hosts are
unregistered. Never unlink/replace lockfiles while participants live. No remote
GC is enabled. Process death releases a file lock but cannot cancel an S3 request
already submitted: local locking alone cannot fence late publication.

## Next single implementation decision

Close remote publication authority before destructive GC. Evaluate replacing the
separate generation and mutation mutable heads with one versioned CAS authority
containing authenticated root plus mutation-state references and a durable GC
fence. Old generation/mutation commits must fail against a fenced version;
recovering a fence after process death must be safe. No prerelease compatibility
layer is needed. Cheapest gate: an in-flight stale conditional commit paused
before store commit, fence installation, then release; the stale write must fail
and current referenced objects remain readable. Include lost-ack/fence-recovery
checks before enabling bounded deletion. Cross-host pin coordination remains an
additional boundary; do not claim safe multihost GC from this local increment.

Both-vendor matched recall/e2e/cost,100M RAM/scaling and publishable release
qualification stay OPEN.
