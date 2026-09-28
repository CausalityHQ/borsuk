# Unified conditional publication authority

Replace separate mutable generation/mutation heads with one required v2 control
body: generation, root, mutation reference/seal, monotonic epoch and write fence.
No legacy reader/migration. Every conditional commit changes epoch; enter/exit
fence also changes epoch, preventing content-ETag ABA. Capture generation commit
version BEFORE uploads, not after GC. Mutation snapshot version names the same
control authority. Retired generations retain already-decoded reader snapshots;
latest mutation recovery fails on retired roots instead of an independent mutable
historical head. Compaction seals, builds and publishes through the same control.

Fence entry authenticates current root and CAS-freezes both write paths. Resume
an existing durable fence after crash; exit CAS preserves all data references,
changes epoch and reconciles lost acknowledgements. This is publication fencing,
not reader pinning or destructive GC. Same-host local exclusive coordination and
cross-host durable reader pins remain separate obligations before deletion.

Cheapest check pauses real mutation and empty-generation publication immediately
before final head PUT; fence entry/exit, delete unreferenced staged objects, then
release delayed requests: both MUST fail CAS and previous logical state survives.
Check lost fence-entry/exit ack and resume; stale fence token must fail. Existing
ID/mutation/compaction/HTTP checks catch integration regressions. One AWS causality
Spot red/green worker; source/hash/exit/closeout,30min/$0.30 compute cap, no local
Cargo/Spark or quality/benchmark panels. Prior dual-review cooldown holds.
