# Single-host reader lifetime coordination

Next destructive GC requires an exclusive reclamation boundary. Add coordinated
latest-reader open holding a shared standard file lock from BEFORE head recovery
through index destruction. Owned compaction workers hold that same shared lock
through completion, including caller cancellation. Bind the maintenance directory
to the index prefix under a short initialization lock. Existing direct readers
remain explicitly unmanaged. No remote deletion in this increment.

Scope is cooperating processes sharing one host/filesystem and the SAME directory
and bucket/index. Other hosts, independent direct APIs or replaced lockfiles are
outside that guarantee. Keep remote GC disabled until authoritative pins cover
all readers/writers. Cross-host durable pin/fence protocol is a separate gate.

One AWS causality Spot red/green functional check,2integration+4HTTP tests.
Missing open_coordinated API proves red; exclusive lock must fail during reader
lifetime, coordinated reader/compactor must fail under exclusive lock, releasing
reader unlocks reclamation; shared readers and compaction coexist, wrong namespace
rejects. Packageclean between archives.30min/$0.30 compute cap. No local Cargo,
Spark, quality panel or vendor claim. Native dual critique remains cooldown-held.

Crash boundary: filesystem lock release on process death does not cancel an
already-submitted S3 head CAS. Shared lock coordination is necessary reader
lifetime evidence, not sufficient publication fencing or destructive-GC proof.
No reclamation is enabled until the remote publication protocol closes that race.
