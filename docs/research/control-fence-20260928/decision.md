# GO: one durable publication authority

Replace the separate mutable generation/mutation heads with required
`borsuk-two-bit-head-v2` at index/head.json. It holds root/generation, mutation
revision/digest/seal, advancing epoch and write fence. No old control reader,
migration layer or historical mutable mutation head. Already-decoded snapshots
continue serving pinned readers; latest recovery on a retired root now rejects.

Generation publication captures the sealed control version before uploads.
Mutation publication uses its recovered control token. Fence entry authenticates
the current root, CAS-freezes both paths and can recover an interrupted fence;
exit preserves all references and advances epoch. Exact control-body readback
reconciles lost acknowledgements. Advancing epochs avoid recreating the earlier
control body on release. No remote deletion is enabled.

## Verified functional gate

One original AWS causality eu-central-1 c7i.4xlarge Spot worker,
`i-09ef230f601dbd6b9`, terminal exit0/complete, verified terminated,277s.
Missing-API red exit101; green3 integration tests plus4 existing HTTP tests pass.
The new integration test pauses actual mutation and empty-generation publishers
at their final control PUT, fences/release, deletes their unreferenced staged
objects, then releases the delayed writes. Both CAS requests fail and original
state remains readable. Fence entry/exit lost acknowledgements, interrupted-fence
resume and old-fence-token rejection are covered. Existing mutation, seal,
compaction/empty transitions, corruption and HTTP query checks also pass.

These are in-memory object-store CAS faults plus existing HTTP functional tests,
not an authenticated live-S3 reclamation benchmark. Small D2 fixtures (512 source
rows in the existing integration and a one-row delta/empty base in the new race).
Frozen source SHA2808fcb892f68b6a95bf1ffcfbabe56a97a0158305f23ca5e752f335ebd6913f.
All three changed Rust files match the archive; terminal/every artifact length
andSHA verify. Compute estimate$0.0279 excludes EBS/S3, not an invoice. No local
Cargo, Spark, duplicate worker or benchmark. Prior review cooldown remains held;
no override or duplicate consultation.

## Next single gate

Bounded callable reclamation under the SAME directory's exclusive reader gate
and durable write fence. Authenticate the keep-set, delete only recognized owned
objects, preserve current root/SQ8/canonical/delta, handle partial delete/restart
and late orphan uploads. Add content-body/epoch assertions across fence release
and native live-S3 conditional-write validation alongside that gate. A fence is
not reader quiescence. Other-host/unmanaged readers remain outside the local pin
protocol; safe distributed GC must not be claimed.

Matched BOTH-vendor quality/e2e/cost and100M qualification remain OPEN. No new
recall, latency, throughput, RSS, lifecycle-cost or vendor-win measurement.
