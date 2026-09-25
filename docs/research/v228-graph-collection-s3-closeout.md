# V228 graph collection S3 closeout

**Decision:** The single collection head, authenticated mutation snapshot,
pinned readers and staged base-root switch passed a real S3 transaction
gate. Continue to a 100k mutation query-quality and latency falsifier.

The sole Causality c7i.4xlarge Spot attempt `a0001` ran on
`i-0c42e103426110894` in `eu-central-1c` and is terminated. Source
commit `236e29e8fef01c30434b6827d17c6dd3d58153bc`, source archive
SHA-256 `eaa7e204e60e95d1e5002e22bc53fe841c977a211d54d903e24c57bc296faa49`,
terminal SHA-256 `0d71f2e9879d6b9fad4d68f81753596486b632f390fbdedba39dd22dcb31e651`.
The complete terminal and all five artifacts passed independent length
and SHA-256 readback. Immutable evidence is under
`s3://borsuk-bench-453182569524-euc1/research/v228-graph-collection-s3/236e29e8fef01c30434b6827d17c6dd3d58153bc/runs/a0001/`.

The four-row deterministic S3 graph held public IDs `[42,7,19,33]` at
collection revision 1. Revision 2 deleted 42 and put 99 with the same
vector, returning `[99,7,19,33]`. A reader pinned before that CAS still
returned `[42,7,19,33]`. Staging the next authenticated five-blob graph
root did not move the original graph head. Collection revision 3 switched
to the staged graph with an empty mutation snapshot, again returning
`[99,7,19,33]`; the held reader remained unchanged. A stale revision-1
CAS was rejected and revision 3 stayed current.

Cold initial graph hydration used five blob GETs and 66,078 response
bytes. The mutation-only revision reused cached graph blobs with zero
GETs and zero blob response bytes. The new base root used five GETs and
66,078 bytes. The gated example took 2.67 seconds wall and peaked at
22,756 KiB (23,302,144 bytes) RSS. The launch-time Spot quote was
$0.3631/hour; compute to terminal is estimated at $0.02519, excluding
EBS, S3, billing rounding and termination tail.

This gate proves four-row S3 publication and reader isolation. It does
not prove a generic compaction builder, 100k/1M mutation recall or
service latency. The next single measurement is a frozen 100k same-vector
upsert panel against V218; its exact GT100 witness remains valid because
public IDs and vector values remain unchanged.
