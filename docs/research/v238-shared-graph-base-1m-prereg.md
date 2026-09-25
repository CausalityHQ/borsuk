# V238 shared base resident-memory probe, ReLAION-1M

One `causality` c7i.4xlarge Spot host in `eu-central-1c`, a fresh
attempt prefix and the immutable V236 revision-2 S3 collection. The
host starts with an empty local cache, authenticates the pinned root
and mutation snapshot, hydrates one decoded reader, then constructs a
second decoded overlay with `hydrate_graph_collection_decoded_reusing_base`.
The production method must use the same `Arc` base and return identical
IDs for a deterministic cosine query while both readers remain pinned.
This memory probe reuses the **same revision-2 snapshot**; the unit
test at source checks a changed mutation revision and wrong-root
rejection. It does not remeasure 1M recall or HTTP latency.

Authority: V236 collection root SHA-256
`c59650ec920d031ff236f5ab47331db88b71fdac0462cd07a568d51c549b7caf`,
mutation SHA-256
`6b9ba6391dff9de0867447045504beaa1c22776ccf9502d4065c7a3b76ab4153`.
Dataset ReLAION-1M D768, cosine k=100, 10,000 same-vector upserts.
The old full rehydration/swap peaked at 4,346,863,616 bytes RSS in V236.

Gate: revision/root/snapshot identities match; initial graph hydration
uses exactly five blob GETs and 1,879,697,462 response bytes; both
overlays use the same base pointer; one deterministic k=100 query
returns identical complete ID lists; steady RSS increase after the
second overlay is ≤256 MiB and whole-process peak RSS ≤2.5 GiB.
Record both overlay-owned byte counts, cold hydration and reuse wall
times, steady/peak RSS, instance identity and compute cost. The reuse
method has no object-store handle, so it cannot fetch graph blobs.
Metadata GETs in initial head reading are uninstrumented.

Freeze source and Spot quote in an immutable reservation. On
interruption, discard and restart under a new prefix. Seal terminal
and artifact hashes; terminate immediately after the terminal marker.
If the memory gate passes, use same-root reuse for mutation-only slot
swaps and retain full hydration for root-changing compaction. The
compaction admission envelope must be set by recall, latency and RAM
budgets without a fixed vector-count knee.
