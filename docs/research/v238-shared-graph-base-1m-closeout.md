# V238 shared authenticated base, ReLAION-1M memory probe

**Decision: the shared-base memory gate passed.** A fresh Spot worker
read the V236 S3 collection revision 2, hydrated its decoded graph,
then constructed a second decoded overlay through the new same-root
library method. Both overlays pointed to the **same** authenticated
base allocation and returned the same complete k=100 ID list for the
frozen deterministic probe query. The second overlay raised steady RSS
by 46,084,096 bytes and took 81.510 ms to construct. This is a memory
probe on one snapshot, not a fresh 1M recall or HTTP comparison.

The sole `causality` c7i.4xlarge Spot attempt `a0001` on
`i-048f0881abd0675e0` in `eu-central-1c` was independently confirmed
**terminated**. Source commit
`c6ea3f71c6c612ab5de575e096d44d9b2c9f0260`, source archive
SHA-256
`24df915de9b1c35e3fbc085f5da3e4a3ea9e4502a974541b42b402caa33e157b`.
Original terminal SHA-256
`16dd9bc0100260ebe20f3a2875a493febc0c1fabc0ebd2c0f91595aad41734a8`;
closeout SHA-256
`936e98b199cd1249b5afcc174d01114b9ae861cf800fa6a37bab3110edbb89e2`.
The terminal exited zero; all five artifact lengths/hashes passed
independent replay. Evidence:
`s3://borsuk-bench-453182569524-euc1/research/v238-shared-graph-base-1m/c6ea3f71c6c612ab5de575e096d44d9b2c9f0260/runs/a0001/`.

Dataset: ReLAION-1M D768, cosine k=100, same authenticated V236
revision-2 root and 10,000-row mutation snapshot. The graph root
SHA-256 was
`c59650ec920d031ff236f5ab47331db88b71fdac0462cd07a568d51c549b7caf`;
snapshot SHA-256 was
`6b9ba6391dff9de0867447045504beaa1c22776ccf9502d4065c7a3b76ab4153`.
The source-level unit test separately covers an actual changed
mutation revision, old-reader pinning and rejection of a different
authenticated root. V238 held the same snapshot twice to isolate
the resident-memory effect.

| Verified V238 observation | Value |
| --- | ---: |
| Cold graph blob GETs / response bytes | 5 / 1,879,697,462 |
| Cold hydration wall time | 33.635 s |
| Base `Arc` shared / probe IDs equal | true / true |
| Old / new overlay-owned bytes | 31,005,000 / 31,005,000 |
| Same-root reuse wall time | 81.510 ms |
| RSS before / after second overlay | 2,062,880,768 / 2,108,964,864 bytes |
| RSS increase | 46,084,096 bytes |
| Whole-process peak RSS | 2,108,964,864 bytes |
| Spot compute estimate to terminal | $0.02642 |

V236's two **independently loaded** pinned graph readers peaked at
4,346,863,616 bytes in a different run. The V238 peak is 2.109 GB;
the separate-run comparison is descriptive, while the same-process
46.1 MB increment and shared pointer directly demonstrate this
method's resource behavior. The reuse method has no object-store
handle, so it makes no graph blob requests. Initial collection-head,
root and snapshot metadata GETs are uninstrumented. Compute cost
excludes EBS, S3 and billing adjustments.

**Production decision:** use same-root reuse for mutation-only
collection revisions. Continue to hydrate a new base for root-changing
compaction and budget simultaneous pinned roots there. Retain the
authenticated root-digest check and decoded overlay byte cap.
Next qualification is an actual new-revision swap on a larger frozen
panel, with recall/latency/RAM budgets governing mutation admission
and compaction; no fixed vector-count knee. The completed V237 HTTP
result remains the current 1M serving evidence. This memory probe
does not establish a matched S3 Vectors or Turbopuffer win.
