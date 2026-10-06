# Primary research checkpoint, 2026-10-06

Turbopuffer ANN v3 (updated2026-05-05) describes hierarchical centroid routing plus binary quantization and refinement, drawing its incremental clustering foundation from SPFresh. Its published scale/latency targets are not matched BORSUK measurements. Source: https://turbopuffer.com/blog/ann-v3 .

SPANN describes balanced hierarchical clustering into posting lists, boundary closure augmentation, and query-aware pruning. This couples routing with the physical fetch unit, unlike preserving an arbitrary global graph nominee set then trying to pack its groups. Source: https://arxiv.org/abs/2111.08566 .

Inference for the next decision: investigate routing and physical payload selection together, while bounding duplication and update costs. Merely adopting mean centroids, median cuts or binary quantization is not justified: BORSUK already has closed failures of simple hierarchical routing, boundary overlap and low-bit refinement. Read those receipts before proposing a distinct mechanism. Different paper dimensions do not invalidate the mechanism; quality and resource claims require our matched D768 corpus/query gates.

Existing read-only consultation44c5af9c05414c41 remains active. No new design or source implementation is selected by this note.
