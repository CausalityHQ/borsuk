# Early native source review: not execution evidence

Root inspected the active child draft on2026-10-05 before source freeze or Cargo. The draft remains UNVERIFIED; no native failure or measured RSS is claimed.

## Required producer/reader repair

`PqVectorGraph::build` reuses `build_reachable_hnsw_adjacency(...,m32,m0=64,efconstruction128)`. Existing `centroid_hnsw::repair_reachable_hnsw_adjacency` appends a source-order backbone edge and reverse reachability edges without evicting incoming paths, allowing degrees up to256. The draft authenticated reader rejects base degrees>64. Therefore a valid repaired producer artifact may fail reopening. Preserve the fixed builder and actual repair algorithm; reader bounds and aggregate admission must admit its legitimate output without hiding it by pruning. Add actual build/write/plane-free reopen/nominate regression, with a repaired>64 degree and malformed bound negative.

## Required reusable identity binding

Draft public `nominate_pq` checks only PQ rows/dimensions. Same-geometry codebooks must not be substituted for the graph's authenticated PQ identity. The API must prove the exact PQ binding or restrict access through an opaque already-validated binding. Include swapped same-size codebook/codes negative.

Both requirements were sent to SAME child in durable message1791181524115668685-2054090; no new child, review, compiler or experiment was started. Independently completed engineering critic68079a4e6c114408 already requires authenticated aggregate identity and actual builder capacity accounting.
