# Early native source review: not execution evidence

Root inspected the active child draft on2026-10-05 before source freeze or Cargo. The draft remains UNVERIFIED; no native failure or measured RSS is claimed.

## Required producer/reader repair

`PqVectorGraph::build` reuses `build_reachable_hnsw_adjacency(...,m32,m0=64,efconstruction128)`. Existing `centroid_hnsw::repair_reachable_hnsw_adjacency` appends a source-order backbone edge and reverse reachability edges without evicting incoming paths, allowing degrees up to256. The draft authenticated reader rejects base degrees>64. Therefore a valid repaired producer artifact may fail reopening. Preserve the fixed builder and actual repair algorithm; reader bounds and aggregate admission must admit its legitimate output without hiding it by pruning. Add actual build/write/plane-free reopen/nominate regression, with a repaired>64 degree and malformed bound negative.

## Required reusable identity binding

Draft public `nominate_pq` checks only PQ rows/dimensions. Same-geometry codebooks must not be substituted for the graph's authenticated PQ identity. The API must prove the exact PQ binding or restrict access through an opaque already-validated binding. Include swapped same-size codebook/codes negative.

Both requirements were sent to SAME child in durable message1791181524115668685-2054090; no new child, review, compiler or experiment was started. Independently completed engineering critic68079a4e6c114408 already requires authenticated aggregate identity and actual builder capacity accounting.

## Subsequent draft inspection

The live draft now admits repaired base degrees up to256 and checks the exact PQ artifact digest. These are source observations; compilation and regression execution remain pending.

Two additional findings were sent to the same worker before freeze:

- `FineSq8Index::search_excluding` and the paired CLI reject evaluation exhaustion. The prospective method requires scoring the already-collected bounded shortlist, recording exhaustion separately from convergence and retaining denominator100 and underfill. An algorithmic cap is not an authentication INVALID. Messages1791181948487949477-2054090 and1791182070781501175-2054090 identify both sites.
- Plans bind only a numeric mutation revision. Two divergent snapshots created from one parent can share that revision, allowing a plan to cross snapshots. Bind exact immutable snapshot content, and test same-revision divergence, old-generation pinning and mutation ID-domain rejection. Message1791181969615253285-2054090 identifies this finding.

The paired draft freezes and syncs all128 plans before either dataset truth body is opened. This source ordering still needs the actual native pipeline negative tests and external resource closure; it is not scientific qualification.

The same worker subsequently implemented explicit convergence/actual-shortlist fields and exact mutation-content plan binding. Its new S3 method reuses the existing one-attempt conditional reader. Root found that partial failures initially labelled planned bytes as attempted bytes and verified bytes as read bytes; message1791182632354858493-2054090 required preserving only known reader observations. The current draft keeps planned bytes separately and serializes unavailable per-query attempted/read bytes as null, with a partial-failure regression authored. All these observations remain source-only until the exact candidate is compiled and tested remotely.
