**Choose one fixed SQ4 refinement arm in the original physical order, retaining the frozen PQ64 graph nomination.** This directly tests the byte bottleneck without another packing permutation or centroid-routing retry. It is a falsifier, not a production architecture freeze.

The decisive calculation comes from the [closed original-layout cover metadata](/home/rb/worktrees/borsuk-prod-ready-v9/docs/research/performance-architecture-20260930/semantic-1m/fine-sq8-groups/paired100k/a0002/frozen-cover-arithmetic.json):

| Dataset | Maximum rows covered by 32 ranges | Existing SQ8 bytes | Calculated SQ4 bytes |
|---|---:|---:|---:|
| ReLAION | 41,984 | 32,747,520 | **16,625,664** |
| CoHere | 34,720 | 27,081,600 | **13,749,120** |

A record containing ID, reconstructed norm and packed D768 nibbles costs `12 + 768/2 = 396` bytes. Both maxima fit **16,777,216 bytes**, conditional on resident authentication metadata and no additional payload framing. Worst-case headroom is only **151,552 bytes**. This is arithmetic over closed plans; SQ4 quality remains unmeasured.

The alternatives compare as follows:

| Option | Assessment |
|---|---|
| Coarser source-trained IVF, bounded parallel cell fetching | Attractive resident-memory reduction, but another centroid selector needs stronger justification. Exact extent means already reached only **91.78%** CoHere coverage; normalized sub-prototypes reached **93.06%**. Global24 barely improved hierarchy coverage. |
| Compressed row graph with semantically aligned groups | Retains the strongest nomination evidence. Compact adjacency could reduce RAM, but cannot itself reduce fetched SQ8 bytes. The new graph-affinity packing result rejects its fixed locality rule; preserving nominees also does not preserve CoHere’s incidental-row rescue. |
| **Same graph/order, SQ4 refinement** | Changes the offending per-row transfer cost while preserving nomination and explicitly retaining surrounding rows. Its independent risk is ranking distortion, cheaply measurable. |

IVF with a centroid graph is an established mechanism, but that supports feasibility of the design pattern, not BORSUK recall under this envelope. [Faiss documentation](https://github.com/facebookresearch/faiss/wiki/The-index-factory)

The historical failures remain intact: V139’s threshold admission was unbounded; V146’s full centroid scan failed scaling; V149 lost discovery through physical summaries; V150 lost both discovery and distinct-page coverage. Hierarchy8–24, global24 and boundary overlap failed coverage. Even the perfect 32-cell selector on the unchanged hierarchy reaches only **97.17% mean / 88 p05** for CoHere. None establishes that this SQ4 arm works. [Cell oracle decision](/home/rb/worktrees/borsuk-prod-ready-v9/docs/research/performance-architecture-20260930/semantic-1m/hierarchical-cells/source-witness-router/a0001/decision.md)

1. **Freeze one codec and one plan rule.** Retain source order, group16, graph, PQ books/codes, nominees and coefficients. Quantize each existing SQ8 code to its nearest multiple of 17, store the quotient as a nibble, and reconstruct through the original coefficients. Recompute the reconstructed squared norm; copying the SQ8 norm would be incorrect. No query/GT fitting, per-group coefficient additions, replication or second refinement wave.

   Run `cover_pages(..., row_bytes=396, page_rows=16, max_gets=32)`. Its deterministic smallest-gap ordering makes the 32-range row set a superset of the corresponding 256-range set; verify this explicitly.

2. **Use the smallest native falsifier.** Authenticate the original SQ8 authority, sealed nominee prefix and actual request identities. Encode and seal both SQ4 datasets before opening queries; seal all candidate plans/results before truth evaluation.

   On the same consumed64 panels, score every fetched row. Also score **the identical row sets with SQ8** as a local quality reference, charging its separate reads and memory. That reference exceeds the candidate transfer envelope on some queries and is not an eligible 32-GET serving result.

   **Reject this fixed SQ4 arm** if either dataset has returned mean R@100 below 98%, sorted p05 index3 below 95, any lost nominee, or any candidate exceeding 32 ranges/16 MiB. Report containment, fetched coverage, returned recall and the paired SQ4-versus-SQ8 ranking loss separately. Preserve infrastructure/authentication failures as INVALID.

   Proposed execution bound: CPU1, 1 GiB, no swap, ten minutes, datasets sequentially, no graph/PQ rebuild. Root validates admission and canary, then freezes resources/source. No parameter ladder after failure.

3. **Keep implementation narrow.** Add a diagnostic codec/probe in [fine_sq8_groups.rs](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/fine_sq8_groups.rs), exposed through the existing [native diagnostic CLI](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/bin/hierarchical_semantic_cells.rs). For this falsifier, expand a bounded SQ4 range into existing SQ8-shaped records and reuse the current scorer.

   One tiny end-to-end fixture should independently check nibble decoding, recomputed norms, nonunit queries, ties, odd dimensions, tail groups, corruption rejection and complete nominee coverage. Existing snapshot/pin regressions remain required. A serving survivor needs a new format marker binding the unchanged router identity separately from the new payload identity.

4. **Charge the actual 100M architecture.** Let \(N=10^8\), \(D=768\), \(g=\lceil N/16\rceil\), \(B=16\) MiB and \(E\) be encoded graph bytes. Projections—not RSS measurements:

   \[
   R=G_{\mathrm{heap}}+68N+8N+32g+786432+2\text{ MiB}.
   \]

   Using the current conservative \(G_{\mathrm{heap}}\le512N\), residency remains **59.003 GB per distinct generation**. SQ4 does not fix it.

   \[
   Q=4N+3B+8\text{ MiB}
      +256\min(N,\lfloor B/396\rfloor)+4D+X_{\mathrm{decode}}.
   \]

   This is **469.57 MB/query before expansion buffers**; the diagnostic must additionally charge every live expanded copy, up to 33.05 MB each.

   \[
   M_{\mathrm{peak}}
   =R_{\mathrm{unique\ pinned}}+C Q+M_{\mathrm{open}}
    +M_{\Delta}+M_{\mathrm{maintenance}}+M_{\mathrm{runtime}}.
   \]

   Two distinct full generations plus 32 queries already project **133.03 GB before extras**. Shared unchanged graph/PQ allocations count once.

   Persistent index bytes become approximately **47.001 GB + \(E\)**, excluding canonical originals, WAL and retained generations. Transcoding reads 78 GB and writes 39.6 GB of records. The current full-builder admission extrapolates to **2.6624 TB**, while canonical FP32 records alone occupy 308 GB. Neither establishes a viable 100M builder; retain the current geometry guard.

5. **Keep lifecycle in scope.** New-ID insertion still requires a bounded, query-visible durable delta; deletion/update need version visibility, replay and suppression. Budget source-bearing delta rows as approximately  
   \(U[(4D+8)+396+m_{\Delta}]\), plus tombstones and indexes. Compaction must publish authenticated immutable artifacts, preserve old-reader pins and reclaim only after release. Qualify maintenance memory, rewrite bytes and update backpressure before production promotion.

The main risk is SQ4 ranking loss: CoHere currently has only **0.516 percentage points** above the mean floor. Moreover, calculated mean payload becomes **6.27/8.49 MB**, above today’s **5.31/6.88 MB**; fewer GETs do not establish better QPS or cost.

No edits, Cargo, ANN execution, cloud work or corpus/GT hydration were performed. No fresh1M, cold-S3, lifecycle or vendor-win claim follows.
