**Verdict: minimal contract repairs first; then GO for the bounded Rust implementation in `compare_native_replay.rs`.** The diagnostic is worthwhile, but its current summary leaves two gaps in the geometry question.

I read all three pending documents fully and inspected the named committed documents and connected code through `git show e359f0a33661107f55861af9a17d5fe58f56d432`. The historical reconciliation is sound: leaf nomination is the established first loss; both 312/320 KILLs remain valid; q537 remains separate. The stored/resident corrections are also sound.

1. **Blocker — outside top48 does not imply outside SOURCE scoring.**  
   The proposed summary counts owning-leaf ranks, but an unselected leaf’s rows can share a physical page with nominated units and therefore enter SOURCE-scored closure. High cross-leaf or outside-top48 mass could describe fragmentation with no present coverage loss. This follows directly from the committed layer reconciliation.

   **Smallest fix:** For each sealed source anchor, add counts outside the frozen selected-leaf union **and separately outside its physical-page closure**. Derive these from authenticated membership and existing nomination; open no SOURCE payload, requests, truth or producer. Keep fetched gap bridges separate.

   **Required falsifier:** A neighbor belongs to an unselected leaf but shares a page with a selected unit. It must count outside selected leaves and inside SOURCE closure. This measures new source geometry, without repeating historical attribution.

2. **Blocker — the proposed statistics cannot establish unit-level resolution.**  
   Own-unit containment and distinct target leaves do not reveal whether neighbors concentrate in coherent destination units or select isolated rows from internally mixed units. Furthermore, 32 neighbors necessarily occupy at most 32 destination units: showing that these fit within a 64-unit leaf is tautological. One anchor per unit leaves the other rows’ affinities unobserved.

   **Smallest fix:** Report per-anchor destination-unit frequencies and canonical within-unit dispersion for the 32 selected units. Keep this bounded; no additional neighborhood searches are needed. Replace “locality below units implies insufficient resolution” with a predeclared **inconclusive** outcome unless those canonical statistics support it. [Current interpretation](/data/target/borsuk-post-boundary-next-arm.pending/decision.pending.md:19).

   **Required falsifier:** Homogeneous units and units mixing opposing directions can have similar leaf-containment counts; the diagnostic must distinguish their canonical dispersion. Zero canonical means are valid for these L2 statistics.

   Treat anchors as the sampling units; the 1,024 neighbor slots are correlated. The draft correctly limits negative evidence to “no support from these anchors.” Preserve that wording. These anchors cannot establish the geometry of the historical misses or qualify a unit graph.

3. **Admission blocker — explicitly bind canonical bytes to the generation descriptor.**  
   At this revision, `coverage_router` checks canonical geometry and SHA syntax around lines 5098–5105; it never authenticates the canonical body. `loss_binary` hydrates its entire input around lines 4753–4764. Neither is a ready-made canonical streaming validator.

   **Smallest fix:** The new source-only admission must require the configured canonical SHA/length to equal the pinned generation’s `canonical` descriptor, validate its object-key binding, and stream exact records through SHA/EOF validation. Under the explicit ordinal fixture contract, verify ID range, uniqueness and the ordered-ID digest against `source_order_sha256`. Keep canonical, raw-source and construction-root digests distinct. Retain descriptor/path identity stamps through computation and publication.

   Reuse low-level snapshot and publication helpers, while giving the new branch its own strict config. Existing `CoverageConfig` includes requests, truth and completed-producer fields.

   **Required falsifier:** Reject a same-sized, independently hash-valid canonical file belonging to another generation; reject a permuted ID order, duplicate ID, trailing byte and mutation between validation and publication before producing successful evidence.

4. **Reproducibility blocker — finish the numerical contract.**  
   Hash domains and “digest modulo row count” still lack literal bytes and a defined integer interpretation. Residual formulas and router-input normalization also need explicit definitions.

   **Smallest fix:** Freeze domain literals, digest ordering/reduction, partial-unit row counts, scalar coordinate order, cosine clamping and tie comparison. Exclude only the authenticated self ID; retain other IDs with identical coordinates. Require exactly the admitted neighbor count.

   Use actual stored-coordinate norms for reference cosine. For router ranks, use production normalization and the existing squared-L2 ranking with leaf-ID ties. Stored unit means are unnormalized SQ8 reconstructions averaged into FP16; root prototypes are **unweighted means of decoded unit means**, including a partial final unit. Fresh canonical means and their residuals need separate names. The draft’s prohibition on attributing SQ8 versus FP16 loss from aggregate differences is correct.

   **Required falsifier:** Independent golden hashes and scalar neighbor expectations covering partial units, nonidentity IDs, nonunit norms, duplicate coordinates, self exclusion and ties; verify frozen L2 rank parity separately.

5. **Resource blocker — component sizes are not a phase coexistence model.**  
   With the existing full decoder, the 64,000,032-byte encoded centroid body can coexist with 128,125,000 decoded-array bytes: approximately **192.125 MB before** router copies, membership/directory arrays, buffers, retained statistics and serialization. Canonical page cache is charged memory too.

   **Smallest fix:** Enumerate live allocations and drops for admission, validation, sealing, neighborhood scanning, residual computation and publication. Account for bounded heaps, any retained neighbor coordinates, canonical-unit statistics, serialization/readback copies and runtime overhead.

   Freeze a staging allowlist and peak scratch coexistence. Canonical plus raw FP32 plus SQ8 already totals **9.236 GB**, exceeding 8 GiB. This diagnostic needs no raw-source or SQ8 body staging. Use downloads followed by rename, and explicitly count temporary files.

   **Required falsifier:** Allocation refusal before large-body opening, plus a separate disposable smoke that verifies charged memory/cache, zero swap, scratch limits and cleanup. Actual 1M completion remains unproved. CPU1/1GiB/1200s is a cap, not a prediction.

6. **Publication blocker — define success across abrupt termination.**  
   The existing publication helper writes the complete JSON before final directory sync and identity checks. Returned errors trigger invalidation; SIGKILL or OOM can bypass that cleanup and leave apparently successful JSON.

   **Smallest fix:** Require the durable publication result, matching report SHA and successful runtime terminal receipt together for acceptance. An anchor seal or surviving `MEASURED` text alone cannot establish success. Preserve interruption as INVALID and retain the latest draft’s no-automatic-retry rule.

   **Required falsifier:** Interrupt after payload writing but before final publication completion; the receiving gate must reject the surviving file. Also exercise file/directory sync and identity-recheck failures.

Graph-cut grouping remains a plausible hypothesis. Lower edge cut does not establish better fixed48 prototype nomination or physical-page cost, and these measurements cannot rank it as a winner over alternatives. For the smallest first implementation, omit optional adjacency analysis; it is unnecessary to answer the primary geometry question.

After these repairs, the example-only scope is appropriate. Native correctness, compilation, Clippy, complete test-build, exact-source admission and separate smoke remain root-owned on causality EC2. No ANN arm, serving adoption, cold-quality claim or 100M qualification follows.

No files were edited. No Cargo, native execution, tests, mocks, benchmark data, network or children were used.
