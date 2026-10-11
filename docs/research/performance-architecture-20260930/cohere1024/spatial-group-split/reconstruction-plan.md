**Yes: the authenticated whole BORSUCP1 SHA is sufficient for exact construction-byte authority. Reconstruction does not need the historical root decoder.** Historical publication validity and radial control parity remain separate gates; neither follows from the centroid hash.

This follows from `Discovery::Semantic` independently pinning the complete leaf payload and construction centroid digest, while `Discovery::input` binds the source identities to the authenticated plane. The supplied seams should therefore narrow their descriptor-validation requirement to the later historical-control gate.

1. **Add one experiment-only reconstruction path.**

   Keep it in the Rust replay example, preferably a private module beside `compare_native_replay.rs`. Reuse its strict JSON, bounded input, no-follow file, snapshot and durable-publication helpers. Do not change serving readers or the historical Native100k CLI.

   Proposed private interfaces:

   ```rust
   fn authenticate_construction(
       generation: &CompletedInput,
       plane: &CompletedInput,
       approved_generation_sha256: &str,
       expected_generation: u64,
       expected_base_epoch: u64,
   ) -> Result<ConstructionAuthority>;

   fn reconstruct_centroids(
       authority: &ConstructionAuthority,
       leaves: &CompletedInput,
       max_payload_bytes: u64,
       already_pinned_bytes: u64,
   ) -> Result<ReconstructedCentroids>;
   ```

   `ConstructionAuthority` must have private fields, no deserialization constructor, and originate only from the runtime authenticator. Store:

   - Generation and plane byte identities, generation/base epoch and retained snapshots.
   - Profile, rows, dimensions, `unit_rows=32`, `page_rows=256`, unit count.
   - Original `input_schema`, `input_root_sha256`, `centroids_sha256`.
   - `source_sha256`, `source_order_sha256`, `mean_sha256`, `records_sha256`, `sq8_sha256`.
   - Historical router, membership and leaves byte/SHA descriptors from Discovery.

   `ReconstructedCentroids` contains the authenticated `Vec<u8>` and leaf snapshot. Its construction identity comes from `ConstructionAuthority`; callers cannot substitute a newly calculated digest.

   Authenticate generation bytes against the **independently approved original generation pin**, then its plane against `plane_manifest_sha256`. Apply the existing generation/plane/source-binding predicates, including geometry, calibration, source-only provenance, valid digests and Discovery lengths. Caller configuration locates assets; it does not establish their authority. Reuse the predicates in [`coverage_router` and `neighborhood_router`](/data/target/borsuk-spatial-native-gates-a0001-repaired.pending/frozen-source/crates/borsuk/examples/compare_native_replay.rs:5023).

2. **Reconstruct by streaming fixed records, without opening root or membership.**

   Before opening the leaf body, use checked arithmetic to admit:

   ```text
   U = ceil(rows / 32)
   record_bytes = 4 + 2*D
   leaves_bytes = U*record_bytes
   centroid_bytes = 32 + U*2*D
   seen_bytes = ceil(U / 8)
   ```

   Require the supplied leaf pin to match Discovery exactly. Stream precisely `U` records through one bounded buffer while hashing **every input byte**. Each record must have an in-range LE `u32` physical unit ID, appear exactly once, and contain only finite FP16 coefficients. Reject exponent-all-ones half words; copy their original two bytes unchanged.

   Scatter coefficients to `32 + unit_id*2*D`. Preserve signed zero and subnormals. Do not recompute means or round-trip through floating point.

   Write the exact header: `BORSUCP1`, LE rows/D/32/256, zero reserved word. Require exact EOF, complete roster, authenticated full leaf SHA, and finally **whole reconstructed blob SHA equal to Discovery’s construction `centroids_sha256`**. Recheck snapshots before returning or sealing an output.

   For the specified input, this means **31,250 records, 64,125,000 leaf bytes and 64,000,032 reconstructed bytes**. Header values follow the existing native construction contract; a digest mismatch refuses rather than searching for another header. The existing [`preflight` and builder](/data/target/borsuk-spatial-native-gates-a0001-repaired.pending/frozen-source/crates/borsuk/src/semantic_unit_router.rs:581) then enforce that contract again.

   Seal a reconstruction receipt binding source/config/binary identities, generation/plane/leaves pins, geometry, centroid SHA, allocation phases and `requests_opened=false`, `truth_opened=false`. This establishes exact centroid bytes only.

3. **Use the frozen old validator for the historical structural handoff.**

   Exact `2e2a789a` retains BORSUSR2 decoding; repaired `5cc7330b` deliberately accepts BORSUSR3. Reuse the old validation routines in a bounded, source-bound **experiment adapter**. Any new exporter needs qualification; the existing reports do not supply a complete exported prototype plane.

   That adapter authenticates the original root/membership/leaves against the same generation authority, validates every directory and leaf, complete membership/roster, row accounting and bitwise prototypes, and checks original FP16 against the reconstructed blob. It need not run the canonical neighborhood investigation again.

   Export `HistoricalControlEvidence` with:

   - Approved generation/plane pins and construction identity.
   - Original router/membership/leaves lengths and SHAs.
   - Geometry and fixed trainer, assignment, ordering and prototype contracts.
   - Ordered leaf tuples: `leaf_id`, `group_ordinal`, `chunk_ordinal`, `offset`, `bytes`, `unit_count`, `source_rows`, SHA and prototype **`u32` bits**.
   - Frozen validator source/binary/config identities and successful terminal receipt.

   The receiving runtime authenticates this handoff against its independently approved execution/output pins. A self-hashed JSON projection or caller assertion that validation passed is insufficient.

   Regenerate BORSUSR3 radial control from the reconstructed blob. Require exact membership and payload equality with the historical pinned bodies, plus ordered descriptor and prototype-bit parity through the handoff. Check unchanged algorithm semantics explicitly; acknowledge the new format, partition marker, allocation metadata and root identity separately.

   Thus, **old decoding is unnecessary for reconstruction but must have occurred somewhere trustworthy for historical structural parity**, unless an already authenticated complete handoff exists. It belongs outside the new production reader.

4. **Admit cumulative memory by actual lifetimes.**

   Let `M` cover caller pins, metadata, decoded copies and evidence buffers; `B` be centroid bytes, `S` uniqueness state, and `I` streaming scratch.

   | Phase | Simultaneous charge |
   |---|---|
   | Reconstruction | `M + B + S + I` |
   | Historical validation/export | `M + B + old-validator live allocations + export buffers` |
   | Radial or spatial build | External retained bytes **plus that builder’s full phase model** |
   | Paired replay | Both resident routers, both complete nomination sets, requests, producer state, seals, truth and SQ8 scratch |

   Drop reconstruction scratch before building. Build, validate and durably stage radial; drop its payload buffers before building spatial. Do not hold two complete builders simultaneously. Charge retained historical payload if using the full-buffer old validator.

   The current builder model already includes `B`: avoid charging it twice, but add everything it excludes. For 1M/D1024, its source-derived training model is approximately **506.7 MB before external retained allocations**, so admitting only the 64 MB reconstruction would substantially undercount. Reuse [`allocation_phases`](/data/target/borsuk-spatial-native-gates-a0001-repaired.pending/frozen-source/crates/borsuk/src/semantic_unit_router.rs:639).

   Keep scratch disk, file cache, runtime overhead and measured charged memory separate from logical payload accounting. Preserve host safety limits; preregister the new envelope from actual stage measurements.

5. **Make the smallest paired-harness change after parity passes.**

   Add a distinct partition-comparison config with common historical generation/plane/producer/request/SQ8/truth authority, reconstruction and historical-handoff pins, and separate complete `radial`/`spatial` publication pins.

   Keep the producer’s original generation binding intact. Generated routers are experimental sidecars; do not rewrite producer provenance to make existing one-router validation accept them.

   Open both authenticated BORSUSR3 routers and reuse unchanged request normalization and `coverage_arm(SquaredEuclidean)` for both. Preserve top48 and SOURCE 128 GET/64 MiB limits. Durably seal both complete selections, partition identities, publication pins and implementation identities using `publish_selection_seal_with_sync` **before the first truth open**.

   Retain exact historical trace checks, returned **313/320**, radial SOURCE closure **314/320**, per-truth-slot gains/losses and fetched-only bridge separation. Candidate screening remains at least **315/320 with gains > losses**. Artifact/protocol failures are INVALID; preserve historical FAIL/KILL dispositions.

6. **Bounded falsifier before real-input reconstruction.**

   On an authorized native lane, run one Rust fixture process with one CPU, 256 MiB, zero swap and a 120-second execution deadline after compilation.

   Use **4,097 rows, D1024, 129 units**, including a one-row final unit. Create real original BORSUCP1 and historical radial publication bytes, retain the published roster without `centroids.bin`, then reconstruct and compare byte-for-byte. Include signed-zero, finite subnormal and large finite half coefficients. Test reconstruction with historical root/membership absent; restoring them is required for structural parity.

   Refusal oracles must cover:

   - Wrong approved generation, plane binding, construction digest or geometry.
   - Duplicate/missing/out-of-range IDs, NaN/Inf, truncation, growth and wrong full-body SHA.
   - Wrong header/reserved bytes and checked-arithmetic/cap exhaustion.
   - File replacement, aliases, mutation and occupied output.
   - Historical directory, membership, leaf-hash or prototype corruption.
   - Radial regeneration mismatch, forged/unapproved handoff and failed file/directory sync.

   A particularly useful distinction test: reauthenticate a permutation of the flat records. Reconstruction must recover the same centroid blob; historical control validation must reject it when it disagrees with the immutable historical leaf publication. This falsifies any accidental conflation of byte reconstruction with publication validity.

   Extend the existing small coverage fixture to two roots and assert that either arm’s admission or seal failure prevents truth access. Then require affected-target compilation, locked workspace Clippy correctness/suspicious and `check_rust_test_build.sh` on the delivered revision. Actual retained-input runtime admission and a separate disposable canary precede any frozen consumed32 comparison.

The principal risks are an untrusted generation pin, an inadequately authenticated historical handoff, trainer/arithmetic drift that breaks exact radial parity, and underestimated overlapping allocations. SHA equality resolves centroid identity; it does not validate an old directory or establish scientific parity.

No files were edited, data bodies parsed, tests run, network accessed or children launched. The live qualification on `i-0481ab18c279b2bb2` / `67ee5144` was untouched; reconstruction and parity remain unverified, with no quality or cold-performance claim.
