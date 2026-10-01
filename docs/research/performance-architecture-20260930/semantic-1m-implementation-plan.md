# Read-only 1M implementation contract

Consultation `43828fb5c8d741a0`, Jev-selected GPT-6 Astra/XHigh, completed exit0. This is an implementation proposal and allocation arithmetic, not measured RSS, fresh corpus quality or launch authority. Root verified the fanout/quota bound, borrowed assignment threads and explicit pre-publication drops against the frozen source. Serving read and memory limits remain subject to exact-source qualification.

Implement one explicit **`Fresh1m` admission profile with a compact binary router root**. Keep training, first-eight/1.15/max-sixteen nomination, source completion, two-bit scoring and SQ8 ranking unchanged.

Inspection used exact commit `09c62f23781720939c66b2280f8b2f035ba01a36` and the committed admission/Fable evidence. No files, campaign artifacts, cloud resources or jobs were changed; no builds or consultations ran.

1. **Make scale admission explicit and fail closed.**

   In [semantic_unit_router.rs](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/semantic_unit_router.rs:155), add:

   ```rust
   pub enum SemanticProfile {
       Native100k,
       Fresh1m,
   }
   ```

   Require the profile in `SourceIdentity`, the authenticated generation discovery descriptor and the router header. There is no inference from row count and no default when decoding persisted artifacts.

   | Contract | `Native100k` | `Fresh1m` |
   |---|---:|---:|
   | Rows | Existing 1–100,000 | Exactly 1,000,000 |
   | Dimensions | Existing 1–768 | Exactly 768 |
   | Original centroid input | Existing 8 MiB cap | Exactly 48,000,032 bytes |
   | Construction payload cap | Existing 128 MiB | Explicit 512 MiB |
   | Encoded router root cap | 1 MiB | 4 MiB, plus exact geometry check |

   Change `preflight`, `admit` and `seed_walk` to receive the profile. `build`, `open` and `validate_publication` obtain it from `SourceIdentity`. Every path verifies identical profile/geometry; `admit` also rejects inconsistent caller-constructed `Geometry`.

   Add `TwoBitGenerationBuilder::build_with_semantic_profile(order, profile, output, cap)`. Existing `build_with_discovery(...Semantic...)` remains restricted to `Native100k`. Imports use their explicitly authenticated profile.

   Reject 1M under the native profile, unsupported dimensions, intermediate/larger research scales, unknown profiles and insufficient budgets. Do not increase nomination or physical-read caps.

2. **Replace the JSON router with one authenticated binary root.**

   Use `router/root.bin`, magic **`BORSUSR2`**, and generation schema **`borsuk-two-bit-generation-v8`**. Accept only this router format in new serving code; preserve historical artifacts with their original source archives.

   Concrete layout:

   - **512-byte header:** magic; header size; profile; rows; dimensions; unit/leaf/requested-center/training-center counts; source-schema length; original-centroid, membership and leaf-payload lengths; modeled construction peak and limit; four raw SHA-256 digests for input root, original centroids, membership and complete leaf payload; source-schema UTF-8 bytes in a fixed 256-byte area. All unused bytes must be zero.
   - **64-byte directory entry per leaf:** group and chunk ordinals (`u32`), offset (`u64`), byte length/unit count/source-row count (`u32`), four zero reserved bytes, and raw SHA-256. Leaf ID is its directory ordinal.
   - **Prototype plane:** exactly `leaf_count × dimensions` little-endian `f32::to_bits()` words, in leaf order.

   Header scalar offsets are: magic at 0; header size/profile at 8/12; rows at 16; six `u32` fields at 24–47; five `u64` lengths/accounting fields at 48–87; reserved bytes through 127; four digests at 128–255; schema bytes at 256–511.

   Algorithm details become fixed semantics of `BORSUSR2`: existing trainer, 12 iterations, squared-Euclidean training, 64-unit chunks, existing ordering, and f64 accumulation followed by f32 prototype conversion. No prototype quantization or normalization change.

   At 1M:

   ```text
   U = 31,250 units
   C = ceil(U / 64) = 489 requested centers
   L ≤ 2C − 1 = 977 leaves
   root_bytes = 512 + L × (64 + 4D)
              ≤ 3,064,384 bytes
   ```

   Authenticate the entire root against the enclosing generation SHA before parsing it. Check profile, counts, checked lengths and exact EOF before allocating directory/prototype arrays. Preserve existing complete-partition, membership, original-FP16, bit-exact prototype and selected-leaf checks.

   Reuse `UnitCentroidPages`’ explicit little-endian parsing, SHA-256 identities and existing publication sequence: validate complete artifacts, write new files, sync, publish the root last. No migration reader is needed.

3. **Replace the exaggerated recursion charge with a lifetime-based construction bound.**

   The trainer is actually [logical_cell_catalog.rs](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/logical_cell_catalog.rs:33); `unit_centroid_router.rs` does not exist at this revision.

   Its relevant lifetimes are:

   - The original FP16 blob, decoded f32 scorer, copied sample and trainer’s copied geometry coexist during training.
   - Assignment threads borrow those arrays and join before recursion. They do not clone the sample.
   - Lloyd sums/counts expire before recursion. Parent centroids, assignments and nearest-distance arrays can remain live during child calls.
   - Parent indices are consumed into groups; unvisited sibling groups remain live.
   - Every child receives at least one quota. With `k=min(32, leaves)`, a child receives at most `leaves−31` while `leaves>32`; at `leaves≤32`, children are terminal.
   - Therefore 489 centers permit **at most 16 nonterminal frames**, not 489 and not an assumed logarithmic depth.
   - Trainer geometry is gone when training returns. `build` drops scorer/sample/centers/groups before publication validation decodes centroids again. These phases must be charged with `max`, not added together.

   Make group capacity accounting explicit with count-first allocation in `train_centroid_partition` and `assign_groups`; reserve one slot for an initially empty trainer group so existing empty-group repair cannot grow it. Preserve insertion, repair, tie and arithmetic order. This small allocation-only change removes dependence on assumed `Vec` growth.

   For the supported 64-bit target, retain generous existing per-vector allowances and use checked arithmetic throughout:

   ```text
   B = 32 + 2UD                       original blob
   F = 4UD + 4U                       decoded scorer
   V = 4UD + U(24 + 64)               one nested-vector plane
   I = C(4D + 128)                    centers/identity allowance
   H = ceil((C−1)/31)                 nonterminal frame bound
   R = H(32D×16 + 64U + 32×128)       conservative recursive scratch

   training = 2×64KiB + B + F + 2V + R + 2I + 8MiB
            = 391,631,304 bytes
   ```

   The recursive allowance deliberately charges every frame a full sample’s scratch, including arrays that are actually disjoint or already consumed.

   | Phase bound | Bytes |
   |---|---:|
   | Training, including caller’s centroid blob | 391,631,304 |
   | Assignment/leaf construction | 317,158,184 |
   | Publication validation with original and staged payload copies | 268,342,232 |
   | Generation build, including a retained 1M-entry row order | **400,316,456** |

   For reproducibility, the latter two phase formulas use `P=U(2D+4)`, `W=512+L(64+4D)`:

   ```text
   emit = 2×64KiB+B+F+V+I+64U+P+4U+3L(4D+256)+4MiB+8MiB
   validate = 2×64KiB+B+F+2P+16U+3W+3L(4D+256)+8MiB
   outer build = 128D+32ceil(N/256)+256(D+12)+262144+8N
   ```

   Charge `max(training, emit, validate) + outer build`; separately check the source-builder phase’s existing admission.

   **The current 128 MiB cap genuinely cannot hold this implementation:** the input blob plus decoded scorer already consume 144,125,032 bytes before either training copy. A 512 MiB research payload cap accommodates the unchanged algorithm; it is an explicit new profile, not weakened accounting. Enforce a separate 1 GiB process/cgroup ceiling during qualification and report its observed peak independently.

4. **Carry the format/profile through the existing seams, with a runnable native scale caller.**

   | Owned files | Required change |
   |---|---|
   | `semantic_unit_router.rs` | Profile checks, binary encode/decode, phase admission, unchanged nomination/publication validation |
   | `logical_cell_catalog.rs` | Count-first group capacity allocation; recursion/quota and deterministic-output tests |
   | `two_bit_build.rs` | Explicit profile builder entry point, profile-aware admission/import validation, binary root publication |
   | `two_bit_generation.rs` | v8 descriptor/profile validation, binary root roster/open, existing query and pin accounting |
   | `object_native_generation.rs` | Stage `router/root.bin` using authenticated exact length and profile cap; retain current metadata waves |
   | `bin/build_two_bit_generation.rs` | Hashed config explicitly selects semantic profile; source-only construction |
   | `bin/check_semantic_router_scorer.rs` | New strict scale config using the current native generation and production diagnostic/planner/ranker APIs; remove mandatory historical graph/control inputs from this execution path |
   | `bin/build_semantic_unit_router.rs`, `bin/repackage_semantic_generation.rs` | Mechanical binary-root/API updates; retain their explicit 100k admission |
   | `two_bit_compaction.rs` | Reject unsupported `Fresh1m` maintenance before work starts; never downgrade its profile implicitly |

   Use a file-backed `ObjectStore` for the first scorer execution, exercising paged metadata, selected leaves, source coverage and SQ8 reads without cloud deployment. Report those as local/logical I/O, not S3 measurements.

   Add one small `scripts/check_semantic_scale_admission.py` controller: authenticate the frozen config and executable, invoke the native builder/scorer, and validate complete terminal receipts. Let Rust own router parsing and scoring.

   Existing Python callers are independently frozen:

   - `check_semantic_router_coverage.py`: 100k rows, 128 MiB accounting, JSON prototypes, capped order input.
   - `package_semantic_native_generation.py`: exact FIRST100k geometry and historical import authority.
   - `prepare_native_semantic_publication.py`: fixed 100k canonical/source envelopes.
   - `run_native_semantic_router_cold.py`: fixed 100k panel, truth bounds, references and runtime-code identities.

   Keep these rejecting the new scale arm. Their frozen authorities must not become 1M authorities through substituted constants.

5. **Freeze the serving and pinning envelope before corpus execution.**

   At 1M, original centroids are construction/publication input; serving retains root/membership and reads selected leaves.

   | Component | Bound |
   |---|---:|
   | Complete leaf payload, stored | 48,125,000 B |
   | Membership, encoded / decoded `usize` | 125,000 / 250,000 B |
   | Prototype coefficients | 1,502,208–3,001,344 B |
   | Selected leaves | ≤16 GETs; ≤1,576,960 B, within existing 2 MiB cap |
   | Nominated units / after seed completion | ≤1,024 / ≤1,031 |
   | Physical page closure | ≤1,024 pages |
   | Source reads | Existing ≤128 GETs, 64 MiB, parallelism 16 |
   | SQ8 reads | Existing ≤32 GETs, 16,773,120 B, parallelism 16 |

   Keep the conservative existing `32 × root_bytes` serving charge initially. With three enclosing JSON manifests capped at 64 KiB, metadata bytes `M≤4,514,088`.

   For scratch `400,000` and one active query, the existing [generation admission](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/two_bit_generation.rs:1011) gives:

   ```text
   metadata = 3M + 32W + 64(4D) + 131072
            = 111,930,232 B

   query = 3×16,773,120 + (400,000 + 1MiB)
         + 256×21,504 + 4D
         + 2×64MiB + (2×2MiB + 1MiB)
         = 196,736,640 B

   admission = metadata + active_queries×query + already_pinned_bytes
   ```

   Thus:

   - One generation/one query: **308,666,872 B**, approximately **294.37 MiB**.
   - One equally charged old generation plus the new generation: **617,333,744 B**, approximately **588.74 MiB**.
   - Four queries without old pins: approximately **857.24 MiB**.

   Preregister one active query initially: 512 MiB payload admission, or 768 MiB for the deliberate two-generation pin test, under the separate 1 GiB process ceiling. Charge retained diagnostics/order maps and mutation snapshots separately. Reject additional pins before allocation.

   These are conservative allocation bounds. They exclude a claim about allocator, stacks, transport or page-cache RSS.

6. **Use focused correctness gates, then one decisive fresh-data falsifier.**

   Before corpus execution, require:

   - **Format/identity tests:** exact f32 bits including signed zero; malformed/truncated/trailing roots; overflowed lengths; unknown profile/version; wrong hashes; nonfinite prototypes; duplicate/missing units; refreshed leaf hashes failing original-FP16 or prototype verification.
   - **Scale/cap tests:** native rejects 1M; fresh profile rejects wrong geometry; exact-cap acceptance and one-byte-under rejection; maximum 977-leaf synthetic root fits its computed size. These need no full-size training in ordinary unit tests.
   - **Trainer tests:** skewed quota distributions establish the 16-frame bound; empty-group repair respects capacities; byte-for-byte center, membership and nomination parity on deterministic small fixtures.
   - **Serving tests:** selected-leaf bounds, partial tails, seed completion, source/SQ8 limit failure without truncation, concurrent admission and old-generation pin rejection. Include a widely scattered 1,024-page closure.
   - **Publication tests:** complete validation precedes root publication; interrupted writes expose no completed root.

   On the exact implementation revision, record commands, source identity and exit statuses:

   ```bash
   cargo test --locked -p borsuk --lib --no-run
   # Run affected library, builder/scorer binary and integration tests.
   cargo clippy --locked --workspace --all-targets -- \
     -D clippy::correctness -D clippy::suspicious
   bash scripts/check_rust_test_build.sh
   cargo test --release --locked --workspace --all-targets
   ```

   Run the full execution gate once after focused checks pass, with bounded compiler/test concurrency. Build and hash the actual release builder/scorer executables separately. None of these checks was run during this planning turn.

   **Decisive falsifier:** one fresh ReLAION 1M build, then one preregistered 64-query development panel, followed by CoHere only if ReLAION survives. Freeze source, source order, original centroid, query and exact 1M-truth identities before execution; reject overlap with already consumed development panels.

   Hold all algorithm and read limits above fixed. Require **mean returned R10 ≥95%: at least 608 hits out of 640**. Report actual returned R100 separately. For each query, record:

   - Truth@10/@100 inside nominated units and their page closure.
   - Truth retained after source scoring and physical SQ8 admission.
   - Final returned R10/R100, separating nomination, source/planning and ranking loss.
   - Selected leaves, seed additions, logical requests/bytes, route CPU and observed memory.

   Bound this first attempt to one build, one panel, two CPU threads and the declared memory ceiling; preregister a 60-minute build and 15-minute evaluation deadline. A budget error, timeout, identity failure or missing terminal record cannot count as a quality pass. Do not retune the beam after seeing results.

The principal remaining risk is unchanged nomination losing coverage as the corpus grows; source-range fragmentation can also exceed the existing byte budget. Either outcome falsifies this arm without requiring another architecture review. The existing 98.125%/95.78125% R10 and 452.618/436.060 ms p90 remain FIRST100k development evidence—not fresh1M qualification, a vendor win or a 100M latency projection.
