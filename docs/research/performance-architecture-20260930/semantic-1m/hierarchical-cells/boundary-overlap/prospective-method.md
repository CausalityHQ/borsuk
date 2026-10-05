# Prospective bounded boundary overlap

Status: native source implemented at `afd58b557d01cafc307e650d9c29f381dbf24c41`, not yet compiled or qualified. Exact-source remote implementation-gates/a0001 is running from bundle `2b3f683a9783945c994b3f99059df4dccbd0235d`; its reservation and launch receipts bind the 403-file native inventory and seven serial gates. No paired overlap quality or performance has been measured. This is a new arm. Historical witness and balanced-partitioner failures remain unchanged.

## Causal change

Keep the primary partition, stored routing prototypes, query normalization, routing beams and SQ8 scoring fixed. Change only which source rows an authenticated fetched cell contains. Use SQ8-only extents in BOTH matched control and candidate so removing SQ2 block nomination is not a treatment confound. The original consumed64 FIRST100k D768 cosine k100 panels falsify the mechanism; they cannot establish fresh quality or vendor superiority.

Replay the original primary builder from authenticated original canonical inputs. Share its partition implementation, capture actual trained centers, capacity-adjusted cuts, source-ID tie rules and coordinate-median fallback cuts. Require replayed primary source memberships, ordering, SQ8 bytes and prototype bits to match the retained layout. Stop INPUT_UNAVAILABLE/INVALID if original inputs or exact replay are missing; never infer boundaries from lossy SQ8 or child means. Do not alter original artifacts.

For each source row choose its nearest nondegenerate split boundary on its primary path using a fully specified normalized margin. Cross that boundary once, descend its sibling subtree using the original cut predicates, and propose one alternate leaf. Deterministic priority is normalized margin, then source ID, then destination ID. Coordinate cuts use their actual axis/threshold. Identical/zero-normal cuts are explicitly ineligible, not silently divided by zero. Capacity-shifted margins and threshold ties must be specified and tested before source freeze.

Admit proposals in that priority order, at most one replica/source row, 128 replicas/destination and floor(N/4) replicas globally. A full destination rejects that proposal and admission continues; quotas are fixed policy, not tuning. Record admitted and rejected counts. Numeric/allocation/body overflow is execution INVALID. Primary rows always remain. Persist owner and optional replica destination with the generation identity.

## Library and fetch contract

New native module semantic_cell_overlap owns build/open/search APIs and authenticated whole SQ8 cell extents. Export exact partition replay and select_cells seams from hierarchical_semantic_cells; preserve old algorithms and tests. Add unique-ID SQ8 ranking using unchanged distance arithmetic, deduplicate BEFORE top-k, and reject different bytes for the same logical ID in one pinned generation. No new controller, Python algorithm, quantizer or dependency is needed.

Select at most 32 extents before payload reads, one parallel payload wave, all bytes including replicas and framing charged within 16MiB. At 512 primary plus128 replica rows/cell, raw D768 payload upper bound is 32*640*780=15974400 bytes; framing allowance is 802816 bytes. Router startup, head, retry and mutation/delta I/O are separate measured costs, not hidden inside a cold32GET claim. Route primary8/boundary24 remains fixed. Full unique SQ8 scoring is the same in matched no-overlap control and overlap candidate.

Queries must use a pinned immutable revision; conflicting copies and stale update/delete replicas may never leak. Implement and independently test bounded revision-pinned delta suppression for deletes/replacements across all copies, including concurrent old/new readers and capacity refusal. Reuse existing mutation foundations where they fit. In-process incremental affected-cell publication, crash recovery, compaction/GC and generation swaps remain mandatory product promotion work; no prototype claims they already pass. Reject unsupported operations explicitly.

## Exact Rust scope

crates/borsuk/src/hierarchical_semantic_cells.rs: shared exact partition replay and cell-selection seams, no routing change.
crates/borsuk/src/semantic_cell_overlap.rs: new bounded library build/open/search and replica admission/receipts, inline independent tests.
crates/borsuk/src/returned_sq8.rs: additive unique-ID ranking, existing arithmetic unchanged.
crates/borsuk/src/bin/hierarchical_semantic_cells.rs: native build/paired diagnostic entry point, strict config and terminal/fsync/GT-order guards.
crates/borsuk/src/lib.rs: one module registration.

## Gates and resources

First compile/run independent small native fixtures: capacity-shifted cuts, centroid/coordinate/tied/degenerate boundaries, quotas, primary replay mismatch, replica byte identity, corrupt spans and mappings, nonunit scoring, dedupe before truncation, delete/replace suppression and revision pinning. Report mandatory test names and commands. Then exact-source workspace correctness/suspicious Clippy and real shim-unset test compilation on a bounded REMOTE host. No local Cargo, corpus, cloud or paid run by implementation child. Long compilation is acceptable; build safety caps persist.

Before one paired measured arm: authenticate exact real inputs locally, separate bounded infrastructure canary, freeze policy/source/resources. Build panels serially with <=4 workers, 8GiB/noSwap,8GiB scratch,20min per panel; queries one worker,512MiB/noSwap,5min per panel. These prospective caps are not universal product limits. Root may reduce concurrency, never silently lift a frozen cap. No parameter ladder.

PASS requires BOTH datasets selected coverage mean>=.98/p05>=95 AND returned recall mean>=.98/p05>=95 plus every declared resource/fetch/identity/cleanup gate. Scientific FAIL rejects this policy without posthoc tuning; execution errors are INVALID. Survivor needs fresh held-out quality and then matched cold HTTP/scale/lifecycle measurements. No S3 Vectors/Turbopuffer or100M win is presently established.

100M arithmetic: primary SQ8 78GB, extra copies<=19.5GB. Existing resident directories remain substantial and unmeasured; count actual capacities, startup copies, unique old/new pinned generations, delta, query concurrency and maintenance. Overlap cannot substitute for a scale RAM/cost measurement. SIMD follows a viable representation and actual kernel profile; eliminating witness CPU only saves its measured2.38/2.45ms means without fixing lost coverage.

## Deterministic extension of batch cuts

This rule resolves the fact that several predicates could reproduce the original training batch while assigning a new outside row differently. Do not choose an extension after observing queries.

For an unchanged unconstrained center assignment, use the exact original rounded f32 squared-distance difference and numerical delta<=0 predicate. For a capacity-adjusted cut, normalize both signed zero margins to positive zero exactly as capacity_partition does, record the actual maximum left `(rounded f32 delta under total_cmp, source ID)` key, then use lexicographic key<=cut to descend any source row, including one outside the original batch. For a coordinate-median fallback use the actual maximum left `(f32 coordinate under total_cmp, source ID)` key in the same way, preserving its original signed-zero comparison. Store cut kind, original centers or axis, exact cut score bits and tie ID. Replaying every original row must recover exactly its original side; a midpoint or ordinary centroid bisector is not interchangeable. Crossing forces only the chosen branch; subsequent descent evaluates these predicates on the unchanged canonical vector and source ID, including exact predicates of degenerate nodes that are ineligible crossing candidates.

Nearest-boundary priority is a declared score-margin proxy, not exact geometric distance under f32 rounding. Center cuts use abs(f64(original rounded delta)-f64(cut score))/(2*sqrt(f64(original nonzero squared-center-separation))). Unconstrained cut score is zero. Coordinate cuts use abs(f64(row coordinate)-f64(cut coordinate)). Finite/nonzero separation and nonzero source coordinate range are required for eligibility. Zero-range artificial ID cuts are ineligible. When one row has equal nearest-boundary margins, choose smaller path depth then boundary identity. Global proposal priority remains margin, source ID, then destination ID; alternate-leaf descent uses the recorded predicates and source ID. All definitions and constants are source-bound before the first new build.

Within an immutable base, duplicate logical IDs must have identical complete SQ8 bodies even if later hidden. A pinned delta may intentionally replace a base body: validate each base duplicate first, then suppress every base copy for the delta's visible tombstones/upserts, add each visible upsert once from the bounded resident delta, then rank unique IDs with the unchanged SQ8 scorer. Old pinned readers retain the old base/delta pair; newer revisions must not be mixed during a query. Charge delta scans and buffers explicitly, and report underfill instead of fetching more cells. Unsupported inserts or maintenance must remain explicit refusals until independently implemented and qualified.

## API and matched-control precision

The retained primary layouts are the capacity-constrained resident-v4 roots pinned in source-witness-router/a0001/native/config.json and retained-input-authority.json, NOT the earlier median-fallback diagnostic's arm named original. Original inputs means those exact capacity-layout builder inputs. Preserve canonical read order, source-ID order and metric reduction order during replay.

Expose logical source count, encoded physical record count/ordinals, and framed file offsets separately. Authenticate and charge complete extents, then pass only validated record slices to the additive unique-ID scorer; do not reinterpret existing ReturnedRange offsets or geometry.rows. Check full duplicate record bytes even when equal scores or visibility suppression would hide a conflict.

Selection is the exact existing primary/wider union from the route() seam with fixed8/24 beams and max32, not the newer nomination helper's exactly24-cell restriction. Both arms must use identical selected-cell IDs. Independently prove candidate unique rows equal control rows plus precisely admitted replicas. Both arms score ALL SQ8 records in selected cells, bypassing SQ2 nomination in both; existing WholeCell still nominates blocks and is not this matched full-cell scoring path. Do not change the legacy path.

Freeze this scientific pair with an empty delta. Mutation correctness tests are separate: replacement owner absent/replica present, old/new pinned readers, all-base-rows suppressed underfill, and capacity refusal without revision publication. The existing FP32 mutation scorer does not satisfy an unchanged-SQ8 promise; the new bounded delta must declare exact SQ8-body scoring or separately identify its different arithmetic. Never silently mix them.

All selected locations form one dependency wave. A local serial reader reports actual max_parallel_gets=1; it must not claim measured parallel S3 fetching. Actual async object-store concurrency, cold startup and lifecycle costs are later promotion gates.

Both arms must match selected cell IDs, primary flags and routing distance bits BEFORE payload reads. Complete both arms' 64 selections and full unique scored result rosters before opening truth; authenticate a durable paired seal binding their config/source/extent identities. Freeze p05 as sorted index floor(.05*(64-1))=3 and recall denominators at100 even on underfill. Candidate selected unique coverage must not fall below control, since primary memberships and selection are identical; returned recall may fall when additional candidates displace true neighbors. That difference must remain visible.

The representative ordinal for identical base duplicates is the smallest encoded physical record ordinal, separately mapped to its authenticated file extent and logical ID. Replica placement records must identify exactly which additional bodies the candidate adds. Actual framing and all resource/accounting formulas are mandatory source-contract outputs before final experiment freeze; the raw record bound alone is not sufficient.
