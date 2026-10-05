# Prospective bounded boundary overlap

Status: native implementation authorized, source not yet implemented or qualified. This is a new arm. Historical witness and balanced-partitioner failures remain unchanged.

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
