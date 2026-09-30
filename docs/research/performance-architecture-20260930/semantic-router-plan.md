# Bounded semantic unit router implementation

Consultation b388df1cb2ce4609, Jev-selected GPT-6 Astra/XHigh, completed225s, sourceeee07f45. Root accepts construction scope only. No corpus execution or architecture adoption: boundary/query protocol remains unfrozen, nomination quality unknown.

At `eee07f45`, add only `crates/borsuk/src/bin/build_semantic_unit_router.rs`. Preserve accepted graphdecode and failed bulk evidence. No dependencies, production changes, corpus fit, cloud, or scale claims.

1. **CLI:** `ROOT ROOT_SHA OUTPUT_DIR`. Bound/authenticate `manifest.json` (64KiB), then `centroids.bin` (8MiB), **before decode**. Preflight header with checked arithmetic: rows 1–100000, dimensions 1–768, unit_rows=32, page_rows=256, U=ceil(rows/32)≤3125, exact payload length and manifest agreement. Reuse `UnitCentroidPages::decode`; reject nonfinite values, accept zero means. Do not open the full generation.

2. **Fit:** `train_logical_cell_centroids(sample, SquaredEuclidean, ceil(U/64), 12)`. All-identical input uses one training center; other trainer/duplicate-center errors fail explicitly. No normalization/quantization. Assign every unit to its nearest center, ties by center ordinal. Sort groups by (squared distance, original unit ID), split into ≤64-unit leaves; stable group/chunk numbering. Root prototype: unweighted mean of each leaf’s unit means, f64 accumulation → finite f32. Preserve original FP16 bytes/IDs and final-unit row count `min(32,rows-32*id)`.

3. **Format:** research v1 `manifest.json`, `membership.bin` (LE u32 unit→leaf), `leaves.bin` (LE u32 unit ID + original D FP16 values). Manifest binds input root/blob SHAs, geometry, algorithm constants, f32 root prototypes, leaf offsets/counts/SHAs, whole-file lengths/SHAs. Print manifest SHA as external authority. Stage and publish without replacement after partition, identities and hashes validate.

4. **Admission:** checked peak allocation ≤128MiB before decode/training: blob capacity, decoded centers/norms, sample **and trainer geometry copy**, recursive scratch, memberships/prototypes/output capacities. Record estimate; it is not a process limit. Root supplies an enforced cgroup memory cap, zero swap and timeout before any real fit.

5. **One synthetic selfcheck:** byte/ID identity, deterministic repeat, complete disjoint partition, oversized split, ties, valid zero, NaN/Inf rejection, partial unit, SHA/header/length tampering and admission failure. Before integration run `cargo build --locked -p borsuk --bin build_semantic_unit_router`, then the same target with `cargo test`; `cargo clippy --locked --workspace --all-targets -- -D clippy::correctness -D clippy::suspicious`; `bash scripts/check_rust_test_build.sh`. Record revision, commands and exits.

**Distinguishing hypothesis:** semantic partition plus bounded boundary coverage preserves returned R10≥95% with sparse router hydration. This changes V149’s contiguous groups and avoids V150’s nearest-unit truncation only if nominees retain all selected-leaf units. Mean-only root nomination still repeats the closed coarse-summary risk; repacking alone is insufficient.

**Cheapest later rejection:** frozen 100k ReLAION-first paired control/candidate queries. Mean fetched truth@10<95% immediately falsifies the recall ceiling. Otherwise measure actual two-bit/SQ8 returned R10≥95%, report R100 and per-query losses; then CoHere. Charge root/leaves and physical-page closure against frozen caps. Trace-cover geometry establishes neither recall nor latency.

**Blocker before corpus execution:** root must freeze boundary coverage, leaf/GET/byte limits, physical closure and query roster. Builder implementation is unblocked; its artifact alone cannot test that hypothesis. No builds/tests ran here.

Root acceptance: preserve original FP16 bytes, no real corpus fit/cloud, one owned Rust binary. Compiler concurrency2 and shared /data/target used serially; no overlapping gate. Actual affected build/test, workspace Clippy and workspace test-build required. Root alone runs final full assurance and authorizes subsequent dataset gate.
