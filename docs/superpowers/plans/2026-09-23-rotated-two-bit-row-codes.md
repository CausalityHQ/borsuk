# Rotated Two-Bit Row Codes Implementation Plan

> Execute inline in the existing isolated `devbox/prod-ready-v9` worktree. The persistent user goal authorizes iterative research, while current developer instructions prohibit spawning subagents. Use the `superpowers:executing-plans`, `test-driven-development`, `systematic-debugging` and `verification-before-completion` workflows as applicable.

**Goal:** Run one authenticated source-only 100k decision cell for a new 200-byte rotated two-bit scalar row code under the frozen four-page group read schedule.

**Spec:** `docs/superpowers/specs/2026-09-23-rotated-two-bit-row-codes-design.md`.

**Fixed evidence:** Closed page-centered group attempt from source `fd3b14ba`, terminal SHA `06f62bc2e511482697395d36c6035f9205051bf55c55503103f12d2619e91694`, and 1,000-query evidence SHA `24c0f36d947206141b536dd0e6d7536e29e1b56bf3c60700394546fb1e1d20d9`. The attempt itself is immutable and is not modified or restarted.

**Read/quality gates:** 32 actual S3 GET and 16,777,216 code bytes; 32 planned final pages and 16,777,216 data bytes; mean GT100 >=97.5%, p05 GT100 >=90%, GT10 page containment >=96%; <3 GiB worker phase RSS including the range broker, zero swap. A 100k pass is only `quality-advance-memory-pending`.

## Task 1: deterministic source-only scalar code and group artifact

Create `scripts/native_rotated_two_bit_codes.py` and focused tests. First write tests for deterministic signed block-Hadamard, orthogonality, four levels and little-endian packing, least-squares scale, 200-byte records, physical membership order, per-group hashes and source seal rejection after one source/membership mutation; run a missing-module RED. Keep membership seed `20260921` separate from rotation seed `20260923`. Implement source-mean training, fixed sign generator and eight scale updates, row exact norm, group format, canonical artifact identities and strict readback. Run the narrow tests, Ruff and diff check; commit the slice.

## Task 2: actual group-wave scoring and independent replay

Create `scripts/native_rotated_two_bit_evaluation.py`, `scripts/native_rotated_two_bit_evidence.py`, `scripts/validate_native_rotated_two_bit_result.py` and focused tests. Bind the same retained-leaf group order, authenticate each returned range and executed S3 GET/byte counts, score every grouped row with the preregistered **reconstructed-distance primary** and mandatory norm-corrected diagnostic on the same sealed bits, and pair exact original-vector distances over identical candidates. Apply unchanged top-100/page nomination. Join all new exact pages and hits to the terminal-closed group evidence by query ordinal; any mismatch invalidates the cell. Write canonical per-query evidence and aggregate. Validator must independently unpack, recompute source-only training, score, nominate and compare every one of the 1,000 records, joined to prior control hits by query ordinal. Test a 768D equal-distance tie, one corrupt/short/reordered group range, altered page/score/hit/aggregate, and a waveform that exceeds the 16-MiB cap. Run focused tests, Ruff and diff check; commit the slice.

## Task 3: phase-separated Spot cell and controller

Create `scripts/native_rotated_two_bit_cell.py`, `scripts/native_rotated_two_bit_range_broker.py` and `scripts/launch_native_rotated_two_bit_spot.py` with focused tests. Adapt the proven source-only construct/evaluate/validate, networkless construction, network-isolated non-root evaluation through a root Unix-socket broker limited to sealed group ranges, no-retry actual S3 range reader, immutable reservation, terminal roster and immediate controller termination. Use batches of at most 4,096 source rows for construction/replay and 2,048 candidate rows for scoring so no whole float64 source/candidate matrix survives. Use a distinct prefix `research/native-rotated-two-bit/<source-commit>/runs/relaion-100k-dev1000-a0001/`. Test source/query capability boundary, source archive/requirements binding, exact roster, shell syntax and user-data size, validator Python paths, prefix collision and interruption terminal. Keep phase RSS monitoring including the broker, zero-swap verification and `/usr/bin/time -v` gate. Run focused tests, Ruff, diff check; obtain one read-only final implementation review and fix concrete findings. Commit and fast-forward push source.

## Task 4: one immutable measured decision

Freeze `git archive HEAD` with commit marker; compare every tracked file byte/mode against `git archive`, hash and read back, upload source with `If-None-Match:*`. Check unique attempt prefix and matching active instance inventory empty. Launch one `causality` `c7i.8xlarge` Spot attempt using the original controller session; monitor terminal marker and infrastructure health only. After terminal, verify controller exit and instance termination, read back and hash every artifact, inspect producer and independent validator agreement, recompute paired query metrics and resource caps. If interrupted, discard the cell and use a distinct attempt; do not treat partial measurements as evidence. Record the kill or limited pass, full identities, resource measurements and exact next 1M/redesign gate in the research ledger; commit and fast-forward push. Do not claim production readiness or competitor parity from page containment.

## Review checkpoints

- Review fixed byte arithmetic: the closed 1,000 group plans contain at most 77,113 rows, and a new 200-byte row record plus 640 group-header bytes projects a maximum 15,423,240-byte wave. Assert the same bound from new sealed manifest, not merely from the old evidence.
- Independent replay must reconstruct source mean/signs/scale and group physical bytes without calling the producer scorer or planner. Sharing artifact schemas is allowed.
- Exact and coded arms must differ only in row score; stable source-ordinal ties and final page cap are identical. The new exact arm must agree query by query with the closed group exact control.
- If scientific quality fails, kill representation and move to a materially different design. Do not sweep row width or norm choices on the frozen queries.
