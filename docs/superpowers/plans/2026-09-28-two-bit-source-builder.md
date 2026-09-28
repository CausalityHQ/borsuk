# Bounded Rust Two-Bit Source Builder Implementation Plan

> **For agentic workers:** Use superpowers:executing-plans in this session.

**Goal:** Make the verified source-only code-plane encoder callable from Rust
and usable for the frozen ReLAION validation preparation without Python fitting.

**Architecture:** Stream raw little-endian f32 rows once for source digest and
mean. Stream the SQ8 physical IDs for digest and a permutation, then seek raw
rows in that physical order and call the existing codec. Hash/write records
incrementally; publish a manifest last. A failed build never publishes a manifest.

**Tech Stack:** Rust stdlib, existing SHA256/serde_json/codec; no new dependency.

**Spec:** `docs/research/v294-rust-source-parity-prereg.md` and
`docs/research/v296-paired-validation-prereg.md`.

## Global constraints

- Preserve seed20260923 and the qualified Rust v1 encoding arithmetic.
- No query or truth input. Bind source/SQ8/mean/record digests and geometry.
- Reject duplicate/out-of-range physical IDs, nonfinite/zero source vectors,
  wrong hashes/lengths, arithmetic overflow and insufficient memory admission.
- Source files are caller-owned immutable snapshots during build; output must
  be a new directory. Never replace a prior artifact. Failed directories remain
  unpublished for caller cleanup; only a final manifest is a publication token.
- Process memory: O(N) IDs+bitset and O(D) codec/buffers, never O(ND) vectors or
  a resident output code plane. Explicit memory cap covers payload allocations;
  allocator/OS/runtime overhead must be measured separately.
- This builder produces nomination metadata, not a complete ANN index. No cloud
  job, validation outcome, format qualification or vendor win is implied.

## One implementation slice

- [x] Add a focused Rust integration test calling `TwoBitSource::build` on a
  two-row D5 source and reversed SQ8 IDs. Check exact zero-scale records/mean,
  hashes, and clean failures for duplicate IDs, wrong source SHA, tiny memory
  cap and overwrite. Observe missing-API red before implementing.
- [x] Add `crates/borsuk/src/two_bit_source.rs`, export it in `lib.rs`, and add
  `build_two_bit_source` thin CLI. Reuse `RotatedTwoBitCodec` and stream stdlib
  I/O; no parallel encoder or format compatibility layer.
- [x] Run only the focused integration test. Check Rust formatting and a narrow
  CLI build; no full suite while remote-resource access is pending.
- [x] Document the public constructor/build call and memory/security boundary;
  seal measured evidence, commit a coherent slice, verify fast-forward and push.

Continue full frozen paired validation on the intended existing remote target
when authenticated access is supplied. Do not retune the winning method.

## Execution ledger

Missing-API red observed; final single Rust integration test passes. Original
full100k CoHere CLI exit0,64.64s/2432KiB reported peak RSS, exact V294 mean and
complete plane bytes. Source-plane manifest/source/test time receipts are sealed
in docs/research/native-two-bit-source*. Generation/paired/HTTP scope stays open.
Ruling: independent dual review deferred after native cooldown rejection; no
override or paid replacement. No production-release claim is made by this slice.

## Authenticated reload follow-up

Reuse the codec and manifest for a caller-trusted Rust metadata opener. Require
manifest SHA and generation SQ8 SHA, exact bounded file reads, format/geometry
checks and metadata admission before large allocations. Expose borrowed physical
records and the existing per-query admitted lookup. No alternative index,
compatibility reader, raw-vector cache or new dependency.

- [x] Missing-opener API red observed in focused integration test.
- [x] Implement opener and share padded geometry with the codec.
- [x] Focused test: reload score, wrong root/SQ8, corruption, cap, incompatible format.
- [x] Seal evidence, document resident metadata limits, commit and fast-forward push.

Original session71870 completed exit0: both focused tests pass,0.03s test
runtime after165s build/link. Formatting and diff checks pass for touched Rust
files. Receipt `docs/research/native-two-bit-open-check.json` seals this slice.
No cloud job or full suite was started.
