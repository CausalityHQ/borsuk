# V32 bounded code-object format Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [x]`) syntax for tracking.

**Goal:** Build the authenticated bounded Arrow code-object codec needed by the subsequent streamed provider.

**Architecture:** One object contains ordered parent-local mixed-width code planes and original residual centroids. No query, global plane or storage client is admitted into this component; the resident router remains unchanged.

**Tech Stack:** Rust, existing Arrow58.3, half, SHA256; pinned Python/PyArrow for interchange verification.

**Spec:** `docs/superpowers/specs/2026-09-05-v32-bounded-code-object-format-design.md`

## Global Constraints

- Format marker `borsuk-v32-bounded-code-object-v1`; no compatibility aliases.
-1..8192 rows,1..32 parents,1..128 ranges,1..524288 encoded bytes.
- Original finite f16[96] centroids; local-row LSB-first fidelity bits;24/48 packed bytes.
- Exact nonnullable schema; one uncompressed IPC batch; no dictionaries.
- Test-first, narrow local selectors only; heavy assurance/science on causality Spot.
- No routing/default/S3/production_bench change in this slice.

## Task1: Parent-local addressing and invariant boundary

**Files:** create `crates/borsuk/src/v32_code_objects.rs`; add private module in `crates/borsuk/src/lib.rs`.
Initially use `#[cfg(test)] mod v32_code_objects;`; promote only with a real
production consumer and a production build/Clippy gate. No fake exports or
dead-code lint suppression. This is a verified codec checkpoint, not serving.

**Interfaces:** `V32CodeRange { logical_start:u64,row_count:u32 }`;
`V32ParentCodes { code_parent_ordinal:u32,centroid:[f16;96],ranges:Vec<V32CodeRange>,high_bits:Vec<u8>,base_codes:Vec<u8>,high_codes:Vec<u8> }`;
`V32CodeObject { parents:Vec<V32ParentCodes> }`.
`validate(&self)->Result<()>`, `code(&self,local_row:usize)->Result<(V30PqWidth,&[u8])>`,
`logical(&self,local_row:usize)->Result<u64>` are crate-private methods.
Both parent and object have `validate`; lookups on validated immutable parents
perform checked local indexing without revalidating the whole object per row.

- [x] Stage `v32_code_object_parent_local_addressing` test with one parent, ranges(10,2),(20,2), bitmap0b1010, base bytes24*[1] followed by24*[3], high bytes48*[2] followed by48*[4].

```rust
assert_eq!(parent.logical(0)?, 10);
assert_eq!(parent.logical(2)?, 20);
assert_eq!(parent.code(0)?, (V30PqWidth::Base24, &[1_u8;24][..]));
assert_eq!(parent.code(3)?, (V30PqWidth::High48, &[4_u8;48][..]));
assert!(parent.code(4).is_err());
```

- [x] Stage `v32_code_object_invariant_rejections` table cases: empty parents/ranges,33 parents,129 ranges,8193 rows, duplicate/out-of-order IDs, zero range, endpoint overflow, within/across-parent overlap, NaN centroid, nonzero padding, short/extra bitmap, short/extra base/high bytes. Include exact-limit valid fixtures.
- [x] Run `rtk proxy cargo test -p borsuk --lib v32_code_object_ -- --nocapture`; preserve intended missing-interface RED, correcting only fixture compiler mistakes first.
- [x] Implement checked integer arithmetic, ordered range validation and local rank lookup. Use row-bounded bitmap prefix popcounts; map logical IDs by subtracting range counts. Do not materialize per-row code or logical vectors.
- [x] Rerun the same selector, inspect every result; format/diff-check, review scope, commit coherent invariant slice. No public export or blanket lint suppression.

## Task2: Authenticated Arrow encoding and interchange

**Files:** same Rust module; create `scripts/test_v32_code_object_interchange.py` for PyArrow fixture checks.

**Interfaces:** `encode_v32_code_object(&V32CodeObject)->Result<Vec<u8>>`;
`decode_v32_code_object(bytes:&[u8],expected_sha256:&str,expected_bytes:usize)->Result<V32CodeObject>`.
Use the repository SHA256 dependency and exact digest syntax; authenticate before Arrow parse.

- [x] Stage `v32_code_object_arrow_roundtrip_and_authentication`: encode the Task1 literal fixture; decode with actual SHA/length and compare exact object; wrong digest, wrong length and a single changed byte must fail.
- [x] Stage `v32_code_object_arrow_schema_and_resource_rejections`: independently construct fixtures for nullable fields/children, wrong child name/type/width, extra fields/metadata, extra batches, dictionaries/compression, invalid footer/body extents and excessive rows/parents/ranges. Preserve valid hashes to reach semantic gates.
  Include small encoded files declaring more than128 range children or3072
  centroid elements and impossible binary buffer extents; reject before
  materialization, not after trusting declared allocation sizes.
- [x] Stage `v32_code_object_arrow_maximum_shape`:32 parents, each256 all-high rows, four64-row ranges each, unique disjoint logical ranges; require encode length<=524288 and decode equality. This checks8192 rows/128 ranges together.
- [x] Run the narrow selector for intended RED. Implement exact schema and pre-materialization IPC checks, then owned bounded decode, calling invariant validation at both encode/decode boundaries.
- [x] Add an env-gated Rust interchange test writing the literal Arrow fixture to an explicit scratch path and reading a separate Python fixture. Python uses exact spec schema and literal code bytes, verifies Rust values, emits its independent file; Rust checks exact values, not just roundtrip. The Python test invokes only that one Cargo node serially; no network or scientific payloads.
- [x] Run codec selector then this single interchange gate in a dependency-complete environment. Preserve terminal evidence and explicit scratch cleanup.
- [x] Run targeted library Clippy plus fmt/diff checks; request Astra read-only review of actual diff and test evidence. Repair narrow failing layer only. Commit/push fast-forward after verification; report codec-only completion without implying provider/scalability completion.

## Codec verification checkpoint

The final `v32_code_object_` gate executed8 tests successfully in0.06s;
the explicit interchange node is ignored in that selector and was separately
executed by the pinned Python/PyArrow gate (1 Python test invoking exactly1
Rust test, successful in0.279s). Both directions preserve literal mixed-width
codes, gapped logical ranges and exact f16 negative-zero/subnormal bits;
the test asserts temporary-directory removal. Maximum shape encoded to
405114 bytes, below524288, with8192 all-high rows,32 parents and128 ranges.

Astra identified two concrete decoder issues, each reproduced before repair:
four malformed raw-schema cases panicked in Arrow conversion; three
contradictory validity-bit cases were accepted despite declared zero nulls.
Both now fail closed before materialization. Authenticated buffer/offset,
node-length/null-count, schema, compression and dictionary mutations also
reject. Astra's final read-only delta review reported READY.

Strict `cargo clippy --locked -p borsuk --lib --tests -- -D warnings`,
scoped pinned Ruff, Python syntax, formatting and diff checks passed.
This checkpoint remains test-gated codec infrastructure: no production
consumer, S3 performance, write throughput or larger-scale recall claim.

## Exit and next plan

The format slice is complete only when both tasks and cross-language proof
pass. Next prepare the scorer/streamed-provider plan against this exact codec;
do not silently extend this plan with speculative directory or ingestion APIs.
