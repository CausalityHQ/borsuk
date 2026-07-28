# Build and FP8 Foundation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the complete Rust test-binary build bounded and add bit-exact FP8 E4M3FN/E5M2 support through BORSUK’s schema, Arrow/Parquet storage, exact rows, and public bindings.

**Architecture:** A small `float8` module owns format conversion and exhaustive byte-level tests. `VectorElementType` delegates canonicalization and fixed-width encoding to it, while existing typed Arrow and WAL paths add `UInt8` physical arrays plus explicit metadata. Workspace test-profile settings and a timed build script keep the many integration binaries buildable without weakening release-mode optimization.

**Tech Stack:** Rust 1.91+, Arrow/Parquet, Cargo profiles, Python/Node binding declarations, Bash.

---

### Task 1: Reproduce and bound the full test-binary build

**Files:**
- Modify: `Cargo.toml`
- Create: `scripts/check_rust_test_build.sh`
- Create: `scripts/test_check_rust_test_build.py`
- Modify: `.github/workflows/ci.yml`

- [ ] **Step 1: Add a failing static contract test**

The Python test reads the root manifest and CI workflow and requires:

```python
self.assertIn("[profile.test]", cargo_toml)
self.assertIn("debug = 0", cargo_toml)
self.assertIn("incremental = false", cargo_toml)
self.assertIn("split-debuginfo = \"off\"", cargo_toml)
self.assertIn("check_rust_test_build.sh", workflow)
```

It also executes the build script with `BORSUK_TEST_BUILD_COMMAND=true` and
asserts that the script accepts the override and prints elapsed seconds.

- [ ] **Step 2: Run RED**

```bash
python3 -m unittest scripts.test_check_rust_test_build -v
```

Expected: failure because the test profile and script do not exist.

- [ ] **Step 3: Implement the bounded profile and build gate**

Add:

```toml
[profile.test]
debug = 0
incremental = false
split-debuginfo = "off"
```

The script runs:

```bash
env CARGO_BUILD_JOBS="${BORSUK_TEST_BUILD_JOBS:-2}" \
  cargo test --locked --workspace --all-targets --no-run
```

It records start/end UTC timestamps and elapsed seconds, supports a command
override only for its own unit test, and exits with the Cargo status. CI invokes
the script before executing tests.

- [ ] **Step 4: Run GREEN and the real reproduction**

```bash
python3 -m unittest scripts.test_check_rust_test_build -v
BORSUK_TEST_BUILD_JOBS=2 bash scripts/check_rust_test_build.sh
```

Expected: unit tests pass and the complete test-binary build exits zero without
an indefinitely sleeping compiler fan-out.

### Task 2: Bit-exact FP8 conversion

**Files:**
- Create: `crates/borsuk/src/float8.rs`
- Modify: `crates/borsuk/src/lib.rs`

- [ ] **Step 1: Add failing conversion tests**

Tests cover:

```rust
assert_eq!(F8E4M3FN::from_f32(1.0).to_bits(), 0x38);
assert_eq!(F8E4M3FN::from_f32(-1.0).to_bits(), 0xb8);
assert_eq!(F8E5M2::from_f32(1.0).to_bits(), 0x3c);
assert_eq!(F8E5M2::from_f32(-1.0).to_bits(), 0xbc);
```

Also test signed zero, smallest subnormal, largest finite, ties-to-even,
saturation, infinity behavior, NaN rejection at the public vector boundary,
and all 256 byte values against decode→encode canonicality.

- [ ] **Step 2: Run RED**

```bash
cargo test -p borsuk --lib float8 -- --nocapture
```

Expected: compilation fails because the module and types are absent.

- [ ] **Step 3: Implement conversion without a dependency**

Define:

```rust
pub(crate) trait Float8Format {
    fn encode(value: f32) -> u8;
    fn decode(bits: u8) -> f32;
}

pub(crate) struct E4M3Fn;
pub(crate) struct E5M2;
```

Use integer decomposition of `f32::to_bits`, explicit exponent/mantissa
rounding, and ties-to-even. Keep conversion independent of Arrow and index
code.

- [ ] **Step 4: Run GREEN**

Run the command from Step 2. Expected: all float8 tests pass.

### Task 3: Extend the scalar schema and fixed-width rows

**Files:**
- Modify: `crates/borsuk/src/record.rs`
- Modify: `crates/borsuk/tests/build_config.rs`

- [ ] **Step 1: Add failing schema tests**

Add parsing and canonicalization cases for:

```rust
"float8-e4m3fn" => VectorElementType::Float8E4M3Fn
"fp8" => VectorElementType::Float8E4M3Fn
"float8-e5m2" => VectorElementType::Float8E5M2
```

Assert one byte per dimension, stable persisted names, exact fixed-row byte
round trips, finite overflow policy, and query/record canonical equivalence.

- [ ] **Step 2: Run RED**

```bash
cargo test -p borsuk --test build_config float8 -- --nocapture
```

Expected: missing enum variants.

- [ ] **Step 3: Implement enum integration**

Add `Float8E4M3Fn` and `Float8E5M2` to `VectorElementType`. Update
`as_str`, `FromStr`, `canonicalize`, `fixed_width_bytes`,
`encode_fixed_width`, and `decode_fixed_width` using the `float8` module.

- [ ] **Step 4: Run GREEN**

Run the command from Step 2. Expected: all selected tests pass.

### Task 4: Persist FP8 in Arrow exact sidecars and Parquet WAL

**Files:**
- Modify: `crates/borsuk/src/arrow_vector_sidecar.rs`
- Modify: `crates/borsuk/src/format.rs`
- Modify: `crates/borsuk/src/index.rs`
- Modify: `crates/borsuk/tests/format.rs`

- [ ] **Step 1: Add failing physical-schema tests**

For each FP8 type, encode records and assert:

```rust
assert_eq!(vector_child_type, DataType::UInt8);
assert_eq!(
    schema.metadata()["borsuk.vector.element_type"],
    "float8-e4m3fn"
);
```

Round-trip through Arrow sidecar, WAL Parquet, flush, and reopen. Corrupt the
type metadata and require an `InvalidStorage` error.

- [ ] **Step 2: Run RED**

```bash
cargo test -p borsuk --lib arrow_vector_sidecar::tests::typed
cargo test -p borsuk --test format float8 -- --nocapture
```

Expected: non-exhaustive matches or absent FP8 physical schema.

- [ ] **Step 3: Implement typed UInt8 arrays**

Use `FixedSizeList<UInt8>` for FP8. Encoding writes format bytes; decoding
maps bytes back to canonical f32. WAL schema metadata remains authoritative.
Update the fixed exact-page Arrow conversion in `index.rs`.

- [ ] **Step 4: Run GREEN**

Run the commands from Step 2. Expected: all selected tests pass.

### Task 5: Validate index semantics for FP8

**Files:**
- Modify: `crates/borsuk/src/index.rs`
- Create: `crates/borsuk/tests/fp8_vectors.rs`

- [ ] **Step 1: Add failing lifecycle tests**

For E4M3FN and E5M2, create a dense index and prove:

- exact search matches a brute-force reference over canonical values;
- approximate search returns valid ordered results;
- add/reopen/upsert/delete/flush/compact/reopen preserves values;
- incompatible binary metrics are rejected;
- cosine normalization observes canonical FP8 values.

- [ ] **Step 2: Run RED**

```bash
cargo test -p borsuk --test fp8_vectors -- --nocapture
```

Expected: compile or storage failure before FP8 index support.

- [ ] **Step 3: Complete exhaustive type matches**

Update query canonicalization, exact-page construction, descriptor validation,
statistics, and any type compatibility match that does not yet admit numeric
FP8.

- [ ] **Step 4: Run GREEN**

Run the command from Step 2. Expected: all FP8 lifecycle tests pass.

### Task 6: Expose FP8 through CLI, Python, and Node

**Files:**
- Modify: `crates/borsuk-cli/src/main.rs`
- Modify: `crates/borsuk-cli/tests/cli.rs`
- Modify: `crates/borsuk-node/src/lib.rs`
- Modify: `packages/borsuk/src/index.ts`
- Modify: `packages/borsuk/test/api.test.ts`
- Modify: `crates/borsuk-python/src/lib.rs`
- Modify: `python/src/borsuk/__init__.py`
- Modify: `python/src/borsuk/__init__.pyi`
- Modify: `python/tests/test_api.py`

- [ ] **Step 1: Add failing binding tests**

Each binding creates an FP8 index using both explicit stable names, inserts
values that visibly round, reopens, retrieves the canonical values, and runs
exact search. Type declarations must reject an unqualified unknown FP8 name
other than the documented `fp8` alias.

- [ ] **Step 2: Run RED**

```bash
cargo test -p borsuk-cli fp8 -- --nocapture
npm test -- --runInBand
python3 -m pytest python/tests/test_api.py -k fp8
```

Expected: validation/type declaration failures.

- [ ] **Step 3: Update public spelling and validation**

Add `"float8-e4m3fn"` and `"float8-e5m2"` to Rust docs, TypeScript unions,
Python validation, and stubs. Keep typed input arrays as f32-compatible source
values; physical rounding remains schema-driven in Rust.

- [ ] **Step 4: Run GREEN**

Run the commands from Step 2. Expected: all selected binding tests pass.

### Task 7: Foundation verification

**Files:**
- Modify only when a verification failure identifies a defect.

- [ ] **Step 1: Format and lint**

```bash
cargo fmt --all -- --check
cargo clippy --locked --workspace --all-targets -- -D warnings
```

Expected: zero errors.

- [ ] **Step 2: Run focused Rust suites**

```bash
cargo test -p borsuk --lib
cargo test -p borsuk --test build_config
cargo test -p borsuk --test format
cargo test -p borsuk --test fp8_vectors
```

Expected: zero failures.

- [ ] **Step 3: Re-run the bounded full test build**

```bash
BORSUK_TEST_BUILD_JOBS=2 bash scripts/check_rust_test_build.sh
```

Expected: complete workspace/all-targets test binaries build successfully.

- [ ] **Step 4: Record the measured build behavior**

Add elapsed time, peak concurrent rustc count, and the effective test profile
to `docs/research/reproducibility.md`. Do not claim the full tests pass until
the later lifecycle and release plans execute them.
