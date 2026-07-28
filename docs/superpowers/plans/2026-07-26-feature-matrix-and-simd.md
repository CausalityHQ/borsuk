# Feature Matrix and SIMD Implementation Plan

**Goal:** Prove every supported BORSUK scalar type and retrieval kind through
one lifecycle contract, then optimize missing hot paths without changing scalar
semantics.

**Architecture:** A single `feature_matrix` integration binary owns
declarative dense-type, sparse-type, late-interaction, and hybrid-combination
case tables. Shared lifecycle helpers exercise WAL visibility, typed query
canonicalization, flush, reopen, upsert, delete, compaction, exact/approximate
search, and safe garbage collection. SIMD kernels retain independent scalar
references and randomized bulk/tail equivalence tests.

## Task 1: Declarative dense type coverage

- Add a failing coverage-policy test enumerating every stable dense scalar type.
- Exercise primary and named dense fields for float32, float16, bfloat16,
  E4M3FN, E5M2, int8, and packed binary.
- Validate canonical values, compatible metrics, exact/approximate ordering,
  mutations, compaction, reopen, and active-object-safe GC.

## Task 2: Retrieval-kind and hybrid coverage

- Add sparse float32/float16 lifecycle cases and rejection cases for unsupported
  sparse types.
- Add BM25-only, dense+BM25, sparse+BM25, dense+sparse, and
  dense+sparse+BM25 cases.
- Add late-interaction float32/float16 cases and rejection cases for other
  scalar types.
- Cover UTF-8, non-UTF8, and integer-compatible record IDs through existing
  opaque-byte APIs.

## Task 3: Public smoke-matrix parity

- Keep CLI, Node/TypeScript, and Python smoke cases aligned with every public
  dense type spelling and every retrieval kind.
- Run each native package's complete API suite.

## Task 4: SIMD audit and implementation

- Inventory dense, half/FP8 decode, int8, binary, sparse, late-interaction, and
  lexical hot paths against the stabilization design.
- Add failing scalar/SIMD equivalence tests before each missing optimized path.
- Implement portable `wide` kernels first; use architecture intrinsics only
  behind safe dispatch.
- Run release microbenchmarks and retain only kernels that improve at least one
  supported architecture without correctness drift.

## Task 5: Release evidence

- Run format, lifecycle, mutation, recovery, S3-compatible, package, Clippy,
  documentation, and full workspace test gates.
- Generate a release-readiness matrix that names every tested cell and its
  command.
- Freeze publication methodology and execute the paid confirmatory benchmark
  only after every release cell is green.
