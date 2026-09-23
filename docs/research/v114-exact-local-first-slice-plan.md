# V114 Exact Local SQ8 First Slice Implementation Plan

> **For agentic workers:** implement this plan with the repository's
focused-check, source-frozen, Spot-first workflow. Keep the V114 design in
`docs/research/v114-exact-local-nominee-gate.md` authoritative.

Goal: add one reusable production-facing exact SQ8 row scorer with explicit
RAM or local-file placement and prove its 100k primary sets match an
independent reference. This slice does not add a live S3 reader or claim
returned recall. It prepares the paired 1M gate in the design document.

## File boundaries

- `crates/borsuk/src/exact_sq8_nominee.rs`: exact row geometry, bounds,
  candidate score and `(score, ID)` ranking over RAM or positioned local
  reads. No S3 client or research dataset constants.
- `crates/borsuk/src/exact_sq8_mirror.rs`: generation-bound full-object
  and 4-KiB block-digest verification, explicit RAM/file placement and
  fail-closed read wrapper around the pure scorer.
- `crates/borsuk/src/lib.rs`: export the module.
- `scripts/v114_exact_local_100k.py`: frozen 100k two-phase gate. It writes
  a source-only authenticated SQ8 plane, then reads development queries
  and compares the Rust scorer's primary sets against an independent
  Python score implementation over the same 512-row PQ64 roster.
- `scripts/validate_v114_exact_local_100k.py`: recompute source/query
  identities, plane hashes, all 1,000 primary sets and the reduction.
- `scripts/launch_v114_exact_local_100k_spot.py` and remote runner: one
  registered Causality Spot attempt, terminal sync and termination.
- `docs/research/v114-exact-local-100k-closeout.md`: terminal-bound result,
  code revision, resource observations, decision and next gate.

## Task 1: local exact scorer and placement

1. Write focused Rust tests first for a 3-row, 4-coordinate SQ8 object in
   the existing `id<i8>, norm<f4>, code[D]` little-endian layout. Check
   record offsets, deterministic score/ID tie order, a short final row,
   invalid ordinals, short files, wrong dimension, nonfinite query/norm
   and identical RAM/file outputs. The scorer must
   accept an explicit placement enum; it cannot switch on row count.
2. Run only those tests. Confirm the new tests fail for the absent module.
3. Implement the pure scorer. In the mirror wrapper, stream SHA-256 once
   before exposing a generation. Bind the caller's generation marker,
   maximum nominee count, SQ8 object hash and 4-KiB digest-sidecar hash
   to the opened handle.
   For RAM, hold verified bytes. For file placement, retain the verified
   descriptor and digest table, read every 4-KiB block intersecting a
   nominated row, and verify each block hash before scoring. Include the
   short final block. Reject a changed or corrupt block. Keep row scratch
   bounded by the 512-nominee roster and block scratch bounded by its
   intersecting blocks. The hash table is a deliberate measured memory
   cost, not an unaccounted cache.
4. Run the focused tests and direct `rustc --test` check if Cargo would
   raise devbox memory pressure. Run `cargo fmt --check` for touched Rust
   files. Commit the coherent scorer slice.

## Task 2: source-only 100k parity gate

1. Write tiny two-phase Python fixtures with source columns
   `feature_row_id, embedding` and query columns `query, vector`; reject
   unordered query IDs and changed source/query hashes. The query file
   must not be accessible to the build phase.
2. Reuse frozen source SHA
   `a199e151b89a496ed20e39fdd951591bbfb4817d682e9111ebe2e1cab7ae550d`
   and development query SHA
   `4834cf63a50971b7d605c00f91b5142f67b049e91ea2c62c220271b50bffa6ac`.
   Derive the SQ8 bytes from the already completed V113 source-only artifact
   at its terminal S3 prefix. Require the historical artifact seal SHA
   `803b00d9366bc8feb0e58e4900e27b1973c5d9578c33a0d6007fe4c28722c7b2`
   and check each artifact file against that seal. V113 trained its PQ64
   books and SQ8 codes from the frozen source with no query access. Persist
   a new manifest binding exact local row bytes and low/step, plus a source
   record binding that manifest to the source SHA and historical seal.
   Record both before query bytes arrive. Reuse those
   V113 PQ64 books/codes for the top-512 roster; no retraining or parameter
   selection is allowed in the 100k correctness gate.
3. For every query, score the same 512 nominated rows using both explicit
   Rust placements and an independent Python scorer. Record all three
   top-100 lists, scores at any disagreement, page votes, interval plans,
   local read time and physical caps. Require identical lists and plans
   on all 1,000 queries, zero hash failures and zero cap violations.
4. Independently validate every query and reduction. Keep 100k results
   labeled score/route correctness, not GT recall or live latency.

## Task 3: one frozen Spot execution and decision

1. Add a launcher and runner with one immutable source archive, terminal
   marker, input hashes, Spot instance ID, interruption detection and S3
   artifact hashes. Query bytes are downloaded only after the source-only
   SQ8/PQ/layout artifacts are sealed. Do not inspect partial evidence.
2. Run shell syntax and tiny launcher tests locally. Run the 100k gate on
   Causality Spot once from a pushed revision. Stop/terminate immediately
   after terminal. A failed attempt gets a new immutable attempt prefix.
3. Require terminal status complete and independently verify downloaded
   reduction/evidence hashes. If any placement or reference primary set
   differs, diagnose the score operation/tie or layout before 1M. If all
   match, write and push a closeout that names the next paired 1M
   development and live S3 gate; do not promote on RAM or SSD arithmetic
   alone.

## Review focus

- A verified file can be mutated after whole-object SHA-256 validation:
  authenticate each local 4-KiB block at query time against a
  manifest-bound digest table.
- Rust scalar `f32` accumulation can differ from NumPy matrix products:
  the 100k gate compares primary sets, records score differences, and
  uses one canonical operation order for both placements.
- The source and query Parquet schemas differ: source uses `embedding`,
  development queries use `vector` plus ordered `query` IDs.
- The 100k layout can hide short-final-page byte errors: include a short
  final page fixture and exact byte accounting.
- A small 100k file may fit page cache: measured placement timings are
  diagnostic only; the 1M/10M concurrency gates decide viability.

Verification policy: no local full suite while devbox swap/pressure is
elevated. Run focused tests after changes, then the repository full gate
once only when the code diff is stable and resources permit. Heavy data
construction and performance work belongs on Spot.

## Registered 100k execution resources

The one 100k correctness attempt uses `c7i.8xlarge` Linux/UNIX Spot in
`eu-central-1`, trying zones c, b, a in that order. It has one encrypted,
delete-on-termination 100-GiB gp3 root volume and a four-hour worker
deadline. The instance shuts down after the terminal upload; the launcher
also terminates and waits for EC2 termination. No fixed Spot maximum price
is set, so the charged rate can change. On 2026-09-23 the live AWS EC2
`DescribeSpotPriceHistory` response showed the most recent per-zone rates
as USD 0.6814/hour in 1c (16:00 UTC), 0.7658/hour in 1b (17:00 UTC), and
0.6970/hour in 1a (20:00 UTC). These are **price observations**, not a
campaign charge or performance measurement; the terminal instance ID and
actual bill determine final cost. AWS documents the meaning and scope of
the [Spot price history API](https://docs.aws.amazon.com/AWSEC2/latest/APIReference/API_DescribeSpotPriceHistory.html).

The attempt uses the completed V113 source-only artifact and its frozen
seal, a new source archive at the V114 commit, and the frozen 1,000-query
development object. The builder cannot see query bytes before the mirror
and provenance record are sealed. A Spot interruption makes the attempt
invalid: sync the terminal artifacts, discard the interrupted cell, and
reserve a new immutable attempt prefix before rerunning.
