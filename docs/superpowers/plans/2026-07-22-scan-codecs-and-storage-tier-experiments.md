# Scan codecs and storage-tier experiment implementation plan

> Implement test-first and preserve the active 100M SRHT-PQ AWS run as a
> baseline artifact.

**Goal:** Implement unambiguous PQ/SRHT-PQ/TurboQuant scan codecs, configurable
cache execution, and reproducible recall/latency/resource comparisons without
promoting an unqualified default.

## 1. Public names and persisted codec identity

- Add failing parser/default/serde tests for `pq-scan`, `srht-pq-scan`, and
  `fast-turboquant-scan`; assert that each maps to a distinct enum value.
- Add a persisted global scan-codec discriminator and bump the descriptor
  format. Reject descriptors without an exact supported codec/version.
- Rename the current resident implementation and diagnostics to SRHT-PQ while
  retaining its algorithm and bytes as the AWS baseline.
- Update Rust, CLI, Python, Node, examples, and documentation defaults to
  `srht-pq-scan`.

## 2. Classical PQ control

- Add identity/no-rotation support to the learned product quantizer.
- Add deterministic encode/decode/ADC tests and a test proving classical PQ
  produces different codes from SRHT-PQ on nontrivial input.
- Persist and load classical-PQ descriptors and execute exact reranking through
  the common global scan path.

## 3. Faithful optimized TurboQuant scan

- Add packed 2/3/4-bit coordinate storage with round-trip and odd-dimension
  boundary tests.
- Reuse the seeded in-place SRHT/FWHT; add an allocation regression test proving
  the descriptor is O(dimensions), not O(dimensions squared).
- Implement TurboQuant MSE query preparation and score estimation with the
  fixed sphere-coordinate Lloyd-Max table and stored vector norm. Reject the
  reserved QJL knob until TurboQuant_prod is implemented and validated as a
  separate codec.
- Persist all scoring parameters and add descriptor corruption/version tests.

## 4. Configurable cache execution

- Add `CacheExecutionPolicy::{Scan, Graph, Auto}` to search options, defaulting
  to `Scan`, with strict public parsing and bindings.
- Report requested/selected policy, codec, coverage, and fallback reason.
- Add a manifest-specific local coverage certificate. `Graph` and `Auto` both
  fall back to scan before query execution when incomplete; complete coverage
  makes explicit `Auto` select graph. Default promotion remains evidence-gated.
- Preserve global request/decode/byte admission and same-key single-flight.

## 5. Global-cell graph prototype

- Build immutable graph bundles per global IVF cell under explicit build
  configuration; do not enable them by default.
- Add bounded local warming/pinning and checksum-aware eviction invalidation.
- Benchmark against segment graph and whole-index graph controls on the
  qualification datasets.

## 6. Harness and staged experiments

- Extend benchmark rows with scan codec, cache execution policy, cache
  coverage, graph layout, full resource telemetry, and build footprint.
- Run deterministic local unit/smoke tests and render validation charts.
- Run AWS qualification on Fashion, GloVe, GIST, and Deep-Image.
- Run the full six-corpus matrix for qualifying points, each with three
  fresh-process repetitions and bounded concurrency.
- Run clustered/uniform/adversarial synthetic scale tests and a final 100M run
  only for qualified candidates.

## 7. Evidence and default decision

- Check raw CSV/log/resource samples and generated charts into dated research
  artifacts with source and dataset checksums.
- Update research pages with complete frontiers and failures.
- Change normal documentation or runtime defaults only if the automated
  promotion validator proves every gate; otherwise retain
  `srht-pq-scan + scan` and label alternatives experimental.
