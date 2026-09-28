# Production package boundary: GO; production release: HOLD

## Verified result

The original extracted-package smoke session 83096 exited 0 on existing Spark
`spark-2751` (aarch64). Two source preparation tests, four signed HTTP range
integration tests and one package metadata test passed. The native source-builder
example compiled and then executed normalization and physically ordered SQ8
creation on two rows; the resulting hashes and ordinal order were checked.
The HTTP fixture covers public normalization/build/publish/head/open/search and
corrupt bytes, wrong ETag and conditional failure. Metadata publication uses
InMemory storage, and the HTTP endpoint uses fake credentials. This is not live
IAM, full-corpus search or a latency measurement.

The three source archives have SHA-256 receipts in `archives.json`; remote
readback matches all three. The normalized production manifest and resolved
package graph contain no V25/V41 dependencies. Package-local fixtures retain
exact historical bytes. The archives were created from the recorded dirty
source delta on base `b4751563`; `.cargo_vcs_info.json` is preserved in
`verification.json`. The delivered commit records that source change, but
these archives are development artifacts, not a clean publication cut.

## Decision and causal repair

V25 was used only by an unregistered containment example. V41 was used only by
the old learned-router diagnostic module/export and its unregistered example.
Those entry points are removed from the current prerelease crate. The private
research crates remain workspace members. Historical campaigns must use their
immutable original revisions/source archives; current code has no prerelease
compatibility contract. Serving, source preparation and generation algorithms
are unchanged. FMA/PQ4 package descriptions and the existing project license
are included.

First smoke session 30795 failed before compilation because Spark's offline
cache lacked archived `adler2` 2.0.1. Its exit 101 and log are preserved. Session
83096 fetched the archive lockfile's dependencies, then ran locked offline
checks with helper packages extracted locally. No duplicate live job, local
compilation, new machine, paid benchmark or registry publication was started.
The smoke exit-receipt path was also corrected to the campaign directory.

`verification.json`, `spark-smoke-summary.txt` and the lossless
`spark-smoke.log.gz` record scope and outputs. Existing documentation warnings
remain. Packaging warns about locked, yanked `chacha20` 0.10.1; the locked build
passes, but fresh registry installation is unverified. The full formatting/CI
release gates remain open; root formatting currently reports preexisting
module-order/child-file differences.

## Next product gate

Close native semantic layout construction so the public Rust path can create a
complete candidate from raw vectors without Python-fitted inputs. Reuse the
frozen source-only method and gate any changed layout on the existing paired
100k development/validation protocol before cold HTTP promotion; do not tune on
validation. Arbitrary application IDs and incremental mutation/compaction/GC,
fresh 1M cold HTTP, scale/cost and matched vendor evidence remain incomplete.
No quality or latency number changed in this packaging slice, and no production
release or vendor win is claimed.
