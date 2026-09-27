# Rust v0.1 immutable graph release candidate

Status: resident route retained as research evidence; object-native release gate open,
2026-09-27 UTC. The full-vector hydration and 2.06 GB peak RSS measured on
ReLAION-1M in V280 are outside the intended product serving contract. The
checked items below verify the resident implementation only and do not
authorize its publication as the object-native v0.1 default. The Python and
TypeScript packages retain their existing APIs and make no claim to expose the
cached dual graph until an independent parity gate passes.
This cut builds packages for Linux x64/arm64 and macOS x64/arm64; Windows is
outside the release candidate while historical Unix-only modules remain.

## Contract

The release route is one immutable, authenticated FP16 source plane, diverse
source graph, source-trained PQ64 books/codes and physical row map. Search
uses the V269 cached PQ and fast exact graph union with PQ width 4096,
shortlist 4096 and exact width 2048, then ranks the union in FP16. V271 has
fresh CoHere first1M evidence; V280 verified the ReLAION-1M cosine baseline
through the authenticated HTTP path with exact local ID parity. V272's
CoHere10M library scale gate failed its frozen recall threshold. These are
different workloads and do not establish competitor superiority.

The crate must expose a small create/publish/open/search path. The caller
supplies unique `u64` IDs and finite nonzero F32 vectors, a monotonic generation
number, object-store credentials and a memory cap. Publication writes the
five immutable blobs and root before a conditional head update. Opening pins a
head, authenticates its root and every blob, and admits no more than the
declared search workers. Restart or replacement must never serve a mixed
generation. The object-store account and conditional head are the trust
boundary; the crate authenticates cached bytes against the pinned root, but
does not provide HTTP authentication or tenant isolation.

## Release gates

- [x] A bounded Rust search worker API reuses the cached route and rejects a
  worker lease beyond the declared memory/concurrency cap (`ffa05f39`).
- [x] Generic source-only generation creation from vectors, with a documented
  format/training contract and no CoHere-specific source or path assumption.
- [x] One runnable create → publish → cold open → search → warm reopen demo
  using the public Rust API and a local object store.
- [x] Wrong root, corrupt blob, wrong generation, duplicate ID, bad vector,
  and stale conditional publication fail closed in focused tests.
- [x] Worker cap, bounded resident memory, concurrent queries, pinned reader
  during replacement, and process restart/reload pass focused tests.
- [x] README points Rust users to the authenticated graph route and clearly
  labels the legacy `BorsukIndex` graph-free path and Python/TS scope.
- [x] A revision-pinned Rust workspace source bundle contains the crate and
  all local path dependencies; create/open/search runs from a clean extraction.
  A registry `.crate` is blocked by unpublished workspace dependencies and is
  outside this first cut.
- [ ] Required CI gates pass on the exact release commit. `cargo fmt --all
  -- --check` passes at `69aa4ab6`. The Python package, examples, and release
  policy checkers pass pinned Ruff format/lint locally. Historical campaign
  scripts have 740 format and 451 lint failures in the broad check; CI now
  scopes Ruff to maintained package/release code and leaves campaign scripts
  to their focused tests. CI has not been verified green on a release commit.
- [x] Benchmark claims remain tied to their source revisions and scopes:
  V271 fresh CoHere1M frontier and ANN control, V272 10M no-go, and V280
  ReLAION1M HTTP serving pass. A matched vendor win remains unverified.
- [ ] Replace full-plane hydration in the product route with authenticated,
  generation-pinned on-demand object reads and explicit total memory admission.
  Preserve per-query GET/byte/error accounting. Qualify candidate coverage and
  physical read cost together at 100k before one frozen 1M cold HTTP gate.

V270 `a0001` was launched before the release-cut directive, then deliberately
stopped at 2026-09-26 UTC without a terminal measurement. Spot instance
`i-03c2d9018d7224bcb` is terminated. Its S3 reservation remains historical
evidence; no V270 performance or quality number exists.
