# Rust v0.1 immutable graph release candidate

Status: in progress, 2026-09-26 UTC. This is a Rust crate cut. The Python and
TypeScript packages retain their existing APIs and make no claim to expose the
cached dual graph until an independent parity gate passes.

## Contract

The release route is one immutable, authenticated FP16 source plane, diverse
source graph, source-trained PQ64 books/codes and physical row map. Search
uses the V269 cached PQ and fast exact graph union with PQ width 4096,
shortlist 4096 and exact width 2048, then ranks the union in FP16. Recall and
latency evidence is currently limited to the V269 CoHere first1M D768 cosine
k100 prior-used panel, so other builders, dimensions and queries need fresh
quality evidence. No 10M or competitor superiority claim follows.

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
- [ ] Wrong root, corrupt blob, wrong generation, duplicate ID, bad vector,
  and stale conditional publication fail closed in focused tests.
- [ ] Worker cap, bounded resident memory, concurrent queries, pinned reader
  during replacement, and process restart/reload pass focused tests.
- [x] README points Rust users to the authenticated graph route and clearly
  labels the legacy `BorsukIndex` graph-free path and Python/TS scope.
- [ ] A revision-pinned Rust workspace source bundle contains the crate and
  all local path dependencies; create/open/search runs from a clean extraction.
  A registry `.crate` is blocked by unpublished workspace dependencies and is
  outside this first cut.
- [ ] Required CI gates pass on the exact release commit. The current broad
  Rust formatting drift is a known gate blocker; fix it before the cut.
- [ ] No release benchmark claim beyond V269's exact measured scope. After
  the cut, one fresh-query matched recall/latency frontier plus a standard
  ANN control precedes one 10M scale/cost gate.

V270 `a0001` was launched before the release-cut directive, then deliberately
stopped at 2026-09-26 UTC without a terminal measurement. Spot instance
`i-03c2d9018d7224bcb` is terminated. Its S3 reservation remains historical
evidence; no V270 performance or quality number exists.
