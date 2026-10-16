# GC session progress: qualified bounded library increment

Qualified source: `5d8a87e03b8bd5173ca2c227cd408d04c53eb84a`.
Integrated code: `ee78d687071ca18e1693d02da515d99c695ea2b9`.
The integration preserves main's documentation/runtime work and the original
qualified source history. All 478 packaged source/build-input files match the
integration tree byte for byte; all Rust/Cargo bytes are identical. The physical
root worktree and index were not changed.

## Library change

`TwoBitGcSession` owns one listing stream and at most one pending candidate
across bounded calls. Each call reacquires the same-directory lifecycle lock,
authenticates the current head/fence and rebuilds its keep-set. Retained or
unknown entries therefore cannot permanently starve later orphan entries in a
finite, surviving traversal. DELETE NotFound clears an absent candidate;
ambiguous errors preserve recovery state. Cancellation cannot release an
accepted worker's lock/fence prematurely. The API intentionally breaks the old
one-shot call signature; see `docs/api.md`.

The contract remains cooperating participants on one host using one trusted
lifecycle directory. It is not distributed reader-safe GC, a durable restart
cursor, a LIST snapshot guarantee, physical reclamation of versioned objects,
or a total RSS bound. Caller-configured store timeouts, runtime/SDK buffers and
thread stacks remain explicit responsibilities. Real S3 pagination/lifecycle
qualification is still pending; its existing smoke remained ignored.

## Native evidence

Attempt a0001 compiled but failed its first focused fixture: ObjectPath::join
escaped a multi-segment test key. The original failure and evidence are retained.
The test-only repair corrected path construction and its matching absence
oracle without changing production code, assertions, test names or limits.

Attempt a0002 used causality EC2 Spot, m6a.2xlarge, CPU0-7/quota800%, process
28GiB, swap0, pids512, compiler jobs1, serial native tests, native4200s,
machine5400s/external5520s and total ceiling$0.60. Native execution ran
2026-10-16T00:42:58Z through01:22:36Z.

Independent root verification exited0:

- 69 ordered stages plus the separate final source check passed.
- Debug and release GC inventories:24 each; full targets:24 passed/0 ignored
  each. Seven focused cases passed separately in each profile (14 exact runs).
- Application-ID integration:13 passed/1 explicitly ignored real-S3 smoke;
  generation integration:10 passed; semantic library parity:1 passed.
- 86 native test executions in total, including deliberate focused/full repeats.
- Workspace correctness/suspicious Clippy and genuine shim-unset jobs1 workspace
  test build passed. Compiled-source inventory445 is a byte inventory, not445
  executed targets. All478 source files and5 runtime admission identities were
  unchanged before/after.
- Cgroup peak17,655,025,664B; pids peak26; OOM0, swap0. Owned unit drained
  without forced cleanup. Original instance terminated, volume absent and
  external termination timer inactive.

The complete immutable archive is on S3; `a0002/archive-pointer.json` binds its
key and SHA256. Curated metadata includes commands/timings, test results,
source manifests, independent verification, exact drivers/verifier and cleanup.
The external terminal was published after the archive and is collected
separately. No production benchmark binary or ANN performance result is claimed.
Scoped attributes suppress text diffs for immutable logs/command receipts;
their original trailing delimiters and blank lines are preserved, not edited.
Historical source-only UNVERIFIED comments on the exact scripts are retained;
their subsequent execution evidence is recorded here without rewriting them.

## Remaining work

Real S3 multi-call pagination/lifecycle and broader production release gates
remain open. Transport splitting and SOURCE-residency proposals are separate,
unintegrated work. This increment establishes no recall/latency/QPS/cost gain,
no matched S3 Vectors/Turbopuffer win, and no completion of the product goal.
