**No required source repair found. Keep `b82e524` UNVERIFIED pending the contracted remote qualification.**

Reviewed candidate `b82e524c5fe4d50a0428af887f5c3fb23785e97e` against exact parent `c31c51627e460f59e47dfbcb9cd17e7f6c2ed779`. Both owned-file hashes match the contract. Line references below are candidate-object lines.

What this change does: it reuses arrival and copy-completion timestamps, reducing successful-range probe reads from `5c + 10` to `2c + 9`. It intentionally broadens diagnostic intervals and marks runner diagnostics as v2.

- **Collector correctness:** `sq8_s3_range.rs:853–881` preserves the stream polling, checked length admission, and copy operations. Metadata/ETag checks and page authentication remain intact at `836–846` and `884–896`. The reused timestamps form adjacent gap/copy intervals; I found no new authentication bypass, error suppression, or algorithm change.
- **Test/API compatibility:** `ChunkStore` at `1821–1900` implements the required methods of pinned `object_store 0.14.1`; replacing its public `GetResult.payload` with the boxed stream matches that API. Inspection found no concrete compiler or required-Clippy defect. This is not compilation evidence.
- **Counter validity:** the counter at `909–919` counts actual `Probe::now_ns` calls without adding a field to `Probe` or the async frame. Pinned Tokio’s default `#[tokio::test]` runtime is single-threaded; these directly awaited tests do not migrate their probe work between threads. The success, overlong, and stream-error expectations are respectively `2c + 9`, `8`, and `9`.
- **Test coverage and RAM:** deterministic cases at `1928–2059` cover coalesced, multiple/empty chunks, stream error, overlong, truncated, empty-body, and digest failure. The HTTP test at `2062–2095` correctly counts delivered chunks without assuming HTTP framing. Future-size allowances remain unchanged, but actual compiled size still requires the existing test at `3326–3368`; source inspection cannot establish it.

**Optional improvements, not release blockers:**

1. **Make emitted semantics as explicit as Rust documentation.** `check_cohere_native_baseline.rs:1613–1618` still describes body gaps only as application-observed poll gaps and provides no copy-interval explanation. Update those strings to state prior-copy-completion/initial-mark boundaries and arrival-to-copy-completion accounting. The smallest check is an assertion on the existing panel’s admission record. Without this, an analyst reading JSON alone may miss the broader attribution.

2. **A standalone trace marker is optional for current callers.** `Sq8RangeTrace` directly derives serialization at `sq8_s3_range.rs:1118`, without its own version. However, the current non-test serialization path wraps it in `QueryDiagnostic`, which supplies v2 at `check_cohere_native_baseline.rs:1183–1206`, including failures at `1791–1795`. I found no current standalone persistence consumer requiring repair. If standalone trace persistence becomes supported, add a timing-semantics marker there and test direct serialization; no legacy reader is needed.

The change is a valid distinct **operation-count hypothesis** for fixed delivered chunk count. It establishes neither time saved nor an overhead-gate pass. Changed intervals cannot support pure-copy, bounded-nanosecond, or TCP-cause claims. The closed canary remains INCONCLUSIVE, and the whole-process CPU gate remains necessary.

The smallest next falsifier is the existing clock-count test on the exact candidate, followed by the contracted HTTP, layout, parity, release, Clippy, and workspace test-build gates.

No files changed; no builds, native execution, network calls, children, or additional reviews ran.
