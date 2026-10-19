**Run one query-free, authenticated ownership witness before changing the collector.** The strongest explanation is transport-buffer sharing plus normalization costs, but their frequency in real S3 responses remains unmeasured. Keep the prototype unpromoted.

All repository inspection occurred through SSM on the specified EC2 instance, within CPU1/512MiB/swap0/tasks128/120s limits. I verified donor `1f04de55…` and prototype `e578c535…`. The supplied equivalence between main `2c2918f…` and the donor Rust tree remains an input to this report.

The inspected dependency sources match crate archives whose SHA256 values match `Cargo.lock`: **bytes 1.12.0, reqwest 0.13.4, hyper 1.10.1**. The active backend is vendored **object_store 0.14.1**; the separately locked 0.13.2 is not this S3 path.

| Layer | Exact inspected path and lines | Ownership consequence |
|---|---|---|
| Native transport | Prototype `sq8_s3_range.rs:1446–1488` | Explicit `.http1_only()`, verified TLS, no redirects or reqwest retries. HTTP/2 is outside this path. |
| Hyper receive buffer | hyper `src/proto/h1/io.rs:223–245,345–351` | Reads into reusable `BytesMut`; emits `read_buf.split_to(n).freeze()`. |
| Split ownership | bytes `src/bytes_mut.rs:398–417` | `split_to` creates shared backing references, including the producer’s remaining buffer handle. |
| Reqwest forwarding | `src/async_impl/response.rs:480–484`; `body.rs:138–149,426–440` | Wraps the body and forwards `Bytes` through identity conversion. |
| Object-store forwarding | Vendored `src/client/http/connection.rs:220–234`; `src/client/get.rs:168–214` | Wraps frames and yields their data without cloning payload handles. |
| Native accounting | Prototype `sq8_s3_range.rs:1339–1398` | Checks response metadata and borrows frame data for counters; returns the original frame. |
| Prototype collector | `sq8_s3_range.rs:1014–1065` | Converts chunks to `Vec`, repairs excess capacity, and falls back to coalescing after F256. |
| Authentication/scoring | `sq8_page_authority.rs:297–339`; `sq8_s3_range.rs:2595–2625`; `two_bit_generation.rs:3811–3842` | Checks complete response length and original-unit hashes before invoking scoring. |

Hyper’s split/freeze operation is allocation-sharing machinery, rather than an allocation-size contract. Its producer may subsequently detach or release the buffer before the consumer converts a chunk; therefore the source establishes a sharing mechanism, **not its observed distribution**. EOF also does not guarantee that a pooled connection has released its receive-buffer allocation. [Hyper 1.10.1 source](https://github.com/hyperium/hyper/blob/v1.10.1/src/proto/h1/io.rs)

The safe API distinctions are:

| Input ownership | Locked conversion behavior |
|---|---|
| Unique, convertible backing | `try_into_mut()` can succeed without copying. `Vec::from(Bytes)` can reuse the allocation while moving an offset payload back to its allocation base. |
| Shared backing | Conversion to `Vec` copies visible bytes while another owner remains. |
| Static or opaque `from_owner` backing | `is_unique()` is false; `try_into_mut()` fails. Conversion copies visible bytes. False therefore does not identify the owner category. |

`Bytes::len()` describes the view. Even a successful conversion’s `BytesMut::capacity()` can exclude a retained prefix. `Vec::capacity()` exposes the resulting vector’s full capacity, but after a copying conversion it says nothing about the original backing allocation. Pointer comparison witnesses address preservation or movement, not allocation identity or hardware memcpy instructions. [Bytes 1.12.0 source](https://docs.rs/bytes/1.12.0/src/bytes/bytes.rs.html)

Consequently, the existing **2B bound remains a normalized library-payload/scratch bound**. It does not bound arbitrary backend owners, TLS buffers, allocator overhead or process RSS.

The mechanism hypotheses, ranked by evidence, are:

1. **Shared receive backing removes much of the synthetic handoff opportunity.** The actual HTTP/1 path supplies a concrete mechanism. Its byte-weighted frequency at conversion remains unknown.
2. **Normalization, F256 fallback and crossing-record copies explain additional movement.** These branches are established in source. The supplied real-S3 totals do not partition their contributions.
3. **Segment management costs offset saved movement.** Descriptor processing, allocation and boundary handling are included in collector CPU. Existing timings cannot isolate their causal cost.

The supplied synthetic −92.459% movement result and real-S3 +6.2339% result describe different ownership conditions. The selected8 wall ratio `.80875` remains an unreplicated observation; it does not establish a causal latency gain.

For the requested simulated perspectives—generated arguments, not expert testimony:

| Perspective | Argument and objection | Falsifiable prediction |
|---|---|---|
| Rust HTTP systems | Observe ownership where conversion actually occurs; adding clones changes the measurement. | Real chunks exhibit address movement or excess-capacity repair absent from fresh exact-capacity synthetic chunks. |
| Memory safety/security | Preserve complete authentication and count upstream memory separately. | Visible-length accounting cannot establish a generic backend-owner ceiling. |
| Retrieval researcher | Hold source bytes and authentication geometry fixed. A transport diagnostic establishes no recall result. | Ownership observations require no requests, truth or scoring. |
| Skeptical operator | One bounded diagnostic should resolve the next decision. | A reconciled movement ledger identifies whether immediate handoff has an observed opportunity without a latency campaign. |

The one actionable intervention is a **temporary native diagnostic ledger around the existing F256 collector**, owned and executed by the parent:

1. **Admit source-only inputs.** Freeze the diagnostic revision, lockfile, transport configuration, object identity/ETag and authenticated unit geometry. Choose the first contiguous sequence of complete authentication units fitting within **4MiB**, using source metadata alone. If one unit exceeds that cap, stop before GET. Keep request and truth files unopened. Complete the repository’s required source-bound admission and separate disposable canary first.
2. **Issue one real range GET.** Use the existing native transport, concurrency 1, application cache off and a one-attempt budget. Preserve header checks, complete-body authentication, error handling and cleanup. Invoke a no-op authenticated callback; perform no scoring. This is application-cache-off evidence; S3’s internal cache state remains uncontrolled.
3. **Observe without retaining or cloning incoming `Bytes`.** Immediately around the existing conversion record aggregate counts and bytes for: incoming length/`is_unique()`, original-address preservation, returned vector capacity, normalization copies, F256 prefix coalescing, subsequent appends and crossing-record copies. After coalescing, mark conversion-capacity observations unavailable rather than inventing them. Publish counters only with successful authentication.
4. **Reconcile and terminate.** Assert that branch totals reproduce `collector_copied_bytes`, crossing copies reproduce `owned_copied_bytes`, and accepted lengths equal the authenticated range. Bound metadata input to 8MiB and diagnostic output to 64KiB; retain CPU1/512MiB/swap0/tasks128/120s limits. Record terminal status and cgroup memory events. Authentication, instrumentation, timeout or resource failures are **INVALID execution**, not performance rejection.

The bounded falsifier is: **does any pre-fallback payload reach the existing collector with its address preserved and returned vector capacity equal to length?** Zero eligible bytes falsifies immediate zero-movement handoff opportunity for that authenticated response. Positive eligibility quantifies an opportunity; it establishes neither a generic S3 ownership distribution nor a speedup. The ledger also separates conversion movement from fallback and boundary costs without unsafe introspection.

Three ranked research paths follow that witness:

1. **Locked bytes/Hyper ownership-aware normalization.** Use the ledger to determine whether a useful safe fast path exists under the current owner contract. Investigate a producer contract only if observed movement warrants it; retain normalization for generic backends. [Bytes 1.12.0](https://github.com/tokio-rs/bytes/blob/v1.12.0/src/bytes.rs), [Hyper 1.10.1](https://github.com/hyperium/hyper/blob/v1.10.1/src/proto/h1/io.rs)
2. **Lance I/O scheduling and release ordering.** Its scheduler documentation, displaying 11.0.0, makes unread-byte backpressure explicit. Study admission/release ordering for possible overlap improvements. Its priority exceptions can exceed the byte budget, so that accounting is not an upstream-owner safety proof. [Scheduler documentation](https://docs.rs/lance-io/latest/lance_io/scheduler/struct.SchedulerConfig.html), [implementation](https://docs.rs/lance-io/latest/src/lance_io/scheduler.rs.html)
3. **Reduce cold fetch work through locality and asynchronous partition retrieval.** DSANN v1 §§III/V motivates graph-guided partitions and overlapping retrieval; SPANN v1 §3.2 supplies posting-length and boundary-replication tradeoffs. These are hypotheses to test against fixed quality and lifecycle cost. DSANN’s experiments use Pangu DFS, and SPANN’s use SSDs; neither establishes a cold S3 result. [DSANN v1](https://arxiv.org/html/2510.17326v1), [SPANN v1](https://arxiv.org/html/2111.08566v1)

Durable evidence: checksum verification `de62a353-7c3f-4fa7-9575-3696186a37e1`; transport/authentication trace `596040a3-de65-4930-ba52-a1afdb1818b7`; existing ownership-contract audit `5f914950-a333-4c79-b330-3cde558ab6c0`, all **Success/0**.

Skipped: edits, builds, tests, data GETs, raw payload inspection and infrastructure changes.  
Remaining risk: real ownership frequencies, upstream allocation lifetimes and causal performance gains are still unmeasured.