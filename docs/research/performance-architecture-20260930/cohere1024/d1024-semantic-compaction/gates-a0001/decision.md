# D1024 semantic compaction qualification

Accepted: exact owned-three-file source from `230e8639850002fc4645311f231e1ec5c90d0b16`.
The combined frozen snapshot was `68c6243a0c560f875e33fcc31d83e80ffc6c8813`.

The library admits maintenance dimensions through the shared Native100k authority
before staging and before delta/seal work. D1024 compaction, ready-publication
recovery/retry, application-ID and score-bit preservation pass actual synthetic
native tests. D768 remains covered; D1025 refuses before side effects. The existing
unsupported-dimension fixture now uses D1025 rather than newly supported D769.

| Gate | Native exit | Evidence |
| --- | --- | --- |
| Application-ID integration, debug/release | 0 / 0 | Four new tests each once in each stage; 7 passed per stage |
| Existing compaction module, debug/release | 0 / 0 | 2 passed per stage |
| Exact semantic-store parity, debug/release | 0 / 0 | 1 passed per stage |
| Package release build | 0 | Original recorded command |
| Workspace all-target Clippy correctness/suspicious | 0 | Original recorded command |
| Actual unshimmed workspace test compilation | 0 | Original recorded command |

All 86 terminal artifacts authenticate. All 414 native source files and 2,449
build-support files match before/after and the root's exact compiled snapshot.
Original instance `i-0ab2c3a35c4304516` terminated and its waiter exited 0 before
collection. Compiler peak was 8 GiB; swap/OOM-kill were zero and MainPID drained.
Compiler peak is not serving memory. All raw artifacts are retained as deterministic
gzip; `artifact-receipt.json` maps their original bytes and SHA256 values.

The unchanged `native_s3_conditional_fence_and_gc_smoke` remains ignored because it
requires an owned AWS test prefix and credentials. This gate does not claim real-S3
maintenance correctness, latency/QPS improvement, or a production-ready product.

Next falsifier: the separately preregistered bounded source-cache exact native
gate, followed by a qualified Rust runner/reducer and the frozen disjoint-query
full-residency comparison. No existing paid run is repeated.
