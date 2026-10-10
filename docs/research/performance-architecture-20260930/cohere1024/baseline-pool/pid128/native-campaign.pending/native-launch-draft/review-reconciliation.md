# Wrapper review reconciliation — source repairs, runtime UNVERIFIED

One group259d91aacc8c4b8f completed. Research f582314fecae4da4 and engineering
98620c79dd5249b8 reviewed immutable7cffa155. Both original full results are
retained beside this file. No duplicate review or experiment was launched.

1. Immediate shutdown: a default EXIT request is installed before fallible
   evidence-directory/log creation; finish installs a nested EXIT request before
   reporting. Archive, length, sync and upload failures cannot bypass the request.
   The root watcher also checks instance state every30s to stop polling an absent
   terminal after premature termination. This is a request, not confirmed cleanup.
2. Early failure evidence: create the evidence directory with the bootstrap
   directory before setup; omit installer ZIP/tree from tar. If jq/AWS is not yet
   installed reporting can still fail; shutdown and root-side metadata survive.
3. Backward budget: request deadline is launch+17900, machine deadline+18000.
   Require16800+400 remaining against the former; setup/transport share700s.
   Parent runtime16860, polling16900, cleanup manager90s, tar60s, hash10s,
   sync10s, uploads30s each are finite. Publication commands additionally clamp
   to request-deadline-minus5. Local filesystem operations can still stall;
   independent root termination remains the hard safety backup. No billing cap.
4. Body bounds: all19 support GETs range expected length plus one and validate
   response full ContentRange, exact length and SHA. Installer curl max-filesize
   is the pinned73022935. Native ELF S3 GETs additionally If-Match target HEAD
   ETag. Root collection GETs are range-bounded too. Actual AWS range handling is
   target-UNRUN; byte/SHA errors are INVALID, never algorithm failures.
5. Retained native replay: explicit18-path roster copies raw diagnostic/query16/
   query32/staging results, all7 generated configs, cohort/derivation/publication
   receipts, generation/plane manifests and published head. Each file canonical,
   regular/non-symlink, <=4MiB, copy hash/length checked and source path recorded.
   Missing entries are recorded for failed attempts; every entry is required for
   a closed campaign. No source arrays, corpus, requests/truth or index payloads
   are copied. Closure/seal replay must verify original absolute identities using
   this mapping after termination, without running an ANN or opening GT locally.

Static source-only check: run-p4140563-i704456525.service invocation
07fd8c7131d84791aaf0b0c07a540807; source assembly, worker/watcher bash -n and
ShellCheck all0; CPU1/AllowedCPU0/256MiB/zero swap/PID128/120s,515ms,44.4M.
No entrypoint, fixture, transport, Rust or jq runtime admission ran locally.

Remaining: freeze final source/support/request and one launch; actual EC2
transport plus runtime admission/staging/controlled-negative/widths; root
replay from the retained originals. This document is not release acceptance,
performance qualification, or authority to skip any of those gates.
