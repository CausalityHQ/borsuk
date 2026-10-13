# Shared native download-byte admission qualification

Candidate: 8c250abffb2d006bcbb3d3410a1bea9d731cc35d. Parent public source: 9a5e4e3aefb2a03b042c2fd985ebaa867dbeb607.

Decision: NATIVE_QUALIFIED; delivery pending. No performance, recall improvement or vendor-win claim.

Ten stages passed on identical authenticated 470-file / 437-native-file source: library test compilation; six byte-window tests; three real HTTP body/parity/drain tests; one generation fetch-path/score parity test; one production future-layout test; 31 range regressions; 33 generation regressions; release baseline; full workspace correctness/suspicious Clippy; actual shim-unset complete workspace test compilation. Independent manifest, source before/after, resource, no-swap/no-OOM and drained-process checks passed for both attempts.

Original a0001 driver exited 1 after its fifth native stage passed: --nocapture layout metrics broke the driver's single-line PASS matcher. Preserve that failure. No Rust change followed; a0002 ran only the remaining five stages. Both native sources are candidate 8c250abf. Each instance is terminated, root volume absent and external timer inactive. Original continuation SSM 4f952051-5b2c-4634-b253-a31aedbe5528 completed Success/0. Original watcher 64313 completed 0.

Library change: opt-in shared byte admission across SOURCE and SQ8 fetches, FIFO bounded waiter queue, cancellation-safe permits, explicit state memory accounting, whole-roster oversized-range refusal before I/O and accurate submitted-GET accounting. Existing default without the window is preserved. Routing metadata and leaf fetches are outside this window; request/query/authentication/scoring tiers are not claimed implemented. Completed payload retention has its own limit.

Next falsifiable gate: expose this opt-in window in the Rust benchmark configuration and identity, admit exact real inputs, stage a disposable smoke, then run paired cold measurements against unchanged width64. Require exact quality/score/plan/logical-charge parity for this scheduling-only change; measure p90/p95/p99, QPS, RSS and actual transport. No cold speedup may be inferred from correctness qualification.
