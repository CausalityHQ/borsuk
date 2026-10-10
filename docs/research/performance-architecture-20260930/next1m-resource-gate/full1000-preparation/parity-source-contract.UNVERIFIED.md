# Scale prefix parity source checkpoint — UNVERIFIED

Only crates/borsuk/examples/compare_native_replay.rs changes. New CLI: --scale-prefix-parity CONFIG CONFIG_SHA NEW_REPORT. Schema borsuk-scale-prefix-parity-config-v1: three completed-scale configurations ordered historicalQ32/local1000-panel/S3-1000-panel, request prefix/full pins and truth prefix/full pins.

All three native seals/recall/terminal records validate before any request/truth opens. Compare32 ordered results, score bits, hits and logical charges with zero failed GETs. Authenticate whole input bodies and descriptor stamps before exact prefix comparison. Non-exempt policy/source/corpus fields remain equal; exemptions are explicit and independently checked by the scale reader. New local/S3 panels share both producer receipts. Root generation/provenance equivalence remains an external prerequisite.

Six authored tests cover prefix/suffix/truncation/growth, ordered IDs/score bits/recall/underfill/charges, policy invariants, full synthetic reducer/report dispatch, pathname replacement, direct/hard-link role aliases and resealed incompatible config reuse. None compiled or executed. Actual frozen release CLI execution on closed three-run evidence remains required; synthetic dispatch does not qualify corpus or S3 runtime.

Source-review repair: execute_report recognized only MEASURED, incorrectly returning failure for EXACT_PREFIX_PARITY. The new success alternative requires matching parity caller/report schemas, EXACT_PREFIX_PARITY and complete=true. Historical handling is unchanged. Original c9e6f957 is preserved. The new fixture checks success publication, occupied output, geometry/policy/receipt/duplicate refusals and broken seal before absent truth.

Remaining review findings repaired in source: three native config hashes must differ; prefix and run paths are rechecked against authenticated identities; all seven run/request/truth roles are metadata-checked for direct/hard-link aliases before any run-content read. The common reader has an optional admitted-inode check before its first row read; existing callers retain their original behavior. Broken-seal fixture uses a nofollow truth symlink, since metadata admission deliberately requires all role names to exist.

Local rustfmt passed inside CPU1/256MiB/noSwap/PID128/120s units using the exact formatter and skip_children=true for the isolated snapshot. Initial PATH/toolchain/module-path formatter failures remain in tool receipts; no toolchain defaults changed. No local Cargo/rustc/native/runtime-validator/data execution.

Remote causality EC2 Spot gates: affected example tests including historical modes and dispatch falsifiers; locked release example; locked workspace/all-target Clippy correctness+suspicious; actual env-unset scripts/check_rust_test_build.sh, jobs1. Retain exact source/status/resource/artifact/termination evidence. This is source-only, not production integration or measurement authority.
