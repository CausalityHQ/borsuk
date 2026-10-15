What this change does: Adds v9 validation to the shared experiment reader and correctly labels the synthesized historical fixtures. I verified that the Git diff matches the frozen patch SHA. References below are against candidate `02044f35741d5f16dad838dcd5121ab4cc7e5724`.

**Must fix:**

1. **Payload admission happens after an unbounded allocation.**
   `crates/borsuk/examples/compare_native_replay/v9_panel.rs:187–193`, `read_candidate`.

   With otherwise valid headers and an 8 MiB budget, add a 16 MiB string field to `identity`. Earlier checks allow it; `serde_json::to_vec(value)` allocates the entire serialized header before checking `CONFIG_CAP`. Caller funding of the borrowed tree does not cover this new owned buffer.

   **Minimal repair:** Bound serialization while writing, rejecting at 32 KiB. Add a check proving oversized headers stop at that limit. Otherwise malformed input can exceed the advertised payload envelope before rejection.

2. **The traced admission path accepts contradictory memory-admission evidence.**
   `crates/borsuk/examples/compare_native_replay/v9_panel.rs:70–72`, delegating to `compare_native_replay.rs:790–804`.

   Take `v9_panel_fixture()`, set row 2’s `trace_peak_charged_by_library_at_traced_admission` to `false` and `trace_peak_bytes` to `null`, then reseal and repin. The v9 validator changes its temporary `trace` value to `false`; the historical validator checks neither changed field. The subsequent shared check only restores agreement on the original trace flag and panel scope. This contradictory traced admission therefore survives the source checks.

   The producer explicitly emits the charge flag from `c.execution.trace()` and supplies a peak value for traced execution (`check_cohere_native_baseline.rs:2058–2064`).

   **Minimal repair:** Validate those trace-dependent admission fields before constructing the neutral view, with a resealed negative test. Otherwise successful reader validation can include an explicitly uncharged trace reservation.

I found no source defect in the canonical range merging, partial-tail accounting, exact SQ8 charges, or raw-byte seal/SHA preservation. The historical fixture relabeling is consistent with its synthesized 256-row geometry; the direct native tests remain unmodified. The disclosed missing CLI and parity oracles are not findings.

**Verdict: HOLD** for these repairs and the root-owned targeted five plus new four tests, full 256-test run, Clippy, shim-unset workspace test-build, and release gates. The supplied `90508968` execution evidence does not qualify this candidate.

Not checked: native execution, compilation, RSS, release or retention; prohibited by this review’s scope. No files changed. Findings are source-derived, not executed reproductions.
