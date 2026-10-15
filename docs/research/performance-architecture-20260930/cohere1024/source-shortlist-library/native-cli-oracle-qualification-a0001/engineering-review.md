**HOLD for actual-input admission and paid cold execution.** This is a source-only verdict, not native verification.

What this change does: adds three bounded CLI validators for the fixed eight-query panel, historical rank/count parity, and independently reconstructed SQ8 coverage over the **recorded** SOURCE frontier. I reviewed immutable `ccbb561… → 05fe283…`; the regenerated two-file diff SHA256 matches `fb0dc543…832ee`.

All line references below are at candidate `05fe283…`.

**Must fix before admission:**

1. **Invalid plane metadata can trigger the entire 264 MB SOURCE read before rejection.**
   [v9_panel.rs:55](/home/rb/git/borsuk/crates/borsuk/examples/compare_native_replay/v9_panel.rs:55) checks selected plane fields, then loads records and mean at lines 71–72. Schema, seed, `page_rows == 32`, and positive generation are validated only afterward by `PageAuthority::load_two_bit` ([sq8_page_authority.rs:164](/home/rb/git/borsuk/crates/borsuk/src/sq8_page_authority.rs:164)). This is a source-proven admission-order defect; it does not produce false success, but consumes large I/O and allocation for an already-invalid manifest.
   **Smallest falsifier:** a correctly hashed/rebound plane with `page_rows: 64`, otherwise valid panel, and nonexistent records path. The current path reaches the records-open error before rejecting the plane.
   **Smallest fix:** validate descriptors upfront; load digests and construct the authority before loading records/mean. Test that malformed plane metadata leaves records unopened.

2. **Durable success reports omit the identity of the evidence they validated.**
   Oracle output at [v9_panel.rs:145](/home/rb/git/borsuk/crates/borsuk/examples/compare_native_replay/v9_panel.rs:145), parity output at line 192, and panel output at line 716 omit the reduction-config SHA and panel-input SHA. Parity preserves only the historical report hash. Consequently, distinct valid runs with identical ranks/charges but different timing or runtime identities can produce indistinguishable reports. Internal hashing works, but the saved result cannot identify its candidate evidence without an external receipt.
   **Smallest falsifier:** two separately pinned valid panel fixtures differing only in timing, with corresponding seals/totals updated; compare their success reports.
   **Smallest fix:** include the authenticated config SHA and byte count in each wrapper’s report. That config already binds the input descriptors; no new framework is needed.

**Should fix:**

3. **The exact-cover oracle accepts fabricated target/shortfall counts.**
   [v9_panel.rs:894](/home/rb/git/borsuk/crates/borsuk/examples/compare_native_replay/v9_panel.rs:894) requires the selected pages to equal the complete independently reconstructed shortlist cover. However, delegated geometry validation permits `target_pages ≥ selected.len()` with a matching positive shortfall. The producer computes target pages from those same shortlist blocks ([two_bit_generation.rs:829](/home/rb/git/borsuk/crates/borsuk/src/two_bit_generation.rs:829)).
   **Smallest falsifier:** in the existing 257-row tie fixture, change only `target_pages: 8 → 9` and `target_shortfall: 0 → 1`. Source inspection shows it still passes. Selected pages/ranges remain correct, but impossible accounting is accepted.
   **Smallest fix:** within this oracle, require `target_pages == selected.len()` and `target_shortfall == 0`; add that mutation test.

The inspected scoring path is consistent: original f32 query preparation matches production, descending f64 score plus ascending physical-row ties matches the serving heap, and the oracle scores every recorded SOURCE unit before selecting 256 rows. Payload hashes, unit digests, and final artifact snapshot rechecks are present. Create-only output, file/directory synchronization, and nonzero failure exits are wired through the shared report writer.

The added tests meaningfully cover rank mutations, charges, ties, partial tails, duplicate units, and insufficient workspace. They do **not** establish successful end-to-end oracle execution, actual CLI exit behavior, or the 384 MiB/160 MiB payload models. Those models are not RSS guarantees.

Required native follow-up on the exact frozen revision:

- Compile affected tests; run focused tests, required workspace Clippy, and `scripts/check_rust_test_build.sh`.
- Build shim/release targets and exercise all three CLI commands, including INVALID reports, exit codes, and existing-output preservation.
- Run the fixed actual-eight admission and separate disposable smoke before paid cold1000.

Not checked: native execution, scientific file bodies, allocation/RSS behavior, or live qualification status; no files or jobs were changed.
Residual risk: even successful follow-up proves selected-panel admission only—not router-frontier correctness, producer provenance, population recall, cold performance, or a vendor win.
