What this change does: adds an authenticated three-run comparison for historical Q32, local full1000/panel32, and S3 full1000/panel32. **I found one blocking implementation defect.**

Reviewed `c9e6f9572148b4e04316146be409ae384442e6ff` against `b4406c98…` through Git blobs. Line numbers below refer to that candidate, not the stale checkout.

**Blocking defect**

1. **Successful parity always exits with failure.**
   The new reducer returns `status: "EXACT_PREFIX_PARITY"` at [compare_native_replay.rs:2120](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/examples/compare_native_replay.rs:2120). Its CLI branch directly returns `execute_report(...)`, but that writer returns true **only** for `"MEASURED"` at [line 3366](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/examples/compare_native_replay.rs:3366). `main` maps false to exit 2.

   Consequently, completely valid evidence produces a durable positive JSON report and a failing process status. Automation rejects the successful gate; retrying with the same output path then fails its create-only check.

   **Smallest fix:** extend the writer’s success predicate specifically for the parity schema and `EXACT_PREFIX_PARITY`. Preserve existing modes’ behavior and INVALID exit handling. Do not use a broad “anything except INVALID succeeds” predicate. Test the dispatch result and eventual release CLI exit, not just `reduce_scale_prefix_parity()`.

**Other source findings**

I did not establish another blocking defect in the requested areas:

- **Truth boundary:** the normal dispatch path completes all three strict readers—including terminal reconciliation, whole-result hash, EOF and descriptor checks—before opening either request pair or truth pair.
- **Ordinals and underfill:** despite `Sample` not retaining an ordinal, the reader enforces query ordinals, panel slots and recall ordinals. The new mode fixes all three sequences to 0–31. Requiring ten returned hits therefore also enforces zero underfill.
- **Missing/null fields:** removing an exemption requires its presence; the subsequent reader validates its type and meaning. Local `credential_source: null` is intentional. Missing execution, receipt hashes, or malformed fetch policy do not silently become defaults.
- **Exemptions:** executable/component hashes, producer authority, corpus geometry, reserved-query digest, native payload/order hashes and emitted policy remain invariant. Both fresh arms must share both receipt hashes. Generation equivalence still depends on the explicitly external provenance gate.
- **Prefix authentication:** both complete bodies are hashed before comparing prefixes. Comparison uses the same open descriptors and checks their stamps afterward. It does not reopen paths and accidentally compare replacement bytes. Unlike `Rows::finish`, it does not recheck pathname identity; its proof concerns the authenticated descriptors, not continued ownership of those pathnames.
- **Duplicate evidence/resources:** run hashes and device/inode identities are compared. Configuration, result lines, result bodies and prefix bodies have finite bounds. I found no new unbounded allocation.
- **Compilation:** no definite compile-time defect emerged from source inspection. `Seek::rewind`, equality operations, test types and dependencies appear consistent; this is not compilation evidence.

**Known remaining qualification**

The missing dispatch fixture and remote execution are already declared limitations, not additional discoveries.

The narrow positive fixture should reuse `v2_fixture`, the v7 row construction in `source_bound_full_population_scale_report_and_frozen_config_mismatches`, and `encode`/`authenticate`:

- Construct three **synthetic** sealed runs with the exact required geometry and backend roles, equal ordered results/score bits/hits/charges, and valid local versus S3 transport records.
- Create four deterministic request/truth bodies with the exact required lengths and prefix relationships; bind their actual hashes throughout the fixture.
- Keep expected identity/input rows independently specified, so mutations cannot silently update their own authority.
- Exercise the actual argument dispatcher through `--scale-prefix-parity`, then assert success, the parity status, false performance/cold/vendor claims, unchanged inputs, and create-only refusal without modifying the first report.

Synthetic bodies cannot satisfy the release reader’s hardcoded reserved-query digest. Use the existing test-build allowance for this fixture, preferably through a small dispatcher shared with `main`; **do not relax production pins to accommodate it**.

Required negative cases can remain focused:

| Area | Concrete falsifier |
|---|---|
| Exact parity | Change query 31’s ID, finite score bit or hits; redistribute logical charges between queries while preserving aggregate totals. Reconcile surrounding totals/seals so these reach the parity comparison. |
| Sequence/completion | Duplicate/reorder a query or recall ordinal; wrong selected slot; underfill; failed GET; truncated, unsealed or unsuccessful terminal. |
| Authority/exemptions | Change component/source/policy pins; change only one fresh receipt; omit/null execution or required exemption; unknown/duplicate JSON fields. |
| Input authentication | Correctly hashed but unequal prefix; unchanged prefix with corrupted suffix and stale whole-file hash; truncation, growth and deterministic mutation during reading. |
| File handling | Duplicate run hash/inode, symlinked file/parent, FIFO, occupied output, and write/sync failure propagation. |
| Truth boundary | Corrupt each arm in turn, particularly the third, and observe that no request/truth open is attempted before rejection. |

The existing real native fixture at [check_cohere_native_baseline.rs:2698](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/bin/check_cohere_native_baseline.rs:2698) uses 32 corpus rows and one/two queries. It cannot directly prove this mode’s hardcoded geometry. Do not relabel rewritten tiny-run output as native 1M evidence.

**Actual closed three-run evidence is sufficient native-pipeline proof alongside those dispatch/falsifier tests**, provided the frozen release CLI consumes the original unmodified artifacts and the evidence includes independent source/config authority, producer exit/durability receipts, reducer exit 0, and generation/publication provenance. Historical Q32 evidence alone is insufficient. This also does not replace the acknowledged Spot compilation, tests, Clippy, test-build, actual-input admission or staging gates.

**Verdict: fix 1 before freezing; qualification remains open.**

Not checked: compilation or runtime behavior. No files were edited; no native/data/mock/validator execution, network/AWS access, delegation or consultations were performed.
