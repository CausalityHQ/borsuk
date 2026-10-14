What this change does: `8910ef50c1cce155eb396d71a2e05dd3b9e906a0` closes the SourceABBA header-window binding gap before any result file opens. I reviewed the broader reader, connected validation and replay code, tests, and supplied evidence.

**Verdict: GO for the next gate—native qualification of the repaired revision.** I found no remaining blocking correctness, memory, or path-security defect within this reader’s declared scope.

The repair is sound. The exact runtime field roster forbids `shared_byte_window`; both new `.get()` comparisons therefore require its absence from the headers, rejecting an object or explicit `null`. The native producer omits the field when disabled. See [the repaired checks, line 4786](ز/data/target/borsuk-source-budget96.pending/package-header-window-green/source/crates/borsuk/examples/compare_native_replay.rs:4786). I verified this snapshot’s SHA matches the immutable Git blob.

The existing [RED log, line 6301](/data/target/borsuk-source-budget96.pending/launch-header-window-red-a0002/abba-red-collected/native.log:6301) establishes the parent’s early-refusal failure: actual `A1: No such file…`, expected `SOURCE runtime/header binding`. The earlier 13-gate GREEN evidence applies to `c853f8ae`; it does not establish repaired-revision GREEN.

Nonblocking findings:

1. **The new regression case does not independently protect both comparisons.** [Line 13573](/data/target/borsuk-source-budget96.pending/package-header-window-green/source/crates/borsuk/examples/compare_native_replay.rs:13573) changes both headers together. Removing either new comparison would still produce the expected refusal through the other. Smallest improvement: separate identity-only and inputs-only mutations, keeping all result files deleted. Without those cases, a future regression can restore late refusal for one header.

2. **Replay implementation provenance remains absent from the report.** [Line 4973](/data/target/borsuk-source-budget96.pending/package-header-window-green/source/crates/borsuk/examples/compare_native_replay.rs:4973) reports replay verification without identifying the reducer and planner sources that performed it. This carries forward the prior review’s minor finding. Reuse the three hashes already emitted by the cover-curve report at line 3850. Otherwise, identifying the replay implementation continues to depend on external receipts.

3. **Report consumers must distinguish measurement success from screen success.** [Line 5287](/data/target/borsuk-source-budget96.pending/package-header-window-green/source/crates/borsuk/examples/compare_native_replay.rs:5287) replaces the inherited timing gate while retaining stricter 5% predicates and stale “both pairs required externally” wording. A 1% improvement can legitimately produce mixed booleans; a failed screen still exits successfully when measurement completed. Smallest improvement: remove obsolete wording and clearly identify the authoritative screen field. A consumer using only exit status could incorrectly promote a failed screen.

The reader checks every query’s SOURCE count/bytes against its bound budget, exact returned IDs/score bits, trace/SQ8 parity, recall9780/full-k, failures, distinct artifacts, and final file identities. Bounded reads and retained samples prevent corpus-sized reader allocations; descriptor-based path traversal and exclusive report creation address the reviewed file hazards.

I reused the prior research review. Execution order, binary qualification, cache, resources, wire accounting, and cost remain root-owned gates. The predeclared noise rule remains necessary for adoption; the modest screen alone does not establish it.

Not checked: no edits, builds, native execution, network, children, or new research; repaired seven-gate completion was not verified.  
Risk: SOURCE96 cold performance and matched wins against both vendors remain unproven.
