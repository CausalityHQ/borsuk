**HOLD.** What this change does: adds an optional, memory-admitted byte window and a four-run ABBA reducer. I reviewed `2f049daf…` → `905c8f4…` through Git objects; all line references below belong to **candidate `905c8f4`**, not the older worktree.

**Must fix before campaign acceptance**

1. **The reducer accepts evidence that contradicts width64 admission.**  
   In `crates/borsuk/examples/compare_native_replay.rs:4700–4727`, the new mode reads runs and checks parity but omits the selector check present at lines **4455–4465**. Changing B2 query 999’s `stages.leaf_peak_inflight` to **65**, then resealing, does not affect any new-mode rejection condition. The report can therefore pass despite recorded evidence contradicting its scheduling-only premise.  
   **Smallest repair:** require each run’s observed leaf peak to be ≤64. **Native falsifier:** reseal that single mutation; require refusal, while retaining the width64 positive fixture.

2. **The stated 97.80% recall anchor is neither checked nor explicitly marked unverified.**  
   Lines **4755–4767** derive `performance_screen_passed` solely from timing. The positive fixture at **12390–12441** inherits **9,000/10,000 hits** from **11338–11346**, and **12480** asserts that the screen passes. Exact parity alone allows all four runs to share degraded recall. The external-authority flags name source qualification, chronology, resources and transport; none identifies this missing quality gate.  
   **Smallest repair:** report the anchor check and require ≥9,780/10,000 hits for campaign acceptance, keeping exact cross-run parity. **Native falsifier:** identical sealed cohorts at 9,779 hits must fail acceptance; 9,780 hits with the existing 1% timing improvement must pass.

3. **Controller-state “binding” accepts an impossible one-byte charge.**  
   At **1672–1677**, startup state only needs to be positive and below `max_state_bytes`. A resealed B startup with `shared_byte_window_state_bytes: 1` passes, although the admitted 64-waiter controller necessarily costs more.  
   **Smallest repair:** require the reported charge to be at least `InFlightBytes::modeled_state_bytes(max_waiters)` and no greater than the configured cap. **Native falsifier:** reject one byte and modeled-minimum-minus-one; accept the actual native charge.

**Should fix**

4. **First-query latency is absent despite the report claiming it is retained.**  
   New report construction at **4744–4753** calls `completed_report`, which contains aggregates but no first-query observation (**3745–3776**). The message at **4785** nevertheless says “p99/first-query retained.” The existing paired reducer explicitly emits first-query evidence at **4487–4492**.  
   **Smallest repair:** emit the corresponding observation for all four runs. **Native falsifier:** give ordinal zero a distinctive latency and assert its exact reported value.

5. **The malformed-window fixture can pass without testing window validation.**  
   At **12548–12559**, the test changes the runtime configuration and its SHA but leaves the B runs’ expected config SHA and window headers unchanged. Those stale bindings trigger **4618–4623** even if window validation is removed; the broad error assertion accepts that unrelated failure.  
   **Smallest repair:** update both B runs’ expected header pins/settings consistently with each malformed runtime configuration, keep result files absent, and assert the specific validation error. This makes the fixture falsify admission rather than stale metadata.

The main A/B binding is otherwise sound within its declared trust boundary: it authenticates both runtime configurations, permits only the window difference, preserves source/binary/header equality, verifies sealed full-cohort parity, and checks both timing pairs. Leaving the legacy hardcoded binder unchanged is appropriate. The generation adapter also checks remaining modeled memory before construction; I found no new deadlock or unset-path regression in the inspected source.

**Verdict: HOLD for these repairs and the prescribed exact-source native GREEN gates.** The recorded RED result does not qualify this candidate.

Not checked: compilation, Clippy, native execution, data, network or performance; no files changed. The counterexamples above are source-derived, not executed results.
