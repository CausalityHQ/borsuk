**Block `COLLECTOR_MECHANICS_VERIFIED` until three gaps are fixed.** The nine-case scope is appropriate, and the synthetic phase fields are clearly labelled. I reviewed frozen commit `0fe4f9b5`; the smoke’s hash matches the contract. All references below use that revision.

**Must fix**

1. **The smoke can hide collector cleanup failures** — `collector-smoke.sh:124–136`, `collect-native-chain-outer.sh:21–45`.

   The collector preserves its rejection status even when its failure cleanup records `failure-cleanup.exit=1`. The smoke accepts that rejection, then calls its own `cleanup_unit`. If this second cleanup succeeds, the case counts as verified.

   **Smallest fix:** Before fallback cleanup, require the collector’s cleanup receipt to be `0` for refusals targeting the original invocation, and save an independent stopped/drained observation. Always attempt safety cleanup, but preserve a failed assertion as INVALID. Handle `replaced` separately: refusing cleanup under the stale ID is expected.

   Otherwise, the result qualifies the smoke’s rescue path while masking a broken collector cleanup path.

2. **The live-child and deadline cases can pass without exercising their intended condition** — `collector-smoke.sh:63–64,81–88,118–128`.

   `populated` never records `MainPID=0` with `populated 1`. `deadline` skips readiness entirely. A service that fails before reaching its sleep can produce a nonzero collector rejection and satisfy the current assertions. The common 28-second timeout also does not establish compliance with the four-second deadline plus finite cleanup allowance.

   **Smallest fix:** Save and assert the actual starting conditions:

   - `populated`: original invocation, successful main exit, `MainPID=0`, and `populated 1`.
   - `deadline`: original invocation with a live main process; preserve the collector’s running poll evidence and elapsed time, and enforce the declared deadline plus cleanup allowance.

   For the plain mutation cases, wait for the real successful main exit before changing evidence. Checking only file existence allows `exit-disagreement` to race the observer’s write: the file exists as soon as its redirection opens.

   Otherwise, setup failures can become false evidence of successful refusal behavior.

3. **A failed launch response can bypass ownership cleanup** — `collector-smoke.sh:19,75–83,108–115`.

   `systemd-run` can start the unit before its client times out. With `errexit`, the first launch then exits before capturing `owned_id`, and cleanup returns success because the ID is empty. During replacement launch, both ownership variables have already been cleared.

   **Smallest fix:** Capture launch status without exiting, then perform bounded identity capture even on a nonzero response. Persist the replacement identity too, and reject the launch only after establishing cleanup ownership. The existing pattern in `run-native-chain-observer.sh:45–69` already does this.

   `BindsTo` and runtime limits provide eventual safety; they do not establish that the smoke cleaned the started invocation.

The success-path manifest and filesystem sync are useful. Failure durability still needs an explicit handoff: an interrupted case leaves its current fixtures under `/mnt/borsuk-scale1m`, outside the advertised output directory, and `finish` does not seal them. The forthcoming adapter must retain both locations, or the smoke should preserve the active case during exit handling. Acceptance must require the original process exit and verified seal, not `result.json` alone. I have not assessed the undrafted adapter as implemented.

Optional permutations are late concurrent replacement, clock changes, and additional parser or symlink cases. The present replacement test establishes refusal of an already-replaced invocation; it does not establish atomic protection against replacement between a snapshot and a destructive command. These extensions need not expand this staging run.

**Verdict: fix 1–3 before launch.** No repeat of a0004, new framework, or additional native campaign is needed.

Not checked: runtime behavior, infrastructure, transport, native execution, or data. No files changed and no children or network calls were used.
