What this change does: adds nine real systemd collector checks using explicitly synthetic chain evidence. At frozen commit `0fe4f9b5e0b685d8bc5557558e7777de4791135f`, I found two gaps that can produce false qualification and two smaller failure-path defects.

All line references below are to that commit.

**Must fix**

1. **The harness can hide failed collector cleanup.**  
   `collector-smoke.sh:128,136` accepts a refusal, then independently stops the unit. For example, the populated-child refusal returns 98, but the collector’s cleanup identity query times out, leaving `failure-cleanup.exit=1` and the child alive. If the harness’s next query succeeds, `cleanup_unit` stops the child and the case passes. The final result therefore claims verified mechanics despite failed collector cleanup.

   **Smallest fix:** Before harness cleanup, require successful collector cleanup and independently record the original unit’s drained/stopped state for same-invocation refusals. Keep harness cleanup as emergency cleanup, with failure preserved. Treat `replaced` separately: its replacement must remain alive until the harness stops it.

2. **The refusal oracle does not establish which condition caused rejection.**  
   `collector-smoke.sh:128` accepts command failures and unrelated collector failures, provided they are neither 124 nor 137. The deadline fixture also skips readiness at line 85 and records neither its supplied deadline nor elapsed time. A fixture that exits unexpectedly can be counted as a deadline refusal; a collector returning after 25 seconds can satisfy the nominal four-second case.

   **Smallest fix:** Tighten the existing six cases using their existing evidence. Require the populated case to reach drain checking with `MainPID=0` and `populated 1`; require the deadline fixture to be running, retain its deadline and timing, and enforce the declared deadline plus finite cleanup allowance. Validate each mutation’s prerequisites and the corresponding collector progress evidence. No additional test framework or campaign is needed.

**Should fix before staging**

3. **An ambiguous launch failure skips ownership capture.**  
   At `collector-smoke.sh:75–83`, a nonzero `systemd-run` response exits before capturing `owned_id`; cleanup then returns success immediately at line 19. The replacement launch has the same gap at lines 109–114. A submitted start followed by a lost response can therefore leave a running service without a durable invocation receipt or an attempted authenticated stop.

   `BindsTo` provides a fallback when the parent becomes inactive, but does not supply that missing evidence.

   **Smallest fix:** Preserve the launch status, always attempt bounded identity capture, persist it, then handle the original launch failure. The adjacent production launcher already follows this pattern. Do not retry the launch.

4. **The exit-disagreement mutation races the observer’s write.**  
   The readiness check at `collector-smoke.sh:85` observes file existence. `observer-command.sh` creates that file through redirection before `printf` writes the exit code. Line 98 can write `2` during that interval, after which the observer writes `0`. A correct collector then closes successfully and the smoke reports failure.

   **Smallest fix:** Before mutating completed fixtures, wait for the original invocation’s terminal manager state and validate its completed exit file. Preserve the separate running-state check for the deadline case.

The successful bundle has a checksum manifest and filesystem sync. However, harness cleanup observations are not retained, and a failed case’s collector evidence remains under `/mnt/borsuk-scale1m` because relocation happens only after assertions succeed. Preserve those observations and failed-case artifacts when finishing.

The 240-second value is an **admission limit**, as the contract names it; the script does not enforce a hard 240-second batch deadline. The external 300-second enforcement remains an adapter obligation, which I have not assessed as implemented.

Optional permutations—additional exit codes, parser/symlink variants, or another replacement timing—are not blockers. Nothing here warrants repeating a0004.

**Verdict: fix 1–4 before using this smoke as the staging gate.** Read-only source review only; no edits, children, runtime, native/data access, or network activity.
