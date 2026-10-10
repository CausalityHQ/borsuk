What this change does: it repairs the admission/staging lifecycle and adds a disposable platform-only worker and watcher. **I would not launch candidate `e85fed7c29d40b124ef766dc489fd90d1d5276f6` unchanged.** Seven concrete issues remain. References below use that candidate’s lines.

1. **[P1, canary] Outer sync failures still masquerade as the expected negative.**  
   [observer-command.sh:11](/home/rb/worktrees/borsuk-prod-ready-v9/docs/research/performance-architecture-20260930/cohere1024/baseline-pool/pid128/native-campaign.pending/review-repair-r4/observer-command.sh:11) writes the recipe’s return, then returns **98** if either sync fails. For the controlled negative, the actual-exit file, recipe closure and manager exit can consequently all contain the expected 98 despite failed outer durability.  
   **Smallest fix:** return reserved **97** for enclosing write/sync failures while preserving the recipe’s original return separately.

2. **[P1, canary] Watcher termination depends on writable receipt storage.**  
   In [platform-watch.sh:18](/home/rb/worktrees/borsuk-prod-ready-v9/docs/research/performance-architecture-20260930/cohere1024/baseline-pool/pid128/native-campaign.pending/review-repair-r4/platform-watch.sh:18), failed output-file creation prevents `terminate-instances` from executing. The EXIT trap calls the same function with the same dependency. Also, directory creation and the age check precede trap installation; either can exit without attempting termination. Bash performs these redirections before command execution. [GNU Bash documentation](https://www.gnu.org/software/bash/manual/html_node/Simple-Command-Expansion.html)  
   **Smallest fix:** install cleanup after authenticating the instance arguments, before fallible initialization; ensure failed receipt writes cannot suppress the termination request. Record the evidence failure separately.

3. **[P1, canary] Bootstrap can report successful cleanup after failed stops.**  
   [platform-worker.sh:30](/home/rb/worktrees/borsuk-prod-ready-v9/docs/research/performance-architecture-20260930/cohere1024/baseline-pool/pid128/native-campaign.pending/review-repair-r4/platform-worker.sh:30), and similarly lines 48 and 69, ignores stop-command returns under `set +e`. If the completed parent’s cgroup has already disappeared, a failed stop still leaves `cleanup=0`; the worker can publish its successful terminal state.  
   **Smallest fix:** propagate every required stop failure into cleanup status while continuing teardown. Preserve command success and observed drain as separate evidence.

4. **[P1, native campaign] Both staging entrypoints reject the candidate recipe.**  
   [run-staging-native.sh:31](/home/rb/worktrees/borsuk-prod-ready-v9/docs/research/performance-architecture-20260930/cohere1024/baseline-pool/pid128/native-campaign.pending/review-repair-r4/run-staging-native.sh:31) and [run-cleanup-negative.sh:31](/home/rb/worktrees/borsuk-prod-ready-v9/docs/research/performance-architecture-20260930/cohere1024/baseline-pool/pid128/native-campaign.pending/review-repair-r4/run-cleanup-negative.sh:31) retain recipe pin `dd4033d1…`. The candidate recipe hashes to `40edcc41…`, which the campaign correctly requires. Thus staging exits before executing its gate.  
   **Smallest fix:** update both entrypoint pins and their dependent frozen descriptors.

5. **[P1, native campaign] Negative-proof collection destroys the admission-closure value.**  
   [run-finite-campaign.sh:108](/home/rb/worktrees/borsuk-prod-ready-v9/docs/research/performance-architecture-20260930/cohere1024/baseline-pool/pid128/native-campaign.pending/review-repair-r4/run-finite-campaign.sh:108) overwrites `closure`, previously admission-closure JSON, with `/mnt/…/negative-outer/outer-closure.json`. Lines 121 and 124 subsequently pass that pathname to `--argjson closure`. Widths freezing fails even after successful staging and negative gates.  
   **Smallest fix:** use a separate variable for the negative closure pathname.

6. **[P2, canary envelope] The watcher’s 1080-second deadline does not bound termination.**  
   [platform-watch.sh:44](/home/rb/worktrees/borsuk-prod-ready-v9/docs/research/performance-architecture-20260930/cohere1024/baseline-pool/pid128/native-campaign.pending/review-repair-r4/platform-watch.sh:44) checks time only before a HEAD request and five-second sleep. An iteration beginning just before the deadline can defer the first termination request beyond it; termination and its 120-second wait then follow.  
   **Smallest fix:** reserve cleanup time and cap polling against remaining time. Freeze separate deadlines for requesting termination and confirming it; the current source does not establish “machine stop1080s.”

7. **[P2, canary envelope] Volume absence is enforced after collection.**  
   [platform-watch.sh:63](/home/rb/worktrees/borsuk-prod-ready-v9/docs/research/performance-architecture-20260930/cohere1024/baseline-pool/pid128/native-campaign.pending/review-repair-r4/platform-watch.sh:63) records `absent=false` but still downloads the terminal/archive and writes `root.status`; rejection occurs only at line 80. This violates the declared absence-before-collection ordering. It does eventually return nonzero, so this is **not** a zero-exit success bug.  
   **Smallest fix:** require absence before collection and explicitly report unproven volume cleanup.

The repaired typed drain records bind the original invocation/path without inventing populated-zero evidence. Complete roster checks, independent negative resource/witness checks, and widths’ private baseline/rechecks are present. The six platform support hashes match their roster and bootstrap pins. The FD-source arrangement is coherent under the stated protected-parent/no-in-place-write prerequisites. The environment-expansion change and observer dependency match upstream semantics. [systemd-run documentation](https://raw.githubusercontent.com/systemd/systemd/v255/man/systemd-run.xml), [systemd dependency documentation](https://raw.githubusercontent.com/systemd/systemd/v255/man/systemd.unit.xml)

The standalone positive-plus-timeout pair remains the smallest useful real-systemd falsifier after fixing **1–3 and 6–7** and freezing root-owned launch details. Native-only findings **4–5** need not expand that canary’s scope. Necessary target falsifications remain separate:

- Actual FD inheritance, literal argv preservation, counters, original exits, delayed initialization and empty-versus-removed cgroups.
- Observer death while its sibling payload runs; original payload drain within the bound.
- Receipt/sync/stop failures, including the expected-98 path; root receipt-storage failure and deadline expiry.
- Before native acceptance: namespace rejection, failed filesystem checks and admission mutation through the repaired rechecks.

The current pair alone does not cover those additional injections. The **$0.05 remains a planning allowance**, and this review establishes no scientific, performance or production readiness.

**Tested nothing.** Inspected immutable Git source, diffs, source hashes and primary documentation only. No edits, children, recipes, validators, mocks, ANN/data, Cargo or AWS execution.
