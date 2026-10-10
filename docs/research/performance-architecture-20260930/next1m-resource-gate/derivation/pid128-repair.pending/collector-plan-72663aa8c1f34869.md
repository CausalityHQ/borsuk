**Run one short collector smoke on causality EC2 Spot, then one native 1M/Q32 gate.** Actual systemd exits 0/2/3 with explicitly synthetic metadata are appropriate. Do not make the expensive gate the collector’s first successful execution.

All source references below are from `1fef024f`, read through `git show`.

1. Use the unchanged `collect-native-chain-outer.sh` and `observer-command.sh`, with a tiny synthetic recipe. Launch real `Type=exec`, `RemainAfterExit=yes` units using the launcher’s resource properties (`run-native-chain-observer.sh:46–49`), with a short staging runtime. Generate metadata at the collector’s required canonical paths (`collector:12–15`). Record real invocation IDs and cgroup observations; label all invented phase/native fields synthetic. Pass the synthetic recipe’s hash as `recipe_sha`, and separately record the frozen collector/observer hashes.

2. Run three positive cases: recipe exits **0, 2, 3**. Each must produce collector exit **0**, preserve the corresponding `actual_outer_exit`, bind manager/terminal/wrapper exits, and prove drain and stop. Collector success is distinct from the launcher’s propagated 0/2/3 exit (`collector:75–118,134–161`; `launcher:73–79`).

3. Run these six independent refusals:

   | Case | Required result |
   |---|---|
   | Manager exit 0, actual-exit file says 2 | Reject exit disagreement (`101–102`). |
   | Resealed terminal with wrong config hash | Reject admission (`108–114`). |
   | Valid truncated manifest, omitted file still present | Reject incomplete sealed roster (`119–131`). |
   | Restart the same unit name; supply its previous invocation ID | Reject; do not stop the replacement (`26–40,78–80`). |
   | MainPID=0 with a live descendant in the retained successful unit | Reject populated drain; clean the original invocation (`55–74,134`). |
   | Unit remains running beyond a short collector deadline | Reject within the deadline plus finite cleanup (`49–53,95,21–45`). |

   Preserve each case before resetting only its owned fixture paths. Assert **nonzero rejection**, not universally exit 98: `jq`, `cmp`, and checksum failures can preserve another nonzero status.

4. Bound this single staging run to **CPU1 / 256MiB / PID128 / no swap**, sequential cases, **≤300 seconds including cleanup**. Use short per-case deadlines with room for the collector’s approximately 12-second failure cleanup. Retain source hashes, original IDs, exits, and cleanup observations; terminate Spot compute immediately after terminal collection. No ANN, dataset downloads, full bootstrap, or new controller framework.

The genuine pre-gate blocker is failure of one of these paths: accepting valid closure, refusing inconsistent evidence, fencing replacement invocations, or draining within bounds. Fix demonstrated environment defects and repeat the failed staging case; classify these failures as execution INVALID.

Leave actual phase receipts, frozen input admission, full-chain replay, bootstrap exit, and Q32 quality to the **one real gate**. The collector checks the complete roster of files present; it does not establish that every scientifically required phase artifact exists. `verify-closed.py:785–865` supplies those additional checks. Thus deleting a file from both disk and manifest cannot be treated as collector qualification of native completeness.

Additional mirrored parser/symlink permutations are optional. Canary a0004 remains phase-mechanics evidence; neither it nor this synthetic smoke establishes full-chain scientific qualification. No files changed or runtime executed.
