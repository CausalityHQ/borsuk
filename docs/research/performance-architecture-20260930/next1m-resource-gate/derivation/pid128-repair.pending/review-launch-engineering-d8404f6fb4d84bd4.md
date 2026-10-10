What this change does: it assembles a single-instance Spot canary and collects mechanics evidence after teardown. **Verdict: hold the paid launch.** I found five source-level issues.

Reviewed only blobs at `8fc761b6362420cbd637afda72c034e2b4c8500b`. References below are relative to `docs/research/performance-architecture-20260930/next1m-resource-gate/derivation/`, not the stale checkout.

1. **[P1] Freeze can authorize a request for multiple instances.**  
   `runtime-support.pending/next1m-canary-freeze.py:12–22,31–44`; `runtime-support.pending/next1m-canary-launch-once.sh:24–27,38–39`.

   Root admission authenticates three scripts, but not the request template. Changing both `MinCount` and `MaxCount` to `2` passes every request assertion. The launcher verifies the newly generated hash, sends that request, then rejects the two-instance response before assigning `instance`; its cleanup trap consequently owns neither instance. Extra block devices and several security settings also escape validation.

   **Smallest repair:** authenticate the request template against root admission and enforce the exact one-instance, one-volume request after substitution, before any launch. Bind the support roster to admission as well; matching files to an independently editable roster is only internal consistency.

2. **[P1] Post-request receipt failures can bypass reconciliation and cleanup.**  
   `runtime-support.pending/next1m-canary-launch-once.sh:15–23,35–50`.

   If AWS accepts the request but writing `run-instances.exit` fails, `set -e` exits before identity extraction or reconciliation. A response parse failure or failed reconciliation command has the same problem: `instance` remains empty, so the trap does nothing, and the unresolved-outcome notification is skipped. Even with a known instance, failure to open `emergency-terminate.json` prevents the termination command from executing.

   **Smallest repair:** record that the request was attempted before issuing it; route every subsequent unsuccessful exit through bounded reconciliation of the original token. Always report unresolved ownership. Give emergency termination the watcher’s existing receipt-descriptor fallback. Never repeat `RunInstances`.

3. **[P1] Watcher startup is mistaken for cleanup being armed.**  
   `runtime-support.pending/next1m-canary-launch-once.sh:56–61`; `runtime-support.pending/next1m-canary-watch.sh:6–7,34–36`.

   A pre-existing `$D/collection` makes the watcher exit at line 7, before installing its termination trap. Starting `/bin/bash` successfully can nevertheless satisfy `Type=exec`; the launcher sets `watch_started=true` and merely records manager properties without validating them. Its own emergency cleanup is then disabled.

   **Smallest repair:** reject an occupied collection destination before launch and require a watcher acknowledgment, bound to its invocation and instance, after cleanup is armed. Retain launcher cleanup responsibility until that acknowledgment.

4. **[P2] The declared time and cost envelope is not the implemented bound.**  
   `runtime-support.pending/next1m-canary-worker.sh:32,85–92`; `runtime-support.pending/next1m-canary-watch.sh:10–34,39`; `pid128-repair.pending/canary-execution-envelope.pending.json:55–72,100–108`.

   Guest cleanup reserves 120 seconds, followed by two 30-second uploads with additional kill grace; archive creation, hashing and synchronization have no shared deadline. Thus the stated 180-second allowance is not enforced.

   Separately, a failed 120-second termination waiter leaves `terminated=false`; the EXIT trap starts another termination/wait sequence. That exceeds the modeled launch+2620 allowance. If compute remains billable through two full waits, the bid-cap model is already `$0.606667` including the ancillary allowance, before API overhead. Wait duration alone does not prove billable duration, but the stated ceiling is unsupported.

   **Smallest repair:** use absolute deadlines shared across cleanup, packaging and uploads, and across all termination attempts—including EXIT cleanup. Reconcile the accounting with those enforced bounds; keep AWS completion uncertainty explicit.

5. **[P2] Python optimization removes the launch-admission checks.**  
   `runtime-support.pending/next1m-canary-freeze.py:3–40,43–44`.

   With `python -O` or `PYTHONOPTIMIZE=1`, the source hashes, approval disposition, quote freshness, resource checks and 16 KiB bootstrap check disappear. The script still writes `launch_authority=true`; the launcher does not independently restore those checks.

   **Smallest repair:** replace admission assertions with explicit rejection checks, or unconditionally reject optimized execution before processing inputs.

The static checks did confirm:

- All nine checked text assets match their recorded SHA-256 and byte counts. Worker size is **12,403 bytes**; the generated bootstrap remains unproduced. The 16,543-byte wrapper is downloaded support, not user data.
- The parent poll requires an actual terminal state; connected phase handling preserves original manager exits before stopping units.
- Live-cgroup cleanup checks recorded ownership and refuses missing identity. The watcher requires instance termination and volume absence before writing its collection-only status.
- No automatic launch retry or performance qualification is asserted.

Kernel enforcement, fast-exit/GC behavior, interruption handling, actual ELF contents, and root replay remain **UNRUN/unverified**. The pending fresh quote and root freeze are acknowledged prerequisites, not findings. No files were edited and no runtime, mock, native, network, AWS, or child jobs were executed.