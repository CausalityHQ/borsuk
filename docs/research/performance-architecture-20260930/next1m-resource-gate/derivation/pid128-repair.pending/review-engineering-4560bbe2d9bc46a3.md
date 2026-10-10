What this change does: separates the observer from five bounded payload services while preserving plain baseline exits 0/2/3. The five-phase invocation/configuration tail matches the base byte for byte.

**Verdict: fix findings 1–4 before qualification. This checkpoint remains UNVERIFIED.**

Reviewed candidate `640f71a7ef2b757df5b41df73b733e46c3cf5358` against base `793a0699c097a9f48ce1416d35bd639b78feff0a` and the accepted r7 scripts. All references below are candidate Git-file lines under:

`docs/research/performance-architecture-20260930/next1m-resource-gate/derivation/runtime-support.pending/`

**Must fix — source findings**

1. **The new phase runner omits a receipt required by replay.**  
   `run_native_scale_build_gate.sh:658–683`; `verify-closed.py:810–811`.

   Replay unconditionally reads `phases/<phase>/timeout.seconds`. The replacement `run_phase` never writes it. The base runner wrote this receipt at line 448. Consequently, even an otherwise correctly closed 0/2/3 chain fails replay with missing evidence.

   **Minimal repair:** write the exact admitted `secs` to that file before launch and include it in closure sealing.  
   **Minimal falsifier:** require the receipt in each authorized remote phase fixture; removing or changing it must make replay refuse.

2. **The escaped-descendant canary cannot reach its required witness.**  
   `wrapper-canary.sh:155–166`; `run_native_scale_build_gate.sh:597–620,630–632,707–727`.

   The `setsid` child inherits the payload’s stdout/stderr pipes and log descriptors, then sleeps for 1,000 seconds while ignoring TERM. After the ordinary timeout, it still holds those pipes open. The payload therefore cannot finish its pipeline/log waits or write `done`. `observe` reaches its deadline before execution reaches the witness block. The subsequent assertion requiring `child.witness.json` fails.

   **Minimal repair:** make this witness fixture close inherited log descriptors and redirect standard streams before its long sleep. Test pipe-holding descendants separately as an observer-timeout cleanup case.  
   **Minimal falsifier:** require the same PID/start-time/session witness after the native timeout, followed by authenticated drain and `cleanup.exit=0`. Exit 98 alone is insufficient.

3. **Independent replay accepts missing PID-event evidence.**  
   `verify-closed.py:329–335,617–649`, particularly line 643.

   `counters('absent')` returns `{}`. Two non-root snapshots with `pids_events="absent"` therefore pass the equality check. Missing memory-event maps similarly compare equal through `.get(...)`. This permits replay without the counters needed to distinguish staying within PID128 from attempting to exceed it; `pids.peak <= 128` does not establish that distinction.

   **Minimal repair:** require the mandatory non-root event keys before comparing counters; permit absent counters only where the frozen root-cgroup schema allows them.  
   **Minimal falsifier:** in coherently re-sealed metadata, replace both phase PID-event records with `"absent"` or empty maps. Replay must refuse despite unchanged peaks and validation markers.

4. **Replay does not validate the original launch identity.**  
   `verify-closed.py:37,654–694`; producer: `run-native-chain-observer.sh:58–68`.

   `chain-launch.identity` is mandatory in the archive roster but never parsed. Replay takes the identity from `outer-closure.json` and checks subsequent records against it. A contradictory InvocationID in the original launch receipt is accepted by these checks.

   **Minimal repair:** compare launch `Id`, `Description`, `InvocationID`, and `ControlGroup` with the observer unit, outer closure, and wrapper identity.  
   **Minimal falsifier:** change only the launch InvocationID in coherently re-sealed metadata. Replay must reject that contradiction.

**Should fix — source sealing**

`wrapper-canary.sh:25,53–56` hashes `f32.jq` and subsequently appends assertions to that same file, leaving `source.sha256` stale. This behavior is inherited from the base. The newly extracted `observer-init.sh` is also omitted from that source manifest. Keep the excerpt immutable, place assertions separately, and seal the complete excerpt roster.

**Fail-closed refusals, separate from these defects**

The collector checks original manager termination, actual command exit, wrapper intent, InvocationID, and drain evidence. Plain baseline 2/3 have explicit closed-nonzero handling. Missing evidence, identity mismatches, resource-event failures, and exhausted supervision budgets remain execution INVALID; they are not algorithm failures.

**Qualification limitations**

Full-chain collector negatives, disposable kernel canary, real 1M input admission, and target cloud-final exit mapping remain pending. Their absence is not itself an additional implementation finding. Historical `oldactual1m-chain-a0001` remains INVALID and untouched.

No files were edited. No reviewed scripts, fixtures, native/Cargo workloads, data/GT, network/AWS operations, or experiments were executed. All source reads used the requested bounded envelope.
