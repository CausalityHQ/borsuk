**Source verdict: fix the findings below. Candidate `640f71a7ef2b757df5b41df73b733e46c3cf5358` remains UNVERIFIED.**

What this change does: it adapts the accepted r7 supervision mechanics to the existing five-phase 1M chain. The native invocation/configuration tail is byte-identical to base `793a0699…`, and the template and transport pins consistently select baseline ELF `59fe47aa…`.

All references below are candidate Git-blob lines under:

`docs/research/performance-architecture-20260930/next1m-resource-gate/derivation/runtime-support.pending/`

**Must-fix source findings**

1. **[P1] Every complete chain will fail replay because `timeout.seconds` is missing.**  
   References: `run_native_scale_build_gate.sh:L658–671`; `verify-closed.py:L810–811`.  
   The new runner reads the configured timeout but never writes this receipt. Replay still requires it for every phase; the base runner wrote it explicitly. This affects closed exits 0, 2 and 3 alike.  
   **Minimal repair:** write the actual timeout to `$phase_dir/timeout.seconds` before release and include it in closure sealing.  
   **Falsifier:** a complete source-bound closed fixture must replay; deleting or changing one timeout receipt must refuse.

2. **[P1] The escaped-child canary cannot reach its required witness capture.**  
   References: `wrapper-canary.sh:L155–166`; `run_native_scale_build_gate.sh:L591–620`, `L707–727`.  
   The detached child inherits logging descriptors and sleeps for 1,000 seconds. Those open pipe writers prevent the payload pipeline/log collectors from finishing and creating `done`. Consequently, `observe()` reaches its deadline before the subsequent `child.witness.json` capture. Cleanup can kill the child, but the canary then fails its witness assertion.  
   **Minimal repair:** make this fixture close all inherited logging descriptors before its long sleep, including inherited numbered descriptors. Alternatively, capture the witness while timeout completion is observable, independently of `done`.  
   **Falsifier:** require a live, same-start-time, separate-session descendant after timeout, followed by authenticated cleanup with `cleanup.exit=0`; missing native exit evidence must remain missing and INVALID.

3. **[P2] Replay does not validate the original launch identity receipt.**  
   References: `run-native-chain-observer.sh:L58–68`; `verify-closed.py:L37`, `L654–694`.  
   Replay requires `chain-launch.identity` to exist but never parses it. A sealed launch receipt naming another InvocationID can coexist with matching collector/wrapper receipts without triggering this fence.  
   **Minimal repair:** compare its `Id`, `Description`, `InvocationID` and `ControlGroup` against the observer identity already validated by replay.  
   **Falsifier:** alter only the launch InvocationID in an otherwise complete fixture, recompute evidence seals while retaining source/configuration pins, and require refusal.

4. **[P2] Replay accepts phase intervals beyond the declared runtime envelope.**  
   References: `verify-closed.py:L698–722`; `run_native_scale_build_gate.sh:L669–681`.  
   Replay checks only that drain time follows release time. It neither bounds that interval nor validates the recorded manager’s runtime ceiling. An arbitrarily late drain therefore satisfies this predicate despite the runner declaring `secs+80`.  
   **Minimal repair:** validate the manager runtime limit and elapsed-time envelope against the frozen phase configuration. Keep failure-cleanup allowance separate from normal closure.  
   **Falsifier:** independently change the runtime ceiling and move drain beyond the permitted interval; each mutation must refuse.

5. **[P2] The canary’s source manifest is stale and incomplete.**  
   References: `wrapper-canary.sh:L23–25`, `L53–57`, `L90`.  
   `source.sha256` hashes `f32.jq` before assertions are appended, so that entry no longer describes the executed file. It also omits the executed `observer-init.sh` excerpt. The final archive checksum does not correct this source manifest.  
   **Minimal repair:** keep the extracted f32 source immutable and put assertions in a separate file, or seal after composition; include `observer-init.sh`.  
   **Falsifier:** check the source manifest after assembly and require mutation of either executed file to fail verification.

**Fail-closed refusals**

The outer collector correctly distinguishes launch success from actual command exit, checks original manager termination, and permits fully closed baseline exits 2/3 without treating them as scientific success. Identity mismatch, missing receipts, supervision failure and failed drain remain refusals.

The unfrozen bootstrap/watcher placeholders intentionally refuse execution. Reusing historical canary assembly pins would also refuse the changed source; that is a freeze/wiring constraint, not evidence against the algorithm.

**Research judgment and qualification limits**

Adapting the existing 1M recipe is the smaller coherent alternative to porting the entire 100k campaign. Accepted r7 mechanics provide useful prior art, but acceptance does not transfer automatically to these new fixtures, collectors and envelopes.

Full-chain collector negatives, the disposable kernel canary, cloud-final exit mapping and real1M input admission remain pending qualifications—not additional implementation findings merely because they have not run. Hash authentication also warms cache, and CPU0 observation shares query execution resources; a later performance protocol must account for both.

The historical `actual1m-chain-a0001` PID-event INVALID disposition must remain unchanged. No edits, repository execution, builds, fixtures, data/GT reads, network/AWS calls or additional research jobs were performed. No performance or release claim is established.
