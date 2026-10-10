# research_critic — f582314fecae4da4

Group 259d91aacc8c4b8f; immutable input 7cffa1552a62fab65f6c983524d027dd3e57132a.

What this change does: Commit `7cffa1552a62fab65f6c983524d027dd3e57132a` wraps the accepted platform mechanics around authenticated native inputs and the finite controller. Its terminal claims correctly remain subject to root replay.

**Verdict: fix 1–4 before launch.** No demonstrated scientific false acceptance was found; the blockers concern deadline admission, resource bounds, termination and failure evidence.

Locations below are relative to the supplied `native-campaign.pending/` prefix and refer to the immutable commit.

**Must fix — unsafe admission or incomplete cleanup**

1. **The gate reserves against the wrong deadline.**  
   Locations: `native-launch-draft/native-worker.sh:158–172`, `native-watch.sh:38–39`, `assemble-worker.py:44`.

   The worker permits a controller start at launch+900: `900+16800+300=18000`. The watcher can terminate at launch+17900, leaving only **200 seconds**, before controller startup and closure overhead. Moreover, the worker’s 90-second budget covers manager calls; archive creation, hashing, sync and uploads do not share an absolute publication deadline.

   **Smallest repair:** derive the admission gate and finalization limits from launch+17900. Preserve the 300-second reserve against that deadline, account for startup/closure, and clamp finalization to the remaining time. Update the assembler too. Otherwise an admitted run can lose its terminal evidence to the watcher’s correctly enforced cutoff.

2. **Support downloads have identity checks but no byte bound.**  
   Location: `native-launch-draft/native-worker.sh:149–152`; installer download at `112–115`.

   The 19 support objects are downloaded completely before their lengths and hashes are checked. An oversized replacement under a SHA-named key can consume disk before rejection; the 30-second timeout bounds time, not bytes. The installer download has the same ordering.

   **Smallest repair:** apply the existing bounded native S3 transport pattern to support fetches: expected-length-plus-one range, exact response/body validation, then SHA. Add a download-size limit for the installer. Content addressing establishes identity after retrieval; it does not enforce retrieval size.

3. **Publication failure bypasses immediate shutdown.**  
   Location: `native-launch-draft/native-worker.sh:87–100`.

   Archive failure, an oversized archive, or either failed upload executes `exit 94` before `shutdown -h now`. If no terminal object exists, the watcher normally continues polling until launch+17900. A failed campaign can therefore retain idle compute for most of the envelope.

   **Smallest repair:** install unconditional shutdown as a nested exit cleanup before attempting evidence publication. Keep publication errors and their original exit codes separate from termination. The independent watcher remains the backup.

**Must fix — failures that safely refuse but lose evidence**

4. **Early setup failures cannot pass the archive path.**  
   Locations: `native-launch-draft/native-worker.sh:92–95`, `106–128`.

   The archive always requires `/mnt/borsuk-pid-evidence`, but that directory is created only after setup. Any earlier setup failure reaches an archive command guaranteed to fail on the missing member. Failures before installer removal can also include the 73 MB installer ZIP and unpacked installer in a bundle capped at 16 MiB.

   **Smallest repair:** initialize the evidence directory before fallible setup, adjust its later creation accordingly, and exclude installer payloads from the evidence archive. Preserve a compact failure bundle when uploading is available; always terminate when it is unavailable. These are execution-invalid attempts, not algorithm failures.

The checks that hold up statically:

- All **19 support roster lengths and SHA-256 values match** the committed bodies.
- The seven native assets undergo exact length/SHA checks before controller execution. The S3 range and If-Match path is the appropriate existing pattern.
- Parent exit validation checks the original invocation and execution fields. Cleanup enumerates the four observer labels and eight phase names.
- Successful watcher collection requires confirmed instance termination and root-volume absence before downloading terminal/archive bodies.
- `COLLECTED_NATIVE_GATE_EVIDENCE_ROOT_REPLAY_REQUIRED` does not claim that an INVALID terminal passed.

The remaining evidence gap is runtime behavior: ShellCheck and Bash parsing do not establish range-response handling, deadline-bound finalization, or publication-failure cleanup. The accepted a0002 proof establishes its observed platform mechanics only. After repairs, those new wrapper paths still require bounded EC2 validation before their results can support acceptance.

**Not checked:** runtime, fixtures, builds or live AWS behavior. This review used committed source and read-only hash/length checks; no files, jobs or historical dispositions were changed.

