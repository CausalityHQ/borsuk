# Exact e472 wrapper engineering review

Consultation: 640e46e0275d4c85. Completed; source inspection only. Candidate remains unqualified.

**Verdict: needs minimal repair before GO-to-bounded-canary.** Three concrete findings; none requires a Rust architecture change.

Reviewed exact candidate `e4722262`, confirmed its stated SHA and single-file scope. Wrapper line references below refer to that commit’s `scripts/run_native_scale_build_gate.sh`.

1. **Evidence-write failures can produce success — blocker.**
   Lines **379–387** suppress every disk-sample failure, including failure to open or write the evidence file. Lines **662–663** similarly allow failed inventory publication to continue; lines **668–670** can then declare the chain closed and exit zero. This violates the contract’s requirement that evidence-publication failures force exit 98. Observational measurements may fail without rejecting the run, but recording that failure must succeed.

   **Smallest repair:** distinguish measurement failure from evidence-write failure; propagate the latter to exit 98. Apply the same distinction to `tools.txt` at **300–301**.
   **Smallest remote falsifier:** exercise the exact sampling function in a disposable shell canary with its destination deliberately unwritable. It currently returns success. No native workload is needed.

2. **Ordinary shell failures escape with undocumented exit codes — blocker.**
   `finish()` at **52–61** converts only an *unverified zero* to 98. Nonzero statuses from `jq`, `cmp`, `cp`, or redirections pass through unchanged. For example, schema rejection at **298–299** returns 1; `finish()` preserves 1 instead of the promised 98. Tool failures returning 2 or 3 also breach the reservation of those codes for a fully closed baseline.

   **Smallest repair:** normalize unverified, nonsignal failures to 98 while retaining the original failure separately; preserve baseline 0/2/3 only after verified closure.
   **Smallest remote falsifier:** invoke the exact wrapper with a correctly hashed, schema-invalid config and fresh evidence directory. Require exit 98, `INVALID`, and no baseline invocation. This fails before reading dataset artifacts.

3. **Derivation config is parsed before authentication — blocker.**
   Lines **332–333** feed the derivation-config path directly to `jq`. Its regular-file, exact-size and SHA checks occur later through **339/491**. A FIFO at that path blocks admission; an oversized JSON file reaches the parser despite the declared 1,145-byte pin. Resource evidence has not yet been sampled, either.

   **Smallest repair:** move this semantic validation after `authenticate_inputs`, before derivation.
   **Smallest remote falsifier:** in disposable staging, substitute a FIFO or oversized file at the configured derivation-config path. Require prompt exit 98 before parser consumption or native execution.

The inspected native interfaces match the generated configuration: semantic `scale1m`, ordinary local publication, baseline/full execution, and parallelism 16. The producer-source hashes and exact derivation-config pin match. The ETag calculation matches `object_store` 0.14.1’s quoted inode–microseconds–size formula. The baseline independently checks calibration bits against the producer receipt.

The wrapper appropriately leaves recall interpretation to root and makes no performance claim. After these repairs, the bounded canary still needs runtime evidence for float serialization, logger failures, timeout/signal handling, and complete process cleanup. V3’s documented reliance on the outer deadline also needs integration verification.

**Limits:** source inspection only under the requested bounded units. No edits, builds, runtime tests, dataset/query/truth reads, network, consultations, or paid jobs. These findings establish neither native nor performance qualification.
