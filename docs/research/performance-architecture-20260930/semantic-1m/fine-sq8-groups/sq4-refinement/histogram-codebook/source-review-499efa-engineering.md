# Histogram SQ4 engineering source review

Source: `499efa02ee9513f34740b9bf11d96fedf77e2e4c`. Read-only review; native execution remains unverified. Consultation `ffc4cc9579a244c9`, effective provider/model `codex/gpt-6-astra`, collected exit 0.

Two additional findings at **`499efa02ee9513f34740b9bf11d96fedf77e2e4c`**. Both are inherited pipeline accounting defects exercised by the new arm; neither establishes incorrect learned-codec scores.

1. **P2 — Failed transcoding leaves unaccounted partial payloads.**  
   [fine_sq8_groups.rs:4309](/home/rb/worktrees/borsuk-histogram-sq4-native-codebook/crates/borsuk/src/fine_sq8_groups.rs:4309) writes each group, but `output.bytes` increases only after the entire second pass and EOF check succeed, at line 4318. A later group-authentication failure, operation/deadline failure, or EOF growth therefore leaves a partial—or complete—payload file while INVALID reports omit its bytes at [line 2774](/home/rb/worktrees/borsuk-histogram-sq4-native-codebook/crates/borsuk/src/fine_sq8_groups.rs:2774). Initial cap admission still bounds the write; the defect is incomplete failure evidence and retained-output accounting.  
   **Minimal fix:** track bytes written separately from successfully published bytes, including partial writes, and include the failed artifact in INVALID accounting. Add a failure after at least one successful group write. The current second-pass tamper hook corrupts the first group, so it misses this case.

2. **P2 — INVALID can falsely report that truth was never opened.**  
   [fine_sq8_groups.rs:4926](/home/rb/worktrees/borsuk-histogram-sq4-native-codebook/crates/borsuk/src/fine_sq8_groups.rs:4926) sets `truth_opened` only after `descriptor()` succeeds. But [descriptor:2856](/home/rb/worktrees/borsuk-histogram-sq4-native-codebook/crates/borsuk/src/fine_sq8_groups.rs:2856) opens the file before validating its type and length. A truncated or oversized **first-panel** truth file acquires a truth FD, fails validation, and produces `truth_opened:false`. This does not open truth before freezing or read its body; it misstates the explicitly documented FD-acquisition boundary.  
   **Minimal fix:** record successful truth-file acquisition before metadata validation, or distinguish attempted/opened/body-read states. Add a truncated first-panel truth test asserting all128 results remain frozen and the acquisition is recorded.

The remaining inspected paths support the stated design:

- DP uses ascending predecessors and strict `<` for deterministic computed-f64 ties; negative interval costs are bounded before clamping.
- Actual decoded-f32 SSE is checked against uniform17 using the explicit allowance `128·ε32·uniform_SSE + 128·ε32²·max(source_energy,1)`.
- Both corpus passes authenticate groups, IDs, whole SHA and exact EOF. Books bind source, coefficients, histogram and trainer.
- Combined admission includes training workspace and both retained books. Learned scoring reads nibbles directly and preserves the specified sequential arithmetic.
- Both generations precede requests; all128 outputs and closure authentication precede truth. Quality failure does not bypass later truth validation.

No additional concrete algorithmic, authentication-bypass, or compile defect emerged from source inspection. **Compilation, tests, recall and resource performance remain unverified.** No files were edited; no native commands, corpus payloads, AWS operations or children were used.

