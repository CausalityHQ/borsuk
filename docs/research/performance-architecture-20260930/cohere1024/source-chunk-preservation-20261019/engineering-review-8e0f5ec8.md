What this change does: preserves normalized SOURCE chunks and distinct crossing-row copies, then authenticates each complete physical response before scoring. The planner retains borrowed access for later completion units.

**Verdict: suitable to advance to bounded qualification after the parent’s assurance and admission gates. I found no demonstrated production correctness or security blocker.** One focused test gap remains.

1. **P2 — Fragmented late-completion row identity is insufficiently tested.**  
   Evidence: `crates/borsuk/src/two_bit_generation.rs:10011–10012` creates identical records; the planner comparison at `10069–10081` calls `range.record_at` directly, bypassing the production range lookup at `1566–1589`. The varied-record replay self-check scores its supplied roster but does not replay planning.

   **Counterexample:** a lookup returning a neighboring crossing row during late completion could pass the identical-record fixture while changing nominations on varied data. This is a coverage weakness, not a reproduced defect.

   **Smallest falsifier:** extend the existing test with varied records, signed-zero cases, multiple ranges with nonzero starts, and crossing rows in completion units. Invoke `verified_source_segment_record`, compare score bits, plans and evaluation order, and assert that completion units were absent from early `SourceScores`.

   **Classification:** nonblocker for initial bounded qualification; should fix before claiming comprehensive generic parity coverage.

The systems and skeptical-operator hypotheses exposed an important boundary: **`2B` bounds normalized library payload/scratch, not all simultaneously live backing allocations.** At `sq8_s3_range.rs:1040–1048`, an oversized incoming allocation survives conversion until normalization finishes. The existing seven-byte/1-MiB test at `5393–5398` demonstrates this distinction. The updated contract explicitly excludes that pre-existing transport owner; I therefore do **not** classify it as a newly demonstrated regression. Qualification must retain separate transport/RSS headroom rather than use `2B` as a process-memory ceiling.

The security and retrieval checks supported the implementation:

- Exact length and ordered unit digests cover clipped tails; callbacks follow complete authentication.
- Crossing rows have distinct retained storage. Early scoring remains restricted to walked units; later completion uses the borrowed fallback.
- Ordered draining restores request order before selecting the first error.
- SOURCE byte leases cover worker authentication/scoring. The outer query permit remains held through canonical FP32 completion.
- Runtime, runner and strict reader consume the shared additional-memory calculation. Allocation, authentication and scoring scopes inspected do not overlap.

These perspectives were review hypotheses, not external expert testimony.

I reconstructed the frozen files from base `1f04de55…` and the supplied diff on the authorized EC2 instance. All three frozen hashes matched. Subsequent source differences were the declared test-only additions; verification receipt: `e8e45180-881a-4bdf-bed2-7bc97682c5b4`.

F256 validation finished during review. Existing job `b5c3bab0-136b-4464-92c5-21bf6e3e5de3`, its terminal marker and matching source-diff hash establish **15 passed, 1 intentionally ignored**, including cancellation, admission and actual producer-to-strict-reader checks. This is correctness evidence, not performance evidence. The offline roster replay correctly disclaims original planner replay and cold latency.

Not performed: edits, builds, tests, fixture reads or new experiments. Full workspace Clippy/test-build gates and performance qualification remain unverified here.
