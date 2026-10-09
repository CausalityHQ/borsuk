## Verdict: do not freeze. The canary is honest in what it accounts for, but too small to certify the ≤2% / ≤1 ms gate.

I measured the noise directly, using the closed admission-a0001 raw evidence in `/data/target/borsuk-cold-membership-native/sq8-attribution-admission-a0001/collected`. It ran the same binary, ordinals and configs. Each block was one untraced call followed by one traced call on the same 64 ordinals.

| block | median per-query CPU, traced vs untraced | median wall difference | system time per call, untraced → traced |
|---|---|---|---|
| A_first | **−8.0%** | −4.7 ms | 0.76 → 0.53 s |
| B_first | **+7.2%** | +1.8 ms | 0.66 → 0.94 s |
| B_last | **−4.3%** | −1.0 ms | 1.21 → 0.92 s |
| A_last | **−3.7%** | −4.3 ms | 0.68 → 0.64 s |

Within a single call, per-query relative CPU varies with an SD of 5–9%. But the shift between calls moves all 64 queries together. So the real unit of replication is the process call, not the query.

## Concrete defects

1. **The test can't detect a 2% / 1 ms overhead, and the pass rule doesn't bound the error (Critical).**
   - One call's CPU varies by about 4.6%. With 8 calls per state, the standard error of the aggregate CPU difference is about 3%, which is larger than the 2% threshold. The latency difference has a standard error of about 1.5 ms, larger than 1 ms.
   - The CPU repeatability check compares 4 calls against 4, so its difference has an SD of about 4%. Each trace state passes about 35–40% of the time; both pass together about 15% of the time. Add the latency repeatability check and the canary is very likely INCONCLUSIVE before it runs.
   - The "256 pairs" are not 256 independent samples: the 64 queries in a call share that call's shift. A pass would not be certification either. Acceptance uses a point estimate, and the repeatability check only tests consistency; it does not bound the estimate's error.
   - The untraced runs sit at the outer positions (1 and 4) of each block. If the first exposure carries a one-off step `b` (A blocks: about 4.5 ms in admission), the estimate shifts by −b/2. That means a true overhead of about 1.5 ms, or about 3% CPU, can pass both checks.

2. **Pooling baseline and direct hides the arm that matters (High).** The latency median is taken over a 50/50 mix of baseline and direct. The direct arm records far more ranges and timestamps. If baseline adds ~0 ms and direct adds ~2 ms, the pooled median can still come out ≤1 ms. The CPU ratio is pooled the same way. Each gate should apply to each arm separately.

3. **The latency repeatability rule is ambiguous (Medium).** "Absolute median paired repeat difference" can be read as |median(d)| or median(|d|). The second is about 10 ms from per-query noise alone, so it always fails. The rule must say |median(d)|.

4. **Whole-process rounding interval is wrong (Low–Medium).**
   - GNU `time` truncates user and system time to 10 ms, so a reported value x means a true value in [x, x+0.01), not x ± 0.005.
   - Quantisation uses about 0.5% of the 2% budget: 8 calls × 2 fields × 10 ms ≈ 0.16 s out of about 32 s.
   - The runner already emits `process_cpu_ns` at nanosecond resolution, with almost the same scope (query CPU 3.63 s vs process CPU 3.69 s, so there is no dilution). Make that the primary whole-process figure and use `time -v` as a cross-check with the truncation interval.

5. **The negative check proves less than it claims (Medium).**
   - Exit code 2 covers the usage error, a failed output create, any config parse failure, and any INVALID result. The stage only checks the exit code.
   - On a wrong SHA, the runner at 516ee0fd writes an identity line plus a terminal summary of `INVALID` with `stage:"config"`, `completed_queries:0`, `truth_opened:false` and zero charges.
   - The preregistered check should require exactly that, plus no `bound_inputs`, `generation_open` or `startup` phase, and the SHA-mismatch error text. Otherwise "INVALID before query/payload/truth open" is not shown.

6. **Smaller gaps:**
   - The stage dropped the admission's per-call `started`/`finished` UTC stamps. Untraced calls then have no absolute time at all, so drift can't be checked against the backend. Restoring them costs two lines.
   - No user-data or launcher was in the pending material. The canary scratch directories (`sq8-attribution-canary-a0001/*/query-scratch`) are never created in `stage.pending.sh`. Admission created them in user-data, so this needs a check.

**Checked and found fine:**
- **Throttling:** `cpu.stat` is read only at the end, but one CPU at 100% quota cannot be throttled, and admission showed `nr_throttled 0`.
- **Deadlines:** calls took 6.6–7.7 s against a 60 s cap, and 16 calls fit easily within 1020 s / 1800 s.
- **Resources:** memory peaked at 90.9 MB against 512 MiB.
- **Measurement scope:** host counter reads and trace output are outside the timed query window, and whole-process CPU still includes them. The per-query CPU clock includes all threads.

## Is block order or backend drift what makes it inconclusive?

Not mainly. The ABBA order cancels linear drift within each ~30 s block. What it can't do is resolve a 2% / 1 ms effect when each process call carries a coherent shift of about ±5%. That shift could come from per-process memory layout, from network softirq time charged as system time on CPU0, or from the backend; this data doesn't tell them apart. There is prior art: Mytkowicz et al. 2009 showed per-process layout bias of a few percent, and Stabilizer (Curtsinger & Berger 2013) treats the randomised process as the unit and replicates it.

## Smallest correction, with no native change and no weaker thresholds

- **Replicate calls.** Preregister 32 ABBA quads per arm: 64 untraced and 64 traced calls per arm, about 256 calls. One call takes about 7.5 s, so that is roughly 35 minutes of native time and still well under a dollar on Spot. Raise the unit/machine caps to match. Alternating ABBA and BAAB across quads would also spread any first-exposure step over both trace states.
- **Use each quad as one observation.** Compute the CPU ratio contrast and the median-wall contrast per quad.
- **Decide on a confidence bound, separately for baseline and direct.**
  - **PASS** only if the 95% upper bound is ≤ 2% and ≤ 1 ms.
  - **FAIL** if the lower bound exceeds a threshold.
  - **INCONCLUSIVE** otherwise, with no extension.
- **Apply fixes 3–6** as written above.

The better long-term design is to alternate trace on/off per query inside one process, which cancels the per-call shift. That needs a Rust runner change and requalification, so it is not the smallest fix. Leaving the thresholds as declared but running only 16 calls is, statistically, a predetermined INCONCLUSIVE.

I edited no repository files. I saved the noise numbers as a session memory note so future reviews don't re-derive them.
