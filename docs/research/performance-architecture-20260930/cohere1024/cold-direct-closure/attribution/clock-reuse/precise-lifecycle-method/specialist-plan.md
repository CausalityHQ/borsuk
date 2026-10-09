Recommend **an external `wait4` supervisor around the existing homogeneous trace-on/off runner calls**, with disposable cgroup accounting to verify completeness. Keep query CPU, whole-process user+system CPU, and median added query wall as three separate gates for each serving arm.

This is a prospective method plan only. Canary a0001 remains **INCONCLUSIVE**; b82e524c’s live qualification and watcher43463 remain untouched.

1. **Preserve the accounting boundary.**

   At `8b009397`, the runner uses `rustix::time::ClockId::ProcessCPUTime` for query CPU. Host sampling lies outside that query window. Its terminal `process_cpu_ns` starts after output creation and ends before terminal serialization, final sync, stdout, object destruction and process exit. It therefore cannot replace whole-process accounting.

   Capture the existing `timeout --kill-after=5 30 runner …` invocation externally, from process creation through termination and reaping. Include all runner threads, user and system CPU, procfs sampling, diagnostic allocation and serialization, result sealing, truth authentication/reduction, syncs, stdout and exit cleanup. Retain the timeout wrapper’s CPU, matching the old GNU-time scope. Keep output destinations and scratch cleanup placement unchanged.

   Two feasible designs:

   | Design | Advantage | Required qualification |
   |---|---|---|
   | **Per-invocation `wait4`**, collecting raw user/system seconds and microseconds | Smallest replacement for GNU-time presentation; leaves ANN and runner behavior unchanged | Prove timeout correctly waits for descendants, all threads are included, and no child escapes accounting |
   | **Fresh per-invocation cgroup**, reading final `cpu.stat` after `populated=0` | Includes assigned descendants without relying on their wait hierarchy | More orchestration; establish placement before execution, stable final accounting, and absence of unrelated tasks or migration |

   Prefer the first, using the second as a completeness check during method qualification. Existing `scripts/benchmark_with_resources.py` already obtains before/after `RUSAGE_CHILDREN`, but also polls and sleeps; borrowing its accounting idea is smaller than introducing that entire observer.

   Serialize raw accounting fields without centisecond formatting. Microsecond representation does **not** establish microsecond accuracy: qualify effective resolution and discrepancy on the eventual host. Record supervisor self-CPU separately and bound its treatment-dependent cost; do not silently subtract it.

   These counters measure process/cgroup user+system CPU. They do not establish total host CPU, including unrelated work or kernel work charged elsewhere. That limitation must remain explicit.

2. **Freeze three exact estimands, separately for A and B.**

   Preserve 64 selected ordinals per fresh invocation and the first/last panels. Do not lengthen batches to dilute lifecycle overhead.

   For quad \(b\), let \(Q_{b,t}\) be the summed query CPU and \(L_{b,t}\) the complete lifecycle CPU across its two invocations in state \(t\).

   - **Query CPU:** \(R_Q=E[Q_{b,on}]/E[Q_{b,off}]\), ceiling **1.02**.
   - **Whole-process CPU:** \(R_L=E[L_{b,on}]/E[L_{b,off}]\), ceiling **1.02**.
   - **Added query wall:** retain the historical construction: for each ordinal, subtract the mean of its two off walls from the mean of its two on walls; take the panel median, then the median across quad observations. Ceiling **1 ms**.

   State that wall estimand precisely. It is neither the difference between marginal latency medians nor total request-cycle latency.

   Preserve CPU ratio-of-total estimation. For prospective inference, invert bounds on paired contrasts \(on-r\,off\), retaining the preregistered critical multiplier and disclosing its approximate coverage. Keep the latency order-statistic bound, with its independence/stability assumptions. Propagate validated accounting uncertainty into bounds rather than declaring it zero.

3. **Control nuisance through design, without removing real costs.**

   Keep each invocation entirely on or entirely off. Fresh processes avoid trace-state persistence within the runner; mixed per-query toggling would require another whole-lifecycle experiment anyway.

   Preserve randomized ABBA/BAAB treatment orientation. Randomize the order of the four arm/panel quads within each chronological block, rather than repeating the historical fixed arm/panel order. Freeze the seed and complete schedule before execution.

   Startup, authentication and truth reduction remain included in lifecycle CPU. Match them through identical authenticated inputs and lifecycle operations; do not subtract estimated startup time.

   Use the chronological block as the conservative inference cluster: it contains both panels for both arms. Queries and calls within it are correlated observations, not independent trials. Analyze arms separately.

   Preregister descriptive checks for orientation, previous-call treatment, panel, chronological position and early/late effects. Include a fixed between-call reset/check sequence, identical in both states, without assuming it resets S3 backend state. Account for any treatment-dependent cleanup before advancing.

   A nonsignificant carryover test does not prove absence of carryover. Material disagreement between orientations or chronological strata, or unresolved treatment-dependent persistence, prevents GO unless a preregistered conservative bound covers it. Do not choose the favorable order afterward.

4. **Require a bounded native accounting falsifier before performance measurement.**

   Prospectively qualify the supervisor with a disposable native fixture: **at most 16 invocations, CPU1, 256 MiB, no swap, 60 seconds, ext4 scratch, no network**. This is a proposed test, not executed here.

   Exercise the real timeout/supervisor/cgroup path with:

   - CPU work on multiple threads and a waited child.
   - Procfs reads and file operations outside query timers.
   - Serialization and CPU work after a fake terminal CPU snapshot, followed by final writes, sync and cleanup.
   - Separately injected query-window work and wall delay.
   - Nonzero exit, timeout and an intentionally surviving descendant.

   Use native clocks to record the actual injected CPU intervals. Include a sub-centisecond pulse to expose centisecond formatting, and a larger late-lifecycle pulse that must breach the whole-process ceiling while leaving the query CPU oracle unchanged.

   Mandatory outcomes: account for thread/child and late-lifecycle work; detect the deliberately failing gates; propagate exit status; reject survivors and incomplete accounting. Compare `wait4` against final cgroup accounting using an error envelope established by the fixture’s measured brackets and host accounting behavior.

   Mutating the supervisor to use the premature terminal snapshot must make this falsifier fail. If effective resolution or discrepancy cannot support the ceiling, the method is unqualified.

5. **Closed noise does not justify a confirmatory sample size.**

   Bounded streaming of the 256 completed calls reproduced the following:

   | Closed measurement | A | B |
   |---|---:|---:|
   | Query CPU ratio | 1.00830 | 1.00534 |
   | Whole-process CPU ratio, reported GNU time | 1.01462 | 1.01030 |
   | Whole-process upper bound **omitting only the formatting allowance**, still approximate | 1.03147 | 1.02693 |
   | Query CPU quad residual SD | 3.21 percentage points | 3.19 percentage points |
   | Internal precise CPU quad residual SD | 3.16 percentage points | 3.16 percentage points |
   | Historical median-wall upper bound | 1.654 ms | 0.685 ms |

   Removing the centisecond allowance alone would therefore not resolve this closed result. The internal precise counter corroborates substantial remaining variation, but remains an incomplete lifecycle measurement.

   The observations also differ by position: A’s reported whole-process ratio is approximately **1.02537** for the first panel and **1.00374** for the last; B’s is approximately **1.00599** and **1.01462**. These differences do not identify their cause, but argue against assuming interchangeable, stationary trials.

   As a **sensitivity calculation only**, with independent units, SD of 3.2 percentage points and multiplier 3, making the CPU uncertainty smaller than the available margin requires roughly:

   \[
   n>\left(\frac{3\times3.2}{2-\delta}\right)^2
   \]

   where \(\delta\) is true overhead in percentage points. That gives approximately 24 units at zero overhead, 93 at 1%, and 369 at 1.5%—before desired power, clustered dependence or wall feasibility.

   These are not defensible launch sizes. The revised accounting, clock-reuse candidate and conservative clustering lack measured joint variance. The closed data cannot support a guaranteed or adequately powered fixed size for all six gates. **Do not freeze a paid repetition from this extrapolation.** Independent method qualification and a separately authorized sizing basis remain prerequisites.

6. **Freeze dispositions before any future launch.**

   - **INVALID:** broken source/input pins, semantic mismatch, missing lifecycle accounting, escaped descendants, incomplete cleanup, accounting qualification failure, protocol deviation or execution/environment failure. Preserve the attempt.
   - **INCONCLUSIVE:** valid execution with any interval crossing its ceiling, insufficient independent units, or unresolved carryover/order effects that undermine coverage.
   - **GO:** all six conservative upper bounds meet their original ceilings, with accounting and inference assumptions supported.
   - **Overhead FAIL:** a qualified lower bound exceeds a ceiling. This rejects observer overhead for that frozen workload; it does not reject the ANN architecture.

   No optional extension, favorable-subset selection or query-only substitution is allowed.

Remaining gaps are target-host accounting qualification, descendant completeness, supervisor perturbation, backend carryover control, and a defensible fixed-size joint uncertainty calculation. Clock counts establish no timer-cost saving; this protocol would establish neither population tail latency nor a vendor win.

Inspection used committed evidence at `8b009397` and completed a0001 artifacts only, inside one CPU1/256 MiB/no-swap/120-second unit with bounded reads and page-cache discard advice. The unit reached its deadline after emitting the closed-analysis results; it was not restarted. No files were edited and no native tests, builds, network operations or experiments were launched.
