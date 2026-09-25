# V205 exact boundary-price certificate preregistration

## Question and theorem

Can a cheap GET-cap-only search certify some of V203's 51 hard two-cap
queries exactly, reducing dense DP calls without changing prices, caps,
quality model or physical witnesses? The Lean theorem
`boundary_priced_one_cap_certificate` proves the condition: if a winner is
globally optimal under the GET cap after adding a nonnegative unit penalty,
and its *physical witness* uses exactly the original unit cap, it is optimal
under both original caps. A below-cap winner has dual slack and is **not**
certified; use the existing hard DP in that case. The theorem is conditional
on the Rust one-cap recurrence refining the mathematical optimum.

On the **already-used** ReLAION-1M D768 validation-1000 real-query frozen
V200 weights/V198 plans, preserve V203's linear → unit → GET hierarchy. For
only the prior 51 hard cases, search extra integer unit prices 1, 2, 4, …
up to 65,536 with at most 32 GET-cap solver probes. If a winner crosses
below the unit cap, use integer bisection of the last bracket within the
same probe limit, stopping immediately on an exactly-at-cap witness. This
fixed policy is a bounded search budget, not a vector-count or dataset
knee. Failed certification falls back to the unchanged V203 exact hard DP.

Record per-query probe count, certificate penalty, tier and phase
nanoseconds. Require the same 1,000/1,000 exact V198 interval/mass/unit/GET
parity, remote targeted tests, terminal artifact readback and instance
termination. Compare certificate yield and complete planner p50/p95/p99
against V203, including the cost of failed searches. Accept the extra
tier only if exactness holds and the measured tail improves; otherwise
remove it. This is planner-only evidence on a used split, with no new
recall, live S3 latency, concurrency or 100M result.
