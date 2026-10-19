# Mathematical feasibility audit

ID c18dedbe08da424c. Completed; proposals only. Actual scorer applicability requires EC2 source verification.

**The idea is mathematically feasible, but applicability to BORSUK remains unverified.** Proceed only if the actual scorer admits a sound bound and refinement ranges can physically remain unread.

For record \(i\), assume exactly the supplied hypothetical model:
\[
z_{ij}=a_i b_{0ij}+c_i b_{1ij}+d_i,\qquad b_{1ij}\in\{0,1\}.
\]
After reading \(b_0\), define
\[
A_i=\sum_j q_j(a_i b_{0ij}+d_i),\qquad t_{ij}=c_iq_j.
\]
The tight real-arithmetic interval is
\[
\boxed{L_i=A_i+\sum_j\min(0,t_{ij}),\quad
U_i=A_i+\sum_j\max(0,t_{ij}).}
\]
Each endpoint is attainable by choosing each unknown bit according to the sign of \(t_{ij}\). Thus this is tight when all bit assignments are admissible. Its width is
\[
U_i-L_i=|c_i|\,\|q\|_1.
\]
Reading the first bit changes the interval’s location, but does not reduce this residual width.

If the supplied finalizer is \(F_i(s)=\alpha_i s+\beta_i\), its interval is
\[
[\beta_i+\min(\alpha_iL_i,\alpha_iU_i),\
 \beta_i+\max(\alpha_iL_i,\alpha_iU_i)].
\]
A negative factor reverses the endpoints. Other finalizers require verified monotonicity or a sound range calculation. Factors depending on unread bits require a joint bound; treating them as known constants is invalid.

The floating-point proof obligation is stronger:
\[
[\ell_i,u_i]\supseteq
\{\operatorname{score}_{\text{original,float}}(i,v):v\in\{0,1\}^{D}\}.
\]
The real-arithmetic formula alone does not establish this. Use a proved error envelope or outward interval evaluation covering the actual decoding, casts, products, FMA/reduction order and finalizer. Reassociating the score, adding an arbitrary epsilon, or rounding only the final endpoints outward is insufficient.

Let \(m\) be the actual SOURCE shortlist capacity—neither final \(k=10\) nor automatically rerank16. Orient comparisons so larger scores rank first. Once at least \(m\) rows have been scored exactly, let \(\tau\) be the worst retained score.

- A row is safely excluded when \(u_i<\tau\).
- Equality requires refinement unless the original tie rule proves the row loses.
- Skip a 32-row refinement unit only when **every row** is safely excluded. Otherwise refine the whole unit.
- Stop when no unresolved unit contains a possible entrant. Maintain the threshold using fully evaluated rows and the original comparator.

This preserves the shortlist because every excluded row already loses to \(m\) exact rows; subsequent refinements can only improve that threshold. Duplicate handling, signed zeros, nonfinite values and arrival-dependent tie behavior must follow the original semantics.

An explicit adversarial example uses \(D=2\), \(q=(1,-1)\), \(a=c=1,d=0\), and \(b_0=(0,0)\). The interval is \([-1,1]\):

| Unknown \(b_1\) | Exact score |
|---|---:|
| \((0,0)\) | 0 |
| \((1,0)\) | 1 |
| \((0,1)\) | −1 |
| \((1,1)\) | 0 |

Against an exact leader scoring \(1/2\), the row cannot be pruned. Put one such row in every unit; give the other 31 rows \(b_0=(0,1)\), whose upper bound is 0. Every unit still requires refinement—even if every ambiguous row actually scores −1. For the tie falsifier, use a leader scoring 1 and an ambiguous row attaining 1 with the preferred tie key: pruning on \(u_i\le\tau\) loses the correct winner.

One actionable plan:

1. **Root verifies four source facts on EC2 before coding.** Pin the actual source, including dirty changes; the supplied commit alone is insufficient. Inspect:
   - Decoder and query representation: actual bit equation, coefficient dependencies, transforms and admissible bit combinations.
   - Complete scorer/finalizer: factor availability, signs, normalization, arithmetic types and reduction order.
   - SOURCE selection: capacity, comparator, ties, duplicates and dependence on processing order.
   - Storage reader: independently addressable refinement ranges, metadata, compression/checksum boundaries, and coalescing behavior. Determine whether validation itself requires reading both planes.

2. **Specify one bounded native diagnostic.** Freeze source/layout, nominations, and the first eight requests in the existing fixture order before examining results. First exhaust the tiny example’s completions and tie cases. Then compare the untouched full scorer with the progressive path on those requests, using separate cold client caches. Seed the progressive heap by refining units in descending upper-bound order, with fixed deterministic ties and the existing concurrency limit. Retain the original scorer for refined rows. Require identical ordered shortlist IDs and retained score bits. Keep truth admission required by the existing protocol, but use no truth-guided query selection or parameter fitting.

   Run through the actual range reader. Record logical rows excluded, units skipped, **actual response-body bytes**, GETs including retries, coalesced overfetch, and serial dependency waves. Include first-plane and factor/metadata costs. A simulated skipped-byte counter is insufficient. Existing admission and disposable canary gates remain prerequisites to any paid run.

3. **Use explicit stop/continue evidence.**

| Evidence | Disposition |
|---|---|
| Scorer enclosure or independent range access cannot be established | Stop coding this formulation; resolve applicability first. |
| Any interval escape, shortlist mismatch or tie mismatch | Correctness failure; stop performance qualification. |
| No refinement ranges actually avoided, or no net SOURCE byte reduction | Stop promotion from this diagnostic; no demonstrated byte-saving mechanism. |
| Zero mismatches and net physical byte savings | Continue to the existing paired p95 gate; no latency win established yet. |

For an additional early rejection calculation, use the reference run’s final cutoff **only offline**, never as the progressive path’s threshold. If every unit still has a possible entrant even at that final cutoff, this bound cannot eliminate any refinement unit on that query.

The principal collapse conditions are wide residual intervals, one survivor per unit, coalescing that transfers supposedly skipped ranges, and extra serial RTTs overwhelming byte savings. Fetching both covers repeats the historical mirrored-plane failure. Neither split-format inspiration nor the supplied recall result proves deterministic pruning.

Not checked: repository or EC2 source, datasets, native execution or physical I/O. Codec applicability and performance remain unverified.
