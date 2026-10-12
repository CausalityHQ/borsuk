# Prospective cold ANN hill climbing

Operator direction: retain improvements rather than reject useful speed/quality tradeoffs for narrowly missing an aspirational recall threshold. Applies to NEW trials only; all frozen historical protocols, source revisions and original GO/FAIL outcomes remain immutable.

## Measured anchor

Current1M Cohere/D1024/cosine/k10 width32: R10=97.80%, p90243–258ms, p95280–292ms, sequential4.76–4.84QPS. Width16 has the SAME quality and exact returned ID/score/trace/plan parity, p95498–540ms and2.92–2.94QPS. Evidence is committed at e35d97c4. Width32 is retained; missing the98% reference by0.20 percentage points does not erase the measured gain. Provider cache remains uncontrolled; two repetitions and sequentialQPS do not establish concurrent capacity or vendor wins.

## Prospective promotion rule (root's initial engineering choice)

- Keep all nondominated quality/tail-latency/resource points, including candidates outside the promotion tolerance. A reference miss is reported separately from correctness and execution validity.
- For speed-led promotion, require paired p95 improvement of at least10%, p90 and whole-pass throughput each no worse by more than5%, and recall no more than0.20 percentage points below the FIXED97.80% anchor (floor97.60%). At1000 queries/k10, that allowance is20 hits out of10000. Do not ratchet the floor downward after each accepted candidate.
- Confirm the speed direction in both balanced paired repetitions; single-run improvements are provisional. Preserve per-arm results rather than average away a regression.
- Keep recall improvements on the frontier even when they modestly cost latency; select by matched vendor quality and lifecycle cost, not one universal scalar score.
- Correctness/authentication/wrong-input/transport/resource-enforcement failures remain executionINVALID. An actual algorithm resource overrun is a measured result, not an environment failure. Larger resource envelopes may be predeclared from measurements; do not silently inherit historical caps as universal gates.
- Vendor-win/publication claims still require equal or better recall at disclosed matched corpus/query/metric/cache/load settings. A97.60% local candidate cannot be presented as beating a higher-recall comparator.98% remains a reference, not an unconditional rejection gate for this prospective frontier.

## Faster feedback

Prefer configuration changes supported by a qualified binary and reuse fresh unchanged correctness evidence. Do not rebuild the full workspace for a configuration-only trial. Use one causal change per performance experiment on the SAME frozen dataset; no warm-cache or new-dataset campaign.

For code changes: compile affected targets and run the fast native correctness falsifier first; perform actual-input admission and separate canary; obtain exploratory full1000-query numbers before spending time on unrelated assurance checks. Candidate measurements are explicitly UNVERIFIED source until mandatory workspace Clippy and real shim-unset test-build pass. Those gates remain mandatory before integration/push. The current live SDK qualification finishes unchanged.

Keep build/falsifier/conditional assurance in one preregistered finite campaign where appropriate, with distinct resource-controlled phases and no idle instance retention. Use existing full1000 query execution (roughly3.5minutes per width32 arm) rather than add a new subset runner/controller. Stage only exact native inputs; record terminal results and close compute immediately. Narrow environment failures to their demonstrated layer; never rerun an unchanged paid experiment because observation timed out.
