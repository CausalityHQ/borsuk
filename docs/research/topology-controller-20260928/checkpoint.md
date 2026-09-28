# Controller checkpoint

The native product goal remains active and unchanged. Base `346aa524` is the
last delivered revision. This slice adds six stdlib metadata/controller helpers;
it does not execute the scientific comparison or select production defaults.

## Verified checks

- Red a0001: EC2 rejected the overlength client token before creating an
  instance. Preserve the failed reservation; no measurement or compute charge
  is inferred from it.
- Red a0002: all six explicit stubs failed as intended on Causality Spot.
  `i-0e8f429d8169ed964` is terminated. Its source and closed artifacts were
  verified before the implementation replaced the stubs.
- Green a0001: all six implemented checks passed on Causality Spot.
  `i-05e6edff63f314571` is independently verified terminated. Archive
  `87d1747663886e3999ed5e0cd9a227d58f73ac30e30e1c68f1adb732a2a4aa01`
  matches 395 relevant files; all four closed artifacts match their recorded
  lengths and SHA256s. Terminal SHA256 is
  `6ee3a89ac86ad48c35092e1d2634badc03c9335d27963472096a1ef2d07c161e`.
  Observed launch-to-close time was 36 seconds; compute estimate $0.0018 from
  the observed Spot quote, excluding EBS/S3 and not an invoice.
- No compiler, corpus, planner or scorer ran in these short cells. Unchanged
  Rust/dependencies retain the adapter slice's verified 2684-pass assurance;
  a duplicate full Cargo run would add no evidence about these Python helpers.
- The narrow Opus code review completed and its twelve concrete regressions
  all failed as intended on review-red a0001 before the fixes. Instance
  `i-052a3059e19c13675` is verified terminated; archive
  `656b077154f742d512d1271198cedb6cc81d85136fa2baf40fd1391283db104e`
  and four artifacts were verified against 395 relevant files before editing.
- Final review-green a0001 passes all six original checks and all twelve
  regressions. `i-09f00e0bd1e8d407a` is independently verified terminated.
  Archive `691f6322a1744ce4ecefbfac2794fb9b31d1be86260d07e056197bfaf45002db`
  matches 395 relevant files and all four artifacts are verified. Each review
  worker took 36 seconds launch-to-close, estimated compute $0.0018 each,
  excluding EBS/S3 and not an invoice. No worker or consultation remains active.

## Next executable gate

Finish the corpus wrapper and freeze its identities and full resource budget.
It must hash-check the frozen scorer files, reproduce the current-v3 control's
historical query-component fingerprint, and require all 64 normal control plans
and per-query fetched/returned/exhaustive counts before candidate queries.
Then require topology-only root equality, the full alternating arm/hash roster,
GT gained/lost identities at every stage, and the fixed integer quality gates.
The normal preflight plus paired run consumes 192 native planner calls per
executed corpus; disclose them and reuse only the full SQ8 score vectors.

Run one ReLAION-first development 0–63 screen after every preregistration HOLD
is closed. Its first scientific failure skips CoHere, validation and scale.
No automatic retry, query warm-up, sweep, cap change or architecture re-review.
The previous dual critique remains the authority, including its disagreement;
the next default dual-review eligibility is 2026-09-29 00:18:08 UTC.

ReLAION method-validation 256–999 remains failed against exhaustive native SQ8
(0.717742 percentage-point recall deficit, allowed 0.5). Development survival
cannot repair or independently confirm that failure. A scalable source layout,
post-maintenance quality/rewrite cost, a genuinely fresh sealed cohort, cold
physical HTTP, and BOTH vendor comparisons remain open product gates.

## Current quality evidence (unchanged historical measurements)

All rows below are first100k, D768, cosine, k100. Recall is percent; p05 uses
the frozen order statistic. Native results are Rust plans scored with the
frozen Python f32 SQ8 mirror, not physical serving measurements. Closed
artifacts and their reductions were verified in the preceding slices. No
controller cell added a quality measurement.

| Dataset and split | Native returned mean / p05 (%) | Paired exhaustive SQ8 mean / p05 (%) | Historical unchanged V282 graph mean / p05 (%) |
|---|---:|---:|---:|
| ReLAION development 0–63 | 99.15625 / 97 | 99.515625 / 99 | 97.984375 / 94 |
| CoHere development 0–63 | 99.09375 / 98 | 99.234375 / 98 | 96.25 / 92 |
| ReLAION method-validation 256–999, 744 queries | 98.8508064516 / 97 | 99.5685483871 / 99 | Different architecture; not a topology control |

V282 development records share verified corpus/query/GT identities, but use
a different layout/encoding/architecture. Its bounded flat centroid-routing
ablation is not exhaustive SQ8. V282 timings and old Spark timings are stale
for the current AWS revision. CoHere native method-validation was skipped
after ReLAION's first failure. Native development fetched totals are 6372
ReLAION and 6385 CoHere; all recorded plans stay within 32 logical ranges and
16,773,120 bytes. Physical requests, cold p90/p95 milliseconds, QPS per total
lifecycle dollar, RSS during swaps and wins against either vendor are unmeasured.
