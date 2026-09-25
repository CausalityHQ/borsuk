# V200 generic linear priced-cover fast path preregistration

## Decision

Can a two-state linear, *unconstrained* priced interval optimizer replace the
V198 exact capped NumPy DP on queries for which its own output satisfies the
frozen hard caps, with identical physical interval witnesses and materially
lower planner-core CPU time? This is a GT-blind planner implementation gate,
not a recall retune or a claim of full request latency. The model, prices,
mandatory units, candidate ranking, 672-unit cap and 32-GET cap remain V198's
frozen values. A query whose unconstrained optimum exceeds either cap must
use a future capped solver; do not accept it from this fast path.

On the complete, already-used ReLAION-1M D768 validation-1000 real-query
features, a pure Python read-only calculation found 861/1,000 unconstrained
solutions feasible and exactly equal to V198's interval lists, predicted
mass, units and GETs. The remaining 139 exceeded one or both caps. This
calculation was made before the Rust performance cell and is development
evidence, not a latency measurement. Validate it independently in Rust on
the same sealed GT-blind features/plans and V192 model. Source SHA-256s are
`3b4fe2d7a83bd0af16b1ecf460052b78311c6526bd7263fb3e708b56dd26a08c`
(features), `0a61974457030d2e2ce828e7bbbbaf3d8acd9849e70a7aaafd0cb44b0c5dda00`
(plans) and `b79683695350b4bc21eb4cad14f3588ed5ebaef088a1dd9443b3cc429ef62a01`
(V192 fit). Freeze a generated per-query optional-weight file before the
Rust timing run. No GT enters weight generation or the worker.

## Pass and limitations

Pass the exact fast-path gate only if Rust admits **at least 861/1,000**
queries and reproduces the exact V198 interval witness, mass, units and GETs
on every admitted query, with no false admission above either cap. Record
all-queries and admitted-queries p50/p95/p99 of only the linear solver,
process RSS, site-count distribution and every fallback cause. The latency
screen is diagnostic, not a product pass: target admitted p95≤5 ms as a
research hypothesis, then measure full route, feature, planner, transport
and rerank in a separate end-to-end gate. Do not extrapolate the 861/1,000
fraction to another dataset or assume the remaining 139 are cheap.

`formal/PredictedIntervalGuarantees.lean` proves that a truly global priced
optimum, when cap-feasible, is also a constrained optimum, and gives a
conditional linear state-work/time implication. It does not prove that this
Rust code implements the recurrence, that the V192 model predicts actual
GT100 on unseen data, or a measured CPU/latency upper bound. Small exhaustive
tests, exact witness comparison and timing provide separate evidence.

Use one Causality Spot cell from an immutable source commit, with terminal
artifacts and read-back hashes. Stop/terminate immediately at terminal.
An interruption discards the cell. A negative result requires a root-cause
decision before another attempt. A pass advances to an exact capped fallback
for the remaining 139 and then to concurrent end-to-end serving.

## Bootstrap correction after a0001

Attempt `a0001` on source commit `d7f4aff2` ended in GT-blind weight
generation before any Rust build or timing. The base AMI's default `python3`
is too old for `zip(strict=True)` in the frozen generator; the closed log
reported `TypeError: zip() takes no keyword arguments`. No measurement cell
was produced. The Spot instance `i-00d7e20d6c3b60d79` was terminated.
Attempt `a0002` uses Python 3.12 explicitly for the same generator, same
three source artifacts, same model, and same Rust parity/timing rule.

Attempt `a0002` sealed exactly the expected 1,000-query GT-blind weight
file (SHA-256
`797a83a7d830afd5cec491e70de3f49023a52c2f972af2045704b68df6f76313`)
but ended during Rust compilation: the benchmark counter used invalid
`usize += bool` syntax at two lines. No planner timing ran. The Spot
instance `i-00d4fbfe87be452fc` was terminated. Attempt `a0003`
changes only those counter conversions and retains the same inputs,
weights, reference plans and pass rule.
