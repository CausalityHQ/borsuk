# V192 optional-unit utility: closed fit diagnostic

V189/V190's frozen rank curve was fitted to source truth on **all**
candidate physical units. The planner must fetch the primary/mandatory
units independently of optional utility. Reusing the full-candidate
curve to value optional units therefore assigns to optional pages much
of the truth normally carried by mandatory pages. This creates a large
optional-total miscalibration under the original price scale, separate
from V190's missing hard unit constraint. A refitted global price can
absorb a uniform scale error; whether the new model improves plans is
still unmeasured.

`scripts/optional_rank_utility.py` fits an optional-only rank curve.
It also fits a monotone, four-bin estimate of candidate optional truth
mass from the number of mandatory units, a GT-blind query feature.
At prediction time it normalizes the optional rank weights to that
predicted mass; mandatory units receive no optional weight. These
weights can feed a planner with caller price and resource budgets.
The utility has no query-ID exception or vector-count memory branch,
and excludes truth outside the candidate
universe from predicted *fetchable* gain.
Normalization uses unrounded rank means before integer apportionment,
so a small caller price scale does not collapse all mass onto one page.
Queries with no optional units contribute zero optional gain to fitting
and return an empty optional weight map.

The reproducible `scripts/v192_optional_rank_fit_diagnostic.py` checked
the SHA-256-sealed V189 ReLAION-1M D768 fit features and fit labels. It
fitted on the first 64 V189 fit ordinals (2432–2495) and evaluated on
the other 64 fit ordinals (2496–2559). The full-rank comparator was
refitted on the same first 64, so this is a paired model-definition
diagnostic:

| Fit-only split | Candidate optional source GT100 hits | Full-rank predicted optional hits | Optional-only predicted hits | Mean absolute error, full-rank / constant / optional-only / zero (hits/query) | Hits within first 200 optional ranks |
| --- | ---: | ---: | ---: | ---: | ---: |
| Model fit, 64 queries | 126 | 1,323.222 | 126.000 | 19.028 / 2.239 / 1.873 / 1.969 | 117 |
| Internal fit validation, 64 queries | 74 | 1,440.300 | 97.109 | 21.493 / 1.983 / 1.194 / **1.156** | 65 |

The observed candidate optional truth is much smaller than the
full-rank optional prediction. The new estimate reduces that raw
miscalibration on the internal fit validation, but it overpredicts 74
as 97.109 and has **higher** absolute error than the zero predictor.
It has not shown useful calibration skill or a recall/physical-plan
gain. The first 200 optional PQ ranks contain 65/74 optional truth
hits; the new model preserves this rank order and has not shown that
its priced intervals fetch them efficiently. These 64 queries belong
to an already examined
closed fit panel, so they are **development evidence**, not a new
untouched holdout. Neither V189 nor V190 holdout truth was used to fit
the model.

An exact **truth-aware** hard-cap oracle on all 128 closed V189 fit
queries now shows the physical ceiling is high: mandatory-only
contained 12,579/12,800 GT100 hits with p05 93; all scored candidate
units contained 12,779 with p05 99; the candidate-restricted oracle
also captured 12,779 with p05 99 at 150,259,200 planned bytes and
2,359 GETs. Every mandatory cover was feasible at 672 units/32 GETs.
This used 1.45 seconds wall time and 151,320 KiB peak RSS locally
because its sparse truth sites are small. These are source
containment and offline-process numbers, not a GT-blind policy or
serving performance. The oracle fixes the next decision: physical
reachability is available on this fit panel, so utility/price must
demonstrate that it can find those pages without truth.

The next developmental screen must pair the utility with the exact
hard-unit/GET planner and refit a comparable price for the full-rank
control on the same disjoint price-fit portion. It must count full
GT100 including candidate omissions and compare actual contained
hits, p05, bytes and GETs. A diagnostic win still needs a fresh
held-out source panel and then returned S3 serving, latency, RAM
and cost measurements. Neither fit diagnostic can promote the
architecture directly.
