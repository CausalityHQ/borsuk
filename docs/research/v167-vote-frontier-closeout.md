# V167 minimum-cost vote frontier: killed at fit gate

## Closed campaign identity

- ReLAION-1M D768, source-pseudoquery human ranks 257–384 (128 fit
  queries). Ranks 385–512 were sealed but **not parsed or scored** because
  no fraction passed fit. These source rows were present during router
  and layout training; this is an internal proxy, not validation recall.
- Source commit `4958d9bf10d1efd911823363935379dd547c469e`, archive
  SHA-256 `26d6e64845eb64c009a21ee7b2225a2a95e29e0781d3087f6b70cec1e8c43903`.
- Causality Spot `c7i.12xlarge` instance `i-0f1b860535e825154`, attempt
  `a0001`, S3 prefix
  `research/v167-vote-frontier/4958d9bf10d1efd911823363935379dd547c469e/runs/a0001/`.
  Terminal status `complete`, terminal SHA-256
  `86860fc0e8b8af91c90bd73947a21489b3cacf9eb6e8041ac03bc53591f8c1a1`.
  The controller streamed and rehashed all 12 terminal-listed artifacts,
  then terminated the instance. The separate deterministic checker
  returned `pass` with decision `killed` for all 256 planned rosters.

## Frozen fit result

The paired baseline is the unchanged V165 full-cap 513/1 vote plan on
these same 128 queries. Capture counts are above-threshold SQ8
non-nominee rows in the declared nominee-unit-plus-neighbor universe.
Bytes are planned encoded SQ8 bytes, not S3 billed bytes. GETs are
planned contiguous ranges, not observed request latency.

| Fraction | Captured vs V165 442 | Planned bytes vs V165 1,716,149,760 B | GETs vs V165 2,266 | p95 lost rows | Fit quality |
| --- | ---: | ---: | ---: | ---: | --- |
| 4/5 | 381 | 269,717,760 B | 2,816 | 2 | fail |
| 9/10 | 407 | 396,564,480 B | 3,072 | 2 | fail |
| 19/20 | 421 | 536,515,200 B | 3,277 | 1 | fail |
| 39/40 | 428 | 674,519,040 B | 3,454 | 1 | fail |
| 99/100 | 434 | 824,229,120 B | 3,606 | 0 | fail |
| 1/1 | 434 | 980,353,920 B | 3,715 | 0 | fail |

The strongest capture in this grid is 434/442 = 98.19%, below the
preregistered 99% fit floor. At `1/1`, planned bytes were 57.13% of
V165 but GETs were 163.95%. No fraction passes, so `selected_fraction`
and holdout metrics are null. V167 is **killed**. The panel does not
measure returned Recall@100, live S3 latency, or production RAM.

## Fit-only root-cause diagnosis

After the terminal closed, the exact roster and plan objects were
downloaded and checked against their terminal SHA-256 values:
`rosters.jsonl` = `7cf7b85ae20b3404cb9fe7c64e05915ac5e7018cd98847fb146c745c4e92df03`,
`plans.jsonl` = `1f75e70da0958d43b5055c0be228aaae539b91868fa960271a5417b7979048bf`.
Only the first 128 lines of the proxy-label stream were parsed. This
fit prefix is 341,366 B with SHA-256
`d1cb182656ae020c4f6ab5bc68bbb450de9f5ba8d48a488add438d8a4e2d2c34`;
the holdout lines were not parsed.

For `1/1`, V165 and V167 had equal 513/1 vote scores on **128/128**
queries and both captured **386** rows in units carrying a nominee vote.
V165 captured **56** useful rows in unvoted units included by contiguous
GETs; V167 captured **48**. Exactly eight useful rows were in V165-only
unvoted units, zero useful rows were in V167-only units. Paired V167 capture
wins/ties/losses were **0/122/6**. V165 charged 68,756 total units,
including 50,568 unvoted units; V167 `1/1` charged 39,277 units,
including 21,089 unvoted units. These are aggregate unit-query counts,
not distinct physical units.

Thus V167's minimal-byte objective removes useful gap or adjacent units
that have no nominee vote; retaining **all** achievable votes cannot
recover them. It also spends many more GETs to avoid gap bytes. This is
a structural mismatch between the vote objective and physical coverage,
not a fraction-selection error. V166's earlier per-unit isotropic mass
surrogate also failed to predict this tail. Do not reuse this fit panel
to tune a coefficient or reopen its untouched holdout under a revised
rule.

## Next decision

The next candidate must change the information available to the
planner or the physical layout so that useful unvoted neighborhoods are
represented, while accounting for both per-GET cost and bytes. A new
method needs a generic source-derived rule, a fresh panel or the
cheapest decisive 100k returned-quality gate, and explicit build/RAM
bounds before 10M/100M promotion. The V167 result does not qualify any
production default or 100k advancement.
