# V206 Deep-Image FP16 representation closeout

## Closed authority

The preregistered **already-used** Deep-Image-96-angular publication
test-first-1000 diagnostic completed on 9,990,000 corpus rows from source
`ac6738bdec80e9ba94ee505c271168ab50d0ff88`, attempt `a0002`,
Causality Spot `i-0b30e9d5fbd2e129b`. Terminal SHA-256 is
`8a16c5053a3d65235ef25986bd45b49ddf4db697c4a8bbb028a481118fb71000`.
The launcher read back all 11 artifacts and both pretruth S3 copies,
then verified the instance terminated. `scripts/check_v206_deep_image_fp16.py`
independently re-read the closed inputs and artifacts, published GT,
all 1,000 V121 paired physical plans and original SQ8 returned IDs, and
all GT100 intersections and summary counts; it passed. The returned IDs
and seal were uploaded to S3 **before** GT download.

Attempt `a0001` at `f12d5638` stopped in input authentication because
the layout SHA string had 89 characters. Its source hash and numeric
smoke passed, no query or GT was processed, and Spot
`i-0b4e3cc0b859fdc92` terminated. The corrected digest and static
length guard were frozen in `a0002`; the scoring method did not change.

## Same-range returned quality

All three arms use **the exact V121 physical ranges**. FP16 and float32
rank each fetched source vector by float64 cosine with stable train-ID
ties; SQ8 is V121's closed Rust returned list. Planned SQ8 transport is
unchanged at 13,763,450,880 bytes and 21,536 GETs across 1,000 queries.

| Deep-Image D96 test-first-1000 | GT100 hits / 100,000 | p05 hits / 100 | Queries below 90 |
| --- | ---: | ---: | ---: |
| V121 SQ8 returned | 98,034 | 96 | 12 |
| V206 same-range FP16 | **99,547** | **99** | 12 |
| V206 same-range original float32 | 99,579 | 99 | 12 |

FP16 beat SQ8 on 821 queries, tied 179 and lost none. It recovered 1,513
GT100 hits on identical ranges. V121's independently measured physical
coverage was 99,580/100,000, so the float32 arm misses one covered GT100
hit and FP16 misses 33. The remaining 420 GT100 positions outside the
ranges cannot be recovered by a scoring representation change. All 12
queries below 90 have exactly the same hit count under SQ8, FP16 and
float32; their severe tail is upstream of final-score precision on this
panel. FP16 lost only 32 hits to float32 over 32 queries.

### Closed-artifact tail trace (post hoc)

`scripts/check_v206_deep_image_tail.py` authenticates the V120 layout,
V121 512-row nominee rosters and physical replay, V206 pretruth returned
IDs, and publication GT, then maps each GT ID to its physical row. Its
independent trace on this **used** panel found 99,994/100,000 GT IDs in
the nominee rosters; the ranges covered 99,580. Of the 420 uncovered GT
IDs, 415 were nominated and then dropped by physical selection, while
six were absent from nomination (one of those six was covered anyway).
FP16 returned 99,547 of the covered IDs.

Across the 12 below-90 queries, 1,196/1,200 GT IDs were nominated,
1,021 were physically covered, and FP16 returned all 1,021. All 12
plans used the full 32 GETs and 16,699,392–16,754,688 bytes of the
16,777,216-byte cap. Thus capped physical selection is the immediate
cause of this panel's severe tail. This is a post hoc layer diagnosis,
not a prospective quality result or evidence that a different planner
will recover the misses within the same cap.

The same checker computes a **GT-aware, non-deployable exact oracle** for
each tail query: among all unions of at most 32 page-aligned intervals
under 16,777,216 bytes, it maximizes the number of GT100 IDs covered.
The dynamic program minimizes pages needed for each hit count and GET
count; interval endpoints can be trimmed to GT-bearing pages without
losing hits or increasing cost. None of these 12 queries has a GT hit
on the short final page, so full-page accounting is exact here.
`scripts/test_v206_deep_image_tail.py` checked the DP against exhaustive
physical-page subsets in 50 small random layouts.

| Test ordinal | V206 FP16 hits | Oracle maximum hits at 32 GET / 16 MiB | Minimum bytes for 90 hits at 32 GET |
| ---: | ---: | ---: | ---: |
| 63 | 89 | 91 | 13,492,224 |
| 212 | 86 | 86 | 24,662,016 |
| 327 | 84 | 84 | 36,135,936 |
| 552 | 84 | 84 | 36,412,416 |
| 591 | 88 | 88 | 20,321,280 |
| 681 | 88 | 89 | 18,220,032 |
| 684 | 89 | 91 | 14,155,776 |
| 772 | 78 | 78 | 51,674,112 |
| 895 | 89 | 91 | 11,335,680 |
| 925 | 87 | 87 | 22,422,528 |
| 928 | 84 | 85 | 31,297,536 |
| 999 | 75 | 76 | 82,750,464 |

**Nine of 12 cannot reach 90 hits with any planner under this layout and
I/O cap.** Three have theoretical room to clear 90, but the GT-aware
oracle is an upper bound, not a serving algorithm. The severe tail
therefore calls first for a generic physical-layout or resource-policy
change. The I/O cap should be a measured recall/latency/cost choice,
not an arbitrary vector-count knee. Any candidate must be frozen before
untouched queries and measured for real GET, byte, latency and memory
costs; these oracle byte minima are not observed serving costs.

The preregistered representation screen **passes**. The offline FP16 and
float32 return phase took 3:03.45 wall/442.05 user CPU seconds and peaked
at 9,000,868 KiB process RSS; GT reduction took 0.26 seconds and
125,428 KiB. These are batch research resources, **not** live S3 latency,
concurrent throughput, charged serving RAM or a production resident-plane
build measurement.

## Scope and next gate

This is a strong cross-dimensional **representation** result on a used
real-query split. V120's Deep-Image layout/router builder and V197's
ReLAION layout/planner are different, so these results do not establish
one frozen corpus-generic end-to-end method. The severe-tail diagnosis
now points to layout/cap feasibility, with limited planner headroom on
three queries. Do not insert dataset-name branches or query exceptions.
Define a corpus-only physical-layout or resource-policy candidate on
development evidence, then freeze it and run untouched Deep-Image
real-query ordinals with a paired FP16/SQ8 control. Build the same
source-only method on ReLAION for a matched cross-corpus comparison
before defaults, live S3, 10M scale claims for one architecture, or a
100M RAM policy.
