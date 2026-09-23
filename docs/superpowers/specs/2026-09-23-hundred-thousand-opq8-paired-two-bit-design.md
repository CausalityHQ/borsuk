# 100k OPQ8 paired two-bit read gate

## Decision

The terminal-closed OPQ8 source-only group-containment gate at source
`87236c5765186fb8ab977ef86f87a6b1047effb1` passed all preregistered
stops. This cell tests whether that fixed plan improves final page quality
when the unchanged historical 200-byte rotated two-bit scorer nominates
pages. It cannot change the OPQ8 model, top-four group ordering, group
budget, code records, page layout, nomination rule, or development cohort.

## Frozen inputs and phase boundary

Use the 100k source, membership, queries, truth, tree and page authority
identities from `scripts/native_page_microcluster_cell.py` and
`scripts/native_rotated_two_bit_cell.py`. Authenticate the completed OPQ8
model, codes, source seal and all 1,000 plans by their terminal artifact
identities before admitting queries or truth. Publish a source seal, then
authenticate and publish a separate 1,000-plan seal before admitting query
or truth objects. Authenticate the original
two-bit mean, groups and code seal from the terminal-closed rotated-two-bit
confirmation at `80ddf40533aefd3c24b7d3ea4539887aa94f91bb`.
The sealed OPQ8 plans are input, not recomputed or tuned in this cell.

Use one Causality Spot attempt from a pushed source revision, with a
create-only source archive, reservation, artifacts and terminal, full
readback, immediate compute termination, process-tree RSS plus 64 MiB below
3 GiB, and zero swap. The source/artifact seal must be published before
queries, truth or historical per-query outcomes can be read.

## Paired scoring

For each query, read the exact OPQ8-selected group ranges from the
historical S3 two-bit code object through the sealed Unix range broker,
one HTTP 206 GET per group. The unprivileged evaluator has no network
namespace access or AWS credentials. Validate
Content-Range, length and the sealed per-group SHA-256. Score each fetched
row with the existing `score_records` primary expression and use the
existing `nominate_pages` logic. Also compute the existing exact-source and
exact-norm diagnostics over the same candidate groups, with exact results
reported separately. The diagnostic cannot rescue a primary failure.

Replay the historical tree route and unchanged scorer over its original
group plan and its own S3 range GETs. Require every control per-query field
to match the terminal-closed rotated-two-bit evidence sample, including
plans, code bytes and page lists. Recompute ordered GT hit masks from each
arm's page list and the frozen ordered truth. Record each arm's GET
and byte count, group and page outcomes, data-page plan and resource
receipts. Validate the fixed candidate's selected groups against its
authenticated source-only plan for all 1,000 queries. Each arm has at
most 32 GETs and 16,777,216 code bytes per query. The candidate may skip
a ranked group that cannot fit the byte cap; it must preserve the
presealed resulting group list.

## Fixed gate

Advance only if the candidate primary has at least 98,418 GT100 hits,
at least 92 GT100 hits at p05, at least 9,951 GT10 hits, fewer than 36
queries with fewer than 90 GT100 hits, at most 32 code GETs and 16 MiB
code bytes per query, and at most 32 final data pages and 16 MiB planned
data bytes per query. The full historical control must replay exactly.
Source authority, read, replay or resource failures invalidate the
attempt. A valid quality miss kills this fixed OPQ8/top-four hypothesis.
If it passes, promote the unchanged source-trained model and scoring plan
to the preregistered 1M containment and read gate, then separately
measure serving throughput and two-generation 100M memory.
