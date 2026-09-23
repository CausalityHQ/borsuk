# ReLAION-1M OPQ8 query-only data-range selector gate

Status: design for the next source-only decision. No outcome is claimed.

## Decision

The closed 32-physical-page oracle upper bound is 86,474/100,000 GT100,
so that final wave cannot pass. An exploratory, truth-aware interval
witness from the same closed cohort fits 98,935 GT100, p05 94 and 19
sub-90 queries within 32 role-preserving contiguous ranges and 16,777,216
encoded bytes. The witness only establishes that a query-only selector is
worth testing. This gate determines whether the existing OPQ8 row code
plane can nominate enough data ranges on the original layout. It does
not claim production reads, latency or untouched validation recall.

## Frozen authority and comparison

Reuse the source, generation, base/delta physical pages, OPQ8 model and
codes, query cohort, and both candidate/control selected-group plans of
the terminal-closed 1M OPQ8 route at source `cc0dd60de8b63a87656475591be12127ad4f3769`.
Authenticate every reused object against its terminal-listed byte length
and SHA-256 before planning. Rebuild the physical row-to-page map from
the authenticated Arrow runs and assert its source row order digest and
group membership against the OPQ8 source seal. The page-centroid control
uses its own frozen selected groups but exactly the same OPQ8 scores,
page priority rule and data-range selector. Neither arm reads truth while
planning. The development truth file is available only after both plan
files and their digests are sealed.
The prior plans artifact also contains a historical `source_distance` arm
that used exact source vectors. This gate reads only candidate and control
group plans; that diagnostic arm has no role in page priority, admission
or evaluation.

## Query-only rule to freeze before launch

For each of the 1,000 ordered queries, evaluate the existing eight-table
OPQ8 ADC expression on the authenticated 1M code plane. Reject nonfinite
query or score values. Within each arm's frozen selected groups, sort rows
by `(float32 score, physical row ordinal)`, take the first 100, and rank
their owner pages by descending count, then smallest row score, then
`(role, page ordinal)`. Append the other pages in those selected groups
by smallest row score and the same stable physical tie. The ranked page
list is the sole page-priority input to range admission.

Scan ranked pages in order. For each tentative accepted-page set, find
the minimum encoded-byte covering with at most 32 contiguous ranges,
never crossing base/delta object roles. Start with one range per maximal
run of accepted pages. If more than 32 ranges exist, bridge the cheapest
physical gaps, where gap cost is the exact sum of intervening encoded
page bytes; break equal gap costs by role and earlier page ordinal.
This greedy gap choice is optimal for fixed target pages because every
bridge reduces the range count by one and gap costs are independent.
Accept a page exactly when this covering costs at most 16,777,216 bytes;
otherwise skip it and continue. Seal the final ranges, all included
pages including bridged pages, GET count, exact encoded bytes, and the
score/tie arithmetic identity. No query truth influences admission.
The two role objects have distinct byte coordinates; authenticate their
physical page spans and reject overlap, out-of-bounds offsets or any
reported budget excess.
Before launch, the authenticated 416,563-byte generation manifest
(`45fa4e708ab660151a7b1ea79e35eada6090faced1bfb147f7e20cac7055e754`)
was checked across every page: each role starts at offset zero, all 3,639
pages per role tile without gaps, and their byte sums equal 708,888,104
base bytes and 80,900,000 delta bytes in the sealed objects.

## Evaluation and stop rule

After sealing plans, independently map all 100 ordered GT IDs and first
ten IDs to physical owner pages. Count a hit whenever its owner page is
inside a planned range, including bridged pages. Record per-query hit
masks, GT100 and GT10 counts, p05 GT100, sub-90 count, range count,
encoded bytes and candidate/control paired wins, losses and ties. The
evidence also records each truth owner's one-based rank in the sealed
page priority, or null when absent, and classifies each position as a
target-page hit, a bridged-page hit or a miss. This lets a failed gate be
assigned to group containment, score ranking or range dispersion. The
candidate advances only if it reaches at least 98,151/100,000 GT100,
9,928/10,000 GT10, p05 90 and at most 49 sub-90 queries, with all
queries within 32 ranges and 16,777,216 encoded bytes. Report the
control's full outcome; retain the old 1M selected-group control as
historical route evidence, not a final-range control. A valid miss
rejects this selector and triggers diagnosis of ranking versus layout
or representation; do not tune on this development cohort. A pass
licenses an actual authenticated data-range read gate, not production
readiness.
That later read gate must include a fresh untouched query cohort. The
current 1,000-query development cohort already informed the truth-aware
interval witness and selector design, so its outcome is development
evidence only.

## Execution and independent closeout

Run one create-only Causality Spot attempt from a pushed source revision
and source archive, with a 3-GiB process-tree cap, 64-MiB margin and
zero swap. Do not start an overlapping attempt. Sync terminal-listed
artifacts to S3, verify create-only readback, and terminate compute
immediately on terminal closure. A Spot interruption invalidates that
measurement cell and restarts from the same frozen source in a new
attempt. Monitor an incomplete attempt through terminal and instance
health only. Independently authenticate every terminal-listed artifact,
recompute every page-range cover and hit mask from sealed plans and
truth, and reject any mismatch before entering a result in the ledger.
The sealed priority list is the cross-host closeout authority. Re-scoring
on a different BLAS host is diagnostic because float32 ties may change;
the on-host validator reruns the exact planning phase. Receipt closeout
separately authenticates the terminal, source archive, artifact roster,
instance identity and zero-swap resource files before acceptance.

## Formal boundary

The minimum-byte bridge rule and the 32-range/16-MiB accounting can be
proved over integer page lengths and sealed role boundaries. Code-to-page
mapping, float32 ranking, and the executable planner need refinement
checks against that abstract model. A conditional score-error certificate
could then lower-bound recall on authenticated queries. Without checked
score margins and owner-page evidence, Lean cannot prove empirical recall.
The Lean sequential latency ceiling requires certified service-time and
local-compute premises; this source-only gate supplies neither.
