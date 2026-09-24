# V120/V121 layout-method qualification audit

Recorded while V120's frozen source-only Spot attempt was running, before
downloading any V121 query or GT payload. This note does not change V120's
preregistered method or its source archive.

V120's builder uses `scripts.v82_scale_build.lloyd` for 12 iterations, a
64-rows-per-centroid sample, V82 seeds, and the V82 greedy centroid chain.
It sorts rows by chain rank and a source-derived radius key. V116's
ReLAION-1M artifact instead used V63's `CoarsePartition` fitter: the same
iteration and sample-count targets, but a different seed sequence, explicit
empty-centroid reseeding, and a different radius scale. The V120 smooth
cluster-count rule also has no matched ReLAION-1M quality measurement yet.

Therefore the V120 build and any V121 deep-image result test a **new
source-only layout method** combined with the V116 router/planner/scorer.
They do not establish that the exact V116 method generalizes across
embedding families, even if V121 clears its untouched quality rule. V82's
historical deep-image results remain different-format context, not a paired
baseline. Do not compare V121's aggregate directly with V116's aggregate as
one frozen architecture or infer a release default from the pair.

The promotion gate for this V120 method is a newly built, source-only
ReLAION-1M index using the same layout, SQ8, balanced PQ64, router and
scorer revision, followed by same-run candidate and V109-style capped
control on development and untouched validation splits. Freeze that method
before the latter split. If the 10M runtime screen rejects the flat router,
preserve its negative evidence and change the routing architecture before
spending on this matched quality campaign. No dataset-name branch, query/GT
training, or vector-count quality-tier switch is justified by V120.

V120 is **construction**, not a promoted 10M quality cell. The D96 balanced
PQ64 change has passed unit checks but no 100k returned-quality gate. Before
running V121's 1,000-query GT reduction, use a preregistered 100k
deep-image development screen with a fixed, corpus-only subset and disjoint
held-out queries; derive exact GT within that subset without using V121's
first 1,000 test queries. A poor 100k result rejects the method early and
keeps V121's untouched split unread. This restores the cheapest-decisive-gate
sequence for the format change without relabeling V120's already launched
construction job as a quality measurement.
