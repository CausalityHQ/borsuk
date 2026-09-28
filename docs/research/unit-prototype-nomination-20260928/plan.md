# Isolate prototype norm bias under cosine

Base9c0f62c8. Prior exact raw-mean squared-distance arms are closed/KILL.
One change: normalize existing32-row source means before exact nomination.
Same immutable hierarchical source order/extents, source reservoir, prototype
blocks, top21 extents, <=16773120B, query normalization, source/request/GT/SQ8
identities and reused f32 SQ8 mirror. No fit, seed/beam/GET/layout change.

Hypothesis: differing mean norms bias squared-Euclidean ranking toward diffuse
near-origin summaries; unit prototypes should rank angular direction for a
cosine workload. Existing unit-centroid routing already represents cosine
centroids this way; this check diagnoses the new candidate's geometry, rather
than inventing another metric or sweeping budgets. It does not retrospectively
change either prior negative terminal. Capture source-mean norm quantiles.

Synthetic assertion: raw distance prefers a long, worse-aligned mean; unit
normalization selects the shorter, better-aligned direction and stays invariant
to positive independent scales. Reject zero/nonfinite prototypes. Source-only
means still accumulate in f64 and store in f32. Unit squared-distance ranking
is the exact small-router reference, not an approved100M flat scan.

Frozen check: CoHere first100k D768 cosine GT100 development0–63 (64 used
queries), fetched mean>=98.9/p05>=96 before SQ8. Then returned mean>=98%,
p05>=95 and <=.5pp to paired exhaustive native SQ8. ReLAION only if CoHere
survives. First failure kills this arm before graph/format/cloud; no retuning.
Same feasible100M candidate resource envelope from prior plan, not measured.
No serving/cache/vendor/lifecycle qualification follows from a short screen.
One original existing Spark job,600s/4GiB,BLAS/OMP1, cachedNumPy2.3.3/PyArrow24.
