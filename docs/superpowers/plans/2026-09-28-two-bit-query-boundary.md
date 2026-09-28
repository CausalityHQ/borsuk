# Cosine query boundary

Reproduce public plan failure for a large finite positive scaling of a valid
query. SQ8 scoring and centroid routing use normalized Euclidean geometry;
raw query scale must not leak into those stages. Existing generic f32 normalizer
underflows small values/overflows large values, so use codec-validated f64 norm.
Prepare the existing cosine lookup first; then normalize for graph and SQ8 so
normalization storage reuses admitted preparation scratch. Leave already-unit
queries (squared norm within1e-6) unchanged to preserve the frozen panel.

- [x] Focused red/green: scaled valid queries plan; zero/nonfinite/wrong width
  rejected. Apply boundary to public plan/search together.
- [x] Replay64 frozen CoHere development queries: parity FAILED (58 changed).
  Requests are raw unnormalized; V296 routing/scoring also used them raw.
  Treat normalization as a method correction requiring fresh qualification.
- [x] Seal source/check receipts and push. No layout/parameter/new dataset run.
