# Page-dispersion mass routing implementation plan

**Spec:** `docs/superpowers/specs/2026-09-23-one-million-page-dispersion-mass-routing-design.md`.

1. Pin `scipy.special.ndtr` for this experimental runner. Add source-only,
   two-pass float64 page moments, sealed counts/arrays and fixed 48-step
   group-mass scoring. Keep the original physical planner. Test moments,
   ranking, baseline reproduction and source/query boundary on synthetic
   inputs before a remote run.
2. Add independent construction/scoring/plan replay and Spot mode with a
   tenth sealed moments artifact. Run focused tests, scoped Ruff and diff
   check; commit and push the exact source revision.
3. Archive create-only with source sentinel and readback. Verify the full
   attempt prefix is empty and no matching instance lives, then run one
   Causality Spot attempt. Observe terminal and infrastructure only while
   incomplete. Authenticate terminal artifacts, recompute 1,000 paired
   samples, verify resource/termination gates, record the fixed decision,
   and advance only to actual 100k codes on a pass.
