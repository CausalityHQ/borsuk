# 100k OPQ8 row-router implementation plan

**Spec:** `docs/superpowers/specs/2026-09-23-hundred-thousand-opq8-row-router-design.md`.

1. Build the source-only OPQ8 trainer/encoder with deterministic selection,
   farthest-first initialization, eight alternating Lloyd/Procrustes steps,
   float32 sealed model and physical eight-byte row codes. Add a source-only
   100k group planner using ADC top-four scores and the existing exact
   four-page group byte roster. Add an independent replay and synthetic
   tests for ties, code ownership, serialization and byte/read caps.
2. Add a phase-separated Spot wrapper with source/query separation,
   create-only artifacts and terminal/readback, process-tree resource gate
   and interruption handling. Run focused local tests and scoped lint,
   commit/push, archive with source sentinel and readback.
3. Verify no live matching instance and an empty full attempt prefix; run
   one Causality Spot attempt and observe terminal/health only until it
   closes. Authenticate and independently aggregate all 1,000 samples.
   Stop on a containment failure; only a pass authorizes the separately
   preregistered actual two-bit scorer cell. Do not launch it concurrently.
