# Geometric code-group order implementation plan

**Spec:** `docs/superpowers/specs/2026-09-23-one-million-geometric-code-group-order-design.md`.

1. Add a deterministic source-only group permutation constructor and a
   query-only physical range evaluator. Keep logical membership and score
   authority separate from physical positions. Add an independent replay
   that reconstructs the permutation and the closed PQ96 baseline.
2. Extend the existing phase-separated Spot launcher for the one new sealed
   layout artifact, failure terminal, create-only uploads and readback. Run
   focused synthetic tests, Ruff and diff check; commit/push source.
3. Archive that exact source revision with sentinel and readback. Verify
   empty attempt prefix and no matching live compute; launch one Causality
   Spot attempt. Until terminal, inspect only terminal/infrastructure. After
   closure authenticate all artifacts, recompute the 1,000 sample metrics,
   check zero swap and termination, then record the fixed decision. A pass
   leads only to actual paired 100k PQ96 codes; a miss leads to a new routing
   signal or layout architecture, not a permutation sweep.
