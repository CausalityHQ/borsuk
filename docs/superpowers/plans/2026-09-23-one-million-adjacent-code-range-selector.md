# ReLAION-1M Adjacent Code-Range Selector Plan

**Spec:** `docs/superpowers/specs/2026-09-23-one-million-adjacent-code-range-selector-design.md`.

1. Add one fixed range planner over source-only page-centroid group rankings.
   Test isolated and bridging interval admission, byte-limit skips, stable
   ties, role boundaries, full-order scan, and agreement with the prior
   32-singleton page selector when only 32 groups can be admitted.
2. Add canonical per-query evidence and aggregate quality/budget decision.
   Rebuild the source-only page artifact before queries become available;
   assert its three defining SHA-256 values match the completed prior
   screen. Add an independent reducer that recomputes all 1,000 plans
   without calling the producer planner.
3. Extend the tested Causality Spot selector worker to a new `range` mode,
   output namespace and terminal schema. Keep create-only writes,
   source/query separation, controller interruption terminal, artifact
   readback and fresh-attempt support. Test shell syntax/size and failed
   terminal handling. Run focused tests, Ruff and diff check.
4. Commit/push the source after fast-forward ancestry check, archive with
   the source-commit sentinel, upload create-only and verify readback.
   Check active instance inventory and empty attempt prefix, then start
   exactly one Spot session. Observe only terminal/infrastructure health.
5. After terminal closure, collect original controller exit, prove instance
   termination, read back all artifacts, independently aggregate all 1,000
   samples, record the fixed decision and next gate in the ledger, and
   commit/push that evidence.
