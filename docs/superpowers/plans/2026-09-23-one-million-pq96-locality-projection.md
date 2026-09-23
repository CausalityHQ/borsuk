# ReLAION-1M PQ96 Locality Projection Plan

**Spec:** `docs/superpowers/specs/2026-09-23-one-million-pq96-locality-projection-design.md`.

1. Add a fixed 96-byte group-length view over the sealed page-selector
   groups; leave routing scores and the 32-range planner unchanged. Test
   byte arithmetic, source seal equality, query containment, aggregate
   thresholds and the `row_bytes=96` marker. Independently replay every
   score, admission, interval and byte total.
2. Extend the tested Spot selector worker with a fresh `pq96` mode,
   namespace and terminal schema. Keep create-only source artifact writes,
   query capability boundary, unprivileged networkless evaluation,
   controller recovery, all-artifact readback and resource checks.
   Run focused tests, Ruff and diff check; commit/push the frozen source.
3. Archive with source-commit sentinel, upload create-only, read back,
   check active instance inventory and empty attempt prefix, then start
   exactly one Causality Spot controller. Monitor terminal/infrastructure
   only. On closure verify controller exit, instance termination, every
   artifact and independent 1,000-sample aggregation. Record the exact
   decision and next 100k PQ96 or routing redesign gate in the ledger.
