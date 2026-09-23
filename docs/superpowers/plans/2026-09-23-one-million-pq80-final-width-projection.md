# ReLAION-1M PQ80 Final Width Projection Plan

**Spec:** `docs/superpowers/specs/2026-09-23-one-million-pq80-final-width-projection-design.md`.

1. Refactor the existing source-only range projection to accept only its
   frozen 200-, 96- and new 80-byte row widths, with distinct canonical
   schemas and decisions. Test exact group byte lengths, interval and
   quality replay for all three without changing previous defaults.
2. Add a `pq80` phase-separated Spot mode with prior source-seal SHA
   checks, create-only artifact writes/readback, unprivileged networkless
   evaluation, controller interruption recovery and the same 3-GiB/
   zero-swap gate. Run focused tests, Ruff and diff check; commit/push.
3. Archive with source sentinel, create-only upload and readback. Check
   active instance inventory and empty attempt prefix, launch one
   Causality Spot instance, then observe terminal/infrastructure only.
   After closure authenticate every artifact, controller exit and
   termination, independently aggregate all 1,000 samples, then record
   the fixed decision and actual 100k PQ80 or routing redesign gate.
