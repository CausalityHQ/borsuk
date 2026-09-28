# Full workspace gate passed on x86 Causality Spot

Frozen source cf329c81b6518d7997b3c1dd7832a08bd0b06dfa0358941c64707c496684a685, base1643c471. First focused historicalV26 library87passed/0failed. Then cargo test --locked --workspace --all-targets --jobs4 passed:146target summaries,2682passed,0failed,26ignored. Initial87check repetition excluded from full-gate totals; exact per-target counts in counts.json. Existing ignored checks were not newly skipped by this repair.

Both repaired fixtures execute the real historical runner on x86, assert exact AArch64-only unavailable error, and verify no evidence artifact was emitted. Corrupt query identity still rejects without output. Original ARM positive evidence/depth/warmup/claim assertions retained; ARM execution/performance is not measured on this x86 worker. No production runner, arithmetic, format or historical measurement changed.

Independent verification: all3terminal artifact hashes/lengths, changed Rust file against frozenS3archive, terminal exit0 and EC2 terminated. i-0128209956ceb8d2e terminated after897seconds. Compute estimate$.0530 from observedSpotquote$.2128/hour excludesEBS/S3, not invoice. No active assurance worker remains.

This closes the tracked workspace assurance failure chain, including decoder admission, current native bulk finalization and portable PQ4. It does not close ANN quality, native cosine qualification, fresh query identities, live cold latency, RSS/generation-swap, total lifecycle cost, scalable build/maintenance or either matched vendor comparison. All production-routing KILL decisions remain; native goal stays active.
