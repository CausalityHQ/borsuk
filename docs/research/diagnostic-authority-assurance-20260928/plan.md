# Explicit diagnostic authority qualification

Base 7890cee6. Eleven fixture locations require the research V20 authority which is intentionally not a production default. Add explicit calls to the existing test-only diagnostic builder after bulk finalization, including both shared fixtures. Preserve all existing assertions and production behavior. No dependency, compatibility path or restored default.

Seventeen named checks in cases.txt cover codebook binding, counters, RAM overflow, warm-cache durable validation, transactional refresh, retained-manifest GC, deterministic layout, read-only V21/V22/V23 diagnostics and authenticated corruption rejection. Sixteen previously failed; one existing preload test also uses a changed shared fixture.

One frozen-source AWS causality eu-central-1 c7i.2xlarge Spot worker; four Cargo jobs, 30-minute wall cap, 20-minute test cap and $.30 estimated compute cap excluding EBS/S3. Sequential named checks continue after individual failure to collect all results, then fail the gate if any fail. No local Cargo, repeated full gate, performance campaign or duplicate jobs. Preserve terminal artifacts and terminate worker immediately; interrupted work discarded with no automatic replacement.
