# Mutation fence — GO for the primitive only

2026-09-28 UTC; source parent `9f3890eb`. Required release comparison remains BOTH S3 Vectors and Turbopuffer; no matched axis passes from these functional checks.

## Delivered

`seal_two_bit_mutations` reuses the bounded complete-state delta and CAS publication. Mutation head schema v2 requires a sealed flag; v1 rejects. Sealing creates an empty closed snapshot if there is no head, or advances the complete latest state when present. Reads preserve all states. Observed closed snapshots reject writes before staging; stale and initial tokens fail CAS. Idempotent sealing requires authenticated closed-state readback. Lost seal acknowledgements cannot be accepted from an equal-revision/equal-SHA **unsealed** concurrent no-op put.

`publish_two_bit_generation` refuses replacement until the old per-generation mutation head is sealed, before any upload. Initial create is unchanged. Low-level publisher does not prove that target corpus includes sealed mutations; caller still owns that obligation. ACLs authorize writers; content hashes do not substitute for authorization. Irreversible seal means crash-before-replacement leaves queries readable but writes paused until compaction resumes.

## Verified original jobs

Existing Spark only, two Cargo build workers and 600s bound per original invocation. Missing API red exit101; first implementation compile exit101 due to misplaced test setup; corrected focused two integration tests exit0; affected real local HTTP four tests exit0. Final touched source/test SHA parity checked with Spark. Raw logs compressed deterministically with decompressed SHA and exact exits retained in verification.json. No paid/cloud/quality run. No stale local BORSUK build; local memory PSI avg10/60/300 zero. Existing review cooldown unresolved; no duplicate or override.

Checks cover absent and nonempty fences, lost commit acknowledgement, same-body unsealed competing writer, prior schema rejection, pinned state recovery, idempotence, post-fence stale/current/initial write rejection, zero PUT on observed closed token, unsealed replacement rejection and successful sealed publication/reload. Small D2/512-row fixtures are correctness tests, not ANN recall/latency evidence.

## Decision and next single gate

GO: use this fence for the full in-process compactor. OPEN: stream authenticated canonical rows, apply complete sealed puts/deletes, preserve signed IDs, build/publish replacement and resume safely after failure. Empty all-deleted index semantics and active-reader-safe GC remain required. Do not unseal or publish a corpus that omitted acknowledged mutations. No vector hydration for queries, no new maintenance service.

Quality redesign and independent matched vendor runs, 100M bounded-RAM/lifecycle qualification remain unresolved release gates. This primitive establishes no speed or vendor win.
