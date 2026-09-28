# Existing dual review

Group `0122d55c38b34216`; both terminal, no replacement review launched.

## research_critic (claude-opus-5-5, 084bad983158434d)

**Verdict: one commit blocker, which is payload admission, plus one small companion fix. Reclamation safety holds within the stated single-host, same-directory contract.**

I did not re-hash the evidence files or read the gzipped logs. I relied on your statement that the source hashes match.

## Blocker: payload admission runs after the durable fence is taken

- `two_bit_gc.rs:187-199`: GC admission only checks `mutations.max_memory_bytes + 524288 <= max_memory_bytes`. It never runs the mutation cap check, `two_bit_mutations::admit` (`two_bit_mutations.rs:184-195`: `max_snapshot_bytes*12 + 4096 <= max_memory_bytes`, `max_snapshot_bytes >= HEADER`).
- That check first runs inside `decode` (`two_bit_mutations.rs:208`), which is called from `validate_mutations` (`two_bit_store.rs:514-540`). That happens in `keep_set` (`two_bit_gc.rs:160`), after `begin_two_bit_write_fence` has already committed the fence (`two_bit_gc.rs:205`, `two_bit_store.rs:567-573`).
- A fenced head makes `read_two_bit_head` fail (`two_bit_store.rs:161`). That blocks new `open_coordinated` readers, mutations and compaction.
- **Failure:** a pending mutation exists and the caller passes a mutation cap that violates `admit`, or a real snapshot larger than `max_snapshot_bytes`. The call returns `Err`, the fence stays in place, the index can no longer be read or written, and retrying with the same limits fails the same way forever. The product API has no way to abort. The only escape is the low-level `end_two_bit_write_fence`, which the contract says the caller must not use against an active fence.
- The same outage follows from any transient HEAD/GET error in `keep_set` (`two_bit_gc.rs:108`, `:145`, `:151`). In that case zero objects were deleted, so keeping the fence protects nothing.

**Minimal fix:**
1. Make `admit` `pub(crate)` and call `admit(limits.mutations, 0)` inside the GC admission block, before the lock and fence.
2. In `collect_two_bit_garbage`, if `keep_set` or anything else fails while `report.delete_attempts == 0`, call `end_two_bit_write_fence(&fence)` before returning the error. This is safe: nothing was deleted, and the epoch advanced twice, so any writer that read the head before the fence still fails its CAS.
3. Add one test: a pending mutation plus a too-small `max_snapshot_bytes` gives `Err`, and `read_two_bit_head` still returns `Ok` afterwards.

Keep the retain-on-failure behaviour for failures after the first delete attempt, as the plan says.

## Checked and sound (no action)

- **Writer epoch argument:** every commit is a CAS on `head.json`'s version (`publish_head`, mutation commit). The fence advances the epoch (`two_bit_store.rs:570`), and fenced reads refuse (`:161`, `two_bit_mutations.rs:339`). So a writer that uploaded or checked an object before the fence always fails its CAS afterwards. This holds for writers on other hosts too; readers are the only part that stays single-host.
- **Reader and compaction exclusion:** `open_coordinated` keeps the shared lock for the index's lifetime (`two_bit_index.rs:39,45`). Compaction takes the shared lock (`two_bit_compaction.rs:269`) and GC takes it exclusively (`:239-242`). The test covers the pinned-reader refusal.
- **Keep set matches the listing:** `metadata_location` splits on `/` (`object_native_generation.rs:168`), so `plane/*` keys compare equal to what LIST returns and are matched by `recognized`. SQ8 identity (size + ETag) and canonical size are checked before any delete. The compaction job's `mutation_sha256` is the sealed current state, which GC keeps.
- **Compaction rebuild after GC:**
  - If GC deleted the SQ8, `two_bit_compaction.rs:392-406` removes `ready.json` and rebuilds under a new owner UUID. The test covers this.
  - If a capped pass deleted the target canonical or metadata but kept the SQ8, the ready path still publishes. That is safe because `publish_two_bit_generation` re-uploads the canonical and all metadata with unconditional multipart (`two_bit_store.rs:352-376`, `resident_graph_store.rs:301-313`) and re-checks the SQ8 ETag first (`:337`). An explicit test for this case would be nice, not required.
- **Claim ordering:** `claim.json` sorts before `objects/`, so a pass that hits a cap can leave objects without their claim. They are still recognized and deleted on the next pass. Nothing reads claims after creation.

## Not blockers

- **Recovering a fence** (`two_bit_store.rs:567`) adopts any existing fence. A second GC run from another directory or host would share it and could release it mid-scan. That is outside the contract and is old fence code.
- **Retaining the fence doesn't cover ambiguous deletes:** a retry releases the fence straight away, so an ambiguous delete that lands late could still remove a content-addressed key that was re-uploaded after release. This only needs a one-line doc caveat; the current wording implies the retained fence gives more protection than it does.
- **Scan cap:** because each call restarts the scan from the prefix, kept and unknown keys can use up `max_objects_scanned` so GC never reaches the rest. It is already marked `ponytail:` and reported through `scan_complete=false`.
- **`keep_object` doesn't check that keys are under the prefix:** it only ever adds to the keep set, so this is harmless.

## engineering_critic (gpt-6-astra, 64757030c5d0451f)

Found two concrete issues to fix before commit:

1. **P1 — Mutation payload admission happens after allocation.** [two_bit_store.rs:520](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/two_bit_store.rs:520) downloads the snapshot using `max_snapshot_bytes`; memory admission runs only afterward inside `decode`. GC’s [initial admission](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/two_bit_gc.rs:192) checks the declared mutation memory allowance, without validating its relationship to the snapshot cap. A large snapshot cap and small memory allowance therefore permit allocation far beyond the advertised budget before rejection—and leave the index fenced.

   **Minimal remedy:** reuse mutation admission before acquiring the durable fence or fetching payloads. Add a regression asserting inconsistent limits cause no remote I/O or fence change.

2. **P2 — Capped GC can remove a namespace claim that resumed compaction never restores.** [GC recognizes claims independently](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/two_bit_gc.rs:82), so a capped pass can delete an uncommitted namespace’s `claim.json` while leaving its SQ8 object. [Compaction recovery checks only SQ8 existence](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/two_bit_compaction.rs:394), accepts the ready generation, and publishes without restoring the claim. Subsequent GC merely puts the nonexistent claim in its keep-set. This breaks the stated namespace-claim invariant; it is not evidence of immediate query corruption.

   **Minimal remedy:** require the matching claim when accepting recovered ready state; rebuild under a fresh namespace if absent. Add a claim-only capped-sweep regression. The existing full-sweep test misses this intermediate state.

Source hashes and the decompressed terminal-log hash match the supplied evidence. Recorded checks pass, including native S3 IDs `[1,2,99]`; closeout records the instance terminated. I ran no tests, builds, jobs, or consultations and made no edits. No additional reclamation-safety blocker found within the stated cooperating single-host contract.
