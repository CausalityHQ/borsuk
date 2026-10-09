# Review 2073fefb6d264b68
Candidate a07b979286293fdf5e6e205302b1808de3e09b84
Reviewer afb6558cd26446d5

# Review of a07b9792 against base 344740c8: HOLD on two blockers

I reviewed the committed blobs only. Nothing was built or run, and I edited nothing.

**Pins and integrity check out.** Ancestry is correct and the author/committer is the operator. The contract now pins a07b9792, and all six owned-file SHA-256 hashes match the committed blobs. All eight test names exist. The `authenticated_root` repair is correct: `two_bit_compaction.rs:589` now passes `base.metadata_prefix()`, which is what `two_bit_store.rs:141` requires. The other caller of that API in `two_bit_store.rs` (lines 730–731) was already correct.

## Blockers

**A. The Fresh1m maintenance refusal was removed, which lets compaction wedge after sealing.**
- **Where:** the guard `reject_unsupported_profile` and its test `fresh_profile_rejects_compaction_and_empty_replacement_before_side_effects` were deleted.
  - `admit_compaction_dimensions` (`two_bit_compaction.rs:441`) now admits Fresh1m at D768.
  - The pre-seal clamp at `:607-622` turns any row count of at least 1M into exactly 1M for Fresh1m, so admission passes.
  - `publish_empty_with_profile` (`two_bit_store.rs:1455`) now inherits or accepts Fresh1m.
- **Failure scenario:** a Fresh1m 1M/D768 index receives one delete or one new ID, then compaction runs.
  1. The pre-seal check sees 1M rows and passes.
  2. The mutation log is sealed at `:624`. Writers now get "mutation generation sealed".
  3. The job is pinned to Fresh1m. The exact merged count (999,999 or 1,000,001) then fails `valid_geometry` at `:779`, and fails the same way on every retry.
  4. Asking for Scale1m instead is refused (see blocker B).
  5. The only ways out are switching to Graph mode (whose build bound at 1M is about 1.5 GB) or using a new maintenance directory.
  6. Separately, delete-all now produces an empty Fresh1m root that can only be refilled with exactly 1M rows.
- **Why it blocks:** the contract says frozen historical behaviour is preserved. This lifts a frozen, tested refusal without listing it.
- **Smallest fix:**
  - Restore the Fresh1m refusal in three places, all before sealing or any side effect: in `compact_with_profile`/`compact_owned` for the base, requested and resolved profiles, and in `publish_empty_with_profile` for an inherited or explicit Fresh1m.
  - Restore the deleted test using the v4 schemas.
  - Scale1m already covers 1M/D768 generically.

**B. An explicit profile change is refused on a captured job that has not reached ready, even though that change is the only fix for a deterministic post-seal failure.**
- **Where:** the early check at `:551` (`requested_profile.is_some_and(|p| previous.profile != Some(p))`). Also `replace_mode` at `:650`, which only fires when the discovery mode changes.
- **Failure scenario:** a default Native100k index at 100,000 rows gets one new ID.
  1. The clamp at `:611` admits 100k rows, the log is sealed and the job is pinned to Native100k.
  2. The exact count of 100,001 is refused after sealing, every time.
  3. `compact_two_bit_index_with_semantic_profile(Scale1m)` then returns "compaction job changed".
- **Inconsistency:** on a captured, unready Graph job, `compact_two_bit_index_with_discovery(Some(Semantic))` is accepted through `replace_mode`. The profile API is refused on the same job, because `None != Some(p)`.
- **Already in base:** Native100k growth past 100k also failed after sealing in the base. But the new API is the advertised remedy, and the new clamp deliberately pushes the refusal past the seal.
- **Smallest fix:**
  - Drop the `requested_profile` clause at `:551`.
  - Change `:650` to: replace when `(requested.is_some() || requested_profile.is_some()) && (previous.discovery, previous.profile) != (job.discovery, job.profile) && !ready_path.exists()`.
  - Keep refusing once `ready.json` exists.
  - Update the test at line 2566: the unready phase 2 should expect a replacement; phases 1 and 3 keep "compaction job changed".
  - Alternatively, if pinning the profile at capture is intended, document that recovery needs a fresh maintenance directory and add a test for that path.

## Non-blockers

1. **Publisher change affects every caller.** `publish_two_bit_generation` now adds the old head's retained bytes (H) for any call with an expected head (`two_bit_store.rs:1135`), not just compaction. Compaction does not double-count it, since it passes caller pins + sealed snapshot + 262,144 bytes of bookkeeping (P+S+K). H is small (64 KiB or less), but any existing test with tight limits could flip.
2. **Remote read before validation.** `publish_empty_with_profile` reads the previous discovery profile (line 1464) before the namespace/order check (line 1488). A wrong-namespace head triggers a read of another prefix before being refused. Moving the read after the check fixes it.
3. **Implicit dimension check.** "Does this profile allow dimension d" is expressed as `valid_geometry(1, d) || valid_geometry(1_000_000, d)` (`two_bit_store.rs:87`, and again in `admit_compaction_dimensions`). It is correct for the three current profiles but fragile; a named helper would be clearer.
4. **Over-conservative disk check.** The pre-seal disk bound uses base rows plus all puts, unclamped. A 1M/D1024 index with 1M updates needs about 22.1 GB of disk to start, although the output is still 1M rows.
5. **The SQ8 "oracle" copies the implementation.** The test at lines 2747–2776 reproduces `sq8_source.rs:242-273` expression for expression (`max(1e-12)`, `round_ties_even`, sequential f32 norm). It pins the format but cannot catch a wrong formula. The score oracle likewise mirrors the sequential f32 order.
6. **D=1 rows tie by construction.** At D=1 every row normalizes to ±1, so the k=4 and k=2 assertions depend on every truncation stage breaking ties by id. The final sort does (`returned_sq8.rs:75`). If this fixture fails, suspect the tie before suspecting Scale1m.
7. **Fixed 48-leaf selection does not scale down.** The nominated fraction of the corpus falls as 1/N, so at roughly N ≤ 50–100k it is close to exhaustive. Small-N Scale1m recall or latency says nothing about 1M; any scale ladder should report nominated rows per query at each N.
8. **The 1M/D1024 operating point is very tight (model numbers only, no measured RSS).**
   - Router training peak is 506.7 MB, 94% of 512 MiB.
   - After the full build, only 13.1 MB is left for caller pins + old head + sealed snapshot (P+H+S). At D1024, where a put row is at least 4 KiB, that is roughly 3k puts per compaction, and likely a refusal with any serving generation declared alongside.
   - The 11.05 GB compaction disk bound exceeds the 8 GiB envelope.
   - A separate maintenance resource envelope should be preregistered.
9. **Pre-seal admission re-runs on ready recovery.** Tighter caps on a retry could block publishing a completed ready build.

## Checked and fine

- **Historical arithmetic is unchanged:** Native100k's 8→16 tie extension, Fresh1m's `== 48` checks, `seed_walk`, the old `selected_leaf_bytes` constants, `BLOB_CAP` applying only to Native100k, and the caps.
- **Profile authority is bound end to end:**
  - Empty root v4 and job v4 require the profile fields.
  - The ready file is bound to the job hash.
  - `validate_target` and the publish binding compare the profile; the router checks its profile against the input.
  - Old schemas (empty-generation v3, job v3, ready v1) are refused, and no stale schema literals remain anywhere in the repo.
- **Coexistence accounting is consistent:**
  - Retained memory is caller pins + old head + 262,144 bookkeeping bytes.
  - The effective mutation cap, `min(original cap, source cap − retained)`, is applied before any mutation read.
  - Publication charges caller pins + sealed snapshot + 262,144 bytes in compaction, and the publisher adds the old head.
- **The remaining-budget fixture tests the clamp itself:**
  - Without the clamp, the original 1,000,000-byte cap would have passed (needs about 397.5k).
  - With it, the allowance is 397,311 and the read is refused.
  - It checks the exact error string and zero operations on `/mutations/` paths.
- **The 2 MiB refusal fixture is well founded:** the router admission model has more than 12 MiB of fixed terms.
- **Likely to compile (not compiled):** exhaustive matches are updated, serde imports are present, and the test-helper signatures match.

Still unrun: the affected tests, the release build, workspace Clippy (correctness + suspicious) and the real shim-unset test compilation. Nothing here supports a 1M readiness or performance claim.

