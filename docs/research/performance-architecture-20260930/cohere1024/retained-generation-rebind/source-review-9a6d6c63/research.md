# Review of retained-generation republication at 9a6d6c63 (base 3c652b1a)

I read only the committed blobs (`git show 9a6d6c63:…`) and the plan and method docs at dbffad79. The contract snapshot SHA matches `8dd58dbf…955d`. I made no repo edits and ran no Cargo, rustc, tests, data or network. Native behaviour is still **UNVERIFIED**. I did save one review note to my memory folder, outside the repo.

## Verdict

I found **no must-fix code bug** for the stated scope: an initial semantic generation republished on LocalFileSystem. The trust chain holds: approved root SHA → manifest → `sq8_object_sha256` → the full SQ8 body authenticated under If-Match on the approved ETag.

Things that check out:
- Every body is authenticated with exact SHA, length and EOF, before and after copying.
- Payloads stay in place and only the root's SQ8 key/ETag fields are patched; the patched root is written create-only, with `head.json` last.
- Source control is rechecked at the start and again just before the head is written.
- `head_attempted = true` is set before the head create is polled (`two_bit_store.rs:1033`). Cleanup only runs when no head was attempted (`:1067`).

The copied roster matches what semantic serving reads: `Discovery::files(false)` gives the same 10 files, centroids excluded.

The ETag "approval" adds no integrity on its own. The method doc reads it from the same store after authentication; the SHA is the real authority. That is fine as long as the CLI takes `root_sha256`, generation and epoch from the frozen original receipts (`4ec270e5…`), not from the copied head.

## First-principles finding: on LocalFileSystem, the "copy" is a hard link

In object_store 0.14.1, `copy_opts(CopyMode::Create)` calls `std::fs::hard_link(from, to)` (`object_store-0.14.1/src/local.rs:678-683`, `:970`). So the destination metadata files share inodes with the retained source.

- This is correct for object_store's own `put` and `delete`, which use rename and unlink. Your "reopen after deleting old metadata" check holds with link semantics.
- **Runbook constraints:**
  - The destination must be on the same writable filesystem as the retained assets. Otherwise you get EXDEV or EROFS; treat that as environment INVALID.
  - Any in-place write to a source file outside object_store (`cp` over it, `rsync --inplace`, truncate) also changes the published destination. Treat staged retained assets as write-protected after publication.
- Destination copies consume no extra data blocks, so the scratch admission is conservative here.

## Fix before freeze (cheap; these close evidence gaps, not observed bugs)

1. **The "coefficient f32 bits preserved" claim is not checked on the real root.**
   - The new root is a struct round-trip (`two_bit_store.rs:963-968`). serde_json is used without `float_roundtrip`, so f32 values decode via f64.
   - The fixture uses constant `low = -0.0` and `step = 1/255` (`tests/two_bit_generation.rs:163-164`), so it cannot falsify this for the real 2,048 coefficients.
   - The planned ABBA parity compares runs on the **same republished root**. It cannot see a change introduced by republication.
   - Actual risk is tiny (on the order of 1e-6 per root), but nothing downstream would detect it.
   - **Minimal repair:** after `:968`, re-parse `new_root` as `Manifest` and require `low`/`step` `to_bits()` equality with `manifest`. All other fields are integers or strings and round-trip exactly. Don't use a raw byte comparison: a field-order difference from an older writer would cause false refusals.

2. **Ownership is validated against the wrong prefix** (`:758-760`).
   - Keys are checked with `validate_owned_object(store, &retained.prefix, key, 0)`. That accepts epoch-0 maintenance keys owned by the *source* index.
   - The normal publisher at the destination (`:1166`) would refuse such a key as "foreign maintenance owner".
   - **Repair:** pass `destination` instead. That is equivalent to rejecting any `/maintenance/` key and restores parity with normal publication.
   - Reachability is low, but confirm the real Cohere SQ8 and canonical keys contain no `/maintenance/`.

3. **The serving validator opens the source root, not the root the head will publish** (`:945`).
   - **Repair:** move it rather than add one. Call `TwoBitGeneration::open_remote(store, &new_prefix, &new_sha, …)` after the metadata creates and before `recheck_retained_control` (`:1021`).
   - Same cost, and it validates exactly the bytes the head will point to.

## Are the two fixtures meaningful?

**Fixture 1 (`:321-695`) — yes, it is a real falsifier.**
- It shows the stale-ETag search failure is real (`:364`) and that the stale approval is refused before any write (`:372`).
- Every metadata file is byte-identical, source objects are unchanged, and returned IDs and score bits match.
- It covers the ETag-only case with the same key.
- The lost-response sub-test would catch cleanup running after the head attempt, because the committed head must still open after the original store is deleted (`:674-694`).
- Weaknesses:
  - The constant coefficients (item 1 above).
  - The 64-row, single-page geometry. This is acceptable because byte equality carries the metadata claim.
  - `matches!(Generic{..})` at `:593` is not unique to the injected failure, since LocalFileSystem I/O errors are also `Generic`. The committed-head read at `:599` makes the test specific anyway. Optionally match `store: "LostHeadResponse"`.

**Fixture 2 (`:698-1202`) — meaningful for corruption, page schema, occupancy and pre-head cleanup.**
- The SHA flips on `records.bin` and `leaves.bin` are good: they are caught only by this API, since serving startup never reads those bodies.
- The mid-copy control race tests pre-head cleanup and checks the destination lists empty. It also acts as an implicit positive control: if the publisher failed before copying, the race would hit its 10 s timeout.
- Gaps:
  - Most faults assert only `is_err()`.
  - The `mutation` and `fence` faults read `retained` *before* changing control (`:971`). They are therefore refused by the ETag check (`:534`), and the field predicates (`:569-570`) never run. Repair: for `mutation`, read `fixture.head()` after the write so the ETag matches and the field check must refuse.
  - The nesting predicate (`:704`) is untested. A destination like `retained/index/x` passes the occupancy list, so add that one fault.

## Deferred (not blockers)

- **Uncharged list page.** `list(Some(destination)).next()` (`:716`) loads a whole first page into memory: up to 1,000 S3 entries or a 1,024-entry local walk, roughly 2–3 MiB. This only happens when the destination is occupied, which is then refused. Charge a constant or document it.
- **Error cannot tell the caller a head was attempted.** Pre-head and post-head errors share the `Store(..)` variant. For now, the CLI should treat any error as "maybe committed":
  - read the destination head;
  - accept it only if its root decodes to the retained manifest with just the approved SQ8 key/ETag changed;
  - otherwise use a fresh destination.

  A future `HeadAttempted { root_sha256, source }` variant would remove the guesswork.
- **Generation number is no longer unique.** Source and destination share it. Receipts should be keyed by root SHA plus head ETag.
- **Shared objects are unprotected from source GC.** The destination references the SQ8 and canonical objects, which may live in the retained namespace, so source-side GC must not run while the destination is live.
- **Root cause.** The design keeps a transport ETag inside a content-addressed root. `records.bin` and `leaves.bin` already bind their ETag by HEAD at open (`two_bit_generation.rs:880-915`). A v9 schema that HEAD-binds SQ8 the same way would make this API unnecessary for future copies and restores. It can't land now because the old binary in the ABBA replay must read v8.
- **S3.** Create-only copy needs the copy-if-not-exists capability, and NotSupported is environment INVALID, as you stated. Multipart-copy ETags pass `retained_etag`.
