# Opus5.5 implementation review 6387b158c14946c9

Completed static read, not test evidence. Frozen d7279f candidate; subsequent test cap changed1MB ->4MB, production sources unchanged.

I found no Critical or Important bugs. This was a static read of the dirty diff against `a4027d8e` plus the new files; I built, tested and consulted nothing, and didn't touch the Spot instance.

## Causal trace (what I checked)

- **Epoch source.** Every control commit calls `HeadBody::advance()` (`two_bit_store.rs:57`). Starting a fence advances the epoch and releasing it advances it again (`two_bit_store.rs:633-676`). Resuming an existing fence keeps the same epoch. `mutation_authority` refuses a fenced control (`two_bit_mutations.rs:340`), so no publisher can capture the fence epoch itself. Every key a GC sweep sees, including a DELETE still in flight from an earlier interrupted pass, has an owning epoch below the fence epoch. Every later publication needs the epoch after release.
- **Populated publication** (`two_bit_store.rs:344-386`). The sealed authority is captured first. Then it checks `manifest.base_epoch == authority.epoch` and runs `validate_owned_object` on SQ8, and on canonical when its owner differs from SQ8's. Only after that come the canonical and metadata PUTs, then the final CAS against that same captured version (`publish_head`). A GC that starts during the uploads makes the CAS fail. A retry then captures the newer epoch and rejects the stale manifest.
  - **Root IDs:** `base_epoch` is inside the root manifest bytes (`two_bit_build.rs:206`), so the root SHA and every `generations/<root>/…` path are unique to one epoch.
- **Physical guard** (`two_bit_store.rs:285-324`). Owner is 48 hex characters, and `owner[..16]` must equal the captured epoch. The object must be `objects/<sha>`. The claim must be job v2 with the same `index_prefix` and `base_epoch`. The epoch lives in the path, so rewriting a claim cannot relabel an old key.
  - SQ8 is covered directly. Canonical is covered either directly or through the same owner (same owner means same claim).
  - A foreign `/maintenance/` path is rejected.
  - Other keys must end in `/objects/<sha>` (`valid_object_key`). GC's `recognized()` never matches those outside `maintenance/`, so GC never deletes application-owned refs.
- **Empty root** (`two_bit_store.rs:540-545`). `base_epoch` comes from the captured authority. The digest differs per epoch, and it is written with a create-only PUT.
- **Mutations** (`two_bit_mutations.rs:521`). The epoch is written into the snapshot bytes before hashing, and the PUT is create-only. `decode` rejects epoch 0 and the old `BTMUT001` magic.
- **Compaction** (`two_bit_compaction.rs:356-385, 516-530`). The job epoch is captured after the seal. The owner name is the epoch plus a UUID, and the claim is a create-only PUT of the job bytes. If a resumed job finds a newer epoch, it wipes the job directory and rebuilds with a fresh owner. `old_epoch >= new` and any other job difference are both rejected.
  - Empty compaction checks the recomputed digest against `ready`.
  - GC and compaction in the same directory exclude each other through the exclusive or shared `lifecycle.lock`.
- **GC** (`two_bit_gc.rs`). Caps and mutation admission are checked before the lock and before any store I/O. It takes the exclusive lock non-blocking.
  - The keep set covers `head.json`, the root and metadata, SQ8 and canonical with their `claim.json`, and the current mutation snapshot. It verifies the root SHA, the SQ8 size and ETag, and the canonical size.
  - If the keep set fails, it ends the fence (safe even on a resumed fence, by the epoch argument above). A delete failure leaves the fence durable.
  - Deletes, bytes and scanned objects are each capped.

## Minor (no fix required before delivery)

1. **Stale comment** at `two_bit_compaction.rs:431`. It says GC can reclaim an uncommitted claim or SQ8 at the same epoch, but GC always advances the epoch, so that case goes through the job-epoch rebuild at `:370-380`. The `NotFound` branch is only reached by external deletion. Fix: reword the comment.
2. **GC can stall** (`two_bit_gc.rs:215`, already marked with a `ponytail:` note). Each call rescans from the start of the prefix. If more kept or unknown objects than `max_objects_scanned` sort ahead of the garbage, repeated calls reclaim nothing, and `scan_complete` stays false. This is a progress issue, not a safety one.
3. **Old owners are never collected.** Maintenance owners from before this change (32 hex) are not recognized, so GC never deletes them. That matches the no-compatibility policy, but existing experimental prefixes will need to be discarded by hand.
4. **Loose claim binding.** The publisher accepts any job v2 claim with a matching epoch and prefix; it isn't tied to this manifest's mutation or root. That's harmless for delete safety, because the epoch in the physical path is what fences it.

## Verification limits

- **Not read in full:**
  - `TwoBitGeneration::open` manifest-key validation
  - `upload_authenticated_file`
  - `commit_control`
  - the rest of the compaction publication tail after `:600`
  - the bodies of the new tests (I only confirmed which assertions exist: `"prepared generation epoch changed"`, `"maintenance owner epoch"`, the three delayed-DELETE cases and the admission-before-I/O cases)
- **Not checked:** the HTTP and server callers.
- **Not verified:** the archive hash `d7279f14…` or the `source-parity.json` match.
- **Not re-examined:** the 52 retired-default fixture failures.
- **By design:** safety depends on the declared contract — one host, one directory, callers preparing only after reading the sealed epoch, never guessing a future one (the builder's `base_epoch` is caller-supplied), and external objects being immutable.
