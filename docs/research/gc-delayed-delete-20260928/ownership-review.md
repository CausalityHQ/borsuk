# Targeted Opus5.5 review cb888918e2944f6e

Read-only repository review completed; not verification evidence. Independently check findings before implementation.

**Verdict:** the tombstone design would stop delayed DELETEs from destroying acknowledged generations on every existing publication path. As written, though, it has four gaps, and one of them eventually jams GC. A cheaper design that is equally correct also exists: bind every staged root and owner to the control epoch at seal, with no tombstones at all. I recommend that one.

This review is from reading the code only. I did not run builds or tests, did not start a consult, and did not touch the green worker `i-0528a95c860f00784`.

## Paths that are unsafe today (confirmed by tracing)

1. **`publish_two_bit_generation` retries** (`two_bit_store.rs:293,359,375`). The head is re-read at the start of every call, and the head CAS is conditional on that fresh read, not on the caller's `expected`. GC keeps the sealed state and the root, so a retry after GC passes, re-uploads the same `generations/<R>/*` files and canonical object, and commits. A delayed DELETE then removes a committed object. The same applies to direct callers of the API and to compaction's `ready.json` resume.
2. **Compaction resume check** (`two_bit_compaction.rs:394-417`). It only checks that the SQ8 object and claim still exist. An ambiguous DELETE that is still pending does not show up in that check, so it passes and publication goes ahead.
3. **Empty root** (`two_bit_store.rs:492`). The key depends only on generation and dimensions, and "already exists with identical bytes" is accepted. The compaction target computes the same key, so a retry after GC reuses it.
4. **Mutations are safe.** The epoch goes into the snapshot key (`two_bit_mutations.rs:523`) and the CAS uses the caller's version, so any GC fence in between changes the key or fails the CAS. They need no tombstones.

## Is the tombstone proposal sufficient?

The core ordering holds. With (capture CAS token → check tombstones → stage → final CAS) on one side, and (fence → write tombstone → DELETE → unfence) on the other, three cases cover everything:
- GC finished before the capture, so its tombstones are visible (S3 reads its own writes).
- GC started before the capture, so the fenced head rejects the publisher.
- GC started after the capture, so the final CAS fails.

This relies on the existing paths already capturing the CAS token before staging, which I checked (`two_bit_store.rs:293,467`).

These gaps would need fixing first:

- **GC would eventually stop completing.** GC lists the whole prefix (`two_bit_gc.rs:217`). `reclaimed/` keys are not recognized, so they count against the scan cap forever and full scans become impossible. GC would have to list only `generations/` and `maintenance/`.
- **Mutation snapshots must be exempt.** Every mutation batch leaves the previous snapshot as garbage. Tombstoning those means one permanent object per write batch. They are already safe through the epoch.
- **A rejected `ready.json` has to be discarded.** If publish rejects a retired key, compaction must delete the job and `ready.json` and rebuild with a new owner. Retrying or returning an error would jam compaction permanently. The same applies to the `"compaction job changed"` hard error (`two_bit_compaction.rs:369`).
- **Tombstone write failures.** A DELETE may only be sent after the tombstone write is acknowledged. An ambiguous tombstone write must end the GC pass with the fence still in place.

**Cost.** One PUT per deleted key: about 10 per retired generation (7 metadata files, SQ8, canonical, claim). About 10 extra HEADs per publish. Permanent objects that grow with history, which breaks the earlier WAL3 gate C ("object count grows with live data only"). Tombstoning per root and per owner instead of per key cuts this to 2 PUTs and 2 HEADs, but objects still grow with history.

## Cheaper alternative: epoch binding

Once mutations are sealed, the only thing that moves the control epoch is a GC fence. A publish CAS would also move it, but that retires the job anyway. So "the epoch is unchanged" proves no GC ran in between. The changes:

- Add `base_epoch` (the sealed epoch, or 0 for initial creation) to `Manifest`, `EmptyRoot` and the compaction `Job`. The job doubles as `claim.json`, so the owner is bound too.
- In both publishers, reject **before staging** when the captured epoch is not equal to `base_epoch`.
- Because the root digest now includes the epoch, every `generations/<R>` and empty-root key is unique per epoch. Retired keys can never be committed again.
- For SQ8 or canonical keys under the maintenance area, publish reads `claim.json` and requires its epoch to equal `base_epoch`. That one read replaces compaction's current existence checks, which can then be deleted, so the request count is unchanged. It also stops direct callers from reusing a retired owner.
- On a mismatch, compaction discards the job and rebuilds with a new owner.
- `TwoBitHead` or the sealed snapshot needs to expose the epoch, crate-private for compaction and public for direct builders.

**Cost:** no extra storage, no extra requests, no change to GC. The only downside is a forced rebuild when GC runs between a crashed compaction and its resume. GC would have deleted that unreferenced SQ8 and claim anyway, so almost nothing is lost.

**Unchanged from your proposal:** the trusted same-host lifecycle directory, a single head writer using CAS, and no support for readers on other hosts that aren't registered.

**Smallest falsifier tests** (same fault-injecting store as `two_bit_gc_delayed_delete.rs`):
- populated retry after GC,
- empty-root retry after GC,
- compaction resume after an ambiguous SQ8 DELETE,
- a direct publish that references a retired owner.

I saved these conclusions to memory (`gc-delayed-delete-epoch-binding-review.md`).
