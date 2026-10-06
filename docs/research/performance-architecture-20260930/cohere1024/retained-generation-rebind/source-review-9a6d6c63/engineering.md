**The head-publication repair looks correct by inspection. The two fixtures are meaningful, but some required rejection cases are not effective falsifiers yet.** All references below are to frozen commit `9a6d6c63`, not the changing worktree.

1. **P2 — The base-epoch test passes even if its guard is removed.**
   `crates/borsuk/tests/two_bit_generation.rs:1024–1079` creates only the changed root manifest. For `"base-epoch"`, removing the rejection at `crates/borsuk/src/two_bit_store.rs:736` still produces an error because the changed generation lacks its child metadata.
   **Minimal repair:** assert the precise `Invalid("retained initial semantic generation")` result, or populate the complete changed roster before testing refusal.

2. **P2 — Mutation refusal is tested through stale authority, not the mutation check.**
   `crates/borsuk/tests/two_bit_generation.rs:970–1008` captures `retained` before overwriting control. The changed ETag triggers `two_bit_store.rs:530–534` before the mutation check at line 569. Removing that mutation check therefore leaves this case passing.
   **Minimal repair:** for the mutation case, read a fresh head after writing mutation-bearing control, then require publication to refuse. `read_two_bit_head` permits obtaining that head.

3. **P2 — An identical raced head is not exercised.**
   The retry at `crates/borsuk/tests/two_bit_generation.rs:621–634` encounters an already occupied destination and exits at `two_bit_store.rs:715–722`. It never reaches a failed native `Create(head)`.
   **Minimal repair:** extend the existing wrapper to commit the identical head immediately before delegating the publisher’s create. Require `AlreadyExists`, preserved metadata, and independent reopen/search. This directly falsifies accidental success recovery for a competing identical publication.

These are release-test gaps, not demonstrated acceptance bugs in the current guards. They can be repaired within the existing two fixtures.

**Boundedness qualification:** the scratch guarantee depends on the documented source-immutability prerequisite. Admission uses captured sizes at `two_bit_store.rs:922–930`, but native copies at lines 984–990 have no source version/ETag condition; destination size is checked afterward. A source enlarged between authentication and copying can therefore exceed the admitted copied-byte total before rejection. The shared serving pass similarly re-observes JSON lengths (`object_native_generation.rs:508–531`). This does not invalidate the frozen immutable-assets use case, but the API does **not** enforce a hard scratch ceiling against concurrent source replacement. An extra HEAD check would not close that race.

The positive fixture provides substantial evidence once executed:

- Real D1024 semantic construction, copied LocalFileSystem assets, and original-tag refusal.
- Metadata-byte equality, non-whitelisted root-value equality, coefficient bits, and exact search IDs/score bits.
- Reopen/search after construction and old control/metadata deletion.
- An actual committed head followed by an injected lost response, with publication returning `Err` and preserved metadata remaining searchable.

The repair at `two_bit_store.rs:1033–1069` correctly marks the head attempted before awaiting create, propagates failure, and avoids deleting potentially committed metadata. I found no confirmed committed-head corruption or unauthorized-root acceptance defect under the stated immutable-source/backend assumptions.

Contract and owned-blob hashes match; committed `git diff --check` passes. **Native status remains UNVERIFIED** pending the root’s planned exact-source gates. No builds, experiments, edits, CLI review, or network activity were performed.
