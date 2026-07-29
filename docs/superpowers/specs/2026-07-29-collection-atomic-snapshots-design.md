# Collection-Atomic Snapshots and Multimodal Transactions

**Status:** Approved by the user's 2026-07-29 instruction to make BORSUK
production-ready without preserving unreleased on-disk compatibility.

## Context

A BORSUK collection can contain a primary vector, named dense vectors, named
sparse/text vectors, and late-interaction matrices. Sparse and text state is
stored with the primary index, while named dense and late-interaction fields
currently use child `BorsukIndex` instances below `vectors/<name>/`.

Each child currently owns an independent `CURRENT` manifest pointer and an
independent cell-WAL commit marker. Public mutation, refresh, flush, and
compaction methods advance those indexes sequentially. A crash or object-store
failure between child operations can therefore expose only part of a logical
row or retain only part of a delete. A refresh failure can also leave an
in-memory handle containing manifests from different logical points in time.
Compensation cannot make these operations atomic because another reader may
observe the partial state before compensation completes.

BORSUK is unreleased. Old experimental collection layouts will be rejected
instead of migrated or supported.

## Decision

The root collection owns the only visibility boundary. Immutable modality
objects are prepared independently, but one checked root control object pins
the exact state of every participating modality. Readers never infer a
collection snapshot from child `CURRENT` pointers or child-local WAL commit
markers.

There are two root-level protocols:

1. `collection/CURRENT` points to an immutable collection snapshot that pins
   the exact primary and named manifest objects.
2. `collection/transactions/<id>/COMMIT` pins every modality-local WAL
   descriptor participating in one logical mutation.

The last conditional write to either protocol is the visibility point.

## Persistent control records

Control records use the repository's checked binary envelope: fixed magic,
format version, payload length, and checksum. Paths and field names are
validated before use. All lists are stored in canonical bytewise name order and
duplicate modality names are rejected.

### Collection snapshot

```text
collection/snapshots/<snapshot-id>.bin
collection/CURRENT
```

An immutable snapshot contains:

- a monotonically increasing collection generation;
- the primary manifest path, manifest checksum, and manifest version;
- one named manifest reference per dense or late-interaction field;
- the exact collection-WAL visibility frontier used by the snapshot;
- the schema fingerprint covering vector names, kinds, dimensions, metrics,
  and encodings;
- the previous snapshot checksum for audit and recovery diagnostics.

`collection/CURRENT` contains the immutable snapshot path and checksum. It is
created or replaced with object-store compare-and-swap. The manifest reference
checksum covers the complete persisted manifest object; routing and quantizer
references remain transitively checked by the manifest loader.

Child `CURRENT` objects are not written or read in the new format. Their
existence is treated as an unsupported pre-release layout rather than a source
of truth.

### Collection WAL commit

```text
collection/transactions/<transaction-id>/COMMIT
```

The commit contains:

- the shared transaction ID;
- the collection snapshot generation against which validation ran;
- a canonical list of modality name, descriptor path, descriptor checksum,
  and modality prefix;
- the schema fingerprint;
- a checksum covering the complete canonical commit body.

The primary modality has the reserved name `@primary`. Named sparse and text
mutations are included in its descriptor metadata because their sidecars live
in the primary modality. Each named dense or late-interaction child is a
separate modality.

Modality-local transaction states and descriptors still fence competing
writers and make prepared runs recoverable, but their local commit markers do
not grant read visibility. A collection reader admits a descriptor only when
the root collection commit pins its exact path and checksum.

## Create, open, and refresh

Creation stages every primary and named manifest plus its immutable routing
objects. It validates every exact manifest reference, writes one immutable
collection snapshot, and create-only publishes `collection/CURRENT`.

Open performs these steps without mutating a live handle:

1. Load and checksum `collection/CURRENT`.
2. Load the immutable collection snapshot and validate its schema fingerprint.
3. Load every exact primary and named manifest reference.
4. Validate the complete modality set against the schema.
5. Collect the root-authorized collection WAL frontier once.
6. Build all caches and child handles from those exact manifests.
7. Return the fully constructed handle.

Refresh follows the same preparation sequence into temporary state. It swaps
the primary manifest, all child manifests, all WAL snapshots, and derived
routing caches only after every read and validation succeeds. Failure leaves
the previous in-memory snapshot intact.

## Foreground mutations

`add`, `upsert`, and `delete` create one shared collection transaction ID.
After claim acquisition and snapshot refresh, validation covers every supplied
field before any visibility write.

Each participating modality then prepares its immutable WAL runs and
descriptor under that shared ID. These independent uploads use the existing
bounded I/O executor. Once every prepared descriptor has been reloaded and
checksum-validated, the writer:

1. fences every modality-local transaction into `committing`;
2. create-only writes the root collection commit;
3. advances local modality state to `committed` best-effort;
4. releases exact claim-shard versions;
5. installs all committed descriptors in the handle together.

Failure before step 2 leaves only invisible, garbage-collectable objects.
Failure after step 2 is an acknowledged committed mutation even if cleanup or
local state updates fail.

Late-interaction upsert prepares new token rows and old token tombstones in the
same child descriptor. It must not call child add and child delete as two
visible transactions. A delete affecting multiple modalities similarly uses
one root commit.

An idempotent retry with the same transaction ID reloads the root commit. It
succeeds only when the requested canonical modality descriptor set matches the
existing commit; a mismatch is a hard idempotency conflict.

## Flush and compaction

Flush and compaction are collection snapshot publications:

1. Pin one collection snapshot and its root-authorized WAL frontier.
2. Build replacement primary and child base manifests without publishing
   modality pointers.
3. Validate every exact staged manifest and its consumed WAL boundary.
4. Write the next immutable collection snapshot.
5. CAS `collection/CURRENT` from the pinned version to the new snapshot.
6. Install all manifests and WAL frontiers in the handle together.

A losing maintenance writer discards its unpublished snapshot and reloads the
winner. Orphan immutable objects are safe for later garbage collection.
Foreground commits published after the pinned frontier remain visible as WAL
tail and are not consumed accidentally.

## Concurrency boundaries

- There is no collection-wide process mutex around immutable uploads.
- Modalities and cell/lane runs prepare concurrently under existing bounded
  object-store and decode admission limits.
- The root commit and root snapshot CAS are short serialization points, each
  requiring one final coordination PUT.
- A collection transaction may omit modalities it does not change.
- Writers validate the schema fingerprint and pinned collection generation
  immediately before root publication.
- Manifest maintenance and foreground commits have separate root paths;
  snapshot publication pins a WAL frontier so neither protocol loses the
  other's visible state.

## Error and recovery behavior

- Missing, corrupt, duplicated, non-canonical, or checksum-mismatched control
  references are hard corruption errors.
- Missing modality descriptors referenced by a root commit are hard
  corruption, never silently ignored partial rows.
- Prepared modality descriptors without a root commit are invisible.
- A root commit with incomplete best-effort modality state updates is visible
  and recoverable from the root record.
- A staged collection snapshot without a successful `CURRENT` CAS is
  invisible.
- A refresh or post-commit local-install error rebuilds the entire handle from
  root truth; it never installs a subset.
- Old collections without the new root control record fail with an explicit
  unsupported-format error.

## Performance constraints

The atomicity protocol must not add work proportional to row count. One logical
mutation adds one root commit PUT and a bounded number of descriptor
validations proportional only to participating modalities. Immutable payloads
continue to upload concurrently.

Open and refresh must not add a second cell-by-lane scan per modality.
Collection commit authorization is designed to become the aggregate
coordination source in the following WAL-read-amplification work. This change
must preserve existing byte budgets and avoid retaining decoded WAL records in
the collection control layer.

## Testing and success gates

1. Checked-codec tests reject truncation, checksum damage, duplicate modalities,
   invalid names, reordered names, unknown versions, and trailing bytes.
2. Creation and reopen load exact primary and child manifests solely from one
   root snapshot.
3. Fault injection at every modality prepare/fence step proves no mutation is
   visible before the root commit.
4. Failure after the root commit proves every modality is visible after reopen.
5. Add, upsert, and delete never expose mixed primary, named dense, sparse/text,
   or late-interaction generations.
6. Late-interaction replacement atomically publishes new tokens and old-token
   tombstones.
7. Refresh failure at every load/validation point leaves the old handle
   unchanged.
8. Flush and compaction failures before the root CAS leave the prior collection
   snapshot active; success advances all modalities together.
9. Two writers racing the same snapshot publication yield exactly one CAS
   winner without losing foreground commits.
10. Local filesystem, in-memory object store, and S3-compatible crash/reopen
    suites pass.
11. Request tracing proves foreground atomicity adds one bounded root
    coordination PUT and no per-row requests.
12. The full Rust workspace, feature matrix, Python bindings, Node bindings,
    format fuzzing, and production workload suites pass before benchmarking.

## Out of scope for this increment

- Compatibility or migration for any prior experimental collection layout.
- A global garbage collector for orphan immutable objects.
- Replacing all modality storage with one physical segment format.
- Publishing comparative performance claims before production gates and the
  subsequent large-dataset AWS campaign pass.
