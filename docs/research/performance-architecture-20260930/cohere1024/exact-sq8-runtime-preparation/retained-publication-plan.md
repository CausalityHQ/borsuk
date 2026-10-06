# Retained generation transport rebinding

Status: prospective native change; not implemented or qualified. Source inspection at 1cd0405106d226e4ed05728992cce0a0571b0a77. Read-only specialist 1d2f5c32f0f24e4c completed; root independently checked local ETag formula, serving/publication checks and retained roster.

## Causal problem

object_store 0.14.1 LocalFileSystem derives ETag from inode, modification time in microseconds and size. Identical copied bytes therefore do not preserve transport identity. SQ8 ETag is persisted in the generation root; source record and semantic leaf ETags are acquired at remote open. Retained semantic publication omits centroids.bin, although fresh-build publication requires that construction artifact. The graph-only D<=768 repackage helper cannot serve the retained D1024 semantic case.

## Native intervention

Add a bounded retained-publication function to two_bit_store.rs, with tests in crates/borsuk/tests/two_bit_generation.rs. Reuse authenticated head/root metadata, remote serving validation, checked descriptors, bounded streaming authentication and head commit primitives. No rebuilding, normalization, encoding, fitting or query changes.

Caller must supply an independently approved original root SHA and expected original generation/epoch, alongside the freshly read retained head. A copied head is not independently trusted construction evidence. Reject mutations, fences, stale control authority, nonzero base epoch and occupied destination. The first implementation targets initial semantic generation republishing, with checked profile/geometry rather than dataset names.

Authenticate every retained metadata/payload body, canonical object and SQ8 object against the approved original descriptors. HEAD length alone is insufficient. Use conditional reads and exact length/EOF/SHA, charge all simultaneous buffers/caller pins and cumulative scratch before allocation, and recheck pinned control and object identity before final publication. Require a strong approved current SQ8 ETag and valid digest-suffixed object key. Do not bypass object identity or add a custom lying store adapter.

Only sq8_object_key and sq8_etag may change in the root. Preserve coefficient f32 bits, complete Discovery provenance, canonical descriptor/location, generation/base epoch and all other parsed root values. Payload bytes remain identical. New root and head identities and metadata publication locations differ explicitly. Head is committed last with create-only destination semantics; failures produce no qualified new head. Temporary/unreachable objects and scratch have bounded accounting and cleanup. Original artifacts are immutable.

For local paired replay, both original and candidate binaries query the same newly frozen root/head, same retained query/truth bytes, same host/resources and alternating order. Exact returned ID/score-bit parity precedes performance admission. This is local-file evidence. Physical S3 requires actual S3-native object versions and physical request/byte/error measurement separately.

## Bounded falsifier

Actual tiny semantic D1024 builder/publication fixture: retain exactly the published roster without centroids, copy onto a second LocalFileSystem, show original SQ8 tag refusal, then successful rebind. Independently compare every body hash and all non-whitelisted root values, exact returned IDs/score bits, and reopen after deleting original build and prior publication metadata. Corrupt SHA, truncate/grow body, wrong ETag/key/epoch/root, mutate source control midstream, occupied destination and cap exhaustion must refuse without new head. Test uses existing ChunkedStore for real local-file payload streaming, not mocks alone.

Source-only authoring checks CPU1/256MiB/noSwap. Native tests, release, workspace Clippy correctness/suspicious and real shim-unset workspace test compilation run remotely on exact source before integration. Preserve live original scorer qualification exec79718/i-08769cc86a13173d8 and held candidate9d540e52. No new paid replay before its terminal qualification and root decision.
