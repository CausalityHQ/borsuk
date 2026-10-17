# BORSUK conditional multipart patch (UNVERIFIED)

This directory is the published object_store 0.14.1 crate, extracted on the
Causality EC2 instance from its cached crates.io archive. The archive SHA-256
was checked against the object_store 0.14.1 checksum in BORSUK base commit
98b2afdd14ed3a873612c6652c98424722989e92 before patching.

- Published archive: https://static.crates.io/crates/object_store/object_store-0.14.1.crate
- Archive SHA-256: d354792e39fa5f0009e47623cf8b15b099bf9a652fa55c6f817fe28ac84fea50
- Cached archive: /mnt/borsuk-workspace/cargo-home/registry/cache/index.crates.io-1949cf8c6b5b557f/object_store-0.14.1.crate
- Published VCS metadata: {"status": "not present in the authenticated published archive"}
- Original license and notice: LICENSE.txt and NOTICE.txt, preserved byte for byte.
- BORSUK-UPSTREAM-SHA256SUMS records every original archive file.
- BORSUK-CONDITIONAL-MULTIPART.patch is the complete delta against those files.
  It uses zero context (apply with git apply --unidiff-zero from this directory).
  All other original files are preserved byte for byte. The three BORSUK files
  named here are additions. The upstream version number is preserved.

The repository root patches crates.io object_store to this directory and
excludes it from workspace membership. BORSUK's direct dependency is pinned
to =0.14.1. Cargo.lock changes only that package from registry to local path;
object_store 0.13.2 is unchanged. A Cargo patch is a workspace-root setting,
not a transitive override for downstream consumers.

## API

ObjectStore gains this async, object-safe method:

    async fn put_multipart_create(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>>;

The default returns Error::NotSupported before any initiation. Existing
put_multipart_opts and ObjectStoreExt::put_multipart keep overwrite behavior.
No silently ignored mode field is added to PutMultipartOptions.

AmazonS3 uses its existing CompleteMultipartMode::Create and If-None-Match: *.
HTTP 412 is translated to AlreadyExists. The pinned client's existing HTTP
409 -> AlreadyExists mapping is preserved; that error does not establish
which writer committed. S3ConditionalPut::Disabled refuses before initiation.
An S3-compatible endpoint must actually implement conditional completion;
this patch cannot detect a server that accepts and ignores the header.

LocalFileSystem publishes with the existing atomic hard-link helper, retaining
fsync behavior and refusing an existing destination. Failed conditional
publication attempts best-effort staging cleanup without deleting the
destination. No HEAD/existence check precedes an overwriting publication.

InMemory performs create under the same write lock as insertion. Entries
retain PutPayload chunk references, so completing the ObjectStore multipart
API does not concatenate another full object. Stream and range reads handle
chunk boundaries; get_ranges still materializes each requested Bytes range,
as its existing return type requires. The older low-level MultipartStore API
is unchanged and does not provide conditional completion.

Arc/Box forwarding, PrefixStore, LimitStore, ThrottledStore and ChunkedStore
forward the new operation and retain their wrapping behavior. Azure, GCP,
HTTP, and external implementations inherit NotSupported unless they implement
the method explicitly. BORSUK CountingObjectStore also forwards the operation
and wraps its upload with the existing request/payload counter instrumentation.
The existing LostHeadResponse test wrapper forwards it explicitly, preserving
its strict missing-trait-methods lint.

## Bounds and failure limits

S3/local uploads stream parts using the existing machinery. Callers must bound
part size and in-flight concurrency. There is no completed staging namespace,
whole-object copy, new 5 GiB object limit, or full-payload readback. InMemory
necessarily retains O(object bytes) plus chunk/part metadata in RAM; this is
not a bounded-total-memory storage implementation.

The original upload abort/drop behavior is retained. Failed or abandoned S3
uploads may require abort or a lifecycle rule, and cleanup cannot be guaranteed
after a crash. A lost completion response can leave an atomically committed
object. A retry's AlreadyExists cannot identify the writer; callers need
application-level reconciliation. Local close/fsync or cleanup errors can
also leave an ambiguous committed object or temporary file. No universal
orphan-storage bound is claimed.

## Qualification status

UNVERIFIED: no compiler, Cargo build, test, native run, real S3 request, service
probe, or build script was executed by this worker. Only rustfmt and static
source/manifest/archive/diff checks were authorized. The authored BORSUK test
file is crates/borsuk/tests/conditional_multipart_create.rs.

The loopback tests use the actual AmazonS3 backend and inspect HTTP completion
headers, winner/loser bodies, HTTP 412/409/500 handling, and a response failure
after committing. They are unrun and cannot establish real AWS behavior.

The parent must compile and run the affected native tests and the full existing
InMemory/backend regression coverage (get/range/copy/metadata included),
qualify workspace Clippy and an actual unshimmed build, and run the tiny real S3
canary before adoption. No lint suppression was added.

The parent scope amendment 1792182670210430619-3532086 authorized only the
necessary forwarding additions to BORSUK src/storage.rs CountingObjectStore
and tests/two_bit_generation.rs LostHeadResponse. Existing methods and all
uploader/publication/GC/serving behavior are unchanged.
