# V175 immutable relaid generation authority

## Trust root

V174 bound the row-map header to router/SQ8 identities, but a second
bijective map with the same header could still bind if a caller supplied
its own whole-file hash. V175 adds one strict immutable
`borsuk-relaid-generation-v1` JSON root, loaded only against a separately
trusted whole-file SHA-256. It pins generation, row count, dimensions,
source hash, router-manifest hash, checked row-map artifact hash, old and
new SQ8 object hashes, S3 key and conditional ETag. Unexpected keys,
noncanonical hashes, zero geometry and an untrusted whole-file digest are
rejected. The binder requires this loaded root and checks every identity
it can see, including the row-map artifact SHA retained by the loader.

The generation publisher must obtain the row-map digest from
`write_verified_row_permutation`, then atomically publish the root and
its digest through a durable control-plane pointer. It must never accept
an arbitrary map digest from a query or model output. The root format is
new; old experimental generation artifacts are not silently accepted.

This is a trust-root and row-conversion slice, not a complete generation
publisher or query executor. It does not yet pin a separately serialized
SQ8 mirror manifest or page-authority manifest digest; those loaders
still rely on their caller's authenticated inputs. Stable-ID uniqueness,
source/SQ8 relation, page-range fetch, exact rerank, mutation recovery,
serving latency and charged memory require separate complete-generation
and live S3 gates. The first narrow gate tests a valid root, a wrong
whole-file digest and a root that pins the wrong row-map digest.
