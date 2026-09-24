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

## Closed narrow gate

Pushed source `b69621d801e41d4378ec0aaa387939a95d71fd99` ran on one
Causality Spot `c7i.8xlarge` worker `i-0109dc6c639c66c6c`. The
`relaid_generation_authority::tests` target passed **1/1**; the
`serving_generation::tests` name filter passed **8/8** (two binder tests
and six graph-serving tests). The complete terminal SHA-256 is
`264f07cb67c2ed403f1be8dc38d8793effd90d47985921a7987bad6b5c54e6ce`
at `s3://borsuk-bench-453182569524-euc1/research/v176-authority-compile/b69621d801e41d4378ec0aaa387939a95d71fd99/runs/a0001/`.
The controller streamed and rehashed all three terminal-listed artifacts
and confirmed the worker terminated. Compile/tests took 101.89 wall
seconds and peaked at 5,007,416 KiB RSS on the remote build worker.
This establishes the narrow trust-root and binder checks only; it does
not establish a production publisher, live S3 serving or full-suite pass.
