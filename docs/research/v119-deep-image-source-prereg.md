# V119 deep-image source materialization preregistration

This is a corpus-only construction cell, not a quality or performance
comparison. The input is the immutable publication-v3 deep-image-96 staging
receipt at
`s3://borsuk-bench-453182569524-euc1/publication/v3/20260812/datasets/deep-image-96/attempts/0001/STAGING_COMPLETE.json`,
SHA-256 `eb10a317cc778b30e2ee88eee8614b760e36f31d2d7af1a62a60f5f32c163230`
and 18,800 bytes. It declares 58 ordered training Parquet shards, 9,990,000
rows and 3,839,147,200 encoded bytes; the upstream source SHA-256 is
`a0a44dfe80c58e63860862eeea2d34e62cff958dc3aec14fd65263d1d3c751f8`.

Use the generic streamed source materializer with dimensions 96 and metric
`cosine`. Authenticate every shard's byte length and SHA-256 before decoding;
require fixed-list float32 vectors, finite values and nonzero norm. Normalize
each vector to unit L2 norm and write a source-only Parquet table with
monotone `feature_row_id` in the original training row order. The original
train ordinal is the shipped GT identifier. Do not fetch query or GT values
in this cell. Seal source SHA-256, length, row count, staging identity,
per-shard identities, and the explicit absence of query/GT use. The output
is an input to a later paired cross-corpus routing and returned-quality gate;
it is not a 10M ANN result.

Run once on Causality Spot with a 7,200-second science wall cap, an immutable
source archive and attempt prefix, interruption detection, a terminal marker,
and immediate termination. Discard incomplete output after interruption.
Monitor only infrastructure and terminal state until the marker closes.
Before reuse, a later build must download the output and independently check
its full SHA-256 and schema against the sealed provenance.
