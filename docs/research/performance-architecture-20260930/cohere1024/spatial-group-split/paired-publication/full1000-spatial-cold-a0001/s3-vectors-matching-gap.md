# Verified 1M S3 Vectors matching gap

Audit of qualified source ddbcd91c, check_s3_vectors_baseline.rs:
- validate_config measurement requires exactly100000 rows and1000 queries.
- Measurement admits only the frozen old100k corpus/query/truth SHA constants.
- Corpus input is rows*1024*4 raw typed f32; publication keys are six-digit
  row ordinals. The retained1M BORSUK canonical object is4104000000 bytes,
  including explicit record IDs, rather than a4096000000-byte raw matrix.
- Historical service protocol requires3600s idle before a once-only measurement;
  provider cache is unobservable. Preserve that protocol and original results.

Therefore the old service numbers do not match the current1M corpus/query split.
A distinct Rust benchmark adaptation must authenticate the exact1M canonical
records (ID and f32 values) or a native-derived explicitly pinned raw matrix,
preserve the query/truth IDs and tie conventions, preregister population/shape,
namespace ownership, cache/idle protocol and cost, and retain strict once-only
calls/seal/cleanup ledgers. No raw decode, ID reconstruction or scoring in Python.
Avoid presenting dataset-family agreement or the100k recall as a1M baseline.

Native compilation/Clippy/workspace test-build and actual-input admission plus
SDK canary are required before service launch. No new service experiment,
consultation, compiler or measurement has started from this audit. The current
full1000 BORSUK command remains the original live handle, untouched.

## Canonical format confirmed from qualified Rust source

canonical_source.rs writes physical-order records: signed LE i64 logical ID,
followed by D normalized LE f32 coordinates; exact width8+4D, no extra header.
CanonicalSource.valid authenticates rows/dimensions/length/SHA/object key.
write_canonical_source obtains IDs from the SQ8 records and vectors through the
validated source-plane permutation, then normalizes through cosine_vector.
Do not reinterpret this as a contiguous headerless f32 matrix or replace IDs
with physical row ordinals. The 1M SDK adapter can stream these exact records,
validate finite/nonzero vectors and unique in-domain logical IDs using a bounded
bitset, preserve those IDs as service keys, and recheck full SHA/EOF/identity.

prepare_two_bit_compaction exports source.f32/ids for maintenance but participates
in generation/mutation/fence workflows; it is not an appropriate read-only
benchmark converter. Avoid invoking maintenance on published benchmark roots.
The adaptation should stay experiment-private, reuse the existing service call,
seal and cleanup machinery, and leave product source/scoring untouched.
