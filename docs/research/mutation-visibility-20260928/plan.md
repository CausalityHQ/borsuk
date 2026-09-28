# Apply mutation visibility before top-k

Base a79733e5. Shared returned-SQ8 core must exclude sorted unique signed-i64
logical IDs before top-k selection, so deleting or replacing a high-scoring
base row does not incorrectly underfill or retain stale rows. Keep existing
strict rank wrapper unchanged; new exclusion path returns up to k visible rows,
including zero when every fetched row is masked. Authenticate and validate all
rows, including excluded ones; duplicate physical IDs still fail closed.

Expose through one shared range reader and TwoBitGeneration search path. Existing
search delegates with empty exclusions; preserve range plan, GET/byte caps,
conditional ETag/page hashes and original failure stats. Validate sorted unique
roster before network I/O. Borrow immutable roster, binary search per row; no
full base-ID hydration, scan or additional request. Caller owns/authenticates
snapshot; charge its payload in already_pinned_bytes and reject a roster that
exceeds that charge. This is query-side visibility only; snapshot CAS, mutation
recovery and compaction remain subsequent work and release gates OPEN.

Red std-only exact production core test, green same, then focused original Spark
Cargo SQ8 range and TwoBit generation checks once. No local Cargo/cloud/full suite.
