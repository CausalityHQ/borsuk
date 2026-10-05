# Closed population authority

`closed-populations.json` is derived from the immutable closed histogram campaign's terminal-pinned prefix and all 128 result bodies. It contains no vectors, truth IDs, quality scores or candidate measurements. Its schema is `borsuk-corrected-four-bit-closed-populations-v1`.

Output: 175,929 bytes, SHA256 `764674f0623797d1303158750fa82a568ea5fabd4fd57f7df5b333865e02a29a`. Each result retains its original body descriptor. The terminal and prefix descriptors bind the historical source authority. Fetched IDs use ordered little-endian signed i64 hashing; nominee physical ordinals use ordered little-endian u64 hashing.

The one original derivation ran in `borsuk-corrected-population-authority-179123.service` with CPU100%, MemoryMax256MiB, MemorySwapMax0 and TasksMax64. Collected exit0, 1.024s runtime, 905ms CPU, kernel memory peak33.3M, swap0. No Cargo, native ANN, vectors, truth files or cloud action ran. Its temporary source is retained at `/tmp/borsuk-corrected-population-authority.py`.

Checks authenticated each used body against the original terminal bytes/SHA, matched all 128 result dataset/ordinal/query/nominee identities against the frozen prefix, validated ordered nonoverlapping physical intervals, and required identical full fetched-ID order across histogram and same-population SQ8. Range counts and verified bytes matched those intervals. Maximum41,984 rows and16,625,664 payload bytes reproduce the closed result.

The next native diagnostic must authenticate this pointer and compare complete ordered intervals plus the full fetched-ID hash. Nominee retention alone is insufficient. This authority is an input for a future prospective configuration; it does not qualify the new codec or certify execution/resource closure.
