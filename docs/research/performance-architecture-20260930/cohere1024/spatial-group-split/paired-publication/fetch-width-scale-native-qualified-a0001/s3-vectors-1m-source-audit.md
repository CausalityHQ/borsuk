# Matched 1M S3 Vectors blockers — source audit only

Inspected exact unchanged source from bb80700f export: crates/borsuk/src/bin/check_s3_vectors_baseline.rs. No service job or corpus processing launched.

The old qualified harness admits measurement100k/count1000 with old raw-corpus/truth hashes. Current 1M canonical source is4104000000B, signed LEi64 logical ID followed by1024 LEf32 values; it is not a4096000000B raw matrix. A future Rust adapter must preserve logical IDs and authenticate the complete canonical body, valid finite/nonzero vectors, bijective ID domain and file lifetime. Direct ordinal seek currently used for raw readback is incorrect for physically permuted canonical records; retain an authenticated logical-ID→physical-row map or use another correct bounded strategy.

Additional hard blockers found in the actual source:

- ListVectors maxResults500 implies at least2000 calls for1M. LIST_PAGE_CAP1024 cannot cover the population even with full pages. Short pages caused by service byte ceilings can require more; preregister a bounded new envelope with progress/token checks.
- Full readback uses100 keys/GetVectors: exactly10000 calls at1M. Publication adds2000 PutVectors calls; enumeration adds at least2000; measurement adds1000 QueryVectors. Thus at least15000 calls plus create/get/delete bookkeeping, versus CALL_CAP4096. Earlier estimate counting only upload+query was incomplete and is superseded by this audit.
- Raising the call ceiling also requires reviewing the4MiB ledger cap,32MiB output cap, token-set memory, retained response limits and replay validation. Preserve once-only claims, call reservation/authentication, ownership and cleanup evidence; do not simply bypass caps.
- Existing measurement publication paces PutVectors at1s intervals:2000 puts imply roughly33min minimum, followed by the unchanged3600s idle protocol. A same-protocol1M comparator is therefore a long service campaign, with an explicitly larger finite time/cost envelope. No claim these setup/idle costs are query latency.

The current BORSUK baseline and native input pins stay frozen. Matched service publication must use those exact logical vectors/query/truth bytes and record preparation/readback/lifecycle costs separately from query timing. Existing100k service and published Turbopuffer10M results remain unmatched references.
