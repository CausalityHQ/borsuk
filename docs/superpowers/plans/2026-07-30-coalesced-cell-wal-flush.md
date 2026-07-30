# Coalesced cell-WAL flush implementation plan

## Goal

Bound physical segment and PUT amplification when many small committed record
runs are materialized from the collection WAL.

## Invariants

- transaction selection and collection-atomic visibility remain unchanged;
- record runs coalesce only within the same logical cell;
- each emitted segment contains at most `segment_max_vectors`;
- flush keeps at most one target-sized pending record batch per cell;
- every selected run is marked consumed only in the newly published manifest;
- tombstone and ID-directory runs retain their existing handling; and
- reopen, exact search, MVCC suppression, pruning, and garbage collection keep
  their current semantics.

## Tasks

- [x] Add a regression proving four one-record transactions in one cell flush
  into one target-sized segment.
- [x] Group selected record runs by logical cell and stream them into bounded
  target-sized batches.
- [x] Preserve locality sorting for individual oversized runs and all existing
  consumed-run bookkeeping.
- [x] Run the WAL, cell-WAL, crash, fault, formatting, and Clippy gates.
- [x] Record the implementation checkpoint in the production hardening audit
  and production-readiness documentation.
- [x] Commit and push the verified slice (`4d8cd28`; push recorded by the
  following documentation checkpoint).

## Promotion gate

The implementation removes the one-segment-per-run behavior. Production claims
still require a frozen workload measuring segment/object count, PUTs, bytes,
flush p95, search GETs, and subsequent compaction amplification.
