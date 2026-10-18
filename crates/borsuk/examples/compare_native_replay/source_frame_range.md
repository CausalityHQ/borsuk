# SOURCE frame range replay

This is a source-only local byte-cost diagnostic. It does not run ANN, open
requests/truth/f32, call S3, measure serving latency/QPS, infer wire or billed
requests/bytes, or change production storage/defaults.

## Frozen interface

Create a JSON config with schema
`borsuk-source-frame-range-replay-config-v1` and exactly these fields:

```json
{
  "schema": "borsuk-source-frame-range-replay-config-v1",
  "authority": {"path": "/absolute/root-input-authority.json", "bytes": 1, "sha256": "..."},
  "completed_config": {"path": "/absolute/completed-reader-config.json", "bytes": 1, "sha256": "..."},
  "source_config": {"path": "/absolute/source-frame-config.json", "bytes": 1, "sha256": "..."},
  "max_replay_metadata_bytes": 16777216
}
```

Replace descriptor lengths and hashes with actual frozen values. The authority
uses schema `borsuk-source-frame-range-root-authority-v1`.
Its exact completed input, expected identity, bound inputs, source inputs, and
config paths/hashes must match the two independently pinned configs. The
completed input must be a full 1000-query concurrent native trace with flat
SOURCE layout. Its generation root SHA, rows and dimensions must match the
SOURCE config. SOURCE generation -> plane -> original records/digest
authentication uses the existing qualified streaming reader.

Run the same frozen config through admission first:

```sh
compare_native_replay --source-frame-range-admit CONFIG CONFIG_SHA NEW_ADMISSION
compare_native_replay --source-frame-range CONFIG CONFIG_SHA NEW_REPORT
```

Both create reports exclusively and return 0 on success, 2 on refusal. Refusal
can produce a durable INVALID report. Admission checks every native trace row,
seal, full SHA and EOF before opening SOURCE; then it authenticates every
original SOURCE unit/full hash without encoding or calculating costs. Cost
mode first repeats full admission, then scans SOURCE with the unchanged
256-row transpose/Zstd1/raw-fallback codec, retains sizes in original physical
frame order, and repeats the strict trace reader with a cost observer.
A separate disposable canary and exact-source qualification are operator gates;
this CLI does not authorize paid experiments.

## Accounting

The strict current native reader reconstructs the shared flat cover from
`trace.ranked_candidate_pages` using the original SOURCE GET cap.
It checks every recorded interval and original logical submission/verified-byte
charge, including bridge pages. No historical 16-leaf or 1024-unit restriction
is imported into the current trace.

Each original object interval is mapped to every touched complete compressed
256-row frame, with the last frame clipped to actual corpus rows. Primary
bytes include actual 25-byte headers and repeated frames across original
logical range reads; primary logical GET count is exactly the original roster.
This is a counterfactual payload accounting model, not measured transport.
Per-query unique-frame bytes are a separately labeled lowerbound; no dedup
GET/merge/cache result is inferred.

Ordered frame sizes and their LE-u64 SHA witness, plus an ordered
query-ordinal/range-count/start/end LE-u64 hash, support accounting audits.
Totals and nearest-rank p50/p95/max cover all 1000 queries for bytes and logical
GETs. Proposed directory storage (64+24*N) is separate from primary bytes and
does not establish directory fetch cost or an adopted format.

## Memory, timing and evidence limits

Only original/encoded/decoded frame buffers, bounded metadata, ordered sizes,
and per-query costs are retained. Original source bytes are streamed once.
The replay metadata cap is checked before SOURCE IO. Its conservative model
reserves two report caps, three small-config buffers and 64 bytes per frame;
the existing strict completed-reader model and SOURCE authentication
metadata/buffers are additional. Resource snapshots include process RSS/HWM
and cgroup memory/current/stat/peak/cache information. Cgroup memory includes
attributed file cache; it is not process RSS. The local input cache state is
uncontrolled.

Encode/decode CPU and wall timing covers whole fresh codec calls over the
whole plane. These are observed local codec costs and are never labeled query
latency. A byte/CPU/memory tradeoff is reported without an arbitrary percent
gate. Production serving-format/range correctness and matched performance
qualification remain subsequent work.

Native tests cover aligned and unaligned intervals, overlap, repeated frames,
complete bridge intervals, partial tails, raw fallback expansion, ordering
distinct from the sorted histogram, cap-minus-one refusal, valid but unrelated
SOURCE generation, corrupt original units/pins, and existing strict native
range/counter/seal/header refusals.
