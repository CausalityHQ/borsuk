# Cold flat SOURCE score overlap

Only flat remote SOURCE with Offload2 and no persistent SOURCE cache uses the
overlap path. The same reader futures, two global auth workers, download permits,
byte leases and ordered drain remain in use. Whole response metadata and all page
hashes (including physical bridges) authenticate before the callback. The callback
scores only the original admitted walk units. Original page completion and
nomination replay run after all reads succeed; no SQ8 or result escapes earlier.

One shared prepared query and fixed OnceLock slots retain exact f64 score bits or
codec errors. Replay consumes them in original mandatory-unit order. Range errors
retain their original ordered precedence and charges; codec/score errors remain
planning errors after successful SOURCE charges. Cancellation joins active jobs
and drops queued callbacks before query-owned data is released. Bodies remain
retained through planning.

The shared runtime / strict FP32 model adds source_score_overlap_bytes for each
active flat Offload2 query, before payload I/O. Capacity is
min(ceil(rows/32), floor(source_byte_cap/(32*record_width))+1), with padded codec
record width, actual OnceLock<Result<[f64;32], TwoBitError>> and usize sizes,
two scoring-block scratch allowances and 4096 control bytes. Allocated score
capacity is checked; unit and score arrays are boxed slices. The D1024/64MiB
forecast is below 4MiB; the focused test prints actual sizes and reservation.
Inline/framed/capsule execution stays unchanged; flat cache users conservatively
retain this admission charge although their execution stays unchanged.

Wire contract: no selector, schema, identity or bound-input field changes.
source_authentication_extra_library_bytes now includes the flat score reservation.
The complete FP32 modeled_memory_bytes increases by the same amount. RangeCpu
optionally emits source_scoring only when measured (4 clock reads per callback);
authentication remains hash-only. SOURCE wall includes overlapped scoring;
planning wall contains replay, completion and selection. The strict reader
validates the optional scope and includes it in process CPU residual accounting.
Original logical/physical requests and bytes, SQ8/FP32 plans and result ordering
are acceptance invariants.

Focused tests cover actual scoring while a final HTTP response is held, a late
scoring falsifier, shuffled completion, scalar score-bit parity including zeros,
ties, tails at D1/257/1024, bridges not scored, deferred codec errors, ordered
corruption/transport drain, active/queued callback cancellation and recovery,
model-minus-one before payload reads, and existing FP32/offload regressions.

Metadata-only recipe: use the original A1-config.json as immutable control
metadata, with cache0/Q1/coverage64/shortlist16/coalescefalse/Offload2/fetch128,
SOURCE128GET/64MiB, SQ832GET/32MiB, canonical16GET/65664B and 2GiB cap unchanged.
Root must bind candidate source/native/reader hashes, recompute the auth-extra
and full model through these shared functions, then run its existing metadata
preparation controller. This worker does not open real requests, truth or corpus,
run admission/performance, revise quality gates, or control cloud shutdown.

After the parent binds its authorities.json and protocol.json to the verified
commit, qualification receipt and ELFs, its metadata-only preparation entrypoint is:

```sh
python3 /mnt/borsuk-workspace/evidence/cold-source-score-overlap-screen68-control-a0001/run-prepared.py prepare
```

This writes derived configuration/expected-header metadata only. The separate
`run` entrypoint opens real inputs and starts the experiment; authority remains
with the parent. The packet itself has not invoked either entrypoint.
