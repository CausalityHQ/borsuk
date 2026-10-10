# 100k real-input PID128 gate — accepted for mechanics only

Original causality Spot instance i-098477877e462a70d, eu-central-1a,
c7a.2xlarge, source4402ace589903e841e97e08b0bbada6ba441d507. Qualified Rust
source c3e52c8bbf0fcc985c0a8a06d2abeec5d7442d38; no Rust source change in this
campaign. Bootstrap c666f6a2c61615ecff6771dc9cec61899946f16c011b49f39af679e9d89fb97e.

| Gate | Original result | Disposition |
|---|---:|---|
| Actual shard/ELF transport + source admission | 0 | Accepted, exact pinned bodies |
| Separate actual native staging | 0 | Accepted for this local-store fixture |
| Controlled timeout | supervisor124 / outer98 | Expected negative; live escaped child then drained in15.24s |
| PID128 width16, width32 | both0 | Accepted, no PID-limit/OOM/swap events |
| Bootstrap / controller / cleanup | 0 / 0 / 0 | Closed |
| Independent watcher | original0 | Instance terminated and root volume absent before collection |

Width16 PID peak26; width32 peak42. Both have pids.events max0 and unchanged
ancestor resource events. Native query RSS peaks34390016 /37412864 bytes,
separately from cgroup memory peaks30507008 /34275328. Resource counters and
original native/time/timeout/tee/manager exits were independently inspected.

Both native summaries report317/320 hits, mean recall@10 **0.990625**,
zero underfill on the **first32 queries**, 100k/D1024/cosine/k10. Query-only wall
totals4632340211 /4717572646ns are **local-file gate timings**. They are not
cold S3 p90/p95/QPS or full1000-query recall. physical_s3_measured=false and
population_percentiles_valid=false in both originals. Do not use this prefix
result to claim a matched S3 Vectors/Turbopuffer win, a width winner, or1M result.

The native summaries count1536 logical submitted reads and1208040192 verified
bytes for each32-query arm. These are not physical S3 GET/wire/billed counters.
The prospective cold gate must measure physical transport separately.

Root inspection authenticated archive709904B SHA
c6514c9545817c6b88d60d94f6eeb0144fd8627f804c722828675a268c3f7468,
1661 regular files, all1499 entries in the four stage closure manifests with
exact membership, all18 replay files and both raw prefix/query-seal hashes.
No local native, recipe, input validator, request/GT or reducer execution occurred.
Raw query bodies were hashed as opaque bytes; original native summaries are
retained. Actual result/schema/GT checks ran on the pinned EC2 entrypoints.

Metadata inspection first had two incorrect expectations: the intentionally
timed-out fixture has no native.exit (actual timeout.exit/time.exit124, outer98),
and the query seal covers the pre-GT portion, not later recall/terminal rows.
Those inspection mistakes were corrected without altering original evidence or
launching another experiment. Final bounded inspection unit
run-p1148884-i705649202.service, invocation b56b38bbd8494e1dbf89f73620609189,
exit0,336ms,15.9MiB/swap0. Earlier failed tool receipts remain in the transcript.

Next: use the existing Rust S3 reader and generic blob publication APIs to
prepare a separately frozen, authenticated object-native cold fixture/protocol;
runtime input admission and disposable staging precede measurements. Keep the
same Cohere corpus/query bytes across competitors, expand beyond the32-query
mechanics prefix for population results, count physical requests/bytes, and
report cache/region/concurrency differences. Existing100k S3 Vectors comparator
is preserved and not repeated merely because this gate passed. Turbopuffer's
published10M numbers remain unmatched targets until exact split parity exists.
SIMD/layout changes require a measured stage bottleneck; serving already uses
native binary artifacts, with Parquet only in offline ingestion.
