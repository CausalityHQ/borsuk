# V233 decoded mutation delta HTTP, ReLAION-1M

**Decision tested:** whether V232's exact resident FP32 mutation delta
reduces end-to-end network serving latency at 1M without changing any
V230 returned ID. Use one two-host `causality` c7i.4xlarge Spot attempt
in `eu-central-1c`, with authenticated V223 single-root graph and its
five immutable blobs. Upsert every hundredth physical row under the
same ID and authenticated FP16 vector (10,000 rows). Decode those
mutation coordinates once to resident FP32. The logical corpus and
GT100 do not change. Search uses ef/shortlist 4,096/4,096.

Dataset/split: ReLAION-1M D768 validation ordinals 0–999, previously
used, cosine k=100. The strongest relevant BORSUK baseline is V230's
sealed 10,000-row linear-delta first and immediate-repeat HTTP passes;
the paired workload, server/client instance type, VPC-peer transport,
eight persistent HTTP/1.1 connections and no response cache are fixed.
Client timing includes JSON encode, HTTP exchange and decode. Run first
and immediate-repeat passes, seal each per-query ID/latency stream to S3
before downloading exact GT100, then recompute p50/p90/p95/p99, QPS,
logical request/response bytes and exact ID parity from those streams.
Record server RSS, overlay-owned bytes, vector-body GETs, instance IDs,
Spot quote and estimated compute to terminal. First and repeat compare
to their corresponding V230 passes. Source revision and host differ, so
timing deltas are descriptive even though workload and shape match.

Pass only if **both** passes match all 1,000 complete V230 ID lists,
match its per-split GT100 hits (25,509/25,600 and 74,159/74,400),
have p95 at most 75% of the corresponding V230 p95 and throughput at
least 1.5× the corresponding V230 QPS, peak server RSS at most 3 GiB,
overlay resident bytes at most 64 MiB, exactly 10,000 pending rows,
and zero vector-body GETs. V230 first p95 is 38.788652 ms and
244.077 queries/s; repeat is 38.195128 ms and 247.929 queries/s.
If parity fails, reject the decoded path and inspect FP16-to-FP32
scoring. If performance fails, keep the exact representation as a
candidate but choose a material exact-score or mutation-tier change
before another 1M cell. No vendor win, 1M durable mutation publication,
cold per-query S3 retrieval, or 10M/100M scaling is inferred here.

Preserve source archive, original terminal, every artifact hash and
both sealed raw streams. Spot interruption invalidates the complete
two-host cell. Sync interruption and terminal evidence, discard the
partial cell and restart only under a new attempt. Terminate compute
on terminal. Do not inspect incomplete measurement files.
