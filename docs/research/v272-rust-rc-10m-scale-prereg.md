# V272 frozen Rust RC CoHere 10M scale/cost gate

Status: preregistered before any V272 reservation or measurement. One
`causality` r7i.8xlarge Spot instance (256 GiB, 32 vCPU) in eu-central-1c,
250 GiB encrypted gp3, with a 12-hour hard stop. Production library and
lockfile stay frozen at `aa4182a2e6b78a254bca732663c7528818eb6561`.
The verified V271 fresh-query closeout SHA-256 is
`d64015fc9501cc2146378a54b8e24866898cf2c6fcd446c37c42ce2b0a3147b0`.

## Inputs and fixed method

- CoHere-large-10M full 10,000,000 canonical train rows, D768 cosine, from
  all 458 shards in the sealed staging receipt (SHA-256
  `0965aa0241199822dfac3410bba4edad5536ac0eb0aaa8ab83c216e8c5749a87`).
  Authenticate every shard before appending its FP32 source rows.
- Queries: first 1,000 rows of canonical `test.parquet`, SHA-256
  `5e0123f163df0e53a7e329fd92fbfd49f079756acfb47387ee6664c267b6f94e`.
  These queries were used in earlier experiments and are disclosed as
  **prior-used**. Ordinals 0–255 and 256–999 are separately reported.
- Build exactly one immutable generation through the public Rust source-only
  builder. Publish the five authenticated blobs and root to a new S3 prefix,
  then reopen and hydrate that root through the public Rust API in a separate
  process. Resident cap is `2,500 × rows` bytes (25 GB at 10M); it scales
  linearly with row count and has no row-count-specific switch. Search the
  1,000 queries sequentially at k100, one worker after one warmup query, with
  no response cache or query-time object GETs.
- Exact truth: FAISS `IndexFlatIP`, FP32 unit-normalized all 10M source rows
  and the same 1,000 queries, k100. Compute after ANN raw output is sealed
  locally; retain exact IDs and raw ANN query IDs/latencies. FAISS here is
  only the exact-truth engine; the seven-arm ANN control was already measured
  in V271.

## Terminal and decision

The launcher reserves a unique attempt and source archive before Spot launch,
records the instance and quote, and permits only one active BORSUK instance.
It checks terminal identity, terminates compute immediately, replays every
terminal artifact hash and size, verifies the published root from S3, and
reads back a closeout. A Spot interruption or hard stop invalidates the cell;
rerun only under a new attempt ID after identifying the cause. Do not inspect
incomplete measurement files.

GO to a separate HTTP/product and matched-vendor gate requires a complete
authenticated build/publish/reopen/search/truth cycle, both prior-used splits
at recall@100 ≥0.995 and fifth-percentile hits ≥98/100, p95 ≤150 ms and p99
≤180 ms from the same 1,000 query samples, peak search-process RSS ≤32 GiB,
and exactly five hydration object GETs with zero query-time GETs. Report
p50/p90/p95/p99, throughput, build/preparation/truth/hydration times and
peak RSS, generation bytes, source and hydration GETs/bytes, raw identities,
Spot quote and compute through terminal. Object-storage and EBS lifecycle
charges must be separately estimated from disclosed rates before any total
cost claim. This gate measures in-process resident library performance, not
HTTP or vendor service latency. Failure requires one root-cause design
decision and a cheap falsifier before another paid 10M attempt.
