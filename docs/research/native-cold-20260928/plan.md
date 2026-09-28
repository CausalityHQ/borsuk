# Native cold serving implementation plan

Goal: make the existing two-bit library callable against real S3, and measure
its nearest baseline end to end. Scope is serving integration/performance,
not a replacement architecture or revival of the failed diverse graph arm.

Use inline execution with the existing AWS-only workflow. Operator autonomous
production mandate authorizes this bounded composition of existing APIs;
do not add another architecture/reviewer cycle or request repeated permission.

- [ ] Add a development-only live mode to two_bit_plan_demo. Observe its split
  admission regression RED on one bounded AWS Spot worker.
- [ ] Compose publish_two_bit_generation, read_two_bit_head, open_remote and
  search; preserve ordinary and paired plan modes. Record publication/open
  separately, then complete per-query wall/CPU/returned IDs/read accounting.
- [ ] Freeze the nearest ReLAION artifact identities from topology a0003,
  publish SQ8 immutably at a fresh namespace, rebuild only serving bindings
  with actual ETag, prove all query-dependent components and64 normal plans.
- [ ] Run focused GREEN plus one serial64-query,32-way-range live S3 cell on
  AWS Spot. Check exact returned ID/count parity and physical request/byte
  caps; retain all terminal artifacts, independently verify, terminate.
- [ ] Deliver code, receipts and a row with measured p90/p95/QPS/read charges,
  whole resource/startup costs and explicit quality/vendor/scale limitations.

No local compilation/tests/query/scoring. No library default/dependency change,
new candidate fit, validation reuse as fresh data, or promotion of diverse graph.
Cold means no client SQ8 cache and fresh conditional GETs for each query;
metadata/router resident, HTTP connections may be reused, S3 server cache
uncontrolled.64 queries including first; no warm-ups. One cell, no sweep.
