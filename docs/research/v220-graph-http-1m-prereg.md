# V220 frozen HTTP serving gate

**Decision:** does the selected V219 graph retain exact IDs and usable tail latency
when served through an eight-worker HTTP process? This is an incremental product
serving gate, not a matched competitor comparison.

- Source: one committed revision, archived with SHA-256 before launch. One `causality`
  `c7i.4xlarge` Spot attempt in `eu-central-1c`, 90-minute hard stop; discard an
  interrupted cell and restart under a new attempt. Terminate after terminal.
- Inputs: authenticated V219 `a0002` prep, graph, map, and build artifacts under
  terminal SHA-256 `782fe56ee77a7f16c39e77a6012da63d899e201a694984d919952e894cc90180`;
  original V219 FP16 plane, PQ64 books/codes and requests. ReLAION-1M D768,
  **validation ordinals 0–999 already used**, k=100. No new fit or tuning.
- Service: source-only reachable graph, PQ64 cosine navigation, resident FP16
  rerank; ef=4,096 and shortlist=4,096. Eight fixed workspaces/worker threads.
  Client uses eight persistent HTTP/1.1 connections over **loopback on the same
  instance**, one query stream per connection. One pass, no response cache.
  The index is resident after authenticated hydration. Server internal cache
  warmth and client CPU contention are part of this measurement.
- Measure: raw returned IDs, request and response bytes, per-query elapsed
  nanoseconds from HTTP send through full response body, visits, p50/p90/p95/p99
  from those same 1,000 samples, completed QPS, and process peak RSS. Record
  source archive, instance, elapsed wall and Spot price/cost estimate at closeout.
  Vector-body GETs must be zero. HTTP loopback timings are not external network
  product latency and cannot be called an S3 Vectors or Turbopuffer win.
- Seal raw HTTP returns to S3 and verify readback SHA-256 **before** downloading
  the V219 raw IDs and V198 GT100 witness. Gate: every ID list equals V219
  4,096/4,096 exactly; 99,664/100,000 GT100 hits, p05 98; p95 <100 ms,
  p99 <150 ms, >=100 completed QPS, peak serving RSS <=3 GiB. A failed
  performance gate requires diagnosis from the recorded samples, not retuning
  this cell.

If this passes, the next gate is a direct ReLAION-1M validation-split S3 Vectors
query measurement and external-client BORSUK HTTP measurement with the same
query panel, k, concurrency, region, cache disclosures and raw p50/p90/p95/p99.
Historical S3 Vectors development-split and vendor Turbopuffer values remain
separately labelled context until an authenticated matched reproduction exists.
