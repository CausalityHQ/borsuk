# V221 direct S3 Vectors ReLAION-1M validation comparator

**Question:** on the same ReLAION-1M validation panel as V220, what quality,
client-observed latency and cost does a direct S3 Vectors index deliver?
This is a direct service measurement, not yet a matched BORSUK network win.

- Immutable inputs: source Parquet SHA-256
  `2796b579f37afe99ca4aff57e282335a6a79ad30596645957d26326a0560cf86`,
  validation query Parquet
  `869e225181f7d01a972d8faa144eaff4838c7c1f8f7c0c55091487e234f0bd5e`,
  validation GT100 Parquet
  `bf0fb0c934c986d05282e3d1c63dc351c553976ea05bfcab0cd3f06d2979e871`.
  N=1,000,000, D=768, validation ordinals 0–999 already used, k=100.
- One `causality` `c7i.4xlarge` Spot client in `eu-central-1c` first, with
  serialized fallback to the two existing eu-central-1 zones if capacity is
  unavailable. Two-hour hard stop; discard an interrupted cell and relaunch
  under a fresh immutable prefix. Terminate compute after terminal.
- Create a fresh float32 **cosine** S3 Vectors index, matching BORSUK's
  navigation and rerank metric. The shared V36 GT100 is squared Euclidean;
  source squared norms were previously measured at 0.998774–1.001197, so
  cosine and Euclidean ranks are close but are not proven identical. Score
  both products against this same frozen GT and disclose the distinction.
  Ingest the complete
  authenticated source with five upload workers, settle 60 seconds, then make
  two passes over the validation panel in ordinal order with eight concurrent
  requests. First-pass and immediate-repeat labels describe execution order;
  server cache state is vendor-managed and opaque. BORSUK V220 uses cosine
  navigation/rerank against this GT, so metric semantics must be disclosed.
- Record each query's returned IDs, recall@10/100, client SDK start/end and
  latency, p50/p90/p95/p99 from the same raw samples, completed QPS, response
  bytes, upload bytes/time, client peak RSS, instance identity, launch/terminal
  elapsed time, Spot quote and compute cost estimate. Keep the S3 index/bucket
  cleanup receipt. Seal terminal artifacts and validate all hashes.
- Compare quality directly with V220's 99,664/100,000 GT100 hits and p05 98.
  V220's 14.896/19.809/21.224/24.208 ms p50/p90/p95/p99 are **resident
  loopback HTTP** and are not equivalent to S3's regional service path.
  A product latency claim waits for the external-client BORSUK gate at the
  same concurrency, region, panel and declared cache state. Turbopuffer
  remains vendor context until tenant access exists.
