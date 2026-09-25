# V223 authenticated library graph HTTP gate

**Decision:** Does loading the selected V219 generation through the new
single-root library API preserve exact IDs and V222 separate-host HTTP
serving performance? This is an integration and resource gate, not a new
algorithm or a new S3 Vectors measurement.

- Freeze one source commit, the trusted SHA-256 of
  `v223-relaion-1m-generation.json`, one immutable attempt prefix, and
  the V219/V221/V222 terminal identities before launching. The root binds
  the complete graph, FP16 plane, PQ books/codes, physical map, generation,
  source, N and dimensions. The worker downloads only pinned artifact keys
  and verifies SHA-256 and length before library load. A separately pinned
  root digest, not an untrusted sibling file, is passed to the service.
  Frozen root SHA-256:
  `c59650ec920d031ff236f5ab47331db88b71fdac0462cd07a568d51c549b7caf`.
  V219 terminal SHA-256:
  `782fe56ee77a7f16c39e77a6012da63d899e201a694984d919952e894cc90180`;
  V221 terminal SHA-256:
  `9301c109829af1b86d6b6ad08c274ad2d398f3711b5925d14fd83d6302f164b6`;
  V222 closeout SHA-256:
  `81aba13fb991aa18868d607531cc3380d833fe1c5bf1e3d16bfcaa80ab4651e4`.
- ReLAION-1M D768, validation ordinals 0–999 already used, k=100;
  eight persistent HTTP/1.1 connections over two same-AZ `causality`
  c7i.4xlarge Spot peers in `eu-central-1c`. The client timer includes
  JSON encode, HTTP exchange and JSON decode. The index is resident after
  hydration with no response cache and zero vector-body GETs. Run first
  and immediate repeat passes using the exact V222 request file and scoring
  witness. Seal and read back each raw result before downloading truth.
- Each pass must match all 1,000 V222/V219 ID lists exactly: 99,664 GT100
  hits of 100,000 and p05 98. Performance gates: p95 <100 ms,
  p99 <150 ms, completed throughput >=100 QPS, server peak RSS <=3 GiB.
  Compare p50/p90/p95/p99 and QPS from the same raw samples against V222
  first and repeat, but treat run-to-run differences as descriptive.
  The client deliberately reuses the unchanged V222 scorer and its schema;
  the V223 terminal and source archive identify this campaign.
- Preregister interruption handling: if either Spot host is interrupted,
  discard the complete attempt and start a new one under a new prefix;
  preserve interruption receipts. Record both instance IDs, source archive,
  quote, cost estimate and all terminal artifact hashes. Terminate both
  hosts immediately after terminal markers and independently replay every
  artifact before a positive closeout.

No new Turbopuffer claim follows from this gate; its direct tenant access
remains unavailable. V221 S3 Vectors is historical matched-panel context
with different cache and transport semantics, as detailed in the V222
closeout.
