# V120 deep-image source-only index construction closeout

Status: **construction complete**. No query or GT object entered this cell;
no returned recall or live S3 query latency was measured.

- Frozen source commit `b919685cf1db6c14912c2118d5226c0fb426226b`;
  archive SHA-256
  `847960e6074ff10634ee1621976c5a50c20fe49b310028a7dbca7db3e3eff374`
  (11,560,337 bytes).
- Immutable attempt:
  `s3://borsuk-bench-453182569524-euc1/research/v120-deep-image-index/b919685cf1db6c14912c2118d5226c0fb426226b/runs/v120-20260924T004145Z/a0001`.
- Causality Spot `c7i.12xlarge` `i-0b189e743492c2a08` in
  `eu-central-1c` completed with exit 0 in 1,775 seconds. The original
  launcher returned that terminal; EC2 subsequently reported `terminated`.
  Terminal SHA-256:
  `4f789d9b7762c96178873bf3bd3fe8ff9cdaf7311c25f7752f9f550fad563387`.
- The worker downloaded and independently checked V119's full 3,566,768,562
  byte source payload against SHA-256
  `8f88122f412554107d97c07f440352f9043b8cb4b58fe08434ac75f4b90776ee`
  **before** fitting.
- The source-only layout manifest records 9,990,000 rows, D=96, 17,644
  clusters and 256-row SQ8 pages. `layout.npy` is 79,920,128 bytes,
  SHA-256 `419f9280d2e85f6fa275c115dd342c249ac31ec6af2d19a42f5b96c38ed247c1`.
  `sq8.bin` is 1,078,920,000 bytes, SHA-256
  `7c78292cef359f1a3bc7f7406a0cedffe79a5bf06b7356f1a97de209ed6c5e7c`.
  The balanced-PQ64 router's code plane is 639,360,000 bytes, SHA-256
  `80fc3b9e5e970c39d0e7070ec4b4f46924932c9bdc55522d93385b388b753ea8`;
  its summary plane is 29,970,432 bytes. Router, mirror and page-authority
  manifests all bind the same source, layout, SQ8 body and generation.
- Layout fitting took 19:52.59 wall time with 11,585,388 KiB maximum
  **process** RSS. Router fitting took 6:38.87 with 12,940,016 KiB maximum
  process RSS; mirror/page sealing took 10.14 seconds with 10,257,608 KiB
  maximum process RSS. These are construction measurements on Spot, not
  serving cgroup memory or query latency.
- The full BORSUK crate compiled and eight targeted `pq64_` Rust tests
  passed, with 1,596 filtered; compile/test wall time was 1:41.92.
- All 27 terminal-listed S3 object lengths independently matched. The 24
  smaller objects were streamed back and matched to their terminal SHA-256.
  The three large objects (`layout.npy`, `sq8.bin`, `router/codes.bin`) have
  matching object lengths and worker hashes, but no independent full S3
  payload hash yet. A later quality consumer must download and verify each
  full SHA-256 before use.

The next decision is the preregistered V122 D96 100k development screen.
V120 itself is not a 10M quality pass. The layout fitter differs from the
V63 fitter behind V116's ReLAION-1M result; the qualification consequence
and matched 1M rebuild gate are in `v120-v121-method-audit.md`.
