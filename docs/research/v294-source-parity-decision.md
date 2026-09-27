# V294 decision: source parity PASS

CoHere first100k D768 raw source, query-blind. Source mean and all first196
bytes of each 200-byte record match the sealed Python reference exactly:
512-row sample exit0, then100000-row full exit0. The Rust v1 final scalar is
inverse reconstructed norm; that field intentionally differs from the
historical source norm and is not covered by prefix parity.

| Check | Rows | Wall seconds | Peak RSS KiB | Exit | Result |
|---|---:|---:|---:|---:|---|
| Python reference | 100000 | 7.97 | 544952 | 0 | source-only export |
| Rust sample | 512 | 40.04 | 399548 | 0 | exact mean/prefix |
| Rust full | 100000 | 80.23 | 418080 | 0 | exact mean/prefix |

These are local debug-build verification costs, not index-build throughput or
query latency. All three report zero swaps. No paid instance or full suite.

Original reference receipt SHA256:
`f40cbfedbfe4a589926bdf380372253fedee55858c84db461596c246e2f2752a`.
Rust full v1 plane SHA256:
`224d6c64758106f8d67ef1cc1a1f9a2088df9c7811d420c0d1a003f45a5db596`.
Raw results and exact time receipts accompany this decision. Large source
planes remain in the local V283 scratch; source scripts and input digests
permit replay. V295's frozen development page-selection parity is next. This
pass does not qualify cross-dataset recall, S3 serving, scale or vendor wins.
