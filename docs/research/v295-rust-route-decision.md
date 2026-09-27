# V295 decision: exact frozen route parity PASS

Source `e22d8a4c`. CoHere first100k D768 cosine k100 development0–63.
Four-query screen exit0 (2.51 seconds, 25,352 KiB peak RSS), then all64
exit0 (14.21 seconds, 25,388 KiB peak RSS). Both satisfy the preregistered
2 GiB / 600 second local caps. The public Rust query lookup scorer and reused
budgeted page planner produced the exact V293 physical page sets for every
query. The full plans serialize to the same SHA256:
`00190ff7ed017b2150d1415f36077be774c0e59075ec0eb0e691f81511ae3aa4`.

Therefore reuse V293's independently authenticated exact Rust SQ8 replay
without repeating it: same root, coefficients, source payload, queries and
pages, hence identical fetched rows and final scorer inputs. This verifies
code parity; it adds no independent quality sample.

| Frozen route, development64 | Mean | p05 |
|---|---:|---:|
| Candidate GT100 coverage | 99.578125% | 98 |
| Fetched GT100 coverage | 99.546875% | 98 |
| Exact returned recall@100 | 98.015625% | 96 |

Maximum planned32 GET, 16,773,120 bytes; maximum40,704 compressed row scores.
The source code plane uses200 bytes/row (20 MB here; 20 GB at100M is a
projection before centroid metadata, query buffers or pinned generations).
No full source-vector cache is used by the Rust lookup route. All records
are resident nomination metadata in this local diagnostic; actual S3 reads,
HTTP transport, concurrency, scale and total memory remain unqualified.

**GO only to frozen paired ReLAION/CoHere100k validation256–999.** The mean
margin remains one GT hit across64 development queries. Apply mean>=98%,
p05>=95 and paired flat loss<=0.5pp, <=32 GET/16 MiB before authorizing any
1M cold HTTP run. No parameter sweep, paid run or vendor claim is justified.
Raw result/time receipts accompany this decision. Debug replay costs are
verification wall/RSS, not product latency or throughput.
