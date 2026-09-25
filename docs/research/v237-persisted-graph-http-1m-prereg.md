# V237 persisted mutation collection HTTP, ReLAION-1M

One frozen two-host `causality` c7i.4xlarge Spot attempt in
`eu-central-1c`, server and VPC-peer client. The server reads V236's
real S3 collection head at revision 2, authenticates the single graph
root and mutation snapshot, hydrates into a fresh local cache, then
serves the decoded overlay through the existing eight-worker HTTP
example. The client reuses the frozen V233 1,000-query request file,
eight persistent HTTP/1.1 connections, scorer and exact GT100 witness.
Both first and immediate-repeat passes must finish on the same two
instances. No response cache is used. This is resident serving after
cold startup hydration, not cold per-query retrieval.

Authority: V236 terminal SHA-256
`5f32aca133f8544d23f13e30e2f9c469ca87a2b3f626fdbc8cf4eb029989c7d8`
and closeout SHA-256
`bb6d7cf60d62ee06e486951fc193dc921d4f13c69a16654344312d9ce772da6c`.
The published S3 collection prefix is the V236 `a0001/published`
subtree; root SHA-256
`c59650ec920d031ff236f5ab47331db88b71fdac0462cd07a568d51c549b7caf`,
mutation SHA-256
`6b9ba6391dff9de0867447045504beaa1c22776ccf9502d4065c7a3b76ab4153`.
The comparison is V233 decoded mutation HTTP, terminal and closeout
identities in its closeout, with first p95 32.016 ms and 319.2 QPS,
repeat p95 30.622 ms and 327.0 QPS. Both sides use ReLAION-1M D768,
previously used validation ordinals 0–999, cosine k=100,
ef/shortlist 4096/4096 and 10,000 same-vector upserts.

Gate on **each pass**: all 1,000 complete ID lists equal sealed V233
and V230, 99,668/100,000 GT100 hits with splits 25,509/25,600 and
74,159/74,400, p05≥99; p95≤1.20× matched V233 pass and QPS≥0.80×;
report p50/p90/p95/p99 from the same sealed raw samples. Server peak
RSS≤3 GiB, overlay≤64 MiB, zero query vector-body GETs. Cold startup
must authenticate expected root and snapshot and fetch exactly five
graph blobs totaling 1,879,697,462 bytes. Record hydrate wall time,
client request/response bytes, transport scope and compute cost.
Collection metadata GETs are not instrumented; disclose that limit.

Reserve immutable attempt prefix and source archive before launch.
Seal each raw stream before downloading truth. On Spot interruption,
discard the whole two-host cell and use a new attempt prefix. Record
instance IDs and terminal artifact hashes; terminate both hosts
immediately after terminal markers. If the gate fails, diagnose the
persisted startup or serving layer before a further 1M run. If it
passes, decide the production API and memory/compaction envelope from
measured recall, latency, RAM and cost rather than a vector-count knee.
