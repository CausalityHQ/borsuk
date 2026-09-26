# V269 CoHere first1M: authenticated cached dual graph over HTTP

V268 passed the frozen first1M quality/resource gate with 1000/1000
ordered IDs equal to V261 and 99,717/100,000 GT100 hits. V269 wires
that cached search method into the production generation loader and
eight-worker HTTP server. The five persisted artifacts and trusted
generation root are unchanged. Load from an empty server cache, then
run one first and one immediate repeat client pass without a response
cache. V262 is the historical same-artifact VPC-peer HTTP baseline on
the older FP64 dual route, from another source revision.

Dataset: CoHere-large-10M canonical first1,000,000 source rows, D768
cosine k100; prior-used test ordinals0–999, development0–255,
validation256–999. One c7i.4xlarge Spot server and one c7i.4xlarge
Spot client in eu-central-1c, persistent HTTP/1.1 VPC peer, eight
concurrent connections. Both passes must return the exact V261/V268
ordered k100 lists, 99,717 GT100 hits with development25,511 and
validation74,206, p05 98/99. Require each pass client p95≤110 ms,
p99≤130 ms, throughput≥85 QPS; server peak RSS≤3 GiB; exactly five
authenticated cold blob GETs totaling1,880,914,634 B and zero query
vector-body GETs. Report each pass's same-sample p50/p90/p95/p99,
QPS, request/response bytes, cold hydration, RSS and both-host cost.
These thresholds are a production decision gate, not forecasts.

If GO, keep this method as the current first1M production architecture
and compare against direct S3 Vectors on disclosed matched quality,
transport, cache state, concurrency and cost. Turbopuffer published
cold p90 is context only until direct authenticated access exists.
If NO-GO, inspect the closed same-revision HTTP samples and make one
server/transport or architecture decision before any 10M run.

One immutable source archive and S3 prefix; terminal and artifact
size/SHA-256 replay for both hosts; terminate both immediately after
the terminal. Discard and restart the entire cell after interruption.
Before terminal, monitor only markers and infrastructure. Do not read
partial client samples.
