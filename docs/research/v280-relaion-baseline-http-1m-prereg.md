# V280 ReLAION-1M baseline HTTP serving gate

Status: preregistered before any V280 launch. V279 terminal SHA-256
`b7c14d3917a596d7d5451a15cd48196332651af7a89a3aa3228d9b9f323c74e7`
and closeout SHA-256
`ac7c7f36c78e45c1ade5f9843c1950255e4181f8bf8176cbbdcc554ce669b3da`
select the baseline generation. No graph rebuild, parameter sweep, or new
quality target is part of this gate.

## Frozen identity and workload

ReLAION-1M D768 cosine, k100, validation ordinals0–999 (historically used).
Source Parquet SHA-256
`2796b579f37afe99ca4aff57e282335a6a79ad30596645957d26326a0560cf86`;
request JSONL SHA-256
`c250ef3c871af55ee1ea91e61214a93903b3d575ef458926b1f8c5ad0547b2c9`;
normalized FP32 queries SHA-256
`82fe696c1d765d27b9f8f6c5277a998a4ebbf2bd6d6f89533ad3a5445105525a`;
FAISS exact cosine GT100 SHA-256
`23180f9b7e7727f478672d919e08e7bc17bdb75a76c9308d69dbc44c41bc1db7`.
The V279 baseline authenticated root SHA-256 is
`bdc994e1f58c817ebfd95c7ddaa3aca37fe182f7dc73a569a61ad39a73ec2565`.
Its five blobs total1,879,999,990 bytes. Reference local default raw
SHA-256 is `291c10bcea15003959d556fdb7827522a498c166f3ea3a9dca30480ecc733d41`:
99,989/100,000 GT100 hits. The source revision for the HTTP harness must be
recorded separately; do not relabel V279 local timing as an HTTP result.

One eu-central-1c c7i.4xlarge Spot server and one same-class Spot client.
The server publishes the unchanged V279 baseline blobs to a fresh immutable
generation prefix, authenticates a pinned head/root and all five blobs from
an empty local cache, then serves eight bounded workers. The client runs
eight persistent HTTP/1.1 VPC-peer connections, once and immediately again,
using all1,000 requests in the same worker-strided order as V269. No response
cache. Client timing includes JSON encode, HTTP exchange and decode. Record
each raw per-query ID list, latency and visits, then same-sample
p50/p90/p95/p99, QPS, request/response bytes, server peak RSS, cold
hydration duration/GETs/bytes, query GETs, region/instance IDs and quote.

## Frozen decision

Both passes must return all1,000 ID lists exactly equal to V279 baseline
local output and therefore 99,989 GT100 hits, R@100=0.99989. This is an
exactness guard for the serving path, not a demand for perfect recall.
The measured service gate passes if each pass has p95≤110 ms, p99≤130 ms,
QPS≥85, zero query-time vector GETs, server peak RSS≤3 GiB, and cold
hydration reports exactly five authenticated blob GETs and1,879,999,990
response bytes. A failure must be diagnosed at the serving/harness layer
before any scale run. V269 CoHere HTTP and direct S3 Vectors results are
historical context with different source revision, dataset or cache/transport;
they are not a paired V280 control. No Turbopuffer win follows from this gate.

Reserve one attempt only after the source archive and harness tests pass.
On Spot interruption, discard both hosts and restart the entire cell under
a new attempt ID. Sync each terminal and all raw artifacts to S3, replay
SHA-256 and sizes, and terminate both instances at terminal. Record the
launch-time Spot quote and label compute cost as an estimate, not a bill.
