# V262 CoHere first1M authenticated dual-graph HTTP closeout

**Decision: pass.** The production generation loader and eight-worker HTTP
server preserved every V261 result and met the frozen client latency,
throughput, recall, and memory gates. Keep this authenticated dual-graph
serving path as the current 1M architecture. This is a resident-after-cold-
hydration product measurement, not a matched S3 Vectors or Turbopuffer win.

Dataset: CoHere-large-10M canonical first1,000,000 source rows, D768 cosine,
k100, prior-used query panel ordinals0–999 (development0–255,
validation256–999). The five closed V261 artifacts and search parameters
were reused under generation root SHA-256
`1e483859b96f5270209678e0f76f7cc9e26a80162a24c7fa48a942cf010ec92e`.
V262 serving source commit is `a80b855ef325e8c395754cac69f63f2506baa4ae`;
the V261 in-process predecessor terminal SHA-256 is
`00c7d4803354f15d5f62bea6e53fd0672a0f5fc91cc482e1fa4b20b8951a9f82`.

| Sealed pass, 1,000 queries | V261 ordered lists | GT100 hits / 100,000 | Client p50 / p90 / p95 / p99, ms | Throughput, QPS |
| --- | ---: | ---: | ---: | ---: |
| First | 1,000 / 1,000 | 99,717 | 116.236 / 142.162 / 149.749 / 158.941 | 67.13 |
| Immediate repeat | 1,000 / 1,000 | 99,717 | 116.049 / 141.497 / 147.844 / 157.258 | 67.37 |

Both passes had development25,511/25,600 (p05 98 hits/query) and
validation74,206/74,400 (p05 99). Timings are each pass's own raw
client JSON encode, persistent HTTP/1.1 VPC peer exchange and JSON decode
samples, with eight connections and no response cache. First and repeat raw
SHA-256 were `2fbc989c1356cb4f70fc664946b75e6c836289eb54b2f301267f4cc8babcd28b`
and `3614ad8e64f52a67c463308834347dc2ab30d4a8242c8d80f4dcb5d02ca7e16f`.
Each pass sent 15,356,605 request bytes and received 738,866 response bytes.

Server peak RSS was 2,050,895,872 B. Empty-cache startup fetched exactly
five authenticated blobs totaling 1,880,914,634 B in 34.9996 s; queries
made zero vector-body GETs. Two c7i.4xlarge Spot instances in
eu-central-1c cost about $0.07015 through terminal at the recorded
$0.3663/host-hour quote. Server `i-0d59d8d214d23587a` and client
`i-07afaa2d0c740b89a` both wrote complete terminals and the original
launcher confirmed termination. No BORSUK instance remained active.

Immutable S3 prefix:
`research/v262-cohere-dual-http-1m/a80b855ef325e8c395754cac69f63f2506baa4ae/runs/a0001/`
in `borsuk-bench-453182569524-euc1`. Readback verified client terminal
SHA-256 `fc08573c72b974363f963bd40074c9c64562530dcffff0491a59cc59ca4ef446`,
server terminal SHA-256
`4899280b8aed1d6b31de4790e0eb67bbe20b132c9f2d8434687b0fcb40f068c9`,
and closeout SHA-256
`eca88b883d56387ced98632a1df541ef9134b204b635b2b42e6732bda45215a9`.

Next gate: a direct S3 Vectors CoHere comparison on this exact corpus,
query panel and GT100 truth, disclosing its ingestion cost, cold/cache
semantics, transport, same-sample p50/p90/p95/p99, recall, throughput,
resource use, and cost. Do not add V261 in-process latency to V262 HTTP
latency or treat unequal transport/cache as an apples-to-apples vendor win.
