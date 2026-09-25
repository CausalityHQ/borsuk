# V246 V245 graph, authenticated S3 generation and VPC-peer HTTP

One frozen two-host `causality` c7i.4xlarge Spot attempt in
`eu-central-1c`. Publish V245's 1M graph under a fresh S3 generation
head using the existing authenticated graph store, then hydrate that
head into an empty server cache and serve through the existing
eight-worker HTTP handler. A separate client uses eight persistent
HTTP/1.1 connections for first and immediate-repeat passes, with no
response cache. This is resident query serving after cold S3 startup
hydration, not a cold object-store GET per query.

Authority: V245 terminal SHA-256
`632bb364eb026220116f45e7e5ff5f980d197a4d0668c491773344cfd03ff689`
and graph SHA-256
`92df3782b2836e608d206401c4efdd3d71bd8e81fe18887b0e93af4d23d8ade3`.
The new generation root SHA-256 is
`58dcfbda4e5dec2402043c44e0e7f02b649d1d0a46d69ab1be48c61cf5add3dc`.
It retains V245's authenticated FP16 plane, map, PQ books/codes and
generation 196, replacing only the graph identity. Expected five
graph blob GETs total **1,879,696,738 bytes**; metadata GETs are
reported separately or disclosed as uninstrumented.

Dataset: ReLAION-1M D768 cosine k=100, validation queries 0–999
already used, dev 0–255 and remaining 256–999. The V245 in-process
reference returned 99,662/100,000 exact GT100 hits (dev 25,506,
remaining 74,156), p05=98 at ef/shortlist 4096/4096. V237's prior
persisted HTTP serving baseline on the same corpus/query panel but
different graph and mutation revision was first-pass
p50/p90/p95/p99 19.223/24.015/25.254/27.783 ms, 395.8 QPS,
99,668 hits and 2,115,366,912 B server peak RSS. It is historical
BORSUK context, not an exact same-topology or same-revision speed
comparator.

Each V246 pass must match all 1,000 complete V245 4096/4096 ID
lists; exact hits must be 99,662/100,000 and p05≥98. Frozen
resource gates: client-observed p95≤35 ms, p99≤45 ms, throughput≥300
completed QPS, server peak RSS≤3 GiB, zero query vector-body GETs.
Report p50/p90/p95/p99 from each pass's own sealed raw query samples,
request/response bytes, both hosts' compute quote and elapsed cost,
startup hydration and blob bytes. S3 publication must be a fresh
conditional head with authenticated readback; the HTTP server must
authenticate the same root and GET all five blobs from an empty cache.

Reserve source archive and immutable attempt prefix before launch.
If Spot interrupts either host, discard the whole cell and restart
under a new attempt. Preserve original terminal and artifact hashes,
terminate both hosts after terminal markers, and do not inspect an
incomplete measurement stream. Pass promotes one direct fresh-index
S3 Vectors comparison under the same frozen panel, k, client class,
concurrency and region. S3 Vectors server cache remains opaque; label
first/repeat semantics and transport/TLS differences. Turbopuffer's
published cold result is context until authenticated access exists.
