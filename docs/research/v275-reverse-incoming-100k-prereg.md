# V275 one-shot reverse-incoming graph falsifier

Status: preregistered before candidate measurement. The frozen V272 10M
CoHere graph missed 701/100,000 GT100 hits. A read-only audit independently
replayed its authenticated graph and query/truth artifacts: median base-layer
in-degree was 99 for hit GT rows versus 7 for missed GT rows; 55.8% of misses
had in-degree ≤8 (1.4% of hits). This identifies weak incoming connectivity
as a credible 10M failure mechanism. V273 efConstruction256 and V274 one-hop
reranking were rejected. The V274 100k stress-panel misses had median
in-degree 31, so its quality response alone is not a scale-equivalent proxy.

## Fixed candidate and inputs

Use the V271 authenticated 100k root
`440beffd321dfeeae25ba6277a2e2f1f938c389a0c4a9007177f4bacc4b1d6e3`
as baseline. Candidate builds from the exact same CoHere-large-10M canonical
train rows0–99,999 (D768 cosine). Keep M32, M0=64, efConstruction128, worker
cap8, source FP32, PQ and FP16 unchanged. After the existing diverse build and
reachability repair, process rows with in-degree <16 in increasing original
in-degree order. For each, consider its 16 closest geometric outgoing base
neighbors; protect existing reverse edges or insert a reverse edge by replacing
the farthest RNG-backfill edge. Never evict the cycle backbone, an already
protected edge, or an edge whose target has in-degree ≤16. Cap protected edges
per source list at16. Degree and persistent graph layout remain unchanged.
This is one fixed topology algorithm, with no ef, degree or cap sweep.

Source first100k raw FP32 SHA-256:
`0f3631d71c105e5ea3d701c96033b362c2f84bd43002a9c8a5c70040801be06e`;
ID-plus-vector source identity:
`d638878523cfdd349cb28e214d59010709d6451f3eeca2a70da88c0c9d9ea753`.
Fresh excluded queries are train rows102,000–102,999, distinct from V271,
V273 and V274 panels. Query raw SHA-256:
`51cbd05d06c84f75c7f3394e42366a1e963447c0ebc90233351409cfda6a44c3`.
Truth is FAISS `IndexFlatIP` on FP32 unit-normalized source/query vectors,
k100. Authenticate the baseline root and blobs against V271's terminal; build
one candidate generation and open it by its new trusted root SHA.

## Measurement and decision

One `causality` c7i.4xlarge Spot host in eu-central-1, two-hour hard stop,
single terminal attempt. Build candidate once. Run baseline and candidate in
separate processes on the same 1,000 queries after one warmup, sequentially,
one worker, zero query-time GETs. The primary diagnostic widths are PQ ef256,
shortlist256, exact ef128; also record the default library widths PQ4096,
shortlist4096, exact ef2048 as a separate 100k context arm. Record per-query
IDs/visits/latency and p50/p90/p95/p99 from each arm's same raw samples;
recall, p05 hits, graph in-degree distributions, per-node degree equality,
reachability, build/search RSS and time, artifact sizes, instance/quote/cost
and all SHA-256 identities. Upload artifacts and terminal before terminating
Spot. An interruption discards the cell.

The primary falsifier is inconclusive if the baseline has <100 GT100 misses
among rows whose **baseline** graph in-degree is ≤8. Otherwise it passes only
if candidate misses in that fixed low-degree GT subgroup fall ≥30%, overall
misses do not rise, p05 hits do not fall, mean visits and p95/p99 are each
≤110% of the paired baseline, and search RSS is ≤110% of baseline. In
addition, the all-row fraction with in-degree ≤8 must fall at least 50%,
every node must retain its baseline out-degree, min in-degree must stay ≥4,
the graph must remain fully reachable, and authenticated open/search must
succeed. Candidate build time and peak RSS must stay ≤150% of the V271 100k
builder receipt. Failure rejects the candidate; a pass permits one fresh 1M
gate at default widths and no direct 10M rerun. The 1M gate must beat V271's
99,771/100,000 hits with p95 ≤61.314 ms and its serving RSS before any new
10M scale/cost run. These in-process timings are not HTTP or vendor latency.
