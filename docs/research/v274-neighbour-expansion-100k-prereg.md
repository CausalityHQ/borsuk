# V274 bounded exact-neighbour expansion falsifier

Status: preregistered before candidate measurement. V273 rejected a stronger
graph-construction policy: on its first100k CoHere stress panel it reduced
misses only 10.28% and exceeded both preregistered tail-latency limits.
The production builder returned to the V271 RC topology.

## Fixed candidate and inputs

Use the V271 authenticated first100k graph root
`440beefd321dfeeae25ba6277a2e2f1f938c389a0c4a9007177f4bacc4b1d6e3`
for both arms. Baseline is its dual graph at diagnostic widths PQ ef256,
shortlist256, exact ef128. Candidate takes that same final FP16-ranked k100,
adds base-layer neighbours in rank order until at most 32 × k physical candidates
have been gathered (k=100, cap3,200), deduplicates, then reranks the union
by the same FP16 cosine score. It has no extra persisted graph, query-time
GETs, or trained parameters. No search-width or expansion-cap sweep.

Dataset: CoHere-large-10M canonical train first100k, D768 cosine, from V261's
sealed FP32 `vectors.raw`. The first100k raw bytes SHA-256 is
`0f3631d71c105e5ea3d701c96033b362c2f84bd43002a9c8a5c70040801be06e`;
the ID-plus-vector source identity is
`d638878523cfdd349cb28e214d59010709d6451f3eeca2a70da88c0c9d9ea753`.
Fresh excluded queries are train rows101,000–101,999, distinct from V271 and
V273 panels. The 3,072,000 raw query bytes SHA-256 is
`ba64f7134f252314e12cac2b1fe8911a87f6d7112e7a472bff81d33ac86ee822`.
Truth is FAISS `IndexFlatIP` on FP32 unit-normalized source/query vectors,
k100. Authenticate the V271 root and every blob before opening it.

## Measurement and decision

Use one `causality` c7i.4xlarge Spot host in eu-central-1, with a two-hour
hard stop and one terminal attempt. Run each arm in a separate process after
one warmup, on the same 1,000 queries sequentially, one worker and no
query-time GETs. Record raw IDs, visits, per-query latency, p50/p90/p95/p99
from the same samples, recall, p05 hits, search RSS, instance/quote/cost and
all input/output SHA-256. Upload artifacts and terminal before terminating
Spot. An interruption discards the cell and requires a new attempt ID.

The test is inconclusive if baseline misses are fewer than 100/100,000.
Otherwise the candidate passes only if it cuts misses at least 30%, does not
lower p05 hits, keeps p95 and p99 at most 20% above the paired baseline, and
keeps peak search RSS at most 20% above baseline. Any error or failed
authentication rejects the candidate. A pass permits one fresh 1M gate; it
does not establish 10M quality, HTTP product latency, or vendor superiority.
