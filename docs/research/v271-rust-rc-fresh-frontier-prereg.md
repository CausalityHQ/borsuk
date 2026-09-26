# V271 Rust RC fresh-query frontier

Status: preregistered; no measurement exists. Production library and lockfile
are frozen at `aa4182a2e6b78a254bca732663c7528818eb6561`. The V271 source
commit may add only benchmark harness and this preregistration, never change
that library or its dependencies. One `causality` c7i.4xlarge Spot instance in
eu-central-1c runs the sequential campaign; a 2-hour hard stop, S3 terminal
receipt, artifact SHA-256 replay, and immediate instance termination apply.
An interrupted cell is discarded and rerun under a new attempt ID.

## Frozen inputs

- CoHere-large-10M D768 cosine: first100,000 rows for the falsifier and
  first1,000,000 rows for the frontier, from the sealed V261 `vectors.raw`
  (3,072,000,000 B, SHA-256
  `6c82a340e3e1b4226640e593efa9c4000c6a5962d4b13063093a1dab689a9005`).
- Fresh holdout: canonical train rows1,000,000–1,000,999, excluded from both
  indexes, within `train-00000045.parquet` (67,122,282 B, SHA-256
  `1fae833dd9cbdb775b177176a2d301f0ee887988575b1152c13d7d0517cd25f4`).
  The sealed staging receipt SHA-256 is
  `0965aa0241199822dfac3410bba4edad5536ac0eb0aaa8ab83c216e8c5749a87`.
  The old canonical 1,000-query test panel is **not** reused.
- Strongest prior artifact: V269's immutable first1M generation, pinned root
  `1e483859b96f5270209678e0f76f7cc9e26a80162a24c7fa48a942cf010ec92e`.
  It is opened by the new public Rust `ResidentGraphGeneration::searcher()` API.
- Exact truth: FAISS `IndexFlatIP` over FP32 unit-normalized source rows, with
  the same 1,000 FP32 unit-normalized holdout queries and k100. Also compute
  100k truth for the first100 queries of that panel. IDs are source ordinals.

## Decision sequence

1. Run the generic Rust builder on first100k, open the authenticated root,
   and query the first100 fresh holdouts at k100. Stop before any 1M build if
   recall is below 9,800/10,000 exact GT100 hits, if authentication fails, or
   if the 3 GiB resident cap is exceeded. This threshold only rejects a
   collapse; it is not a published quality target.
2. If the falsifier passes, build a generic Rust first1M generation once,
   authenticate and query it on all1,000 fresh holdouts. Separately hydrate
   and query the V269 generation through the same public Rust API. No parameter
   tuning of BORSUK is allowed: PQ ef4096/shortlist4096 and exact ef2048.
3. On the same host and fresh panel, build a standard FAISS IVF-Flat cosine
   control: 2,048 lists, 100,000 evenly spaced training rows, one build,
   nprobe `{16,32,64,128,256,512,1024}`. Pin `faiss-cpu==1.15.1`
   ([upstream release](https://github.com/facebookresearch/faiss/releases/tag/v1.15.1))
   and record the installed wheel. This is an algorithmic resident-memory
   control, not a vendor service comparison.

All arms use one query at a time after a warmup, with no response cache.
Report raw per-query IDs and latency; calculate recall@100 and p50/p90/p95/p99
from those same samples, throughput QPS, peak RSS, resident bytes, build time,
startup/hydration time, GETs/bytes, source and artifact hashes, EC2 identity,
Spot quote, elapsed cost, and failures. The Rust and Python API timings have
different language overhead; label them as library-level, not HTTP/product or
matched vendor latency. V269 prior-used numbers are historical context only.

The generic builder is a GO for a 10M scale/cost gate only if its fresh 1M
recall is within 0.5 percentage points of the prior V269 artifact, p95 is no
more than 20% higher on this same host/panel, it stays within the measured
resident envelope, and the immutable generation survives an authenticated
reopen. Otherwise stop and choose one build/training or format repair from the
sealed evidence. No S3 Vectors or Turbopuffer superiority claim follows from
this library-level campaign.
