# Source parity and sealed prospective cohort: construction PASS only

Both current frozen first100k raw-source hashes were reproduced before any GT
publication.1000 query vectors per corpus from fixed source rows100000–100999,
outside the indexed first100k, and exhaustive f64 cosine GT100 are sealed in S3.
This is source/input engineering evidence, NOT fresh ANN quality or production
qualification. Main agent opened no sealed vector/request/truth bodies and no
native ANN query/scorer ran. No old threshold/mechanism/source/scorer was tuned.

## Authority and measured construction

ReLAION source is the pinned **andropar/relaion2b-natural-embeddings** corpus,
revisionbfc7465dcf1245bd605d35dcaf5d2177bbc2025a, from the existing V36 first1M
parquet. Streaming its first100k reproduces rawSHA
0d55a09756f41361c89f0ca355a3bc98da6799b4dc7ea39ab34a905844a758c2.
CoHere source is the canonical staged cohere-large-10m-768 prefix, rawSHA
0f3631d71c105e5ea3d701c96033b362c2f84bd43002a9c8a5c70040801be06e.
Source rows are originalf32, finite/nonzero, canonical shard order; fixed-size
D768 columns and artifact byte/SHA/count authority checked. Stream8192-row
batches; the1M ReLAION shard is never fully allocated. This establishes the
first100k prefix equality missing from the old source-fit cost receipt; it does
not merge old and new benchmark architectures/measurements.

Exhaustive oracle: normalize originalf32 source/query in f64, cosine distance
1-dot, full100k ranking with signed source-ID tie order. NumPy2.3.3/AL2023ARM/4threads,
16-query matrix batches; scalar check on fixed2queries×3rows within1e-12, finite
score bounds.2000 exhaustive ground-truth query scans total, **zero ANN queries**.
No SQ8 or native metric approximation in oracle. No cutoff ties; minimum100th/
101st cosine margin R2.2112831898901675e-8,C1.7426816212307017e-8, far above the
scalar-check floating tolerance. Oracle/construction-plus-upload phase wall
13.630464R/13.618596Cs; this is NOT per-query serving latency/QPS.

Immutable sealed keys and identities are in `fresh/a0002/verification.json`.
Each raw query body3072000B, GT400000B; requests R14673389B/C15368978B. Conditional
create and SHA metadata. Independent verifier hashed all12 closed metadata
artifacts, matched395sourcefiles incl392 unchanged Rust/Cargo, checked source
parity and six S3HEAD byte/SHA identities, and actualtermination **without GETting
sealed contents**. Same2689-pass compiled core/scorer authority, no new Cargo gate.

Original native session48420 closed0; instance`i-03ff41564af5fe2d5` **terminated117s**,
Causality eu-central-1c c7g.2xlarge Spot,4CPU/4threads,8GiBcgroup/4GiBprocess/zeroSwap,
80GiBencrypted disposableEBS. Compute estimate$.0058 excludesEBS/S3, not invoice
or lifecycle dollars. Cgroup peak3395739648B includes file cache/source extraction/
oracle/kernel,swap0/OOM0. Source archive
4b9b78f3f74c863930f7d03b9b0bc4d64377f89324985c5abb708cad94c6b319,
terminale69fcf4d3e8c9a76d905646920bc0aa92546d44682c675e45b6eeae4cc0efe98.
No active compute remains.

## Rejected first prospective source and cost correction

A0001 used VDBBench's staged **laion-100m** prefix, which is not this frozen
ReLAION corpus. Required0d55a097... vs measured46b30b50ebc731c42604351793517112843c1ff1a3cb1f7daaec9f02eb9dc5df.
The exact source gate rejected it BEFORE CoHere or any GT publication/ANN quality.
Retain as engineering INVALID prospective corpus, not mechanism KILL. The pinned
V36 source lineage supplied the correction; no substitution of dataset identity.

Instance`i-0b83f4d01b9fa2ef9` terminated76s,9metadata artifacts/395source files
verified before changes. Original launcher deadline text replacement accidentally
changed seconds-per-hour cost divisor3600→900. Original erroneous closeout$.0153
is retained; **corrected compute estimate$.0038** from observed0.179900/h×76s/3600,
in `corrected-cost.json`, supersedes it. Fixed shared launcher arithmetic, no
measurement/body rewrite. Original source/config/preregistration remain in its
immutable archive; current files represent the corrected attempt.

## Current convergence and next decisive gates

Latest VERIFIED ANN quality remains both first100k,D768,cosine,k100 consumed
method-validation256–999(744): ReLAION matched nearest98.850806%→union99.463710%,
flat99.568548%,gap.104839pp; CoHere99.059140%→99.215054%,flat99.315860%,gap.100806pp.
See validation-decision.md. Latest dev0–63 native cold candidatep90/p95ms
R108.488395/109.708413,C106.605597/107.813765; serialQPS9.419019/9.504854,slightly
slower than matched nearest. No new timing/quality from prospective sealed panels.

1. **Novelty is still a gate.** Disjoint indexed rows do not alone prove these
   vectors were never used as source pseudoqueries in older1M research. Audit
   against known query/pseudoquery authority before calling them genuinely unused.
   Do not peek ANN quality, filter by difficulty or quietly change the fixed panel.
   The cohort is for the100k index only; its source rows are inside a1M index and
   cannot serve as held-out1M queries. Source/GT construction is PASS, not fresh
   qualification or official test-distribution representativeness.
2. **1M source/build/maintenance feasibility.** Preserve flat-fitter120s KILL; do
   not extend/rerun it. Existing hierarchical source fitter has measured1M mechanics
   but mean/minimum extent query routes remain KILL. Use the surviving native unit-
   graph union mechanism with a separately qualified scalable physical order;
   fitter success alone is not recall/serving evidence. Actual callable build and
   physical cold/maintenance gates remain required. No new architecture/review loop.
3. **100M phase memory and BOTH vendors.**39.5125GB resident arithmetic is projection,
   currentdisk×3ledger/unprovensteady48GiB/pinned96GiB and maintenance rewrite/quality/
   total lifecycle costs remain open. Equivalent S3 Vectors/Turbopuffer access,
   matched recall/cache/region/concurrency/performance/dollars unmeasured.

Goal remains ACTIVE; no production/default freeze or completion. No operator
choice is required for source/novelty/scalable-layout engineering preparation.
