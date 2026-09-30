# Performance architecture investigation: independent reconciliation

No replacement architecture is selected. The current valid graph-decode a0004
candidate and its receipts remain intact. Fable consultation `72453a13276b4bba`
completed; its raw result is preserved separately. The operator explicitly
authorized this broader investigation after seeing the measured cold gap.

## Independently reproduced critical path

`python3 -m scripts.check_native_cold_critical_path critical-path.json` authenticates
the accepted verification, terminal and all four closed record bodies. It checks
all 256 records and each 64-query arm/dataset roster. Individual duration
partitions must fit the enclosing cold interval. It never sums marginal quantiles.

FIRST1M D768 cosine k10, consumed development queries 0–63:

| Candidate quantity | ReLAION | CoHere | Evidence scope |
|---|---:|---:|---|
| Cold p90 | 1242.809 ms | 1289.024 ms | Measured |
| Head-read p50 | 92.935 ms | 92.004 ms | Measured interval; TLS share unknown |
| Staging p50 | 447.007 ms | 442.886 ms | Measured |
| Decode p50 | 268.399 ms | 268.539 ms | Measured aggregate; component split unknown |
| Incoming HTTP p90 | 400.611 ms | 430.743 ms | Measured; source/SQ8/CPU split unknown |
| Query verified bytes p50 | 39,633,920 | 40,376,320 | Measured source + SQ8 payload |
| Source / SQ8 GET counts p50 | 128 / 32 | 128 / 32 | Native submitted counts |
| Cold metadata bytes | 57,694,505 | 57,694,558 | Measured, every query |
| Cold p90 after per-query subtraction of decode | 976.233 ms | 1020.385 ms | Conditional trace arithmetic |
| Cold p90 after per-query subtraction of ALL remote open | 509.384 ms | 543.043 ms | Conditional trace arithmetic |

Subtraction is an idealized erasure with every other recorded interval held fixed,
not a measurement or prediction of a new system. It nevertheless shows why changing
only recorded startup work cannot close the observed 444 ms context gap. Query
fetch/scoring and protocol setup must also change. Current offered QPS remains
unknown; historical predecode 2-QPS results are not a matched current control.

## Corrections to the research proposal

1. Summing separate p50 values cannot prove a latency lower bound. Use per-query
   partitions and the authenticated subtraction above. The proposed 360 ms p50,
   540–640 ms alternative and linear 10M/100M timings remain untested estimates.
2. The proposed SQ8 router centroids do not exactly match current FP16 centroid
   scores. A first distinct router falsifier should retain exact FP16 values;
   test router quantization separately. This doubles the proposed router leaf
   payload: 256 × (768 × 2 + 4) = 394,240 bytes before headers/digests. A 16-leaf
   request therefore needs at least 6,307,840 bytes, not the proposed 3.2 MB.
3. A count of 128 GETs at concurrency 16 is not a measurement of eight serial
   round trips. `fetch_verified_ranges_inner` uses an ordered buffered stream.
   Ordered completion can delay admitting later reads; actual phase/connection
   timing is missing. This is a concrete scheduling hypothesis, not a measured
   attribution. Preserve ordering, byte admission and failure charges if tested.
4. The 2544-unit ceiling times 32 rows times 200 bytes is a bound, not an actual
   minimum needed source payload. Current source planning fetches full 256-row
   page closures for adaptive completion. Dropping that closure changes routing.
5. Reordering physical units changes page geometry, source identities, plans and
   references. A trace-cover oracle is conditional on old discovered units and
   cannot qualify query-blind tree nomination or predict returned quality.
6. Current callable compaction rebuilds a complete generation through
   `prepare_two_bit_compaction` and `TwoBitGenerationBuilder`. Node-local immutable
   rewrites, splits, reassignments and ancestor updates are new engineering work,
   not something the proposed tree can reuse unchanged. Lifecycle equivalence
   needs actual mutation, recovery, pin and GC tests plus rewrite-cost evidence.
7. The current cold boundary launches a process and constructs the S3 client for
   every query. Published vendor namespace-cold protocol is incompletely known.
   Preserve this boundary for matched internal arms; later measure process-cold
   and namespace-cold separately rather than retroactively relabeling results.

## Prior failures and the distinct hypothesis

V139 failed a centroid threshold/physical-witness mismatch on used ReLAION data.
V149's summaries of consecutive physical pages missed candidate pages; V150 lost
pages both in bounded discovery and in converting nearest units to distinct pages.
Those frozen verdicts remain failures. They are not evidence that every semantic
hierarchy fails the new 95% recall@10 product floor. Conversely, naming a new tree
or replacing row copies with router-entry copies does not resolve boundary loss.

The distinct hypothesis worth falsifying is a query-blind semantic partition of
exact unit centroids, with bounded router-only boundary coverage and collocated
payloads, whose root and requested nodes replace full router hydration. It must
demonstrate useful nominations and byte/GET geometry together. A blanket rewrite
before that test would risk repeating the closed arms.

## Ordered delivery

1. Finish and preserve the already compiled bulk-decoder mechanism and its bounded
   controller qualification. It is an incremental candidate, not the product fix.
2. Build one source-only semantic router/physical-unit mapping using existing
   fitting code. Declare FP16 scoring, closure policy and replication bound before
   examining query quality. First inspect construction/storage and conditional
   trace geometry; do not call an oracle cover a latency or recall measurement.
3. Run the cheapest paired ReLAION-first 100k returned-quality falsifier, then
   CoHere, decomposing discovery, coverage, two-bit selection and SQ8 loss. Use
   mean recall@10 >=95%, report recall@100 separately, contemporaneous control,
   fixed memory/GET/byte caps and cold tails. A changed format gets new authority.
4. Only a survivor earns representative fresh 1M cold plus offered 8-QPS and
   saturation/cost measurements. Derive 10M/100M latency from a measured curve;
   qualify bounded building, incremental maintenance, pins and recovery in-process.

The proposed tree is a multi-day implementation with unresolved build/lifecycle
costs. The immediate useful deliverable is a falsifiable source/router geometry
and quality gate, not another unmeasured performance forecast.

## Primary-source context checked 2026-09-30

[Turbopuffer ANN v3](https://turbopuffer.com/blog/ann-v3), updated May 05, 2026,
describes hierarchical SPFresh and binary quantization with refinement. Its
100-billion-vector result uses distributed compute with the full tree on SSD;
it is not a no-cache cold latency guarantee. The relevant architectural lesson
is bounded cold fetch depth and useful spatial locality, not importing its
cached throughput or tail numbers into BORSUK release gates.
[SPFresh author page](https://ustc-mlsys.github.io/publications/spfresh-sosp-2023/)
provides the incremental rebalancing reference. These sources motivate a test;
neither proves the proposed BORSUK layout's recall or maintenance costs.
