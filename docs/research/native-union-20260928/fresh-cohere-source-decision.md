# CoHere FIRST1M transfer: provisional fresh source cohort

2026-09-29. A metadata-only preflight pinned CoHere-large-10M D768 cosine
train rows **1,005,000–1,005,999** as a candidate query cohort for an index
built from train rows0–999,999. The source is the closed publication receipt
SHA256 `0965aa02…`, shard `train-00000046.parquet` SHA256 `13195bd1…`,
67,122,091B, with global train rows1,004,870–1,026,714. The query range
lies wholly inside that shard. The S3 HEAD size and `borsuk-sha256` metadata
matched the receipt. **No vector, query, GT or ANN result body was opened.**

Authenticated prior CoHere query families use registered test rows0–999,
train rows100,000–104,999,1,000,000–1,000,999, and1,001,000–1,001,999.
The candidate range is disjoint from each. A direct literal scan of149 exact
archive matched Git source trees found zero executable-source references to
the candidate row endpoints or shard46. This is a useful exclusion check,
not complete historical lineage closure: dynamic row selection and indirect
inputs require further audit. The cohort status remains **provisional**.

Next gate: authenticate prior indirect query inputs and selected-vector
duplicates; reject the entire panel if reuse is proven. Build the current
two-bit generation for the same CoHere first1M source with pinned source,
ordering, scorer and physical caps. Only then decode this fixed panel, seal
exhaustive f64 cosine GT100, and run a bounded native R@10/R@100 and offered
HTTP transfer gate. Use mean R@10>=95% as the initial practical quality
floor, report R@100 separately, and measure cold request tails/8 offered
QPS/GETs/bytes/RSS/cost under a declared protocol. Preserve the ReLAION
prospective GO and all historical strict failures. No CoHere1M candidate
quality, latency or vendor comparison is claimed here.

The exact source identities, known exclusions, audit scope and reproducible
check are in [the metadata report](fresh-cohere-source-candidate.json) and
[`check_fresh_cohere_source.py`](../../../scripts/check_fresh_cohere_source.py).
