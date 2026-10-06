# Cohere native baseline

Verified local-file correctness/preflight on100k corpus/1000 disjoint D1024 cosine k10 queries. Mean recall97.23%; nearest-rank p90 80.403322ms/p95 82.411070ms; sequential native query throughput13.332298QPS. No underfill. These are not physical S3 or competitor measurements.

SQ8 fetch/scoring mean44.977513ms dominates source18.021308ms, planning6.982548ms and discovery4.919300ms. Investigate that exact native path and preserve ranking bits/recall before any scale benchmark. Current100k profiles do not establish10M support. No multi-dataset or100B work.

All7 native phase closures passed; original instance terminated.31 retained objects bound to the sealed inventory.
