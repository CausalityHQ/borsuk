# Cosine top16 FP32-coordinate quality diagnostic

Decision: RETAIN QUALITY PARETO; efficient authenticated serving is next.

| Development cohort | Current cosine SQ8 | Top16 canonical rerank | Gains / losses |
|---|---:|---:|---:|
| first32, k10 |315/320|319/320|4 / 0|
| full1000, k10 |9746/10000|9803/10000|57 / 0|

All1000 reranked top10 lists exactly match the all-fetched canonical reference;
all10000 reference IDs are contained in current cosine SQ8 top16. No underfill.
Baseline and candidate share authenticated fetched ranges, including bridges.
Truth never selects candidates. Cohere1M/D1024/cosine/k10 development data,
not a blind holdout or generalization claim.

Source0c421132 adds an offline native diagnostic; serving defaults remain the
qualified cosine implementation. FP32 stored coordinates use the existing
sequential-f64 canonical oracle and its normalization contract. This diagnostic
reads/scores every visible fetched canonical row to establish the reference.
Its79.72s total and120728KiB processRSS are NOT16-row serving latency, S3 cost,
cold-cache performance, or a vendor comparison. No production raw-FP32 serving
plane or matched competitor win is established.

Seven new native tests, legacy diagnostic regressions, release, workspace
correctness/suspicious Clippy and actual shim-unset workspace test compilation
passed. The mode-only publication repair was RED->GREEN for cap/newline and
file/directory sync/replacement failures. Both independent source reviews were
read and reconciled. Three test-only constructor files repair28 demonstrated
stale-fixture failures;55 affected tests pass, production/assertions unchanged.
Preserved failures: stubRED101, cosine-producer versus legacy-L2 fixture101,
publicationRED101. These are not algorithm rejections.

verification.json binds source/config/ELF/qualification and independent recount.
Full per-query reports, source bundle and gate logs belong to its immutable S3
closure prefix; terminal checksum receipts must be verified separately.

Next: generic authenticated partial-FP32 top16 serving. Audit existing canonical
publication/range authority first: full-file SHA or ETag alone does not prove a
range. Preserve9803 hits, then measure actual GETs/bytes/RSS and paired cold
end-to-end latency. Retain this quality gain despite efficiency misses;
production promotion still requires lifecycle/concurrency/recovery evidence.
