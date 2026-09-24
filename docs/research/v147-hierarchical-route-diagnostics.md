# V147 route-design diagnostics from completed V140/V144 evidence

These are postterminal calculations on authenticated **used** development
cohorts. V144 regenerated V140's raw plans byte-for-byte and recorded the
f32 score matrices used by the exact-parity Rust replay. Its terminal
SHA-256 is `2e73fee33b2b67c74059e342c1a27a2431f4a98e28ec0b7930313dd922d0fbb0`.
For each query, rank all physical pages by `(score, page ID)` and examine
the β=4 selected roster. Distances below are differences in physical
page ordinals to the nearest primary page. The primary rosters are frozen
in `docs/research/inputs/v138-deep-primary.jsonl` and
`v139-relaion-primary.jsonl`. The deterministic recount is
`scripts/v147_route_diagnostics.py`; run it once per cohort with the
V144 `deep.raw.jsonl`/`deep.scores.bin` or
`relaion.raw.jsonl`/`relaion.scores.bin` artifacts, the matching primary
file, and `--rows 100000` or `--rows 1000000`.

| Used cohort | Selected page count p50 / p95 / max | Worst selected score rank p50 / p95 / max | Queries with every selected page in top 256 scores | Selected pages within ±8 / ±32 primary pages |
|---|---:|---:|---:|---:|
| deep-image-96-angular random100k train subset, publication-test ordinals 9000–9999 | 80 / 160 / 208 | 84 / 160 / 208 | 1,000/1,000 | 73.54% / 92.50% |
| ReLAION-1M validation-1000 | 64 / 84 / 84 | 69 / 1,866 / 3,890 of 3,907 | 739/1,000 | 64.87% / 73.33% |

The ReLAION 16 MiB cap can reject a high-score page because its bridging
cost is large, then accept a much lower-score page that fills a cheap
physical hole. Thus an approximate candidate route that omits a tail
page can fail **exact V140 page-plan parity** without necessarily changing
returned quality. Conversely, a nearby-page-only route omits many
V140-selected ReLAION pages. Neither diagnostic establishes which
omitted pages affect Recall@100; that requires returned-ID replay with GT.

The next route screen must compare returned recall, per-query tails,
actual scored-candidate work, planned GETs/bytes and memory against the
strongest β=4 BORSUK baseline. It must use a single corpus-independent
recall/resource rule and then a fresh held-out gate before freezing
defaults. Exact plan parity remains appropriate for arithmetic
refinements of the *same* scorer, as in V145/V146, not for a new
candidate-routing architecture.
