# V160 geometric relayout with frozen exact-primary routing: closeout

## Decision

**Candidate advance to a separate 1M transfer gate.** On the used
ReLAION-100k D768 development-1000 split, reordering the same SQ8 rows by
the closed query-blind balanced-two-means membership and applying the
budget-derived 512-row page geometry raised returned Recall@100 from the
paired source-order V158 exact-primary control's **77.302%** to **99.170%**.
The new arm's p05 Recall@100 was **97%**, mean Recall@10 **99.45%**. It
passed the preregistered 97.5% / 90% / 96% quality gate, all 1,000 plans
obeyed 32 GETs and 16,777,216 SQ8 bytes, and an independent checker passed
all 1,000 rows. The candidate won every paired query, with no ties or losses.

This is a combined physical-order and page-width policy test. It does not
isolate those two components. It uses exact-primary rosters derived by
scoring 512 nominees against an authenticated local SQ8 mirror. The result
does not validate a PQ-first or cache-free object-storage query path; V158's
PQ-first-100 row ceiling remains 69.066% on this cohort before page fetch.

## Paired evidence

| Used ReLAION-100k D768 development-1000 | V158 source order, exact primary | V160 geometric relayout, exact primary |
| --- | ---: | ---: |
| Returned GT100 hits / 100,000 | 77,302 (77.302%) | **99,170 (99.170%)** |
| p05 returned GT100 hits per query | 71 | **97** |
| Mean returned Recall@10 | not recorded in V158 | **99.45%** |
| GT100 IDs in fetched SQ8 ranges | 77,302 | **99,545** |
| Distinct exact-primary pages, median | 88 (V159 postterminal) | **20** |
| Planned GETs per query, median / p95 / max | 32 / 32 / 32 | **10 / 19 / 27** |
| Planned SQ8 bytes per query, median / p95 / max | not tabulated / not tabulated / capped | **16,773,120 / 16,773,120 / 16,773,120** |

The V160 total was 9,894 planned GETs and 15,562,734,720 planned bytes
over 1,000 queries, averages of 9.894 GETs and 15,562,734.72 bytes per
query. V158's exact-primary control planned 16,766,231,040 bytes in total
over the same 1,000 queries. The byte cap is nearly saturated for the median
V160 query. Candidate
physical coverage exceeds returned hits by 375 GT100 positions in total;
the remaining loss is post-fetch SQ8 ranking on this layout. No live S3
GETs, latency, throughput, retry cost or charged serving RAM was measured.

## Provenance and validation

One Causality `c7i.xlarge` Spot instance `i-0835bb97cf135a1f6` ran from
clean pushed source `1a874d21abe5166bbd769c91eb2de29746af0957`, then
reported a complete terminal and was confirmed terminated. Immutable prefix:
`s3://borsuk-bench-453182569524-euc1/research/v160-geometric-relayout/1a874d21abe5166bbd769c91eb2de29746af0957/runs/a0001/`.
Source archive SHA-256:
`3bb01b6e72f78fad2712a836f7775847cde05991e83edd0d8ee2f7762762e894`.
Terminal SHA-256:
`026bf6792ae140f8cba83bc4452d4db0e54cb31facfdd84394309117931f0c52`.

The controller separately streamed all **eight** terminal-listed artifacts
from S3 and matched each byte length and SHA-256. The relaid 78,000,000-byte
SQ8 object's SHA-256 is
`ea4d7e86ad06d6791b03dfa4b07057b097cb729219632513ac3f8d2ec8f6250a`;
the sealed row-order SHA-256 is
`c1599130af8abe70e506a300ace390d1f83a68b987a872ce5b3d320ce2b34fec`.
The GT-blind plan seal SHA-256 is
`44c0aa7b5c55b9100e7df88b089f1104cb1201065c82076d34c66894065609b7`.
Raw, summary and independent-check SHA-256 values are respectively
`c2d0676ba3f0003a6f90681ce28143d509847786a3714ce98f0ffca02d1eab67`,
`963e768797ae02af5847bb1fca2ba73e1f394b7487356d0fb556fa8429b3105a`,
and `b8dcd50e1cf0d4dd35661ef2e9d19e7f222575389d2260a834c7e1d8e2954bd3`.
The checker verified the SQ8 row permutation, each frozen roster's relaid
plan, caps, fetched-ID membership, GT hits, aggregate and gate verdict.

## Limits and next gate

The geometric membership was constructed without queries or truth, but
its layout family had previously been screened on this **used** development
cohort. Neither V160 nor V155 is fresh or a matched cross-scale comparison.
V160's 100k layout and V155's 1M V63 layout are different physical formats.
The 100k win therefore supports a layout-and-admission hypothesis, not a
general recall claim or a production default.

The next frozen 1M gate must apply a source-only geometric build rule and
the same budget-derived page policy to the ReLAION-1M validation-1000
cohort, compare paired returned quality and resource counts against the
strongest V155 exact-primary cached-sparse baseline (99.567% returned
Recall@100) on that same cohort,
and include a GT-blind physical-locality stage before opening truth.
Preregister the precise builder and immutable source identity before
launch; do not reuse the 100k membership or tune by dataset name. Then
measure local SQ8-primary access latency, actual S3 GETs/bytes, and a
two-generation 100M memory/build worksheet. Requested recall may select a
larger charged local tier or RAM cap; there is no vector-count knee or
dataset-specific operating rule. Lean can prove conditional page/GET/byte
and memory arithmetic once implementation counters are refined to the
model. Recall, latency and cost still require measured evidence.
