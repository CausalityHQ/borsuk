# Read-recall release default (2026-08-25)

Status: prerelease methodology decision. The measurements below are
claim-ineligible diagnostics, not publication results.

## Decision

Publication V3 read-recall workloads use:

- leaf-page budget: `64`
- maximum candidates per segment: `512`
- minimum recall@10: `0.95`

The `32`-page setting remains supported for diagnostics, but it is not a
publication or SIFT qualification factor because it did not meet the
preregistered recall floor on DeepImage-96. Explicit `diagnose-read` runs can
still exercise it without becoming claim eligible.
Candidate widths above 512 remain available for diagnostics; they are not the
release default because their small quality gain did not justify their request
cost. The width is the source-bound `V20_COMPATIBILITY_CANDIDATES` constant,
not a manifest factor.

This is a provisional campaign default derived from DeepImage-96. It is not
evidence that 64 pages and 512 candidates meet the floor on the other 18 read
datasets. Each publication cell must still pass the unchanged 0.95 hard gate;
a miss blocks publication and requires a separately registered qualification
and methodology revision rather than lowering the floor.

## Immutable authority

- Git commit: `cf3109b6dea5b9218855719e073849dfa28cc061`
- source archive SHA-256:
  `087d220dfdf600dc386a05f41041894adba1f94de48e4a1edb7a01efb775e25b`
- diagnostic input manifest SHA-256 (before this decision):
  `3944571d0885e905914621680795f9e09bb156f3bc05ff768413317d69b0838b`
- protocol SHA-256:
  `9d872723c4e3ae0edd2bae1a23c1d1d846f706b6149fea29b157e5069627563d`
- dataset: DeepImage-96, 9,990,000 vectors, 1,000 queries, recall@10
- AWS account/profile: `453182569524` / `causality`
- region: `eu-central-1`
- runtime: Spot `c7g.xlarge`, 2 GiB BORSUK RAM budget, no disk cache,
  three CPU threads
- build instance: `i-0ede2ff797ddce352`
- diagnostic instance: `i-05a12e4914d966b05`
- diagnostic attempt: `runtime-read-diagnostic-r01-4cef136689e64f1af726f053-arm-0000-a0002`
- terminal result SHA-256:
  `5a65f716f0987b76d910752f032d16be92a6c07c49e2cf74f97ee2c91a6a74c7`
- query samples SHA-256:
  `dca8ddd3ca2155c26aff93a9d01713ca65605993ce0233ded3f29825b9ae61d3`
- summary CSV SHA-256:
  `f699d9f1ff6ac5aaee254541e95334ea78971157bc8efc2f4edc73e535350c69`

The terminal document declares `claim_eligible: false`. The raw samples and
summary were downloaded and their SHA-256 values were checked against the
terminal receipt before this decision was recorded.

## Diagnostic matrix

All latencies are milliseconds. Request cost is physical S3 GETs per query.

| pages | candidates | recall@10 | mean | p50 | p95 | GETs/query |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 32 | 512 | 0.942 | 113.306 | 104.779 | 160.350 | 38.623 |
| 32 | 1024 | 0.944 | 111.990 | 103.199 | 158.315 | 46.503 |
| 32 | 2048 | 0.944 | 115.310 | 105.904 | 161.550 | 53.791 |
| 32 | 4096 | 0.944 | 119.012 | 109.214 | 169.232 | 55.532 |
| 64 | 512 | 0.974 | 132.655 | 125.634 | 171.786 | 54.634 |
| 64 | 1024 | 0.976 | 131.651 | 124.284 | 171.519 | 61.819 |
| 64 | 2048 | 0.976 | 143.089 | 137.367 | 183.160 | 73.987 |
| 64 | 4096 | 0.976 | 156.582 | 149.365 | 197.794 | 89.528 |

At 64 pages, widening 512 to 1024 candidates gained 0.2 recall percentage
points while increasing GETs by about 13%. Wider settings added no material
recall and worsened latency and request cost. At 32 pages, recall plateaued at
0.944, below the 0.95 publication floor. Across all 1,000 paired queries,
64 pages never read fewer exact pages than 32 pages at any tested candidate
width. On this dataset, the 64-page default also used about 41% more GETs than
32 pages at 512 candidates. Some easier datasets may meet the quality floor at
the lower request cost; that possibility is not established by this diagnostic
and is deliberately traded for one preregistered MVP default.

## Scope

This decision freezes the BORSUK factor used by the forthcoming standard,
realistic, and synthetic Publication V3 campaigns. It does not establish a
cross-system performance claim. Product comparisons and final website/paper
numbers require complete, receipt-authenticated paired campaigns from the
source revision that includes this methodology change.
