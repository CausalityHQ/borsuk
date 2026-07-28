# Publication v2 attempt ledger — 2026-07-28

This ledger records every confirmatory-prefix attempt, including attempts that
failed before producing a usable comparison. It exists to make outcome-based
run selection detectable.

An attempt is eligible for reporting only when:

1. its result tree contains `PUBLICATION_V2_COMPLETE` and no
   `PUBLICATION_V2_FAILED`;
2. all five repetition markers and all frozen dense, hybrid, and direct
   coverage rows are present;
3. the raw direct evidence contains exactly 5 × 1,000 paired queries per cache
   phase; and
4. `validate_publication_v2_results.py` accepts the complete tree and
   independently reproduces both claim files.

No artifact from a failed attempt is merged into a later attempt.

| Attempt | Source SHA-256 | Manifest SHA-256 | Schedule SHA-256 | Terminal state | Failure boundary |
|---|---|---|---|---|---|
| v1 | `1b45e31a0f02097b01bf1f978bcc14dc171c5400f4e971c7c23967ed86e98951` | `f04d9af3d54cb3fa1bcb0e9750366c4fe6c2a323d19ade81caadead1be46b57d` | `eb0777d072809549026d46341fc32cd516338b74195ad33ff31fe7224a824642` | failed | Dependency installation rejected the nonexistent `boto3==1.43.57` pin; no benchmark measurement began. |
| v2 | `fc06e085c2313382004976f1817427ca60a0ca1c83ae1e6f2b62a4c13c91a83e` | `d1a0e7f0102436d8d3cca14c293380a60ea11b8bc45138d185a54794814894ee` | `af7fd909681d6aa93b824d4de976e4610aced51d595d886fdd02fd3415461832` | failed | The EC2 instance role lacked S3 Vectors permissions; the service preflight failed before measurements. |
| v3 | `aa359a79aed0894b938e135d3300b34e3f6debbd466043750fd83e1b2383d440` | `b27aa20064a32fc8e1c83a99f792a9cecc0a9b9bf9cb38e28c10465030a48bd5` | `0aa53476dd7e92c02c8f8d0f0676c64ca711728c35fbe5901b53389bfb125d2a` | failed | Amazon Linux portability assumptions (`shasum` and pip inside a uv-created environment) failed before measurements. |
| v4 | `316738dc7a82e7534b5c979eb61445e7e61c3ab08108c28c7ee3d92699914a23` | `56de47e4982882597978b0fd0dfca8c28fe789f073f298a6f90476b5afd5d867` | `33bbf84951b5a114b66dea2779d7399f57f8e7835cee4babab9272cd7b6a18c0` | failed | Hybrid preparation and validation completed, but CRLF schedule bytes polluted the shell `cache_key`; no measured repetition began. |
| v5 | `9d1e484f0e8a6354450c055f9fa08ff58a78d6d0aab5bf7eb475ced267e1ac96` | `4f7bf9f5d7cfb2b44d73b99f592d7b1cbc4057202a691ed67514667249ed8594` | `1a1298f554ee7598aebbcf69e37ebbfb7f72ea9602475c694728e8a47492c466` | failed | Repetition 1's BORSUK direct arm completed, but the resource chart loader rejected valid blank final-only telemetry columns before the paired S3 Vectors arm. The unpaired BORSUK measurements are ineligible and are not reused. |
| v6 | `75369f9e42a4814b7ee9aa203f205ed93614b34b29b8e1b9c5c0c084e7faa741` | `dc4d7d7bd42da6862e0b77eb3f6aa147dba4ea10aaa29006cc7fe556a3e8275b` | `883ac384c55595f31bf8452764ee90f66d763104da679da7e20ae24ce4ea2895` | failed | The generic dense runner executed unplanned 1,000-query full-corpus exact scans and unrelated post-claim phases. The run was operator-terminated during r01 NyTimes, explicitly marked failed, and preserved. Its complete direct pair and partial dense rows are ineligible and are not reused. |
| v7 | `9805df95efd9c3fa38b22e8b720e435e6c0b852694c6e8777b13ff955b8124c8` | `a9a1087cd62cd33b94d4baf421c44aa3ca053060caf3dfe3e4616527eeaa0120` | `61ea2fa8ef9ddda293c7a62e003c249ab0062a1c0d836eff1bb41fd6699673b7` | failed | Repetition 1 completed the direct pair and five dense datasets, then exhausted the client disk during the SciFact `dense+text`, `hot-1` hybrid query while writing a range-bundle cache file. The runner marked the campaign failed at 2026-07-28T17:38:33Z; no repetition marker exists and no measurement is reused. |

The local preserved trees are under
`.borsuk-scratch/publication-v2-confirmatory-20260728-vN-failed`. These are
audit artifacts, not publishable benchmark sources.
