# V189 first attempt: closed pre-holdout implementation failure

Attempt `a0001` at source commit
`c65a6b2d5154204a344291ed8f410863e8326903` failed in `fit-plan`
before a model or holdout plan was written. The Causality Spot worker
`i-013cceba9ce58d04c` is terminated. Its immutable failed terminal is
`s3://borsuk-bench-453182569524-euc1/research/v189-predicted-interval-source/c65a6b2d5154204a344291ed8f410863e8326903/runs/a0001/terminal.json`,
SHA-256 `589ba1746b6afdf5958b07870195d6b136454de16c06247537b322dd4d6994df`.
The closed log and terminal were read after termination. No holdout truth,
source-quality summary or performance result exists for this attempt.

Root cause: V189 passed full fit GT100 unit counts to
`fit_rank_utility`, whose contract requires truth counts only for scored
candidate units. Some fit GT100 units lie outside the width-32 candidate
universe, so its geometry check correctly raised
`ValueError: source truth unit counts differ`. The candidate misses remain
accounted for separately; filtering them from the **rank model's training
input** does not change the specified utility or erase them from the
quality denominator. The fixed source applies the same filtering when
independently refitting the sealed model.

The controller first rehashed the failed artifacts and terminated the
instance, then masked this original error with an S3 404 while trying to
read the success-only `source-labels.jsonl`. The controller now reports a
failed terminal's phase and exit code before checking success-only seals.
The complete V189 cell will restart once under a new source commit and
attempt `a0002`; `a0001` is immutable negative execution evidence and
must not be treated as a measured quality cell.
