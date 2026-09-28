# Reviewed GC regression gate

Existing source check and dual review completed; do not duplicate them.
Prove regression red on one causality Spot correctness worker, then terminate.
Repair admission before remote IO, release a fence when validation fails before
any delete, and rebuild ready state whose namespace claim was reclaimed.
Follow with one distinct green worker and exact frozen source; never overlap.
30 minute/$0.30 maximum compute per worker, sync terminal and authenticated logs,
terminate immediately, discard interrupted checks without automatic relaunch.
No DGX/local Cargo; no quality, performance or vendor claim.
Ambiguous late DELETE versus reuse remains a separate production safety gate.

Red attempt a0001 was cancelled before tests: transformed user-data replaced
the insertion anchor before adding red commands. Instance i-0dce510e53a685fe8
was terminated; original launch/closeout preserved. Retry adds an assertion that
the red command is present; no overlapping worker or review.

Claim-only red reproduced expected key-reuse assertion. Original green worker
i-0dd9a350c279d4aae failed at compile: Vec<u8>.as_ref() equality was ambiguous
(E0283). It terminated; all closed logs preserved. Use .as_slice() explicitly;
sequential green a0002 retries compilation/narrow checks, then one full gate.
Do not rerun the already verified claim red phase.

Green retry a0002 passed eight focused checks. Native S3 stopped before IO at
its owned gc/ prefix assertion because launcher used gc-review-green/.
No workspace suite ran. Preserve closed receipt; correct smoke prefix and run
only native S3 plus the still-unrun full workspace gate in sequential a0003.
Rust code unchanged; reuse eight green tests, no duplicate focused rerun.
