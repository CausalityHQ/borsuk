# Continuation checkpoint

Full native goal restored verbatim, active. Delivered base is 29e578ba and
origin/main matched at the latest fetch. Six affected sources in green-retry
source-parity.json match source5432e9053b7d8c8c1b09b96162232fc10522193e1a35a30119c446b8ef31d65e.
No code committed/pushed from this resumed provider thread yet.

Original GC worker and both original reviewers are completed; do not rerun.
Existing dual review group0122d55c38b34216 results in review.md.
New checks were distinct regression verification, sequential:

- red/i-0dce510e53a685fe8: launcher cancellation before tests, terminated.
- red-retry/i-092cef977a48f6382: expected two assertion failures, terminated.
- green/i-0dd9a350c279d4aae: claim-only red reproduced, green compile E0283, terminated.
- green-retry/i-0de2dd4c3fee2f6d1: eight focused checks PASS, native S3 prefix guard failure before I/O; terminated.
- assurance/i-0eb3624a8423fa427: native S3 PASS; workspace lib1630PASS/57FAIL/6ignored, terminated; session43228 exited101.

Keep observing the same session or its S3 terminal prefix in
assurance/aws-launch.json. Do not execute aws-green-retry.py again.
Await original terminal, authenticate logs, verify termination, inspect failures.
Current worker runs focused native/HTTP, actual S3 smoke and full workspace tests
once, with30min max and termination trap. All workers use AWS causality Spot.

Admission now runs before I/O; keep_set failure releases fence before deletes;
ready reuse requires exact namespace claim body, rebuilds absent claim/SQ8.
Reconciliation records separate late DELETE versus key-reuse production hazard.
No current result closes vendor, quality, scale, cost, multihost or full GC recovery.

## Latest valid checkpoint

All new AWS workers and both inherited reviews are terminal. No remote job
remains active for this GC check; do not relaunch any recorded attempt.
Nine focused/native S3 checks pass on identical affected sources. Workspace
command stopped at borsuk library failures, so later workspace targets are NOT
certified. Full failure names/details in workspace-failures.txt. No code pushed.
Native goal stays active. Next work: repair/qualify full gate and test the
delayed DELETE mutation-key reuse hazard before claiming crash-safe GC.

Initial trace: format fixture builds26 arrays against current27-column schema
(format.rs11375); native/public/resident index fixtures expect authorities that
current finish_bulk_load does not populate; remaining geometry/backend/GET
assertions need independent diagnosis. All six failing module files are
unchanged in this diff and have no two_bit_store/compaction/gc/mutations calls.
This is code-path evidence, not a measured clean-base57-failure reproduction.
