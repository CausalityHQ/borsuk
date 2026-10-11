# Fixed48 coverage diagnostic: source checkpoint

Base origin/main: 556e5bf147ef0ac3f8fa083212204ea4af560065.
Initial source-only slice: f7476d5a7cb4cb1327e03625131191a15e916449.
Separate repaired slice: 4ec3cb767f9628923786573e82ab4b756b2a6dfb.
Only changed repository file: crates/borsuk/examples/compare_native_replay.rs.
No checkout reset, shared source cleanup, worker control or paid job has occurred.

Dual critique 3b10afdb4a4041f7 reviewed f7476d5, completed. Research consultation
f6d7e4494c5f4615 and engineering consultation 1f669b6d16694bcb independently
identified the same two blockers. Full durable answers are in dual-review.f7476d5.txt.

1. Failed publication could leave complete MEASURED JSON. Repaired by invalidating
   the original inode through a retained descriptor; no check-then-unlink race,
   no write to a replacement path. Invalidation and directory sync errors remain
   explicit failures. Original external exit zero plus authenticated receipts are
   mandatory for acceptance; an I/O failure cannot self-certify durability.
2. Returned IDs lacked the existing fetched-SQ8-cover check. Every control
   LossCoverage is now retained and the existing loss_bucket refusal is reused.
3. Helper-only truth tests were insufficient. New native fixtures invoke the
   actual reducer with strict scope supplied internally (CLI scope stays fixed),
   observe the actual truth opener, reject a corrupted final terminal and injected
   seal sync failures, and check both 48-leaf selections before truth. Another
   whole-SQ8 test changes a nontruth returned ID 200 to 300 without changing hit
   totals and requires the actual reducer to reject it outside the fetched range.
   Actual report publication is tested with sync failure, SQ8 mutation, and a path
   replacement; only the original inode is invalidated, foreign content is kept.
4. Redundant control reconstruction was removed. Existing loss_coverage authenticates
   full SOURCE completion and deterministic charges; semantic leaf/unit/seed equality
   derives identical closure, while the independent production cover supplies both arms.
5. Prototype fixture remains metadata-only, deliberately not publication-valid.
   Prior exact real-generation publication evidence and source-bound native admission
   remain prerequisites for the later consumed-panel execution.
6. SOURCE coverage >=315/320 is a screening rule, not a returned-recall feasibility
   theorem: bridge rows may rescue hits. Original313/320 FAIL stays immutable.
   No candidate returned recall, latency, QPS, population or vendor claim is made.

Verification so far: bounded rustfmt parsing/formatting and exact source diff check.
All Cargo/native tests, release, workspace correctness/suspicious Clippy and actual
unset complete workspace test compilation remain UNRUN. Eight new test functions.
Synthetic boundary fixture derives an actual tiny native producer envelope, then
explicitly synthesizes larger metadata and trace geometry; this is not ANN evidence.
No real request/truth/vector/SQ8/result decoding or reduction took place locally.

Next: one causality eu-central-1 Spot qualification, CPU0-3/8GiB/zero swap,
jobs2/tests serial, machine8400s/external8520s/global native6000s, <=$1.45.
Stop at failing layer, preserve exact source/log/exit and do not duplicate launch.
Scientific consumed32 execution remains separate and unlaunched.
