# Fixed corpus center: development quality candidate

Native source34a23c7709b37d4b87db4d7f215d687a17033edd. Strict opt-in v3 diagnostic, origin controls retained; production library/scorer/defaults unchanged. Native22 tests, required workspace Clippy, real unset workspace testbuild and release passed on exact source. Independent dense transform/arithmetic, scaled nonunit input, row-dependent affine correction, zero residual, authentication and memory boundary tests executed. Short-lived specialist completed/archived.

A fixed center was computed from the entire authenticated canonical corpus: f64 normalize each stored f32 row, sequential f64 component sums, divide by total row count. Every ID+record digest, sidecar SHA and complete canonical SHA checked. Center8192B SHA022d40ae6cee11ed0dc1d24147f9d50b3578df82209a713bac999b7a823d839a. No query/truth payload used in preparation. Authority JSON nomination metadata was parsed but not used to select rows or calculate the mean. The old helper receipt field query_truth_nominees_opened=false refers to separate payload files, not an assertion that authority metadata was unread.

Original preparation timed out120s with debug dependency rlibs despite frontend-O, exit124/PID0, no center output. Same source/inputs/algorithm/time/resource limits with optimized dependency rlibs passed:10.48s/RSS44316KiB. Both attempts preserved. This is preprocessing, not cold ANN timing; it incurs a full4.104GB canonical pass.

Preregistered S512, fixed transform seed, development queries0..63. Native admission0, separate selfcheck/bad-center canary0, science0/controller0 on Spot i-0d7b659a37c17915f; original unit borsuk-centered-onebit-development-a0001/invocation2fd170a152d14748aabf8bfff196c2b7. Root caller2GiB, host8GiB,CPU0-1/swap0.

Development: two-bit256636/636 nominated truth IDs; origin one-bit256633; origin one-bit512635; centered one-bit512636. No per-query regression. Original two-bit and origin one-bit256 ranking/order/scorebits exactly reproduce immutable wide64.640 total truth IDs;4 absent from original nominations remain unavailable.

Decision: retain a valid DEVELOPMENT quality/compression candidate. Untouched validation128..191 remains UNRUN because the frozen launch cutoff passed; do not claim generalization. Do not relax that cutoff. No cold ANN/QPS/physical IO/lifecycle/vendor win.
Scalar score/sort centered42.136344598s, origin41.633450331s, two-bit2.231838715s; shared encoding26.756655675s; whole process120.27s/RSS707600KiB/no swap. Centered code+two factors107274240B vs two-bit196669440B on744960 union rows (45.45% smaller logical representation); IDs/routing/build/lifecycle overhead excluded. Modeled1345718784B.

Next falsifier: exact frozen source/center/S/seed on untouched128..191 with independent admission/canary and the same per-query quality gate. Only then investigate a portable lookup scorer against scalar oracle. Scalar timing is currently unacceptable for a production speed claim.

Estimator: r=normalize(x)-c; alpha=norm(r)^2/L1(Tr), score=q.c+[r.c-alpha*sign(Tr).Tc]+alpha*sign(Tr).Tq. Structured signed Hadamard remains NON-HAAR heuristic. No RaBitQ error/unbiasedness theorem is claimed. Primary sources https://arxiv.org/html/2405.12497v1 and https://github.com/VectorDB-NTU/RaBitQ-Library/blob/main/docs/docs/rabitq/estimator.md .
