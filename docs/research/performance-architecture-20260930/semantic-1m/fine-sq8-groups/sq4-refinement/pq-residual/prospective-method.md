# PQ residual refinement: prospective native falsifier

Status: UNQUALIFIED. One new refinement intervention; no routing, order, nomination, precision sweep or scientific promotion.

## Evidence and numerical target

Corrected whole-vector four-bit is completed REJECT: consumed FIRST100k D768 cosine k100 ReLAION .95734375/p05 93, CoHere .90421875/85. Same-fetched SQ8 .99515625/99 and .9903125/97 passes. CoHere original nominees have .95609375 mean GT coverage; all incidental fetched rows must remain eligible. Graph-affinity packing32 is also REJECT. Historical bodies and conventions remain immutable.

Decode each original SQ8 coordinate with separate ordered f32 multiplication/addition. Reconstruct its predictor p from the existing physical row's raw PQ64 codewords and books, without cosine-normalizing p. Respect subspace floor boundaries and padded codeword width, including D<64 and nonmultiples of64. Residual r=x-p uses declared literal f32 subtraction.

Train one per-coordinate 16-center residual book without queries or truth. Authenticate extrema in pass1, accumulate 256-bin histograms in pass2, reuse the existing weighted contiguous-partition DP, then encode actual residuals against the final centers in pass3. Freeze bin endpoint/rounding/tie/constant-axis policies before execution. Do not refit PQ, normalize p, introduce a dense rotation or use GT to select bins.

Row: LEi64 ID, unchanged original SQ8 LEf32 norm, ceil(D/2) packed residual bytes. D768 width396. Predictor plus residual yields xhat. Query preparation stays the original native SQ8 cosine preparation. Candidate score is original_norm - 2*ordered_dot(query,xhat): omit the query-norm constant as the existing native SQ8 core does. Ideal score error is -2 q dot(rhat-r); no unbiasedness, f32 parity or achieved recall is assumed. Preserve native SQ8 and direct decoded-coordinate controls separately. Specify exact f32 operation order and independently test near ties.

New codec/root marker must bind PQ body and physical order, original coefficients and source norm semantics, centers/trainer, payload/group digests, query/source/config identities. Equal row width never permits dispatch as old SQ4. A reusable pure codec must not import experiment controllers.

## First real test: no requests or GT

Authenticate both retained FIRST100k sources. Fixed cohort: 4096 logical source ordinals floor(j*N/4096), j=0..4095, mapped through authenticated order to physical rows. Fixed64 anchors: cohort indices64*a, a=0..63. Cohort and anchor choice independent of coordinates, requests, truth and candidate scores; all selections sealed before scoring. Exclude self from both arms.

Train once over the complete source. Rank only the fixed4096 cohort for each anchor using unchanged native SQ8 and residual candidate. Freeze complete results before reduction; requests and truth must never open. Per panel survive iff sum top100 intersections >=6336/6400 and fourth-smallest intersection >=98. Completed failure is REJECT; authentication/execution/resource/closure failure is INVALID. Stop a failed arm without adjusting bins, bits or cohort.

Prospective host caps: CPU1,1GiB,noSwap,600s,20B counted operations, scratch1GiB and output64MiB. The implementation must derive conservative cumulative read/work and simultaneous allocation bounds before any corresponding body read; report INPUT_UNAVAILABLE/INVALID if the actual inputs cannot fit. No local BORSUK compilation or data execution. Root owns exact remote gates and frozen execution. Tiny synthetic codec/scalar/pipeline tests precede the real probe.

## Subsequent scientific gate

Only after mechanism survival: original64 requests per panel, identical complete fetched populations including incidental rows, unchanged SQ8 and decoded-coordinate controls, both payloads/all128 result seals before any GT. Mean recall >=.98, fourth-smallest hits >=95, every payload <=32 ranges and16MiB. Shared residual books are separately charged startup; not excluded from total cold bytes. No serving-QPS, vendor or100M feasibility claim from this probe.

## Product resource limits still unresolved

Payload396*T, resident residual centers64*D bytes plus headers; histogram scratch256*D*counter_width plus extrema/DP/stream buffers; predictor/PQ already resident but fully charged. Build O(ND) plus fixed D-dependent DP, three original source passes; no ND² transform. At100M D768 payload39.6GB alone; existing PQ/IDs/hashes/graph and retained generations remain additional. Existing conservative graph residency may approach59GB per generation and query workspace includes4N bytes; this arm does not solve those scale blockers. Admission must account unique pinned generations, queries, deltas, compaction and runtime before scale promotion.
