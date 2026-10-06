# Budget object selector: first native specification

Status: prospective mechanism, no recall/performance qualification. Root selects the bounded evaluator accepted by both reviews in group bf121fb269bb4ea6. User's standing authorization applies. Production objective and all historical dispositions remain intact.

## Current increment

Own only NEW crates/borsuk/src/budget_object_selector.rs and one registration in lib.rs. No CLI, new dependency, fitter, network, object publication or lifecycle scaffolding. A pure Rust evaluator consumes immutable finite model weights, complete membership, source-neighbor group weights and a total read budget. It produces candidates, occupied candidates, selected labels, modeled read/byte charges, coverage and explicit underfill. It exposes conditional move evaluation but does not mutate a published index.

The module is reusable native routing logic. Caller-supplied labels are diagnostic inputs, never authenticated exact-neighbor evidence by construction. No receipt may label source agreement, ANN recall or lifecycle as qualified.

## Frozen numeric selector

Dimension1..768; grid side1..335; hidden64; mixtures4. Parameters in exact order: hidden weights row-major64*D, hidden biases64, mixture weights4*64, mixture biases4, first-head weights4*s*64 and biases4*s, second-head weights4*s*64 and biases4*s. No additional weights. Parameter count64*(D+1)+4*65+8*s*65.

Normalize original queries with existing sq8_source::cosine_vector; reject malformed/nonfinite/zero vectors. Sequential f32 dot accumulation; ReLU hidden; stable softmax subtract maximum, sequential f32 exponent sum, finite positive divisor. Validate every parameter and intermediate before candidate emission.

For each mixture take8 highest entries from each head, or all if s<8. Ties use smaller coordinate. Enumerate Cartesian products, convert to row-major i*s+j, deduplicate, then discard unoccupied labels. Score each candidate by sequential r=0..3 mixture accumulation pi[r]*u[r,i]*v[r,j]; score descending and label ascending. Select up to15 objects in that order if each complete-body charge fits. Do not refill from outside candidates or scan the whole grid in serving selection. A body that does not fit is skipped with an explicit budget diagnostic; all labels processed are charged. Empty/short candidate sets are visible.

The full-grid model diagnostic is a separate bounded operation, never a serving fallback. Record its operation charge separately. The best-coverage oracle is exhaustive only for <=12 occupied objects, <=12 groups and <=4 selected objects; refuse larger inputs instead of substituting greedy and calling it exact. Report candidate-restricted best cover separately.

## Membership and budget

Every original group occurs once; owners are in0..s*s. Full groups have16 rows, only the final group may have1..16. No replicas/padding. Each object<=64 groups AND<=1024 rows. Digests indexed by row-major label; all-zero digest means empty, nonzero means occupied. Reject mismatched occupancy, duplicate digest, invalid geometry and overflow. Actual object payload is64+rows*(D+12); empty labels cost0.

Keep model/grid frozen independently of current live row count. No claim that target56 guarantees insertion capacity. A membership move preserves all groups and capacity. Its receipt compares exact covered neighbor weight before/after under the actual selector. Positive weight per unique group, bounded by that group's actual rows. Distinguish this coverage from model probability.

Budget total operations<=32 and bytes<=16MiB includes caller-declared head/root/delta/attempt reservations before selected bodies. Because the compact directory has no length table, actual serving selection reserves64+1024*(D+12) bytes for EVERY occupied body before selection, including short bodies. Report actual modeled complete body bytes separately; they never fund extra serving reads. Tiny offline cover diagnostics may additionally show the exact-size ideal, explicitly labeled unavailable to this serving format. No real transfer is claimed. A caller-declared metadata reservation that already exhausts the cap yields a valid budget-refused outcome with its charges, not setup INVALID. Malformed model/membership/snapshot/query is error. Less than requested visible rows yields explicit underfill, never invented neighbors.

Bind receipts to SHA256 of canonical model bytes, canonical membership bytes, original query f32 bytes and caller snapshot/delta identities. Different query, coefficients/model, membership or mutation identity cannot reuse a plan. Pre-admit vectors/candidates/diagnostic buffers with checked arithmetic and actual Vec capacities; compute can have a separate admitted diagnostic ceiling. No allocation from an unchecked count. Frozen parameters are borrowed; immutable owned model/membership storage and transient evaluation capacities are reported separately. Membership group-owner tables are offline diagnostic state and must be charged, not included implicitly in the small serving-root projection. Serialized bytes are not RSS.

## Independent native oracles

Tests inline and independent from production selection/move helpers:
1. Exhaustive six-group/four-label capacity2 assignment and every <=2-object subset; include unequal group weights, unequal body sizes and short tail. Independently compare coverage, candidate-restricted cover and exact move deltas.
2.33-label mixture counterexample in candidate-enumeration-counterexample.md, positive epsilon probabilities, duplicate components, ties and empty labels. Full-mixture winner must be reported missing by actual shortlist, never patched by fallback.
3.Scale-shaped s335 fixture with bounded256 candidates and finite charges, without corpus/model training.
4.Exact byte boundary, metadata/delta reducing body count, insufficient rows, checked overflow/invalid geometry/nonfinite logits.
5.Root/model/query/delta identity mutation refusal and deterministic result/repetition.
6.Capacity-preserving move acceptance: strictly greater total training coverage and no lower-tail regression; zero-gain refused. Independent enumerator computes expected before/after.
7.Nonzero-input MLP scalar oracle: D3 original query[3,-4,12], norm13, nonzero hidden/mixture/both-head weights and biases, positive/negative preactivations and nonuniform mixture. Independently encode parameter order, sequential f32 normalization/dot/ReLU/softmax/mixture arithmetic; compare occupied full-grid probability bits and actual shortlist ordering. Do not call production offsets, inference, normalization or scoring helpers for expected values. Mandatory name: oracle_finite_mlp_nonunit_query_matches_independent_scalar.

Oracle source must not call production score/coverage/move helpers. The full-grid model ranking is not the neighbor-coverage oracle. A model missing its own highest score is measured approximation loss, not concealed implementation error.

## Subsequent fitting method (not implemented by this slice)

Single prospective100k source mechanism arm: canonical original f32 vectors and unchanged authenticated SQ8 records/coefficient/order bodies. Deterministic domain-separated SHA256(canonical SHA raw32 || LEu64 logicalID) anchor ordering, seed20260923. First256 training anchors, next64 disjoint held. Normalize via unchanged cosine helper; exclude self-ID. Exhaustively score all100k visible SQ8 records with unchanged native score/ID ordering; first100 remaining IDs are authoritative teacher. No old PQ nominees replace this teacher.

Initial membership uses retained fine physical16-row groups, contiguous56-group target objects, row-major labels; no shuffled or held-chosen initialization. MLP ReLU64/mixtures4 as above. Initialize each weight via domain-separated SHA256(seed/raw input identity/parameter LEu64 index), signed integer from first LEu32 centered at2^31, scaled by2^-39; hidden bias1/256, all output biases0. Neighbor-count target mass per label normalized by100. Loss is negative weighted log mixture probability, epsilon2^-24 for log/divisor. Accumulate batch gradients sequential f64; apply averaged gradient with learning rate1/256, round stored parameters to f32. Plain SGD, no momentum/decay, batch16 in fixed anchor order,16 epochs, no shuffle. Refuse nonfinite model updates.

Two fixed alternate rounds. Group traversal ascending physical ordinal; destinations are up to8 highest aggregate training-neighbor probability labels, ties label ascending. Evaluate capacity-fitting move or smallest-ordinal single-group swap with a destination; accept only strict improvement of total actual-selector training neighbor coverage and no decrease in13th-smallest training hits (256 observations). Recompute exactly after acceptance. Accept an entire fitted model checkpoint by the same rule versus prior model/membership; otherwise rollback. No held-selected checkpoint/constants. Modeling/training ceilings512,000,000,000 modeled operations,8GiB RAM,8GiB scratch,4CPU/noSwap/1800s per panel; reaching a preregistered algorithm ceiling is a valid failed gate. These are ceilings, not runtime estimates. Teacher/reconstruction/scoring/fitting have separate charged ledgers.

Mechanism held64 requires>=99.5% mean agreement with exhaustive SQ8 top100 and fourth-smallest>=99 per panel; underfill/budget refusal is failed gate. This is not original-vector ANN recall. Consumed external64 may only falsify later: seal both complete actual-population outputs before GT, >=6272/6400 hits and fourth-smallest>=95 each panel, <=32GET/16MiB including metadata/delta. Fresh split required for promotion. No parameter ladder on rejection.

## Product release blockers preserved

Durable new IDs, latest-state delete/update suppression, crash recovery, generation pinning, atomic conditional publication, bounded in-process compaction/GC and model-changing rebuild quality are unimplemented by this module. Grid side is frozen per generation; compaction cannot hide a searchable delta row in an unreachable object. Old pins and opening transients are charged. No 100M, cold p90/p95, QPS or competitor win is inferred.
