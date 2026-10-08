# One-wave closure cost: evidence-only amendment

Same closed inputs and same bounded Rust replay slice. This amendment adds a physical-cost estimate, not a production algorithm, query execution or a new performance arm.

The observed cold pipeline has a source fetch followed by SQ8 fetch/rank. Source is about 40 seconds per 1000 queries; SQ8 about 52 seconds. A causal alternative worth costing is to skip source nomination and fetch SQ8 for the complete discovered physical closure. It trades more SQ8 bytes/scoring for one fewer dependent fetch wave. Broader SQ8 candidates can alter recall even if they include selected rows; SQ8 gap rows also differ. Neither quality nor latency is established by this calculation.

In the existing --source-utilization mode, add an explicit direct_sq8_get_cap=32 configuration field, bound by the archived producer's max_query_gets. For each authenticated trace, use its SAME reconstructed 256-row source closure with the SAME shared cover_pages helper, row width dimensions+12, and that 32-GET allowance. Report direct_closure_sq8_bytes/gets, mandatory_closure_sq8_bytes and bridged bytes. Compare with the recorded original source+SQ8 bytes/GETs; aggregate totals/median/p95/max. No byte cap is silently raised: this is only a cost estimate and a future candidate would need an explicit new resource envelope. Reject arithmetic or trace/charge contradictions. Retain first-wave source-utilization bounds and old reader modes unchanged.

Explicitly mark counterfactual_cost_only=true, score_or_recall_evaluated=false, production_change=false and claims_latency=false. Corpus, requests and truth stay unopened. Add small hand-counted fixtures with contiguous and fragmented closures, tied gaps, a clipped tail and complete closure expansion; no new production/test files or dependencies. The extension uses only already authenticated trace/configuration fields and does not need a new collector or dataset.

This estimate informs one subsequent causal design decision. It does not authorize adopting resident source caching, dropping authentication, repeating failed packing/centroid schemes, or launching a paid ANN measurement.
