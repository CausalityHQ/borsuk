# SOURCE nomination preflight: proposed boundary absent
Source: bb5964bada1e1ad51cfe4cf2b5a672639d56c267, compiled Rust ancestor efef1d988359076da2f552a90e41765f7665e217.
Disposition: SOURCE_PREFLIGHT_STOP_NO_IMPLEMENTATION. Not an ANN scientific negative or resource rejection.

The read-only contextless plan aa14e18b7e914e7b proposed dropping the bottom quarter of an assumed SQ8-scored nomination roster before SOURCE fetching. Source verification contradicts that order:
- two_bit_generation.rs:2695 source_walks selects semantic leaves and applies the existing leaf limit.
- :2897 plan_paged_measured prepares query, derives source walks, constructs authenticated capsule request, fetches SOURCE, then invokes plan_capsules on fetched records.
- :3655 search_store_measured awaits that plan before :3736 rank_verified_sq8_pages_traced_with_split.
Thus SQ8 results do not exist before the SOURCE read this proposal intended to avoid. There is no suitable existing SQ8-before-SOURCE gate. Adding one would be a different architecture, not this bounded intervention.

The actual existing earlier lever is semantic leaf prefix selection (generic library supports limits1..48). Current concurrent/runtime production probe intentionally binds leaf40 at runner:984; changing to30 is not admitted by the unchanged Q2 runner. Leaf40 versus48 and shortlist studies already exist; do not rebrand a repeat as a new idea. A future genuine earlier-routing or source-layout intervention must read the corresponding closed studies before selecting its causal factor. This preflight does not authorize bypassing the fixed probe contract or changing scientific thresholds.

Root did bounded EC2 source inspection because the sole implementation specialist e9dcf239-3dbc-4268-9ce1-056243bcc149 was blocked before its brief at the hook-review menu. Devbox/main exact receipt1792321595622757330 proved the deployed no-trust guard is hard-pinned to old4143a07e; no supported current-child action. Parent revoked unused menu authorization1792321656037151732 and queued superseding HOLD1792321656330719544. No restart, raw keys, trusted hooks, duplicate worker or broad guard extension. No child source consumption claimed.

No code changed, no Cargo/rustc/tests/compiler/data/native performance job, no requests/truth/records/raw result reads. Initial source preflight command lacked rg; fallback grep then revealed sparse file absent; Git show exact committed blob provided authoritative evidence. Those setup failures are not algorithm failures.
