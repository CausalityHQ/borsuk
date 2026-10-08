# Cold source utilization: decision and bounded implementation

Root source: 89f1fad56c0e80d935c578d13025708e944800aa. Read-only planning consultation 284c987cd3314567 exited 0; its complete result is retained beside this decision.

## Decision

Measure source utilization from the closed membership A/B traces before changing production geometry. No smaller SQ8 layout, wider SIMD, new packing rule, cache, or repeated vendor benchmark is selected.

Source authentication already covers 32-row units. Source nomination starts from walked units and then completes units inside their 256-row pages. The source fetch covers those pages, including any request-budget gap bridges. SQ8 authentication uses 256-row pages and its scorer ranks all returned rows, including bridges. Dropping those rows would change the scored population and may reduce recall.

Existing gap merging minimizes bytes for fixed mandatory pages and the existing request allowance. Historical co-selection packing and page-centroid failures remain unchanged. The four-row SIMD candidate remains unintegrated after its valid timing REJECT.

## Authorized bounded Rust evidence slice

Extend only crates/borsuk/examples/compare_native_replay.rs, reusing the existing authenticated completed-v2 reader, duplicate-key refusal, byte/line/report caps, descriptor stability, full SHA/EOF, seal/terminal checks and create-only output. Preserve existing reduction CLI and behavior.

Add an explicit source-utilization mode with a strict SHA-authenticated config. The config pins the completed native JSONL and exact historical identity/bound_inputs rows, plus explicit source geometry and request/byte limits. Geometry must match the authenticated producer workload; root separately binds limits to the archived producer source/config. This evidence mode does not establish that current production executed the historical run.

Stream exactly the complete 1000 ordered queries. Decode the existing bounded trace fields. Validate unit bounds/uniqueness, seed additions, scored-unit membership within closure pages, completion accounting and recorded source charges. Reconstruct original closure from semantic_units union semantic_seed_additions; reproduce original 256-row cover. Compute the minimum 32-row cover of actual nomination_evaluated_units with the SAME 128-GET allowance, clipping the final partial unit. Use the shared checked cover_pages helper. Assert baseline GET/byte parity and lower-bound bytes <= baseline.

Report per-query and aggregate baseline bytes/GETs, walked/completion/scored units, scored payload bytes, bridge bytes, ideal bytes/GETs, bytes potentially saved, completion-limit frequency, median/p95/max. Keep output within the existing report cap or refuse before publication. Include all input/config/reducer source pins and explicit optimistic_hindsight=true, production_change=false, claims_quality=false, claims_latency=false. Actual scored units depend on scoring; this bound cannot be used directly as a serving schedule.

Meaningful fixtures cover completion that contains the winning page, completion limit, tied gaps, partial tail, duplicate/out-of-range units, contradictory baseline charges, unknown/duplicate config keys, file replacement/truncation/growth/seal errors, occupied output and unchanged old reducer modes. No production module/tests, format, dependency or controller edits are authorized.

Local worker checks are source-only CPU1/256MiB/swap0/120s. Native qualification is root-owned remote: affected example tests, release example, locked workspace all-target Clippy correctness/suspicious and real env-unset jobs1 test-build on the exact revision. No real trace execution before compile qualification and frozen input admission.

## Scientific next gate

Run the qualified evidence mode against a fully authenticated CLOSED membership result. Corpus, request vectors and truth remain unopened. A zero aggregate byte-saving upper bound stops this source-read approach. A positive bound is evidence for a subsequent causal design, not a latency win: score-dependent completion may require another fetch wave and can consume the saving. No 64-query algorithm rerun is authorized until such a design is chosen and preregistered.

The current cold comparison remains recall@10 97.23%, p95 123.88–126.89 ms and serial QPS 10.09–10.12. No overall S3 or matched Turbopuffer win is established.
