# Cold fetch observations from frozen main 3cee684a

Read-only source inspection; no new timing, recall or physical-S3 measurement.

`two_bit_generation.rs:1743` runs resident discovery, source fetching, then source-based planning. Source walks use 32-row units. At line 1777 the closure rounds units to groups of eight; `plan_two_bit_source_cover` uses 256-row pages. Selecting one 32-row unit can therefore require its complete 256-row page. This is a geometry observation, not measured total amplification: other selected units can share the same page and cover coalescing can alter requests and bytes.

`two_bit_generation.rs:1671` uses the shared authenticated range fetcher when no source cache is configured; cache hits and misses take separate paths. Cold experiments must freeze cache settings and report physical backend traffic independently of library counters.

`sq8_s3_range.rs:615` fetches admitted ranges with ordered `.buffered(max_parallel)`, then collects and drains all outcomes before examining errors. A slow early range may prevent replenishing the request window even when later requests have completed. This is a scheduling hypothesis to test with the existing per-range trace, not proof that this dominates cold p95. Any future completion-order change must preserve bounded memory, full drain, deterministic error selection and authenticated byte/GET charges.

Next qualified native measurement should record source discovery/planning/fetch and SQ8 stages, planned versus fetched bytes, per-range completion times, and actual backend requests. Choose SIMD, request scheduling or layout changes from those measurements. The invalid historical 1M run's reported CPU clock cannot select a compute optimization, and these source observations do not justify a performance claim.
