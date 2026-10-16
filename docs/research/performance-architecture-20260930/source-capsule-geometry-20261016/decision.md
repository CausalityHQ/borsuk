# SOURCE capsule geometry: real 1M metadata result

This diagnostic is qualified metadata analysis. It changes no serving path, persistent layout or defaults and measures no ANN recall or latency.

On the authenticated Cohere 1M/D1024 generation, 731 semantic leaves cover 3,907 original 256-row SOURCE pages. Materializing each leaf's complete page closure creates 7,839 page copies: 529,740,288 record bytes versus 264,000,000 original bytes, or 2.006592 times the storage. Hypothetical 32-byte headers plus four bytes per page add 54,748 bytes. The final physical page has 64 rows; its actual size is charged.

The 48 largest capsule payloads total 58,324,992 bytes. Even adding 48 maximum-size headers keeps this conservative payload bound below the historical 64 MiB SOURCE allowance. This establishes neither RSS nor network-buffer requirements. A model of one GET per selected capsule has at most 48 SOURCE requests, excluding directory reads/retries/SQ8. The closed incumbent averaged 107.882 SOURCE GETs/query; this comparison does not prove a latency improvement.

Native tests passed all six cases: overlapping pages and weighted tails, partial-page charging, D1/257/1024, metadata hash/schema/width rejection, rebound corrupt router/membership rejection, and overflow/cap refusal. Release compilation, workspace correctness/suspicious Clippy and actual unshimmed workspace test compilation passed on exact source39c8c570e4706f9482da7a6636baeff5dc161eb5. Source39c8 is formatting only atop authored a2db885e; the two changed files are the diagnostic example and its manifest registration. No dependency or production API changed.

The real diagnostic ran once on Causality EC2 with one spare CPU,128MiB,no swap and60 seconds. Its config pins the generation/plane/router/membership. It opens no queries, truth, vector records, centroids or leaf payloads. Output SHA256 is515a037fc55b91347a3c9fd2a8211d1d22644b2967c9c3cf76ecdae76f62eae4. Qualification used separate cores from the live S3 Vectors publication.

Historical V139/V146/V149/V150 and earlier Fable findings were recovered and reconciled. Capsule duplication overlaps earlier replication-cost concerns; historical cost rejection is retained and is not a universal rejection rule for today's quality-first Pareto policy.

Decision: geometry supports further investigation, not adoption. Before implementation, independently verify that deduplicated leaf-page capsules preserve every row the current SOURCE path scores, including any range-bridge effects. Then preregister generation/publication/pinned-memory/transport bounds and require exact score/shortlist/SQ8-plan parity plus a cheap paired latency falsifier. Any quality-changing variant needs its own declared arm. No vendor win is claimed.
