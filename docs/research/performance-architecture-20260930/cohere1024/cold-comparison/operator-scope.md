# Operator benchmark scope correction — 2026-10-07

Latest operator direction supersedes the cache performance draft as the next scientific benchmark priority:

- Run one reusable S3 Vectors comparison on the exact frozen Cohere corpus/query/truth bytes; retain Rust benchmark source, configuration, raw completed receipts and reduction in BORSUK GitHub/main.
- Cold comparison first. For Turbopuffer use its published cold reference, disclosing scale/region/query/recall mismatches.
- Defer warm benchmarks and local drive caching until after cold comparison.
- Preserve and finish the original cache compiler qualification i-0ad0cffa253fefc0d/watcher16346. This is correctness work, not authorization to launch the deferred warmed cache performance draft.
- Establish a reproducible cold/infrequent-access protocol before S3 Vectors execution. Do not label an entire repeated-query stream cold merely because the client starts fresh; vendor backend cache state is not client-controlled evidence.
- Preregister region, index ingestion/readiness, idle interval, query cadence, client reuse, top-k, metric, retries, recall, p50/p90/p95/p99, throughput and costs. Record exact observed conditions when backend cache state cannot be verified.
- No new Python/TypeScript controller. Reuse native Rust and existing minimal execution glue.

Further operator clarification: maximize cold-case performance now. Match or beat cold S3 Vectors and published cold Turbopuffer at preserved recall before adding local cache layers. No warmed RAM-cache or SSD-cache experiment next. Preserve the already running cache correctness gate only; prioritize measured cold routing/layout/fetch-byte/GET/scheduling improvements after the once-only competitor reference is established.

Read-only plan e3e05e1b5a474d33 completed. Root independently corrected its mistaken original-corpus SHA: retention-receipt.json binds prepared/corpus.f32 to 409600000 bytes SHA256 3c95fa49a7d3f9d4bf6178f5ac2493e700a30fbcfe91da97a5fcf16a1f5fc09c. SHA20936913... is normalized.f32, not original raw corpus. Raw S3 key is research/semantic-router/20261006/cohere-native-preflight-a0002/retained/prepared/corpus.f32. HEAD metadata is not full-body authenticity; exact native full-body SHA/EOF admission remains required before upload. No data body read locally.
