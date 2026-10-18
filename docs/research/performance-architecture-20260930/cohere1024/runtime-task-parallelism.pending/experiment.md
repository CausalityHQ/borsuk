# Root-owned experiment slice

No real dataset, requests, truth, S3 or performance run was executed by this worker. These commands are for the root after final source/review/admission gates. Reuse its existing authenticated full1000 cohort and exact generation; do not truncate a cohort or invent a prefix receipt.

## Prepare the two configs

Use the qualified runner ELF and the existing full1000 config. The helper preserves every field except schema, explicit Q2 and runtime selection. It asserts that the two new configs differ only in query_runtime.

    python3 prepare-configs.py EXISTING_FULL1000_CONFIG QUALIFIED_RUNNER NEW_CONFIG_DIRECTORY

This writes current_thread.json, multi_thread2.json and their SHA256 pins, then prints the exact positional runner argv in A1/B1/B2/A2 order. Each output path must be new. The CLI is:

    QUALIFIED_RUNNER CONFIG CONFIG_SHA256 NEW_OUTPUT_JSONL

Root already prepared unadmitted runtime config drafts; the helper is a reproducible transformation recipe, not a request to discard or duplicate those drafts. The root must compare its drafts against this transformation before using them.

## Freeze expectations independently

Before executing any cell, build the expected identity and bound_inputs from the input config, source files, authenticated cohort receipt and qualified runner ELF. Never copy the first two result rows into the expected headers.

Preserve the base's independent header construction, with these exact changes:
- identity.schema = borsuk-cohere-native-baseline-result-v18
- both config_schema = borsuk-cohere-native-baseline-config-v17
- both max_active_queries =2
- both query_runtime = the selected config value
- both query_dispatch = spawned_ordered_batches
- both query_wall_semantics = dispatch_to_completion_including_initial_runtime_queue
- identity.config_sha256 = this config's original-byte SHA256
- identity.binary_sha256 = qualified runner ELF SHA256
- identity.runner_source_sha256 = final runner source SHA256

The seven other library source pins are unchanged by this packet. Freeze them from their actual source bytes. Preserve all source/producer/request/truth/generation pins and optional retry/semantic-leaf fields. Both arms charge two runtime workers and reserve143261696B, keeping caller-pinned and staging bytes identical. Startup separately emits actual dedicated workers0/2 and observed Tokio num_workers1/2; SOURCE and query budgets do not change.

## Execute and reduce

Root owns the single experiment controller and external resource gate. Use the same fixed CPU envelope, external8GiB memory cap, zero swap, one fresh process per cell and unchanged global SOURCE workers2/per-query downloads64. Do not run cells concurrently. No prewarm is introduced; backend/cache state remains unknown.

After each cell, bind its full original-byte length and SHA256 in the completed-reader descriptor, while retaining the independently frozen headers. The descriptor schema is borsuk-completed-native-reduction-config-v4. Its keys are schema, input:{path,bytes,sha256}, expected_identity and expected_bound_inputs.

    QUALIFIED_READER --completed-scale COMPLETED_CONFIG COMPLETED_CONFIG_SHA256 NEW_REPORT_JSON

The reader enforces original-byte SHA, EOF, complete query count, one cohort, seal-before-truth, exact headers, startup resource consistency, null per-query process counters and the ordered-batch elapsed floor. Failed/incomplete cells have no successful completed evidence.

Only after every cell passes the reader, compare each ordinal's returned, returned_physical_ordinals, plan, trace, charges and sum fields exactly across arms, including score bits. Require all1000 identities and logical GET/byte charges to match. Use cohort QPS/elapsed/processCPU and p95/RSS as descriptive repeated-cell tradeoffs, keeping Q1 and earlier cooperative Q2 results as separate Pareto points. Do not derive service-peak throughput, significance, physical billed totals or isolated TLS/network CPU from this packet.

The full frozen sequence is C2/M2/M2/C2 (the helper labels these A1/B1/B2/A2). The newly spawned current_thread Q2 is the control; earlier joined/cooperative Q2 is not this control. Report all four cells descriptively. Both M2 cells exceeding both C2 cells may describe separation, without a new significance/noise-floor gate. A positive result does not identify an internal mechanism; a null result does not isolate a bottleneck. Query p95/p99 include dispatch-to-first-poll queue delay.
