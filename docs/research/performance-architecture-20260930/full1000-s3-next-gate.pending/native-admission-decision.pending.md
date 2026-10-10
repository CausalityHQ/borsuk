# Retained S3 admission decision

Pending native execution on causality EC2. This gate is correctness and source admission, not a cold performance campaign.

## Executable provenance

Baseline and publisher native source: c3e52c8bbf0fcc985c0a8a06d2abeec5d7442d38. Their exact ELF pins are in transport-roster.pending.json.

The separately qualified compare_native_replay ELF is 1871136 bytes, SHA256 03aca9d786119e15762add1f2f0d9331fc3c697b73214b98c8072a4b1477de4f. Its source revision is afb70da60727ba80147a97e71fce738ab15e6456; crates/borsuk/examples/compare_native_replay.rs SHA256 is f3073473a9a4b0255c9244d26f930a3c0aa39f9033ccbbe9328ccd569ab6cf89. Do not substitute the older reducer source at c3e52c8. Original compiler execution succeeded; the compiler campaign's independent closure failure remains INVALID.

## Required evidence, in order

1. Authenticate both closed compressed archives and manifests, verify every archive member against the exact inventory, extract the twelve selected roles create-only, and sync outputs. ARCHIVE_INVENTORY_EXTRACTED is inventory evidence only.
2. Independently verify each original native terminal, original actual manager exit, observer identity and drain proof. A successful extraction does not prove execution closure.
3. Reconstruct the historical 131072-byte request prefix from the authenticated 4096000-byte full request file on EC2. Require SHA256 664f5b269756a1de5a77c4ec359e56ccbe85c87603fa01fc5d87cc3f02e52667. Preserve the original archived 2560-byte truth; full truth is 80000 bytes. No corpus preparation or truth regeneration.
4. Bind expected native identity and bound-input rows from independently pinned original configs, cohort receipts, qualified baseline ELF and seven component source hashes. Run the qualified reducer separately for each original completed run. Each must have 32 selected queries, 314 hits of 320, and zero underfill. These are exact prefix observations, not a population recall threshold.
5. Authenticate original generation components independently: identical derivation receipt, normalized/source-order/SQ8/canonical pins, plane/router bodies and coefficient bits. Allow only the explicitly justified publication/SQ8 ETag rebinding. Equal returned results alone do not prove component provenance.
6. Publish the retained generation on S3 using the qualified native publisher, a fresh disjoint destination and the pinned actual S3 SQ8 ETag. Require source whole-body authentication, create-only metadata, destination validation and head-last publication. Use the resulting native receipt's metadata prefix and root SHA for the S3 baseline configuration.
7. Execute the unchanged native baseline on the full1000 inputs with diagnostic ordinals 0..31, trace=false; original manager exit and native exit must both be zero, resource limits proved and owned payload drained. Reduce that completed run independently, requiring the same 314/320 and zero underfill.
8. Run --scale-prefix-parity with schema borsuk-scale-prefix-parity-config-v1. Ordered runs are historical local Q32, full-input local panel32, full-input S3 panel32; all nested completed configs use v4. Require EXACT_PREFIX_PARITY, complete=true, matched_queries=32, ordered_ids_and_score_bits_equal=true, per_query_logical_charges_equal=true, request_and_truth_prefix_authenticated=true. The native reducer independently requires distinct artifacts, exact source/policy invariants, zero failed logical GETs and exact query-by-query returned IDs, f32 score bits, recall and charges.

Any failed source, config, environment, closure or resource prerequisite stops the gate as execution INVALID. Preserve the original attempt; no automatic paid retry. Every stage runs sequentially within resource-envelope.pending.json. Stop and terminate compute on terminal evidence; record instance and volume deletion.

## Advancement

Only after all eight checks pass may a separate disposable real-S3 canary establish the cold campaign's asset/CLI/cleanup mechanics. Then freeze and run the full1000 cold campaign, measuring physical S3 requests/bytes/errors, end-to-end quantiles, QPS, RSS and total cost. Neither this prefix gate nor the canary supports a matched S3 Vectors or Turbopuffer win.
