# Bounded metadata waves

Status: implementation authorized; no candidate measurement or launch authority yet.

## Measured reason

The closed metadata-head/a0001 gate retained identical recall and GET/payload counts while reducing FIRST100k D768 cosine k10 cold p90 from600.271 to560.681ms on ReLAION and596.348 to542.670ms on CoHere. Both use64 unique consumed development queries and128 observations per role. Metadata GET-header intervals still sum to roughly190ms; this is serial work, not a prediction of savings from overlap.

## One change

Authenticate and parse the root alone. Admit child metadata in ordered batches of at most four, validating total declared lengths and JSON/object bounds before payload requests. Reuse existing futures and synchronous scratch output. Keep4MiB ranges, at most8 aggregate GET futures and32MiB conservative payload buffers by assigning floor(8/batch-width) range slots per object. Preserve exact body/range/hash checks, unknown-length HEADs, descriptor-derived HEAD omission, cancellation and scratch cleanup. No router, scorer, format, quality, query or memory-cap change.

Record the wave number and measured wave wall time on each object. A wave wall is counted once in staging critical time; object header/transfer intervals may overlap and are not added as critical latency. Validate wave width, roster order, shared wall identity, request and buffer bounds.

## Qualification and next decisive gate

A bounded specialist owns the shared stager and necessary checker/test fixtures. Before handoff require affected Rust tests, HTTP release compilation, workspace Clippy correctness/suspicious and complete workspace test compilation on one frozen revision. Root verifies the diff and executes one actual changed-native full-workspace suite. Compile lanes are serial and explicitly granted; preserve original handles and stop/collect on sustained resource pressure.

After native qualification, freeze a new paired protocol using the qualified HEAD winner82c02967 as concurrent control and the new wave binary as candidate. Reuse immutable indexes,64-query panels and ordered references. No native build/refit/publication in timing. All512 ABBA calls must succeed with exact ordered IDs/quality/GET/payload parity and bounded resources/cleanup; BOTH candidate p90 must improve and p95 must not regress. A failed gate remains FAIL. No paid launch before complete authority, source/code/binary pins, clean pushed revision and safe terminal cleanup.

This reused100k panel does not qualify1M/10M/100M or beat vendor measurements. Fresh1M compact-router/bounded-training and quality remain the next scale requirement. Offered throughput and cost require separate closed measurements; serial reciprocal latency is not QPS.
