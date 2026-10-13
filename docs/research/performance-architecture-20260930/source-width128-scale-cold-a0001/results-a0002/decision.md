# SOURCE width64/128 cold decision — 2026-10-13

Decision: retain explicit width64; width128 is not selected. This is a valid negative scheduling result, not an execution or recall failure. Library defaults remain unchanged.

Frozen source public5c4a82f4 (native32bce58e), protocol c4115b79, same baseline ELF b52f801c and comparator4daaa7d6 for both arms. Cohere Wikipedia multilingual-v3,1M/D1024/cosine/k10, identical1000 queries/truth/corpus/generation and source cache off. One sequential query, CPU0/100%, library512MiB,outer1GiB,swap0,pids128. Causality Spot c7i.2xlarge/eu-central-1a. Original a0001 failed capacity without creating an instance; a0002 ran once.

| Cell | Width | R10 | p90 ms | p95 ms | p99 ms | Sequential QPS |
|---|---:|---:|---:|---:|---:|---:|
| A1 |64|97.80%|171.471|185.347|319.936|6.664|
| B1 |128|97.80%|336.751|345.395|376.743|5.363|
| B2 |128|97.80%|339.120|349.678|521.251|5.075|
| A2 |64|97.80%|173.338|204.102|376.616|6.399|

All18 stages passed: both actual-query527 admissions, both disposable query0 canaries, four full1000 cells, completed reductions and two paired reductions. Exact ordered result ID/score bits, full trace/physical plan and logical-charge parity passed;9780/10000 hits, underfill0 in every full cell. Input/binary/config before/after checks passed; all owned units drained without forced cleanup; swap/OOM0.

Candidate paired p95 regressed86.35%/71.33%, p90 regressed96.39%/95.64%, QPS fell19.53%/20.68%, p99 regressed17.76%/38.40%, first-query wall regressed32.66%/9.74%. SOURCE p95 moved94.92/100.48ms to260.66/261.63ms; query CPU totals remained roughly95–99s per1000 queries. This localizes the observed regression to SOURCE tails but does not prove a socket, connection-pool, service, HTTP protocol or kernel cause. Do not attribute it to insufficient recall or waive the scheduling parity gate.

Every full cell has137785 logical GETs and44137574400 verified bytes; native process transport observed137802 attempts/137797 GET methods and44141928264 payload bytes including startup/credentials, no transport/stream failures. These are separate counters, not billed S3 requests, wire bytes or an actual bill. Process peak RSS: A1 81096704B,B1 96636928B,B2 87908352B,A2 78536704B. Outer cgroup peaks are retained separately.

Instance i-0dc0b50b530253533, SSM65d5b247-d00c-44ca-9bf1-c05151d50260, setup70b6e9e6-5936-4cb8-967b-21aa624d568d. Original watcher exited0, instance terminated, rootvol-03740370434c807f1 absent, timerinactive,BGoff. Quote0.2095USD/h; maximum4h compute at quote0.838USD; attempt ceiling1.50USD; actual lifecycle bill unqualified.

S3 Vectors retained reference: R1096.10%,p95215.023ms,QPS7.358. Width64 has higher observed recall and lower p95 in these runs, but lower QPS. Provider cache state is not controlled; raw versus normalized f32 ranking parity is not independently measured. This does not establish a matched vendor or total-dollar win. Turbopuffer has no matched reproduction; published dataset-family numbers do not establish matching selected IDs/query split.

Next causal gate: investigate the completed SOURCE tail distribution and transport scheduling at width64/128 using native evidence; do not repeat the failed128 arm or assume more concurrency is faster. Preserve the earlier width32/64 gain and portable8-row scoring negative. Any subsequent causal change needs a cheap native correctness falsifier before scale. SIMD remains guided by the retained CPU profile; Parquet/Arrow ingestion is outside this measured query path. CRUD/recovery/compaction/GC scale and lifecycle-dollar product gates remain open.
