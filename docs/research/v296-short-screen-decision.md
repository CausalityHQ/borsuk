# V296 decision: short screen PASS; paired validation incomplete

Frozen source `3031b29a`. The exact Rust plan producer plus sequential-f32
reference SQ8 scorer reproduced all64 V293 development physical page sets,
fetched counts and returned counts. It also reproduced the V291 flat control.
This validates the combined harness before validation queries. The reference
arithmetic was already independently matched by production Rust on these
same development inputs; this screen adds no new Rust scorer replay.

| CoHere first100k D768 cosine k100 | Arm | Candidate mean | Fetched mean | Returned R@100 | Returned p05 |
|---|---|---:|---:|---:|---:|
| Development0–63 | bounded graph | 99.578125% | 99.546875% | 98.015625% | 96 |
| Development0–63 | paired flat | 99.812500% | 99.765625% | 98.187500% | 96 |
| Validation256–319 short screen | bounded graph | 99.562500% | 99.531250% | **98.296875%** | **97** |
| Validation256–319 short screen | paired flat | 99.859375% | 99.828125% | 98.578125% | 97 |

Both arms max32 planned GET /16,773,120bytes and40,704 compressed row scores.
Graph max1400 centroid evaluations; flat3125. The screen's graph deficit is
0.28125pp, within0.5pp. Mean and p05 clear their98%/95 gates. Nomination loses
0.4375 GT100 hit, fetch selection another0.03125, and SQ8 ranking1.234375:
SQ8 is now the dominant residual quality loss. No layer fails this screen.

| Local functional check | Wall seconds | Peak RSS KiB | Exit |
|---|---:|---:|---:|
| Development plan pair | 35.10 | 87376 | 0 |
| Development score pair | 6.98 | 237540 | 0 |
| Validation screen plan pair | 34.89 | 87132 | 0 |
| Validation screen score pair | 4.09 | 238324 | 0 |

These debug/reference verification costs are not HTTP query latency/QPS,
S3 transport/physical billing or lifecycle cost. All time receipts report zero
swaps. One wrong-config SHA check rejected before producing outputs. No
instance, paid job, architecture change or parameter sweep was launched.
The source-only mean/record plane and historical generation are pinned through
the trusted validation config, not a new production publication format.

**GO only to full frozen ReLAION+CoHere validation256–999.** This64-query
screen cannot qualify the744-query tail, a second dataset, cold service
latency,100M memory, or competitor superiority. Preserve the method unchanged.
ReLAION semantic-layout preparation and full validation belong on the intended
existing remote development machine; its current authenticated execution target
is not available in this session. MCP messaging failed with "cross-agent
messaging needs a live sender agent"; shell fallback was explicitly denied.
An operator access request was sent via `devbox-tell`. No new paid replacement
is authorized by this receipt. The product goal remains active.

Original raw plans, samples, results, exact time receipts and their hashes are
bound by [local closeout](v296-local-closeout.json). Historical V282 negative
results stay closed; V296 does not resurrect its PQ nomination route.
