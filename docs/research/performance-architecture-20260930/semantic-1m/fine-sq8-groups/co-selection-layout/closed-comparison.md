# Closed locality comparison

All rows use ReLAION and CoHere FIRST100k D768, consumed64 per dataset, unchanged frozen nomination, and 32-range/16MiB admission. These are virtual cover results, not wire latency, QPS or new recall measurements.

| Placement | ReLAION fit/64 | CoHere fit/64 | Worst ReLAION bytes | Worst CoHere bytes | Decision |
|---|---:|---:|---:|---:|---|
| Original order | 50 | 32 | 32747520 | 27081600 | REJECT |
| Source co-selection | 48 | 33 | 34831680 | 28928640 | REJECT |

Original arithmetic: ../sq4-refinement/pq-residual/source-probe/native-diagnostic/a0001/closed-nominee-group-bound-audit.md. Co-selection: native-diagnostic/a0002/{decision.md,root-verification.json,screen/report.plans.jsonl}. Both mandatory and expanded covers have the same fit counts. Source co-selection does not repair mandatory dispersion; it loses two fitting ReLAION queries and gains one CoHere query. No parameter ladder or payload publication follows.

Next design must reduce mandatory physical dispersion while retaining quality, or replace nomination with a jointly budgeted high-recall router. Independent design consultation44c5af9c05414c41 is running; its effective provider fallback is recorded, not assumed Fable. Selection awaits its evidence and independent critique. Product RAM, durable updates/compaction, 1M cold performance and matched vendor qualification remain incomplete.
