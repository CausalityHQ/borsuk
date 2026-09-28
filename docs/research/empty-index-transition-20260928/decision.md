# Empty transition and logical index API — GO for the primitives

2026-09-28 UTC; source parent `3bc23eda`. Beating BOTH S3 Vectors and Turbopuffer remains mandatory. No current matched release axis is closed by this result.

## What changed and why

All-deleted compaction input now has a real serving replacement: `publish_empty_two_bit_generation` writes only a typed schema/generation/dimensions root. No sentinel, zero-code plane, source/SQ8 object or empty graph is stored. Root creation is conditional; publication uses the shared authenticated create/CAS and lost-ack head path. Both populated and empty replacements must preserve dimensions/increase generation; replacing a base requires its mutation seal. Low-level callers still must prove corpus equivalence/all-deleted state.

`TwoBitIndex` gives both base kinds one logical-ID search API. It pins an authorized head, validates empty roots or delegates to the existing populated loader, charges wrapper/head/query conversion payload and gates the full query with a semaphore. Snapshot namespace/root/dimensions/payload must match. `None` is explicitly base-only, not an implicit latest transaction. Empty queries return zero rows or score bounded normalized pending puts with zero base GETs; populated queries preserve the existing authenticated range path. Oversized k clamps to actual candidates. Canonical recovery from empty roots authenticates one root GET and emits zero source-object bytes/GETs, enabling bounded preparation from resurrected pending puts.

This closes the empty-serving prerequisite. It does not implement a complete compaction orchestrator or reader-safe GC, and it does not qualify the disqualified ANN route. The next single implementation is callable crash-resumable compaction, including verified empty->populated rebuild/publication. The present test verifies pending-put search and two-row source preparation on an empty base; it does not yet execute that full back-transition through an orchestrator.

## Frozen original evidence

Both attempts use the same green archive `b8a0c51f7e80d2894330b8721fa44b111fedd2ac6c15a5ec251434436fefc367`; all7 source/test files match its immutable S3 bytes. Red archive `e822ea13108264ced144b1fb75ca8858211004ea0bc1b8539d6b8a41a1b04da3` contains the missing-API test over the original source. AWS causality Spot only, eu-central-1, c7i.4xlarge,4 Cargo jobs,30min machine cap. No Spark or performance/quality campaign.

| Original attempt | Terminal and causal result | Closeout |
|---|---|---|
|a0001 / i-05f040e9b34304930 | exit101: after red, Cargo reused the old library because restored green source mtimes predated the cached compilation. Source archive parity was valid; this did not run green product code. Negative retained. | EC2 terminated;177s; estimated compute$0.0178 |
|a0002 / i-09a12cd03b38ea17f | Native `cargo clean -p borsuk` invalidates only that package between archives. Red exit101 with missing APIs; green integration2 + affected real local HTTP4 all pass, terminal exit0/complete. | EC2 terminated;272s; estimated compute$0.0274 |

Quotes/elapsed estimates sum to$0.0452 compute, exclude EBS/S3 and are not invoices or total lifecycle costs. Actual source/instance/exit identities, terminal/artifact SHA, preregistrations, user-data, launcher and both closeouts are retained. No overlap or automatic replacement. Existing review cooldown remains unresolved; no duplicate consultation/override.

The512-row D2 integration checks nonempty all-delete->empty, lost empty head acknowledgement, authenticated reopen, empty result/no range GET, normalized signed MIN/MAX pending puts, oversized k, query/memory-pin/namespace/dimension/unsealed-replacement rejection, corrupt empty root rejection, and two-row canonical/delta preparation with exactly one root GET/zero source bytes. Populated HTTP checks use a synthetic2-row D2 object: logical base-only k clamp and pending-snapshot delegation match existing results, one signed conditional range GET/28 bytes, plus failure checks. These are functional identities, not recall or serving-latency measurements.

## Release gaps carried intact

Complete callable compaction/restart/GC and100M bounded-memory lifecycle qualification remain OPEN. Current full-corpus ANN quality has not recovered its frozen internal deficit. Independent matched S3 Vectors and Turbopuffer recall/lower-tail/p90/p95/QPS/$ and lifecycle evidence remain OPEN. Do not treat empty/delta exact scoring or local HTTP tests as a vendor win.
