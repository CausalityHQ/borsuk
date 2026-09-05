# V32 Global Serving Baseline

## Decision and scope

Use the existing physical1M Deep Image index and the global768-leaf route whose
burned32-query control achieved308/320. Do not rebuild or replicate pages.
The new global packing failed; its immutable terminal supplies replay hashes
and original control page selections, not a new production layout.

The first controller slice is a pure strict batch validator. It consumes exact
global schema3 qualifier bytes, an authenticated expected replay per query,
and authenticated physical page identities. No network, truth file, or page
body is needed to validate routing parity. It returns validated rows unchanged
for later independent quality/timing reduction. Concrete schema/type checks,
finiteness/order, bounds and all derivable row/byte/work relations fail closed.

## Authority

Each expected query binds ordinal, candidate replay SHA256 and ordered page
ordinals. Each page binds ordinal, SHA256, full Arrow IPC bytes, primary rows
and zero replicas. Serving must request exactly those sixteen identities and
return ten distinct ordered source matches within the registered source count.
The expected maps must be loaded from exact SHA/length-authenticated objects
before invocation; changing both result and untrusted metadata cannot re-root
authority. The pure validator accepts preauthenticated inputs and does not
pretend to authenticate their provenance itself.

Frozen configuration: global_leaf_limit768,scan_budget262144,
candidate_depth12288,page_count16,k10. Observed counts may be below scan/depth
ceilings but must be internally consistent. One32-row batch has explicit global
schema/scope/configuration; all rows must match. Source query ordinals come from
the external registered sequence, not row-owned labels. Preserve exact raw bytes
with their digest; JSON lexical floats are not byte-normalized for authority.

## Measurement progression

1. Pure mutation tests and a no-S3 integration using coherent synthetic rows.
2. Authenticate original manifest/page locations, frozen terminal and query/
   truth artifacts. Compare original64..95 control before admitting any result.
3. One Spot physical-read regression baseline on the existing index; no builder.
   Then separately register a disjoint quality cohort before observing it.
4. Measure sustained read concurrency and writes/visibility independently and
   together. Larger scales require a bounded code-store memory experiment first.

The baseline reports raw phase/end-to-end timing and empirical quantiles with
sample count.32 samples do not establish stable tail latency or throughput.
No15ms cold-S3 veto. Client-cache policy and connection reuse must be disclosed;
S3 internal cache state is not controllable. Logical page reads, encoded payload
bytes and any unmeasured transport attempts are labeled honestly; retry-inclusive
physical request instrumentation is required for amplification claims.
Total3GiB memory remains a release constraint, not proved by the resident1M
replay. No perfect-recall or release claim from this development cohort.

All science uses causality Spot, one original process, terminal/health-only
monitoring, immutable receipts and immediate termination. No automatic retuning
or larger rerun after failure; consult Astra with exact evidence.
