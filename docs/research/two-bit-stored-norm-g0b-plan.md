# G0b: stored-norm returned-ranking diagnostic

Status: preregistered before opening G0b output. This is a new score using
already sealed bytes, not a rerun of the failed G0 primary score and not an
S3 query-path benchmark.

The complete G0 ReLAION-100k development attempt at source `9cf22cad`
returned 90.295% Recall@100 with the reconstructed-norm two-bit score versus
98.590% exact on the same fetched rows. The old code record contains a
four-byte exact centered source norm, but that primary formula ignored it.
G0b tests whether replacing only the coded norm term can recover the
8.295-point paired loss. This is the cheapest causal diagnostic before
training a different representation or changing the physical page layout.

## Frozen method

- Same terminal-closed old code-wave source, query, truth, code, membership,
  1,000 query/group plans and candidate rows as G0. Authenticate all old
  identities. Recompute the four-byte stored norm from source vectors and
  require bit-identical float32 values for all 100,000 source rows.
- For every query and each selected candidate row, independently rank three
  arms with float32 scores and source-ordinal ties: historical reconstructed
  two-bit distance; stored-norm distance
  `||q-mean||² + stored_norm - 2 * decoded_two_bit_dot`; and exact float32
  distance from source vectors. Return 100 distinct stable IDs. The same
  32-GET/16-MiB historical code plan applies to both code arms; no new S3
  query reads occur in this replay.
- Seal per-query GT100 hits, paired losses and candidate/GET/byte plans in a
  versioned canonical artifact. Independently recompute all counts from
  authenticated inputs with a separate score expression and sort. Require
  exact returned hits to equal old grouped containment for all 1,000 queries.
- Run once on Causality `c7i.8xlarge` Spot under the 3-GiB worker tree limit
  and zero swap, from an exact pushed source archive, at a new immutable
  `native-two-bit-norm-g0b/<commit>/runs/relaion-100k-dev1000-a0001`
  prefix. Publish terminal/validation/resources, authenticate all final
  artifact bytes and terminate immediately. An interruption discards the
  attempt and requires a new ordinal.

## Decision

Advance **stored-norm score fidelity only** if its net paired mean loss
against exact is at most 0.25 percentage point (250 GT100 hits over 1,000
queries) and its marginal p05 GT100 count is no more than one below exact.
Report returned Recall@100, p05, p95 per-query paired loss, sub-90 query
count, and the unchanged physical plan for all three arms. If it fails,
stop this record/scorer family and screen a materially different
representation before any 1M route experiment. A pass still does not prove
1M routing, live S3 latency, throughput, or 100M memory.
