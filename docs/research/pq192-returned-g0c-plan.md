# G0c: PQ192 code-only returned ranking on the closed 100k roster

Status: preregistered design, no G0c output opened.

G0 and G0b rejected two scoring formulas on the authenticated 200-byte
rotated scalar code. ReLAION-100k development returned Recall@100 was
90.295% and 88.464% versus 98.590% exact on the identical fetched rows.
The earlier V65 ReLAION-1M development PQ192x8 result retained all known
GT100 IDs inside an exact-rescored 512-row shortlist at some page budgets;
it did **not** report PQ-only returned top-100. G0c tests that missing
representation decision at nearly the same physical row width.

## Frozen method and bytes

- Reuse the G0/G0b closed 100k development source, 1,000 queries, truth,
  physical row order, and the exact same 32 selected groups/query. Authenticate
  the old complete terminal and every input. Training sees only the source.
- Train 192 independent four-coordinate, 256-centroid Lloyd codebooks on all
  100,000 source rows in the sealed physical order, 10 iterations,
  deterministic seed 6501 plus subspace ordinal, following V65's algorithm.
  Assign each physical source row one unsigned-byte centroid per subspace.
  Score each selected row by the sum of asymmetric query-to-centroid squared
  distances, cast to float32, with source-ordinal ties. Compare returned top
  100 stable IDs against exact float32 on those same rows. Persist codebooks
  and code bytes with SHA-256 receipts so a second implementation can replay
  every query and verify the code assignment.
- Price each row as 192 code bytes plus 16 stable-ID bytes, or **208 B/row**,
  and retain each original group header's page-count framing. At the G0
  largest roster of 77,113 rows, 208 B/row costs 16,039,504 B plus under
  1 KiB of group headers, within 16,777,216 B. An independent calculation
  from the authenticated G0 evidence gives 15,787,832 to **16,040,144 B**
  and exactly 32 GETs for all 1,000 fixed plans. This is a *planned*
  one-wave transfer, not a new actual S3 read. The 16-byte stable ID is
  conservatively charged to PQ; the old 200-byte two-bit record did not
  carry an ID and relied on the separately sealed positional mapping, so
  their quoted row widths are not like-for-like. The
  codebooks (192 × 256 × 4 × 4 = 786,432 B) are common resident data, not
  per-query S3 payload. The 100M generation-rollover memory ledger remains
  a separate gate.
- Publish canonical per-query PQ and exact hits, paired loss, p05, sub-90,
  GETs/bytes, trainer identity, codebooks/codes, resource logs and terminal.
  Require exact hits to reproduce old grouped containment for all queries.
  Use one immutable Causality Spot attempt from a pushed source archive,
  3-GiB worker-tree cap, zero swap, independent full artifact readback and
  prompt termination. An interruption invalidates the attempt.

## Decision

Advance **PQ192 code-only fidelity only** if paired mean loss versus exact
is at most 250 GT100 hits/100,000 (0.25 percentage point) and PQ marginal
p05 is at most one hit below exact. Otherwise stop this representation at
this fixed roster. Even a pass cannot satisfy the 99% product target on the
old plan: its measured exact ceiling is 98.590%. A new layout/router must
raise containment before any 1M serving promotion. PQ-only returned recall
on the existing 1M split and physical-byte limits are unmeasured until a
separate frozen G1 gate.
