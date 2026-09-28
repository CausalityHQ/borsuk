# Next decision: source-only epsilon-closure copy admission

Existing completed Fable review418e158ae6644129 proposed spill-replicated exact
SQ8 tier: retain primary copy, source-only epsilon0.15 closure, at most3 copies,
mean copy amplification<=2, keep compressed nomination and select co-locating
copy before budget selection, logical-ID dedup. Its proposed fetched gate98.9
mean/p05 96 and returned frozen gate remain unchanged. The review did not
measure this design; its disk observations and100M costs are historical/model.

Independently reconciled prior evidence:
- V38 (publication-v3-attempt-ledger.md:4702): ReLAION1M,123 postings<=10240
  rows, primary plus one alternate, globally capped250000 alternates (rho1.25).
  GT-aware14-posting ceiling98.855%/minimum76 failed its99.8%/minimum80 gate.
  This is a containment failure for that bounded hyperplane relation, not an
  actual source-cell copy-choice query result or the current SQ8 byte contract.
- V39: same exact relation,21-posting feasible ceiling99.851%/minimum90 passed
  its oracle contract. This is not a routing/byte/serving/vendor win; don't
  revive its large postings or raise the current GET/byte caps.
- V139/V146/V149/V150 and page-score/diverse variants remain KILL. No
  single-copy page hierarchy,100M exact scan or compressed graph-walk arm
  is reopened by spill. The current hierarchy's three centroid nomination
  failures also remain immutable.

Next cheapest admission check, before any new query route or format: stream
CoHere's sealed normalized source, retain original hierarchical extent owner,
calculate source-only alternate owner eligibility using unit extent means,
epsilon0.15 distance closure, nearest eligible alternates/owner-ID tie break,
max3 copies. Record rho, per-cell populations and resulting <=1024-row physical
extent count, using original ordinal order within an owner. Reject nonfinite
or zero direction, and stop if mean copies>2 or source-build memory/time cap
fails. No source query/GT input; no new fit or parameter change. This only
checks whether the proposed replication cost is admissible; a pass certifies
neither containment nor nomination. Primary owners must never be dropped.

Exact closure rule to freeze in executable plan before execution: an alternate
has squared unit-center distance <=1.15 times that row's closest unit-center
distance; retain the current primary even if not closest, take at most two
eligible non-primary alternates in distance/owner-ID order. If closest distance
is zero, only zero-distance alternatives qualify. This is distinct from V38's
one hyperplane alternate/global25% allocation. It applies generically to rows,
not GT or dataset-specific tuned thresholds. Lowest-cost source-only test
must precede any query/GT replay.

Production decision if source admission and actual copied-page/returned-quality
falsifiers survive: authenticated CSR row-to-copy physical locations plus
logical-ID dedup, source-only insert copy assignment, logical tombstones,
in-process copy-aware compaction. Reuse current immutable publication/head CAS;
no separate cleaner instance. Resident compressed nomination stays bounded and
no full-vector/full-graph hydration is permitted. Copy-map and pinned-generation
RAM, storage/build/update/GET lifecycle cost must be charged. At rho<=2, SQ8
storage alone projects<=156GB at100M D768 (780B/row); this is arithmetic only,
not a price or competitive measured cost. No cloud/1M/100M promotion from a
copy-admission check. Both-vendor final gates remain OPEN.
