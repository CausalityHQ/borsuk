# V171 1M primary feasibility: strict per-query profile rejected

## Decision and root cause

**Reject only the strict V155 per-query GET/byte matching profile.** On the
**used** ReLAION-1M D768 validation-1000 rosters and V164 smooth physical
order, 999 of 1,000 queries can cover every SQ8 primary unit within their
paired V155 sparse GET and byte caps. Query **898** cannot. It has 26
distinct primary 32-row units in nine disconnected physical runs. V155's
paired cap for that query is **3 GETs and 3,394,560 bytes**. The exact
minimum 3-GET cover must bridge 3,894 empty units, fetching 3,920 units
or **97,843,200 bytes**. That exceeds the paired cap by 94,448,640 bytes
and also exceeds the global 16,777,216-byte query cap. Nine separate
primary runs could instead be fetched with nine GETs and 26 units
(648,960 bytes); this is a valid coverage witness, not a measured
returned-quality plan.

The failing constraint is the inherited **per-query GET allocation**, not
the PQ score, corpus size, or a generic 1M impossibility. The old V155
layout needed only three GETs for query 898; the V164 order spreads its
mandatory rows across nine runs. A rule that gives each query enough
GETs to cover mandatory primary units, then prices optional units under
the caller's resource profile, is the material next scheduling change.
This must be preregistered and evaluated over all queries; no branch may
special-case query 898 or the dataset name. Aggregate competitiveness
against V155 remains open because GETs can be allocated differently
across queries. The paired scored V171 cell was **not** launched, and no
source vectors, PQ code plane, SQ8 body, truth or returned IDs were opened
in this feasibility cell.

## Closed evidence

One Causality Spot `c7i.xlarge` instance `i-06820e36ec2001c83`
ran pushed source `49b97ad8bb72a40df29a3eb5296e55e99c1c48dd` at
`s3://borsuk-bench-453182569524-euc1/research/v171-primary-feasibility/49b97ad8bb72a40df29a3eb5296e55e99c1c48dd/runs/a0001/`.
The complete terminal SHA-256 is
`0944954f4a01e4f3f329ca5556a659cbd2876f199ecb08eaeb3f3e69055b9e69`;
the summary SHA-256 is
`7828df94ceda41e3ddfbb8a9507a966aa038a1da87af58ab9b45fa8149978df2`.
The frozen input hashes in the terminal-bound summary include V116's
primary rosters, V63/V164 old/new source permutations, the V164 layout
seal and complete terminal, and V154's GT-blind sparse plan and complete
terminal. The query-level raw artifact SHA-256 is
`eba12f08207b93dff4059f8ea313812c19b0fc5efef2c472b4ea05955baa3a56`.
The checker deterministically replayed the 1,000-query calculation and
matched raw/summary hashes, returning `pass` /
`per-query-profile-infeasible`. The controller streamed and rehashed all
six terminal-listed artifacts and confirmed the instance `terminated`.
An independent postterminal read rehashed the terminal, summary and raw
rows, and isolated query 898's 94,448,640-byte deficit.

The V154/V155 control caps sum to the verified V155 point:
11,134,007,040 planned bytes and 22,126 planned GETs over 1,000
queries. The exact minimum-primary witness under each query's V155 GET
cap sums to 1,196,008,320 bytes and 17,678 GETs, but includes the
infeasible query 898 and **does not** predict a feasible aggregate
quality/resource point. Offline evaluation took 1.79 wall seconds and
peaked at 66,064 KiB process RSS on the Spot worker. These are planner
diagnostics, not serving latency or charged production RAM.

## Next gate

Replace the per-query historical GET caps with a **generic
geometry-aware** minimum: first cover mandatory primary runs within the
physical 32-GET/16-MiB caps, then admit optional nominee/neighbor units
from a calibrated utility/cost frontier. Preregister the caller resource
profile, tie rule, quality target, aggregate comparison and GT-blind seal
before opening the 1M truth. Compare direct PQ and neighbor-rank arms at
disclosed bytes and GETs, then apply V155's exact-source rerank and
99,567-hit/p05-98 baseline gate. A one-query feasibility failure must
not be hidden by an aggregate success claim. Lean can prove the
mandatory-gap lower bound and conditional resource caps; returned recall,
latency and memory still require measurement.
