# Single-page-per-GET containment insufficient on CoHere

Exact optimistic GT-aware32-page bound, first100k D768 cosine k100,
development0–63 (64 used queries per dataset), current native semantic order
versus one fixed random permutation. No source fitting or query router.

| Dataset | Native mean upper GT100 coverage (%) | Native p05 (hits/100) | Random mean / p05 | Mean GT-bearing pages, native / random |
|---|---:|---:|---:|---:|
| ReLAION |99.468750|97|43.609375 /39|17.890625 /88.390625|
| CoHere |97.625000|89|43.765625 /40|28.312500 /88.234375|

Exactly32 physical256-row pages imply at most32 GETs and6389760 bytes.
This tighter containment test is different from the actual84-page/32-range
frozen gate. It cannot reject all current84-page serving policies, certify
returned recall, predict100M quality, or establish a vendor win. The current
native source fitter remains KILL from its earlier full-panel result.

Decision: do not specify a single256-row-page-per-GET leaf as the qualified
production default on this layout. Even GT-aware best-page selection misses
the proposed98.7 mean/95 p05 necessary bound on CoHere. Native order has strong
locality versus random, so this is not evidence that clustering is useless.
Next material layout specification: bounded hierarchical source fitting with
multi-page semantic extents and boundary coverage, then a cheap development
oracle/actual nomination falsifier. Do not tune more graph frontier schedules
or infer100% retrieval from the earlier84-page optimistic oracle.

Original attempt exit2 before Python: Snap Go launcher failed pthread_create
under the1GiB address-space cap. Cached temporary Python path lookup then
failed exit127 because UV removes that temporary environment after exit.
Final single scientific run exit0 after applying RLIMIT_AS1GiB inside Python
rather than to the Snap launcher. Both setup failures, final raw samples and
terminal are preserved; no scientific result was overwritten, no paid job or
compile started. Source/order/root/GT authentication passed in final run.
