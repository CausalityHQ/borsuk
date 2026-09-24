# V179 mandatory primary cover in the production interval planner

V178 established that a fixed 32-GET/16-MiB cap cannot cover every primary
unit of the V164 relaid layout on the ReLAION-1M source pseudoquery panel.
Production planning must reject that condition explicitly and expose the
minimum resource floor so a caller can choose a larger budget when allowed.
No corpus-size threshold or special query ID is part of the policy.

`minimum_primary_cover` takes physical page geometry, a GET cap and a list
of mandatory pages. It deduplicates the pages, coalesces adjacent runs, then
joins the shortest gaps until the runs fit the GET cap. This gives the exact
minimum units and exact physical bytes among whole-page covers under that
GET cap. A short final page charges one rounded unit but only its actual
bytes. The function separately reports `minimum_budget_bytes`, the caller
byte cap required by the rounded-unit planner. For example, a 546-byte
physical cover consisting of a full page and a short tail needs an 832-byte
cap when each unit is 416 bytes. The function reports primary page count,
disconnected runs, bridged pages and all three resource floors without
looking at utility or ground truth.

`plan_weighted_nominee_pages` calls this floor before its weighted interval
DP. If the caller's byte cap is below `minimum_budget_bytes`, it returns
`InsufficientBudget`. After planning it independently checks that every
primary page is in a returned range. Any mismatch is an inconsistent-witness
error. This closes the previous possibility of returning a partial-primary
plan as if it were usable.

The function does not allocate a larger cap on its own. A later global
allocator can use this geometry-derived floor and a caller-approved cap to
redistribute resources across queries. Its calibration and paired quality,
latency, charged RAM and 10M/100M gates remain separate obligations.

The focused remote gate runs the `physical_interval::tests` Rust module from
the pushed source archive on one Causality Spot worker. It includes exact
shortest-gap, insufficient-cap, full-primary witness and short-tail tests.
This is a production correctness increment, not a serving performance
measurement or full repository assurance gate.
