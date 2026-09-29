# Authenticated native HTTP boundary: correctness/build GO

The development-only Rust example wraps existing public authenticated native S3
search. Exact root/generation/control epoch perrequest, k100/D768 finite nonzero
vectors,64KiB bodycap, nonqueued onepermit before JSON extraction, loopback listener,
existing1GiBmemory/400000scratch/32GET/16773120B/32parallel limits. One immutable
pinned generation perprocess. No separate scorer, cache, architecture, dependencies
or source-layout default. No corpus/HTTP performance qualification from compilation.

First narrow check discovered real shared API integration bug: the range-fetch
future's borrowed-tuple async map failed Axum's Send bound (`FnOnce not general
enough`). Original a1 closed INVALID, six artifacts/395 sources authenticated,
i-04fee36a9f60e9309 terminated393s/$0.0196 compute estimate excludingEBS/S3. No
benchmark query. Repair shared iterator with `.iter().copied().map(|(first,last)|...)`:
same order/offsets/work, no range-vector allocation; native object and two-bit
callers share it. Add compile-time Send regression check in existing range tests.

Corrected a2 source archive
`67acdf5698cbd5f58f59f5a5bc0c8e330d3be19a8911b773090bc723c225a977`,
base213973ec, terminal
`acf69a64ec5fd7e2c48fe3a0f69a573b05747d67efc1985e5d54149b2e0e5fba`.
One explicit epoch-comparison mutation failed the HTTP test (0pass/1fail), then
exact original source restored and original example test passed1/0. Affected
range tests5/0. ONE full-workspace gate: **2690passed /0failed /26existing ignored /
148executed test targets**. Cargo all-targets compiles this example, but its test is
run separately by explicit `--example`: it does not add a149th executed full-gate
target. Corrected verifier's inferred2691/149 expectation from the actual closed
roster; no test/build/benchmark rerun. Unit fixtures invoked searches; no corpus
ANN or HTTP benchmark ran. Source/scorer/route/physical admission parameters fixed.

Native session46097 CLOSED exit0. Instance **i-045b5c06a11565216 terminated2331s**,
compute estimate **$0.1165**, excludesEBS/S3, notinvoice/lifecyclecost. Check wall
2236.27s /maxRSS5387548KiB.10GiBcgroup reached10737418240B with18550 memory.max
reclaim events, swap0B, OOM0; this includes compilation and filecache, not serving
RSS. Check completed within bounds; do not extrapolate these resources to queries.

Release binary12523896B, SHA
`9202a5358e080454404cef15e2ef1f0605c29b6a5c6f73d3f015b7ed257707ab`.
Independent closed verification authenticated12artifacts,395 source files including
393Rust/Cargo, RED/GREEN/focused/full records, source archive, binary identity,
cgroup and actualEC2termination. This compiled authority can now be reused; no
new Cargo/full-gate/reviewer job for unchanged code. New actual HTTP controller's
wire/parity/quality check is the next development gate, not implied by unit tests.

Next: one ReLAION-first, CoHere-if-survives consumeddev0–63 incomingHTTP ABBA cell,
exact same-binary nearest/union heads and ordered IDs/counters as closed native
serving. Fixed gates and cleanup in http-preregister.md. Realrequest-start through
responseparse timing on samehost loopback, residentrouter/clientSQ8cacheabsent/
connectionsreused/S3servercacheuncontrolled; disclose topology and serialQPS only.
No fresh sealed panel access; ReLAION prospective cohort fails novelty as separately
verified. Source construction/reachability,1M/10M/100M, maintenance/recovery quality,
phase/pin RAM and BOTH-vendor matched lifecycle economics remain open.

Product goal stays active. No performance/vendor/production completion claim.
