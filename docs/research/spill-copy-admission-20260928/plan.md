# Source-only spill copy admission (no query/GT)

Base08b71536, sealed78a3d4c4 hierarchical CoHere first100k D768 unit source and
extent layout. Use completed Fable418e158ae6644129 epsilon0.15/max3/rho<=2
proposal afterV38/V39 reconciliation, not a new centroid-minimum query arm.
Source-only extent means: f64 accumulate then normalize as f64; reject zero
or nonfinite mean. For each source row, retain its original physical owner.
Alternate unit-center squared distance <=1.15 times closest unit-center squared
distance. At most two eligible non-primary alternates, distance/owner-ID ties.
Zero closest distance admits only zero-distance alternatives. No query/GT file
is opened; no geometry/seed/layout fit is rerun.256-row exact source blocks.

Count assignments and owner populations, partition hypothetical postings into
<=1024-row chunks, immutable primary preserved. KILL if full rho>2. An early
KILL is valid only if assigned copies so far + one compulsory copy per unread
row already exceeds2*N; report a full-source lower bound, not completed rho.
No sample-average extrapolation. Geometry/hash/admission failure is setup/error,
not a scientific KILL. Source hash must match before any scientific processing.

One existing Spark job <=180s/4GiB/BLAS1, no paid run. Synthetic assertion covers
primary retention even when not closest, epsilon alternatives, zero-distance
boundary and deterministic ties. Only a source-cost admission: pass permits
an actual replicated-page nomination/coverage check at unchanged GET/byte caps.
Neither pass nor KILL is recall/latency/vendor or100M empirical evidence. Rho<=2
is a replication design budget, not a lower quality target. Stop at first fail.
