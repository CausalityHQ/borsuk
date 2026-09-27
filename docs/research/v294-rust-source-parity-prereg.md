# V294 Rust two-bit source parity

Status: frozen before source parity outcomes. V293's bounded two-stage route
passes CoHere first100k D768 cosine k100 development0–63 by one returned hit
over its 98% mean gate. Rust `rotated_two_bit` is now source/query-generic,
but only a mathematical D768 encoder fixture has been checked. No new route,
query, layout, byte or recall parameter is authorized here.

Export a query-blind Python reference from the fixed raw source SHA
`0f3631d71c105e5ea3d701c96033b362c2f84bd43002a9c8a5c70040801be06e`
and layout SHA `303f31ab8a182a0aaa304c4ef551a046be41071ac24e67a793882eb74c5b532e`.
Use the unchanged source mean, seed 20260923 and historical `_fit_records`.
Seal the mean and physical-order reference records before Rust comparison.
No query or truth file is accepted by either source program.

Rust independently recomputes the source mean, authenticates the physical ID
order through the V283 SQ8 object, then compares every first 196 record bytes
(192 code bytes plus f32 scale) against Python. First compare 512 physical
rows; stop at any byte or mean mismatch before encoding all 100k. If that
passes, use the exact same source/configuration for all 100k. The new Rust
last 4 bytes contain the inverse reconstructed norm, deliberately different
from the historical centered-source norm; its metric relation is checked in
the focused library tests and must survive the later frozen route replay.

KILL/promote neither cloud nor validation if any prefix differs. A full
source pass permits one Rust lookup-scored replay of the V293 bounded page
route on development0–63, with unchanged 98% mean/p05 95 and 32 GET/16 MiB
gates, before paired validation. Local sample <=120 s, full <=600 s, each
<=2 GiB process RSS. Record exact exits, artifact hashes and wall/RSS only as
source-build verification cost. No paid job or full suite.
