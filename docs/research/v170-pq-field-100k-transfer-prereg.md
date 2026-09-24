# V170 direct PQ field: paired 100k transfer screen

## Decision

Test whether the V168 direct PQ64 candidate score improves returned quality
on real queries when physical resources are matched to the strongest V163
ReLAION-100k layout point. This is a feature-transfer gate, not a production
latency, RAM, cost, or fresh-holdout gate. The method uses no dataset name,
corpus-size knee, or fitted coefficient in its query-time score.

## Frozen inputs and split

Use the **reused** ReLAION-100k D768 development-1000 V114 requests
(`b2485629b919614bf46877a779b16d678cd1690d1872b7d4f9c9cbe6ddd94eb0`),
reference (`fa42050d6610630576f3f00232aecb8c43e6a0af350bf0ac90094aaba00c9b7b`),
SQ8 manifest (`14a12fa3f7a571a99bf4e4411fea0a9f4a2180579decc3e539db558accda5be5`),
and truth (`ab8bfae34f753512f352581218596fc0f043354f8168192c856278b3ab5a0ce7`).
The V113 complete terminal binds source-only PQ books
(`e90c011aa3a013bed606d7f40b30bd41f5ed446637ac9c8724ced6cf2f078c87`)
and codes (`2eea1c265da723e799575b98ba29997eca9d58ef789a5244a96ca3bf89f6638f`).
The V163 complete terminal (`f4d2bd82d1ac4a7c7c52e03f1496c6a44ce77f619e0878d0e73577aaa6f4828f`)
binds new-physical-to-old-source `order.npy`
(`d7be74b09ade0a7477b62c2e14d68b94640dede5ac38be240e6e7428d41ac6e6`),
the relaid SQ8 object
(`76d325d20dd38063bb050f83cfa693f7748921c75280cb1bf1f988041329dd38`),
the V163 plan (`713511887fb7f853250ca073a40f432fb8fbb47471c0338e6c85468bcc30fecd`),
and returned rows (`a7656420c699b41a16ebf9124dae47ed81de977c6fb300128034d5503f6b16f1`).
Only terminal-listed complete artifacts are inputs. Authenticate byte length,
SHA-256, terminal identity, row permutation, and SQ8 stable-ID order.

## GT-blind plan

For each frozen query, use its 512 PQ nominees and 100 SQ8 primary rows.
Map old-source rows through the authenticated V163 inverse permutation.
Candidate units are every nominated 32-row unit and its immediate physical
neighbors, as in V168. Compute the V168 float32 PQ64 ADC score of candidate
rows. A unit's feature is its minimum row score. Assign rank weight
`candidate_count - rank`, breaking equal scores by physical unit ordinal.
Add an exact-primary priority greater than the sum of all candidate weights
to each primary unit. Maximize total weight with the existing exact interval
DP, capped **per query** at V163's GET count and full 32-row unit count.
Independently recount that the resulting plan covers every primary unit,
never exceeds V163's GETs or bytes, and obeys 32 GET/16 MiB global caps.
The priority is an implementation device, not a learned score; the checker
must reject any missing primary. Plan and source-feature seals are written
before truth is downloaded or opened.

## Return gate and stop rule

Score only fetched SQ8 rows with the frozen V163 quantizer and deterministic
top-100 rule. Compare each query with V163's closed returned IDs and GT100,
including wins/ties/losses, total hits, p05, physical GT coverage, bytes and
GETs. The screen advances only if total returned GT100 hits are at least
**99,347** (25 above V163's verified 99,322), p05 hits/query is at least
**98**, every primary remains covered, and no query exceeds its paired V163
GET or byte amount. This 25-hit threshold was fixed before new returns were
computed. If it fails, retain raw evidence and change the responsible
feature, threshold, or planner layer before another test. Even a pass
requires an untouched real-query holdout, an independent 1M comparison,
live-S3 latency, charged RAM, and recovery gates before production selection.

## Execution and interruption

Run one immutable archive from a pushed source commit on Causality Spot,
with a hard wall cap. Upload terminal-listed artifacts and hashes even on
failure. Stream and rehash every artifact only after a complete terminal;
discard and restart an interrupted measurement cell with a new attempt ID.
Terminate the worker immediately after the terminal marker. No local full
suite or large data build runs on the swap-pressured devbox.
