# V138 a0001 GT-bearing input failure

The V138 a0001 Spot attempt completed its D96 computation and correctly
followed the fail-fast decision, but its evaluator parsed the full V122
`evidence.jsonl` record. That file contains `truth_ids`. The V138 algorithm
did not reference that field, and the negative result is numerically
reproducible, but the preregistration said **no GT is read**. Parsing the
record violated that input-isolation claim. Treat a0001 as a diagnostic,
**not claim-eligible** source-only evidence. No ReLAION input was downloaded
or evaluated after the D96 stop.

The complete terminal is at
`s3://borsuk-bench-453182569524-euc1/research/v138-unit-bound/2832f76339547fa5facf785fe53083ead6bb364c/runs/v138-20260924T085148Z/a0001/terminal.json`,
SHA-256 `17125405b80c93c309d95a39218954480bbe90ed292bb492de507be5fadba5cf`.
Source archive SHA-256 was
`3e6a3fabdfad670c32adca3d08ef08fa86c876c1326d506a8429671689d621b2`.
Spot `i-0170d0689a8ea3f6d` terminated after its 31-second terminal.
All eight terminal-listed artifacts were authenticated. The 1,000-row raw
SHA-256 was `3f93269c1d1e247583523bbaecfebb892261253b45138bdd9c2f22c8bb318ae0`.

The diagnostic had p95 **3,120 of 3,125** units and **391 of 391** pages
promising. Mean minimum 32-GET fetch was **10,781,973.504 bytes/query**,
against the registered 4,714,063-byte selectivity screen. The V132
candidate's historical mean S3 response was 9,428,126.976 bytes/query;
this exact bound would read more. These numbers are diagnostic only until
the GT-free a0002 reproduces the decision.

For a0002, `scripts/derive_v138_primary_only.py` authenticates V122 evidence
and writes only query ordinals and 100 source-only primary row ordinals to
`docs/research/inputs/v138-deep-primary.jsonl` (646,204 bytes, SHA-256
`04cdbea9079837806059799d9a90c2f579526de100b7743f83a3794a15c0d6e3`).
The worker receives this frozen derived file and never downloads V122 GT
or evidence. The unit geometry, error envelope, query cohort, stop rules,
SQ8 input, and timing method remain unchanged. a0002 has a new source
archive and immutable attempt prefix.
