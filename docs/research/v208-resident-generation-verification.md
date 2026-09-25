# V208 resident nominee production binding verification

## One implementation decision

V207's used-split Deep-Image D96 result selects full-roster resident
FP16 scoring as the next production architecture candidate. V208 adds a
strict authenticated generation root that pins the source, source-only
PQ64 router manifest, layout, physical row map, resident FP16 plane,
immutable S3 object key and ETag. Search nominates from the router,
maps every ordinal to the plane's physical order, and scores the entire
shortlist from resident memory. It does not plan or fetch SQ8 vector
body ranges. RAM admission remains explicit in `ResidentFp16Tier`;
the caller must budget router, row map, concurrent generations and
workspace separately. No vector-count cutoff is introduced.

The production format marker is
`borsuk-resident-nominee-generation-v1`. No old experimental index
reader is required. A trusted whole-root SHA-256 and all component
digests must match before queries. The root's object key/ETag bind
conditional S3 hydration; content hash and FP16 header validation
remain mandatory. Query-time candidates are physical ordinals from
the authenticated bijection, not caller-supplied public IDs.

## Verification gate

Run one Causality Spot cell from a frozen Git source archive. Targeted
Rust tests must pass for generation root hash/schema, source and
artifact identity mismatch, nonidentity physical row mapping, complete
shortlist FP16 ranking, invalid query and top-k, and existing FP16
authentication/budget/tie behavior. Capture build/test logs and a
terminal artifact, read back every artifact digest, then terminate
compute. This checks the new binding and query path; it does not
establish live S3 latency, throughput, end-to-end recall or memory
at 10M/100M. Next gates are full assurance from the same stable diff,
authenticated production plane build/hydration, untouched Deep-Image
test ordinals 1000–1999 and a matched ReLAION source-only build.
