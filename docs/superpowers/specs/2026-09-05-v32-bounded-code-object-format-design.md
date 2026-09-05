# V32 bounded code-object format

## Purpose and evidence

Implement the first independently testable component of a streamed code
provider: an authenticated, bounded Arrow IPC code object. This does not
change routing or provide a complete S3 serving path. The evidence ledger's
root-preserving comparison verifies identical row unions, with mean83.9
objects/12.09MB useful codes versus226.6 fragmented objects/14.82MB for the
same population. Actual encoding overhead remains to be measured.

Routing regions and storage objects are distinct. Objects preserve original
code-parent residual centroids; group representatives never enter PQ scoring.
The successful root64 diagnostic remains an experimental differential oracle,
not a100M-qualified router. The eventual reader must remove global code-plane
and fidelity residency, score/release bounded objects and retain only bounded
candidates. This format slice is necessary but does not establish that result.

## Exact object contract

New format marker `borsuk-v32-bounded-code-object-v1`; no legacy aliases.
One uncompressed Arrow IPC file, exactly one record batch, no dictionaries.
Each record represents one code parent, sorted by strictly increasing parent
ordinal. Schema metadata is exactly `borsuk.format` with the marker above.
Fields and every nested child are non-nullable and contain no nulls:

| Field | Arrow type |
|---|---|
|code_parent_ordinal|UInt32|
|centroid|FixedSizeList(element: Float16,96)|
|ranges|List(element: Struct(logical_start: UInt64,row_count: UInt32))|
|high_bits|Binary|
|base_codes|Binary|
|high_codes|Binary|

Use original finite f16 code-parent centroid bits. Each parent's nonempty
positive ranges are sorted by logical_start, nonoverlapping, and checked for
u64 endpoint overflow. Logical ranges across parents must not overlap either.
Concatenating a parent's ranges defines its local row order; gaps are legal.
Fidelity uses little-endian bit positions within each byte: bit i selects
high48 for local row i, otherwise base24. Require exactly ceil(rows/8) bytes
and zero unused high bits. The packed base/high arrays contain only their
respective codes in local row order, with exact lengths24*base_rows and
48*high_rows; no48-byte padding or per-row logical-ID column.

Hard bounds per object:1..8192 rows,1..32 parents,1..128 total ranges,
1..524288 encoded bytes. A parent is indivisible. Empty objects, oversized
parents, duplicate IDs, overlap, nonfinite centroids, bad padding and any
length mismatch fail closed; no truncation or spill to a second format.
These are format constraints, not claims that every future partition fits.

Authenticate exact expected SHA256 and length before parsing. Inspect IPC
footer/message metadata before batch materialization: reject compressed
record bodies, dictionaries, extra batches, invalid offsets/body extents,
record count above32 and incompatible schema. Then validate all decoded
Before materialization also bound nested field-node populations: at most128
range children,3072 centroid elements and code/bitmap buffer lengths allowed
by8192 rows. Every buffer extent must lie inside the authenticated body;
small encoded size alone does not justify trusting declared child lengths.
Then validate all decoded
shape, nullability, ranges, bitmaps and lengths. Encoded512KiB is not a claim
of total memory use; decoded copies and Arrow metadata are additional bounded
work whose allocation behavior must be covered by malformed-input tests.

## Internal interfaces and ownership

Create `crates/borsuk/src/v32_code_objects.rs`, registered privately in lib.rs.
Until a production consumer exists, register it with `#[cfg(test)]` so the
codec-only checkpoint does not require fake exports or dead-code suppression.
Promotion to an ordinary private module requires a later production build and
Clippy gate; this checkpoint is not an enabled serving implementation.
Use crate-private types `V32CodeRange`, `V32ParentCodes`, `V32CodeObject`.
The owned parent stores centroid/ranges/bitmap and its two bounded code arrays.
`V32ParentCodes::code(local_row)` returns the width and borrowed code slice;
`logical(local_row)` maps through ranges without a per-row mapping allocation.
Addressing consumes a validated immutable parent; do not repeat complete
object validation for every row. Local lookup still uses checked bounds.
Provide `validate`, `encode_v32_code_object`, and `decode_v32_code_object`.
The codec has no network, page store, query, truth, routing selection or global
code-plane API. Do not export public symbols before a real consumer needs them.

Higher-layer directory validation is separate: it must bind object root,
parent/range membership, row totals, codebook identities, SHA and length to a
complete unique selected population before any code GET. The codec cannot
prove that an independently coherent object belongs to a specific directory.

## Verification and following components

First lock literal mixed-width/gapped-range addressing and invalid-state
branches with TDD. Then lock exact Arrow schema, source-byte authentication,
roundtrip identity and malformed file rejection. Independently read a Rust
object with PyArrow and a Python-written object with Rust; do not accept only
self-roundtrip as cross-language proof. Measure uncompressed byte size on a
synthetic8192-row all-high object with32 parents/128 ranges; require<=512KiB.

Next components, not delivered by this slice: extract the existing lazy
parent scorer with resident-versus-object bitwise candidate/page differential
tests; validate the full directory; add a four-slot fetch/authenticate/decode/
score/release pipeline whose slot lasts through scoring; then add a streaming
construction emitter. The initial fixture repacker is diagnostic-only, never
the production ingestion path. Cold S3 latency, throughput, write amplification
and100M geometry remain unqualified until their own measurements pass.
