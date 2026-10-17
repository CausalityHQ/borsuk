# Source-only construction helper: UNVERIFIED / HOLD

This package is a one-off research constructor. It has not been compiled or run.
It does not change the production library, admit historical v10 serving artifacts,
publish to S3, run queries, or make performance claims. Root owns lock generation,
builds, native verification, launch, publication, integration, and push authority.

Source/API contract base: 329fd2227a17c049b6fa9ceeee71f3ea57f24aad.
Root has subsequently applied test-only assurance fixes to the active library.
The runtime construction receipt records the API contract base, not a claim
about the compiled library revision. Root must bind the final library source,
lock and executable externally before launch.
Authenticated library path:
`/mnt/borsuk-workspace/source-capsule-library-combined-source-a0008/crates/borsuk`.
Source package:
`/mnt/borsuk-workspace/evidence/capsule-page-range-v2-construction-source-a0001`.
Construction design: the full Rank 1 recipe in
`evidence/capsule-page-range-v2-design-a0001/construction-plan.md`, plus
`spec.md` and `root-reconciliation.md`, all read on EC2.

## CLI and authority

`capsule-page-range-v2-construction CONFIG_JSON CONFIG_SHA256`

The config file is a regular absolute path, at most 65,536 bytes; the second
argument is its independently approved lowercase SHA256. The config is strict:
unknown fields, missing fields, duplicate fields, and wrong JSON types reject.
Its exact fields are:

| Field | JSON shape / meaning |
| --- | --- |
| original_root | { "path": absolute string, "sha256": lowercase SHA256 } |
| old_asset_root | Absolute historical flat asset directory |
| local_sq8 | { "path": absolute string, "bytes": positive u64, "sha256": lowercase SHA256 } |
| order | Same file-pin shape; physical-position to raw-ordinal LE u64 array |
| fresh_flat | Absent absolute directory, existing parent |
| fresh_caps | Absent absolute directory, existing parent |
| cap_bytes | Positive usize; total modeled payload capacity, including live caller pins |

The original root SHA must equal the independently approved
`96609071a9186cb3e9a159522f3ee6dfbc5dc81e86f87a8038632e5531b815ef`.
Its bytes must also equal `old_asset_root/manifest.json`. The admitted historical
case is exactly v10, flat_records, generation 1, base_epoch 0, semantic discovery
whose input root is the plane-manifest SHA. The original source-plane schema,
seed, record geometry and original 32-row digests remain unchanged.

N and D come from the authenticated plane and canonical descriptors and must fit
the authenticated semantic profile. There is no dataset-name or D1024 special
case. SQ8 must match all of R/P/Q authorities; order must be exactly 8*N bytes and
match the plane/discovery order digest. The config cannot replace those pins.

Inputs must remain immutable and exclusively controlled throughout this offline
operation. Every path component is opened using directory FDs and NOFOLLOW;
special files, symlinks and parent path components reject. The historical asset
tree is strict: exactly manifest.json, plane/ and router/, plus the eleven files
below. Unknown siblings reject; the helper does not filter or alter a historical
tree containing other files. Root may supply an independently prepared immutable,
byte-identical allowlisted snapshot if its historical directory has extra files.
Source FDs and original metadata stamps are retained and checked again after
construction, as well as all original content hashes.

Outputs cannot overlap each other or any input, including ancestor overlap.
They must be absent. Parent directories must already exist and remain exclusively
controlled. Partial directories/pending files are preserved on failure. Reusing a
failed output path is rejected. The helper performs no cleanup.

## Eleven exact asset pins

R, P and Q mean the authenticated original root, plane manifest and page manifest.
Manifests' exact lengths are measured only under a 64-KiB bound, then authenticated
by the independently rooted SHA. Remaining sizes use checked geometry.

| Relative path | Exact bytes | SHA authority |
| --- | --- | --- |
| plane/manifest.json | Authenticated P length | R.plane_manifest_sha256 |
| plane/mean.bin | 4*D | P.mean_sha256 |
| plane/records.bin | N*P.record_bytes | P.records_sha256 |
| plane/page_digests.bin | 32*ceil(N/32) | P.page_digest_sha256 |
| page_manifest.json | Authenticated Q length | R.page_manifest_sha256 |
| page_digests.bin | 32*ceil(N/32) | Q.page_digest_sha256 |
| router/root.bin | R.discovery.root_bytes | R.discovery.root_sha256 |
| router/membership.bin | 4*ceil(N/32) | R.discovery.membership_sha256 |
| router/leaves.bin | ceil(N/32)*(4+2*D) | R.discovery.leaves_sha256 |
| centroids.bin | 32+ceil(N/32)*2*D | R.discovery.centroids_sha256 |
| canonical.bin | N*(8+4*D) | R.canonical.sha256 |

Each copy is create-only, bounded streaming, SHA-checked and synced. Copies use
new inodes and become owner-read-only. No shared writable hardlinks are created.
The flat root preserves every original logical field, including source/router/
canonical/order/SQ8 key and exact ETag; only its schema becomes v11. JSON spelling
of low/step may normalize, but their directly decoded f32 bits must be identical.
The flat generation remains 1 and epoch remains 0, so Q is copied unchanged.

## Exact native API composition and validation order

1. Check the original root and plane/page bindings, pin all input file metadata,
   reject unknown paths, and require the production call
   `TwoBitGeneration::open(old_asset_root, approved_sha, limits)` to fail with
   `Invalid("root identity")`. An unrelated failure is not schema-rejection proof.
2. Create a private 0700 flat directory and copy only the eleven assets.
3. Use public `semantic_unit_router::validate_publication(body, membership,
   leaves, &SourceIdentity, centroids)` with the plane-bound identity. This checks
   all partition coverage, original FP16 unit bytes and prototype bits.
4. Use `PageAuthority::load_two_bit` and `verify_payload` for every complete
   original SOURCE unit, plus the full logical flat SHA.
5. Stream canonical and SQ8 together: full canonical and SQ8 SHA, every original
   SQ8 page digest, canonical-ID == SQ8-ID at every physical position, exact N,
   sorted-ID uniqueness, and a separate raw-order SHA and full permutation check.
   Negative application IDs are allowed. They never substitute for raw ordinals.
6. Sync child directories; write/sync a create-only pending flat root, install it
   with RENAME_NOREPLACE, then sync its directory and parent. No complete
   construction receipt exists yet.
7. Call exactly `two_bit_build::repackage_source_layout(flat, flat_sha, capsules,
   2, SourceLayoutMode::LeafCapsulesV2, limits)`. Its existing internal
   `validate_local_publication` runs before creating capsule output. That method
   is pub(crate), so the standalone helper does not call it directly. Its SOURCE
   and root admission is composed with steps 3–5; it alone is insufficient.
8. Authenticate the native capsule root. Only generation, source layout, and
   page-manifest generation/hash may change from the fresh flat authority.
   Require unchanged coefficient bits and every other logical field. Fully hash
   every unchanged copied logical asset. Reuse the complete router/ID proof only
   because those exact full bytes are unchanged.
9. Authenticate the v2 directory with public `Directory::open`; compare EVERY
   roster to public `leaf_pages(authenticated_router, leaf_id, cap)` before blob
   reads. Require BORSDR02. For every complete untrimmed capsule, use public
   `Directory::verify` with one complete leaf, `FetchedSpan`, and generation-2
   original SOURCE authority. This verifies its whole SHA, BORSCP02 framing and
   every original-unit occurrence, including duplicates. It does not use the
   partial-page request API. Stream the full physical blob SHA and exact length.
10. Use paged `TwoBitGeneration::open` on the fresh capsule root to check current
    native root/codec/router/page admission without hydrating SOURCE or canonical.
    Recheck all original file stamps/content pins and both new root hashes.
11. Only now seal the separate receipt at
    `FRESH_CAPS.construction-receipt.json` (append that suffix to the complete
    directory name). Receipt installation is create-only and synced. Its SHA and
    both root hashes are printed. A manifest without this final receipt is not
    full helper approval.

No whole-artifact approval is derived from a partial proof. This source contains
no S3 client, query interface, query/truth file input, timer-based scientific
gate, or launch wrapper.

## Capacity and operation limits

All input vectors reserve exact admitted lengths and reject unexpected capacity.
Canonical values remain one row at a time; SQ8 stays one original 32-row page;
original flat SOURCE stays one 32-row unit. Application IDs use an exact 8*N-byte
Vec and in-place sort; raw-order permutation uses ceil(N/8) separate bytes.
SHA/copy streaming scratch is 64 KiB. No canonical or SQ8 body is hydrated.

CONTROL = 8 MiB conservatively reserves bounded config/JSON trees, manifest
copies, path and asset descriptors, retained file stamps/FDs, SHA scratch and
receipts. Limits reject before large allocation or payload I/O in each phase.
Actual exposed Vec capacities and authority proof capacities are checked.

The semantic publication validator receives all encoded router/leaf/centroid
assets. Their exact capacities are charged in addition to the authenticated
profile's entire native construction allocation cap (128 or 512 MiB), which
conservatively reserves private validator allocations. All large encoded arrays
are dropped at that function boundary. During capsule approval the private
router is conservatively charged that same entire allocation cap, in addition
to actual authority/directory/leaf-buffer capacities. Directory open/leaf
charges include decoded rosters, lookup and framing scratch; only one capsule
body and one temporary comparison roster coexist. This intentionally overcharges
private metadata whose capacities the public API does not expose.

All ID/buffer/router arrays are dropped before the native repack or paged open.
Both calls charge CONTROL as already_pinned_bytes. Their query-related limits
are positive minimal values (one query, one byte/GET/scratch) used only for
publication admission; this helper never queries and these limits do not
represent a serving operating point. The caller must supply enough total cap
for native publication/build admission as well as these helper phases. This is
a payload-capacity model, not an allocator, page-cache, RSS or elapsed-time bound.

Authoring/format/static inspection only: EC2 instance i-048610f331fa447cc,
eu-central-1, profile causality; CPU10, weight10, 100% quota, 1GiB memory, no swap,
128 tasks, 120s per source operation. No Cargo, rustc, test, native execution or
actual payload reads were performed. Root controls a larger native host/time
budget, may predeclare 900s preparation, and must obey the existing hard shutdown
at 06:00:23 UTC. The helper imposes no arbitrary tiny time gate.

## Root-owned acceptance before any experiment

Generate/freeze the standalone lock, compile against the frozen authenticated
library source, and run the included focused unit tests (they are source only,
unexecuted here). Then source-only canary construction must independently confirm
all receipt pins/IDs and old-v10 rejection. Use a disposable corruption case:
alter a late duplicated capsule record and refresh physical capsule/blob/
directory/root hashes while retaining the original unit authority. Full
approval must reject it. Check malformed authority, symlink, changed-source,
existing-output and model-minus-one failures. Resource/time/setup failures are
execution INVALID. Native evidence and full artifact approval remain HOLD until
root verifies those checks; this source handoff is not that evidence.
