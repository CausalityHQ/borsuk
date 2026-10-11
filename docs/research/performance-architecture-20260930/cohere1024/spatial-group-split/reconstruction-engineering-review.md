**ACCEPT this concrete approach as a design.** A separate BORSUSR2 validator/exporter is unnecessary for this exact frozen source pair, provided the byte gate remains exactly as specified. This is not implementation or runtime qualification.

I read all three design records, inspected `git show 2e2a789…`, and verified that the four critical frozen files match `5cc7330b`.

The proof holds for these sources:

- **Construction authority is independent of router decoding.** `Discovery::Semantic` pins both the whole leaf payload and original construction centroid digest; `Discovery::input` binds construction provenance to the plane. Reconstruction can therefore establish original BORSUCP1 bytes without opening historical root or membership. The proposed header matches the [native encoder](/data/target/borsuk-spatial-native-gates-a0001-repaired.pending/frozen-source/crates/borsuk/src/unit_centroid_pages.rs:110).
- **The root comparison covers the historical structural requirements.** With equal lengths, literal old/new magic, literal `0 → 1` at bytes 88–92, and equality everywhere else, the current radial decoder enforces the old header, reserved bytes, accounting, directory, source identities and prototype requirements. Radial allocation formulas are unchanged. See [encoding and decoding](/data/target/borsuk-spatial-native-gates-a0001-repaired.pending/frozen-source/crates/borsuk/src/semantic_unit_router.rs:1105).
- **Whole publication validation supplies the remaining checks.** It verifies membership, complete payload hash, leaf hashes, unique complete unit coverage, row accounting, original FP16 bytes and bitwise prototypes. Equal historical membership/payload transfers those checks to the old publication. See [validate_publication](/data/target/borsuk-spatial-native-gates-a0001-repaired.pending/frozen-source/crates/borsuk/src/semantic_unit_router.rs:1755).
- **Unstored algorithm names do not require an exporter here.** Both decoders reconstruct the fixed algorithm contract. Those radial constants, trainer, assignment rules, prototype computation and L2 ranking are unchanged across the inspected revisions. Ranking still accumulates squared distance in f64 and sorts by distance, then leaf ordinal. An exporter would reconstruct those same unstored names. This conclusion is specific to the reviewed source pair.

I found no source-backed blocker in that proof. These are the concrete implementation boundaries that must remain enforced:

1. **Publication validation cannot replace actual radial regeneration.**  
   The radial validator checks publication consistency but does not rerun training or verify radial assignment/order against trained centers. Merely changing historical magic/partition bytes and validating the result would miss that requirement. Invoke the real current radial builder on the authenticated reconstruction, then validate and compare its outputs. The proposed approach already does this.

2. **Extract metadata authentication; do not reuse `coverage_router` unchanged.**  
   That function requires the generation’s router digest to equal the router being opened, then invokes the BORSUSR3-only reader. A regenerated sidecar necessarily has a different root digest. Rebinding the historical generation or producer to satisfy this check would fabricate provenance. Preserve the original generation pin and construct a private runtime authority token before opening bodies; authenticate sidecar publications separately. See [coverage_router](/data/target/borsuk-spatial-native-gates-a0001-repaired.pending/frozen-source/crates/borsuk/examples/compare_native_replay.rs:5023).

   Apply independent metadata caps before parsing, recursive duplicate-key rejection, exact field/type checks, geometry and descriptor-length checks, calibration and source bindings. Authenticate the entire bounded historical root before reading its limit at 80–88. Reject an unsupported limit; never adjust accounting fields to obtain parity.

3. **The existing coverage budget cannot admit construction.**  
   Its one-router model is `32 × root_bytes + 64 MiB`; reconstruction and building introduce materially different lifetimes. The builder’s training estimate for this geometry is approximately **506.7 MB**, already including the centroid blob. Add external retained allocations—including historical comparison buffers, metadata and any retained router—without charging that blob twice. Release reconstruction scratch and complete artifact buffers between phases. See [allocation_phases](/data/target/borsuk-spatial-native-gates-a0001-repaired.pending/frozen-source/crates/borsuk/src/semantic_unit_router.rs:639).

   Stream historical payload comparison where possible. The existing `loss_binary` loads a complete body, so using it for leaves creates a real additional retained allocation. Logical allocation accounting remains separate from enforced host memory, scratch disk and measured charged memory.

4. **Existing tests do not qualify this new proof.**  
   Current tests cover BORSUSR2 rejection, algorithm mutations and publication corruption, but the builder roundtrip compares two current builds. It does not establish historical regeneration.

   The new EC2 falsifier must cover:
   - Wrong approved generation/plane/construction pins; duplicate JSON fields; malformed geometry and arithmetic/cap refusals.
   - Duplicate, missing and out-of-range IDs; NaN/Inf; truncation, growth and full-hash failure; exact signed-zero/subnormal preservation.
   - Every forbidden root difference, including accounting, directory, prototype and reserved bytes; invalid values inside the two permitted regions; unequal lengths.
   - Record permutation: reconstruction can be order-independent under an appropriate fixture authority, while historical parity must still reject disagreement with the immutable publication.
   - File mutation/replacement, symlink/hardlink aliases, occupied output, file-sync and directory-sync failures. Failed publication must leave no consumable success receipt.

   A fixture made solely by relabelling a current root tests comparator mechanics, not historical reproduction. Actual authenticated historical-input parity remains required.

The smallest coherent Rust slice is **one example-private authority/reconstruction/parity module**, narrow CLI dispatch, and its native falsifiers. It should reconstruct, build radial, validate, compare exact historical bodies, and durably publish a receipt binding source/config/binary identities and all artifact pins. No production compatibility reader or historical-root rewrite is needed.

The subsequent paired reducer must preserve original producer authority, use unchanged normalization and L2 top48 for both sidecars, and retain SOURCE limits of 128 GETs/64 MiB. Both complete selections and publication identities must pass durable sealing before truth opens. Keep [exact historical trace checks](/data/target/borsuk-spatial-native-gates-a0001-repaired.pending/frozen-source/crates/borsuk/examples/compare_native_replay.rs:5297), **313/320 returned**, **314/320 radial SOURCE closure**, and per-slot gains/losses before any spatial verdict. Coverage establishes neither warm nor cold performance.

**Release remains blocked on implementation and native qualification, not on an old exporter.** No edits, Cargo, tests, mocks, data-body reads, network, AWS actions or children were performed. The original qualification job and worker were untouched.
