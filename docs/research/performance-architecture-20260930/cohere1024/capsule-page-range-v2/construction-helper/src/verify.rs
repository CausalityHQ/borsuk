use crate::{
    Result,
    authority::{Authority, Config, text},
    ensure,
    io::{self, CONTROL, add, mul},
};
use borsuk::{
    semantic_unit_router::{self, SemanticUnitRouter, SourceIdentity},
    source_capsule::{Directory, FetchedSpan, Geometry, LeafPages, leaf_pages},
    sq8_page_authority::PageAuthority,
    two_bit_generation::{
        SourceLayout, TwoBitGeneration, TwoBitGenerationError, TwoBitGenerationLimits,
    },
};
use serde::Serialize;
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::{io::Read, path::Path};

pub fn limits(cap: usize) -> TwoBitGenerationLimits {
    TwoBitGenerationLimits {
        max_memory_bytes: cap as u64,
        already_pinned_bytes: CONTROL as u64,
        max_active_queries: 1,
        max_query_bytes: 1,
        max_query_gets: 1,
        max_parallel_gets: 1,
        max_source_bytes: 1,
        max_source_gets: 1,
        max_parallel_source_gets: 1,
        max_query_scratch_bytes: 1,
    }
}
pub fn reject_historical(c: &Config) -> Result<()> {
    // Only the strict schema-rejection outcome counts; an unrelated resource or
    // missing-file failure cannot stand in for production's v10 rejection.
    match TwoBitGeneration::open(
        &c.old_asset_root,
        super::authority::ORIGINAL_ROOT_SHA,
        limits(c.cap_bytes),
    ) {
        Err(TwoBitGenerationError::Invalid("root identity")) => Ok(()),
        _ => Err("production did not reject historical v10 at root identity".into()),
    }
}
fn input(a: &Authority) -> Result<SourceIdentity<'_>> {
    Ok(SourceIdentity {
        profile: a.profile,
        schema: &a.plane.schema,
        root_sha256: text(&a.root, "plane_manifest_sha256")?,
        centroids_sha256: text(&a.root["discovery"], "centroids_sha256")?,
        rows: a.plane.rows,
        dimensions: a.plane.dimensions,
    })
}

pub fn router_publication(a: &Authority, root: &Path, cap: usize) -> Result<()> {
    let names = [
        "router/root.bin",
        "router/membership.bin",
        "router/leaves.bin",
        "centroids.bin",
    ];
    let mut retained = CONTROL;
    for name in names {
        retained = add(retained, a.asset(name)?.bytes)?;
    }
    // The public validator's construction cap covers its internal decoded
    // centroid/router/partition allocations; caller-owned encoded inputs are extra.
    io::admit(cap, &[retained, a.profile.allocation_cap()])?;
    let body = a.read_asset(root, names[0])?;
    let membership = a.read_asset(root, names[1])?;
    let payload = a.read_asset(root, names[2])?;
    let centroids = a.read_asset(root, names[3])?;
    io::admit(
        cap,
        &[
            CONTROL,
            body.capacity(),
            membership.capacity(),
            payload.capacity(),
            centroids.capacity(),
            a.profile.allocation_cap(),
        ],
    )?;
    semantic_unit_router::validate_publication(
        &body,
        &membership,
        &payload,
        &input(a)?,
        &centroids,
    )?;
    Ok(())
}
fn page_authority(
    a: &Authority,
    root: &Path,
    generation: u64,
    cap: usize,
) -> Result<PageAuthority> {
    let p = a.asset("plane/page_digests.bin")?;
    io::admit(cap, &[CONTROL, mul(p.bytes, 3)?])?;
    let sidecar = a.read_asset(root, "plane/page_digests.bin")?;
    let proof = PageAuthority::load_two_bit(
        &a.plane_body,
        text(&a.root, "plane_manifest_sha256")?,
        generation,
        &sidecar,
    )
    .map_err(|e| format!("SOURCE authority: {e:?}"))?;
    io::admit(
        cap,
        &[
            CONTROL,
            sidecar.capacity(),
            proof.proof_payload_bytes().ok_or("proof capacity")?,
        ],
    )?;
    Ok(proof)
}
pub fn flat_source(a: &Authority, root: &Path, cap: usize) -> Result<()> {
    let width = mul(a.plane.rows.min(32), a.plane.record_bytes)?;
    io::admit(
        cap,
        &[
            CONTROL,
            mul(a.asset("plane/page_digests.bin")?.bytes, 3)?,
            width,
        ],
    )?;
    let proof = page_authority(a, root, 1, cap)?;
    let mut buffer = io::vector(width)?;
    io::admit(
        cap,
        &[
            CONTROL,
            proof.proof_payload_bytes().ok_or("proof capacity")?,
            buffer.capacity(),
        ],
    )?;
    let pin = a.asset("plane/records.bin")?;
    let path = root.join(&pin.path);
    let mut file = io::Input::open(&path, pin.bytes as u64)?;
    let mut h = Sha256::new();
    for page in 0..a.plane.rows.div_ceil(32) {
        let range = proof
            .byte_range(page, page)
            .map_err(|e| format!("SOURCE range: {e:?}"))?;
        let bytes = &mut buffer[..range.len()];
        file.file.read_exact(bytes)?;
        proof
            .verify_payload(page, page, bytes)
            .map_err(|e| format!("original SOURCE unit {page}: {e:?}"))?;
        h.update(bytes);
    }
    file.end(&path)?;
    ensure(
        format!("{:x}", h.finalize()) == pin.sha256,
        "complete logical SOURCE SHA",
    )
}

#[derive(Debug, Serialize, PartialEq, Eq)]
pub struct IdReceipt {
    pub rows: usize,
    pub ordered_application_id_sha256: String,
    pub physical_to_raw_order_sha256: String,
    pub unique_application_ids: bool,
}
fn mark_order(seen: &mut [u8], ordinal: u64, rows: usize) -> Result<()> {
    let ordinal = usize::try_from(ordinal)?;
    ensure(ordinal < rows, "raw ordinal range")?;
    let mask = 1u8 << (ordinal % 8);
    ensure(seen[ordinal / 8] & mask == 0, "duplicate raw ordinal")?;
    seen[ordinal / 8] |= mask;
    Ok(())
}
fn same_id(canonical: &[u8], sq8: &[u8]) -> Result<i64> {
    ensure(
        canonical.len() >= 8 && sq8.len() >= 8 && canonical[..8] == sq8[..8],
        "canonical ID != SQ8 ID at physical position",
    )?;
    Ok(i64::from_le_bytes(canonical[..8].try_into()?))
}
pub fn canonical_sq8_ids(
    a: &Authority,
    c: &Config,
    root: &Path,
    page_body: &[u8],
    page_sha: &str,
) -> Result<IdReceipt> {
    let (n, d) = (a.plane.rows, a.plane.dimensions);
    let canonical_width = add(mul(d, 4)?, 8)?;
    let sq8_width = add(d, 12)?;
    let sq8_page_bytes = mul(n.min(32), sq8_width)?;
    let side_bytes = a.asset("page_digests.bin")?.bytes;
    // IDs use a contiguous Vec plus in-place sort; raw ordinals use a separate
    // bitset. Application IDs are never interpreted as source ordinals.
    io::admit(
        c.cap_bytes,
        &[
            CONTROL,
            mul(side_bytes, 3)?,
            mul(n, 8)?,
            n.div_ceil(8),
            canonical_width,
            sq8_page_bytes,
        ],
    )?;
    let sidecar = a.read_asset(root, "page_digests.bin")?;
    let proof = PageAuthority::load(page_body, page_sha, &sidecar)
        .map_err(|e| format!("SQ8 page authority: {e:?}"))?;
    ensure(
        proof.rows() == n
            && proof.dimensions() == d
            && proof.page_rows() == 32
            && proof.object_sha256() == c.local_sq8.sha256,
        "SQ8 proof geometry/identity",
    )?;
    drop(sidecar);
    let mut ids = Vec::<i64>::new();
    ids.try_reserve_exact(n)?;
    ensure(ids.capacity() == n, "ID capacity")?;
    let mut seen = io::vector(n.div_ceil(8))?;
    let mut canonical = io::vector(canonical_width)?;
    let mut sq8 = io::vector(sq8_page_bytes)?;
    io::admit(
        c.cap_bytes,
        &[
            CONTROL,
            proof.proof_payload_bytes().ok_or("SQ8 proof capacity")?,
            mul(ids.capacity(), 8)?,
            seen.capacity(),
            canonical.capacity(),
            sq8.capacity(),
        ],
    )?;
    let canonical_pin = a.asset("canonical.bin")?;
    let canonical_path = root.join("canonical.bin");
    let mut cf = io::Input::open(&canonical_path, canonical_pin.bytes as u64)?;
    let mut sf = io::Input::open(&c.local_sq8.path, c.local_sq8.bytes)?;
    let mut of = io::Input::open(&c.order.path, c.order.bytes)?;
    let (mut ch, mut sh, mut oh, mut ih) =
        (Sha256::new(), Sha256::new(), Sha256::new(), Sha256::new());
    for page in 0..n.div_ceil(32) {
        let count = (n - page * 32).min(32);
        let bytes = &mut sq8[..mul(count, sq8_width)?];
        sf.file.read_exact(bytes)?;
        proof
            .verify_payload(page, page, bytes)
            .map_err(|e| format!("SQ8 original page {page}: {e:?}"))?;
        sh.update(&*bytes);
        for row in bytes.chunks_exact(sq8_width) {
            cf.file.read_exact(&mut canonical)?;
            ch.update(&canonical);
            ids.push(same_id(&canonical, row)?);
            ih.update(&canonical[..8]);
            let mut raw = [0; 8];
            of.file.read_exact(&mut raw)?;
            oh.update(raw);
            mark_order(&mut seen, u64::from_le_bytes(raw), n)?;
        }
    }
    cf.end(&canonical_path)?;
    sf.end(&c.local_sq8.path)?;
    of.end(&c.order.path)?;
    ensure(
        format!("{:x}", ch.finalize()) == canonical_pin.sha256,
        "full canonical SHA",
    )?;
    ensure(
        format!("{:x}", sh.finalize()) == c.local_sq8.sha256,
        "full SQ8 SHA",
    )?;
    let order_sha = format!("{:x}", oh.finalize());
    ensure(
        order_sha == c.order.sha256 && order_sha == a.plane.source_order_sha256,
        "physical-to-raw order digest",
    )?;
    ensure(ids.len() == n, "ID count")?;
    ids.sort_unstable();
    ensure(
        ids.windows(2).all(|pair| pair[0] != pair[1]),
        "duplicate application ID",
    )?;
    Ok(IdReceipt {
        rows: n,
        ordered_application_id_sha256: format!("{:x}", ih.finalize()),
        physical_to_raw_order_sha256: order_sha,
        unique_application_ids: true,
    })
}

fn router(a: &Authority, root: &Path, cap: usize) -> Result<SemanticUnitRouter> {
    let b = a.asset("router/root.bin")?;
    let m = a.asset("router/membership.bin")?;
    io::admit(
        cap,
        &[CONTROL, b.bytes, m.bytes, a.profile.allocation_cap()],
    )?;
    let body = a.read_asset(root, &b.path)?;
    let membership = a.read_asset(root, &m.path)?;
    Ok(SemanticUnitRouter::open(
        &body,
        &membership,
        &b.sha256,
        &input(a)?,
        a.profile.root_cap(),
    )?)
}

#[derive(Serialize)]
pub struct CapsuleReceipt {
    pub directory_magic: &'static str,
    pub capsule_magic: &'static str,
    pub directory_bytes: usize,
    pub directory_sha256: String,
    pub blob_bytes: u64,
    pub blob_sha256: String,
    pub leaves_fully_verified: usize,
    pub all_rosters_equal_router: bool,
}
pub fn capsules(a: &Authority, c: &Config, capsule_root: &Value) -> Result<CapsuleReceipt> {
    let SourceLayout::LeafCapsulesV2 {
        blob_bytes,
        blob_sha256,
        directory_bytes,
        directory_sha256,
    } = serde_json::from_value::<SourceLayout>(capsule_root["source_layout"].clone())?
    else {
        return Err("native v2 capsule layout required".into());
    };
    let geometry = Geometry::new(a.plane.rows, a.plane.dimensions, a.profile)?;
    let root_bytes = a.asset("router/root.bin")?.bytes;
    let record = add(64, mul(a.plane.dimensions, 4)?)?;
    ensure(
        root_bytes >= 512 && (root_bytes - 512) % record == 0,
        "router descriptor geometry",
    )?;
    let leaves = (root_bytes - 512) / record;
    let roster_start = add(32, mul(leaves, 64)?)?;
    let roster_bytes = directory_bytes
        .checked_sub(roster_start)
        .ok_or("directory descriptor length")?;
    ensure(roster_bytes % 4 == 0, "directory roster alignment")?;
    let charge = Directory::open_charge(geometry, leaves, roster_bytes / 4)?;
    ensure(
        charge.encoded_directory_bytes == directory_bytes,
        "directory length model",
    )?;
    // Private router vector capacities are charged conservatively with the entire
    // native construction allocation cap, rather than treating borrowed pins as free.
    let router_pin = a.profile.allocation_cap();
    let side_bytes = a.asset("plane/page_digests.bin")?.bytes;
    io::admit(
        c.cap_bytes,
        &[
            CONTROL,
            router_pin,
            charge.memory_bytes,
            mul(side_bytes, 3)?,
        ],
    )?;
    let router = router(a, &c.fresh_caps, c.cap_bytes)?;
    ensure(
        router.manifest().leaves.len() == leaves,
        "directory/router leaf count",
    )?;
    let proof = page_authority(a, &c.fresh_caps, 2, c.cap_bytes)?;
    let proof_pin = add(
        proof.proof_payload_bytes().ok_or("SOURCE proof capacity")?,
        std::mem::size_of::<PageAuthority>() + 64,
    )?;
    let available = c
        .cap_bytes
        .checked_sub(add(add(CONTROL, router_pin)?, proof_pin)?)
        .ok_or("capsule remaining memory")?;
    io::admit(available, &[charge.memory_bytes])?;
    let dir_path = c.fresh_caps.join("plane/capsule_directory.bin");
    let encoded = io::read(&dir_path, directory_bytes, &directory_sha256)?;
    let dir = Directory::open(
        &encoded,
        &io::digest(&directory_sha256)?,
        geometry,
        usize::try_from(blob_bytes)?,
        2,
        available,
    )?;
    io::admit(
        available,
        &[
            encoded.capacity(),
            dir.decoded_bytes()?,
            charge.scratch_bytes,
        ],
    )?;
    ensure(encoded.starts_with(b"BORSDR02"), "directory format2 marker")?;
    drop(encoded);
    let roster_cap = available
        .checked_sub(dir.decoded_bytes()?)
        .ok_or("roster remaining memory")?;
    for item in dir.entries() {
        let expected = leaf_pages(&router, item.leaf_id as usize, roster_cap)?;
        io::admit(roster_cap, &[mul(expected.capacity(), 4)?])?;
        ensure(
            expected.as_slice() == dir.pages(item.leaf_id)?,
            "directory roster != router-derived closure",
        )?;
    }
    let blob_path = c.fresh_caps.join("plane/capsules.bin");
    let mut blob = io::Input::open(&blob_path, blob_bytes)?;
    let mut h = Sha256::new();
    let mut offset = 0u64;
    for item in dir.entries() {
        ensure(item.offset == offset, "noncontiguous capsule blob")?;
        let leaf = LeafPages {
            leaf_id: item.leaf_id,
            pages: dir.pages(item.leaf_id)?,
        };
        let leaf_charge = dir.query_charge(&[leaf])?;
        io::admit(available, &[leaf_charge.memory_bytes])?;
        let mut body = io::vector(usize::try_from(item.total_bytes)?)?;
        ensure(
            body.capacity() == leaf_charge.buffer_bytes,
            "whole-leaf buffer capacity",
        )?;
        blob.file.read_exact(&mut body)?;
        ensure(body.starts_with(b"BORSCP02"), "capsule format2 marker")?;
        h.update(&body);
        let rows = dir.verify(
            &[leaf],
            &[FetchedSpan {
                offset,
                bytes: &body,
            }],
            &proof,
            available,
        )?;
        ensure(
            rows.charge().memory_bytes <= available,
            "actual whole-leaf verifier capacities",
        )?;
        // No row lookup/query is needed: verify authenticated the full capsule,
        // including every duplicate occurrence against original 32-row authority.
        drop(rows);
        offset = offset
            .checked_add(item.total_bytes)
            .ok_or("blob offset overflow")?;
    }
    blob.end(&blob_path)?;
    ensure(
        offset == blob_bytes && format!("{:x}", h.finalize()) == blob_sha256,
        "full physical capsule blob SHA/length",
    )?;
    io::stream(&dir_path, directory_bytes as u64, &directory_sha256, None)?;
    Ok(CapsuleReceipt {
        directory_magic: "BORSDR02",
        capsule_magic: "BORSCP02",
        directory_bytes,
        directory_sha256,
        blob_bytes,
        blob_sha256,
        leaves_fully_verified: leaves,
        all_rosters_equal_router: true,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn ids_are_application_ids_and_order_is_a_permutation() {
        let id = (-19i64).to_le_bytes();
        assert_eq!(same_id(&id, &id).unwrap(), -19);
        assert!(same_id(&id, &20i64.to_le_bytes()).is_err());
        let mut seen = [0; 1];
        mark_order(&mut seen, 2, 3).unwrap();
        mark_order(&mut seen, 0, 3).unwrap();
        mark_order(&mut seen, 1, 3).unwrap();
        assert!(mark_order(&mut seen, 2, 3).is_err());
        assert!(mark_order(&mut seen, 3, 3).is_err());
    }
}
