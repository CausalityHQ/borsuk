//! Streaming, generation-bound SOURCE capsules; no query planner or scoring order.
//!
//! Version 1 uses little-endian integers. Both headers are 32 bytes: magic[8],
//! N:u32@8, D:u16@12, codec width:u16@14, profile:u32@16, leaf count:u32@20.
//! Directory magic is BORSDR01, followed by unit rows (32)@24 and page rows
//! (256)@28. Capsule magic is BORSCP01, followed by leaf ID@24 and page count@28.
//! A capsule then stores sorted unique u32 page IDs and their complete raw pages,
//! including only the true final tail. Directory entries are 64 bytes:
//! leaf:u32, count:u32, offset:u64, total:u64, payload:u64, SHA256[32].
//! Entries and concatenated capsules are in leaf-ID order without gaps.
//!
//! The caller jointly pins the directory hash, geometry/profile and original
//! SOURCE authority in its generation. Capsule SHA covers framing AND payload;
//! logical flat-record SHA and physical blob SHA are separate identities.
//! Transport, ordered errors/drain, SOURCE walks and SQ8 gap scoring remain with
//! the existing fetch/planner layer. Byte ranges here refer to the capsule blob,
//! never to a fabricated PageAuthority. No capsule-order scoring iterator exists.
//!
//! Charges model payload capacities, descriptors and fixed working storage, not
//! allocator overhead, RSS, page cache, transport buffers or the caller's existing
//! authority/codec/router. Borrowed roster slices charge their lengths; callers
//! separately account for any unused backing capacity. Encoded directories are
//! streamed on build and retained by the caller during open, not stored here.
//! Memory admission uses the smaller of the caller cap and profile allocation cap.
//! Physical/hash byte charges are exposed for the caller's I/O and work budgets;
//! they are not memory charges. Leaf closures must be supplied in leaf-ID order.
//! Publication/fsync is caller-owned: Write::flush does not provide durability.
use crate::rotated_two_bit::RotatedTwoBitCodec;
use crate::semantic_unit_router::{SemanticProfile, SemanticUnitRouter};
use crate::sq8_page_authority::PageAuthority;
use crate::two_bit_source::SourceBuildError;
use sha2::{Digest, Sha256};
use std::io::{Read, Seek, SeekFrom, Write};
use std::mem::size_of;
use std::ops::Range;

/// Capsule errors reuse the SOURCE build boundary.
pub type Result<T> = std::result::Result<T, SourceBuildError>;
const HEADER: usize = 32;
const ENTRY: usize = 64;
const UNIT_ROWS: usize = 32;
const PAGE_ROWS: usize = 256;
const SCRATCH: usize = 3 * size_of::<Sha256>() + 128;
const SPAN_DESCRIPTOR_BYTES: usize = size_of::<FetchedSpan<'_>>() + size_of::<Vec<u8>>();

fn require(ok: bool, reason: &'static str) -> Result<()> {
    if ok {
        Ok(())
    } else {
        Err(SourceBuildError::Invalid(reason))
    }
}
fn add(a: usize, b: usize) -> Result<usize> {
    a.checked_add(b)
        .ok_or(SourceBuildError::Invalid("capsule arithmetic"))
}
fn mul(a: usize, b: usize) -> Result<usize> {
    a.checked_mul(b)
        .ok_or(SourceBuildError::Invalid("capsule arithmetic"))
}
fn number(value: u64) -> Result<usize> {
    usize::try_from(value).map_err(|_| SourceBuildError::Invalid("capsule integer"))
}
fn admit(bytes: usize, cap: usize) -> Result<()> {
    require(bytes <= cap, "capsule memory cap")
}
fn reserve<T>(count: usize) -> Result<Vec<T>> {
    let mut result = Vec::new();
    result
        .try_reserve_exact(count)
        .map_err(|_| SourceBuildError::Invalid("capsule allocation"))?;
    Ok(result)
}
fn hash(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
}
fn profile_code(profile: SemanticProfile) -> u32 {
    match profile {
        SemanticProfile::Native100k => 1,
        SemanticProfile::Fresh1m => 2,
        SemanticProfile::Scale1m => 3,
    }
}

/// Explicit source geometry. Generic profiles support D1..=1024.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Geometry {
    rows: usize,
    dimensions: usize,
    width: usize,
    profile: SemanticProfile,
}
impl Geometry {
    /// Derive record width from the existing codec's block padding.
    pub fn new(rows: usize, dimensions: usize, profile: SemanticProfile) -> Result<Self> {
        require(
            profile.valid_geometry(rows, dimensions),
            "capsule geometry/profile",
        )?;
        let width = add(
            RotatedTwoBitCodec::padded_dimensions(dimensions)?.div_ceil(4),
            8,
        )?;
        mul(rows, width)?;
        Ok(Self {
            rows,
            dimensions,
            width,
            profile,
        })
    }
    /// Original physical row count.
    pub fn rows(self) -> usize {
        self.rows
    }
    /// Original coordinate count.
    pub fn dimensions(self) -> usize {
        self.dimensions
    }
    /// Raw encoded bytes per row.
    pub fn record_bytes(self) -> usize {
        self.width
    }
    /// Explicit admission profile.
    pub fn profile(self) -> SemanticProfile {
        self.profile
    }
    fn pages(self) -> usize {
        self.rows.div_ceil(PAGE_ROWS)
    }
    fn page_bytes(self, page: u32) -> Result<usize> {
        let page = page as usize;
        require(page < self.pages(), "capsule page ID")?;
        mul(
            (self.rows - mul(page, PAGE_ROWS)?).min(PAGE_ROWS),
            self.width,
        )
    }
    fn authority(self, authority: &PageAuthority) -> Result<()> {
        require(
            !authority.is_sq8()
                && authority.rows() == self.rows
                && authority.dimensions() == self.dimensions
                && authority.page_rows() == UNIT_ROWS
                && authority.object_bytes() == mul(self.rows, self.width)?,
            "capsule original SOURCE authority",
        )
    }
    fn verify_page(self, page: u32, bytes: &[u8], authority: &PageAuthority) -> Result<()> {
        require(bytes.len() == self.page_bytes(page)?, "capsule page tail")?;
        let first = mul(page as usize, PAGE_ROWS / UNIT_ROWS)?;
        let last = add(first, bytes.len().div_ceil(self.width * UNIT_ROWS))? - 1;
        authority
            .verify_payload(first, last, bytes)
            .map_err(|_| SourceBuildError::Invalid("capsule original unit digest"))
    }
    fn header(self, magic: &[u8; 8], leaves: usize, a: u32, b: u32) -> [u8; HEADER] {
        let mut bytes = [0; HEADER];
        bytes[..8].copy_from_slice(magic);
        bytes[8..12].copy_from_slice(&(self.rows as u32).to_le_bytes());
        bytes[12..14].copy_from_slice(&(self.dimensions as u16).to_le_bytes());
        bytes[14..16].copy_from_slice(&(self.width as u16).to_le_bytes());
        bytes[16..20].copy_from_slice(&profile_code(self.profile).to_le_bytes());
        bytes[20..24].copy_from_slice(&(leaves as u32).to_le_bytes());
        bytes[24..28].copy_from_slice(&a.to_le_bytes());
        bytes[28..32].copy_from_slice(&b.to_le_bytes());
        bytes
    }
}

/// Trusted expected leaf closure, normally derived from authenticated membership.
#[derive(Clone, Copy, Debug)]
pub struct LeafPages<'a> {
    /// Original semantic leaf ID.
    pub leaf_id: u32,
    /// Strictly increasing physical page IDs, each floor(original unit ID / 8).
    pub pages: &'a [u32],
}

/// Derive a leaf's sorted, unique physical pages from the existing router.
/// The result's actual allocation capacity is checked against the cap.
pub fn leaf_pages(router: &SemanticUnitRouter, leaf_id: usize, cap: usize) -> Result<Vec<u32>> {
    require(leaf_id < router.manifest().leaves.len(), "capsule leaf ID")?;
    let cap = cap.min(router.manifest().profile.allocation_cap());
    let membership = router.unit_membership();
    let count = membership
        .chunks(PAGE_ROWS / UNIT_ROWS)
        .filter(|units| units.contains(&leaf_id))
        .count();
    require(count > 0, "capsule empty leaf")?;
    admit(mul(count, size_of::<u32>())?, cap)?;
    let mut pages = reserve(count)?;
    admit(mul(pages.capacity(), size_of::<u32>())?, cap)?;
    for (page, units) in membership.chunks(PAGE_ROWS / UNIT_ROWS).enumerate() {
        if units.contains(&leaf_id) {
            pages.push(page as u32);
        }
    }
    Ok(pages)
}

/// Exact wire counts and modeled in-process payload capacities for one operation.
#[allow(missing_docs)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Charge {
    pub source_bytes: usize,
    pub physical_bytes: usize,
    pub logical_bytes: usize,
    pub duplicate_bytes: usize,
    pub framing_bytes: usize,
    pub hash_bytes: usize,
    pub encoded_directory_bytes: usize,
    pub decoded_directory_bytes: usize,
    pub roster_bytes: usize,
    pub buffer_bytes: usize,
    pub lookup_bytes: usize,
    pub scratch_bytes: usize,
    pub memory_bytes: usize,
}
impl Charge {
    fn memory(&mut self, encoded_resident: bool) -> Result<()> {
        self.memory_bytes = [
            self.decoded_directory_bytes,
            self.roster_bytes,
            self.buffer_bytes,
            self.lookup_bytes,
            self.scratch_bytes,
            if encoded_resident {
                self.encoded_directory_bytes
            } else {
                0
            },
        ]
        .into_iter()
        .try_fold(0, add)?;
        Ok(())
    }
}

/// Authenticated range in the physical capsule blob.
#[allow(missing_docs)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DirectoryEntry {
    pub leaf_id: u32,
    pub page_count: u32,
    pub offset: u64,
    pub total_bytes: u64,
    pub payload_bytes: u64,
    pub sha256: [u8; 32],
}
impl DirectoryEntry {
    fn encode(self) -> [u8; ENTRY] {
        let mut bytes = [0; ENTRY];
        bytes[..4].copy_from_slice(&self.leaf_id.to_le_bytes());
        bytes[4..8].copy_from_slice(&self.page_count.to_le_bytes());
        bytes[8..16].copy_from_slice(&self.offset.to_le_bytes());
        bytes[16..24].copy_from_slice(&self.total_bytes.to_le_bytes());
        bytes[24..32].copy_from_slice(&self.payload_bytes.to_le_bytes());
        bytes[32..].copy_from_slice(&self.sha256);
        bytes
    }
}
fn payload(geometry: Geometry, leaf: LeafPages<'_>) -> Result<usize> {
    require(
        !leaf.pages.is_empty() && leaf.pages.len() <= geometry.pages(),
        "capsule page count",
    )?;
    let mut total = 0;
    let mut previous = None;
    for &page in leaf.pages {
        require(previous.is_none_or(|p| p < page), "capsule page order")?;
        total = add(total, geometry.page_bytes(page)?)?;
        previous = Some(page);
    }
    Ok(total)
}
fn entry(geometry: Geometry, leaf: LeafPages<'_>, offset: usize) -> Result<DirectoryEntry> {
    let payload_bytes = payload(geometry, leaf)?;
    let total_bytes = add(add(HEADER, mul(leaf.pages.len(), 4)?)?, payload_bytes)?;
    add(offset, total_bytes)?;
    Ok(DirectoryEntry {
        leaf_id: leaf.leaf_id,
        page_count: leaf.pages.len() as u32,
        offset: offset as u64,
        total_bytes: total_bytes as u64,
        payload_bytes: payload_bytes as u64,
        sha256: [0; 32],
    })
}
fn closure_charge(geometry: Geometry, leaves: &[LeafPages<'_>]) -> Result<Charge> {
    require(
        !leaves.is_empty() && leaves.len() <= geometry.rows.div_ceil(UNIT_ROWS),
        "capsule leaf count",
    )?;
    let mut result = Charge {
        source_bytes: mul(geometry.rows, geometry.width)?,
        roster_bytes: mul(leaves.len(), size_of::<LeafPages<'_>>())?,
        scratch_bytes: SCRATCH,
        ..Charge::default()
    };
    let mut previous = None;
    let mut payload_bytes = 0;
    for &leaf in leaves {
        require(
            previous.is_none_or(|p| p < leaf.leaf_id),
            "capsule leaf order",
        )?;
        let item = entry(geometry, leaf, result.physical_bytes)?;
        result.physical_bytes = add(result.physical_bytes, number(item.total_bytes)?)?;
        payload_bytes = add(payload_bytes, number(item.payload_bytes)?)?;
        result.roster_bytes = add(result.roster_bytes, mul(leaf.pages.len(), 4)?)?;
        previous = Some(leaf.leaf_id);
    }
    // Bound union accounting without an N-row allocation or hydrated dedup plane.
    for page in 0..geometry.pages() {
        if leaves
            .iter()
            .any(|leaf| leaf.pages.binary_search(&(page as u32)).is_ok())
        {
            result.logical_bytes = add(result.logical_bytes, geometry.page_bytes(page as u32)?)?;
        }
    }
    result.duplicate_bytes = payload_bytes - result.logical_bytes;
    result.framing_bytes = result.physical_bytes - payload_bytes;
    Ok(result)
}

/// Decoded directory. Its pinned hash and the original authority must share a generation.
#[derive(Debug)]
pub struct Directory {
    geometry: Geometry,
    entries: Vec<DirectoryEntry>,
    blob_bytes: usize,
}
impl Directory {
    /// Admission model available before fetching an encoded directory.
    pub fn open_charge(geometry: Geometry, leaf_count: usize) -> Result<Charge> {
        require(
            leaf_count > 0 && leaf_count <= geometry.rows.div_ceil(UNIT_ROWS),
            "capsule leaf count",
        )?;
        let mut charge = Charge {
            source_bytes: mul(geometry.rows, geometry.width)?,
            encoded_directory_bytes: add(HEADER, mul(leaf_count, ENTRY)?)?,
            decoded_directory_bytes: add(
                size_of::<Self>(),
                mul(leaf_count, size_of::<DirectoryEntry>())?,
            )?,
            scratch_bytes: SCRATCH,
            ..Charge::default()
        };
        charge.hash_bytes = charge.encoded_directory_bytes;
        charge.memory(true)?;
        Ok(charge)
    }
    /// Authenticate and parse an exact directory, rejecting gaps, overlaps and trailing bytes.
    pub fn open(
        encoded: &[u8],
        expected_sha256: &[u8; 32],
        geometry: Geometry,
        blob_bytes: usize,
        cap: usize,
    ) -> Result<Self> {
        let cap = cap.min(geometry.profile.allocation_cap());
        require(encoded.len() >= HEADER, "capsule directory header")?;
        let count = u32::from_le_bytes(encoded[20..24].try_into().unwrap()) as usize;
        let mut charge = Self::open_charge(geometry, count)?;
        admit(charge.memory_bytes, cap)?;
        require(
            encoded.len() == charge.encoded_directory_bytes
                && encoded[..HEADER]
                    == geometry.header(b"BORSDR01", count, UNIT_ROWS as u32, PAGE_ROWS as u32)
                && hash(encoded) == *expected_sha256,
            "capsule directory identity",
        )?;
        let mut entries = reserve(count)?;
        charge.decoded_directory_bytes = add(
            size_of::<Self>(),
            mul(entries.capacity(), size_of::<DirectoryEntry>())?,
        )?;
        charge.memory(true)?;
        admit(charge.memory_bytes, cap)?;
        let mut offset = 0;
        for (id, bytes) in encoded[HEADER..].chunks_exact(ENTRY).enumerate() {
            let item = DirectoryEntry {
                leaf_id: u32::from_le_bytes(bytes[..4].try_into().unwrap()),
                page_count: u32::from_le_bytes(bytes[4..8].try_into().unwrap()),
                offset: u64::from_le_bytes(bytes[8..16].try_into().unwrap()),
                total_bytes: u64::from_le_bytes(bytes[16..24].try_into().unwrap()),
                payload_bytes: u64::from_le_bytes(bytes[24..32].try_into().unwrap()),
                sha256: bytes[32..].try_into().unwrap(),
            };
            let pages = item.page_count as usize;
            require(
                item.leaf_id as usize == id && pages > 0 && pages <= geometry.pages(),
                "capsule directory leaf/pages",
            )?;
            let full = mul(mul(pages, PAGE_ROWS)?, geometry.width)?;
            let tail = add(
                mul(mul(pages - 1, PAGE_ROWS)?, geometry.width)?,
                geometry.page_bytes((geometry.pages() - 1) as u32)?,
            )?;
            let bytes = number(item.payload_bytes)?;
            require(
                (bytes == tail || (pages < geometry.pages() && bytes == full))
                    && number(item.offset)? == offset
                    && number(item.total_bytes)? == add(add(HEADER, mul(pages, 4)?)?, bytes)?,
                "capsule directory layout",
            )?;
            offset = add(offset, number(item.total_bytes)?)?;
            entries.push(item);
        }
        require(offset == blob_bytes, "capsule blob length")?;
        Ok(Self {
            geometry,
            entries,
            blob_bytes,
        })
    }
    /// Authenticated entries in original leaf-ID order.
    pub fn entries(&self) -> &[DirectoryEntry] {
        &self.entries
    }
    /// Actual decoded-directory allocation, including the directory value.
    pub fn decoded_bytes(&self) -> Result<usize> {
        add(
            size_of::<Self>(),
            mul(self.entries.capacity(), size_of::<DirectoryEntry>())?,
        )
    }
    /// True half-open byte range in the concatenated capsule blob.
    pub fn byte_range(&self, leaf_id: u32) -> Result<Range<u64>> {
        let item = self
            .entries
            .get(leaf_id as usize)
            .ok_or(SourceBuildError::Invalid("capsule leaf ID"))?;
        Ok(item.offset
            ..item
                .offset
                .checked_add(item.total_bytes)
                .ok_or(SourceBuildError::Invalid("capsule range overflow"))?)
    }
    /// Total physical blob length.
    pub fn blob_bytes(&self) -> usize {
        self.blob_bytes
    }
    /// Stream the directory; the returned SHA is suitable for generation pinning.
    /// Success only flushes the writer, and is not a durable publication.
    pub fn write_to<W: Write>(&self, writer: &mut W) -> Result<[u8; 32]> {
        let header = self.geometry.header(
            b"BORSDR01",
            self.entries.len(),
            UNIT_ROWS as u32,
            PAGE_ROWS as u32,
        );
        let mut digest = Sha256::new();
        writer.write_all(&header)?;
        digest.update(header);
        for item in &self.entries {
            let bytes = item.encode();
            writer.write_all(&bytes)?;
            digest.update(bytes);
        }
        writer.flush()?;
        Ok(digest.finalize().into())
    }
    /// Conservative pre-fetch admission: one exact-capacity response per capsule.
    /// verify charges actual response count/capacities, including coalesced spans.
    pub fn query_charge(&self, leaves: &[LeafPages<'_>]) -> Result<Charge> {
        let mut charge = closure_charge(self.geometry, leaves)?;
        let mut occurrences = 0;
        for &leaf in leaves {
            let expected = entry(self.geometry, leaf, 0)?;
            let stored = self
                .entries
                .get(leaf.leaf_id as usize)
                .ok_or(SourceBuildError::Invalid("capsule leaf ID"))?;
            require(
                expected.page_count == stored.page_count
                    && expected.total_bytes == stored.total_bytes
                    && expected.payload_bytes == stored.payload_bytes,
                "capsule expected closure geometry",
            )?;
            occurrences = add(occurrences, leaf.pages.len())?;
        }
        charge.encoded_directory_bytes = add(HEADER, mul(self.entries.len(), ENTRY)?)?;
        charge.decoded_directory_bytes = self.decoded_bytes()?;
        charge.buffer_bytes = charge.physical_bytes;
        charge.lookup_bytes = add(
            mul(occurrences, size_of::<PageRef<'_>>())?,
            add(
                size_of::<VerifiedRows<'_>>(),
                mul(leaves.len(), SPAN_DESCRIPTOR_BYTES)?,
            )?,
        )?;
        charge.hash_bytes = add(
            charge.physical_bytes,
            charge.physical_bytes - charge.framing_bytes,
        )?;
        charge.memory(false)?;
        Ok(charge)
    }
}
/// Pre-I/O build model, including original flat validation and all duplicate copies.
pub fn build_charge(geometry: Geometry, leaves: &[LeafPages<'_>]) -> Result<Charge> {
    let mut charge = closure_charge(geometry, leaves)?;
    for (id, leaf) in leaves.iter().enumerate() {
        require(leaf.leaf_id as usize == id, "capsule build leaf IDs")?;
    }
    let directory = Directory::open_charge(geometry, leaves.len())?;
    charge.encoded_directory_bytes = directory.encoded_directory_bytes;
    charge.decoded_directory_bytes = directory.decoded_directory_bytes;
    charge.buffer_bytes = mul(geometry.rows.min(PAGE_ROWS), geometry.width)?;
    charge.hash_bytes = add(
        add(mul(charge.source_bytes, 2)?, mul(charge.physical_bytes, 2)?)?,
        add(
            charge.physical_bytes - charge.framing_bytes,
            charge.encoded_directory_bytes,
        )?,
    )?;
    charge.memory(false)?;
    Ok(charge)
}

/// Completed streaming output; still unpublished and not necessarily durable.
#[allow(missing_docs)]
#[derive(Debug)]
pub struct BuildReceipt {
    pub directory: Directory,
    pub logical_flat_sha256: [u8; 32],
    pub physical_blob_sha256: [u8; 32],
    pub directory_sha256: [u8; 32],
    pub charge: Charge,
}
fn write_part<W: Write>(
    writer: &mut W,
    bytes: &[u8],
    capsule: &mut Sha256,
    blob: &mut Sha256,
) -> Result<()> {
    writer.write_all(bytes)?;
    capsule.update(bytes);
    blob.update(bytes);
    Ok(())
}

/// Validate the complete immutable original, then copy authenticated pages using
/// one page buffer. Output writers must start at offset zero in new artifacts.
/// Any error leaves unpublished partial output for caller cleanup.
pub fn pack<R: Read + Seek, W: Write, D: Write>(
    geometry: Geometry,
    leaves: &[LeafPages<'_>],
    authority: &PageAuthority,
    records: &mut R,
    blob: &mut W,
    directory_writer: &mut D,
    cap: usize,
) -> Result<BuildReceipt> {
    let cap = cap.min(geometry.profile.allocation_cap());
    let mut charge = build_charge(geometry, leaves)?;
    admit(charge.memory_bytes, cap)?;
    geometry.authority(authority)?;
    let mut entries = reserve(leaves.len())?;
    let mut buffer = reserve(charge.buffer_bytes)?;
    buffer.resize(charge.buffer_bytes, 0u8);
    charge.buffer_bytes = buffer.capacity();
    charge.decoded_directory_bytes = add(
        size_of::<Directory>(),
        mul(entries.capacity(), size_of::<DirectoryEntry>())?,
    )?;
    charge.memory(false)?;
    admit(charge.memory_bytes, cap)?;
    // Refuse before any output if either length, flat SHA or original units fail.
    require(
        records.seek(SeekFrom::End(0))? == charge.source_bytes as u64,
        "capsule flat length",
    )?;
    records.seek(SeekFrom::Start(0))?;
    let mut flat_digest = Sha256::new();
    for page in 0..geometry.pages() {
        let bytes = &mut buffer[..geometry.page_bytes(page as u32)?];
        records.read_exact(bytes)?;
        geometry.verify_page(page as u32, bytes, authority)?;
        flat_digest.update(&*bytes);
    }
    require(records.read(&mut [0])? == 0, "capsule flat trailing bytes")?;
    let flat_hash = flat_digest.finalize();
    require(
        format!("{flat_hash:x}") == authority.object_sha256(),
        "capsule flat SHA",
    )?;
    let logical_flat_sha256: [u8; 32] = flat_hash.into();
    let mut blob_digest = Sha256::new();
    let mut offset = 0;
    for &leaf in leaves {
        let mut item = entry(geometry, leaf, offset)?;
        let mut capsule_digest = Sha256::new();
        let header = geometry.header(b"BORSCP01", leaves.len(), leaf.leaf_id, item.page_count);
        write_part(blob, &header, &mut capsule_digest, &mut blob_digest)?;
        for page in leaf.pages {
            write_part(
                blob,
                &page.to_le_bytes(),
                &mut capsule_digest,
                &mut blob_digest,
            )?;
        }
        for &page in leaf.pages {
            let bytes = &mut buffer[..geometry.page_bytes(page)?];
            records.seek(SeekFrom::Start(
                mul(mul(page as usize, PAGE_ROWS)?, geometry.width)? as u64,
            ))?;
            records.read_exact(bytes)?;
            geometry.verify_page(page, bytes, authority)?;
            write_part(blob, bytes, &mut capsule_digest, &mut blob_digest)?;
        }
        item.sha256 = capsule_digest.finalize().into();
        offset = add(offset, number(item.total_bytes)?)?;
        entries.push(item);
    }
    blob.flush()?;
    let directory = Directory {
        geometry,
        entries,
        blob_bytes: offset,
    };
    let directory_sha256 = directory.write_to(directory_writer)?;
    Ok(BuildReceipt {
        directory,
        logical_flat_sha256,
        physical_blob_sha256: blob_digest.finalize().into(),
        directory_sha256,
        charge,
    })
}

#[derive(Debug)]
struct PageRef<'a> {
    page: u32,
    leaf: u32,
    bytes: &'a [u8],
}
/// Verified references retain the caller's buffers; only original physical IDs address rows.
#[derive(Debug)]
pub struct VerifiedRows<'a> {
    geometry: Geometry,
    pages: Vec<PageRef<'a>>,
    charge: Charge,
}
impl VerifiedRows<'_> {
    /// Borrow an exact original encoded row; absent/gap/out-of-range rows return None.
    pub fn record(&self, physical_row: usize) -> Option<&[u8]> {
        if physical_row >= self.geometry.rows {
            return None;
        }
        let page = (physical_row / PAGE_ROWS) as u32;
        let index = self
            .pages
            .binary_search_by_key(&page, |item| item.page)
            .ok()?;
        let offset = (physical_row % PAGE_ROWS) * self.geometry.width;
        self.pages[index]
            .bytes
            .get(offset..offset + self.geometry.width)
    }
    /// Selected copy's leaf ID, useful for auditing stable duplicate selection.
    pub fn page_leaf(&self, page: u32) -> Option<u32> {
        self.pages
            .binary_search_by_key(&page, |item| item.page)
            .ok()
            .map(|i| self.pages[i].leaf)
    }
    /// Actual retained capacities and complete verification hash work.
    pub fn charge(&self) -> Charge {
        self.charge
    }
}
/// One retained physical response, containing one or more adjacent selected
/// capsules. The Vec remains caller-owned; verification never clones or splits it.
#[derive(Clone, Copy, Debug)]
pub struct FetchedSpan<'a> {
    /// True starting byte offset in the capsule blob.
    pub offset: u64,
    /// Entire response body, borrowed so its actual capacity can be charged once.
    pub bytes: &'a Vec<u8>,
}
impl FetchedSpan<'_> {
    /// Checked physical half-open response range, independent of SOURCE unit offsets.
    pub fn byte_range(&self) -> Result<Range<u64>> {
        let end = self
            .offset
            .checked_add(self.bytes.len() as u64)
            .ok_or(SourceBuildError::Invalid("capsule response range overflow"))?;
        Ok(self.offset..end)
    }
}
impl Directory {
    /// Verify EVERY capsule copy against its closure and original unit authority
    /// before exposing rows. Responses may arrive in any order and may coalesce
    /// exactly adjacent selected capsules. Their ranges must be disjoint and
    /// contain only complete selected capsules, covering every selected leaf once.
    pub fn verify<'a>(
        &self,
        leaves: &[LeafPages<'_>],
        fetched: &[FetchedSpan<'a>],
        authority: &PageAuthority,
        cap: usize,
    ) -> Result<VerifiedRows<'a>> {
        let cap = cap.min(self.geometry.profile.allocation_cap());
        let mut charge = self.query_charge(leaves)?;
        self.geometry.authority(authority)?;
        require(
            !fetched.is_empty() && fetched.len() <= leaves.len(),
            "capsule response count",
        )?;
        let occurrences = leaves
            .iter()
            .try_fold(0, |count, leaf| add(count, leaf.pages.len()))?;
        let descriptors = add(
            size_of::<VerifiedRows<'_>>(),
            mul(fetched.len(), SPAN_DESCRIPTOR_BYTES)?,
        )?;
        charge.lookup_bytes = add(mul(occurrences, size_of::<PageRef<'_>>())?, descriptors)?;
        charge.memory(false)?;
        admit(charge.memory_bytes, cap)?;
        charge.buffer_bytes = 0;
        let mut physical_bytes = 0;
        for (index, span) in fetched.iter().enumerate() {
            let range = span.byte_range()?;
            require(
                !range.is_empty() && range.end <= self.blob_bytes as u64,
                "capsule response bounds",
            )?;
            for other in &fetched[..index] {
                let other = other.byte_range()?;
                require(
                    range.end <= other.start || other.end <= range.start,
                    "capsule overlapping responses",
                )?;
            }
            let mut next = self
                .entries
                .binary_search_by_key(&range.start, |item| item.offset)
                .map_err(|_| SourceBuildError::Invalid("capsule response start"))?;
            let mut end = range.start;
            while end < range.end {
                let item = self
                    .entries
                    .get(next)
                    .ok_or(SourceBuildError::Invalid("capsule response end"))?;
                require(
                    leaves
                        .binary_search_by_key(&item.leaf_id, |leaf| leaf.leaf_id)
                        .is_ok(),
                    "capsule unselected response leaf",
                )?;
                end = self.byte_range(item.leaf_id)?.end;
                require(end <= range.end, "capsule partial response leaf")?;
                next += 1;
            }
            charge.buffer_bytes = add(charge.buffer_bytes, span.bytes.capacity())?;
            physical_bytes = add(physical_bytes, span.bytes.len())?;
        }
        // Disjoint whole selected capsules plus exact byte count prove full coverage.
        require(
            physical_bytes == charge.physical_bytes,
            "capsule incomplete response coverage",
        )?;
        charge.memory(false)?;
        admit(charge.memory_bytes, cap)?;
        let mut pages = reserve(occurrences)?;
        charge.lookup_bytes = add(
            mul(pages.capacity(), size_of::<PageRef<'_>>())?,
            descriptors,
        )?;
        charge.memory(false)?;
        admit(charge.memory_bytes, cap)?;
        for &leaf in leaves {
            let range = self.byte_range(leaf.leaf_id)?;
            let span = fetched
                .iter()
                .find(|span| {
                    span.offset <= range.start
                        && span
                            .offset
                            .checked_add(span.bytes.len() as u64)
                            .is_some_and(|end| range.end <= end)
                })
                .ok_or(SourceBuildError::Invalid("capsule missing response leaf"))?;
            let buffer: &'a Vec<u8> = span.bytes;
            let bytes =
                &buffer[number(range.start - span.offset)?..number(range.end - span.offset)?];
            let item = &self.entries[leaf.leaf_id as usize];
            require(
                bytes.len() == number(item.total_bytes)? && hash(bytes) == item.sha256,
                "capsule fetched size/hash",
            )?;
            require(
                bytes[..HEADER]
                    == self.geometry.header(
                        b"BORSCP01",
                        self.entries.len(),
                        leaf.leaf_id,
                        item.page_count,
                    ),
                "capsule header",
            )?;
            let mut offset = add(HEADER, mul(leaf.pages.len(), 4)?)?;
            for (index, &page) in leaf.pages.iter().enumerate() {
                let start = HEADER + index * 4;
                require(
                    bytes[start..start + 4] == page.to_le_bytes(),
                    "capsule expected page closure",
                )?;
                let end = add(offset, self.geometry.page_bytes(page)?)?;
                let body = bytes
                    .get(offset..end)
                    .ok_or(SourceBuildError::Invalid("capsule truncated page"))?;
                self.geometry.verify_page(page, body, authority)?;
                pages.push(PageRef {
                    page,
                    leaf: leaf.leaf_id,
                    bytes: body,
                });
                offset = end;
            }
            require(offset == bytes.len(), "capsule trailing payload")?;
        }
        pages.sort_unstable_by_key(|item| (item.page, item.leaf));
        pages.dedup_by_key(|item| item.page);
        Ok(VerifiedRows {
            geometry: self.geometry,
            pages,
            charge,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::two_bit_source::SourcePlaneReceipt;
    use std::io::{self, Cursor};

    fn geometry(rows: usize, dimensions: usize) -> Geometry {
        Geometry::new(rows, dimensions, SemanticProfile::Scale1m).unwrap()
    }
    fn authority(g: Geometry, flat: &[u8], object_hash: [u8; 32]) -> PageAuthority {
        let sidecar: Vec<u8> = flat.chunks(UNIT_ROWS * g.width).flat_map(hash).collect();
        let receipt = SourcePlaneReceipt {
            schema: "borsuk-two-bit-plane-v3".into(),
            rows: g.rows,
            dimensions: g.dimensions,
            seed: 20260923,
            record_bytes: g.width,
            source_sha256: "0".repeat(64),
            sq8_sha256: "1".repeat(64),
            source_order_sha256: "2".repeat(64),
            mean_sha256: "3".repeat(64),
            records_sha256: object_hash.iter().map(|b| format!("{b:02x}")).collect(),
            page_rows: UNIT_ROWS,
            page_digest_sha256: format!("{:x}", Sha256::digest(&sidecar)),
            query_or_truth_used: false,
        };
        let manifest = serde_json::to_vec(&receipt).unwrap();
        PageAuthority::load_two_bit(
            &manifest,
            &format!("{:x}", Sha256::digest(&manifest)),
            7,
            &sidecar,
        )
        .unwrap()
    }
    fn source(g: Geometry) -> (Vec<u8>, PageAuthority) {
        let mut flat = vec![0; g.rows * g.width];
        for (row, bytes) in flat.chunks_exact_mut(g.width).enumerate() {
            for (i, value) in bytes[..g.width - 8].iter_mut().enumerate() {
                *value = (row.wrapping_mul(17).wrapping_add(row >> 8).wrapping_add(i)) as u8;
            }
            bytes[g.width - 8..g.width - 4].copy_from_slice(&1.0_f32.to_le_bytes());
            bytes[g.width - 4..].copy_from_slice(&1.0_f32.to_le_bytes());
        }
        let original = authority(g, &flat, hash(&flat));
        (flat, original)
    }
    fn build(
        g: Geometry,
        leaves: &[LeafPages<'_>],
        flat: &[u8],
        original: &PageAuthority,
    ) -> (BuildReceipt, Vec<u8>, Vec<u8>) {
        let mut blob = Vec::new();
        let mut encoded = Vec::new();
        let receipt = pack(
            g,
            leaves,
            original,
            &mut Cursor::new(flat),
            &mut blob,
            &mut encoded,
            usize::MAX,
        )
        .unwrap();
        (receipt, blob, encoded)
    }
    fn fetch(directory: &Directory, blob: &[u8]) -> Vec<(u32, Vec<u8>)> {
        directory
            .entries()
            .iter()
            .rev()
            .map(|entry| {
                let range = directory.byte_range(entry.leaf_id).unwrap();
                (
                    entry.leaf_id,
                    blob[range.start as usize..range.end as usize].to_vec(),
                )
            })
            .collect()
    }
    impl Directory {
        fn verify_copies<'a>(
            &self,
            leaves: &[LeafPages<'_>],
            fetched: &'a [(u32, Vec<u8>)],
            authority: &PageAuthority,
            cap: usize,
        ) -> Result<VerifiedRows<'a>> {
            let spans = fetched
                .iter()
                .map(|(leaf, bytes)| {
                    Ok(FetchedSpan {
                        offset: self.byte_range(*leaf)?.start,
                        bytes,
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            self.verify(leaves, &spans, authority, cap)
        }
    }
    fn rehash_copy(directory: &mut Directory, fetched: &[(u32, Vec<u8>)], leaf: u32) {
        directory.entries[leaf as usize].sha256 =
            hash(&fetched.iter().find(|(id, _)| *id == leaf).unwrap().1);
    }

    #[test]
    fn dimensions_tails_overlap_stable_copies_and_exact_records() {
        for dimensions in [1, 257, 1024] {
            for rows in [257, 513] {
                let g = geometry(rows, dimensions);
                let codec = RotatedTwoBitCodec::new(&vec![0.0; dimensions], 20260923).unwrap();
                assert_eq!(g.record_bytes(), codec.record_bytes());
                let pages: Vec<u32> = (0..g.pages() as u32).collect();
                let leaves = [
                    LeafPages {
                        leaf_id: 0,
                        pages: &pages,
                    },
                    LeafPages {
                        leaf_id: 1,
                        pages: &[0],
                    },
                ];
                let (flat, original) = source(g);
                let (receipt, blob, encoded) = build(g, &leaves, &flat, &original);
                assert_eq!(receipt.logical_flat_sha256, hash(&flat));
                assert_eq!(receipt.physical_blob_sha256, hash(&blob));
                assert_ne!(receipt.logical_flat_sha256, receipt.physical_blob_sha256);
                assert_eq!(receipt.directory_sha256, hash(&encoded));
                assert_eq!(receipt.charge.physical_bytes, blob.len());
                assert_eq!(receipt.charge.logical_bytes, flat.len());
                assert_eq!(receipt.charge.duplicate_bytes, 256 * g.width);
                assert_eq!(receipt.charge.buffer_bytes, 256 * g.width);
                let directory = Directory::open(
                    &encoded,
                    &receipt.directory_sha256,
                    g,
                    blob.len(),
                    usize::MAX,
                )
                .unwrap();
                let fetched = fetch(&directory, &blob);
                let verified = directory
                    .verify_copies(&leaves, &fetched, &original, usize::MAX)
                    .unwrap();
                assert_eq!(verified.page_leaf(0), Some(0));
                for row in 0..rows {
                    assert_eq!(
                        verified.record(row),
                        Some(&flat[row * g.width..(row + 1) * g.width])
                    );
                }
                let first = &fetched.iter().find(|(id, _)| *id == 0).unwrap().1;
                assert_eq!(
                    verified.record(0).unwrap().as_ptr(),
                    first[HEADER + pages.len() * 4..].as_ptr()
                );
                assert!(verified.record(rows).is_none());
                assert!(verified.record(usize::MAX).is_none());
                assert_eq!(verified.charge().duplicate_bytes, 256 * g.width);
            }
        }
    }

    #[test]
    fn distinct_full_page_substitution_rejected_after_rehashing_capsule_and_directory() {
        for dimensions in [1, 257, 1024] {
            let g = geometry(513, dimensions);
            let leaves = [LeafPages {
                leaf_id: 0,
                pages: &[0, 1],
            }];
            let (flat, original) = source(g);
            let page_bytes = PAGE_ROWS * g.width;
            assert_ne!(&flat[..page_bytes], &flat[page_bytes..2 * page_bytes]);
            let (mut receipt, blob, _) = build(g, &leaves, &flat, &original);
            let mut fetched = fetch(&receipt.directory, &blob);
            {
                let verified = receipt
                    .directory
                    .verify_copies(&leaves, &fetched, &original, usize::MAX)
                    .unwrap();
                assert_ne!(verified.record(0), verified.record(PAGE_ROWS));
                for row in 0..2 * PAGE_ROWS {
                    assert_eq!(
                        verified.record(row),
                        Some(&flat[row * g.width..(row + 1) * g.width])
                    );
                }
            }
            let body = &mut fetched[0].1;
            let payload = HEADER + leaves[0].pages.len() * 4;
            body.copy_within(payload + page_bytes..payload + 2 * page_bytes, payload);
            rehash_copy(&mut receipt.directory, &fetched, 0);
            let mut encoded = Vec::new();
            let directory_sha256 = receipt.directory.write_to(&mut encoded).unwrap();
            assert_eq!(directory_sha256, hash(&encoded));
            let directory =
                Directory::open(&encoded, &directory_sha256, g, blob.len(), usize::MAX).unwrap();
            assert!(matches!(
                directory.verify_copies(&leaves, &fetched, &original, usize::MAX),
                Err(SourceBuildError::Invalid("capsule original unit digest"))
            ));
        }
    }

    #[test]
    fn closure_513_skips_middle_page_and_retains_original_ids_once() {
        let g = geometry(513, 257);
        let leaves = [
            LeafPages {
                leaf_id: 0,
                pages: &[0, 2],
            },
            LeafPages {
                leaf_id: 1,
                pages: &[0],
            },
        ];
        let (flat, original) = source(g);
        let (receipt, blob, _) = build(g, &leaves, &flat, &original);
        let fetched = fetch(&receipt.directory, &blob);
        let verified = receipt
            .directory
            .verify_copies(&leaves, &fetched, &original, usize::MAX)
            .unwrap();
        assert_eq!(verified.pages.len(), 2);
        assert_eq!(
            verified.pages.capacity() * size_of::<PageRef<'_>>()
                + size_of::<VerifiedRows<'_>>()
                + fetched.len() * SPAN_DESCRIPTOR_BYTES,
            verified.charge().lookup_bytes
        );
        let rows: Vec<usize> = (0..513)
            .filter(|&row| verified.record(row).is_some())
            .collect();
        assert_eq!(rows, (0..256).chain([512]).collect::<Vec<_>>());
        assert_eq!(verified.record(512), Some(&flat[512 * g.width..]));
        assert_eq!(verified.charge().logical_bytes, 257 * g.width);
        assert_eq!(verified.charge().duplicate_bytes, 256 * g.width);
        assert_eq!(verified.charge().framing_bytes, 76);
        assert_eq!(receipt.directory.byte_range(0).unwrap().start, 0);
        assert_eq!(
            receipt.directory.byte_range(0).unwrap().end,
            receipt.directory.byte_range(1).unwrap().start
        );
        assert_eq!(
            receipt.directory.byte_range(1).unwrap().end as usize,
            blob.len()
        );
    }

    #[test]
    fn corrupt_duplicate_rejected_even_with_healthy_copy_and_matching_capsule_hash() {
        let g = geometry(513, 1);
        let leaves = [
            LeafPages {
                leaf_id: 0,
                pages: &[0, 2],
            },
            LeafPages {
                leaf_id: 1,
                pages: &[0],
            },
        ];
        let (flat, original) = source(g);
        let (mut receipt, blob, _) = build(g, &leaves, &flat, &original);
        let mut fetched = fetch(&receipt.directory, &blob);
        let duplicate = &mut fetched.iter_mut().find(|(id, _)| *id == 1).unwrap().1;
        duplicate[HEADER + 4] ^= 1;
        assert!(
            receipt
                .directory
                .verify_copies(&leaves, &fetched, &original, usize::MAX)
                .is_err()
        );
        rehash_copy(&mut receipt.directory, &fetched, 1);
        assert!(matches!(
            receipt
                .directory
                .verify_copies(&leaves, &fetched, &original, usize::MAX),
            Err(SourceBuildError::Invalid("capsule original unit digest"))
        ));
    }

    #[test]
    fn malformed_directory_geometry_hash_offsets_overflow_and_lengths_rejected() {
        let g = geometry(513, 1024);
        let leaves = [
            LeafPages {
                leaf_id: 0,
                pages: &[0, 2],
            },
            LeafPages {
                leaf_id: 1,
                pages: &[0],
            },
        ];
        let (flat, original) = source(g);
        let (receipt, blob, encoded) = build(g, &leaves, &flat, &original);
        assert!(Directory::open(&encoded, &[0; 32], g, blob.len(), usize::MAX).is_err());
        for at in [
            0,
            8,
            12,
            14,
            16,
            20,
            24,
            28,
            32,
            36,
            40,
            48,
            56,
            HEADER + ENTRY + 8,
        ] {
            let mut bad = encoded.clone();
            bad[at] ^= 1;
            assert!(
                Directory::open(&bad, &hash(&bad), g, blob.len(), usize::MAX).is_err(),
                "{at}"
            );
        }
        for at in [40, 48, 56, HEADER + ENTRY + 8] {
            let mut bad = encoded.clone();
            bad[at..at + 8].copy_from_slice(&u64::MAX.to_le_bytes());
            assert!(Directory::open(&bad, &hash(&bad), g, blob.len(), usize::MAX).is_err());
        }
        let mut overlap = encoded.clone();
        overlap[HEADER + ENTRY + 8..HEADER + ENTRY + 16].fill(0);
        assert!(Directory::open(&overlap, &hash(&overlap), g, blob.len(), usize::MAX).is_err());
        for bytes in [&encoded[..HEADER - 1], &encoded[..encoded.len() - 1]] {
            assert!(Directory::open(bytes, &hash(bytes), g, blob.len(), usize::MAX).is_err());
        }
        let mut trailing = encoded.clone();
        trailing.push(0);
        assert!(Directory::open(&trailing, &hash(&trailing), g, blob.len(), usize::MAX).is_err());
        assert!(Directory::open(&encoded, &hash(&encoded), g, blob.len() + 1, usize::MAX).is_err());
        assert!(add(usize::MAX, 1).is_err());
        assert!(mul(usize::MAX, 2).is_err());
        assert!(Geometry::new(usize::MAX, 1, SemanticProfile::Scale1m).is_err());
        assert!(Geometry::new(1, 1025, SemanticProfile::Scale1m).is_err());
        assert!(Geometry::new(0, 1, SemanticProfile::Scale1m).is_err());
        assert!(Geometry::new(513, 257, SemanticProfile::Fresh1m).is_err());
        assert_eq!(receipt.directory.blob_bytes(), blob.len());
    }

    #[test]
    fn capsule_magic_leaf_plane_page_order_tail_truncation_and_trailing_rejected() {
        let g = geometry(513, 257);
        let leaves = [
            LeafPages {
                leaf_id: 0,
                pages: &[0, 2],
            },
            LeafPages {
                leaf_id: 1,
                pages: &[0],
            },
        ];
        let (flat, original) = source(g);
        let (mut receipt, blob, _) = build(g, &leaves, &flat, &original);
        for at in [0, 8, 12, 14, 16, 20, 24, 28, 32, 36] {
            let mut fetched = fetch(&receipt.directory, &blob);
            fetched.iter_mut().find(|(id, _)| *id == 0).unwrap().1[at] ^= 1;
            rehash_copy(&mut receipt.directory, &fetched, 0);
            assert!(
                receipt
                    .directory
                    .verify_copies(&leaves, &fetched, &original, usize::MAX)
                    .is_err(),
                "{at}"
            );
        }
        for grow in [false, true] {
            let mut fetched = fetch(&receipt.directory, &blob);
            let bytes = &mut fetched.iter_mut().find(|(id, _)| *id == 0).unwrap().1;
            if grow {
                bytes.push(0);
            } else {
                bytes.pop();
            }
            rehash_copy(&mut receipt.directory, &fetched, 0);
            assert!(
                receipt
                    .directory
                    .verify_copies(&leaves, &fetched, &original, usize::MAX)
                    .is_err()
            );
        }
        let fetched = fetch(&receipt.directory, &blob);
        rehash_copy(&mut receipt.directory, &fetched, 0);
        for pages in [&[2, 0][..], &[0, 0], &[0, 3]] {
            let bad = [LeafPages { leaf_id: 0, pages }, leaves[1]];
            assert!(
                receipt
                    .directory
                    .verify_copies(&bad, &fetched, &original, usize::MAX)
                    .is_err()
            );
        }
        let mut reversed_roster = fetch(&receipt.directory, &blob);
        let body = &mut reversed_roster
            .iter_mut()
            .find(|(id, _)| *id == 0)
            .unwrap()
            .1;
        body[32..36].copy_from_slice(&2u32.to_le_bytes());
        body[36..40].copy_from_slice(&0u32.to_le_bytes());
        rehash_copy(&mut receipt.directory, &reversed_roster, 0);
        assert!(
            receipt
                .directory
                .verify_copies(&leaves, &reversed_roster, &original, usize::MAX)
                .is_err()
        );
    }

    #[test]
    fn wrong_original_plane_unit_digest_and_fetch_set_rejected() {
        let g = geometry(257, 1);
        let leaves = [
            LeafPages {
                leaf_id: 0,
                pages: &[0, 1],
            },
            LeafPages {
                leaf_id: 1,
                pages: &[0],
            },
        ];
        let (flat, original) = source(g);
        let (receipt, blob, _) = build(g, &leaves, &flat, &original);
        let fetched = fetch(&receipt.directory, &blob);
        let mut wrong = flat.clone();
        wrong[0] ^= 1;
        let wrong_authority = authority(g, &wrong, hash(&wrong));
        assert!(
            receipt
                .directory
                .verify_copies(&leaves, &fetched, &wrong_authority, usize::MAX)
                .is_err()
        );
        let (other_flat, other_authority) = source(geometry(257, 257));
        assert!(!other_flat.is_empty());
        assert!(
            receipt
                .directory
                .verify_copies(&leaves, &fetched, &other_authority, usize::MAX)
                .is_err()
        );
        let sq8_flat = vec![0; g.rows * (g.dimensions + 12)];
        let sidecar: Vec<u8> = sq8_flat
            .chunks(32 * (g.dimensions + 12))
            .flat_map(hash)
            .collect();
        let manifest = serde_json::to_vec(&serde_json::json!({
            "schema": "borsuk-v115-sq8-page-authority-v2", "generation": 7,
            "rows": g.rows, "dimensions": g.dimensions, "page_rows": 32,
            "object_sha256": format!("{:x}", Sha256::digest(&sq8_flat)),
            "page_digest_sha256": format!("{:x}", Sha256::digest(&sidecar)),
        }))
        .unwrap();
        let sq8_authority = PageAuthority::load(
            &manifest,
            &format!("{:x}", Sha256::digest(&manifest)),
            &sidecar,
        )
        .unwrap();
        assert!(matches!(
            receipt
                .directory
                .verify_copies(&leaves, &fetched, &sq8_authority, usize::MAX),
            Err(SourceBuildError::Invalid(
                "capsule original SOURCE authority"
            ))
        ));
        let mut bad_ids = fetched.clone();
        bad_ids[0].0 = bad_ids[1].0;
        assert!(
            receipt
                .directory
                .verify_copies(&leaves, &bad_ids, &original, usize::MAX)
                .is_err()
        );
        assert!(
            receipt
                .directory
                .verify_copies(&leaves, &fetched[..1], &original, usize::MAX)
                .is_err()
        );
        let mut extra = fetched.clone();
        extra.push(fetched[0].clone());
        assert!(
            receipt
                .directory
                .verify_copies(&leaves, &extra, &original, usize::MAX)
                .is_err()
        );
    }

    struct NoIo;
    impl Read for NoIo {
        fn read(&mut self, _: &mut [u8]) -> io::Result<usize> {
            panic!("read before admission")
        }
    }
    impl Seek for NoIo {
        fn seek(&mut self, _: SeekFrom) -> io::Result<u64> {
            panic!("seek before admission")
        }
    }
    impl Write for NoIo {
        fn write(&mut self, _: &[u8]) -> io::Result<usize> {
            panic!("write before admission")
        }
        fn flush(&mut self) -> io::Result<()> {
            panic!("flush before admission")
        }
    }

    #[test]
    fn modeled_minus_one_refuses_before_io_and_actual_capacity_is_charged() {
        let g = geometry(513, 257);
        let leaves = [
            LeafPages {
                leaf_id: 0,
                pages: &[0, 2],
            },
            LeafPages {
                leaf_id: 1,
                pages: &[0],
            },
        ];
        let (flat, original) = source(g);
        let model = build_charge(g, &leaves).unwrap();
        assert!(matches!(
            pack(
                g,
                &leaves,
                &original,
                &mut NoIo,
                &mut NoIo,
                &mut NoIo,
                model.memory_bytes - 1
            ),
            Err(SourceBuildError::Invalid("capsule memory cap"))
        ));
        let mut blob = Vec::new();
        let mut encoded = Vec::new();
        let receipt = pack(
            g,
            &leaves,
            &original,
            &mut Cursor::new(&flat),
            &mut blob,
            &mut encoded,
            model.memory_bytes,
        )
        .unwrap();
        assert_eq!(receipt.charge, model);
        let open = Directory::open_charge(g, leaves.len()).unwrap();
        assert!(
            Directory::open(
                &encoded,
                &hash(&encoded),
                g,
                blob.len(),
                open.memory_bytes - 1
            )
            .is_err()
        );
        Directory::open(&encoded, &hash(&encoded), g, blob.len(), open.memory_bytes).unwrap();
        let mut fetched = fetch(&receipt.directory, &blob);
        let query = receipt.directory.query_charge(&leaves).unwrap();
        assert!(
            receipt
                .directory
                .verify_copies(&leaves, &fetched, &original, query.memory_bytes - 1)
                .is_err()
        );
        assert!(
            receipt
                .directory
                .verify_copies(&leaves, &fetched, &original, query.memory_bytes)
                .is_ok()
        );
        fetched[0].1.reserve(1024);
        assert!(
            receipt
                .directory
                .verify_copies(&leaves, &fetched, &original, query.memory_bytes)
                .is_err()
        );
        let verified = receipt
            .directory
            .verify_copies(&leaves, &fetched, &original, usize::MAX)
            .unwrap();
        assert_eq!(
            verified.charge().buffer_bytes,
            fetched.iter().map(|(_, b)| b.capacity()).sum::<usize>()
        );
    }

    struct BoundedReader {
        inner: Cursor<Vec<u8>>,
        max_request: usize,
        zero_seeks: usize,
        change_duplicate: bool,
    }
    impl Read for BoundedReader {
        fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
            self.max_request = self.max_request.max(bytes.len());
            let read = self.inner.read(bytes)?;
            if self.change_duplicate && self.zero_seeks >= 3 && read > 0 {
                bytes[0] ^= 1;
            }
            Ok(read)
        }
    }
    impl Seek for BoundedReader {
        fn seek(&mut self, position: SeekFrom) -> io::Result<u64> {
            if position == SeekFrom::Start(0) {
                self.zero_seeks += 1;
            }
            self.inner.seek(position)
        }
    }
    #[test]
    fn streaming_is_page_bounded_and_reauthenticates_copied_duplicates() {
        let g = geometry(513, 1024);
        let leaves = [
            LeafPages {
                leaf_id: 0,
                pages: &[0, 2],
            },
            LeafPages {
                leaf_id: 1,
                pages: &[0],
            },
        ];
        let (flat, original) = source(g);
        for change_duplicate in [false, true] {
            let mut input = BoundedReader {
                inner: Cursor::new(flat.clone()),
                max_request: 0,
                zero_seeks: 0,
                change_duplicate,
            };
            let result = pack(
                g,
                &leaves,
                &original,
                &mut input,
                &mut io::sink(),
                &mut io::sink(),
                usize::MAX,
            );
            assert_eq!(result.is_err(), change_duplicate);
            assert_eq!(input.max_request, 256 * g.width);
            assert_eq!(input.zero_seeks, 3);
        }
    }

    #[test]
    fn original_flat_length_sha_and_unit_digest_fail_before_output() {
        let g = geometry(257, 1);
        let leaves = [LeafPages {
            leaf_id: 0,
            pages: &[0, 1],
        }];
        let (flat, original) = source(g);
        let mut truncated = flat.clone();
        truncated.pop();
        let mut trailing = flat.clone();
        trailing.push(0);
        let mut corrupt = flat.clone();
        corrupt[0] ^= 1;
        for bytes in [truncated, trailing, corrupt] {
            assert!(
                pack(
                    g,
                    &leaves,
                    &original,
                    &mut Cursor::new(bytes),
                    &mut NoIo,
                    &mut NoIo,
                    usize::MAX
                )
                .is_err()
            );
        }
        let wrong_hash = authority(g, &flat, [0; 32]);
        assert!(matches!(
            pack(
                g,
                &leaves,
                &wrong_hash,
                &mut Cursor::new(flat),
                &mut NoIo,
                &mut NoIo,
                usize::MAX
            ),
            Err(SourceBuildError::Invalid("capsule flat SHA"))
        ));
    }

    #[test]
    fn one_response_adjacent_capsules_borrowed_once_stable_rows_and_capacities() {
        let g = geometry(513, 257);
        let leaves = [
            LeafPages {
                leaf_id: 0,
                pages: &[0, 2],
            },
            LeafPages {
                leaf_id: 1,
                pages: &[0],
            },
        ];
        let (flat, original) = source(g);
        let (mut receipt, mut blob, _) = build(g, &leaves, &flat, &original);
        blob.reserve(2048);
        let model = receipt.directory.query_charge(&leaves).unwrap();
        let actual =
            model.memory_bytes - model.buffer_bytes + blob.capacity() - SPAN_DESCRIPTOR_BYTES;
        let verified = {
            let one_response = [FetchedSpan {
                offset: 0,
                bytes: &blob,
            }];
            assert_eq!(one_response.len(), 1);
            assert_eq!(one_response[0].byte_range().unwrap(), 0..blob.len() as u64);
            assert!(matches!(
                receipt
                    .directory
                    .verify(&leaves, &one_response, &original, actual - 1),
                Err(SourceBuildError::Invalid("capsule memory cap"))
            ));
            receipt
                .directory
                .verify(&leaves, &one_response, &original, actual)
                .unwrap()
        };
        // The ephemeral span list can drop; the original physical buffer stays borrowed.
        assert_eq!(verified.charge().buffer_bytes, blob.capacity());
        assert_eq!(verified.charge().memory_bytes, actual);
        assert_eq!(verified.charge().logical_bytes, 257 * g.width);
        assert_eq!(verified.charge().duplicate_bytes, 256 * g.width);
        assert_eq!(verified.page_leaf(0), Some(0));
        assert_eq!(verified.record(0).unwrap().as_ptr(), blob[40..].as_ptr());
        assert_eq!(
            verified.record(512).unwrap().as_ptr(),
            blob[40 + 256 * g.width..].as_ptr()
        );
        let present: Vec<_> = (0..g.rows)
            .filter(|&row| verified.record(row).is_some())
            .collect();
        assert_eq!(present, (0..256).chain([512]).collect::<Vec<_>>());
        for row in present {
            assert_eq!(
                verified.record(row),
                Some(&flat[row * g.width..(row + 1) * g.width])
            );
        }
        drop(verified);
        let second = receipt.directory.byte_range(1).unwrap();
        blob[second.start as usize + HEADER + 4] ^= 1;
        receipt.directory.entries[1].sha256 =
            hash(&blob[second.start as usize..second.end as usize]);
        assert!(matches!(
            receipt.directory.verify(
                &leaves,
                &[FetchedSpan {
                    offset: 0,
                    bytes: &blob
                }],
                &original,
                usize::MAX
            ),
            Err(SourceBuildError::Invalid("capsule original unit digest"))
        ));
    }

    #[test]
    fn response_spans_reject_overlap_gap_unselected_partial_trailing_and_overflow() {
        let g = geometry(513, 1);
        let leaves = [
            LeafPages {
                leaf_id: 0,
                pages: &[0, 2],
            },
            LeafPages {
                leaf_id: 1,
                pages: &[0],
            },
            LeafPages {
                leaf_id: 2,
                pages: &[2],
            },
        ];
        let (flat, original) = source(g);
        let (receipt, blob, _) = build(g, &leaves, &flat, &original);
        let directory = &receipt.directory;
        let all = FetchedSpan {
            offset: 0,
            bytes: &blob,
        };
        assert!(
            directory
                .verify(&leaves, &[all], &original, usize::MAX)
                .is_ok()
        );
        assert!(
            directory
                .verify(&[leaves[0], leaves[2]], &[all], &original, usize::MAX)
                .is_err()
        );
        assert!(
            directory
                .verify(&leaves, &[all, all], &original, usize::MAX)
                .is_err()
        );
        let first_end = directory.byte_range(0).unwrap().end as usize;
        let last_start = directory.byte_range(2).unwrap().start as usize;
        let first = blob[..first_end].to_vec();
        let last = blob[last_start..].to_vec();
        let gap = [
            FetchedSpan {
                offset: 0,
                bytes: &first,
            },
            FetchedSpan {
                offset: last_start as u64,
                bytes: &last,
            },
        ];
        assert!(
            directory
                .verify(&leaves, &gap, &original, usize::MAX)
                .is_err()
        );
        for (offset, bytes) in [
            (1, blob[1..].to_vec()),
            (0, blob[..blob.len() - 1].to_vec()),
        ] {
            assert!(
                directory
                    .verify(
                        &leaves,
                        &[FetchedSpan {
                            offset,
                            bytes: &bytes
                        }],
                        &original,
                        usize::MAX
                    )
                    .is_err()
            );
        }
        let mut trailing = blob.clone();
        trailing.push(0);
        assert!(
            directory
                .verify(
                    &leaves,
                    &[FetchedSpan {
                        offset: 0,
                        bytes: &trailing
                    }],
                    &original,
                    usize::MAX
                )
                .is_err()
        );
        let overflow = FetchedSpan {
            offset: u64::MAX,
            bytes: &first,
        };
        assert!(overflow.byte_range().is_err());
        assert!(
            directory
                .verify(&leaves, &[overflow], &original, usize::MAX)
                .is_err()
        );
    }
}
