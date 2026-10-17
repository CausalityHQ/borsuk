//! Streaming, generation-bound SOURCE capsules and authenticated query page ranges.
//!
//! Version 2 uses little-endian integers. Both headers are 32 bytes: magic[8],
//! N:u32@8, D:u16@12, codec width:u16@14, profile:u32@16, leaf count:u32@20.
//! Directory magic is BORSDR02, followed by unit rows (32)@24 and page rows
//! (256)@28. Capsule magic is BORSCP02, followed by leaf ID@24 and page count@28.
//! A capsule then stores sorted unique u32 page IDs and their complete raw pages,
//! including only the true final tail. Directory entries are 64 bytes:
//! leaf:u32, count:u32, offset:u64, total:u64, payload:u64, SHA256[32].
//! Entries, concatenated directory rosters and capsules are in leaf-ID order.
//! Directory rosters are authenticated and checked against router membership.
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
//! authority/codec/router. Owned rosters and query plans charge actual Vec
//! capacities; callers account for borrowed input backing capacity. Encoded
//! directories are streamed on build and retained during open, not stored here.
//! Memory admission uses the smaller of the caller cap and profile allocation cap.
//! Physical/hash byte charges are exposed for the caller's I/O and work budgets;
//! they are not memory charges. Build/full-validation closures use leaf-ID order.
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
// The raw-span model already includes the Vec owned by a proof.
const PROOF_EXTRA_BYTES: usize = size_of::<VerifiedPageSpan<'_, '_>>() - size_of::<Vec<u8>>();

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
#[cfg(test)]
pub(crate) fn reset_hash_bytes() {
    tests::HASH_BYTES.with(|count| count.set(0));
}

#[cfg(test)]
pub(crate) fn hash_bytes() -> usize {
    tests::HASH_BYTES.with(|count| count.get())
}

fn hash(bytes: &[u8]) -> [u8; 32] {
    #[cfg(test)]
    tests::HASH_BYTES.with(|count| count.set(count.get() + bytes.len()));
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
        #[cfg(test)]
        tests::HASH_BYTES.with(|count| count.set(count.get() + bytes.len()));
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

/// Decoded directory, jointly pinned with the router and original SOURCE authority.
#[derive(Debug)]
pub struct Directory {
    geometry: Geometry,
    entries: Vec<DirectoryEntry>,
    rosters: Vec<Vec<u32>>,
    blob_bytes: usize,
    generation: u64,
    directory_sha256: [u8; 32],
}
impl Directory {
    /// Admission model available before fetching an encoded directory.
    pub fn open_charge(
        geometry: Geometry,
        leaf_count: usize,
        page_occurrences: usize,
    ) -> Result<Charge> {
        require(
            leaf_count > 0
                && leaf_count <= page_occurrences
                && page_occurrences <= geometry.rows.div_ceil(UNIT_ROWS),
            "capsule leaf/roster count",
        )?;
        let mut charge = Charge {
            source_bytes: mul(geometry.rows, geometry.width)?,
            encoded_directory_bytes: add(
                add(HEADER, mul(leaf_count, ENTRY)?)?,
                mul(page_occurrences, 4)?,
            )?,
            decoded_directory_bytes: add(
                add(
                    size_of::<Self>(),
                    mul(leaf_count, size_of::<DirectoryEntry>())?,
                )?,
                add(
                    mul(leaf_count, size_of::<Vec<u32>>())?,
                    mul(page_occurrences, 4)?,
                )?,
            )?,
            scratch_bytes: add(
                SCRATCH,
                mul(geometry.pages().min(page_occurrences - leaf_count + 1), 4)?,
            )?,
            ..Charge::default()
        };
        charge.hash_bytes = charge.encoded_directory_bytes;
        charge.memory(true)?;
        Ok(charge)
    }
    /// Authenticate an exact v2 directory. Old production directories are unsupported.
    pub fn open(
        encoded: &[u8],
        expected_sha256: &[u8; 32],
        geometry: Geometry,
        blob_bytes: usize,
        generation: u64,
        cap: usize,
    ) -> Result<Self> {
        let cap = cap.min(geometry.profile.allocation_cap());
        require(encoded.len() >= HEADER, "capsule directory header")?;
        require(
            &encoded[..8] == b"BORSDR02",
            "capsule directory format: expected BORSDR02",
        )?;
        let count = u32::from_le_bytes(encoded[20..24].try_into().unwrap()) as usize;
        let roster_start = add(HEADER, mul(count, ENTRY)?)?;
        let roster_bytes = encoded
            .len()
            .checked_sub(roster_start)
            .ok_or(SourceBuildError::Invalid("capsule directory roster length"))?;
        require(roster_bytes % 4 == 0, "capsule directory roster length")?;
        let occurrences = roster_bytes / 4;
        let mut charge = Self::open_charge(geometry, count, occurrences)?;
        admit(charge.memory_bytes, cap)?;
        require(
            encoded[..HEADER]
                == geometry.header(b"BORSDR02", count, UNIT_ROWS as u32, PAGE_ROWS as u32)
                && hash(encoded) == *expected_sha256,
            "capsule directory identity",
        )?;
        let mut entries = reserve(count)?;
        let mut rosters = reserve(count)?;
        charge.decoded_directory_bytes = add(
            add(
                size_of::<Self>(),
                mul(entries.capacity(), size_of::<DirectoryEntry>())?,
            )?,
            add(
                mul(rosters.capacity(), size_of::<Vec<u32>>())?,
                roster_bytes,
            )?,
        )?;
        charge.memory(true)?;
        admit(charge.memory_bytes, cap)?;
        let mut offset = 0;
        let mut cursor = roster_start;
        for (id, bytes) in encoded[HEADER..roster_start]
            .chunks_exact(ENTRY)
            .enumerate()
        {
            let item = DirectoryEntry {
                leaf_id: u32::from_le_bytes(bytes[..4].try_into().unwrap()),
                page_count: u32::from_le_bytes(bytes[4..8].try_into().unwrap()),
                offset: u64::from_le_bytes(bytes[8..16].try_into().unwrap()),
                total_bytes: u64::from_le_bytes(bytes[16..24].try_into().unwrap()),
                payload_bytes: u64::from_le_bytes(bytes[24..32].try_into().unwrap()),
                sha256: bytes[32..].try_into().unwrap(),
            };
            let count = item.page_count as usize;
            require(
                item.leaf_id as usize == id && count > 0 && count <= geometry.pages(),
                "capsule directory leaf/pages",
            )?;
            let stop = add(cursor, mul(count, 4)?)?;
            let encoded_pages = encoded
                .get(cursor..stop)
                .ok_or(SourceBuildError::Invalid("capsule directory roster length"))?;
            let mut pages = reserve(count)?;
            charge.decoded_directory_bytes = add(
                charge.decoded_directory_bytes,
                mul(pages.capacity() - count, 4)?,
            )?;
            charge.memory(true)?;
            admit(charge.memory_bytes, cap)?;
            for bytes in encoded_pages.chunks_exact(4) {
                pages.push(u32::from_le_bytes(bytes.try_into().unwrap()));
            }
            let expected = entry(
                geometry,
                LeafPages {
                    leaf_id: item.leaf_id,
                    pages: &pages,
                },
                offset,
            )?;
            require(
                item.offset == expected.offset
                    && item.total_bytes == expected.total_bytes
                    && item.payload_bytes == expected.payload_bytes,
                "capsule directory layout",
            )?;
            offset = add(offset, number(item.total_bytes)?)?;
            cursor = stop;
            entries.push(item);
            rosters.push(pages);
        }
        require(cursor == encoded.len(), "capsule directory trailing roster")?;
        require(offset == blob_bytes, "capsule blob length")?;
        Ok(Self {
            geometry,
            entries,
            rosters,
            blob_bytes,
            generation,
            directory_sha256: *expected_sha256,
        })
    }
    /// Authenticated page IDs for one leaf.
    pub fn pages(&self, leaf_id: u32) -> Result<&[u32]> {
        self.rosters
            .get(leaf_id as usize)
            .map(Vec::as_slice)
            .ok_or(SourceBuildError::Invalid("capsule leaf ID"))
    }
    /// Independently compare every stored roster with authenticated router membership.
    pub(crate) fn validate_router(&self, router: &SemanticUnitRouter, cap: usize) -> Result<()> {
        require(
            router.manifest().leaves.len() == self.entries.len()
                && router.manifest().rows == self.geometry.rows
                && router.manifest().dimensions == self.geometry.dimensions
                && router.manifest().profile == self.geometry.profile
                && router.unit_membership().len() == self.geometry.rows.div_ceil(UNIT_ROWS),
            "capsule router geometry",
        )?;
        let cap = cap.min(self.geometry.profile.allocation_cap());
        let remaining = cap
            .checked_sub(self.decoded_bytes()?)
            .ok_or(SourceBuildError::Invalid("capsule memory cap"))?;
        for item in &self.entries {
            let expected = leaf_pages(router, item.leaf_id as usize, remaining)?;
            require(
                expected.as_slice() == self.pages(item.leaf_id)?,
                "capsule router roster",
            )?;
        }
        Ok(())
    }
    /// Authenticated entries in original leaf-ID order.
    pub fn entries(&self) -> &[DirectoryEntry] {
        &self.entries
    }
    /// Actual decoded-directory allocation, including all retained roster capacities.
    pub fn decoded_bytes(&self) -> Result<usize> {
        let fixed = add(
            add(
                size_of::<Self>(),
                mul(self.entries.capacity(), size_of::<DirectoryEntry>())?,
            )?,
            mul(self.rosters.capacity(), size_of::<Vec<u32>>())?,
        )?;
        self.rosters
            .iter()
            .try_fold(fixed, |sum, pages| add(sum, mul(pages.capacity(), 4)?))
    }
    fn encoded_bytes(&self) -> Result<usize> {
        self.rosters.iter().try_fold(
            add(HEADER, mul(self.entries.len(), ENTRY)?)?,
            |sum, pages| add(sum, mul(pages.len(), 4)?),
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
    /// Directory-derived extent of one complete original page occurrence.
    pub(crate) fn page_range(&self, leaf_id: u32, page_id: u32) -> Result<Range<usize>> {
        let pages = self.pages(leaf_id)?;
        let index = pages
            .binary_search(&page_id)
            .map_err(|_| SourceBuildError::Invalid("capsule missing page"))?;
        let item = &self.entries[leaf_id as usize];
        // Only the globally last page can be short, so every preceding page is full.
        let start = add(
            add(number(item.offset)?, add(HEADER, mul(pages.len(), 4)?)?)?,
            mul(mul(index, PAGE_ROWS)?, self.geometry.width)?,
        )?;
        Ok(start..add(start, self.geometry.page_bytes(page_id)?)?)
    }
    /// Total physical blob length.
    pub fn blob_bytes(&self) -> usize {
        self.blob_bytes
    }
    /// Stream v2 entries followed by every roster. Flush is not durable publication.
    pub fn write_to<W: Write>(&self, writer: &mut W) -> Result<[u8; 32]> {
        let header = self.geometry.header(
            b"BORSDR02",
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
        for pages in &self.rosters {
            for page in pages {
                let bytes = page.to_le_bytes();
                writer.write_all(&bytes)?;
                digest.update(bytes);
            }
        }
        writer.flush()?;
        Ok(digest.finalize().into())
    }
    /// Conservative full-capsule model; independent of trimmed query requests.
    pub fn query_charge(&self, leaves: &[LeafPages<'_>]) -> Result<Charge> {
        let mut charge = closure_charge(self.geometry, leaves)?;
        let mut occurrences = 0;
        for &leaf in leaves {
            require(
                self.pages(leaf.leaf_id)? == leaf.pages,
                "capsule expected closure roster",
            )?;
            occurrences = add(occurrences, leaf.pages.len())?;
        }
        charge.encoded_directory_bytes = self.encoded_bytes()?;
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
    /// Full-leaf lifecycle admission; never routes through page-range planning.
    pub(crate) fn whole_leaf_charge(&self, leaf_id: u32) -> Result<Charge> {
        self.query_charge(&[LeafPages {
            leaf_id,
            pages: self.pages(leaf_id)?,
        }])
    }
    /// Full capsule SHA, exact framing and every original unit remain mandatory.
    pub(crate) fn verify_whole_leaf(
        &self,
        leaf_id: u32,
        fetched: FetchedSpan<'_>,
        authority: &PageAuthority,
        cap: usize,
    ) -> Result<Charge> {
        let leaf = LeafPages {
            leaf_id,
            pages: self.pages(leaf_id)?,
        };
        let (charge, _) =
            self.response_charge(&[leaf], std::iter::once(fetched), authority, cap)?;
        self.authenticate_leaf(
            leaf,
            self.leaf_body(leaf_id, std::iter::once(fetched))?,
            authority,
        )?;
        Ok(charge)
    }
}
/// Pre-I/O build model, including original flat validation and all duplicate copies.
pub fn build_charge(geometry: Geometry, leaves: &[LeafPages<'_>]) -> Result<Charge> {
    let mut charge = closure_charge(geometry, leaves)?;
    for (id, leaf) in leaves.iter().enumerate() {
        require(leaf.leaf_id as usize == id, "capsule build leaf IDs")?;
    }
    let occurrences = leaves
        .iter()
        .try_fold(0, |sum, leaf| add(sum, leaf.pages.len()))?;
    let directory = Directory::open_charge(geometry, leaves.len(), occurrences)?;
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
    let mut rosters = reserve(leaves.len())?;
    let mut buffer = reserve(charge.buffer_bytes)?;
    buffer.resize(charge.buffer_bytes, 0u8);
    charge.buffer_bytes = buffer.capacity();
    charge.decoded_directory_bytes = add(
        add(
            size_of::<Directory>(),
            mul(entries.capacity(), size_of::<DirectoryEntry>())?,
        )?,
        add(
            mul(rosters.capacity(), size_of::<Vec<u32>>())?,
            leaves
                .iter()
                .try_fold(0, |sum, leaf| add(sum, mul(leaf.pages.len(), 4)?))?,
        )?,
    )?;
    charge.memory(false)?;
    admit(charge.memory_bytes, cap)?;
    for leaf in leaves {
        let mut pages = reserve(leaf.pages.len())?;
        charge.decoded_directory_bytes = add(
            charge.decoded_directory_bytes,
            mul(pages.capacity() - leaf.pages.len(), 4)?,
        )?;
        charge.memory(false)?;
        admit(charge.memory_bytes, cap)?;
        pages.extend_from_slice(leaf.pages);
        rosters.push(pages);
    }
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
        let header = geometry.header(b"BORSCP02", leaves.len(), leaf.leaf_id, item.page_count);
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
    let mut directory = Directory {
        geometry,
        entries,
        rosters,
        blob_bytes: offset,
        generation: authority.generation(),
        directory_sha256: [0; 32],
    };
    let directory_sha256 = directory.write_to(directory_writer)?;
    directory.directory_sha256 = directory_sha256;
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
    /// before exposing rows. Disjoint responses may coalesce adjacent selected capsules.
    pub fn verify<'a>(
        &self,
        leaves: &[LeafPages<'_>],
        fetched: &[FetchedSpan<'a>],
        authority: &PageAuthority,
        cap: usize,
    ) -> Result<VerifiedRows<'a>> {
        let (charge, occurrences) =
            self.response_charge(leaves, fetched.iter().copied(), authority, cap)?;
        for &leaf in leaves {
            self.authenticate_leaf(
                leaf,
                self.leaf_body(leaf.leaf_id, fetched.iter().copied())?,
                authority,
            )?;
        }
        self.row_lookup(leaves, fetched.iter().copied(), charge, occurrences, cap)
    }

    fn response_charge<'a>(
        &self,
        leaves: &[LeafPages<'_>],
        fetched: impl ExactSizeIterator<Item = FetchedSpan<'a>> + Clone,
        authority: &PageAuthority,
        cap: usize,
    ) -> Result<(Charge, usize)> {
        let cap = cap.min(self.geometry.profile.allocation_cap());
        let mut charge = self.query_charge(leaves)?;
        self.geometry.authority(authority)?;
        require(
            authority.generation() == self.generation,
            "capsule authority generation",
        )?;
        require(
            fetched.len() != 0 && fetched.len() <= leaves.len(),
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
        for (index, span) in fetched.clone().enumerate() {
            let range = span.byte_range()?;
            require(
                !range.is_empty() && range.end <= self.blob_bytes as u64,
                "capsule response bounds",
            )?;
            for other in fetched.clone().take(index) {
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
        Ok((charge, occurrences))
    }

    fn leaf_body<'a>(
        &self,
        leaf: u32,
        mut fetched: impl Iterator<Item = FetchedSpan<'a>>,
    ) -> Result<&'a [u8]> {
        let range = self.byte_range(leaf)?;
        let span = fetched
            .find(|span| {
                span.offset <= range.start
                    && span
                        .offset
                        .checked_add(span.bytes.len() as u64)
                        .is_some_and(|end| range.end <= end)
            })
            .ok_or(SourceBuildError::Invalid("capsule missing response leaf"))?;
        let buffer: &'a Vec<u8> = span.bytes;
        buffer
            .get(number(range.start - span.offset)?..number(range.end - span.offset)?)
            .ok_or(SourceBuildError::Invalid("capsule response bounds"))
    }

    fn authenticate_leaf(
        &self,
        leaf: LeafPages<'_>,
        bytes: &[u8],
        authority: &PageAuthority,
    ) -> Result<()> {
        let item = &self.entries[leaf.leaf_id as usize];
        require(
            bytes.len() == number(item.total_bytes)? && hash(bytes) == item.sha256,
            "capsule fetched size/hash",
        )?;
        let range = self.byte_range(leaf.leaf_id)?;
        let start = number(range.start)?;
        self.walk_range(
            start..number(range.end)?,
            |offset, expected| {
                require(
                    bytes.get(offset - start..add(offset - start, expected.len())?)
                        == Some(expected),
                    "capsule expected BORSCP02 header/roster",
                )
            },
            |page, _, extent| {
                self.geometry.verify_page(
                    page,
                    &bytes[extent.start - start..extent.end - start],
                    authority,
                )
            },
        )
    }

    // Only full authentication or an immutable, generation-bound proof reaches this lookup.
    fn row_lookup<'a>(
        &self,
        leaves: &[LeafPages<'_>],
        fetched: impl ExactSizeIterator<Item = FetchedSpan<'a>> + Clone,
        mut charge: Charge,
        occurrences: usize,
        cap: usize,
    ) -> Result<VerifiedRows<'a>> {
        let cap = cap.min(self.geometry.profile.allocation_cap());
        let descriptors = add(
            size_of::<VerifiedRows<'_>>(),
            mul(fetched.len(), SPAN_DESCRIPTOR_BYTES)?,
        )?;
        let mut pages = reserve(occurrences)?;
        charge.lookup_bytes = add(
            mul(pages.capacity(), size_of::<PageRef<'_>>())?,
            descriptors,
        )?;
        charge.memory(false)?;
        admit(charge.memory_bytes, cap)?;
        for &leaf in leaves {
            let bytes = self.leaf_body(leaf.leaf_id, fetched.clone())?;
            let mut offset = add(HEADER, mul(leaf.pages.len(), 4)?)?;
            for &page in leaf.pages {
                let end = add(offset, self.geometry.page_bytes(page)?)?;
                let body = bytes
                    .get(offset..end)
                    .ok_or(SourceBuildError::Invalid("capsule truncated page"))?;
                pages.push(PageRef {
                    page,
                    leaf: leaf.leaf_id,
                    bytes: body,
                });
                offset = end;
            }
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

#[derive(Debug)]
struct PageOwner {
    page: u32,
    leaf: u32,
}

/// Immutable query selection and admitted physical ranges.
pub(crate) struct PageRangePlan<'g> {
    directory: &'g Directory,
    authority: &'g PageAuthority,
    selected: Vec<usize>,
    owners: Vec<PageOwner>,
    ranges: Vec<Range<usize>>,
    charge: Charge,
}
impl std::fmt::Debug for PageRangePlan<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PageRangePlan")
            .field("generation", &self.directory.generation)
            .field("directory_sha256", &self.directory.directory_sha256)
            .field("selected", &self.selected)
            .field("owners", &self.owners)
            .field("ranges", &self.ranges)
            .finish_non_exhaustive()
    }
}

/// Owns only fetched, authenticated bytes; it does not approve unfetched artifact bytes.
pub(crate) struct VerifiedPageSpan<'p, 'g> {
    plan: &'p PageRangePlan<'g>,
    range_index: usize,
    bytes: Vec<u8>,
}
impl std::fmt::Debug for VerifiedPageSpan<'_, '_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VerifiedPageSpan")
            .field("range_index", &self.range_index)
            .field("bytes", &self.bytes.len())
            .finish_non_exhaustive()
    }
}
impl VerifiedPageSpan<'_, '_> {
    pub(crate) fn bytes(&self) -> &[u8] {
        &self.bytes
    }
    #[cfg(test)]
    pub(crate) fn start(&self) -> usize {
        self.plan.ranges[self.range_index].start
    }
}

impl Directory {
    /// Visit directory-derived framing intersections and complete page occurrences.
    fn walk_range(
        &self,
        range: Range<usize>,
        mut framing: impl FnMut(usize, &[u8]) -> Result<()>,
        mut page: impl FnMut(u32, u32, Range<usize>) -> Result<()>,
    ) -> Result<()> {
        require(
            !range.is_empty() && range.end <= self.blob_bytes,
            "capsule page response bounds",
        )?;
        let first = self
            .entries
            .partition_point(|item| item.offset <= range.start as u64)
            .saturating_sub(1);
        let mut covered = range.start;
        for item in &self.entries[first..] {
            let start = number(item.offset)?;
            if start >= range.end {
                break;
            }
            let pages = self.pages(item.leaf_id)?;
            {
                let mut frame = |offset: usize, expected: &[u8]| -> Result<()> {
                    let a = offset.max(range.start);
                    let b = add(offset, expected.len())?.min(range.end);
                    if a < b {
                        require(a == covered, "capsule framing coverage")?;
                        framing(a, &expected[a - offset..b - offset])?;
                        covered = b;
                    }
                    Ok(())
                };
                frame(
                    start,
                    &self.geometry.header(
                        b"BORSCP02",
                        self.entries.len(),
                        item.leaf_id,
                        item.page_count,
                    ),
                )?;
                for (index, id) in pages.iter().enumerate() {
                    frame(add(start, add(HEADER, mul(index, 4)?)?)?, &id.to_le_bytes())?;
                }
            }
            let mut offset = add(start, add(HEADER, mul(pages.len(), 4)?)?)?;
            for &id in pages {
                let stop = add(offset, self.geometry.page_bytes(id)?)?;
                if offset < range.end && range.start < stop {
                    require(
                        range.start <= offset && stop <= range.end && covered == offset,
                        "capsule partial page/coverage",
                    )?;
                    page(id, item.leaf_id, offset..stop)?;
                    covered = stop;
                }
                offset = stop;
            }
        }
        require(covered == range.end, "capsule incomplete page coverage")
    }

    /// Choose the lowest selected leaf and trim each original adjacent GET group.
    pub(crate) fn plan_page_ranges<'g>(
        &'g self,
        selected_leaves: &[usize],
        required_pages: &[usize],
        authority: &'g PageAuthority,
        cap: usize,
    ) -> Result<PageRangePlan<'g>> {
        let cap = cap.min(self.geometry.profile.allocation_cap());
        self.geometry.authority(authority)?;
        require(
            authority.generation() == self.generation,
            "capsule authority generation",
        )?;
        require(
            !selected_leaves.is_empty() && selected_leaves.len() <= self.entries.len(),
            "capsule selected leaves",
        )?;
        require(
            !required_pages.is_empty() && required_pages.len() <= self.geometry.pages(),
            "capsule required pages",
        )?;
        let mut charge = Charge {
            source_bytes: mul(self.geometry.rows, self.geometry.width)?,
            encoded_directory_bytes: self.encoded_bytes()?,
            decoded_directory_bytes: self.decoded_bytes()?,
            roster_bytes: [
                size_of::<PageRangePlan<'_>>(),
                mul(selected_leaves.len(), size_of::<usize>())?,
                mul(required_pages.len(), size_of::<PageOwner>())?,
                mul(selected_leaves.len(), size_of::<Range<usize>>())?,
            ]
            .into_iter()
            .try_fold(0, add)?,
            scratch_bytes: SCRATCH,
            ..Charge::default()
        };
        charge.memory(false)?;
        admit(charge.memory_bytes, cap)?;
        let mut selected = reserve(selected_leaves.len())?;
        let mut owners = reserve(required_pages.len())?;
        let mut ranges: Vec<Range<usize>> = reserve(selected_leaves.len())?;
        charge.roster_bytes = [
            size_of::<PageRangePlan<'_>>(),
            mul(selected.capacity(), size_of::<usize>())?,
            mul(owners.capacity(), size_of::<PageOwner>())?,
            mul(ranges.capacity(), size_of::<Range<usize>>())?,
        ]
        .into_iter()
        .try_fold(0, add)?;
        charge.memory(false)?;
        admit(charge.memory_bytes, cap)?;
        selected.extend_from_slice(selected_leaves);
        selected.sort_unstable();
        require(
            selected.iter().all(|&id| id < self.entries.len())
                && selected.windows(2).all(|pair| pair[0] < pair[1]),
            "capsule selected leaf order",
        )?;
        let mut previous = None;
        for &page in required_pages {
            require(
                page < self.geometry.pages() && previous.is_none_or(|p| p < page),
                "capsule required page order",
            )?;
            let page =
                u32::try_from(page).map_err(|_| SourceBuildError::Invalid("capsule page ID"))?;
            let leaf = selected
                .iter()
                .copied()
                .find(|&id| self.rosters[id].binary_search(&page).is_ok())
                .ok_or(SourceBuildError::Invalid("capsule required page missing"))?;
            owners.push(PageOwner {
                page,
                leaf: leaf as u32,
            });
            previous = Some(page as usize);
        }
        let mut first = 0;
        while first < selected.len() {
            let mut stop = first + 1;
            while stop < selected.len()
                && self.byte_range(selected[stop - 1] as u32)?.end
                    == self.byte_range(selected[stop] as u32)?.start
            {
                stop += 1;
            }
            let mut trimmed: Option<Range<usize>> = None;
            for owner in &owners {
                if selected[first] <= owner.leaf as usize
                    && owner.leaf as usize <= selected[stop - 1]
                {
                    let page = self.page_range(owner.leaf, owner.page)?;
                    if let Some(range) = &mut trimmed {
                        range.start = range.start.min(page.start);
                        range.end = range.end.max(page.end);
                    } else {
                        trimmed = Some(page);
                    }
                }
            }
            if let Some(range) = trimmed {
                ranges.push(range);
            }
            first = stop;
        }
        require(!ranges.is_empty(), "capsule empty page ranges")?;
        let mut payload_bytes = 0;
        let mut logical_bytes = 0;
        let mut framing_bytes = 0;
        for range in &ranges {
            charge.physical_bytes = add(charge.physical_bytes, range.len())?;
            self.walk_range(
                range.clone(),
                |_, expected| {
                    framing_bytes = add(framing_bytes, expected.len())?;
                    Ok(())
                },
                |page, leaf, extent| {
                    payload_bytes = add(payload_bytes, extent.len())?;
                    let mut duplicate = false;
                    for &other in &selected {
                        if other >= leaf as usize {
                            break;
                        }
                        if self.rosters[other].binary_search(&page).is_ok() {
                            let copy = self.page_range(other as u32, page)?;
                            if ranges
                                .iter()
                                .any(|r| r.start <= copy.start && copy.end <= r.end)
                            {
                                duplicate = true;
                                break;
                            }
                        }
                    }
                    if !duplicate {
                        logical_bytes = add(logical_bytes, extent.len())?;
                    }
                    Ok(())
                },
            )?;
        }
        require(
            add(payload_bytes, framing_bytes)? == charge.physical_bytes,
            "capsule range accounting",
        )?;
        for owner in &owners {
            let copy = self.page_range(owner.leaf, owner.page)?;
            require(
                ranges
                    .iter()
                    .any(|r| r.start <= copy.start && copy.end <= r.end),
                "capsule owner coverage",
            )?;
        }
        charge.logical_bytes = logical_bytes;
        charge.duplicate_bytes = payload_bytes - logical_bytes;
        charge.framing_bytes = framing_bytes;
        charge.hash_bytes = payload_bytes;
        charge.buffer_bytes = charge.physical_bytes;
        charge.lookup_bytes = add(
            mul(owners.len(), size_of::<PageRef<'_>>())?,
            add(
                size_of::<VerifiedRows<'_>>(),
                mul(ranges.len(), add(SPAN_DESCRIPTOR_BYTES, PROOF_EXTRA_BYTES)?)?,
            )?,
        )?;
        charge.memory(false)?;
        admit(charge.memory_bytes, cap)?;
        Ok(PageRangePlan {
            directory: self,
            authority,
            selected,
            owners,
            ranges,
            charge,
        })
    }
}
/// Owns exactly the immutable generation pins needed by a response authentication job.
pub(crate) struct OwnedSpanAuthentication {
    directory: std::sync::Arc<Directory>,
    authority: std::sync::Arc<PageAuthority>,
    range: Range<usize>,
    charge: Charge,
    cap: usize,
}

/// Only the shared original-unit verifier can construct this token.
pub(crate) struct AuthenticatedSpan {
    binding: OwnedSpanAuthentication,
    bytes: Vec<u8>,
}

fn authenticate_span_bytes(
    directory: &Directory,
    authority: &PageAuthority,
    range: &Range<usize>,
    charge: Charge,
    cap: usize,
    bytes: Vec<u8>,
) -> Result<Vec<u8>> {
    let offset = range.start;
    require(
        bytes.len() == range.len(),
        "capsule planned response length",
    )?;
    let mut charge = charge;
    charge.buffer_bytes = add(charge.buffer_bytes - bytes.len(), bytes.capacity())?;
    charge.memory(false)?;
    admit(
        charge.memory_bytes,
        cap.min(directory.geometry.profile.allocation_cap()),
    )?;
    directory.walk_range(
        range.clone(),
        |start, expected| {
            require(
                bytes.get(start - offset..add(start - offset, expected.len())?) == Some(expected),
                "capsule page framing",
            )
        },
        |page, _, extent| {
            directory.geometry.verify_page(
                page,
                &bytes[extent.start - offset..extent.end - offset],
                authority,
            )
        },
    )?;
    Ok(bytes)
}

impl OwnedSpanAuthentication {
    pub(crate) fn authenticate(self, bytes: Vec<u8>) -> Result<AuthenticatedSpan> {
        let bytes = authenticate_span_bytes(
            &self.directory,
            &self.authority,
            &self.range,
            self.charge,
            self.cap,
            bytes,
        )?;
        Ok(AuthenticatedSpan {
            binding: self,
            bytes,
        })
    }
}

impl<'g> PageRangePlan<'g> {
    pub(crate) fn ranges(&self) -> &[Range<usize>] {
        &self.ranges
    }
    pub(crate) fn charge(&self) -> Charge {
        self.charge
    }

    pub(crate) fn owned_authentication(
        &self,
        directory: std::sync::Arc<Directory>,
        authority: std::sync::Arc<PageAuthority>,
        offset: usize,
        cap: usize,
    ) -> Result<OwnedSpanAuthentication> {
        require(
            std::ptr::eq(self.directory, directory.as_ref())
                && std::ptr::eq(self.authority, authority.as_ref()),
            "capsule authentication pins",
        )?;
        let index = self
            .ranges
            .binary_search_by_key(&offset, |range| range.start)
            .map_err(|_| SourceBuildError::Invalid("capsule unplanned response offset"))?;
        Ok(OwnedSpanAuthentication {
            directory,
            authority,
            range: self.ranges[index].clone(),
            charge: self.charge,
            cap,
        })
    }

    pub(crate) fn bind_authenticated<'p>(
        &'p self,
        token: AuthenticatedSpan,
    ) -> Result<VerifiedPageSpan<'p, 'g>> {
        require(
            std::ptr::eq(self.directory, token.binding.directory.as_ref())
                && std::ptr::eq(self.authority, token.binding.authority.as_ref()),
            "capsule authentication pins",
        )?;
        let range_index = self
            .ranges
            .binary_search_by_key(&token.binding.range.start, |range| range.start)
            .map_err(|_| SourceBuildError::Invalid("capsule unplanned response offset"))?;
        require(
            self.ranges[range_index] == token.binding.range && self.charge == token.binding.charge,
            "capsule authentication range/charge",
        )?;
        Ok(VerifiedPageSpan {
            plan: self,
            range_index,
            bytes: token.bytes,
        })
    }

    pub(crate) fn authenticate_span<'p>(
        &'p self,
        offset: usize,
        bytes: Vec<u8>,
        cap: usize,
    ) -> Result<VerifiedPageSpan<'p, 'g>> {
        let range_index = self
            .ranges
            .binary_search_by_key(&offset, |range| range.start)
            .map_err(|_| SourceBuildError::Invalid("capsule unplanned response offset"))?;
        let bytes = authenticate_span_bytes(
            self.directory,
            self.authority,
            &self.ranges[range_index],
            self.charge,
            cap,
            bytes,
        )?;
        Ok(VerifiedPageSpan {
            plan: self,
            range_index,
            bytes,
        })
    }

    /// Only required chosen copies enter lookup; authenticated bridge pages remain unscored.
    pub(crate) fn verified_rows<'b>(
        &self,
        fetched: &'b [VerifiedPageSpan<'_, 'g>],
        cap: usize,
    ) -> Result<VerifiedRows<'b>> {
        require(
            fetched.len() == self.ranges.len(),
            "capsule proof range coverage",
        )?;
        let mut charge = self.charge;
        charge.buffer_bytes = 0;
        for (index, proof) in fetched.iter().enumerate() {
            require(
                std::ptr::eq(self, proof.plan) && proof.range_index == index,
                "capsule proof plan/range",
            )?;
            require(
                proof.bytes.len() == self.ranges[index].len(),
                "capsule proof length",
            )?;
            charge.buffer_bytes = add(charge.buffer_bytes, proof.bytes.capacity())?;
        }
        let cap = cap.min(self.directory.geometry.profile.allocation_cap());
        charge.memory(false)?;
        admit(charge.memory_bytes, cap)?;
        let mut pages = reserve(self.owners.len())?;
        charge.lookup_bytes = add(
            charge.lookup_bytes,
            mul(
                pages.capacity() - self.owners.len(),
                size_of::<PageRef<'_>>(),
            )?,
        )?;
        charge.memory(false)?;
        admit(charge.memory_bytes, cap)?;
        for owner in &self.owners {
            let extent = self.directory.page_range(owner.leaf, owner.page)?;
            let index = self
                .ranges
                .partition_point(|r| r.start <= extent.start)
                .checked_sub(1)
                .ok_or(SourceBuildError::Invalid("capsule owner response"))?;
            let range = &self.ranges[index];
            require(extent.end <= range.end, "capsule owner response")?;
            let bytes = fetched[index]
                .bytes
                .get(extent.start - range.start..extent.end - range.start)
                .ok_or(SourceBuildError::Invalid("capsule owner payload"))?;
            pages.push(PageRef {
                page: owner.page,
                leaf: owner.leaf,
                bytes,
            });
        }
        Ok(VerifiedRows {
            geometry: self.directory.geometry,
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

    thread_local! {
        pub(super) static HASH_BYTES: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    }

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
    fn fetch_plan<'p, 'g>(
        plan: &'p PageRangePlan<'g>,
        blob: &[u8],
    ) -> Vec<VerifiedPageSpan<'p, 'g>> {
        plan.ranges()
            .iter()
            .map(|range| {
                plan.authenticate_span(range.start, blob[range.clone()].to_vec(), usize::MAX)
                    .unwrap()
            })
            .collect()
    }

    #[test]
    fn page_range_v2_router_equal_rosters() {
        use crate::semantic_unit_router::{self, SourceIdentity};
        let g = geometry(4096, 1);
        let mut centroids = b"BORSUCP1".to_vec();
        centroids.extend_from_slice(&(g.rows as u64).to_le_bytes());
        for word in [g.dimensions as u32, 32, 256, 0] {
            centroids.extend_from_slice(&word.to_le_bytes());
        }
        centroids.resize(HEADER + g.rows.div_ceil(UNIT_ROWS) * g.dimensions * 2, 0);
        let centroid_sha = format!("{:x}", Sha256::digest(&centroids));
        let input = SourceIdentity {
            profile: g.profile,
            schema: "capsule-router-test",
            root_sha256: "1111111111111111111111111111111111111111111111111111111111111111",
            centroids_sha256: &centroid_sha,
            rows: g.rows,
            dimensions: g.dimensions,
        };
        // This fixture needs the router's 12.7 MiB training model, independently of query caps.
        let artifacts = semantic_unit_router::build(&centroids, &input, 16 << 20).unwrap();
        let root_sha = format!("{:x}", Sha256::digest(&artifacts.manifest));
        let router = SemanticUnitRouter::open(
            &artifacts.manifest,
            &artifacts.membership,
            &root_sha,
            &input,
            g.profile.root_cap(),
        )
        .unwrap();
        let mut rosters: Vec<_> = (0..router.manifest().leaves.len())
            .map(|leaf| leaf_pages(&router, leaf, usize::MAX).unwrap())
            .collect();
        assert!(rosters.len() > 1);
        let leaves: Vec<_> = rosters
            .iter()
            .enumerate()
            .map(|(id, pages)| LeafPages {
                leaf_id: id as u32,
                pages,
            })
            .collect();
        let (flat, original) = source(g);
        let (built, _, _) = build(g, &leaves, &flat, &original);
        built
            .directory
            .validate_router(&router, usize::MAX)
            .unwrap();
        let temporary = rosters.iter().map(|pages| pages.len() * 4).max().unwrap();
        assert!(
            built
                .directory
                .validate_router(
                    &router,
                    built.directory.decoded_bytes().unwrap() + temporary - 1
                )
                .is_err()
        );
        drop(leaves);
        let first = rosters[0][0];
        rosters[0][0] = rosters[1][0];
        rosters[1][0] = first;
        rosters[0].sort_unstable();
        rosters[1].sort_unstable();
        let false_leaves: Vec<_> = rosters
            .iter()
            .enumerate()
            .map(|(id, pages)| LeafPages {
                leaf_id: id as u32,
                pages,
            })
            .collect();
        let (false_built, blob, encoded) = build(g, &false_leaves, &flat, &original);
        let false_directory = Directory::open(
            &encoded,
            &false_built.directory_sha256,
            g,
            blob.len(),
            7,
            usize::MAX,
        )
        .unwrap();
        false_directory
            .verify(
                &false_leaves,
                &[FetchedSpan {
                    offset: 0,
                    bytes: &blob,
                }],
                &original,
                usize::MAX,
            )
            .unwrap();
        assert!(matches!(
            false_directory.validate_router(&router, usize::MAX),
            Err(SourceBuildError::Invalid("capsule router roster"))
        ));
    }

    #[test]
    fn owned_span_authentication_preserves_pins_ranges_and_original_unit_checks() {
        use std::sync::Arc;
        let g = geometry(513, 24);
        let (flat, original) = source(g);
        let original = Arc::new(original);
        let leaves = [
            LeafPages {
                leaf_id: 0,
                pages: &[0, 2],
            },
            LeafPages {
                leaf_id: 1,
                pages: &[1],
            },
        ];
        let (built, blob, encoded) = build(g, &leaves, &flat, &original);
        let directory = Arc::new(built.directory);
        let other = Arc::new(
            Directory::open(
                &encoded,
                &built.directory_sha256,
                g,
                blob.len(),
                7,
                usize::MAX,
            )
            .unwrap(),
        );
        let plan = directory
            .plan_page_ranges(&[0, 1], &[0, 1, 2], &original, usize::MAX)
            .unwrap();
        let other_plan = other
            .plan_page_ranges(&[0, 1], &[0, 1, 2], &original, usize::MAX)
            .unwrap();
        for range in plan.ranges() {
            let bytes = blob[range.clone()].to_vec();
            let inline = plan
                .authenticate_span(range.start, bytes.clone(), usize::MAX)
                .unwrap();
            let binding = || {
                plan.owned_authentication(
                    directory.clone(),
                    original.clone(),
                    range.start,
                    usize::MAX,
                )
                .unwrap()
            };
            let owned = plan
                .bind_authenticated(binding().authenticate(bytes.clone()).unwrap())
                .unwrap();
            assert_eq!(inline.bytes(), owned.bytes());
            assert_eq!(inline.start(), owned.start());
            assert!(
                other_plan
                    .bind_authenticated(binding().authenticate(bytes.clone()).unwrap())
                    .is_err()
            );
            let mut corrupt = bytes;
            corrupt[0] ^= 1;
            assert!(binding().authenticate(corrupt).is_err());
            assert!(
                plan.owned_authentication(other.clone(), original.clone(), range.start, usize::MAX)
                    .is_err()
            );
            assert!(
                plan.owned_authentication(directory.clone(), original.clone(), range.start, 0)
                    .unwrap()
                    .authenticate(blob[range.clone()].to_vec())
                    .is_err()
            );
        }
        assert!(
            plan.owned_authentication(directory.clone(), original.clone(), usize::MAX, usize::MAX)
                .is_err()
        );
        assert_eq!(Arc::strong_count(&directory), 1);
        assert_eq!(Arc::strong_count(&original), 1);
    }

    #[test]
    fn page_range_v2_directory_roster() {
        let g = geometry(513, 257);
        let (flat, original) = source(g);
        let leaves = [
            LeafPages {
                leaf_id: 0,
                pages: &[0, 2],
            },
            LeafPages {
                leaf_id: 1,
                pages: &[1],
            },
        ];
        let (built, blob, encoded) = build(g, &leaves, &flat, &original);
        assert_eq!(&encoded[..8], b"BORSDR02");
        assert_eq!(&blob[..8], b"BORSCP02");
        assert_eq!(encoded.len(), HEADER + 2 * ENTRY + 3 * 4);
        let directory = Directory::open(
            &encoded,
            &built.directory_sha256,
            g,
            blob.len(),
            7,
            usize::MAX,
        )
        .unwrap();
        for leaf in leaves {
            assert_eq!(directory.pages(leaf.leaf_id).unwrap(), leaf.pages);
        }
        let tail = directory.page_range(0, 2).unwrap();
        assert_eq!(tail.len(), g.width);
        assert_eq!(&blob[tail], &flat[512 * g.width..]);
        let roster = HEADER + 2 * ENTRY;
        for ids in [[0u32, 0, 1], [2, 0, 1], [0, 3, 1]] {
            let mut bad = encoded.clone();
            for (index, id) in ids.into_iter().enumerate() {
                bad[roster + index * 4..roster + (index + 1) * 4]
                    .copy_from_slice(&id.to_le_bytes());
            }
            assert!(Directory::open(&bad, &hash(&bad), g, blob.len(), 7, usize::MAX).is_err());
        }
        let mut old = encoded.clone();
        old[..8].copy_from_slice(b"BORSDR01");
        assert!(matches!(
            Directory::open(&old, &hash(&old), g, blob.len(), 7, usize::MAX),
            Err(SourceBuildError::Invalid(
                "capsule directory format: expected BORSDR02"
            ))
        ));
        assert!(Directory::open_charge(g, 2, 1).is_err());
        assert!(Directory::open_charge(g, 1, g.rows.div_ceil(UNIT_ROWS) + 1).is_err());
        assert!(Directory::open_charge(g, usize::MAX, usize::MAX).is_err());
        assert!(Geometry::new(1, 0, SemanticProfile::Scale1m).is_err());
        assert!(Geometry::new(1, 1025, SemanticProfile::Scale1m).is_err());
        let wrong_generation =
            Directory::open(&encoded, &hash(&encoded), g, blob.len(), 8, usize::MAX).unwrap();
        assert!(
            wrong_generation
                .plan_page_ranges(&[0], &[0], &original, usize::MAX)
                .is_err()
        );
    }

    #[test]
    fn page_range_v2_stable_owner_trim_hashes_and_fetched_corruption() {
        for dimensions in [1, 255, 256, 257, 1024] {
            let g = geometry(832, dimensions);
            let (flat, original) = source(g);
            let leaves = [
                LeafPages {
                    leaf_id: 0,
                    pages: &[0, 2, 3],
                },
                LeafPages {
                    leaf_id: 1,
                    pages: &[0, 1, 2],
                },
                LeafPages {
                    leaf_id: 2,
                    pages: &[1, 3],
                },
            ];
            let (built, blob, _) = build(g, &leaves, &flat, &original);
            let directory = &built.directory;
            let full_charge = directory.query_charge(&leaves).unwrap();
            assert_eq!(full_charge.physical_bytes, 1664 * g.width + 128);
            reset_hash_bytes();
            let full_rows = directory
                .verify(
                    &leaves,
                    &[FetchedSpan {
                        offset: 0,
                        bytes: &blob,
                    }],
                    &original,
                    usize::MAX,
                )
                .unwrap();
            assert_eq!(hash_bytes(), 3328 * g.width + 128);
            let plan = directory
                .plan_page_ranges(&[2, 0, 1], &[0, 1, 2, 3], &original, usize::MAX)
                .unwrap();
            assert_eq!(plan.ranges().len(), 1);
            assert_eq!(plan.ranges()[0].start, 44);
            let charge = plan.charge();
            assert_eq!(charge.physical_bytes, 1088 * g.width + 44);
            assert_eq!(charge.logical_bytes, 832 * g.width);
            assert_eq!(charge.duplicate_bytes, 256 * g.width);
            assert_eq!(charge.framing_bytes, 44);
            assert_eq!(charge.hash_bytes, 1088 * g.width);
            reset_hash_bytes();
            let proofs = fetch_plan(&plan, &blob);
            assert_eq!(hash_bytes(), charge.hash_bytes);
            let rows = plan.verified_rows(&proofs, usize::MAX).unwrap();
            assert_eq!(rows.charge(), charge);
            for row in 0..g.rows {
                assert_eq!(rows.record(row), full_rows.record(row));
                assert_eq!(
                    rows.record(row),
                    Some(&flat[row * g.width..(row + 1) * g.width])
                );
            }
            assert_eq!(rows.page_leaf(0), Some(0));
            assert_eq!(rows.page_leaf(1), Some(1));
            assert_eq!(rows.page_leaf(2), Some(0));
            assert_eq!(rows.page_leaf(3), Some(0));
            assert_eq!(hash_bytes(), charge.hash_bytes);
            let response = &plan.ranges()[0];
            let duplicate = directory.page_range(1, 0).unwrap().start;
            let header = directory.entries()[1].offset as usize;
            let selected = directory.page_range(0, 0).unwrap().start;
            for offset in [
                duplicate,
                header,
                header + HEADER,
                selected,
                response.end - 1,
            ] {
                let mut corrupt = blob[response.clone()].to_vec();
                corrupt[offset - response.start] ^= 1;
                assert!(
                    plan.authenticate_span(response.start, corrupt, usize::MAX)
                        .is_err(),
                    "{offset}"
                );
            }
        }
    }

    #[test]
    fn page_range_v2_drop_empty_group_and_hide_bridge() {
        let g = geometry(513, 1);
        let (flat, original) = source(g);
        let leaves = [
            LeafPages {
                leaf_id: 0,
                pages: &[0, 1, 2],
            },
            LeafPages {
                leaf_id: 1,
                pages: &[0, 1],
            },
            LeafPages {
                leaf_id: 2,
                pages: &[0, 1],
            },
        ];
        let (built, blob, _) = build(g, &leaves, &flat, &original);
        let directory = &built.directory;
        let full = directory.query_charge(&[leaves[0], leaves[2]]).unwrap();
        assert_eq!(full.physical_bytes, 1025 * g.width + 84);
        let plan = directory
            .plan_page_ranges(&[0, 2], &[0, 1, 2], &original, usize::MAX)
            .unwrap();
        assert_eq!(plan.ranges().len(), 1);
        assert_eq!(plan.charge().physical_bytes, 513 * g.width);
        assert_eq!(plan.charge().hash_bytes, 513 * g.width);
        assert_eq!(plan.charge().framing_bytes, 0);
        assert_eq!(plan.charge().duplicate_bytes, 0);
        assert!(plan.ranges()[0].end <= directory.byte_range(0).unwrap().end as usize);
        let later = directory
            .plan_page_ranges(&[2, 1], &[0, 1], &original, usize::MAX)
            .unwrap();
        let later_proofs = fetch_plan(&later, &blob);
        let later_rows = later.verified_rows(&later_proofs, usize::MAX).unwrap();
        assert_eq!(later_rows.page_leaf(0), Some(1));
        assert_eq!(later_rows.page_leaf(1), Some(1));
        let bridge = directory
            .plan_page_ranges(&[0], &[0, 2], &original, usize::MAX)
            .unwrap();
        let proofs = fetch_plan(&bridge, &blob);
        let rows = bridge.verified_rows(&proofs, usize::MAX).unwrap();
        assert_eq!(rows.page_leaf(1), None);
        assert!(rows.record(256).is_none());
        assert!(rows.record(511).is_none());
        assert_eq!(rows.record(512), Some(&flat[512 * g.width..]));
        assert_eq!(rows.charge().logical_bytes, 513 * g.width);
        let extent = &bridge.ranges()[0];
        let mut corrupt = blob[extent.clone()].to_vec();
        corrupt[directory.page_range(0, 1).unwrap().start - extent.start] ^= 1;
        assert!(
            bridge
                .authenticate_span(extent.start, corrupt, usize::MAX)
                .is_err()
        );
    }

    #[test]
    fn page_range_v2_private_proof_context() {
        let g = geometry(513, 1);
        let (flat, original) = source(g);
        let leaves = [
            LeafPages {
                leaf_id: 0,
                pages: &[0],
            },
            LeafPages {
                leaf_id: 1,
                pages: &[1],
            },
            LeafPages {
                leaf_id: 2,
                pages: &[2],
            },
        ];
        let (built, blob, encoded) = build(g, &leaves, &flat, &original);
        let directory = &built.directory;
        let plan = directory
            .plan_page_ranges(&[0, 2], &[0, 2], &original, usize::MAX)
            .unwrap();
        assert_eq!(plan.ranges().len(), 2);
        let mut extra = fetch_plan(&plan, &blob);
        extra.push(
            plan.authenticate_span(
                plan.ranges()[0].start,
                blob[plan.ranges()[0].clone()].to_vec(),
                usize::MAX,
            )
            .unwrap(),
        );
        assert!(plan.verified_rows(&extra, usize::MAX).is_err());
        let mut proofs = fetch_plan(&plan, &blob);
        assert!(plan.verified_rows(&proofs, usize::MAX).is_ok());
        assert!(plan.verified_rows(&proofs[..1], usize::MAX).is_err());
        proofs.swap(0, 1);
        assert!(plan.verified_rows(&proofs, usize::MAX).is_err());
        proofs.swap(0, 1);
        let duplicate = plan
            .authenticate_span(
                plan.ranges()[0].start,
                blob[plan.ranges()[0].clone()].to_vec(),
                usize::MAX,
            )
            .unwrap();
        let duplicates = [proofs.remove(0), duplicate];
        assert!(plan.verified_rows(&duplicates, usize::MAX).is_err());
        let proofs = fetch_plan(&plan, &blob);
        let other =
            Directory::open(&encoded, &hash(&encoded), g, blob.len(), 7, usize::MAX).unwrap();
        let other_plan = other
            .plan_page_ranges(&[0, 2], &[0, 2], &original, usize::MAX)
            .unwrap();
        assert!(other_plan.verified_rows(&proofs, usize::MAX).is_err());
        let equivalent_plan = directory
            .plan_page_ranges(&[0, 2], &[0, 2], &original, usize::MAX)
            .unwrap();
        assert!(equivalent_plan.verified_rows(&proofs, usize::MAX).is_err());
        let other_authority = authority(g, &flat, hash(&flat));
        let other_plan = directory
            .plan_page_ranges(&[0, 2], &[0, 2], &other_authority, usize::MAX)
            .unwrap();
        assert!(other_plan.verified_rows(&proofs, usize::MAX).is_err());
        let other_selection = directory
            .plan_page_ranges(&[0], &[0], &original, usize::MAX)
            .unwrap();
        assert!(other_selection.verified_rows(&proofs, usize::MAX).is_err());
        let range = &plan.ranges()[0];
        let body = blob[range.clone()].to_vec();
        for (offset, bytes) in [
            (range.start + 1, body.clone()),
            (range.start, body[..body.len() - 1].to_vec()),
            (range.start, [body.clone(), vec![0]].concat()),
            (usize::MAX, body),
        ] {
            assert!(plan.authenticate_span(offset, bytes, usize::MAX).is_err());
        }
        assert!(
            directory
                .plan_page_ranges(&[], &[0], &original, usize::MAX)
                .is_err()
        );
        assert!(
            directory
                .plan_page_ranges(&[0], &[], &original, usize::MAX)
                .is_err()
        );
        assert!(
            directory
                .plan_page_ranges(&[0, 0], &[0], &original, usize::MAX)
                .is_err()
        );
        assert!(
            directory
                .plan_page_ranges(&[0], &[0, 0], &original, usize::MAX)
                .is_err()
        );
        assert!(
            directory
                .plan_page_ranges(&[0], &[2, 0], &original, usize::MAX)
                .is_err()
        );
        assert!(
            directory
                .plan_page_ranges(&[0], &[1], &original, usize::MAX)
                .is_err()
        );
        assert!(
            directory
                .plan_page_ranges(&[3], &[0], &original, usize::MAX)
                .is_err()
        );
    }

    #[test]
    fn page_range_v2_modeled_minus_one_and_capacities() {
        let g = geometry(513, 257);
        let (flat, original) = source(g);
        let leaves = [LeafPages {
            leaf_id: 0,
            pages: &[0, 1, 2],
        }];
        let (mut built, blob, _) = build(g, &leaves, &flat, &original);
        let before = built.directory.decoded_bytes().unwrap();
        built.directory.entries.reserve(8);
        built.directory.rosters.reserve(8);
        built.directory.rosters[0].reserve(8);
        let directory = &built.directory;
        let decoded = directory.decoded_bytes().unwrap();
        assert!(decoded > before);
        assert_eq!(
            decoded,
            size_of::<Directory>()
                + directory.entries.capacity() * size_of::<DirectoryEntry>()
                + directory.rosters.capacity() * size_of::<Vec<u32>>()
                + directory
                    .rosters
                    .iter()
                    .map(|pages| pages.capacity() * 4)
                    .sum::<usize>()
        );
        let plan = directory
            .plan_page_ranges(&[0], &[0, 2], &original, usize::MAX)
            .unwrap();
        let model = plan.charge();
        assert_eq!(model.decoded_directory_bytes, decoded);
        assert_eq!(
            model.roster_bytes,
            size_of::<PageRangePlan<'_>>()
                + plan.selected.capacity() * size_of::<usize>()
                + plan.owners.capacity() * size_of::<PageOwner>()
                + plan.ranges.capacity() * size_of::<Range<usize>>()
        );
        assert!(
            directory
                .plan_page_ranges(&[0], &[0, 2], &original, model.memory_bytes - 1)
                .is_err()
        );
        assert!(
            directory
                .plan_page_ranges(&[0], &[0, 2], &original, model.memory_bytes)
                .is_ok()
        );
        let range = &plan.ranges()[0];
        let mut body = blob[range.clone()].to_vec();
        body.reserve(4096);
        let actual = model.memory_bytes - range.len() + body.capacity();
        assert!(
            plan.authenticate_span(range.start, body.clone(), model.memory_bytes - 1)
                .is_err()
        );
        let mut excess = blob[range.clone()].to_vec();
        excess.reserve(4096);
        let excess_cap = model.memory_bytes - range.len() + excess.capacity();
        assert!(
            plan.authenticate_span(range.start, excess, excess_cap - 1)
                .is_err()
        );
        let pointer = body.as_ptr();
        reset_hash_bytes();
        let proof = plan.authenticate_span(range.start, body, actual).unwrap();
        assert_eq!(proof.bytes().as_ptr(), pointer);
        assert_eq!(proof.start(), range.start);
        let proofs = [proof];
        assert!(plan.verified_rows(&proofs, actual - 1).is_err());
        let rows = plan.verified_rows(&proofs, actual).unwrap();
        assert_eq!(rows.charge().memory_bytes, actual);
        assert_eq!(hash_bytes(), model.hash_bytes);
        assert_eq!(rows.record(0), Some(&flat[..g.width]));
        assert!(rows.record(256).is_none());
    }

    #[test]
    fn page_range_v2_full_approval_rejects_unfetched_corruption() {
        let g = geometry(513, 1);
        let (flat, original) = source(g);
        let leaves = [LeafPages {
            leaf_id: 0,
            pages: &[0, 1, 2],
        }];
        let (built, blob, _) = build(g, &leaves, &flat, &original);
        let directory = built.directory;
        let mut original_encoded = Vec::new();
        let original_sha = directory.write_to(&mut original_encoded).unwrap();
        for offset in [0, directory.page_range(0, 2).unwrap().start] {
            let mut corrupt = blob.clone();
            corrupt[offset] ^= 1;
            let mut encoded = Vec::new();
            let mut rehashed = Directory::open(
                &original_encoded,
                &original_sha,
                g,
                blob.len(),
                7,
                usize::MAX,
            )
            .unwrap();
            rehashed.entries[0].sha256 = hash(&corrupt);
            encoded.clear();
            let sha = rehashed.write_to(&mut encoded).unwrap();
            let approved_directory =
                Directory::open(&encoded, &sha, g, blob.len(), 7, usize::MAX).unwrap();
            let plan = approved_directory
                .plan_page_ranges(&[0], &[0], &original, usize::MAX)
                .unwrap();
            let proofs = fetch_plan(&plan, &corrupt);
            assert!(plan.verified_rows(&proofs, usize::MAX).is_ok());
            assert!(
                approved_directory
                    .verify_whole_leaf(
                        0,
                        FetchedSpan {
                            offset: 0,
                            bytes: &corrupt
                        },
                        &original,
                        usize::MAX
                    )
                    .is_err()
            );
        }
        let full = directory
            .verify_whole_leaf(
                0,
                FetchedSpan {
                    offset: 0,
                    bytes: &blob,
                },
                &original,
                usize::MAX,
            )
            .unwrap();
        let mut expected = directory.whole_leaf_charge(0).unwrap();
        expected.buffer_bytes = blob.capacity();
        expected.memory(false).unwrap();
        assert_eq!(full, expected);
        assert_eq!(full.hash_bytes, blob.len() + flat.len());
    }

    #[test]
    fn page_range_v2_dimensions_and_tails() {
        for dimensions in [1, 255, 256, 257, 1024] {
            for rows in [1, 31, 32, 33, 255, 256, 257, 511, 512, 513, 832] {
                let g = geometry(rows, dimensions);
                let (flat, original) = source(g);
                let pages: Vec<u32> = (0..g.pages() as u32).collect();
                let required: Vec<usize> = (0..g.pages()).collect();
                let leaves = [LeafPages {
                    leaf_id: 0,
                    pages: &pages,
                }];
                let (built, blob, _) = build(g, &leaves, &flat, &original);
                let plan = built
                    .directory
                    .plan_page_ranges(&[0], &required, &original, usize::MAX)
                    .unwrap();
                assert_eq!(plan.ranges().len(), 1);
                assert_eq!(plan.charge().physical_bytes, flat.len());
                assert_eq!(plan.charge().framing_bytes, 0);
                reset_hash_bytes();
                let proofs = fetch_plan(&plan, &blob);
                let verified = plan.verified_rows(&proofs, usize::MAX).unwrap();
                assert_eq!(hash_bytes(), flat.len());
                for row in 0..rows {
                    assert_eq!(
                        verified.record(row),
                        Some(&flat[row * g.width..(row + 1) * g.width])
                    );
                }
                assert!(verified.record(rows).is_none());
                assert!(verified.record(usize::MAX).is_none());
            }
        }
    }

    fn rehash_copy(directory: &mut Directory, fetched: &[(u32, Vec<u8>)], leaf: u32) {
        directory.entries[leaf as usize].sha256 =
            hash(&fetched.iter().find(|(id, _)| *id == leaf).unwrap().1);
    }

    #[test]
    fn dimensions_tails_overlap_stable_copies_and_exact_records() {
        for dimensions in [1, 255, 256, 257, 1024] {
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
                    7,
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
        for dimensions in [1, 255, 256, 257, 1024] {
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
                Directory::open(&encoded, &directory_sha256, g, blob.len(), 7, usize::MAX).unwrap();
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
        assert!(Directory::open(&encoded, &[0; 32], g, blob.len(), 7, usize::MAX).is_err());
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
                Directory::open(&bad, &hash(&bad), g, blob.len(), 7, usize::MAX).is_err(),
                "{at}"
            );
        }
        for at in [40, 48, 56, HEADER + ENTRY + 8] {
            let mut bad = encoded.clone();
            bad[at..at + 8].copy_from_slice(&u64::MAX.to_le_bytes());
            assert!(Directory::open(&bad, &hash(&bad), g, blob.len(), 7, usize::MAX).is_err());
        }
        let mut overlap = encoded.clone();
        overlap[HEADER + ENTRY + 8..HEADER + ENTRY + 16].fill(0);
        assert!(Directory::open(&overlap, &hash(&overlap), g, blob.len(), 7, usize::MAX).is_err());
        for bytes in [&encoded[..HEADER - 1], &encoded[..encoded.len() - 1]] {
            assert!(Directory::open(bytes, &hash(bytes), g, blob.len(), 7, usize::MAX).is_err());
        }
        let mut trailing = encoded.clone();
        trailing.push(0);
        assert!(
            Directory::open(&trailing, &hash(&trailing), g, blob.len(), 7, usize::MAX).is_err()
        );
        assert!(
            Directory::open(&encoded, &hash(&encoded), g, blob.len() + 1, 7, usize::MAX).is_err()
        );
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
        let open = Directory::open_charge(
            g,
            leaves.len(),
            leaves.iter().map(|leaf| leaf.pages.len()).sum(),
        )
        .unwrap();
        assert!(
            Directory::open(
                &encoded,
                &hash(&encoded),
                g,
                blob.len(),
                7,
                open.memory_bytes - 1
            )
            .is_err()
        );
        Directory::open(
            &encoded,
            &hash(&encoded),
            g,
            blob.len(),
            7,
            open.memory_bytes,
        )
        .unwrap();
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
