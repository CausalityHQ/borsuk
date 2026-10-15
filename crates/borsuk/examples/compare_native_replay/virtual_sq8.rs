//! Authenticated virtual SQ8 placement for the offline locality falsifier.
//! Original generation and SOURCE coordinates remain unchanged.
#![allow(dead_code)]
use super::unit_layout::{Sq8Row, UnitLayout};
use borsuk::sq8_page_authority::PageAuthority;
use sha2::{Digest, Sha256};
use std::io::{Read, Seek, SeekFrom};
use std::ops::Range;

type Result<T> = std::result::Result<T, &'static str>;
const FIXED: usize = 16 * 1024;

pub(crate) struct VirtualSq8 {
    layout: UnitLayout,
    original_units: Vec<[u8; 32]>,
    authority: PageAuthority,
    unit_bytes: usize,
    row_bytes: usize,
    retained: usize,
}
fn sum(values: &[usize]) -> Result<usize> {
    values.iter().try_fold(0_usize, |n, &v| {
        n.checked_add(v).ok_or("virtual payload overflow")
    })
}
fn product(a: usize, b: usize) -> Result<usize> {
    a.checked_mul(b).ok_or("virtual geometry overflow")
}
fn buffer(bytes: usize) -> Result<Vec<u8>> {
    let mut v = Vec::new();
    v.try_reserve_exact(bytes)
        .map_err(|_| "virtual allocation")?;
    v.resize(bytes, 0);
    Ok(v)
}
fn read_at<R: Read + Seek>(input: &mut R, start: usize, bytes: &mut [u8]) -> Result<()> {
    input
        .seek(SeekFrom::Start(
            u64::try_from(start).map_err(|_| "virtual offset width")?,
        ))
        .map_err(|_| "virtual seek")?;
    input.read_exact(bytes).map_err(|_| "virtual read")
}
fn exact_length<R: Read + Seek>(input: &mut R, expected: usize) -> Result<()> {
    if input.seek(SeekFrom::End(0)).map_err(|_| "virtual length")?
        != u64::try_from(expected).map_err(|_| "virtual length width")?
    {
        return Err("virtual original length");
    }
    Ok(())
}
impl VirtualSq8 {
    pub(crate) fn prepare<R: Read + Seek>(
        input: &mut R,
        old: &PageAuthority,
        layout: UnitLayout,
        cap: usize,
    ) -> Result<Self> {
        let (rows, unit_rows) = layout.geometry();
        if rows != old.rows() || old.page_rows() % unit_rows != 0 {
            return Err("virtual authority/layout geometry");
        }
        let row_bytes = old
            .dimensions()
            .checked_add(12)
            .ok_or("virtual row width")?;
        let unit_bytes = product(unit_rows, row_bytes)?;
        let page_bytes = product(old.page_rows(), row_bytes)?;
        if old.object_bytes() != product(rows, row_bytes)? {
            return Err("virtual SQ8 authority width");
        }
        let units = rows.div_ceil(unit_rows);
        let table_bytes = product(units, 32)?;
        let maps = layout.owned_payload_bytes()?;
        // During PageAuthority::load: old unit proofs + target sidecar + copied
        // target proofs coexist. Caller retains/funds the old authority/input.
        let required = sum(&[
            maps,
            product(table_bytes, 3)?,
            page_bytes,
            unit_bytes,
            FIXED,
        ])?;
        if required > cap {
            return Err("virtual prepare payload");
        }
        exact_length(input, old.object_bytes())?;
        let mut page = buffer(page_bytes)?;
        let mut original_units = Vec::<[u8; 32]>::new();
        original_units
            .try_reserve_exact(units)
            .map_err(|_| "virtual proof allocation")?;
        let actual_table = product(original_units.capacity(), 32)?;
        if sum(&[
            maps,
            actual_table,
            product(table_bytes, 2)?,
            page.capacity(),
            unit_bytes,
            FIXED,
        ])? > cap
        {
            return Err("virtual prepare capacity");
        }
        let mut whole = Sha256::new();
        for p in 0..rows.div_ceil(old.page_rows()) {
            let range = old.byte_range(p, p).map_err(|_| "virtual original page")?;
            let body = &mut page[..range.len()];
            read_at(input, range.start, body)?;
            old.verify_payload(p, p, body)
                .map_err(|_| "virtual original proof")?;
            whole.update(&*body);
            for unit in body.chunks(unit_bytes) {
                original_units.push(Sha256::digest(unit).into());
            }
        }
        if original_units.len() != units || format!("{:x}", whole.finalize()) != old.object_sha256()
        {
            return Err("virtual original whole identity");
        }
        let mut sidecar = Vec::new();
        sidecar
            .try_reserve_exact(table_bytes)
            .map_err(|_| "virtual target proof allocation")?;
        let mut unit = buffer(unit_bytes)?;
        if sum(&[
            maps,
            actual_table,
            sidecar.capacity(),
            table_bytes,
            page.capacity(),
            unit.capacity(),
            FIXED,
        ])? > cap
        {
            return Err("virtual target capacity");
        }
        let mut target_hash = Sha256::new();
        for destination in 0..units {
            let row = layout
                .source_row(Sq8Row(product(destination, unit_rows)?))
                .ok_or("virtual inverse row")?
                .0;
            let start = product(row, row_bytes)?;
            let n = product(unit_rows.min(rows - row), row_bytes)?;
            read_at(input, start, &mut unit[..n])?;
            let digest: [u8; 32] = Sha256::digest(&unit[..n]).into();
            if digest != original_units[row / unit_rows] {
                return Err("virtual source changed during prepare");
            }
            target_hash.update(&unit[..n]);
            sidecar.extend_from_slice(&digest);
        }
        let manifest = serde_json::to_vec(&serde_json::json!({
            "schema":"borsuk-v115-sq8-page-authority-v2", "generation":old.generation(),
            "rows":rows,"dimensions":old.dimensions(),"page_rows":unit_rows,
            "object_sha256":format!("{:x}",target_hash.finalize()),
            "page_digest_sha256":format!("{:x}",Sha256::digest(&sidecar))
        }))
        .map_err(|_| "virtual target manifest")?;
        if manifest.len() > 4096 {
            return Err("virtual target manifest size");
        }
        let authority = PageAuthority::load(
            &manifest,
            &format!("{:x}", Sha256::digest(&manifest)),
            &sidecar,
        )
        .map_err(|_| "virtual target authority")?;
        exact_length(input, old.object_bytes())?;
        let target_proofs = authority
            .proof_payload_bytes()
            .ok_or("virtual target proof capacity overflow")?;
        // Library allocation has completed: reconcile its actual capacity while
        // all preparation buffers and the temporary sidecar are still live.
        if sum(&[
            maps,
            actual_table,
            sidecar.capacity(),
            target_proofs,
            page.capacity(),
            unit.capacity(),
            FIXED,
        ])? > cap
        {
            return Err("virtual loaded authority capacity");
        }
        let retained = sum(&[maps, actual_table, target_proofs, FIXED])?;

        Ok(Self {
            layout,
            original_units,
            authority,
            unit_bytes,
            row_bytes,
            retained,
        })
    }
    pub(crate) fn object_sha256(&self) -> &str {
        self.authority.object_sha256()
    }
    pub(crate) fn authority(&self) -> &PageAuthority {
        &self.authority
    }
    pub(crate) fn layout(&self) -> &UnitLayout {
        &self.layout
    }
    pub(crate) fn retained_payload_bytes(&self) -> usize {
        self.retained
    }
    pub(crate) fn read_range<R: Read + Seek>(
        &self,
        input: &mut R,
        range: Range<usize>,
        cap: usize,
    ) -> Result<Vec<u8>> {
        if range.start >= range.end
            || range.end > self.authority.object_bytes()
            || range.start % self.unit_bytes != 0
            || (range.end % self.unit_bytes != 0 && range.end != self.authority.object_bytes())
        {
            return Err("virtual range geometry");
        }
        let count = (range.end - 1) / self.unit_bytes - range.start / self.unit_bytes + 1;
        let segment_bytes = sum(&[product(count, std::mem::size_of::<Range<usize>>())?, 4096])?;
        if sum(&[self.retained, range.len(), segment_bytes])? > cap {
            return Err("virtual range payload");
        }
        exact_length(input, self.authority.object_bytes())?;
        let segments = self
            .layout
            .source_segments(range.clone(), self.row_bytes, segment_bytes)?;
        let mut output = buffer(range.len())?;
        if sum(&[
            self.retained,
            output.capacity(),
            product(segments.capacity(), std::mem::size_of::<Range<usize>>())?,
            4096,
        ])? > cap
        {
            return Err("virtual range capacity");
        }
        let mut offset = 0;
        for segment in &segments {
            let stop = offset
                .checked_add(segment.len())
                .ok_or("virtual assembly overflow")?;
            let body = output
                .get_mut(offset..stop)
                .ok_or("virtual assembly length")?;
            read_at(input, segment.start, body)?;
            let digest: [u8; 32] = Sha256::digest(&*body).into();
            if self.original_units.get(segment.start / self.unit_bytes) != Some(&digest) {
                return Err("virtual source changed after prepare");
            }
            offset = stop;
        }
        if offset != output.len() {
            return Err("virtual assembly incomplete");
        }
        self.authority
            .verify_payload(
                range.start / self.unit_bytes,
                (range.end - 1) / self.unit_bytes,
                &output,
            )
            .map_err(|_| "virtual target proof")?;
        exact_length(input, self.authority.object_bytes())?;
        Ok(output)
    }
}
// Fixtures for the next authenticated virtual reader; not yet wired or executed.
// Mutation target: using destination offsets to read original bytes must fail
// the non-self-inverse cycle; accepting unverified bytes must fail corruption.
#[cfg(test)]
mod tests {
    use super::*;
    use borsuk::sq8_page_authority::PageAuthority;
    use sha2::{Digest, Sha256};
    use std::io::Cursor;

    fn fixture(dimensions: usize) -> (Vec<u8>, PageAuthority) {
        let mut bytes = Vec::new();
        for row in 0..385_usize {
            // Signed, nonordinal IDs; repeated codes deliberately retain ties.
            bytes.extend_from_slice(&((row as i64 * 17) - 4000).to_le_bytes());
            bytes.extend_from_slice(&1.0_f32.to_le_bytes());
            bytes.extend((0..dimensions).map(|d| ((row % 7 + d) % 255) as u8));
        }
        let mut proofs = Vec::new();
        for page in bytes.chunks(256 * (dimensions + 12)) {
            proofs.extend_from_slice(&Sha256::digest(page));
        }
        let manifest = serde_json::to_vec(&serde_json::json!({
            "schema":"borsuk-v115-sq8-page-authority-v2", "generation":1,
            "rows":385, "dimensions":dimensions, "page_rows":256,
            "object_sha256":format!("{:x}", Sha256::digest(&bytes)),
            "page_digest_sha256":format!("{:x}", Sha256::digest(&proofs))
        }))
        .unwrap();
        let hash = format!("{:x}", Sha256::digest(&manifest));
        let authority = PageAuthority::load(&manifest, &hash, &proofs).unwrap();
        (bytes, authority)
    }

    #[test]
    fn virtual_sq8_three_cycle_preserves_authenticated_record_bits_across_proof_pages() {
        for dimensions in [2, 17] {
            let (bytes, authority) = fixture(dimensions);
            let width = dimensions + 12;
            let layout =
                UnitLayout::from_destination_order(385, 128, &[2, 0, 1, 3], 1 << 20).unwrap();
            let mut old = Cursor::new(bytes.clone());
            let prepared = VirtualSq8::prepare(&mut old, &authority, layout, 1 << 20).unwrap();
            let mut expected = Vec::new();
            for unit in [2, 0, 1, 3] {
                expected.extend_from_slice(
                    &bytes[unit * 128 * width..((unit + 1) * 128).min(385) * width],
                );
            }
            assert_eq!(
                prepared.object_sha256(),
                format!("{:x}", Sha256::digest(&expected))
            );
            assert_eq!(
                prepared
                    .read_range(&mut old, 0..385 * width, 1 << 20)
                    .unwrap(),
                expected
            );
            assert_eq!(
                prepared
                    .read_range(&mut old, 384 * width..385 * width, 1 << 20)
                    .unwrap(),
                bytes[384 * width..]
            );
            assert!(
                prepared
                    .read_range(&mut old, 1..128 * width, 1 << 20)
                    .is_err()
            );
            assert!(
                prepared
                    .read_range(&mut old, 0..128 * width, 128 * width - 1)
                    .is_err()
            );
        }
    }

    #[test]
    fn virtual_sq8_rejects_source_corruption_before_prepare() {
        let (mut bytes, authority) = fixture(2);
        bytes[256 * 14 + 9] ^= 1;
        let layout = UnitLayout::from_destination_order(385, 128, &[2, 0, 1, 3], 1 << 20).unwrap();
        assert!(VirtualSq8::prepare(&mut Cursor::new(bytes), &authority, layout, 1 << 20).is_err());
    }

    #[test]
    fn virtual_sq8_rejects_source_replacement_after_prepare() {
        let (bytes, authority) = fixture(2);
        let layout = UnitLayout::from_destination_order(385, 128, &[2, 0, 1, 3], 1 << 20).unwrap();
        let mut old = Cursor::new(bytes);
        let prepared = VirtualSq8::prepare(&mut old, &authority, layout, 1 << 20).unwrap();
        old.get_mut()[256 * 14 + 9] ^= 1;
        assert!(prepared.read_range(&mut old, 0..128 * 14, 1 << 20).is_err());
    }
    // Append inside the virtual reader test module after its identity/byte RED gate.
    // Not part of frozen aed0ed82 or the live experiment; not compiled/executed.
    #[test]
    fn virtual_sq8_relocation_preserves_native_rank_ids_score_bits_and_mapped_ordinals() {
        use borsuk::exact_sq8_nominee::Sq8Geometry;
        use borsuk::returned_sq8::{ReturnedRange, rank_returned_ranges};
        for dimensions in [2, 17] {
            let (bytes, authority) = fixture(dimensions);
            let layout =
                UnitLayout::from_destination_order(385, 128, &[2, 0, 1, 3], 1 << 20).unwrap();
            let mut old = Cursor::new(bytes.clone());
            let prepared = VirtualSq8::prepare(&mut old, &authority, layout, 1 << 20).unwrap();
            let moved = prepared
                .read_range(&mut old, 0..bytes.len(), 1 << 20)
                .unwrap();
            for query in [vec![0.0; dimensions], vec![0.125; dimensions]] {
                let geometry = Sq8Geometry {
                    rows: 385,
                    dimensions,
                };
                let low = vec![0.0; dimensions];
                let step = vec![1.0; dimensions];
                let baseline = rank_returned_ranges(
                    geometry,
                    &[ReturnedRange {
                        start: 0,
                        bytes: &bytes,
                    }],
                    &query,
                    &low,
                    &step,
                    385,
                    bytes.len(),
                )
                .unwrap();
                let candidate = rank_returned_ranges(
                    geometry,
                    &[ReturnedRange {
                        start: 0,
                        bytes: &moved,
                    }],
                    &query,
                    &low,
                    &step,
                    385,
                    bytes.len(),
                )
                .unwrap();
                assert_eq!(
                    baseline
                        .iter()
                        .map(|v| (v.id, v.score.to_bits()))
                        .collect::<Vec<_>>(),
                    candidate
                        .iter()
                        .map(|v| (v.id, v.score.to_bits()))
                        .collect::<Vec<_>>()
                );
                for (before, after) in baseline.iter().zip(&candidate) {
                    // Independent literal three-cycle forward oracle, not a call
                    // back into the coordinate implementation under test.
                    let source_unit = before.ordinal / 128;
                    let target_unit = [1, 2, 0, 3][source_unit];
                    assert_eq!(after.ordinal, target_unit * 128 + before.ordinal % 128);
                }
            }
        }
    }
    // Additional source-review falsifiers; not in frozen07/current Green run.
    // Existing prepare/read behavior is exercised; no mocked authentication/scoring.
    #[test]
    fn virtual_sq8_exact_prepare_and_query_payload_boundaries_precede_io() {
        struct Observed {
            body: Cursor<Vec<u8>>,
            reads: usize,
            seeks: usize,
        }
        impl std::io::Read for Observed {
            fn read(&mut self, b: &mut [u8]) -> std::io::Result<usize> {
                self.reads += 1;
                self.body.read(b)
            }
        }
        impl std::io::Seek for Observed {
            fn seek(&mut self, p: std::io::SeekFrom) -> std::io::Result<u64> {
                self.seeks += 1;
                self.body.seek(p)
            }
        }
        let (bytes, authority) = fixture(2);
        let mut old = Observed {
            body: Cursor::new(bytes),
            reads: 0,
            seeks: 0,
        };
        // Independent literal modeled bytes for four units: maps8Q+4096,
        // three32Q proof tables, old256-row page, one128-row unit, 16KiB scratch.
        let prepare_cap = 8 * 4 + 4096 + 3 * 32 * 4 + 256 * 14 + 128 * 14 + 16384;
        let layout = UnitLayout::from_destination_order(385, 128, &[2, 0, 1, 3], 1 << 20).unwrap();
        assert!(VirtualSq8::prepare(&mut old, &authority, layout, prepare_cap - 1).is_err());
        assert_eq!((old.reads, old.seeks), (0, 0));
        let layout = UnitLayout::from_destination_order(385, 128, &[2, 0, 1, 3], 1 << 20).unwrap();
        let prepared = VirtualSq8::prepare(&mut old, &authority, layout, prepare_cap).unwrap();
        let expected_retained = 8 * 4 + 4096 + 2 * 32 * 4 + 16384;
        assert_eq!(prepared.retained_payload_bytes(), expected_retained);
        let range_cap =
            expected_retained + 385 * 14 + 4 * std::mem::size_of::<std::ops::Range<usize>>() + 4096;
        old.reads = 0;
        old.seeks = 0;
        assert!(
            prepared
                .read_range(&mut old, 0..385 * 14, range_cap - 1)
                .is_err()
        );
        assert_eq!((old.reads, old.seeks), (0, 0));
        assert_eq!(
            prepared
                .read_range(&mut old, 0..385 * 14, range_cap)
                .unwrap()
                .len(),
            385 * 14
        );
    }

    #[test]
    fn virtual_sq8_rejects_mutation_between_authenticated_passes() {
        struct Replace {
            body: Cursor<Vec<u8>>,
            first_pass_read: usize,
            mutated: bool,
        }
        impl std::io::Read for Replace {
            fn read(&mut self, b: &mut [u8]) -> std::io::Result<usize> {
                let n = self.body.read(b)?;
                self.first_pass_read += n;
                Ok(n)
            }
        }
        impl std::io::Seek for Replace {
            fn seek(&mut self, p: std::io::SeekFrom) -> std::io::Result<u64> {
                if self.first_pass_read >= 385 * 14 && !self.mutated {
                    self.body.get_mut()[256 * 14 + 9] ^= 1;
                    self.mutated = true;
                }
                self.body.seek(p)
            }
        }
        let (bytes, authority) = fixture(2);
        let mut old = Replace {
            body: Cursor::new(bytes),
            first_pass_read: 0,
            mutated: false,
        };
        let layout = UnitLayout::from_destination_order(385, 128, &[2, 0, 1, 3], 1 << 20).unwrap();
        assert!(VirtualSq8::prepare(&mut old, &authority, layout, 1 << 20).is_err());
        assert!(old.mutated);
    }

    #[test]
    fn virtual_sq8_refuses_wrong_geometry_and_trailing_original_bytes() {
        let (bytes, authority) = fixture(2);
        let layout = UnitLayout::from_destination_order(384, 128, &[2, 0, 1], 1 << 20).unwrap();
        assert!(
            VirtualSq8::prepare(&mut Cursor::new(bytes.clone()), &authority, layout, 1 << 20)
                .is_err()
        );
        let order = (0..385_usize.div_ceil(3))
            .map(|u| u as u32)
            .collect::<Vec<_>>();
        let layout = UnitLayout::from_destination_order(385, 3, &order, 1 << 20).unwrap();
        assert!(
            VirtualSq8::prepare(&mut Cursor::new(bytes.clone()), &authority, layout, 1 << 20)
                .is_err()
        );
        let mut extra = bytes;
        extra.push(0);
        let layout = UnitLayout::from_destination_order(385, 128, &[2, 0, 1, 3], 1 << 20).unwrap();
        assert!(VirtualSq8::prepare(&mut Cursor::new(extra), &authority, layout, 1 << 20).is_err());
    }

    #[test]
    fn virtual_sq8_refuses_wrong_whole_hash_even_with_valid_page_proofs_and_refuses_truncation() {
        let (bytes, _) = fixture(2);
        let mut sidecar = Vec::new();
        for page in bytes.chunks(256 * 14) {
            sidecar.extend_from_slice(&Sha256::digest(page));
        }
        let manifest = serde_json::to_vec(&serde_json::json!({
            "schema":"borsuk-v115-sq8-page-authority-v2","generation":1,
            "rows":385,"dimensions":2,"page_rows":256,
            "object_sha256":"0".repeat(64),
            "page_digest_sha256":format!("{:x}",Sha256::digest(&sidecar))
        }))
        .unwrap();
        let wrong = PageAuthority::load(
            &manifest,
            &format!("{:x}", Sha256::digest(&manifest)),
            &sidecar,
        )
        .unwrap();
        // Show page proofs are valid; rejection must come from whole-object binding.
        wrong.verify_payload(0, 1, &bytes).unwrap();
        let layout = UnitLayout::from_destination_order(385, 128, &[2, 0, 1, 3], 1 << 20).unwrap();
        assert_eq!(
            VirtualSq8::prepare(&mut Cursor::new(bytes.clone()), &wrong, layout, 1 << 20).err(),
            Some("virtual original whole identity")
        );
        let (_, authority) = fixture(2);
        let layout = UnitLayout::from_destination_order(385, 128, &[2, 0, 1, 3], 1 << 20).unwrap();
        assert_eq!(
            VirtualSq8::prepare(
                &mut Cursor::new(bytes[..bytes.len() - 1].to_vec()),
                &authority,
                layout,
                1 << 20
            )
            .err(),
            Some("virtual original length")
        );
    }

    #[test]
    fn virtual_sq8_disjoint_nonzero_start_ranges_preserve_native_scores() {
        use borsuk::exact_sq8_nominee::Sq8Geometry;
        use borsuk::returned_sq8::{ReturnedRange, rank_returned_ranges};
        for dimensions in [2, 17] {
            let (bytes, authority) = fixture(dimensions);
            let width = dimensions + 12;
            let layout =
                UnitLayout::from_destination_order(385, 128, &[2, 0, 1, 3], 1 << 20).unwrap();
            let mut old = Cursor::new(bytes.clone());
            let prepared = VirtualSq8::prepare(&mut old, &authority, layout, 1 << 20).unwrap();
            let first = prepared
                .read_range(&mut old, 128 * width..256 * width, 1 << 20)
                .unwrap();
            let tail = prepared
                .read_range(&mut old, 384 * width..385 * width, 1 << 20)
                .unwrap();
            let geometry = Sq8Geometry {
                rows: 385,
                dimensions,
            };
            let query = vec![0.125; dimensions];
            let low = vec![0.0; dimensions];
            let step = vec![1.0; dimensions];
            let reference = rank_returned_ranges(
                geometry,
                &[
                    ReturnedRange {
                        start: 0,
                        bytes: &bytes[..128 * width],
                    },
                    ReturnedRange {
                        start: 384 * width,
                        bytes: &bytes[384 * width..],
                    },
                ],
                &query,
                &low,
                &step,
                129,
                129 * width,
            )
            .unwrap();
            let candidate = rank_returned_ranges(
                geometry,
                &[
                    ReturnedRange {
                        start: 128 * width,
                        bytes: &first,
                    },
                    ReturnedRange {
                        start: 384 * width,
                        bytes: &tail,
                    },
                ],
                &query,
                &low,
                &step,
                129,
                129 * width,
            )
            .unwrap();
            assert_eq!(
                reference
                    .iter()
                    .map(|v| (v.id, v.score.to_bits()))
                    .collect::<Vec<_>>(),
                candidate
                    .iter()
                    .map(|v| (v.id, v.score.to_bits()))
                    .collect::<Vec<_>>()
            );
            for (a, b) in reference.iter().zip(&candidate) {
                assert_eq!(
                    b.ordinal,
                    [1, 2, 0, 3][a.ordinal / 128] * 128 + a.ordinal % 128
                );
            }
        }
    }
}
