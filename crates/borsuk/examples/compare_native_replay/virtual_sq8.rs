//! INTENTIONAL MISSING-FEATURE RED SCAFFOLD. Not a candidate authenticated reader.
#![allow(dead_code)]
use super::unit_layout::UnitLayout;
use borsuk::sq8_page_authority::PageAuthority;
use std::io::{Read, Seek, SeekFrom};

struct VirtualSq8 {
    original_hash: String,
}
impl VirtualSq8 {
    fn prepare<R: Read + Seek>(
        _: &mut R,
        authority: &PageAuthority,
        _: UnitLayout,
        _: usize,
    ) -> Result<Self, &'static str> {
        Ok(Self {
            original_hash: authority.object_sha256().to_owned(),
        })
    }
    fn object_sha256(&self) -> &str {
        &self.original_hash
    }
    fn read_range<R: Read + Seek>(
        &self,
        input: &mut R,
        range: std::ops::Range<usize>,
        _: usize,
    ) -> Result<Vec<u8>, &'static str> {
        input
            .seek(SeekFrom::Start(range.start as u64))
            .map_err(|_| "scaffold seek")?;
        let mut bytes = vec![0; range.end - range.start];
        input.read_exact(&mut bytes).map_err(|_| "scaffold read")?;
        Ok(bytes)
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
}
