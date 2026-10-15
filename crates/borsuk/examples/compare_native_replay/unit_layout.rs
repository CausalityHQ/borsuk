//! Test-first source for the explicit-coordinate virtual placement component.
//! Implementation deliberately absent until the remote failing-test checkpoint.
#![forbid(unsafe_code)]

#[cfg(test)]
mod tests {
    use super::{SourceRow, Sq8Row, UnitLayout};

    #[test]
    fn unit_layout_inverse_uses_the_opposite_direction_of_a_three_cycle() {
        let layout = UnitLayout::from_destination_order(97, 32, &[2, 0, 1, 3], 1 << 20).unwrap();
        assert_eq!(layout.source_row(Sq8Row(0)), Some(SourceRow(64)));
        assert_eq!(layout.source_row(Sq8Row(32)), Some(SourceRow(0)));
        assert_eq!(layout.source_row(Sq8Row(64)), Some(SourceRow(32)));
    }

    #[test]
    fn unit_layout_three_cycle_roundtrips_full_units_and_partial_tail() {
        let cap = UnitLayout::required_bytes(97, 32).unwrap();
        let layout = UnitLayout::from_destination_order(97, 32, &[2, 0, 1, 3], cap).unwrap();
        assert_eq!(layout.sq8_row(SourceRow(0)), Some(Sq8Row(32)));
        assert_eq!(layout.sq8_row(SourceRow(32)), Some(Sq8Row(64)));
        assert_eq!(layout.sq8_row(SourceRow(64)), Some(Sq8Row(0)));
        assert_eq!(layout.sq8_row(SourceRow(96)), Some(Sq8Row(96)));
        for row in 0..97 {
            let moved = layout.sq8_row(SourceRow(row)).unwrap();
            assert_eq!(layout.source_row(moved), Some(SourceRow(row)));
            let original = layout.source_row(Sq8Row(row)).unwrap();
            assert_eq!(layout.sq8_row(original), Some(Sq8Row(row)));
        }
        assert_eq!(layout.sq8_row(SourceRow(97)), None);
        assert_eq!(layout.source_row(Sq8Row(97)), None);
        // A complete final unit is free to move; a partial one is not.
        assert!(UnitLayout::from_destination_order(128, 32, &[3, 0, 1, 2], 1 << 20).is_ok());
        assert!(UnitLayout::from_destination_order(97, 32, &[3, 0, 1, 2], 1 << 20).is_err());
    }

    #[test]
    fn unit_layout_identity_preserves_every_row_and_exact_payload_boundary() {
        for (rows, unit_rows) in [(1_usize, 1_usize), (257, 32), (513, 256)] {
            let order = (0..rows.div_ceil(unit_rows))
                .map(|u| u as u32)
                .collect::<Vec<_>>();
            let cap = UnitLayout::required_bytes(rows, unit_rows).unwrap();
            assert!(UnitLayout::from_destination_order(rows, unit_rows, &order, cap - 1).is_err());
            let layout = UnitLayout::from_destination_order(rows, unit_rows, &order, cap).unwrap();
            for row in 0..rows {
                assert_eq!(layout.sq8_row(SourceRow(row)), Some(Sq8Row(row)));
                assert_eq!(layout.source_row(Sq8Row(row)), Some(SourceRow(row)));
            }
        }
    }

    #[test]
    fn unit_layout_refuses_malformed_permutations_and_unsupported_geometry() {
        for order in [
            vec![],
            vec![0, 1, 2],
            vec![0, 1, 1, 3],
            vec![0, 1, 4, 3],
            vec![0, 1, 3, 2],
        ] {
            assert!(
                UnitLayout::from_destination_order(97, 32, &order, 1 << 20).is_err(),
                "{order:?}"
            );
        }
        assert!(UnitLayout::required_bytes(0, 32).is_err());
        assert!(UnitLayout::required_bytes(1, 0).is_err());
        assert!(UnitLayout::required_bytes(usize::MAX, 1).is_err());
    }

    #[test]
    fn unit_layout_virtual_ranges_preserve_destination_sequence_and_tail_bytes() {
        let layout = UnitLayout::from_destination_order(97, 32, &[2, 0, 1, 3], 1 << 20).unwrap();
        // D2 SQ8 records: signed ID8 + norm4 + codes2. No D1024 assumption.
        assert_eq!(
            layout.source_segments(0..896, 14, 1 << 20).unwrap(),
            vec![896..1344, 0..448]
        );
        assert_eq!(
            layout.source_segments(1344..1358, 14, 1 << 20).unwrap(),
            vec![1344..1358]
        );
        // Check a second width without interpreting or changing the payload.
        assert_eq!(
            layout.source_segments(0..49920, 780, 1 << 20).unwrap(),
            vec![49920..74880, 0..24960]
        );
        for range in [0..0, 1..448, 0..447, 0..1372, 1358..1372] {
            assert!(layout.source_segments(range, 14, 1 << 20).is_err());
        }
        assert!(layout.source_segments(0..896, 0, 1 << 20).is_err());
        assert!(layout.source_segments(0..896, usize::MAX, 1 << 20).is_err());
        assert!(layout.source_segments(0..896, 14, 0).is_err());
    }
}
// Missing-feature scaffold for the remote RED checkpoint; not a candidate implementation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct SourceRow(usize);
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Sq8Row(usize);
struct UnitLayout {
    rows: usize,
}
impl UnitLayout {
    fn required_bytes(rows: usize, unit_rows: usize) -> Result<usize, &'static str> {
        if rows == 0 || unit_rows == 0 {
            return Err("geometry");
        }
        let units = rows.div_ceil(unit_rows);
        if units > u32::MAX as usize {
            return Err("unit count");
        }
        units
            .checked_mul(8)
            .and_then(|n| n.checked_add(4096))
            .ok_or("payload overflow")
    }
    fn from_destination_order(
        rows: usize,
        unit_rows: usize,
        _order: &[u32],
        cap: usize,
    ) -> Result<Self, &'static str> {
        if Self::required_bytes(rows, unit_rows)? > cap {
            return Err("payload");
        }
        Ok(Self { rows })
    }
    fn sq8_row(&self, row: SourceRow) -> Option<Sq8Row> {
        (row.0 < self.rows).then_some(Sq8Row(row.0))
    }
    fn source_row(&self, row: Sq8Row) -> Option<SourceRow> {
        (row.0 < self.rows).then_some(SourceRow(row.0))
    }
    fn source_segments(
        &self,
        range: std::ops::Range<usize>,
        _record_bytes: usize,
        _cap: usize,
    ) -> Result<Vec<std::ops::Range<usize>>, &'static str> {
        Ok(std::iter::once(range).collect())
    }
}
