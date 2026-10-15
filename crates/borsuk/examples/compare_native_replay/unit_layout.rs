//! Test-first source for the explicit-coordinate virtual placement component.
//! Minimal coordinate implementation after the verified remote RED checkpoint.
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct SourceRow(pub(crate) usize);
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Sq8Row(pub(crate) usize);

/// Coordinate translation only. The enclosing descriptor authenticates the order.
/// The payload model excludes caller-owned input and allocator/RSS overhead.
pub(crate) struct UnitLayout {
    rows: usize,
    unit_rows: usize,
    source_to_sq8: Vec<u32>,
    sq8_to_source: Vec<u32>,
}
impl UnitLayout {
    pub(crate) fn geometry(&self) -> (usize, usize) {
        (self.rows, self.unit_rows)
    }
    pub(crate) fn owned_payload_bytes(&self) -> Result<usize, &'static str> {
        self.source_to_sq8
            .capacity()
            .checked_add(self.sq8_to_source.capacity())
            .and_then(|n| n.checked_mul(std::mem::size_of::<u32>()))
            .and_then(|n| n.checked_add(4096))
            .ok_or("unit layout retained overflow")
    }
    pub(crate) fn required_bytes(rows: usize, unit_rows: usize) -> Result<usize, &'static str> {
        if rows == 0 || unit_rows == 0 {
            return Err("unit layout geometry");
        }
        let units = rows.div_ceil(unit_rows);
        if units > u32::MAX as usize {
            return Err("unit layout count width");
        }
        units
            .checked_mul(2 * std::mem::size_of::<u32>())
            .and_then(|n| n.checked_add(4096))
            .ok_or("unit layout payload overflow")
    }

    /// `order[destination_unit]` is the original SOURCE unit at that position.
    pub(crate) fn from_destination_order(
        rows: usize,
        unit_rows: usize,
        order: &[u32],
        cap: usize,
    ) -> Result<Self, &'static str> {
        let modeled = Self::required_bytes(rows, unit_rows)?;
        let units = rows.div_ceil(unit_rows);
        if modeled > cap {
            return Err("unit layout payload");
        }
        if order.len() != units
            || order.iter().any(|&unit| unit as usize >= units)
            || (rows % unit_rows != 0 && order.last().copied() != Some((units - 1) as u32))
        {
            return Err("unit layout permutation geometry");
        }
        let mut source_to_sq8 = Vec::new();
        source_to_sq8
            .try_reserve_exact(units)
            .map_err(|_| "unit layout allocation")?;
        source_to_sq8.resize(units, u32::MAX);
        for (destination, &source) in order.iter().enumerate() {
            let slot = &mut source_to_sq8[source as usize];
            if *slot != u32::MAX {
                return Err("unit layout duplicate unit");
            }
            *slot = destination as u32;
        }
        let mut sq8_to_source = Vec::new();
        sq8_to_source
            .try_reserve_exact(units)
            .map_err(|_| "unit layout allocation")?;
        sq8_to_source.extend_from_slice(order);
        let retained = source_to_sq8
            .capacity()
            .checked_add(sq8_to_source.capacity())
            .and_then(|n| n.checked_mul(std::mem::size_of::<u32>()))
            .and_then(|n| n.checked_add(4096))
            .ok_or("unit layout capacity overflow")?;
        if retained > cap {
            return Err("unit layout capacity");
        }
        Ok(Self {
            rows,
            unit_rows,
            source_to_sq8,
            sq8_to_source,
        })
    }

    fn translate(&self, row: usize, units: &[u32]) -> Option<usize> {
        if row >= self.rows {
            return None;
        }
        let unit = *units.get(row / self.unit_rows)? as usize;
        let mapped = unit
            .checked_mul(self.unit_rows)?
            .checked_add(row % self.unit_rows)?;
        (mapped < self.rows).then_some(mapped)
    }
    pub(crate) fn sq8_row(&self, row: SourceRow) -> Option<Sq8Row> {
        self.translate(row.0, &self.source_to_sq8).map(Sq8Row)
    }
    pub(crate) fn source_row(&self, row: Sq8Row) -> Option<SourceRow> {
        self.translate(row.0, &self.sq8_to_source).map(SourceRow)
    }

    /// Original byte segments in destination sequence, one per intact unit.
    /// Callers must authenticate each segment before scoring the assembled range.
    pub(crate) fn source_segments(
        &self,
        range: std::ops::Range<usize>,
        record_bytes: usize,
        cap: usize,
    ) -> Result<Vec<std::ops::Range<usize>>, &'static str> {
        let total = self
            .rows
            .checked_mul(record_bytes)
            .ok_or("unit layout byte overflow")?;
        let unit_bytes = self
            .unit_rows
            .checked_mul(record_bytes)
            .ok_or("unit layout stride overflow")?;
        if record_bytes == 0
            || range.start >= range.end
            || range.end > total
            || range.start % unit_bytes != 0
            || (range.end % unit_bytes != 0 && range.end != total)
        {
            return Err("unit layout range geometry");
        }
        let first = range.start / unit_bytes;
        let last = (range.end - 1) / unit_bytes;
        let count = last
            .checked_sub(first)
            .and_then(|n| n.checked_add(1))
            .ok_or("unit layout segment count")?;
        let modeled = count
            .checked_mul(std::mem::size_of::<std::ops::Range<usize>>())
            .and_then(|n| n.checked_add(4096))
            .ok_or("unit layout segment payload overflow")?;
        if modeled > cap {
            return Err("unit layout segment payload");
        }
        let mut segments = Vec::new();
        segments
            .try_reserve_exact(count)
            .map_err(|_| "unit layout segment allocation")?;
        for destination in first..=last {
            let source = *self
                .sq8_to_source
                .get(destination)
                .ok_or("unit layout destination unit")? as usize;
            let source_row = source
                .checked_mul(self.unit_rows)
                .ok_or("unit layout row overflow")?;
            let source_rows = self.unit_rows.min(self.rows - source_row);
            let start = source_row
                .checked_mul(record_bytes)
                .ok_or("unit layout offset overflow")?;
            let bytes = source_rows
                .checked_mul(record_bytes)
                .ok_or("unit layout size overflow")?;
            let destination_start = destination
                .checked_mul(unit_bytes)
                .ok_or("unit layout destination offset")?;
            if bytes != unit_bytes.min(range.end - destination_start) {
                return Err("unit layout unequal unit lengths");
            }
            segments.push(start..start.checked_add(bytes).ok_or("unit layout end overflow")?);
        }
        let retained = segments
            .capacity()
            .checked_mul(std::mem::size_of::<std::ops::Range<usize>>())
            .and_then(|n| n.checked_add(4096))
            .ok_or("unit layout segment capacity overflow")?;
        if retained > cap {
            return Err("unit layout segment capacity");
        }
        Ok(segments)
    }
}
