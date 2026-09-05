use crate::{BorsukError, Result, V34Rank4Generation, score_v34_rank4_leaf};

const V34_DIMENSIONS: usize = 96;
const V34_MAX_GROUPS: u32 = 64;
const V34_MAX_ROWS: u64 = 262_144;
const V34_MAX_CODE_BYTES: u64 = 8 * 1_048_576;

fn invalid(message: &str) -> BorsukError {
    BorsukError::InvalidStorage(message.to_owned())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Immutable row and encoded-byte authority for one dense storage group.
pub struct V34GroupStorage {
    group_ordinal: u32,
    rows: u64,
    code_bytes: u64,
}

impl V34GroupStorage {
    /// Construct one nonempty dense group identity.
    pub fn new(group_ordinal: u32, rows: u64, code_bytes: u64) -> Result<Self> {
        if rows == 0 || code_bytes == 0 {
            return Err(invalid("V34 group storage authority differs"));
        }
        Ok(Self {
            group_ordinal,
            rows,
            code_bytes,
        })
    }

    /// Dense group ordinal.
    pub fn group_ordinal(self) -> u32 {
        self.group_ordinal
    }

    /// Logical rows stored by this group.
    pub fn rows(self) -> u64 {
        self.rows
    }

    /// Exact encoded code-object bytes stored by this group.
    pub fn code_bytes(self) -> u64 {
        self.code_bytes
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Fixed complete-prefix serving limits for one V34 route.
pub struct V34RouteBudget {
    max_groups: u32,
    max_rows: u64,
    max_code_bytes: u64,
}

impl V34RouteBudget {
    /// Construct limits no wider than the frozen V34 serving envelope.
    pub fn new(max_groups: u32, max_rows: u64, max_code_bytes: u64) -> Result<Self> {
        if max_groups == 0
            || max_groups > V34_MAX_GROUPS
            || max_rows == 0
            || max_rows > V34_MAX_ROWS
            || max_code_bytes == 0
            || max_code_bytes > V34_MAX_CODE_BYTES
        {
            return Err(invalid("V34 route budget differs"));
        }
        Ok(Self {
            max_groups,
            max_rows,
            max_code_bytes,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
/// One group ordered by its exact minimum rank-four leaf score.
pub struct V34SelectedGroup {
    storage: V34GroupStorage,
    score: f64,
}

impl V34SelectedGroup {
    /// Dense group ordinal.
    pub fn group_ordinal(self) -> u32 {
        self.storage.group_ordinal
    }

    /// Exact minimum score among leaves owned by this group.
    pub fn score(self) -> f64 {
        self.score
    }

    /// Logical rows admitted with this complete group.
    pub fn rows(self) -> u64 {
        self.storage.rows
    }

    /// Encoded code-object bytes admitted with this complete group.
    pub fn code_bytes(self) -> u64 {
        self.storage.code_bytes
    }
}

#[derive(Debug, Clone, PartialEq)]
/// Exact complete route prefix and the first group that did not fit.
pub struct V34RoutePrefix {
    selected_groups: Vec<V34SelectedGroup>,
    overflow: Option<V34SelectedGroup>,
    selected_rows: u64,
    selected_code_bytes: u64,
}

impl V34RoutePrefix {
    /// Admitted groups in exact score order.
    pub fn selected_groups(&self) -> &[V34SelectedGroup] {
        &self.selected_groups
    }

    /// First group rejected by any prefix budget, without considering later groups.
    pub fn overflow(&self) -> Option<&V34SelectedGroup> {
        self.overflow.as_ref()
    }

    /// Checked sum of admitted logical rows.
    pub fn selected_rows(&self) -> u64 {
        self.selected_rows
    }

    /// Checked sum of admitted encoded code-object bytes.
    pub fn selected_code_bytes(&self) -> u64 {
        self.selected_code_bytes
    }
}

/// Score every leaf once and admit the exact complete group prefix.
pub fn exhaustive_v34_route(
    generation: &V34Rank4Generation,
    query: &[f32; V34_DIMENSIONS],
    groups: &[V34GroupStorage],
    budget: V34RouteBudget,
) -> Result<V34RoutePrefix> {
    if query.iter().any(|value| !value.is_finite())
        || groups.len() != generation.group_count() as usize
    {
        return Err(invalid("V34 route authority differs"));
    }
    let mut expected_rows = vec![0_u64; groups.len()];
    for leaf in generation.leaves() {
        let slot = expected_rows
            .get_mut(leaf.group_ordinal() as usize)
            .ok_or_else(|| invalid("V34 route group ordinal differs"))?;
        *slot = slot
            .checked_add(u64::from(leaf.population()))
            .ok_or_else(|| invalid("V34 route group rows overflow"))?;
    }
    for (ordinal, (group, expected)) in groups.iter().zip(expected_rows).enumerate() {
        if group.group_ordinal != ordinal as u32 || group.rows != expected {
            return Err(invalid("V34 route group storage differs"));
        }
    }

    let mut minima = vec![f64::INFINITY; groups.len()];
    for leaf in generation.leaves() {
        let score = score_v34_rank4_leaf(leaf, query)?;
        let minimum = &mut minima[leaf.group_ordinal() as usize];
        if score.total_cmp(minimum).is_lt() {
            *minimum = score;
        }
    }
    let mut ordered = groups
        .iter()
        .copied()
        .zip(minima)
        .map(|(storage, score)| V34SelectedGroup { storage, score })
        .collect::<Vec<_>>();
    if ordered.iter().any(|group| !group.score.is_finite()) {
        return Err(invalid("V34 route group minimum differs"));
    }
    ordered.sort_by(|left, right| {
        left.score
            .total_cmp(&right.score)
            .then_with(|| left.storage.group_ordinal.cmp(&right.storage.group_ordinal))
    });

    let mut selected_groups = Vec::new();
    let mut selected_rows = 0_u64;
    let mut selected_code_bytes = 0_u64;
    let mut overflow = None;
    for group in ordered {
        let next_rows = selected_rows.checked_add(group.storage.rows);
        let next_code_bytes = selected_code_bytes.checked_add(group.storage.code_bytes);
        if selected_groups.len() >= budget.max_groups as usize
            || next_rows.is_none_or(|rows| rows > budget.max_rows)
            || next_code_bytes.is_none_or(|bytes| bytes > budget.max_code_bytes)
        {
            overflow = Some(group);
            break;
        }
        selected_rows = next_rows.expect("checked above");
        selected_code_bytes = next_code_bytes.expect("checked above");
        selected_groups.push(group);
    }
    Ok(V34RoutePrefix {
        selected_groups,
        overflow,
        selected_rows,
        selected_code_bytes,
    })
}

#[cfg(test)]
mod tests {
    use super::{V34GroupStorage, V34RouteBudget, exhaustive_v34_route};
    use crate::{V34Rank4LeafInput, build_v34_rank4_generation};

    const DIMENSIONS: usize = 96;
    const MIB: u64 = 1_048_576;

    fn leaf(
        leaf_ordinal: u32,
        group_ordinal: u32,
        logical_start: u64,
        mean: f32,
    ) -> V34Rank4LeafInput {
        let mut center = [0.0; DIMENSIONS];
        center[0] = mean;
        V34Rank4LeafInput {
            leaf_ordinal,
            group_ordinal,
            logical_start,
            population: 1,
            mean: center,
            residual_diagonal: [0.0; DIMENSIONS],
            eigenvalues: [0.0; 4],
            directions: [[0.0; DIMENSIONS]; 4],
        }
    }

    fn fixture() -> (crate::V34Rank4Generation, Vec<V34GroupStorage>) {
        let generation = build_v34_rank4_generation(vec![
            leaf(0, 0, 0, 3.0),
            leaf(1, 1, 1, 1.0),
            leaf(2, 0, 2, 0.5),
            leaf(3, 2, 3, 2.0),
        ])
        .unwrap();
        let groups = vec![
            V34GroupStorage::new(0, 2, 100).unwrap(),
            V34GroupStorage::new(1, 1, 200).unwrap(),
            V34GroupStorage::new(2, 1, 300).unwrap(),
        ];
        (generation, groups)
    }

    #[test]
    fn v34_route_exhaustive_uses_one_minimum_per_group_and_stable_ties() {
        // Break caught: duplicate leaves overwrite rather than minimize, or
        // equal scores are ordered by discovery instead of group ordinal.
        let (generation, groups) = fixture();
        let query = [0.0; DIMENSIONS];
        let route = exhaustive_v34_route(
            &generation,
            &query,
            &groups,
            V34RouteBudget::new(3, 4, 600).unwrap(),
        )
        .unwrap();
        assert_eq!(
            route
                .selected_groups()
                .iter()
                .map(|group| (group.group_ordinal(), group.score().to_bits()))
                .collect::<Vec<_>>(),
            vec![
                (0, 0.25_f64.to_bits()),
                (1, 1.0_f64.to_bits()),
                (2, 4.0_f64.to_bits())
            ]
        );
        assert_eq!(route.selected_rows(), 4);
        assert_eq!(route.selected_code_bytes(), 600);
        assert!(route.overflow().is_none());

        let tied =
            build_v34_rank4_generation(vec![leaf(0, 1, 0, 1.0), leaf(1, 0, 1, -1.0)]).unwrap();
        let tied_groups = vec![
            V34GroupStorage::new(0, 1, 10).unwrap(),
            V34GroupStorage::new(1, 1, 10).unwrap(),
        ];
        let route = exhaustive_v34_route(
            &tied,
            &query,
            &tied_groups,
            V34RouteBudget::new(2, 2, 20).unwrap(),
        )
        .unwrap();
        assert_eq!(
            route
                .selected_groups()
                .iter()
                .map(|group| group.group_ordinal())
                .collect::<Vec<_>>(),
            vec![0, 1]
        );
    }

    #[test]
    fn v34_route_exhaustive_stops_at_first_overflow_without_skipping() {
        // Break caught: a row- or byte-heavy group is skipped so a later group
        // can fit, violating exact complete-prefix authority.
        let (generation, groups) = fixture();
        let query = [0.0; DIMENSIONS];
        for budget in [
            V34RouteBudget::new(1, 4, 600).unwrap(),
            V34RouteBudget::new(3, 2, 600).unwrap(),
            V34RouteBudget::new(3, 4, 250).unwrap(),
            V34RouteBudget::new(2, 2, 250).unwrap(),
        ] {
            let route = exhaustive_v34_route(&generation, &query, &groups, budget).unwrap();
            assert_eq!(route.selected_groups().len(), 1);
            assert_eq!(route.selected_groups()[0].group_ordinal(), 0);
            assert_eq!(route.overflow().unwrap().group_ordinal(), 1);
            assert_eq!(route.selected_rows(), 2);
            assert_eq!(route.selected_code_bytes(), 100);
        }
    }

    #[test]
    fn v34_route_exhaustive_rejects_invalid_authority_and_checked_limits() {
        // Break caught: malformed query/group authority or arithmetic overflow
        // reaches scoring/admission, or serving caps are silently widened.
        let (generation, groups) = fixture();
        assert!(V34RouteBudget::new(0, 1, 1).is_err());
        assert!(V34RouteBudget::new(65, 262_144, 8 * MIB).is_err());
        assert!(V34RouteBudget::new(64, 262_145, 8 * MIB).is_err());
        assert!(V34RouteBudget::new(64, 262_144, 8 * MIB + 1).is_err());

        let mut nonfinite = [0.0; DIMENSIONS];
        nonfinite[7] = f32::NAN;
        assert!(
            exhaustive_v34_route(
                &generation,
                &nonfinite,
                &groups,
                V34RouteBudget::new(3, 4, 600).unwrap(),
            )
            .is_err()
        );
        assert!(
            exhaustive_v34_route(
                &generation,
                &[0.0; DIMENSIONS],
                &groups[..2],
                V34RouteBudget::new(3, 4, 600).unwrap(),
            )
            .is_err()
        );
        let wrong_rows = vec![
            V34GroupStorage::new(0, 1, 100).unwrap(),
            V34GroupStorage::new(1, 1, 200).unwrap(),
            V34GroupStorage::new(2, 1, 300).unwrap(),
        ];
        assert!(
            exhaustive_v34_route(
                &generation,
                &[0.0; DIMENSIONS],
                &wrong_rows,
                V34RouteBudget::new(3, 4, 600).unwrap(),
            )
            .is_err()
        );
    }
}
