//! Deterministic physical SQ8 page admission under an explicit I/O budget.

use std::collections::{BTreeSet, HashSet};
use std::ops::Range;

const PAGE_ROWS: usize = 256;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BudgetedPageError {
    InvalidGeometry,
    InvalidScore,
    InvalidCandidate,
    InvalidPrimary,
    ArithmeticOverflow,
    InsufficientBudget,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BudgetedPagePlan {
    pub selected_pages: Vec<usize>,
    pub ranges: Vec<Range<usize>>,
    pub planned_bytes: usize,
    pub primary_pages_retained: usize,
    pub target_pages: usize,
    pub target_shortfall: usize,
}

fn cover_pages(
    selected: &BTreeSet<usize>,
    rows: usize,
    row_bytes: usize,
    max_gets: usize,
) -> Result<(Vec<Range<usize>>, usize), BudgetedPageError> {
    if selected.is_empty() || max_gets == 0 {
        return Err(BudgetedPageError::InvalidGeometry);
    }
    let mut runs = Vec::<Range<usize>>::new();
    for &page in selected {
        if let Some(last) = runs.last_mut()
            && last.end == page
        {
            last.end += 1;
            continue;
        }
        runs.push(page..page + 1);
    }
    let bridges = runs.len().saturating_sub(max_gets);
    let mut gaps = (0..runs.len().saturating_sub(1))
        .map(|index| (runs[index + 1].start - runs[index].end, index))
        .collect::<Vec<_>>();
    gaps.sort_unstable();
    let joined = gaps
        .into_iter()
        .take(bridges)
        .map(|(_, index)| index)
        .collect::<HashSet<_>>();
    let mut merged = vec![runs[0].clone()];
    for (index, run) in runs.into_iter().enumerate().skip(1) {
        if joined.contains(&(index - 1)) {
            merged.last_mut().expect("nonempty merged runs").end = run.end;
        } else {
            merged.push(run);
        }
    }
    let mut ranges = Vec::with_capacity(merged.len());
    let mut charged = 0usize;
    for run in merged {
        let first = run
            .start
            .checked_mul(PAGE_ROWS)
            .and_then(|value| value.checked_mul(row_bytes))
            .ok_or(BudgetedPageError::ArithmeticOverflow)?;
        let last = run
            .end
            .checked_mul(PAGE_ROWS)
            .map(|value| value.min(rows))
            .and_then(|value| value.checked_mul(row_bytes))
            .ok_or(BudgetedPageError::ArithmeticOverflow)?;
        if first >= last {
            return Err(BudgetedPageError::InvalidGeometry);
        }
        charged = charged
            .checked_add(last - first)
            .ok_or(BudgetedPageError::ArithmeticOverflow)?;
        ranges.push(first..last);
    }
    Ok((ranges, charged))
}

// The admission loop needs only the cost for rejected candidates. A full
// cover (and its range allocations) is materialized after admission.
fn cover_charge(
    selected: &[usize],
    rows: usize,
    row_bytes: usize,
    full_page_bytes: usize,
    max_gets: usize,
    gaps: &mut Vec<usize>,
) -> Result<usize, BudgetedPageError> {
    if selected.is_empty() || max_gets == 0 {
        return Err(BudgetedPageError::InvalidGeometry);
    }
    let mut charged = selected
        .len()
        .checked_mul(full_page_bytes)
        .ok_or(BudgetedPageError::ArithmeticOverflow)?;
    if rows % PAGE_ROWS != 0 && selected.last() == Some(&(rows / PAGE_ROWS)) {
        charged -= (PAGE_ROWS - rows % PAGE_ROWS) * row_bytes;
    }
    gaps.clear();
    for pair in selected.windows(2) {
        if pair[1] > pair[0] + 1 {
            gaps.push(pair[1] - pair[0] - 1);
        }
    }
    let bridges = (gaps.len() + 1).saturating_sub(max_gets);
    if bridges > 0 {
        gaps.sort_unstable();
        for &gap in gaps.iter().take(bridges) {
            charged = charged
                .checked_add(
                    gap.checked_mul(full_page_bytes)
                        .ok_or(BudgetedPageError::ArithmeticOverflow)?,
                )
                .ok_or(BudgetedPageError::ArithmeticOverflow)?;
        }
    }
    Ok(charged)
}

fn choose_ranked_pages(
    page_count: usize,
    score_order: impl IntoIterator<Item = usize>,
    primary: &[usize],
    rows: usize,
    dimensions: usize,
    beta: usize,
    max_gets: usize,
    max_bytes: usize,
) -> Result<BudgetedPagePlan, BudgetedPageError> {
    if rows == 0
        || dimensions == 0
        || beta == 0
        || max_gets == 0
        || max_bytes == 0
        || page_count != rows.div_ceil(PAGE_ROWS)
    {
        return Err(BudgetedPageError::InvalidGeometry);
    }
    let row_bytes = dimensions
        .checked_add(12)
        .ok_or(BudgetedPageError::ArithmeticOverflow)?;
    let full_page_bytes = PAGE_ROWS
        .checked_mul(row_bytes)
        .ok_or(BudgetedPageError::ArithmeticOverflow)?;
    rows.checked_mul(row_bytes)
        .ok_or(BudgetedPageError::ArithmeticOverflow)?;
    if primary.is_empty() || primary.iter().any(|&ordinal| ordinal >= rows) {
        return Err(BudgetedPageError::InvalidPrimary);
    }
    let mut primary_seen = HashSet::new();
    let mut primary_pages = Vec::new();
    for &ordinal in primary {
        let page = ordinal / PAGE_ROWS;
        if primary_seen.insert(page) {
            primary_pages.push(page);
        }
    }
    let target_pages = beta
        .checked_mul(primary_pages.len())
        .ok_or(BudgetedPageError::ArithmeticOverflow)?
        .min(page_count);
    let mut selected = Vec::new();
    let mut gap_scratch = Vec::new();
    let mut final_ranges = Vec::new();
    let mut final_bytes = 0;
    for page in primary_pages.iter().copied().chain(
        score_order
            .into_iter()
            .filter(|page| !primary_seen.contains(page)),
    ) {
        let insertion = selected.binary_search(&page).unwrap_or_else(|index| index);
        selected.insert(insertion, page);
        let charged = cover_charge(
            &selected,
            rows,
            row_bytes,
            full_page_bytes,
            max_gets,
            &mut gap_scratch,
        )?;
        if charged > max_bytes {
            selected.remove(insertion);
            continue;
        }
        let (ranges, exact_charge) = cover_pages(
            &selected.iter().copied().collect(),
            rows,
            row_bytes,
            max_gets,
        )?;
        debug_assert_eq!(charged, exact_charge);
        final_ranges = ranges;
        final_bytes = charged;
        if selected.len() >= target_pages {
            break;
        }
    }
    if selected.is_empty() {
        return Err(BudgetedPageError::InsufficientBudget);
    }
    let retained = primary_pages
        .iter()
        .filter(|page| selected.binary_search(page).is_ok())
        .count();
    let selected_pages = selected;
    Ok(BudgetedPagePlan {
        target_shortfall: target_pages.saturating_sub(selected_pages.len()),
        target_pages,
        selected_pages,
        ranges: final_ranges,
        planned_bytes: final_bytes,
        primary_pages_retained: retained,
    })
}

/// Admit primary pages in primary-rank order, then other pages by score/ID.
/// The final range cover bridges the cheapest gaps needed to respect
/// `max_gets`. This pure planner does not compute page scores or claim
/// empirical recall, S3 latency, or charged serving memory.
pub fn choose_budgeted_pages(
    page_scores: &[f32],
    primary: &[usize],
    rows: usize,
    dimensions: usize,
    beta: usize,
    max_gets: usize,
    max_bytes: usize,
) -> Result<BudgetedPagePlan, BudgetedPageError> {
    if page_scores.len() != rows.div_ceil(PAGE_ROWS) {
        return Err(BudgetedPageError::InvalidGeometry);
    }
    if page_scores.iter().any(|score| !score.is_finite()) {
        return Err(BudgetedPageError::InvalidScore);
    }
    let mut score_order = (0..page_scores.len()).collect::<Vec<_>>();
    score_order.sort_unstable_by(|&left, &right| {
        page_scores[left]
            .partial_cmp(&page_scores[right])
            .expect("finite page scores")
            .then(left.cmp(&right))
    });
    choose_ranked_pages(
        page_scores.len(),
        score_order,
        primary,
        rows,
        dimensions,
        beta,
        max_gets,
        max_bytes,
    )
}

/// The same admission policy over a sparse set of scored pages. Unlisted
/// secondary pages are never admitted. Primary pages are always proposed
/// first, including when their score is absent from `candidates`.
pub fn choose_budgeted_pages_sparse(
    candidates: &[(usize, f32)],
    primary: &[usize],
    rows: usize,
    dimensions: usize,
    beta: usize,
    max_gets: usize,
    max_bytes: usize,
) -> Result<BudgetedPagePlan, BudgetedPageError> {
    let page_count = rows.div_ceil(PAGE_ROWS);
    let mut seen = HashSet::new();
    if candidates
        .iter()
        .any(|&(page, score)| page >= page_count || !score.is_finite() || !seen.insert(page))
    {
        return Err(BudgetedPageError::InvalidCandidate);
    }
    let mut sorted = candidates.to_vec();
    sorted.sort_unstable_by(|&(left_page, left_score), &(right_page, right_score)| {
        left_score
            .partial_cmp(&right_score)
            .expect("finite page scores")
            .then(left_page.cmp(&right_page))
    });
    choose_ranked_pages(
        page_count,
        sorted.into_iter().map(|(page, _)| page),
        primary,
        rows,
        dimensions,
        beta,
        max_gets,
        max_bytes,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sparse_candidates_never_admit_an_unvisited_page() {
        let plan =
            choose_budgeted_pages_sparse(&[(3, 0.1), (1, 0.2)], &[0], 1024, 96, 4, 32, 16_777_216)
                .unwrap();
        assert_eq!(plan.selected_pages, vec![0, 1, 3]);
        assert_eq!(plan.target_pages, 4);
        assert_eq!(plan.target_shortfall, 1);
    }

    #[test]
    fn complete_sparse_scores_refine_dense_page_admission() {
        let scores = [0.3, 0.1, 0.2, 0.0];
        let dense = choose_budgeted_pages(&scores, &[0], 1024, 96, 4, 2, 82_944).unwrap();
        let sparse = choose_budgeted_pages_sparse(
            &[(2, 0.2), (0, 0.3), (3, 0.0), (1, 0.1)],
            &[0],
            1024,
            96,
            4,
            2,
            82_944,
        )
        .unwrap();
        assert_eq!(sparse, dense);
    }

    #[test]
    fn sparse_candidates_reject_duplicates_and_unknown_pages() {
        for candidates in [
            &[(1, 0.0), (1, 0.1)][..],
            &[(4, 0.0)][..],
            &[(1, f32::NAN)][..],
        ] {
            assert_eq!(
                choose_budgeted_pages_sparse(candidates, &[0], 1024, 96, 4, 32, 16_777_216,),
                Err(BudgetedPageError::InvalidCandidate),
            );
        }
    }

    #[test]
    fn incremental_cover_charge_matches_exact_ranges_with_tied_gaps_and_final_page() {
        let rows = 8 * PAGE_ROWS + 1;
        let row_bytes = 108;
        let full_page_bytes = PAGE_ROWS * row_bytes;
        let pages = [0, 2, 4, 8];
        let mut gaps = Vec::new();
        for max_gets in 1..=4 {
            let exact = cover_pages(&pages.iter().copied().collect(), rows, row_bytes, max_gets)
                .unwrap()
                .1;
            assert_eq!(
                cover_charge(
                    &pages,
                    rows,
                    row_bytes,
                    full_page_bytes,
                    max_gets,
                    &mut gaps
                )
                .unwrap(),
                exact
            );
        }
    }

    #[test]
    fn incremental_cover_charge_matches_exact_cover_across_page_patterns() {
        let mut gaps = Vec::new();
        for rows in [1usize, 256, 257, 513, 2048, 2049, 4095] {
            let page_count = rows.div_ceil(PAGE_ROWS);
            for mask in 1usize..(1usize << page_count.min(12)) {
                let pages = (0..page_count)
                    .filter(|page| mask & (1 << page) != 0)
                    .collect::<Vec<_>>();
                for max_gets in 1..=4 {
                    let exact = cover_pages(&pages.iter().copied().collect(), rows, 108, max_gets)
                        .unwrap()
                        .1;
                    assert_eq!(
                        cover_charge(&pages, rows, 108, 108 * PAGE_ROWS, max_gets, &mut gaps)
                            .unwrap(),
                        exact,
                        "rows={rows} pages={pages:?} max_gets={max_gets}"
                    );
                }
            }
        }
    }

    #[test]
    fn primary_page_precedes_better_scoring_secondary_page() {
        let plan =
            choose_budgeted_pages(&[0.3, 10.0, 0.1, 0.2], &[300], 1024, 96, 2, 32, 16_777_216)
                .unwrap();
        assert_eq!(plan.selected_pages, vec![1, 2]);
        assert_eq!(plan.ranges, vec![27_648..82_944]);
        assert_eq!(plan.planned_bytes, 55_296);
        assert_eq!(plan.primary_pages_retained, 1);
    }

    #[test]
    fn cheapest_gap_bridge_counts_bytes_and_rejects_over_cap_page() {
        let scores = [0.0; 8];
        let primary = [0, 512, 1024, 1536];
        let exact_cap = choose_budgeted_pages(&scores, &primary, 2048, 96, 1, 2, 165_888).unwrap();
        assert_eq!(exact_cap.selected_pages, vec![0, 2, 4, 6]);
        assert_eq!(exact_cap.ranges, vec![0..138_240, 165_888..193_536]);
        assert_eq!(exact_cap.planned_bytes, 165_888);

        let too_small = choose_budgeted_pages(&scores, &primary, 2048, 96, 1, 2, 165_887).unwrap();
        assert_eq!(too_small.selected_pages, vec![0, 1, 2, 4]);
        assert_eq!(too_small.primary_pages_retained, 3);
        assert!(too_small.planned_bytes <= 165_887);
    }

    #[test]
    fn short_final_page_is_charged_by_its_actual_rows() {
        let plan = choose_budgeted_pages(&[9.0, 8.0, 0.0], &[512], 513, 96, 1, 1, 108).unwrap();
        assert_eq!(plan.selected_pages, vec![2]);
        assert_eq!(plan.ranges, vec![55_296..55_404]);
        assert_eq!(plan.planned_bytes, 108);
        assert_eq!(
            choose_budgeted_pages(&[9.0, 8.0, 0.0], &[512], 513, 96, 1, 1, 107),
            Err(BudgetedPageError::InsufficientBudget),
        );
    }

    #[test]
    fn signed_zero_scores_tie_by_page_id() {
        let plan =
            choose_budgeted_pages(&[0.0, -0.0, 1.0], &[512], 768, 96, 2, 32, 16_777_216).unwrap();
        assert_eq!(plan.selected_pages, vec![0, 2]);
    }
}
