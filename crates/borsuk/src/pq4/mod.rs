mod core;

#[cfg(test)]
pub(crate) use core::{
    Pq4Codebook, encode_blocks, fit_codebook, projected_resident_bytes, rank_candidates,
    rank_candidates_parallel_scalar_for_test, rank_candidates_scalar, score_rows_scalar,
};

#[cfg(test)]
mod tests {
    use super::{
        Pq4Codebook, encode_blocks, fit_codebook, projected_resident_bytes, rank_candidates,
        rank_candidates_parallel_scalar_for_test, rank_candidates_scalar, score_rows_scalar,
    };

    fn rows(count: usize) -> Vec<[f32; 96]> {
        (0..count)
            .map(|row| {
                std::array::from_fn(|dimension| {
                    let value = ((row * 37 + dimension * 19) % 257) as f32;
                    (value - 128.0) / 129.0
                })
            })
            .collect()
    }

    #[test]
    fn v26_release_contract_pq4_core_projection_and_training_are_deterministic() {
        assert_eq!(
            projected_resident_bytes(100_000_000).unwrap(),
            2_336_975_744
        );
        assert!(projected_resident_bytes(0).is_err());

        let rows = rows(64);
        let first = fit_codebook(&rows).unwrap();
        let second = fit_codebook(&rows).unwrap();
        assert_eq!(first, second);
        assert_eq!(first.centroids.len(), 32);
        assert!(
            first
                .centroids
                .iter()
                .flatten()
                .all(|value| value.is_finite())
        );

        let mut invalid = rows.clone();
        invalid[3][17] = f32::NAN;
        assert!(fit_codebook(&invalid).is_err());
        assert!(fit_codebook(&[[0.0; 96]; 16]).is_err());
    }

    #[test]
    fn v26_release_contract_pq4_core_blocks_preserve_nibbles_order_and_padding() {
        let codes = (0..35)
            .map(|row| std::array::from_fn(|subspace| ((row + subspace) % 16) as u8))
            .collect::<Vec<_>>();
        let blocks = encode_blocks(&codes).unwrap();
        assert_eq!(blocks.len(), 2);
        for (row, code) in codes.iter().enumerate() {
            for (subspace, expected) in code.iter().enumerate() {
                let packed = blocks[row / 32][subspace * 16 + row % 32 / 2];
                let actual = if row % 2 == 0 {
                    packed & 15
                } else {
                    packed >> 4
                };
                assert_eq!(actual, *expected, "row {row}, subspace {subspace}");
            }
        }
        for row in 35..64 {
            for subspace in 0..32 {
                let packed = blocks[1][subspace * 16 + row % 32 / 2];
                let actual = if row % 2 == 0 {
                    packed & 15
                } else {
                    packed >> 4
                };
                assert_eq!(actual, 0, "padding row {row}, subspace {subspace}");
            }
        }

        let mut invalid = codes;
        invalid[4][9] = 16;
        assert!(encode_blocks(&invalid).is_err());
        assert!(encode_blocks(&[]).is_err());
    }

    #[test]
    fn v26_release_contract_pq4_core_histogram_ranking_matches_literal_full_sort() {
        let rows = rows(640);
        let codebook: Pq4Codebook = fit_codebook(&rows).unwrap();
        let codes = rows
            .iter()
            .map(|row| codebook.encode(row).unwrap())
            .collect::<Vec<_>>();
        let blocks = encode_blocks(&codes).unwrap();
        let query = rows[319];

        let actual = rank_candidates_scalar(&codebook, &blocks, rows.len(), &query, 512).unwrap();
        assert_eq!(actual.len(), 512);
        assert!(actual.windows(2).all(|pair| {
            (pair[0].score, pair[0].source_ordinal) <= (pair[1].score, pair[1].source_ordinal)
        }));

        let scores = score_rows_scalar(&codebook, &blocks, rows.len(), &query).unwrap();
        let mut expected = scores
            .into_iter()
            .enumerate()
            .map(|(source_ordinal, score)| (score, source_ordinal as u64))
            .collect::<Vec<_>>();
        expected.sort_unstable();
        expected.truncate(512);
        assert_eq!(
            actual
                .iter()
                .map(|row| (row.score, row.source_ordinal))
                .collect::<Vec<_>>(),
            expected
        );
        assert!(actual.iter().any(|row| row.source_ordinal == 319));
        assert!(rank_candidates_scalar(&codebook, &blocks, rows.len(), &query, 513).is_err());
    }

    #[test]
    fn v26_release_contract_pq4_core_parallel_chunks_match_the_scalar_control() {
        let rows = rows(4_097);
        let codebook = fit_codebook(&rows).unwrap();
        let codes = rows
            .iter()
            .map(|row| codebook.encode(row).unwrap())
            .collect::<Vec<_>>();
        let blocks = encode_blocks(&codes).unwrap();
        let query = rows[4_096];

        let scalar = rank_candidates_scalar(&codebook, &blocks, rows.len(), &query, 2_048).unwrap();
        let parallel =
            rank_candidates_parallel_scalar_for_test(&codebook, &blocks, rows.len(), &query, 2_048)
                .unwrap();
        assert_eq!(parallel, scalar);
    }

    #[cfg(not(target_arch = "aarch64"))]
    #[test]
    fn v26_release_contract_pq4_core_production_scan_rejects_unqualified_backend() {
        let rows = rows(512);
        let codebook = fit_codebook(&rows).unwrap();
        let codes = rows
            .iter()
            .map(|row| codebook.encode(row).unwrap())
            .collect::<Vec<_>>();
        let blocks = encode_blocks(&codes).unwrap();
        let error = rank_candidates(&codebook, &blocks, rows.len(), &rows[0], 512).unwrap_err();
        assert!(error.to_string().contains("AArch64 NEON"));
    }
}
