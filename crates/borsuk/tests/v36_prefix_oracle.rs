//! Fast-fail scalar contracts for the V36 sequential prefix oracle.

use std::{
    collections::{BTreeMap, HashMap},
    io::Cursor,
    sync::Arc,
};

use arrow_array::{ArrayRef, FixedSizeListArray, Float32Array, Float64Array, RecordBatch};
use arrow_ipc::{
    MetadataVersion,
    reader::FileReader,
    writer::{FileWriter, IpcWriteOptions},
};
use arrow_schema::{DataType, Field, Schema};
use borsuk::{
    Result, V35ProjectionBackend, V36_POSTING_ACCELERATOR_EF_LADDER,
    V36CenteredProjectionBlockVisitor, V36CenteredProjectionSource,
    V36CenteredProjectionTrainingSpec, V36CenteredSampleRole, V36CoarseAssignmentIdentity,
    V36CoarseFragmentArm, V36CoarseFragmentArtifact, V36CoarseFragmentContext,
    V36CoarseFragmentRows, V36GeometryStop, V36PostingAcceleratorKind,
    V36PostingAcceleratorObservation, V36PostingGaussianSummary, V36PostingPrefixComparison,
    V36ResidualAssignmentBlockVisitor, V36ResidualAssignmentSource, V36ResidualPq4Codebook,
    V36ResidualPq4Width, V36TransportDirectory, V36TransportFetchObservation, V36TransportFragment,
    V36TransportLimits, V36TransportPosting, V36UniqueLiveTopK, admit_v36_geometry,
    allocate_v36_hamilton_postings, assign_v36_postings, audit_v36_coarse_fragment_sha256,
    authenticate_v36_coarse_fragment, authenticate_v36_posting_centroids,
    build_v36_srht192_control, compare_v36_posting_prefixes, decode_v36_centered_projection_arrow,
    decode_v36_coarse_fragment_arrow, decode_v36_transport_prefix,
    encode_v36_centered_projection_arrow, encode_v36_coarse_fragment_arrow,
    encode_v36_residual_pq4_record, encode_v36_sign24_record, project_v35_query_scalar,
    project_v35_query_simd, project_v36_centered_row_scalar, project_v36_centered_row_simd,
    rank_v36_selected_posting_candidates, score_v36_posting_centroid, score_v36_posting_gaussian,
    score_v36_posting_prototype_six, score_v36_residual_pq4_record, score_v36_sign24_record,
    select_v36_closure_owners, select_v36_flat_centroid_candidates, train_v36_centered_subspace,
    train_v36_posting_centroids, train_v36_posting_gaussian, train_v36_posting_prototype_six,
    train_v36_residual_pq4, v36_effective_ef_search, v36_posting_acceleration_required,
    validate_v36_posting_accelerator_observation,
};
use sha2::{Digest, Sha256};

struct TestProjectionSource {
    corpus_rows: Vec<Vec<f32>>,
    reservoir_rows: Vec<Vec<f32>>,
    block_rows: usize,
    mutate_reservoir: bool,
}

#[test]
fn v36_posting_accelerator_exact_rerank_preserves_binary64_authority() {
    let scores = [-0.0_f64, -1.0, 1.0 + f64::EPSILON, 1.0, 1.0];
    let ranked = rank_v36_selected_posting_candidates(&[4, 3, 2, 1, 0], 5, 5, |ordinal| {
        Ok(scores[ordinal as usize])
    })
    .unwrap();
    assert_eq!(
        ranked
            .iter()
            .map(|item| item.posting_ordinal)
            .collect::<Vec<_>>(),
        vec![1, 0, 3, 4, 2]
    );
    assert_eq!(ranked[1].score.to_bits(), 0.0_f64.to_bits());

    assert!(rank_v36_selected_posting_candidates(&[0, 0], 5, 1, |_| Ok(0.0)).is_err());
    assert!(rank_v36_selected_posting_candidates(&[5], 5, 1, |_| Ok(0.0)).is_err());
    assert!(rank_v36_selected_posting_candidates(&[0], 5, 2, |_| Ok(0.0)).is_err());
    for nonfinite in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
        assert!(rank_v36_selected_posting_candidates(&[0], 5, 1, |_| Ok(nonfinite)).is_err());
    }
}

#[test]
fn v36_posting_accelerator_policy_is_exact_at_the_1024_boundary() {
    assert!(!v36_posting_acceleration_required(1_024).unwrap());
    assert!(v36_posting_acceleration_required(1_025).unwrap());
    assert!(v36_posting_acceleration_required(0).is_err());
    assert_eq!(
        V36_POSTING_ACCELERATOR_EF_LADDER,
        [64, 128, 256, 512, 1_024]
    );
    assert_eq!(v36_effective_ef_search(17, 64).unwrap(), 64);
    assert_eq!(v36_effective_ef_search(1_500, 1_024).unwrap(), 1_500);
    assert!(v36_effective_ef_search(0, 64).is_err());
    assert!(v36_effective_ef_search(17, 63).is_err());
}

#[test]
fn v36_posting_accelerator_flat_centroid_scan_is_bounded_and_deterministic() {
    let mut centroids = Vec::new();
    for first in [3.0_f32, 2.0, 1.0, -1.0, -2.0, -3.0] {
        let mut centroid = [0.0_f32; 192];
        centroid[0] = first;
        centroids.push(centroid);
    }
    let directory = authenticate_v36_posting_centroids(centroids.clone()).unwrap();
    assert_eq!(directory.posting_count(), 6);
    let query = vec![0.0_f32; 192];
    assert_eq!(
        select_v36_flat_centroid_candidates(&directory, &query, 4).unwrap(),
        vec![2, 3, 1, 4]
    );
    assert_eq!(
        select_v36_flat_centroid_candidates(&directory, &query, 6).unwrap(),
        vec![2, 3, 1, 4, 0, 5]
    );
    assert!(select_v36_flat_centroid_candidates(&directory, &query, 0).is_err());
    assert!(select_v36_flat_centroid_candidates(&directory, &query, 7).is_err());
    assert!(select_v36_flat_centroid_candidates(&directory, &query[..191], 4).is_err());
    let mut nonfinite = centroids.clone();
    nonfinite[0][17] = f32::NAN;
    assert!(authenticate_v36_posting_centroids(nonfinite).is_err());
    let mut negative_zero = centroids;
    negative_zero[0][17] = -0.0;
    assert!(authenticate_v36_posting_centroids(negative_zero).is_err());
    assert!(authenticate_v36_posting_centroids(Vec::new()).is_err());
}

fn prefix_comparison(
    query_ordinal: u32,
    requested_prefix_length: u32,
    exhaustive_prefix: &[u32],
    accelerated_candidates: &[u32],
    accelerated_prefix: &[u32],
) -> V36PostingPrefixComparison {
    V36PostingPrefixComparison {
        query_ordinal,
        requested_prefix_length,
        exhaustive_prefix: exhaustive_prefix.to_vec(),
        accelerated_candidates: accelerated_candidates.to_vec(),
        accelerated_prefix: accelerated_prefix.to_vec(),
    }
}

#[test]
fn v36_posting_accelerator_parity_recomputes_order_containment_and_first_difference() {
    let comparisons = [
        prefix_comparison(0, 2, &[1, 2], &[3, 2, 1], &[1, 2]),
        prefix_comparison(1, 2, &[4, 5], &[4, 6, 5], &[4, 6]),
        prefix_comparison(2, 2, &[7, 8], &[7, 9, 10], &[7, 9]),
    ];
    let evidence = compare_v36_posting_prefixes(16, &comparisons).unwrap();
    assert_eq!(evidence.query_prefix_pairs, 3);
    assert_eq!(evidence.ordered_prefix_matches, 1);
    assert_eq!(evidence.parity_ppm, 333_333);
    assert_eq!(evidence.candidate_containment_matches, 2);
    assert_eq!(evidence.candidate_containment_ppm, 666_666);
    let disagreement = evidence.first_ordered_disagreement.unwrap();
    assert_eq!(disagreement.query_ordinal, 1);
    assert_eq!(disagreement.requested_prefix_length, 2);
    assert_eq!(disagreement.position, 1);
    assert_eq!(disagreement.exhaustive_posting_ordinal, 5);
    assert_eq!(disagreement.accelerated_posting_ordinal, 6);
    let missing = evidence.first_missing_candidate.unwrap();
    assert_eq!(missing.query_ordinal, 2);
    assert_eq!(missing.requested_prefix_length, 2);
    assert_eq!(missing.exhaustive_posting_ordinal, 8);

    let mut almost_exact = (0..1_000)
        .map(|query| prefix_comparison(query, 1, &[0], &[0, 1], &[0]))
        .collect::<Vec<_>>();
    almost_exact[999].accelerated_prefix[0] = 1;
    let evidence = compare_v36_posting_prefixes(2, &almost_exact).unwrap();
    assert_eq!(evidence.parity_ppm, 999_000);
    assert_eq!(evidence.candidate_containment_ppm, 1_000_000);
}

#[test]
fn v36_posting_accelerator_parity_rejects_ambiguous_or_malformed_populations() {
    let baseline = prefix_comparison(0, 2, &[0, 1], &[0, 1, 2], &[0, 1]);
    assert!(compare_v36_posting_prefixes(3, std::slice::from_ref(&baseline)).is_ok());

    for malformed in [
        V36PostingPrefixComparison {
            requested_prefix_length: 0,
            ..baseline.clone()
        },
        V36PostingPrefixComparison {
            exhaustive_prefix: vec![0],
            ..baseline.clone()
        },
        V36PostingPrefixComparison {
            accelerated_candidates: vec![0, 0, 1],
            ..baseline.clone()
        },
        V36PostingPrefixComparison {
            accelerated_prefix: vec![0, 0],
            ..baseline.clone()
        },
        V36PostingPrefixComparison {
            accelerated_candidates: vec![0, 1, 3],
            ..baseline.clone()
        },
    ] {
        assert!(compare_v36_posting_prefixes(3, &[malformed]).is_err());
    }
    assert!(compare_v36_posting_prefixes(0, std::slice::from_ref(&baseline)).is_err());
    assert!(compare_v36_posting_prefixes(3, &[]).is_err());
    assert!(
        compare_v36_posting_prefixes(
            3,
            &[
                prefix_comparison(1, 2, &[0, 1], &[0, 1], &[0, 1]),
                baseline.clone(),
            ],
        )
        .is_err()
    );
    assert!(compare_v36_posting_prefixes(3, &[baseline.clone(), baseline]).is_err());
}

fn qualified_accelerator_observation() -> V36PostingAcceleratorObservation {
    V36PostingAcceleratorObservation {
        kind: V36PostingAcceleratorKind::HnswSelectedScore,
        posting_count: 1_025,
        requested_prefix_length: 256,
        ef_search: 512,
        prefix_matches: 999,
        query_prefix_pairs: 1_000,
        parity_ppm: 999_000,
        visited_nodes: 400,
        candidate_generation_score_evaluations: 400,
        exact_rerank_score_evaluations: 512,
        exhaustive_score_evaluations: 1_025,
        allocated_bytes: 4_096,
        accelerated_decoded_hot_p99_ns: 900,
        exhaustive_decoded_hot_p99_ns: 1_000,
        qualified: true,
    }
}

#[test]
fn v36_posting_accelerator_receipt_recomputes_parity_work_latency_and_memory() {
    let baseline = qualified_accelerator_observation();
    assert!(validate_v36_posting_accelerator_observation(&baseline).unwrap());

    for changed in [
        V36PostingAcceleratorObservation {
            prefix_matches: 998,
            parity_ppm: 998_000,
            qualified: false,
            ..baseline.clone()
        },
        V36PostingAcceleratorObservation {
            candidate_generation_score_evaluations: 513,
            qualified: false,
            ..baseline.clone()
        },
        V36PostingAcceleratorObservation {
            accelerated_decoded_hot_p99_ns: 1_000,
            qualified: false,
            ..baseline.clone()
        },
        V36PostingAcceleratorObservation {
            allocated_bytes: 128 * 1024 * 1024 + 1,
            qualified: false,
            ..baseline.clone()
        },
    ] {
        assert!(!validate_v36_posting_accelerator_observation(&changed).unwrap());
    }

    for malformed in [
        V36PostingAcceleratorObservation {
            parity_ppm: 999_001,
            ..baseline.clone()
        },
        V36PostingAcceleratorObservation {
            qualified: false,
            ..baseline.clone()
        },
        V36PostingAcceleratorObservation {
            query_prefix_pairs: 0,
            ..baseline.clone()
        },
        V36PostingAcceleratorObservation {
            prefix_matches: 1_001,
            ..baseline.clone()
        },
        V36PostingAcceleratorObservation {
            posting_count: 1_024,
            ..baseline
        },
    ] {
        assert!(validate_v36_posting_accelerator_observation(&malformed).is_err());
    }
}

impl V36CenteredProjectionSource for TestProjectionSource {
    fn scan(
        &mut self,
        role: V36CenteredSampleRole,
        visitor: &mut V36CenteredProjectionBlockVisitor<'_>,
    ) -> Result<()> {
        let rows = match role {
            V36CenteredSampleRole::CorpusMean => &self.corpus_rows,
            V36CenteredSampleRole::GeometryReservoir => &self.reservoir_rows,
        };
        for (block_index, block) in rows.chunks(self.block_rows).enumerate() {
            let start = block_index * self.block_rows;
            let ordinals = (start..start + block.len())
                .map(|ordinal| ordinal as u64)
                .collect::<Vec<_>>();
            let mut values = block.iter().flatten().copied().collect::<Vec<_>>();
            if self.mutate_reservoir
                && role == V36CenteredSampleRole::GeometryReservoir
                && start + block.len() == rows.len()
            {
                *values.last_mut().unwrap() = 31.0;
            }
            visitor(&ordinals, &values)?;
        }
        Ok(())
    }
}

struct TestResidualSource {
    identities: Vec<V36CoarseAssignmentIdentity>,
    rows: Vec<Vec<f32>>,
    block_rows: usize,
    scans: usize,
    mutate_after_first: bool,
}

impl V36ResidualAssignmentSource for TestResidualSource {
    fn scan(&mut self, visitor: &mut V36ResidualAssignmentBlockVisitor<'_>) -> Result<()> {
        self.scans += 1;
        for (identities, rows) in self
            .identities
            .chunks(self.block_rows)
            .zip(self.rows.chunks(self.block_rows))
        {
            let mut flat = rows.iter().flatten().copied().collect::<Vec<_>>();
            if self.mutate_after_first && self.scans > 1 {
                flat[0] += 1.0;
            }
            visitor(identities, &flat)?;
        }
        Ok(())
    }
}

fn centered_training_spec(
    source_dimensions: usize,
    retained_dimensions: usize,
    corpus_sha256: &str,
    reservoir_sha256: &str,
    energy_dimensions: Vec<usize>,
) -> V36CenteredProjectionTrainingSpec {
    V36CenteredProjectionTrainingSpec {
        source_dimensions,
        retained_dimensions,
        corpus_rows: 4,
        corpus_sha256: corpus_sha256.to_owned(),
        reservoir_rows: 4,
        reservoir_sha256: reservoir_sha256.to_owned(),
        maximum_block_rows: 3,
        energy_dimensions,
    }
}

fn centered_source_digest(domain: &[u8], rows: &[Vec<f32>]) -> String {
    let mut digest = Sha256::new();
    digest.update(domain);
    for (ordinal, row) in rows.iter().enumerate() {
        digest.update(u64::try_from(ordinal).unwrap().to_le_bytes());
        for value in row {
            digest.update(value.to_bits().to_le_bytes());
        }
    }
    format!("{:x}", digest.finalize())
}

fn decode_base64_fixture(encoded: &str) -> Vec<u8> {
    let mut output = Vec::new();
    let mut accumulator = 0_u32;
    let mut bits = 0_u32;
    for byte in encoded.bytes().filter(|byte| !byte.is_ascii_whitespace()) {
        if byte == b'=' {
            break;
        }
        let value = match byte {
            b'A'..=b'Z' => byte - b'A',
            b'a'..=b'z' => byte - b'a' + 26,
            b'0'..=b'9' => byte - b'0' + 52,
            b'+' => 62,
            b'/' => 63,
            _ => panic!("invalid checked-in base64 fixture"),
        };
        accumulator = (accumulator << 6) | u32::from(value);
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            output.push(((accumulator >> bits) & 0xff) as u8);
            accumulator &= (1_u32 << bits).wrapping_sub(1);
        }
    }
    output
}

#[test]
fn v36_hamilton_postings_preserve_every_nonempty_run() {
    // Break caught: proportional allocation rounds tiny non-empty runs to zero,
    // making their rows unreachable before any scorer can evaluate them.
    let allocation = allocate_v36_hamilton_postings(&[999_997, 1, 1, 1], 123).unwrap();

    assert_eq!(allocation, [120, 1, 1, 1]);
    assert_eq!(allocation.iter().sum::<u32>(), 123);
}

#[test]
fn v36_posting_centroids_use_farthest_first_and_ordered_lloyd() {
    // Break caught: local posting construction depends on caller order, uses
    // probabilistic seeding, or changes the registered ten-iteration result.
    let row = |ordinal: u64, x: f32| {
        let mut vector = vec![0.0_f32; 192];
        vector[0] = x;
        (ordinal, vector)
    };
    let rows = vec![row(40, 10.0), row(10, 0.0), row(30, 8.0), row(20, 2.0)];
    let trained = train_v36_posting_centroids(&rows, 2).unwrap();
    assert_eq!(trained.len(), 2);
    assert_eq!(trained[0][0], 1.0);
    assert_eq!(trained[1][0], 9.0);
    assert!(
        trained
            .iter()
            .flatten()
            .all(|value| !value.is_sign_negative())
    );

    let mut reversed = rows.clone();
    reversed.reverse();
    assert_eq!(train_v36_posting_centroids(&reversed, 2).unwrap(), trained);

    // The middle row is equidistant from the initialized endpoints and must
    // join centroid zero before the ordered binary64 recomputation.
    let tie = train_v36_posting_centroids(&[row(1, 0.0), row(2, 4.0), row(3, 8.0)], 2).unwrap();
    assert_eq!(tie[0][0], 2.0);
    assert_eq!(tie[1][0], 8.0);

    // Duplicate vectors exercise the registered empty-centroid transfer while
    // retaining deterministic, finite centroid bytes.
    let duplicate =
        train_v36_posting_centroids(&[row(1, 7.0), row(2, 7.0), row(3, 7.0)], 2).unwrap();
    assert_eq!(duplicate[0][0], 7.0);
    assert_eq!(duplicate[1][0], 7.0);
    assert!(train_v36_posting_centroids(&rows, 0).is_err());
    assert!(train_v36_posting_centroids(&rows[..1], 2).is_err());
    assert!(train_v36_posting_centroids(&[row(1, 0.0), row(1, 1.0)], 1).is_err());
    let mut negative_zero = row(1, 0.0);
    negative_zero.1[11] = -0.0;
    assert!(train_v36_posting_centroids(&[negative_zero], 1).is_err());

    // This fixture changes on the tenth pass; fewer Lloyd iterations produce
    // different literal centroid bits.
    let points = [
        (8.0, 16.0),
        (11.0, 24.0),
        (2.0, 11.0),
        (15.0, 23.0),
        (20.0, 16.0),
        (33.0, 34.0),
        (15.0, 6.0),
        (5.0, 9.0),
        (16.0, 21.0),
        (48.0, 45.0),
        (6.0, 47.0),
        (2.0, 48.0),
        (26.0, 11.0),
        (3.0, 5.0),
        (15.0, 1.0),
        (39.0, 44.0),
        (21.0, 11.0),
        (38.0, 2.0),
        (2.0, 19.0),
        (11.0, 47.0),
    ];
    let iterative = points
        .iter()
        .enumerate()
        .map(|(ordinal, (x, y))| {
            let mut vector = vec![0.0_f32; 192];
            vector[0] = *x;
            vector[1] = *y;
            (u64::try_from(ordinal).unwrap(), vector)
        })
        .collect::<Vec<_>>();
    let iterative = train_v36_posting_centroids(&iterative, 3).unwrap();
    assert_eq!(iterative[0][0].to_bits(), 0x40ca_aaab);
    assert_eq!(iterative[0][1].to_bits(), 0x423d_5555);
    assert_eq!(iterative[1][0].to_bits(), 0x4220_0000);
    assert_eq!(iterative[1][1].to_bits(), 0x4224_0000);
    assert_eq!(iterative[2][0].to_bits(), 0x4161_2492);
    assert_eq!(iterative[2][1].to_bits(), 0x4148_0000);

    let precise = train_v36_posting_centroids(
        &[row(1, 16_777_216.0), row(2, 1.0), row(3, -16_777_216.0)],
        1,
    )
    .unwrap();
    assert_eq!(precise[0][0].to_bits(), 0x3eaa_aaab);
}

#[test]
fn v36_closure_owners_reject_redundant_centroids_without_losing_distinct_regions() {
    // Break caught: closure either retains a redundant near-duplicate centroid
    // or drops a geometrically distinct owner that covers a boundary row.
    let centroids = vec![
        vec![1.0, 0.0],
        vec![-1.0, 0.0],
        vec![1.0, 0.1],
        vec![0.0, 1.02],
    ];

    let owners = select_v36_closure_owners(&[0.0, 0.0], &centroids, 0.05, 8).unwrap();

    assert_eq!(owners, [0, 1, 3]);

    let orthogonal = (0..10)
        .map(|dimension| {
            let mut centroid = vec![0.0_f32; 192];
            centroid[dimension] = 1.0;
            centroid
        })
        .collect::<Vec<_>>();
    assert_eq!(
        select_v36_closure_owners(&vec![0.0; 192], &orthogonal, 0.05, 8).unwrap(),
        (0..8).collect::<Vec<_>>()
    );
}

#[test]
fn v36_population_assignment_is_flat_bounded_and_worker_independent() {
    // Break caught: population assignment depends on scheduling/blocking,
    // stores nested per-row vectors, or computes occupancy from replicas as if
    // they were primaries.
    let vector = |ordinal: u64, x: f32, y: f32| {
        let mut value = vec![0.0_f32; 192];
        value[0] = x;
        value[1] = y;
        (ordinal, value)
    };
    let rows = vec![
        vector(30, 0.0, 0.0),
        vector(10, 1.0, 0.0),
        vector(20, -1.0, 0.0),
        vector(40, 0.0, 1.02),
        vector(50, 1.0, 0.0),
        vector(60, 0.0, 1.02),
    ];
    let centroids = vec![
        vector(0, -1.0, 0.0).1,
        vector(0, 1.0, 0.0).1,
        vector(0, 0.0, 1.02).1,
    ];
    let serial = assign_v36_postings(&rows, &centroids, Some(0.05), 1, 1, 2).unwrap();
    let parallel = assign_v36_postings(&rows, &centroids, Some(0.05), 4, 3, 2).unwrap();
    assert_eq!(serial, parallel);
    assert_eq!(serial.source_ordinals(), &[10, 20, 30, 40, 50, 60]);
    assert_eq!(serial.owner_offsets(), &[0, 1, 2, 5, 6, 7, 8]);
    assert_eq!(serial.owners(), &[1, 0, 0, 1, 2, 2, 1, 2]);
    assert_eq!(serial.primary_occupancy(), &[2, 2, 2]);
    assert_eq!(serial.stored_occupancy(), &[2, 3, 3]);
    assert_eq!(serial.admission().mean_replication_ppm, 1_333_334);

    let single = assign_v36_postings(&rows, &centroids, None, 2, 2, 2).unwrap();
    assert_eq!(single.owner_offsets(), &[0, 1, 2, 3, 4, 5, 6]);
    assert_eq!(single.owners(), &[1, 0, 0, 2, 1, 2]);
    assert_eq!(single.primary_occupancy(), single.stored_occupancy());

    let duplicate_rows = vec![vector(1, 0.0, 0.0), vector(2, 0.0, 0.0)];
    let duplicate_centroids = train_v36_posting_centroids(&duplicate_rows, 2).unwrap();
    let duplicate_single =
        assign_v36_postings(&duplicate_rows, &duplicate_centroids, None, 1, 1, 1).unwrap();
    assert_eq!(duplicate_single.primary_occupancy(), &[2, 0]);
    assert_eq!(duplicate_single.stored_occupancy(), &[2, 0]);
    let duplicate_closure =
        assign_v36_postings(&duplicate_rows, &duplicate_centroids, Some(0.05), 2, 1, 1).unwrap();
    assert_eq!(duplicate_closure.primary_occupancy(), &[2, 0]);
    assert_eq!(duplicate_closure.stored_occupancy(), &[2, 2]);
    assert!(assign_v36_postings(&rows, &centroids, Some(0.10), 1, 1, 2).is_err());
    assert!(assign_v36_postings(&rows, &centroids, None, 1, 1, 4_096).is_err());
    assert!(assign_v36_postings(&rows, &centroids, None, 0, 1, 2).is_err());
    assert!(assign_v36_postings(&rows, &centroids, None, 1, 0, 2).is_err());
    assert!(
        assign_v36_postings(
            &[vector(1, 0.0, 0.0), vector(1, 1.0, 0.0)],
            &centroids,
            None,
            1,
            1,
            1,
        )
        .is_err()
    );
}

#[test]
fn v36_geometry_admission_names_the_first_construction_gate() {
    // Break caught: an over-replicated or badly skewed geometry reaches query
    // evaluation instead of failing at its first registered construction gate.
    let admitted = admit_v36_geometry(&[100; 100], &[300; 100], 100).unwrap();
    assert_eq!(admitted.mean_replication_ppm, 3_000_000);
    assert_eq!(admitted.primary_p99, 100);
    assert_eq!(admitted.stored_p99, 300);
    assert_eq!(admitted.stop, None);

    let cases = [
        (
            vec![100; 100],
            [vec![300; 99], vec![301]].concat(),
            V36GeometryStop::MeanReplication,
        ),
        (
            [vec![100; 98], vec![201, 201]].concat(),
            [vec![300; 98], vec![301, 301]].concat(),
            V36GeometryStop::PrimaryP99,
        ),
        (
            [vec![100; 99], vec![401]].concat(),
            [vec![300; 99], vec![401]].concat(),
            V36GeometryStop::PrimaryMaximum,
        ),
        (
            vec![100; 100],
            [vec![290; 98], vec![601, 601]].concat(),
            V36GeometryStop::StoredP99,
        ),
        (
            vec![100; 100],
            [vec![291; 99], vec![801]].concat(),
            V36GeometryStop::StoredMaximum,
        ),
    ];
    for (primary, stored, expected) in cases {
        assert_eq!(
            admit_v36_geometry(&primary, &stored, 100).unwrap().stop,
            Some(expected)
        );
    }
}

#[test]
fn v36_geometry_admission_rejects_threshold_overflow() {
    // Break caught: saturating threshold arithmetic silently turns malformed
    // oversized authority into an effectively unbounded admission gate.
    assert!(admit_v36_geometry(&[1], &[1], u64::MAX).is_err());
}

#[test]
fn v36_coarse_identity_and_sign24_are_owner_relative_and_exact() {
    // Break caught: source, dense, and feature identities are aliased; a
    // replica is encoded against its primary owner; or sign bit/norm/scoring
    // arithmetic drifts from the frozen 44-byte control.
    let identity = V36CoarseAssignmentIdentity::new(7, 3, u64::try_from(i32::MAX).unwrap() + 9, 1);
    assert_eq!(identity.source_ordinal(), 7);
    assert_eq!(identity.owner_posting_ordinal(), 1);
    let mut row = vec![0.0_f32; 192];
    row[0] = 4.0;
    row[1] = -3.0;
    let mut owner = vec![0.0_f32; 192];
    owner[0] = 1.0;
    owner[1] = -3.0;
    let record = encode_v36_sign24_record(&identity, &row, &owner).unwrap();
    assert!(record.code().iter().all(|byte| *byte == u8::MAX));
    assert_eq!(record.residual_norm(), 3.0);
    assert_eq!(record.dense_ordinal(), 3);
    assert_eq!(
        record.source_feature_id(),
        u64::try_from(i32::MAX).unwrap() + 9
    );
    assert_eq!(record.to_bytes().len(), 44);

    let mut query = owner.clone();
    query[0] = 4.0;
    let scale = 3.0_f64 / 192.0_f64.sqrt();
    let expected = (3.0 - scale).powi(2) + 191.0 * scale.powi(2);
    assert_eq!(
        score_v36_sign24_record(&record, &query, &owner).unwrap(),
        expected
    );

    let zero = encode_v36_sign24_record(
        &V36CoarseAssignmentIdentity::new(8, 4, 99, 0),
        &owner,
        &owner,
    )
    .unwrap();
    assert_eq!(zero.residual_norm(), 0.0);
    assert!(zero.code().iter().all(|byte| *byte == 0));
    let mut negative_zero = owner.clone();
    negative_zero[9] = -0.0;
    assert!(encode_v36_sign24_record(&identity, &negative_zero, &owner).is_err());
}

#[test]
fn v36_unique_live_topk_is_bounded_replica_minimum_and_dense_tied() {
    // Break caught: first replica wins, tombstones consume K, an evicted ID
    // cannot re-enter, or the heap/global-seen state grows with scanned rows.
    let mut top = V36UniqueLiveTopK::try_new(2).unwrap();
    top.offer(9.0, 90, 900, true).unwrap();
    top.offer(8.0, 80, 800, false).unwrap();
    top.offer(7.0, 70, 700, true).unwrap();
    top.offer(6.0, 60, 600, true).unwrap(); // evicts feature 900
    top.offer(5.0, 50, 900, true).unwrap(); // better replica re-enters
    top.offer(4.0, 40, 700, true).unwrap(); // retained replica improves
    assert_eq!(top.ranked(), vec![(4.0, 40, 700), (5.0, 50, 900)]);
    assert_eq!(top.retained_len(), 2);
    assert!(top.membership_len() <= 2);
    assert!(top.offer(3.0, 41, 700, true).is_err());

    let mut tied = V36UniqueLiveTopK::try_new(2).unwrap();
    tied.offer(1.0, 9, 2, true).unwrap();
    tied.offer(1.0, 7, 3, true).unwrap();
    tied.offer(1.0, 8, 1, true).unwrap();
    assert_eq!(tied.ranked(), vec![(1.0, 7, 3), (1.0, 8, 1)]);
    assert!(tied.offer(f64::NAN, 1, 1, true).is_err());
    assert!(V36UniqueLiveTopK::try_new(0).is_err());
}

#[test]
fn v36_residual_pq4_records_are_owner_relative_row_major_and_exact() {
    // Break caught: PQ4 subspaces are strided, nibbles are reversed, lookup
    // distances use f32/reassociated sums, or replica codes use the primary
    // centroid instead of the owner whose object stores them.
    for width in [V36ResidualPq4Width::Code32, V36ResidualPq4Width::Code48] {
        let subquantizers = width.subquantizers();
        let subvector_dimensions = 192 / subquantizers;
        let mut centroids = vec![0.0_f32; 16 * 192];
        for subquantizer in 0..subquantizers {
            for codeword in 0..16 {
                for component in 0..subvector_dimensions {
                    let offset = subquantizer * 16 * subvector_dimensions
                        + codeword * subvector_dimensions
                        + component;
                    centroids[offset] = codeword as f32;
                }
            }
        }
        let codebook = V36ResidualPq4Codebook::try_new(width, centroids).unwrap();
        assert_eq!(codebook.raw_bytes(), 12_288);
        let mut row = vec![0.0_f32; 192];
        for (dimension, value) in row.iter_mut().enumerate() {
            *value = ((dimension / subvector_dimensions) % 16) as f32;
        }
        let owner = vec![0.0_f32; 192];
        let identity = V36CoarseAssignmentIdentity::new(19, 23, u64::MAX - 1, 5);
        let record = encode_v36_residual_pq4_record(&identity, &row, &owner, &codebook).unwrap();
        assert_eq!(record.code().len(), width.code_bytes());
        for (byte_ordinal, byte) in record.code().iter().copied().enumerate() {
            assert_eq!(byte & 0x0f, u8::try_from((2 * byte_ordinal) % 16).unwrap());
            assert_eq!(
                byte >> 4,
                u8::try_from((2 * byte_ordinal + 1) % 16).unwrap()
            );
        }
        assert_eq!(record.dense_ordinal(), 23);
        assert_eq!(record.source_feature_id(), u64::MAX - 1);
        assert_eq!(record.to_bytes().len(), width.record_bytes());
        assert_eq!(
            score_v36_residual_pq4_record(&record, &row, &owner, &codebook).unwrap(),
            0.0
        );
        let mut query = row.clone();
        let mut expected = 0.0_f64;
        for (dimension, value) in query.iter_mut().enumerate() {
            *value += (dimension % 7) as f32 * 0.001;
            let delta = f64::from(*value) - f64::from(row[dimension]);
            expected += delta * delta;
        }
        assert_eq!(
            score_v36_residual_pq4_record(&record, &query, &owner, &codebook).unwrap(),
            expected
        );

        let mut shifted_owner = vec![0.0_f32; 192];
        shifted_owner.fill(1.0);
        let shifted =
            encode_v36_residual_pq4_record(&identity, &row, &shifted_owner, &codebook).unwrap();
        assert_ne!(shifted.code(), record.code());
    }

    assert!(
        V36ResidualPq4Codebook::try_new(V36ResidualPq4Width::Code32, vec![0.0; 3_071]).is_err()
    );
    let mut invalid = vec![0.0_f32; 3_072];
    invalid[3] = -0.0;
    assert!(V36ResidualPq4Codebook::try_new(V36ResidualPq4Width::Code48, invalid).is_err());
}

fn v36_coarse_context(
    arm: V36CoarseFragmentArm,
    posting_ordinal: u32,
    codebook_sha256: Option<String>,
) -> V36CoarseFragmentContext {
    V36CoarseFragmentContext {
        arm,
        codebook_sha256,
        fragment_ordinal: 0,
        generation_manifest_sha256: "11".repeat(32),
        owner_centroids_sha256: "22".repeat(32),
        posting_ordinal,
        projection_sha256: "33".repeat(32),
        uri: format!("s3://frozen-v36/coarse/{posting_ordinal}/0.arrow"),
    }
}

#[test]
fn v36_coarse_fragment_arrow_roundtrips_strict_serving_arms_and_authority() {
    // Break caught: an Arrow fragment loses sign norm/unsigned identities,
    // accepts another generation, or admits projected-f32 as a serving arm.
    let owner = vec![0.0_f32; 192];
    let mut first_row = vec![1.0_f32; 192];
    first_row[1] = -1.0;
    let second_row = vec![2.0_f32; 192];
    let first_identity =
        V36CoarseAssignmentIdentity::new(0, 10, u64::try_from(i64::MAX).unwrap() + 7, 4);
    let second_identity =
        V36CoarseAssignmentIdentity::new(1, 90, u64::try_from(i64::MAX).unwrap() + 9, 4);
    let first_record = encode_v36_sign24_record(&first_identity, &first_row, &owner).unwrap();
    let second_record = encode_v36_sign24_record(&second_identity, &second_row, &owner).unwrap();
    let second_norm = second_record.residual_norm();
    let sign_rows = V36CoarseFragmentRows::Sign24(vec![first_record, second_record]);
    let context = v36_coarse_context(V36CoarseFragmentArm::Sign24, 4, None);
    let (bytes, artifact) = encode_v36_coarse_fragment_arrow(&context, &sign_rows).unwrap();
    assert!(bytes.len() <= 1_048_576);
    assert_eq!(artifact.first_dense_ordinal, 10);
    assert_eq!(artifact.last_dense_ordinal, 90);
    assert_eq!(artifact.row_count, 2);
    assert_eq!(artifact.encoded_bytes, bytes.len() as u64);
    audit_v36_coarse_fragment_sha256(&bytes, &artifact).unwrap();
    assert_eq!(
        decode_v36_coarse_fragment_arrow(&bytes, &artifact).unwrap(),
        sign_rows
    );

    let mut corrupted = bytes.clone();
    let corrupted_index = corrupted.len() / 2;
    corrupted[corrupted_index] ^= 1;
    assert!(audit_v36_coarse_fragment_sha256(&corrupted, &artifact).is_err());
    assert!(decode_v36_coarse_fragment_arrow(&corrupted, &artifact).is_err());
    let mut changed_artifact = artifact.clone();
    changed_artifact.context.generation_manifest_sha256 = "44".repeat(32);
    assert!(decode_v36_coarse_fragment_arrow(&bytes, &changed_artifact).is_err());

    let foreign_context = V36CoarseFragmentContext {
        generation_manifest_sha256: "55".repeat(32),
        ..context
    };
    let (foreign_bytes, _) =
        encode_v36_coarse_fragment_arrow(&foreign_context, &sign_rows).unwrap();
    assert!(decode_v36_coarse_fragment_arrow(&foreign_bytes, &artifact).is_err());

    let mut reader = FileReader::try_new(Cursor::new(bytes.as_slice()), None).unwrap();
    let schema = reader.schema();
    let batch = reader.next().unwrap().unwrap();
    let options = IpcWriteOptions::try_new(8, false, MetadataVersion::V5)
        .unwrap()
        .try_with_compression(Some(arrow_ipc::CompressionType::ZSTD))
        .unwrap();
    let mut compressed = Vec::new();
    let mut writer =
        FileWriter::try_new_with_options(&mut compressed, schema.as_ref(), options).unwrap();
    writer.write(&batch).unwrap();
    writer.finish().unwrap();
    drop(writer);
    let mut compressed_artifact = artifact.clone();
    compressed_artifact.encoded_bytes = compressed.len() as u64;
    compressed_artifact.sha256 = format!("{:x}", Sha256::digest(&compressed));
    compressed_artifact.blake3 = blake3::hash(&compressed).to_hex().to_string();
    compressed_artifact.decoded_capacity_bytes = artifact
        .decoded_capacity_bytes
        .checked_add(compressed.len() as u64)
        .unwrap()
        .checked_sub(bytes.len() as u64)
        .unwrap();
    assert!(decode_v36_coarse_fragment_arrow(&compressed, &compressed_artifact).is_err());

    let foreign_schema = Arc::new(Schema::new_with_metadata(
        vec![
            Field::new("wrong_dense_ordinal", DataType::UInt64, false),
            Field::new("source_feature_id", DataType::UInt64, false),
            Field::new("code", DataType::FixedSizeBinary(24), false),
            Field::new("residual_norm", DataType::Float32, false),
        ],
        schema.metadata().clone(),
    ));
    let foreign_batch =
        RecordBatch::try_new(foreign_schema.clone(), batch.columns().to_vec()).unwrap();
    let mut foreign_schema_bytes = Vec::new();
    let mut writer = FileWriter::try_new_with_options(
        &mut foreign_schema_bytes,
        foreign_schema.as_ref(),
        IpcWriteOptions::try_new(8, false, MetadataVersion::V5).unwrap(),
    )
    .unwrap();
    writer.write(&foreign_batch).unwrap();
    writer.finish().unwrap();
    drop(writer);
    let mut foreign_schema_artifact = artifact.clone();
    foreign_schema_artifact.encoded_bytes = foreign_schema_bytes.len() as u64;
    foreign_schema_artifact.sha256 = format!("{:x}", Sha256::digest(&foreign_schema_bytes));
    foreign_schema_artifact.blake3 = blake3::hash(&foreign_schema_bytes).to_hex().to_string();
    foreign_schema_artifact.decoded_capacity_bytes = artifact
        .decoded_capacity_bytes
        .checked_add(foreign_schema_bytes.len() as u64)
        .unwrap()
        .checked_sub(bytes.len() as u64)
        .unwrap();
    assert!(
        decode_v36_coarse_fragment_arrow(&foreign_schema_bytes, &foreign_schema_artifact).is_err()
    );

    for invalid_norm in [-0.0_f32, f32::NAN, 0.0] {
        let invalid_norms = Arc::new(Float32Array::from_iter_values([invalid_norm, second_norm]));
        let invalid_batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                batch.column(0).clone(),
                batch.column(1).clone(),
                batch.column(2).clone(),
                invalid_norms,
            ],
        )
        .unwrap();
        let mut invalid_bytes = Vec::new();
        let mut writer = FileWriter::try_new_with_options(
            &mut invalid_bytes,
            schema.as_ref(),
            IpcWriteOptions::try_new(8, false, MetadataVersion::V5).unwrap(),
        )
        .unwrap();
        writer.write(&invalid_batch).unwrap();
        writer.finish().unwrap();
        drop(writer);
        let mut invalid_artifact = artifact.clone();
        invalid_artifact.encoded_bytes = invalid_bytes.len() as u64;
        invalid_artifact.sha256 = format!("{:x}", Sha256::digest(&invalid_bytes));
        invalid_artifact.blake3 = blake3::hash(&invalid_bytes).to_hex().to_string();
        invalid_artifact.decoded_capacity_bytes = artifact
            .decoded_capacity_bytes
            .checked_add(invalid_bytes.len() as u64)
            .unwrap()
            .checked_sub(bytes.len() as u64)
            .unwrap();
        assert!(decode_v36_coarse_fragment_arrow(&invalid_bytes, &invalid_artifact).is_err());
    }
}

#[test]
fn v36_coarse_fragment_accepts_independently_hashed_pyarrow_logical_rows() {
    // Break caught: the Rust reader accidentally requires Rust-writer padding,
    // narrows u64 source IDs to signed i64, or changes the logical V36 schema.
    let bytes = decode_base64_fixture(include_str!("fixtures/v36_coarse_fragment_pyarrow.b64"));
    assert_eq!(bytes.len(), 2_114);
    assert_eq!(
        format!("{:x}", Sha256::digest(&bytes)),
        "7feadf996f64bdf64c0d655fbbbab8c5745dd0a086d36ed670071f8a90981d29"
    );
    assert_eq!(
        blake3::hash(&bytes).to_hex().as_str(),
        "033130e5a93beab9a5528f0fe1978023d16787e391fca45a08003087c38b8bc8"
    );
    let context = v36_coarse_context(V36CoarseFragmentArm::Sign24, 4, None);
    let artifact = V36CoarseFragmentArtifact {
        blake3: "033130e5a93beab9a5528f0fe1978023d16787e391fca45a08003087c38b8bc8".into(),
        context,
        decoded_capacity_bytes: u64::try_from(
            2_114
                + 2 * (44 + std::mem::size_of::<borsuk::V36Sign24Record>() + 16 + 96)
                + 64 * 1_024,
        )
        .unwrap(),
        encoded_bytes: 2_114,
        first_dense_ordinal: 7,
        last_dense_ordinal: 11,
        row_count: 2,
        sha256: "7feadf996f64bdf64c0d655fbbbab8c5745dd0a086d36ed670071f8a90981d29".into(),
    };
    audit_v36_coarse_fragment_sha256(&bytes, &artifact).unwrap();
    let rows = decode_v36_coarse_fragment_arrow(&bytes, &artifact).unwrap();
    let V36CoarseFragmentRows::Sign24(rows) = rows else {
        panic!("PyArrow fixture must decode as sign24")
    };
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].dense_ordinal(), 7);
    assert_eq!(rows[0].source_feature_id(), (1_u64 << 63) + 5);
    assert_eq!(rows[0].code(), &[0; 24]);
    assert_eq!(rows[0].residual_norm(), 0.0);
    assert_eq!(rows[1].dense_ordinal(), 11);
    assert_eq!(rows[1].source_feature_id(), u64::MAX - 2);
    assert_eq!(rows[1].code()[0], 0x80);
    assert_eq!(&rows[1].code()[1..], &[0; 23]);
    assert_eq!(rows[1].residual_norm(), 1.25);
}

#[test]
fn v36_coarse_fragment_arrow_rejects_order_duplicates_and_width_drift() {
    // Break caught: owner-local rows are unordered/duplicated, PQ widths are
    // mislabeled, or artifact identity fields are accepted without binding.
    let owner = vec![0.0_f32; 192];
    for width in [V36ResidualPq4Width::Code32, V36ResidualPq4Width::Code48] {
        let arm = match width {
            V36ResidualPq4Width::Code32 => V36CoarseFragmentArm::ResidualPq4Code32,
            V36ResidualPq4Width::Code48 => V36CoarseFragmentArm::ResidualPq4Code48,
        };
        let codebook = V36ResidualPq4Codebook::try_new(width, vec![0.0; 3_072]).unwrap();
        let identities = [
            V36CoarseAssignmentIdentity::new(0, 7, 70, 3),
            V36CoarseAssignmentIdentity::new(1, 11, 110, 3),
        ];
        let rows = V36CoarseFragmentRows::ResidualPq4(vec![
            encode_v36_residual_pq4_record(&identities[0], &vec![1.0; 192], &owner, &codebook)
                .unwrap(),
            encode_v36_residual_pq4_record(&identities[1], &vec![2.0; 192], &owner, &codebook)
                .unwrap(),
        ]);
        let context = v36_coarse_context(arm, 3, Some("66".repeat(32)));
        let (bytes, artifact) = encode_v36_coarse_fragment_arrow(&context, &rows).unwrap();
        assert_eq!(
            decode_v36_coarse_fragment_arrow(&bytes, &artifact).unwrap(),
            rows
        );
    }

    let identity = V36CoarseAssignmentIdentity::new(0, 5, 50, 2);
    let record = encode_v36_sign24_record(&identity, &vec![1.0; 192], &owner).unwrap();
    let context = v36_coarse_context(V36CoarseFragmentArm::Sign24, 2, None);
    let duplicates = V36CoarseFragmentRows::Sign24(vec![record.clone(), record.clone()]);
    assert!(encode_v36_coarse_fragment_arrow(&context, &duplicates).is_err());

    let reversed = V36CoarseFragmentRows::Sign24(vec![
        encode_v36_sign24_record(
            &V36CoarseAssignmentIdentity::new(0, 90, 900, 2),
            &vec![1.0; 192],
            &owner,
        )
        .unwrap(),
        encode_v36_sign24_record(
            &V36CoarseAssignmentIdentity::new(1, 10, 100, 2),
            &vec![2.0; 192],
            &owner,
        )
        .unwrap(),
    ]);
    assert!(encode_v36_coarse_fragment_arrow(&context, &reversed).is_err());

    let code32 =
        V36ResidualPq4Codebook::try_new(V36ResidualPq4Width::Code32, vec![0.0; 3_072]).unwrap();
    let width_drift = V36CoarseFragmentRows::ResidualPq4(vec![
        encode_v36_residual_pq4_record(
            &V36CoarseAssignmentIdentity::new(0, 5, 50, 2),
            &vec![1.0; 192],
            &owner,
            &code32,
        )
        .unwrap(),
    ]);
    let invalid_context = v36_coarse_context(
        V36CoarseFragmentArm::ResidualPq4Code48,
        2,
        Some("77".repeat(32)),
    );
    assert!(encode_v36_coarse_fragment_arrow(&invalid_context, &width_drift).is_err());
    let _type_lock: Option<V36CoarseFragmentArtifact> = None;
}

#[test]
fn v36_coarse_fragment_arrow_accounts_near_ceiling_decode_workspace() {
    // Break caught: admission counts only compact wire rows while the decoder
    // simultaneously retains Arrow buffers, expanded Rust records, identity
    // scratch, and uniqueness state.
    const ROWS: usize = 18_000;
    let owner = vec![0.0_f32; 192];
    let projected = vec![1.0_f32; 192];
    let codebook =
        V36ResidualPq4Codebook::try_new(V36ResidualPq4Width::Code32, vec![0.0; 3_072]).unwrap();
    let records = (0..ROWS)
        .map(|ordinal| {
            let ordinal = u64::try_from(ordinal).unwrap();
            encode_v36_residual_pq4_record(
                &V36CoarseAssignmentIdentity::new(ordinal, ordinal + 1, u64::MAX - ordinal, 8),
                &projected,
                &owner,
                &codebook,
            )
            .unwrap()
        })
        .collect::<Vec<_>>();
    let rows = V36CoarseFragmentRows::ResidualPq4(records);
    let context = v36_coarse_context(
        V36CoarseFragmentArm::ResidualPq4Code32,
        8,
        Some("88".repeat(32)),
    );
    let (bytes, artifact) = encode_v36_coarse_fragment_arrow(&context, &rows).unwrap();
    let per_row = 48 + std::mem::size_of::<borsuk::V36ResidualPq4Record>() + 16 + 96;
    let minimum = bytes.len() + ROWS * per_row + 64 * 1_024;
    assert_eq!(artifact.row_count, ROWS as u32);
    assert_eq!(artifact.decoded_capacity_bytes, minimum as u64);
    assert!(artifact.decoded_capacity_bytes > artifact.encoded_bytes * 4);
    assert_eq!(
        decode_v36_coarse_fragment_arrow(&bytes, &artifact).unwrap(),
        rows
    );
}

#[test]
fn v36_coarse_transport_decodes_postings_atomically_and_deduplicates_replicas() {
    // Break caught: a successful sibling fragment leaks candidates before a
    // later fragment fails, or one source identity is scored twice inside an
    // owner posting after fragment boundaries change.
    let owner = vec![0.0_f32; 192];
    let make = |fragment_ordinal: u32, dense_ordinal: u64, source_feature_id: u64| {
        let identity = V36CoarseAssignmentIdentity::new(
            u64::from(fragment_ordinal),
            dense_ordinal,
            source_feature_id,
            4,
        );
        let rows = V36CoarseFragmentRows::Sign24(vec![
            encode_v36_sign24_record(&identity, &vec![1.0; 192], &owner).unwrap(),
        ]);
        let mut context = v36_coarse_context(V36CoarseFragmentArm::Sign24, 4, None);
        context.fragment_ordinal = fragment_ordinal;
        context.uri = format!("s3://frozen-v36/coarse/4/{fragment_ordinal}.arrow");
        let (bytes, artifact) = encode_v36_coarse_fragment_arrow(&context, &rows).unwrap();
        (bytes, V36TransportFragment { artifact })
    };
    let (first_bytes, first) = make(0, 10, 100);
    let (second_bytes, second) = make(1, 90, 900);
    let directory = V36TransportDirectory::try_new(vec![V36TransportPosting {
        fragments: vec![first.clone(), second.clone()],
        posting_ordinal: 4,
        stored_assignment_rows: 2,
    }])
    .unwrap();
    let first_handle =
        Arc::new(authenticate_v36_coarse_fragment(&first_bytes, &first.artifact).unwrap());
    let second_handle =
        Arc::new(authenticate_v36_coarse_fragment(&second_bytes, &second.artifact).unwrap());
    let handles = BTreeMap::from([
        (first.artifact.context.uri.clone(), first_handle.clone()),
        (second.artifact.context.uri.clone(), second_handle.clone()),
    ]);
    let expected_authority = directory.authority().clone();
    let loaded = decode_v36_transport_prefix(
        &[4],
        &directory,
        &expected_authority,
        V36TransportLimits::qualification(),
        |artifact| {
            V36TransportFetchObservation::try_new(
                handles.get(&artifact.context.uri).cloned().unwrap(),
                1,
                artifact.encoded_bytes + 256,
            )
        },
    )
    .unwrap();
    assert_eq!(loaded.plan.admitted_postings, vec![4]);
    assert_eq!(loaded.postings.len(), 1);
    assert_eq!(loaded.postings[0].posting_ordinal, 4);
    assert_eq!(loaded.postings[0].fragments.len(), 2);
    assert!(Arc::ptr_eq(&loaded.postings[0].fragments[0], &first_handle));
    assert_eq!(loaded.plan.hard_gets_with_retries, 2);
    assert_eq!(
        loaded.plan.hard_returned_bytes_with_retries,
        first.artifact.encoded_bytes + second.artifact.encoded_bytes + 512
    );

    let cached = decode_v36_transport_prefix(
        &[4],
        &directory,
        &expected_authority,
        V36TransportLimits::qualification(),
        |artifact| {
            V36TransportFetchObservation::try_new(
                handles.get(&artifact.context.uri).cloned().unwrap(),
                0,
                0,
            )
        },
    )
    .unwrap();
    assert_eq!(cached.plan.hard_gets_with_retries, 0);
    assert_eq!(cached.plan.hard_returned_bytes_with_retries, 0);
    assert!(Arc::ptr_eq(
        &cached.postings[0].fragments[1],
        &second_handle
    ));

    let foreign = decode_v36_transport_prefix(
        &[4],
        &directory,
        &expected_authority,
        V36TransportLimits::qualification(),
        |_| V36TransportFetchObservation::try_new(second_handle.clone(), 0, 0),
    )
    .unwrap_err();
    assert_eq!(
        foreign.to_string(),
        "invalid storage: V36 cached fragment authority differs"
    );

    let indeterminate = decode_v36_transport_prefix(
        &[4],
        &directory,
        &expected_authority,
        V36TransportLimits::qualification(),
        |artifact| {
            V36TransportFetchObservation::try_new(
                handles.get(&artifact.context.uri).cloned().unwrap(),
                17,
                artifact.encoded_bytes * 17 + 256 * 17,
            )
        },
    )
    .unwrap();
    assert_eq!(
        indeterminate.plan.disposition,
        borsuk::V36TransportDisposition::Indeterminate
    );
    assert_eq!(indeterminate.plan.hard_gets_with_retries, 17);
    assert!(indeterminate.postings.is_empty());

    let mut foreign_authority = expected_authority.clone();
    foreign_authority.projection_sha256 = "ab".repeat(32);
    let authority_error = decode_v36_transport_prefix(
        &[4],
        &directory,
        &foreign_authority,
        V36TransportLimits::qualification(),
        |_| panic!("active-generation mismatch must reject before cache access"),
    )
    .unwrap_err();
    assert_eq!(
        authority_error.to_string(),
        "invalid storage: V36 active transport authority differs"
    );

    let (duplicate_bytes, duplicate) = make(1, 90, 100);
    let duplicate_directory = V36TransportDirectory::try_new(vec![V36TransportPosting {
        fragments: vec![first.clone(), duplicate.clone()],
        posting_ordinal: 4,
        stored_assignment_rows: 2,
    }])
    .unwrap();
    let duplicate_handles = BTreeMap::from([
        (first.artifact.context.uri.clone(), first_handle),
        (
            duplicate.artifact.context.uri.clone(),
            Arc::new(
                authenticate_v36_coarse_fragment(&duplicate_bytes, &duplicate.artifact).unwrap(),
            ),
        ),
    ]);
    assert!(
        decode_v36_transport_prefix(
            &[4],
            &duplicate_directory,
            duplicate_directory.authority(),
            V36TransportLimits::qualification(),
            |artifact| {
                V36TransportFetchObservation::try_new(
                    duplicate_handles
                        .get(&artifact.context.uri)
                        .cloned()
                        .unwrap(),
                    0,
                    0,
                )
            },
        )
        .unwrap_err()
        .to_string()
        .ends_with("V36 posting source identity differs")
    );
}

#[test]
fn v36_residual_pq4_training_is_replay_bounded_exact_and_repairs_empty_codewords() {
    // Break caught: residuals are materialized, assignment replay order or
    // block boundaries change the model, Lloyd runs other than 20 passes, or
    // duplicate values leave empty or unstable codewords.
    let owners = vec![vec![10.0_f32; 192], vec![100.0_f32; 192]];
    let fixture = |block_rows: usize, duplicate: bool| {
        let identities = (0_u64..16)
            .map(|source| {
                V36CoarseAssignmentIdentity::new(
                    source,
                    100 + source,
                    1_000 + source,
                    u32::try_from(source % 2).unwrap(),
                )
            })
            .collect::<Vec<_>>();
        let rows =
            identities
                .iter()
                .enumerate()
                .map(|(ordinal, identity)| {
                    let residual = if duplicate { 0.0 } else { ordinal as f32 };
                    vec![
                        owners[usize::try_from(identity.owner_posting_ordinal()).unwrap()][0]
                            + residual;
                        192
                    ]
                })
                .collect::<Vec<_>>();
        TestResidualSource {
            identities,
            rows,
            block_rows,
            scans: 0,
            mutate_after_first: false,
        }
    };

    for width in [V36ResidualPq4Width::Code32, V36ResidualPq4Width::Code48] {
        let mut scalar_blocks = fixture(1, false);
        let trained = train_v36_residual_pq4(&mut scalar_blocks, &owners, width).unwrap();
        assert_eq!(scalar_blocks.scans, 36);
        let mut wider_blocks = fixture(7, false);
        assert_eq!(
            train_v36_residual_pq4(&mut wider_blocks, &owners, width).unwrap(),
            trained
        );
        assert_eq!(wider_blocks.scans, 36);
        for subquantizer in 0..width.subquantizers() {
            let mut first_components = (0..16)
                .map(|codeword| trained.codeword(subquantizer, codeword).unwrap()[0])
                .collect::<Vec<_>>();
            first_components.sort_by(f32::total_cmp);
            assert_eq!(
                first_components,
                (0..16).map(|value| value as f32).collect::<Vec<_>>()
            );
        }

        let mut duplicates = fixture(4, true);
        let duplicate_model = train_v36_residual_pq4(&mut duplicates, &owners, width).unwrap();
        assert!(
            duplicate_model
                .centroids()
                .iter()
                .all(|value| *value == 0.0)
        );
        assert_eq!(duplicates.scans, 36);
    }

    let identities = (0_u64..32)
        .map(|source| V36CoarseAssignmentIdentity::new(source, source, source, 0))
        .collect::<Vec<_>>();
    let expected_order = [
        0.125_f32, 15.125, 7.125, 11.125, 3.125, 5.125, 9.125, 13.125, 1.125, 2.125, 4.125, 6.125,
        8.125, 10.125, 12.125, 14.125,
    ];
    let rows = (0..16)
        .flat_map(|cluster| [cluster as f32, cluster as f32 + 0.25])
        .map(|value| {
            (0..192)
                .map(|dimension| 10.0 + value + dimension as f32 / 1_024.0)
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    for width in [V36ResidualPq4Width::Code32, V36ResidualPq4Width::Code48] {
        let subvector_dimensions = match width {
            V36ResidualPq4Width::Code32 => 3,
            V36ResidualPq4Width::Code48 => 2,
        };
        let mut nontrivial = TestResidualSource {
            identities: identities.clone(),
            rows: rows.clone(),
            block_rows: 5,
            scans: 0,
            mutate_after_first: false,
        };
        let model = train_v36_residual_pq4(&mut nontrivial, &owners, width).unwrap();
        for subquantizer in 0..width.subquantizers() {
            for (codeword, expected_base) in expected_order.iter().copied().enumerate() {
                let start = subquantizer * subvector_dimensions;
                let expected = (0..subvector_dimensions)
                    .map(|component| expected_base + (start + component) as f32 / 1_024.0)
                    .collect::<Vec<_>>();
                assert_eq!(model.codeword(subquantizer, codeword).unwrap(), expected);
            }
        }
        assert_eq!(nontrivial.scans, 36);
    }

    let mut replicas = fixture(4, false);
    replicas.identities = (0_u64..8)
        .flat_map(|source| {
            [
                V36CoarseAssignmentIdentity::new(source, source, 10_000 + source, 0),
                V36CoarseAssignmentIdentity::new(source, source, 10_000 + source, 1),
            ]
        })
        .collect();
    replicas.rows = replicas
        .identities
        .iter()
        .enumerate()
        .map(|(ordinal, identity)| {
            vec![
                owners[usize::try_from(identity.owner_posting_ordinal()).unwrap()][0]
                    + ordinal as f32;
                192
            ]
        })
        .collect();
    assert!(train_v36_residual_pq4(&mut replicas, &owners, V36ResidualPq4Width::Code48).is_ok());

    let mut changing = fixture(4, false);
    changing.mutate_after_first = true;
    assert!(train_v36_residual_pq4(&mut changing, &owners, V36ResidualPq4Width::Code32).is_err());

    let mut too_small = fixture(4, false);
    too_small.identities.pop();
    too_small.rows.pop();
    assert!(train_v36_residual_pq4(&mut too_small, &owners, V36ResidualPq4Width::Code32).is_err());
    let mut unordered = fixture(4, false);
    unordered.identities.swap(2, 3);
    unordered.rows.swap(2, 3);
    assert!(train_v36_residual_pq4(&mut unordered, &owners, V36ResidualPq4Width::Code48).is_err());
}

#[test]
fn v36_posting_scores_share_slot_and_match_lower_tail_authority() {
    // Break caught: covariance arms receive more resident bytes than the
    // centroid control, or the generalized 192-D lower-tail formula drifts.
    let mut mean = vec![0.0_f32; 192];
    mean[2] = 1.0;
    let mut residual = vec![0.0_f32; 192];
    residual[0] = 1.0;
    residual[1] = 2.0;
    let mut directions = [(); 4].map(|_| vec![0.0_f32; 192]);
    directions[0][..4].copy_from_slice(&[0.5, 0.5, 0.5, 0.5]);
    directions[1][..4].copy_from_slice(&[0.5, -0.5, 0.5, -0.5]);
    let rank_two = V36PostingGaussianSummary::try_new(
        2,
        16,
        mean.clone(),
        residual,
        [4.0, 3.0, 0.0, 0.0],
        directions.clone(),
    )
    .unwrap();
    let mut diagonal = vec![0.0_f32; 192];
    diagonal[..4].copy_from_slice(&[2.75, 3.75, 1.75, 1.75]);
    let diagonal = V36PostingGaussianSummary::try_new(
        0,
        16,
        mean,
        diagonal,
        [0.0; 4],
        [(); 4].map(|_| vec![0.0; 192]),
    )
    .unwrap();
    let rank_four = V36PostingGaussianSummary::try_new(
        4,
        16,
        rank_two.mean().to_vec(),
        rank_two.residual_diagonal().to_vec(),
        [4.0, 3.0, 0.0, 0.0],
        directions,
    )
    .unwrap();
    let mut query = vec![0.0_f32; 192];
    query[0] = 2.0;
    query[1] = 3.0;
    query[2] = 1.0;

    let population_factor = (2.0 * 16.0_f64.ln()).sqrt();
    assert_eq!(
        score_v36_posting_centroid(rank_two.mean(), &query).unwrap(),
        13.0
    );
    assert_eq!(
        score_v36_posting_gaussian(&diagonal, &query).unwrap(),
        23.0 - population_factor * 234.5_f64.sqrt()
    );
    assert_eq!(
        score_v36_posting_gaussian(&rank_two, &query).unwrap(),
        23.0 - population_factor * 272.0_f64.sqrt()
    );
    assert_eq!(
        score_v36_posting_gaussian(&rank_four, &query).unwrap(),
        score_v36_posting_gaussian(&rank_two, &query).unwrap()
    );
    assert_eq!(diagonal.raw_bytes(), 1_540);
    assert_eq!(rank_two.raw_bytes(), 3_084);
    assert_eq!(rank_four.raw_bytes(), 4_628);
    assert_eq!(rank_four.population(), 16);
    assert_eq!(diagonal.used_bytes(), 1_564);
    assert_eq!(rank_two.used_bytes(), 3_108);
    assert_eq!(rank_four.used_bytes(), 4_652);
    assert_eq!(rank_four.resident_slot_bytes(), 4_736);
    assert!(std::mem::size_of::<V36PostingGaussianSummary>() <= 4_736);

    // Break caught: binary32-rounded directions are treated as perfectly
    // orthonormal instead of using their actual norms and dot products.
    let a = std::f32::consts::FRAC_1_SQRT_2;
    let mut rounded = [(); 4].map(|_| vec![0.0_f32; 192]);
    rounded[0][..2].copy_from_slice(&[a, a]);
    rounded[1][..2].copy_from_slice(&[a, -a]);
    let rounded = V36PostingGaussianSummary::try_new(
        2,
        16,
        rank_two.mean().to_vec(),
        rank_two.residual_diagonal().to_vec(),
        [4.0, 3.0, 0.0, 0.0],
        rounded,
    )
    .unwrap();
    let a = f64::from(a);
    let norm = 2.0 * a * a;
    let expected_trace = 3.0 + 7.0 * norm;
    let expected_trace_square = 5.0 + 42.0 * a * a + 25.0 * norm * norm;
    let expected_covariance_projection = 22.0 + 103.0 * a * a;
    let expected = 13.0 + expected_trace
        - population_factor
            * (2.0 * expected_trace_square + 4.0 * expected_covariance_projection).sqrt();
    assert_eq!(
        score_v36_posting_gaussian(&rounded, &query).unwrap(),
        expected
    );

    // Break caught: rank-four scoring truncates after two components or drops
    // the small cross-component terms introduced by binary32 rounding.
    let mut dense_directions = [(); 4].map(|_| vec![0.0_f32; 192]);
    dense_directions[0][..2].copy_from_slice(&[a as f32, a as f32]);
    dense_directions[1][..2].copy_from_slice(&[a as f32, -a as f32 + 1e-6]);
    dense_directions[2][2..4].copy_from_slice(&[a as f32, a as f32]);
    dense_directions[3][2..4].copy_from_slice(&[a as f32, -a as f32 + 2e-6]);
    let dense_residual = (0..192)
        .map(|dimension| match dimension {
            0 => 1.0,
            1 => 2.0,
            2 => 3.0,
            3 => 4.0,
            _ => 0.0,
        })
        .collect::<Vec<_>>();
    let dense = V36PostingGaussianSummary::try_new(
        4,
        16,
        rank_two.mean().to_vec(),
        dense_residual.clone(),
        [4.0, 3.0, 2.0, 1.0],
        dense_directions.clone(),
    )
    .unwrap();
    let mut dense_query = vec![0.0_f32; 192];
    dense_query[..4].copy_from_slice(&[2.0, 3.0, 1.0, 4.0]);
    let delta = dense_query
        .iter()
        .zip(dense.mean())
        .map(|(query, mean)| f64::from(*query) - f64::from(*mean))
        .collect::<Vec<_>>();
    let mut covariance = vec![vec![0.0_f64; 192]; 192];
    for dimension in 0..192 {
        covariance[dimension][dimension] = f64::from(dense_residual[dimension]);
    }
    for (eigenvalue, direction) in [4.0_f64, 3.0, 2.0, 1.0].into_iter().zip(&dense_directions) {
        for row in 0..192 {
            for column in 0..192 {
                covariance[row][column] +=
                    eigenvalue * f64::from(direction[row]) * f64::from(direction[column]);
            }
        }
    }
    let expected_trace = (0..192).map(|i| covariance[i][i]).sum::<f64>();
    let expected_trace_square = covariance
        .iter()
        .flatten()
        .map(|value| value * value)
        .sum::<f64>();
    let expected_covariance_projection = (0..192)
        .flat_map(|row| (0..192).map(move |column| (row, column)))
        .map(|(row, column)| delta[row] * covariance[row][column] * delta[column])
        .sum::<f64>();
    let expected_distance = delta.iter().map(|value| value * value).sum::<f64>();
    let expected = expected_distance + expected_trace
        - population_factor
            * (2.0 * expected_trace_square + 4.0 * expected_covariance_projection).sqrt();
    let actual = score_v36_posting_gaussian(&dense, &dense_query).unwrap();
    assert!((actual - expected).abs() <= 1e-12, "{actual} != {expected}");

    // Break caught: the shared deterministic logarithm drifts on a
    // non-power-of-two population where its fixed series is exercised.
    let population_twelve = V36PostingGaussianSummary::try_new(
        0,
        12,
        diagonal.mean().to_vec(),
        diagonal.residual_diagonal().to_vec(),
        [0.0; 4],
        [(); 4].map(|_| vec![0.0; 192]),
    )
    .unwrap();
    assert_eq!(
        score_v36_posting_gaussian(&population_twelve, &query)
            .unwrap()
            .to_bits(),
        0xc026_46ca_d39d_07d8
    );

    // Break caught: eigensolver sign ambiguity leaks into persisted authority.
    let mut negative_pivot = [(); 4].map(|_| vec![0.0_f32; 192]);
    negative_pivot[0][0] = -1.0;
    assert!(
        V36PostingGaussianSummary::try_new(
            2,
            16,
            rank_two.mean().to_vec(),
            rank_two.residual_diagonal().to_vec(),
            [4.0, 0.0, 0.0, 0.0],
            negative_pivot,
        )
        .is_err()
    );
    let mut negative_zero_residual = vec![0.0; 192];
    negative_zero_residual[17] = -0.0;
    assert!(
        V36PostingGaussianSummary::try_new(
            0,
            16,
            vec![0.0; 192],
            negative_zero_residual,
            [0.0; 4],
            [(); 4].map(|_| vec![0.0; 192]),
        )
        .is_err()
    );

    assert!(
        V36PostingGaussianSummary::try_new(
            1,
            16,
            vec![0.0; 192],
            vec![0.0; 192],
            [0.0; 4],
            [(); 4].map(|_| vec![0.0; 192]),
        )
        .is_err()
    );
    query[7] = f32::NAN;
    assert!(score_v36_posting_gaussian(&rank_four, &query).is_err());
}

#[test]
fn v36_prototype_six_is_equal_byte_deterministic_and_padded() {
    // Break caught: prototype-six becomes centroid plus six extra vectors,
    // depends on caller order, or scores padded slots as active prototypes.
    let row = |ordinal: u64, value: f32| {
        let mut vector = vec![0.0_f32; 192];
        vector[0] = value;
        (ordinal, vector)
    };
    let rows = vec![
        row(50, 10.0),
        row(10, 0.0),
        row(40, 8.0),
        row(20, 2.0),
        row(60, 12.0),
        row(30, 4.0),
    ];
    let mut reversed = rows.clone();
    reversed.reverse();
    let summary = train_v36_posting_prototype_six(&rows).unwrap();
    assert_eq!(
        summary
            .prototypes()
            .iter()
            .map(|prototype| prototype[0])
            .collect::<Vec<_>>(),
        vec![6.0, 0.0, 4.0, 12.0, 2.0, 9.0]
    );
    let reordered = train_v36_posting_prototype_six(&reversed).unwrap();
    assert_eq!(summary, reordered);
    assert_eq!(summary.population(), 6);
    assert_eq!(summary.active_subprototypes(), 5);
    assert_eq!(summary.raw_bytes(), 4_608);
    assert!(summary.used_bytes() <= summary.resident_slot_bytes());
    assert_eq!(summary.resident_slot_bytes(), 4_736);
    assert!(std::mem::size_of_val(&summary) <= 4_736);
    assert_eq!(summary.centroid()[0], 6.0);
    let mut query = vec![0.0_f32; 192];
    query[0] = 6.0;
    assert_eq!(
        score_v36_posting_prototype_six(&summary, &query).unwrap(),
        0.0
    );
    query[0] = 11.0;
    assert_eq!(
        score_v36_posting_prototype_six(&summary, &query).unwrap(),
        1.0
    );

    let singleton = train_v36_posting_prototype_six(&[row(7, 3.0)]).unwrap();
    assert_eq!(singleton.active_subprototypes(), 1);
    assert!(
        singleton
            .prototypes()
            .iter()
            .skip(2)
            .all(|prototype| prototype == singleton.centroid())
    );
    let duplicates =
        train_v36_posting_prototype_six(&[row(9, 3.0), row(7, 3.0), row(8, 3.0)]).unwrap();
    assert_eq!(duplicates.active_subprototypes(), 3);
    assert!(
        duplicates
            .prototypes()
            .iter()
            .all(|prototype| prototype == duplicates.centroid())
    );
    let empty_cluster = train_v36_posting_prototype_six(&[
        row(1, 0.0),
        row(2, 0.0),
        row(3, 10.0),
        row(4, 10.0),
        row(5, 20.0),
    ])
    .unwrap();
    assert_eq!(
        empty_cluster
            .prototypes()
            .iter()
            .map(|prototype| prototype[0])
            .collect::<Vec<_>>(),
        vec![8.0, 0.0, 10.0, 20.0, 0.0, 10.0]
    );
    let assignment_tie = train_v36_posting_prototype_six(&[
        row(1, 0.0),
        row(2, 2.0),
        row(3, 4.0),
        row(4, 6.0),
        row(5, 8.0),
        row(6, 10.0),
    ])
    .unwrap();
    assert_eq!(
        assignment_tie
            .prototypes()
            .iter()
            .map(|prototype| prototype[0])
            .collect::<Vec<_>>(),
        vec![5.0, 0.0, 5.0, 10.0, 2.0, 8.0]
    );
    assert!(train_v36_posting_prototype_six(&[row(7, 1.0), row(7, 2.0)]).is_err());
    assert!(train_v36_posting_prototype_six(&[(1, vec![0.0; 191])]).is_err());
    let mut nonfinite = row(1, 0.0);
    nonfinite.1[3] = f32::NAN;
    assert!(train_v36_posting_prototype_six(&[nonfinite]).is_err());
}

#[test]
fn v36_gaussian_training_reconstructs_rotated_covariance_by_rank() {
    // Break caught: covariance training uses the wrong divisor, loses rotated
    // components, or fails to fold omitted variance into a rank-specific
    // residual diagonal.
    let row = |ordinal: u64, x: f32, y: f32| {
        let mut vector = vec![0.0_f32; 192];
        vector[0] = x;
        vector[1] = y;
        (ordinal, vector)
    };
    let rows = vec![
        row(40, -1.0, 1.0),
        row(10, 2.0, 2.0),
        row(30, 1.0, -1.0),
        row(20, -2.0, -2.0),
    ];
    let diagonal = train_v36_posting_gaussian(&rows, 0).unwrap();
    let rank_two = train_v36_posting_gaussian(&rows, 2).unwrap();
    let rank_four = train_v36_posting_gaussian(&rows, 4).unwrap();
    assert_eq!(diagonal.rank(), 0);
    assert_eq!(rank_two.rank(), 2);
    assert_eq!(rank_four.rank(), 4);
    assert_eq!(diagonal.mean(), &[0.0; 192]);
    assert_eq!(&diagonal.residual_diagonal()[..2], &[2.5, 2.5]);
    assert_eq!(&rank_two.eigenvalues()[..2], &[4.0, 1.0]);
    assert_eq!(&rank_four.eigenvalues()[..2], &[4.0, 1.0]);
    assert_eq!(&rank_four.eigenvalues()[2..], &[0.0, 0.0]);
    assert!(
        rank_two.residual_diagonal()[..2]
            .iter()
            .all(|value| *value <= 1e-6)
    );
    assert!(rank_two.directions()[0][0] > 0.0);
    assert!(rank_two.directions()[0][1] > 0.0);
    assert!(rank_two.directions()[1][0] > 0.0);
    assert!(rank_two.directions()[1][1] < 0.0);
    let mut query = vec![0.0_f32; 192];
    query[0] = 3.0;
    query[1] = -2.0;
    let diagonal_score = score_v36_posting_gaussian(&diagonal, &query).unwrap();
    let rank_two_score = score_v36_posting_gaussian(&rank_two, &query).unwrap();
    let rank_four_score = score_v36_posting_gaussian(&rank_four, &query).unwrap();
    assert!((rank_two_score - rank_four_score).abs() <= 1e-12);
    assert!((diagonal_score - rank_two_score).abs() > 1.0);

    let mut reversed = rows.clone();
    reversed.reverse();
    assert_eq!(train_v36_posting_gaussian(&reversed, 4).unwrap(), rank_four);
    let singleton = train_v36_posting_gaussian(&[row(7, 3.0, -4.0)], 4).unwrap();
    assert_eq!(&singleton.mean()[..2], &[3.0, -4.0]);
    assert_eq!(singleton.eigenvalues(), &[0.0; 4]);
    assert!(
        singleton
            .directions()
            .iter()
            .flatten()
            .all(|value| *value == 0.0 && !value.is_sign_negative())
    );
    let isotropic = train_v36_posting_gaussian(
        &[
            row(1, 1.0, 0.0),
            row(2, -1.0, 0.0),
            row(3, 0.0, 1.0),
            row(4, 0.0, -1.0),
        ],
        2,
    )
    .unwrap();
    assert_eq!(&isotropic.eigenvalues()[..2], &[0.5, 0.5]);
    assert_eq!(&isotropic.directions()[0][..2], &[1.0, 0.0]);
    assert_eq!(&isotropic.directions()[1][..2], &[0.0, 1.0]);
    let mut large = vec![0.0_f32; 192];
    large[0] = 1_000_000.0;
    let mut negative_large = vec![0.0_f32; 192];
    negative_large[0] = -1_000_000.0;
    let mut small = vec![0.0_f32; 192];
    small[191] = 1.0;
    let mut negative_small = vec![0.0_f32; 192];
    negative_small[191] = -1.0;
    let separated = train_v36_posting_gaussian(
        &[
            (1, large.clone()),
            (2, negative_large),
            (3, small.clone()),
            (4, negative_small),
        ],
        2,
    )
    .unwrap();
    assert_eq!(&separated.eigenvalues()[..2], &[5e11_f32, 0.5]);
    assert_eq!(separated.directions()[0][0], 1.0);
    assert_eq!(separated.directions()[1][191], 1.0);
    assert_eq!(separated.residual_diagonal()[0], 2_048.0);
    assert_eq!(separated.residual_diagonal()[191], 0.0);
    assert!(train_v36_posting_gaussian(&[row(1, 0.0, 0.0), row(1, 1.0, 1.0)], 2).is_err());
    let mut negative_zero = row(1, 0.0, 0.0);
    negative_zero.1[9] = -0.0;
    assert!(train_v36_posting_gaussian(&[negative_zero], 0).is_err());
    assert!(train_v36_posting_gaussian(&rows, 1).is_err());
}

#[test]
fn v36_srht192_control_reuses_the_fused_generic_projection() {
    // Break caught: V36 silently changes dimensions/seed or grows a separate
    // scalar projection whose coordinates drift from the fused query path.
    let projection = build_v36_srht192_control().unwrap();
    assert_eq!(projection.dimensions().source, 768);
    assert_eq!(projection.dimensions().routing, 192);
    assert_eq!(projection.seed(), 36);
    assert_eq!(projection.algorithm(), "srht-prefix-orthonormalized-v1");

    let query = (0..768)
        .map(|dimension| ((dimension % 31) as f32 - 15.0) / 32.0)
        .collect::<Vec<_>>();
    let scalar = project_v35_query_scalar(&projection, &query).unwrap();
    let simd = project_v35_query_simd(&projection, &query).unwrap();
    assert_eq!(scalar.coordinates(), simd.coordinates());
    assert_eq!(scalar.complement_energy(), simd.complement_energy());
    assert_ne!(simd.backend(), V35ProjectionBackend::ScalarControl);
}

#[test]
fn v36_centered_covariance_streams_translation_invariant_full_spectrum_evidence() {
    // Break caught: block-local reductions, an uncentered second moment, or an
    // unauthenticated second pass changes projection identity or energy.
    let rows = vec![
        vec![11.0, 22.0, 30.0],
        vec![9.0, 18.0, 30.0],
        vec![11.0, 18.0, 30.0],
        vec![9.0, 22.0, 30.0],
    ];
    let spec = centered_training_spec(
        3,
        2,
        "bb2dc686dcdbe09129f2fc2eb285e3391410d5f4c7a2f530213cb11a99706396",
        "ccbd798f5fb1cecb3409b14e5f87c86310cf597078129f3fbf4ca46b595550f4",
        vec![1, 2, 3],
    );
    let one_row = train_v36_centered_subspace(
        &spec,
        &mut TestProjectionSource {
            corpus_rows: rows.clone(),
            reservoir_rows: rows.clone(),
            block_rows: 1,
            mutate_reservoir: false,
        },
    )
    .unwrap();
    let three_rows = train_v36_centered_subspace(
        &spec,
        &mut TestProjectionSource {
            corpus_rows: rows.clone(),
            reservoir_rows: rows.clone(),
            block_rows: 3,
            mutate_reservoir: false,
        },
    )
    .unwrap();

    assert_eq!(one_row, three_rows);
    assert_eq!(one_row.mean(), [10.0, 20.0, 30.0]);
    assert_eq!(
        one_row.mean_sha256(),
        "f321334d8b936bd1e31bb89f100c8acdf6f246c9b2e94ddc01a451c4cee48295"
    );
    assert_eq!(
        one_row.covariance_sha256(),
        "adc8b1865b02a45137632026311b2cd7133f3eea3b6e863734ff4b57577e72c1"
    );
    assert_eq!(one_row.eigenvalues(), [4.0, 1.0, 0.0]);
    assert_eq!(
        one_row.retained_energy_ppm(),
        [800_000, 1_000_000, 1_000_000]
    );
    assert_eq!(one_row.basis_source_major(), [0.0, 1.0, 1.0, 0.0, 0.0, 0.0]);
    assert!(one_row.max_eigenpair_relative_residual() <= 1e-10);
    assert!(one_row.reconstruction_relative_error() <= 1e-10);

    let changed = train_v36_centered_subspace(
        &spec,
        &mut TestProjectionSource {
            corpus_rows: rows.clone(),
            reservoir_rows: rows,
            block_rows: 3,
            mutate_reservoir: true,
        },
    );
    assert!(changed.is_err());
}

#[test]
fn v36_centered_covariance_canonicalizes_repeated_eigenspace_before_truncation() {
    // Break caught: sign fixing alone cannot make an arbitrary solver rotation
    // deterministic when an equal-eigenvalue cluster crosses retained M.
    let rows = vec![
        vec![1.0, 0.0, 0.0],
        vec![-1.0, 0.0, 0.0],
        vec![0.0, 1.0, 0.0],
        vec![0.0, -1.0, 0.0],
    ];
    let spec = centered_training_spec(
        3,
        1,
        "212cc8493b13f629db89c6ae99b19a0ca35c2f24110619ba913ddb5e85b6f96d",
        "3a7447a4d88cf24e38a8c31774d1e4806710230af1421cabcd1b3c219d026d1b",
        vec![1, 2],
    );

    let projection = train_v36_centered_subspace(
        &spec,
        &mut TestProjectionSource {
            corpus_rows: rows.clone(),
            reservoir_rows: rows,
            block_rows: 2,
            mutate_reservoir: false,
        },
    )
    .unwrap();

    assert_eq!(projection.eigenvalues(), [0.5, 0.5, 0.0]);
    assert_eq!(projection.basis_source_major(), [1.0, 0.0, 0.0]);
    assert_eq!(projection.retained_energy_ppm(), [500_000, 1_000_000]);
}

#[test]
fn v36_centered_covariance_uses_corpus_mean_for_off_diagonal_reservoir_moments() {
    // Break caught: silently recentering on the reservoir changes both the
    // off-diagonal covariance and the registered retained spectrum.
    let corpus_rows = vec![vec![10.0, 20.0, 30.0]; 4];
    let reservoir_rows = vec![
        vec![11.0, 22.0, 30.0],
        vec![11.0, 22.0, 30.0],
        vec![13.0, 26.0, 30.0],
        vec![13.0, 26.0, 30.0],
    ];
    let spec = centered_training_spec(
        3,
        1,
        "d2272823d9d7d32a6eb95601229f084f90fa1c6b5e7a9707cc114bbd337c5cc7",
        "45279d95a26c02e856d1ee6883c042c705d333bff928908dd3576381de4b0fcf",
        vec![1, 2, 3],
    );

    let projection = train_v36_centered_subspace(
        &spec,
        &mut TestProjectionSource {
            corpus_rows,
            reservoir_rows,
            block_rows: 2,
            mutate_reservoir: false,
        },
    )
    .unwrap();

    assert_eq!(projection.mean(), [10.0, 20.0, 30.0]);
    assert_eq!(projection.eigenvalues(), [25.0, 0.0, 0.0]);
    let expected = [1.0_f32 / 5.0_f32.sqrt(), 2.0_f32 / 5.0_f32.sqrt(), 0.0];
    for (actual, expected) in projection.basis_source_major().iter().zip(expected) {
        assert!((*actual - expected).abs() <= f32::EPSILON);
    }
    assert_eq!(
        projection.retained_energy_ppm(),
        [1_000_000, 1_000_000, 1_000_000]
    );
}

#[test]
fn v36_centered_covariance_canonicalizes_non_axis_aligned_repeated_plane() {
    // Break caught: a repeated non-coordinate eigenspace retains nalgebra's
    // arbitrary rotated columns rather than projected-coordinate axes.
    let rows = vec![
        vec![1.0, 1.0, 0.0, 0.0],
        vec![-1.0, -1.0, 0.0, 0.0],
        vec![0.0, 0.0, 1.0, 1.0],
        vec![0.0, 0.0, -1.0, -1.0],
    ];
    let spec = centered_training_spec(
        4,
        1,
        "41a58172c4bb4b9db89eaa1dd535405828d3e5246bcc8a744f3322d78e2516b8",
        "30b6128aab5cb66541a75e8729ed87fa59265c43023a3916f3a0ddf4fc48c9d6",
        vec![1, 2, 4],
    );

    let projection = train_v36_centered_subspace(
        &spec,
        &mut TestProjectionSource {
            corpus_rows: rows.clone(),
            reservoir_rows: rows,
            block_rows: 2,
            mutate_reservoir: false,
        },
    )
    .unwrap();

    assert_eq!(projection.eigenvalues(), [1.0, 1.0, 0.0, 0.0]);
    let inverse_sqrt_two = 1.0_f32 / 2.0_f32.sqrt();
    let expected = [inverse_sqrt_two, inverse_sqrt_two, 0.0, 0.0];
    for (actual, expected) in projection.basis_source_major().iter().zip(expected) {
        assert!((*actual - expected).abs() <= f32::EPSILON);
    }
    assert_eq!(
        projection.retained_energy_ppm(),
        [500_000, 1_000_000, 1_000_000]
    );
}

#[test]
fn v36_centered_covariance_rejects_zero_signal_numerically() {
    // Break caught: an all-constant reservoir publishes an arbitrary nullspace
    // as if it were an admitted principal projection.
    let rows = vec![vec![1.0, 2.0, 3.0]; 4];
    let spec = centered_training_spec(
        3,
        1,
        "1bd398622bda1155d329595b3d25989f7fec098520e24255de924b0fd9f3ac2a",
        "1a523488674ff0f8c55ca80b75aa593f41dfca79b423da3083c808b7212688c4",
        vec![1],
    );

    assert!(
        train_v36_centered_subspace(
            &spec,
            &mut TestProjectionSource {
                corpus_rows: rows.clone(),
                reservoir_rows: rows,
                block_rows: 2,
                mutate_reservoir: false,
            },
        )
        .is_err()
    );
}

#[test]
fn v36_centered_covariance_rejects_unbounded_workspace_before_scanning() {
    // Break caught: malformed dimensions allocate an attacker-sized dense
    // covariance before the registered training workspace gate runs.
    struct PanicSource;
    impl V36CenteredProjectionSource for PanicSource {
        fn scan(
            &mut self,
            _role: V36CenteredSampleRole,
            _visitor: &mut V36CenteredProjectionBlockVisitor<'_>,
        ) -> Result<()> {
            panic!("workspace authority must reject before source access")
        }
    }
    let spec = V36CenteredProjectionTrainingSpec {
        source_dimensions: 2_000,
        retained_dimensions: 1,
        corpus_rows: 1,
        corpus_sha256: "0".repeat(64),
        reservoir_rows: 1,
        reservoir_sha256: "1".repeat(64),
        maximum_block_rows: 1,
        energy_dimensions: vec![1],
    };

    assert!(train_v36_centered_subspace(&spec, &mut PanicSource).is_err());
}

#[test]
fn v36_centered_projection_serving_is_fused_and_translation_invariant() {
    // Break caught: the learned basis is served through an uncentered or
    // separately ordered arithmetic path, changing routing under translation.
    let rows = vec![
        vec![2.0, 0.0, 0.0, 0.0],
        vec![-2.0, 0.0, 0.0, 0.0],
        vec![0.0, 1.0, 0.0, 0.0],
        vec![0.0, -1.0, 0.0, 0.0],
        vec![0.0, 0.0, 0.5, 0.0],
        vec![0.0, 0.0, -0.5, 0.0],
        vec![0.0, 0.0, 0.0, 0.25],
        vec![0.0, 0.0, 0.0, -0.25],
    ];
    let translation = [100.0_f32, -50.0, 25.0, 7.0];
    let shifted = rows
        .iter()
        .map(|row| {
            row.iter()
                .zip(translation)
                .map(|(value, offset)| value + offset)
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();

    let train = |training_rows: Vec<Vec<f32>>| {
        let digest = centered_source_digest(b"borsuk-v36-centered-corpus-v1\n", &training_rows);
        let reservoir_digest =
            centered_source_digest(b"borsuk-v36-centered-reservoir-v1\n", &training_rows);
        train_v36_centered_subspace(
            &V36CenteredProjectionTrainingSpec {
                source_dimensions: 4,
                retained_dimensions: 4,
                corpus_rows: 8,
                corpus_sha256: digest,
                reservoir_rows: 8,
                reservoir_sha256: reservoir_digest,
                maximum_block_rows: 3,
                energy_dimensions: vec![1, 2, 4],
            },
            &mut TestProjectionSource {
                corpus_rows: training_rows.clone(),
                reservoir_rows: training_rows,
                block_rows: 3,
                mutate_reservoir: false,
            },
        )
        .unwrap()
    };
    let projection = train(rows);
    let shifted_projection = train(shifted);
    let row = [1.5_f32, -0.75, 0.375, -0.125];
    let shifted_row: [f32; 4] = std::array::from_fn(|index| row[index] + translation[index]);

    let scalar = project_v36_centered_row_scalar(&projection, &row).unwrap();
    let simd = project_v36_centered_row_simd(&projection, &row).unwrap();
    let shifted_simd = project_v36_centered_row_simd(&shifted_projection, &shifted_row).unwrap();

    assert_eq!(scalar.coordinates(), simd.coordinates());
    assert_eq!(simd.coordinates(), shifted_simd.coordinates());
    assert!(
        project_v36_centered_row_simd(&projection, &[0.0; 4])
            .unwrap()
            .coordinates()
            .iter()
            .all(|value| value.to_bits() == 0)
    );
    assert_ne!(simd.backend(), V35ProjectionBackend::ScalarControl);
    assert_ne!(shifted_simd.backend(), V35ProjectionBackend::ScalarControl);
    assert!(project_v36_centered_row_simd(&projection, &row[..3]).is_err());
    assert!(project_v36_centered_row_scalar(&projection, &row[..3]).is_err());
    let mut nonfinite = row;
    nonfinite[2] = f32::NAN;
    assert!(project_v36_centered_row_simd(&projection, &nonfinite).is_err());
    assert!(project_v36_centered_row_scalar(&projection, &nonfinite).is_err());
}

#[test]
fn v36_centered_projection_matches_dense_ordered_f64_reference() {
    // Break caught: an identity-like four-output fixture cannot detect a
    // transposed basis, a broken second SIMD block, or premature mean rounding.
    let rows = (0..20)
        .map(|row| {
            (0..9)
                .map(|dimension| {
                    let mixed = (row * 13 + dimension * 7 + row * dimension * 3) % 37;
                    (mixed as f32 - 18.0) / 7.0 + 0.1 * (dimension + 1) as f32
                })
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    let corpus_sha256 = centered_source_digest(b"borsuk-v36-centered-corpus-v1\n", &rows);
    let reservoir_sha256 = centered_source_digest(b"borsuk-v36-centered-reservoir-v1\n", &rows);
    let projection = train_v36_centered_subspace(
        &V36CenteredProjectionTrainingSpec {
            source_dimensions: 9,
            retained_dimensions: 8,
            corpus_rows: 20,
            corpus_sha256,
            reservoir_rows: 20,
            reservoir_sha256,
            maximum_block_rows: 6,
            energy_dimensions: vec![1, 8, 9],
        },
        &mut TestProjectionSource {
            corpus_rows: rows.clone(),
            reservoir_rows: rows.clone(),
            block_rows: 6,
            mutate_reservoir: false,
        },
    )
    .unwrap();
    assert_eq!(projection.retained_dimensions(), 8);
    assert!(
        projection
            .mean()
            .iter()
            .any(|mean| f64::from(*mean as f32).to_bits() != mean.to_bits())
    );

    let source = [
        2.125_f32,
        -1.75,
        0.0625,
        8.5,
        -3.25,
        0.333_333_34,
        4.0,
        -0.875,
        1.5,
    ];
    let mut expected = [0.0_f64; 8];
    for (dimension, (value, mean)) in source.iter().zip(projection.mean()).enumerate() {
        let centered = (f64::from(*value) - mean) as f32;
        for (output, coordinate) in expected.iter_mut().enumerate() {
            let coefficient = projection.basis_source_major()[dimension * 8 + output];
            *coordinate = f64::from(coefficient).mul_add(f64::from(centered), *coordinate);
        }
    }
    expected.iter_mut().for_each(|value| {
        if *value == 0.0 {
            *value = 0.0;
        }
    });

    let scalar = project_v36_centered_row_scalar(&projection, &source).unwrap();
    let simd = project_v36_centered_row_simd(&projection, &source).unwrap();
    assert_eq!(
        scalar
            .coordinates()
            .iter()
            .map(|value| value.to_bits())
            .collect::<Vec<_>>(),
        expected
            .iter()
            .map(|value| value.to_bits())
            .collect::<Vec<_>>()
    );
    assert_eq!(
        simd.coordinates()
            .iter()
            .map(|value| value.to_bits())
            .collect::<Vec<_>>(),
        expected
            .iter()
            .map(|value| value.to_bits())
            .collect::<Vec<_>>()
    );
    assert_ne!(simd.backend(), V35ProjectionBackend::ScalarControl);

    let odd_projection = train_v36_centered_subspace(
        &V36CenteredProjectionTrainingSpec {
            source_dimensions: 9,
            retained_dimensions: 3,
            corpus_rows: 20,
            corpus_sha256: centered_source_digest(b"borsuk-v36-centered-corpus-v1\n", &rows),
            reservoir_rows: 20,
            reservoir_sha256: centered_source_digest(b"borsuk-v36-centered-reservoir-v1\n", &rows),
            maximum_block_rows: 6,
            energy_dimensions: vec![1, 3, 9],
        },
        &mut TestProjectionSource {
            corpus_rows: rows.clone(),
            reservoir_rows: rows,
            block_rows: 6,
            mutate_reservoir: false,
        },
    )
    .unwrap();
    assert!(project_v36_centered_row_scalar(&odd_projection, &source).is_ok());
    assert!(project_v36_centered_row_simd(&odd_projection, &source).is_err());
}

#[test]
fn v36_centered_projection_arrow_binds_training_and_complete_object_identity() {
    // Break caught: a valid-looking basis from a different training population
    // is substituted after arm selection, or Arrow framing changes unnoticed.
    let rows = vec![
        vec![2.0, 0.0, 0.0, 0.0],
        vec![-2.0, 0.0, 0.0, 0.0],
        vec![0.0, 1.0, 0.0, 0.0],
        vec![0.0, -1.0, 0.0, 0.0],
        vec![0.0, 0.0, 0.5, 0.0],
        vec![0.0, 0.0, -0.5, 0.0],
        vec![0.0, 0.0, 0.0, 0.25],
        vec![0.0, 0.0, 0.0, -0.25],
    ];
    let spec = V36CenteredProjectionTrainingSpec {
        source_dimensions: 4,
        retained_dimensions: 4,
        corpus_rows: 8,
        corpus_sha256: centered_source_digest(b"borsuk-v36-centered-corpus-v1\n", &rows),
        reservoir_rows: 8,
        reservoir_sha256: centered_source_digest(b"borsuk-v36-centered-reservoir-v1\n", &rows),
        maximum_block_rows: 3,
        energy_dimensions: vec![1, 2, 4],
    };
    let projection = train_v36_centered_subspace(
        &spec,
        &mut TestProjectionSource {
            corpus_rows: rows.clone(),
            reservoir_rows: rows.clone(),
            block_rows: 3,
            mutate_reservoir: false,
        },
    )
    .unwrap();
    let uri = "s3://borsuk-v36-test/projections/centered.arrow";
    let (bytes, identity) =
        encode_v36_centered_projection_arrow(&projection, "centered-projection-basis", uri)
            .unwrap();
    let pyarrow_bytes =
        decode_base64_fixture(include_str!("fixtures/v36_centered_projection_pyarrow.b64"));
    assert_eq!(pyarrow_bytes.len(), 3_482);
    assert_eq!(
        format!("{:x}", Sha256::digest(&pyarrow_bytes)),
        "d5dc7479da5012cbeff7d794f4033a11fb306bfb93bf74aeb089d32f092d2a71"
    );
    let pyarrow_identity = borsuk::V36ArtifactIdentity {
        blake3: blake3::hash(&pyarrow_bytes).to_hex().to_string(),
        encoded_bytes: pyarrow_bytes.len() as u64,
        role: "centered-projection-basis".to_owned(),
        sha256: format!("{:x}", Sha256::digest(&pyarrow_bytes)),
        uri: uri.to_owned(),
    };
    assert_eq!(
        decode_v36_centered_projection_arrow(&pyarrow_bytes, &pyarrow_identity, &spec).unwrap(),
        projection
    );
    assert_eq!(identity.role, "centered-projection-basis");
    assert_eq!(identity.uri, uri);
    assert_eq!(identity.encoded_bytes, bytes.len() as u64);
    assert!(identity.encoded_bytes < 3 * 1_048_576);
    assert_eq!(identity.sha256, format!("{:x}", Sha256::digest(&bytes)));
    assert_eq!(identity.blake3, blake3::hash(&bytes).to_hex().as_str());
    let manifest = {
        let reader = FileReader::try_new(Cursor::new(&bytes), None).unwrap();
        serde_json::from_str::<serde_json::Value>(
            reader
                .schema()
                .metadata()
                .get("borsuk.v36.centered_projection.manifest")
                .unwrap(),
        )
        .unwrap()
    };
    for key in [
        "max_eigenpair_relative_residual_bits",
        "reconstruction_relative_error_bits",
    ] {
        let bits = manifest.get(key).unwrap().as_str().unwrap();
        assert_eq!(bits.len(), 16);
        assert!(
            bits.bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        );
    }
    assert_eq!(
        decode_v36_centered_projection_arrow(&bytes, &identity, &spec).unwrap(),
        projection
    );

    let mut baseline_reader = FileReader::try_new(Cursor::new(&bytes), None).unwrap();
    let baseline_schema = baseline_reader.schema();
    let baseline_batch = baseline_reader.next().unwrap().unwrap();
    assert!(baseline_reader.next().is_none());
    let emit =
        |fields: Vec<Field>, columns: Vec<ArrayRef>, manifest_text: String, batch_count: usize| {
            let mut metadata = HashMap::new();
            metadata.insert(
                "borsuk.v36.centered_projection.manifest".to_owned(),
                manifest_text,
            );
            let changed_schema = Arc::new(Schema::new_with_metadata(fields, metadata));
            let changed_batch = RecordBatch::try_new(changed_schema.clone(), columns).unwrap();
            let mut changed_bytes = Vec::new();
            let options = IpcWriteOptions::try_new(8, false, MetadataVersion::V5).unwrap();
            let mut writer =
                FileWriter::try_new_with_options(&mut changed_bytes, &changed_schema, options)
                    .unwrap();
            for _ in 0..batch_count {
                writer.write(&changed_batch).unwrap();
            }
            writer.finish().unwrap();
            drop(writer);
            let mut changed_identity = identity.clone();
            changed_identity.encoded_bytes = changed_bytes.len() as u64;
            changed_identity.sha256 = format!("{:x}", Sha256::digest(&changed_bytes));
            changed_identity.blake3 = blake3::hash(&changed_bytes).to_hex().to_string();
            (changed_bytes, changed_identity)
        };
    let rewrite_manifest = |mutate: &dyn Fn(&mut serde_json::Value)| {
        let manifest_text = baseline_schema
            .metadata()
            .get("borsuk.v36.centered_projection.manifest")
            .unwrap();
        let mut manifest: serde_json::Value = serde_json::from_str(manifest_text).unwrap();
        mutate(&mut manifest);
        emit(
            baseline_schema
                .fields()
                .iter()
                .map(|field| field.as_ref().clone())
                .collect(),
            baseline_batch.columns().to_vec(),
            serde_json::to_string(&manifest).unwrap(),
            1,
        )
    };
    let rewrite_numerical_evidence = |key: &str, value: serde_json::Value| {
        rewrite_manifest(&|manifest| {
            manifest
                .as_object_mut()
                .unwrap()
                .insert(key.to_owned(), value.clone());
        })
    };
    for (key, bits) in [
        ("max_eigenpair_relative_residual_bits", (-1.0_f64).to_bits()),
        ("max_eigenpair_relative_residual_bits", f64::NAN.to_bits()),
        (
            "max_eigenpair_relative_residual_bits",
            f64::INFINITY.to_bits(),
        ),
        (
            "max_eigenpair_relative_residual_bits",
            (1.1e-10_f64).to_bits(),
        ),
        ("reconstruction_relative_error_bits", (-1.0_f64).to_bits()),
        ("reconstruction_relative_error_bits", f64::NAN.to_bits()),
        (
            "reconstruction_relative_error_bits",
            f64::INFINITY.to_bits(),
        ),
        (
            "reconstruction_relative_error_bits",
            (1.1e-10_f64).to_bits(),
        ),
    ] {
        let (changed_bytes, changed_identity) =
            rewrite_numerical_evidence(key, format!("{bits:016x}").into());
        assert!(
            decode_v36_centered_projection_arrow(&changed_bytes, &changed_identity, &spec).is_err()
        );
    }
    for malformed in [
        serde_json::Value::from(0_u64),
        serde_json::Value::from("000000000000000"),
        serde_json::Value::from("000000000000000G"),
        serde_json::Value::from("000000000000000A"),
    ] {
        let (changed_bytes, changed_identity) =
            rewrite_numerical_evidence("max_eigenpair_relative_residual_bits", malformed);
        assert!(
            decode_v36_centered_projection_arrow(&changed_bytes, &changed_identity, &spec).is_err()
        );
    }
    for (changed_bytes, changed_identity) in [
        rewrite_manifest(&|manifest| {
            manifest["retained_energy_ppm"][0] = 0_u64.into();
        }),
        rewrite_manifest(&|manifest| {
            manifest["basis_sha256"] = "1".repeat(64).into();
        }),
        {
            let manifest = baseline_schema
                .metadata()
                .get("borsuk.v36.centered_projection.manifest")
                .unwrap();
            let mut noncanonical = manifest.clone();
            noncanonical.insert(1, ' ');
            emit(
                baseline_schema
                    .fields()
                    .iter()
                    .map(|field| field.as_ref().clone())
                    .collect(),
                baseline_batch.columns().to_vec(),
                noncanonical,
                1,
            )
        },
        {
            let mut fields = baseline_schema
                .fields()
                .iter()
                .map(|field| field.as_ref().clone())
                .collect::<Vec<_>>();
            fields[0] = Field::new("mean", DataType::Float64, true);
            emit(
                fields,
                baseline_batch.columns().to_vec(),
                baseline_schema
                    .metadata()
                    .get("borsuk.v36.centered_projection.manifest")
                    .unwrap()
                    .clone(),
                1,
            )
        },
        {
            let mut fields = baseline_schema
                .fields()
                .iter()
                .map(|field| field.as_ref().clone())
                .collect::<Vec<_>>();
            fields[2] = Field::new(
                "basis",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, false)), 4),
                false,
            );
            let basis = baseline_batch
                .column(2)
                .as_any()
                .downcast_ref::<FixedSizeListArray>()
                .unwrap();
            let mut columns = baseline_batch.columns().to_vec();
            columns[2] = Arc::new(
                FixedSizeListArray::try_new(
                    Arc::new(Field::new("item", DataType::Float32, false)),
                    4,
                    basis.values().clone(),
                    None,
                )
                .unwrap(),
            );
            emit(
                fields,
                columns,
                baseline_schema
                    .metadata()
                    .get("borsuk.v36.centered_projection.manifest")
                    .unwrap()
                    .clone(),
                1,
            )
        },
        emit(
            baseline_schema
                .fields()
                .iter()
                .map(|field| field.as_ref().clone())
                .collect(),
            baseline_batch.columns().to_vec(),
            baseline_schema
                .metadata()
                .get("borsuk.v36.centered_projection.manifest")
                .unwrap()
                .clone(),
            2,
        ),
        {
            let mut changed_bytes = Vec::new();
            let options = IpcWriteOptions::default()
                .try_with_compression(Some(arrow_ipc::CompressionType::ZSTD))
                .unwrap();
            let mut writer = FileWriter::try_new_with_options(
                &mut changed_bytes,
                baseline_schema.as_ref(),
                options,
            )
            .unwrap();
            writer.write(&baseline_batch).unwrap();
            writer.finish().unwrap();
            drop(writer);
            let mut changed_identity = identity.clone();
            changed_identity.encoded_bytes = changed_bytes.len() as u64;
            changed_identity.sha256 = format!("{:x}", Sha256::digest(&changed_bytes));
            changed_identity.blake3 = blake3::hash(&changed_bytes).to_hex().to_string();
            (changed_bytes, changed_identity)
        },
        {
            let mut changed_bytes = Vec::new();
            let options = IpcWriteOptions::try_new(8, false, MetadataVersion::V5).unwrap();
            let mut writer = FileWriter::try_new_with_options(
                &mut changed_bytes,
                baseline_schema.as_ref(),
                options,
            )
            .unwrap();
            writer.write(&baseline_batch).unwrap();
            writer.write_metadata("forbidden", "footer metadata");
            writer.finish().unwrap();
            drop(writer);
            let mut changed_identity = identity.clone();
            changed_identity.encoded_bytes = changed_bytes.len() as u64;
            changed_identity.sha256 = format!("{:x}", Sha256::digest(&changed_bytes));
            changed_identity.blake3 = blake3::hash(&changed_bytes).to_hex().to_string();
            (changed_bytes, changed_identity)
        },
        {
            let mut columns = baseline_batch.columns().to_vec();
            let mut eigenvalues = columns[1]
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .values()
                .to_vec();
            eigenvalues.reverse();
            columns[1] = Arc::new(Float64Array::new(eigenvalues.into(), None));
            emit(
                baseline_schema
                    .fields()
                    .iter()
                    .map(|field| field.as_ref().clone())
                    .collect(),
                columns,
                baseline_schema
                    .metadata()
                    .get("borsuk.v36.centered_projection.manifest")
                    .unwrap()
                    .clone(),
                1,
            )
        },
        {
            let mut columns = baseline_batch.columns().to_vec();
            let basis = columns[2]
                .as_any()
                .downcast_ref::<FixedSizeListArray>()
                .unwrap();
            let mut coefficients = basis
                .values()
                .as_any()
                .downcast_ref::<Float32Array>()
                .unwrap()
                .values()
                .to_vec();
            coefficients[0] = 0.5;
            columns[2] = Arc::new(
                FixedSizeListArray::try_new(
                    Arc::new(Field::new("element", DataType::Float32, false)),
                    4,
                    Arc::new(Float32Array::new(coefficients.into(), None)),
                    None,
                )
                .unwrap(),
            );
            emit(
                baseline_schema
                    .fields()
                    .iter()
                    .map(|field| field.as_ref().clone())
                    .collect(),
                columns,
                baseline_schema
                    .metadata()
                    .get("borsuk.v36.centered_projection.manifest")
                    .unwrap()
                    .clone(),
                1,
            )
        },
    ] {
        assert!(
            decode_v36_centered_projection_arrow(&changed_bytes, &changed_identity, &spec).is_err()
        );
    }
    assert!(encode_v36_centered_projection_arrow(&projection, "wrong-role", uri).is_err());
    assert!(
        encode_v36_centered_projection_arrow(&projection, "centered-projection-basis", "").is_err()
    );

    for changed_identity in [
        {
            let mut value = identity.clone();
            value.uri.push_str(".other");
            value
        },
        {
            let mut value = identity.clone();
            value.role = "other-role".to_owned();
            value
        },
        {
            let mut value = identity.clone();
            value.encoded_bytes += 1;
            value
        },
        {
            let mut value = identity.clone();
            value.sha256 = "1".repeat(64);
            value
        },
        {
            let mut value = identity.clone();
            value.blake3 = "2".repeat(64);
            value
        },
    ] {
        assert!(decode_v36_centered_projection_arrow(&bytes, &changed_identity, &spec).is_err());
    }
    let mut corrupted = bytes.clone();
    let middle = corrupted.len() / 2;
    corrupted[middle] ^= 1;
    assert!(decode_v36_centered_projection_arrow(&corrupted, &identity, &spec).is_err());
    let rewrite_footer_body_length = |body_length: i64| {
        let mut value = bytes.clone();
        let trailer = value.len() - 10;
        let footer_length =
            u32::from_le_bytes(value[trailer..trailer + 4].try_into().unwrap()) as usize;
        let footer_start = trailer - footer_length;
        let block_offset = {
            let footer = arrow_ipc::root_as_footer(&value[footer_start..trailer]).unwrap();
            let block = footer.recordBatches().unwrap().get(0);
            block.0.as_ptr() as usize - value.as_ptr() as usize
        };
        value[block_offset + 16..block_offset + 24].copy_from_slice(&body_length.to_le_bytes());
        value
    };
    let missing_manifest_key = {
        let mut value = bytes.clone();
        let trailer = value.len() - 10;
        let footer_length =
            u32::from_le_bytes(value[trailer..trailer + 4].try_into().unwrap()) as usize;
        let footer_start = trailer - footer_length;
        let key_slot = {
            let footer = arrow_ipc::root_as_footer(&value[footer_start..trailer]).unwrap();
            let key_value = footer.schema().unwrap().custom_metadata().unwrap().get(0);
            let table = key_value._tab.loc();
            let displacement = i32::from_le_bytes(
                value[footer_start + table..footer_start + table + 4]
                    .try_into()
                    .unwrap(),
            );
            let vtable = (table as i64 - i64::from(displacement)) as usize;
            footer_start + vtable + usize::from(arrow_ipc::KeyValue::VT_KEY)
        };
        value[key_slot..key_slot + 2].fill(0);
        value
    };
    for malformed in [
        b"ARROW1\0\0\0\0\0\0ARROW1".to_vec(),
        {
            let mut value = bytes.clone();
            let footer_length_offset = value.len() - 10;
            value[footer_length_offset..footer_length_offset + 4]
                .copy_from_slice(&u32::MAX.to_le_bytes());
            value
        },
        rewrite_footer_body_length(-1),
        rewrite_footer_body_length(i64::MAX),
        missing_manifest_key,
    ] {
        let mut malformed_identity = identity.clone();
        malformed_identity.encoded_bytes = malformed.len() as u64;
        malformed_identity.sha256 = format!("{:x}", Sha256::digest(&malformed));
        malformed_identity.blake3 = blake3::hash(&malformed).to_hex().to_string();
        assert!(
            decode_v36_centered_projection_arrow(&malformed, &malformed_identity, &spec).is_err()
        );
    }
    assert!(
        decode_v36_centered_projection_arrow(
            &bytes,
            &identity,
            &V36CenteredProjectionTrainingSpec {
                retained_dimensions: 3,
                ..spec.clone()
            },
        )
        .is_err()
    );

    let mut changed_rows = rows.clone();
    changed_rows[7][3] = -0.5;
    let changed_spec = V36CenteredProjectionTrainingSpec {
        reservoir_sha256: centered_source_digest(
            b"borsuk-v36-centered-reservoir-v1\n",
            &changed_rows,
        ),
        ..spec.clone()
    };
    let changed_projection = train_v36_centered_subspace(
        &changed_spec,
        &mut TestProjectionSource {
            corpus_rows: rows,
            reservoir_rows: changed_rows,
            block_rows: 3,
            mutate_reservoir: false,
        },
    )
    .unwrap();
    let (changed_bytes, changed_identity) =
        encode_v36_centered_projection_arrow(&changed_projection, "centered-projection-basis", uri)
            .unwrap();
    assert!(
        decode_v36_centered_projection_arrow(&changed_bytes, &changed_identity, &spec).is_err()
    );
}
