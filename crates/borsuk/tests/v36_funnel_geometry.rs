//! V36 geometry construction work and admission contracts.

use std::sync::Arc;

use arrow_array::{ArrayRef, FixedSizeListArray, Float32Array, RecordBatch, UInt64Array};
use arrow_schema::{DataType, Field};
use borsuk::{
    V36ProjectedCorpusBlockVisitor, V36ProjectedCorpusSource,
    V36SupercellAssignmentAdmissionRequest, V36SupercellPostCountAdmissionRequest,
    V36SupercellTrainingSpec, admit_v36_supercell_assignment_preflight,
    admit_v36_supercell_post_count, bind_v36_registered_supercell_training_spec,
    decode_v36_supercell_model_arrow, encode_v36_supercell_model_arrow,
    load_v36_prefix_source_feature_ids, project_v36_exact_assignment_preflight,
    project_v36_supercell_assignment_admission, project_v36_supercell_post_count_admission,
    project_v36_supercell_training_preflight, train_v36_supercells, v36_prefix_source_schema,
    write_v36_prefix_source_parquet,
};
use sha2::{Digest, Sha256};

const SOURCE_DIMENSIONS: usize = 768;
const ROUTING_DIMENSIONS: usize = 192;

struct ProjectedSource {
    block_rows: usize,
    rows: Vec<(u64, Vec<f32>)>,
    scans: usize,
    second_scan_delta: bool,
}

impl V36ProjectedCorpusSource for ProjectedSource {
    fn scan(&mut self, visitor: &mut V36ProjectedCorpusBlockVisitor<'_>) -> borsuk::Result<()> {
        self.scans += 1;
        for block in self.rows.chunks(self.block_rows) {
            let ordinals = block.iter().map(|row| row.0).collect::<Vec<_>>();
            let mut vectors = block
                .iter()
                .flat_map(|row| row.1.iter().copied())
                .collect::<Vec<_>>();
            if self.second_scan_delta && self.scans == 2 {
                vectors[0] += 1.0;
            }
            visitor(&ordinals, &vectors)?;
        }
        Ok(())
    }
}

fn projected_rows(rows: u64) -> Vec<(u64, Vec<f32>)> {
    (0..rows)
        .map(|ordinal| {
            let mut vector = vec![0.0_f32; ROUTING_DIMENSIONS];
            vector[0] = (ordinal % 4) as f32 * 10.0 + (ordinal / 4) as f32;
            vector[1] = ordinal as f32 * 0.125;
            (ordinal, vector)
        })
        .collect()
}

fn expected_reservoir_ordinals(rows: u64, retained: usize) -> Vec<u64> {
    let mut ranked = (0..rows)
        .map(|ordinal| {
            let mut digest = Sha256::new();
            digest.update(b"borsuk-v36-geometry-reservoir-v1");
            digest.update(ordinal.to_le_bytes());
            (digest.finalize().to_vec(), ordinal)
        })
        .collect::<Vec<_>>();
    ranked.sort_unstable();
    let mut ordinals = ranked
        .into_iter()
        .take(retained)
        .map(|(_, ordinal)| ordinal)
        .collect::<Vec<_>>();
    ordinals.sort_unstable();
    ordinals
}

fn projected_rows_sha256(rows: &[(u64, Vec<f32>)]) -> String {
    let mut digest = Sha256::new();
    digest.update(b"borsuk-v36-projected-corpus-replay-v1");
    for (ordinal, vector) in rows {
        digest.update(ordinal.to_le_bytes());
        for value in vector {
            digest.update(value.to_bits().to_le_bytes());
        }
    }
    format!("{:x}", digest.finalize())
}

fn projected_corpus_sha256(rows: u64) -> String {
    projected_rows_sha256(&projected_rows(rows))
}

fn source_batch(feature_ids: Vec<u64>) -> RecordBatch {
    let rows = feature_ids.len();
    let child = Arc::new(Field::new("item", DataType::Float32, false));
    let mut values = vec![0.0_f32; rows * SOURCE_DIMENSIONS];
    for row in 0..rows {
        values[row * SOURCE_DIMENSIONS + row % SOURCE_DIMENSIONS] = 1.0;
    }
    let embeddings = FixedSizeListArray::try_new(
        child,
        SOURCE_DIMENSIONS as i32,
        Arc::new(Float32Array::from(values)),
        None,
    )
    .unwrap();
    RecordBatch::try_new(
        Arc::new(v36_prefix_source_schema()),
        vec![
            Arc::new(UInt64Array::from(feature_ids)) as ArrayRef,
            Arc::new(embeddings),
        ],
    )
    .unwrap()
}

#[test]
fn v36_geometry_inputs_reuse_strict_prefix_source_membership_scan() {
    // Break caught: geometry invents a second source schema or decodes every
    // embedding merely to establish the authenticated corpus ID order.
    let directory = tempfile::tempdir().unwrap();
    let source = directory.path().join("source.parquet");
    let feature_ids = (7_u64..108).collect::<Vec<_>>();
    write_v36_prefix_source_parquet(&source, &feature_ids, [source_batch(feature_ids.clone())])
        .unwrap();
    assert_eq!(
        load_v36_prefix_source_feature_ids(&source, 101).unwrap(),
        feature_ids
    );
    assert!(load_v36_prefix_source_feature_ids(&source, 100).is_err());
    assert!(load_v36_prefix_source_feature_ids(&source, 102).is_err());
}

#[test]
fn v36_geometry_preflight_projects_exact_assignment_work_before_corpus_execution() {
    // Break caught: a long geometry cell starts without first projecting the
    // exact global assignment work from a bounded measured kernel sample.
    let one_million = project_v36_exact_assignment_preflight(
        1_000_000, 192, 4_096, 1_024, 245, 1_000_000, 43_200,
    )
    .unwrap();
    assert_eq!(one_million.posting_count, 245);
    assert_eq!(one_million.distance_evaluations, 245_000_000);
    assert_eq!(one_million.component_terms, 47_040_000_000);
    assert_eq!(one_million.projected_active_ns, 976_562_500);
    assert!(one_million.within_active_wall_cap);

    let ten_million = project_v36_exact_assignment_preflight(
        10_000_000, 192, 4_096, 1_024, 245, 1_000_000, 43_200,
    )
    .unwrap();
    assert_eq!(ten_million.posting_count, 2_442);
    assert_eq!(ten_million.component_terms, 4_688_640_000_000);

    let hundred_million = project_v36_exact_assignment_preflight(
        100_000_000,
        192,
        4_096,
        1_024,
        245,
        1_000_000,
        43_200,
    )
    .unwrap();
    assert_eq!(hundred_million.posting_count, 24_415);
    assert_eq!(hundred_million.component_terms, 468_768_000_000_000);
    assert_eq!(hundred_million.projected_active_ns, 9_731_744_260_205);
    assert!(hundred_million.within_active_wall_cap);

    let b8192 =
        project_v36_exact_assignment_preflight(100_000_000, 192, 8_192, 1_024, 123, 1_000_000, 1)
            .unwrap();
    assert_eq!(b8192.posting_count, 12_208);
    assert_eq!(b8192.component_terms, 234_393_600_000_000);
    assert!(!b8192.within_active_wall_cap);
}

#[test]
fn v36_geometry_preflight_rejects_unbounded_or_ambiguous_measurements() {
    for request in [
        (0, 192, 4_096, 1_024, 245, 1_000_000, 43_200),
        (1_000_000, 0, 4_096, 1_024, 245, 1_000_000, 43_200),
        (1_000_000, 192, 0, 1_024, 245, 1_000_000, 43_200),
        (1_000_000, 192, 4_096, 0, 245, 1_000_000, 43_200),
        (1_000_000, 192, 4_096, 1_024, 0, 1_000_000, 43_200),
        (1_000_000, 192, 4_096, 1_024, 245, 0, 43_200),
        (1_000_000, 192, 4_096, 1_024, 245, 1_000_000, 0),
    ] {
        assert!(
            project_v36_exact_assignment_preflight(
                request.0, request.1, request.2, request.3, request.4, request.5, request.6,
            )
            .is_err()
        );
    }
    assert!(
        project_v36_exact_assignment_preflight(
            u64::MAX,
            u32::MAX,
            1,
            u64::MAX,
            u32::MAX,
            u64::MAX,
            u64::MAX,
        )
        .is_err()
    );
}

#[test]
fn v36_geometry_supercells_use_query_blind_bounded_reservoir_passes() {
    let spec = V36SupercellTrainingSpec {
        corpus_rows: 24,
        dimensions: ROUTING_DIMENSIONS,
        maximum_block_rows: 7,
        projected_corpus_sha256: projected_corpus_sha256(24),
        reservoir_rows: 16,
        super_cell_count: 4,
    };
    let mut source = ProjectedSource {
        block_rows: 7,
        rows: projected_rows(spec.corpus_rows),
        scans: 0,
        second_scan_delta: false,
    };
    let model = train_v36_supercells(&spec, &mut source).unwrap();
    assert_eq!(source.scans, 2);
    assert_eq!(model.iterations(), 25);
    assert_eq!(model.projected_corpus_sha256(), projected_corpus_sha256(24));
    assert_eq!(
        model.reservoir_source_ordinals(),
        expected_reservoir_ordinals(24, 16)
    );
    assert_eq!(model.centroids().len(), 4);
    assert!(model.centroids().iter().all(|centroid| {
        centroid.len() == ROUTING_DIMENSIONS && centroid.iter().all(|value| value.is_finite())
    }));
}

#[test]
fn v36_geometry_supercells_registered_shape_is_dimension_independent_and_memory_bounded() {
    for (rows, super_cells) in [(1_000_000, 4), (10_000_000, 64), (100_000_000, 512)] {
        let spec =
            bind_v36_registered_supercell_training_spec(rows, 65_536, &format!("{:064x}", rows))
                .unwrap();
        assert_eq!(spec.corpus_rows, rows);
        assert_eq!(spec.reservoir_rows, rows.min(1_048_576));
        assert_eq!(spec.super_cell_count, super_cells);
        assert_eq!(spec.dimensions, ROUTING_DIMENSIONS);
    }
    assert!(bind_v36_registered_supercell_training_spec(0, 65_536, &"a".repeat(64)).is_err());
    assert!(
        bind_v36_registered_supercell_training_spec(100_000_000, 65_537, &"a".repeat(64)).is_err()
    );
}

#[test]
fn v36_geometry_supercell_preflight_projects_complete_training_work_and_owned_memory() {
    for (rows, components, peak_bytes) in [
        (1_000_000, 19_775_998_848_u128, 851_865_216),
        (10_000_000, 334_805_735_424, 890_913_792),
        (100_000_000, 2_679_833_149_440, 891_953_152),
    ] {
        let spec =
            bind_v36_registered_supercell_training_spec(rows, 65_536, &format!("{:064x}", rows))
                .unwrap();
        let preflight =
            project_v36_supercell_training_preflight(&spec, 1_000_000_000, 1_000_000_000, 43_200)
                .unwrap();
        assert_eq!(preflight.component_terms, components);
        assert_eq!(preflight.projected_active_ns, components);
        assert_eq!(preflight.trainer_owned_peak_bytes, peak_bytes);
        assert!(preflight.within_active_wall_cap);
    }

    let spec = bind_v36_registered_supercell_training_spec(
        100_000_000,
        65_536,
        &format!("{:064x}", 100_000_000),
    )
    .unwrap();
    assert!(
        !project_v36_supercell_training_preflight(&spec, 1, 1_000_000_000, 43_200)
            .unwrap()
            .within_active_wall_cap
    );
}

fn external_assignment_request() -> V36SupercellAssignmentAdmissionRequest {
    V36SupercellAssignmentAdmissionRequest {
        worker_count: 4,
        queue_rows_per_worker: 65_536,
        sort_rows_per_worker: 65_536,
        merge_fan_in: 8,
        measured_component_terms: 1_000_000,
        measured_elapsed_ns: 1_000_000,
        measured_cost_microusd: 1,
        measured_external_work_units: 1_000_000,
        measured_external_elapsed_ns: 1_000_000,
        measured_external_cost_microusd: 1,
        maximum_active_wall_seconds: u64::MAX,
        maximum_cost_microusd: u64::MAX,
        maximum_peak_live_bytes: u64::MAX,
        maximum_scratch_bytes: u64::MAX,
    }
}

#[test]
fn v36_geometry_external_assignment_admission_projects_registered_scales_without_population() {
    // Break caught: a 100M assignment is admitted from a balanced-cell RAM
    // assumption or without charging the complete uncompressed shard/merge copy.
    for (rows, shards, merges, encoded, scratch, terms, external) in [
        (
            1_000_000,
            16,
            2,
            781_048_576,
            1_971_041_792,
            768_000_000_u128,
            21_000_000_u128,
        ),
        (
            10_000_000,
            153,
            3,
            7_810_027_008,
            16_028_998_656,
            122_880_000_000,
            230_000_000,
        ),
        (
            100_000_000,
            1_526,
            4,
            78_100_007_936,
            156_608_960_512,
            9_830_400_000_000,
            2_500_000_000,
        ),
    ] {
        let spec =
            bind_v36_registered_supercell_training_spec(rows, 65_536, &format!("{rows:064x}"))
                .unwrap();
        let projected = project_v36_supercell_assignment_admission(
            &spec,
            16 * 1_048_576,
            &external_assignment_request(),
        )
        .unwrap();
        assert_eq!(projected.logical_shards, shards);
        assert_eq!(projected.merge_generations, merges);
        assert_eq!(projected.uncompressed_assignment_bytes, encoded);
        assert_eq!(projected.required_scratch_bytes, scratch);
        assert_eq!(projected.required_peak_live_bytes, 425_721_856);
        assert_eq!(projected.component_terms, terms);
        assert_eq!(projected.external_work_units, external);
        assert_eq!(projected.projected_active_ns, terms + external);
        assert_eq!(
            projected.projected_cost_microusd,
            u64::try_from(terms.div_ceil(1_000_000) + external.div_ceil(1_000_000)).unwrap()
        );
    }
}

#[test]
fn v36_geometry_external_assignment_plan_is_authenticated_and_fail_closed() {
    // Break caught: an executor can receive an unbound projection, or a launch
    // slips through with a resource/cost limit one unit below the exact need.
    let rows = projected_rows(24);
    let spec = V36SupercellTrainingSpec {
        corpus_rows: 24,
        dimensions: ROUTING_DIMENSIONS,
        maximum_block_rows: 8,
        projected_corpus_sha256: projected_rows_sha256(&rows),
        reservoir_rows: 24,
        super_cell_count: 4,
    };
    let mut source = ProjectedSource {
        block_rows: 8,
        rows,
        scans: 0,
        second_scan_delta: false,
    };
    let model = train_v36_supercells(&spec, &mut source).unwrap();
    let uri = "s3://borsuk-v36-test/geometry/supercells.arrow";
    let (bytes, identity) =
        encode_v36_supercell_model_arrow(&model, &spec, "supercell-model", uri).unwrap();
    let authenticated = decode_v36_supercell_model_arrow(&bytes, &identity, &spec).unwrap();
    let mut request = external_assignment_request();
    request.measured_component_terms = 1;
    request.measured_elapsed_ns = 1;
    request.measured_cost_microusd = 1;
    request.measured_external_work_units = 1;
    request.measured_external_elapsed_ns = 1;
    request.measured_external_cost_microusd = 1;
    let projection =
        project_v36_supercell_assignment_admission(&spec, identity.encoded_bytes, &request)
            .unwrap();
    request.maximum_active_wall_seconds =
        u64::try_from(projection.projected_active_ns.div_ceil(1_000_000_000)).unwrap();
    request.maximum_cost_microusd = projection.projected_cost_microusd;
    request.maximum_peak_live_bytes = projection.required_peak_live_bytes;
    request.maximum_scratch_bytes = projection.required_scratch_bytes;
    let admitted = admit_v36_supercell_assignment_preflight(&authenticated, &request).unwrap();
    assert_eq!(admitted.model_identity(), &identity);
    assert_eq!(admitted.training_spec(), &spec);
    assert_eq!(admitted.projection(), &projection);
    assert_eq!(admitted.request(), &request);

    for mutation in 0..4 {
        let mut rejected = request.clone();
        match mutation {
            0 => rejected.maximum_active_wall_seconds -= 1,
            1 => rejected.maximum_cost_microusd -= 1,
            2 => rejected.maximum_peak_live_bytes -= 1,
            3 => rejected.maximum_scratch_bytes -= 1,
            _ => unreachable!(),
        }
        assert!(admit_v36_supercell_assignment_preflight(&authenticated, &rejected).is_err());
    }

    let mut unbounded_calibration = request.clone();
    unbounded_calibration.measured_component_terms = projection.component_terms + 1;
    assert!(
        admit_v36_supercell_assignment_preflight(&authenticated, &unbounded_calibration).is_err()
    );
    let mut unbounded_external_calibration = request.clone();
    unbounded_external_calibration.measured_external_work_units =
        projection.external_work_units + 1;
    assert!(
        admit_v36_supercell_assignment_preflight(&authenticated, &unbounded_external_calibration)
            .is_err()
    );
}

fn post_count_request() -> V36SupercellPostCountAdmissionRequest {
    V36SupercellPostCountAdmissionRequest {
        measured_component_terms: 1_000_000,
        measured_elapsed_ns: 1_000_000,
        measured_cost_microusd: 1,
        maximum_active_wall_seconds: u64::MAX,
        maximum_cost_microusd: u64::MAX,
        maximum_peak_live_bytes: u64::MAX,
        maximum_scratch_bytes: u64::MAX,
    }
}

#[test]
fn v36_geometry_post_count_admission_projects_skew_and_hamilton_without_population() {
    // Break caught: local training treats one super-cell as a RAM bound or
    // omits farthest-first, ten Lloyd passes, worst repair, or replay reduction.
    let spec = bind_v36_registered_supercell_training_spec(
        1_000_000,
        65_536,
        &format!("{:064x}", 1_000_000),
    )
    .unwrap();
    let assignment = project_v36_supercell_assignment_admission(
        &spec,
        16 * 1_048_576,
        &external_assignment_request(),
    )
    .unwrap();
    let projected = project_v36_supercell_post_count_admission(
        &spec,
        &assignment,
        &[999_997, 1, 1, 1],
        4_096,
        4,
        &post_count_request(),
    )
    .unwrap();
    assert_eq!(projected.posting_count, 245);
    assert_eq!(projected.postings_per_supercell, [242, 1, 1, 1]);
    assert_eq!(projected.initialization_distance_evaluations, 240_970_116);
    assert_eq!(projected.lloyd_distance_evaluations, 2_419_992_770);
    assert_eq!(projected.repair_distance_evaluations, 2_419_992_770);
    assert_eq!(projected.source_reduction_terms, 1_920_000_000);
    assert_eq!(projected.local_component_terms, 977_463_485_952);
    assert_eq!(projected.required_scratch_bytes, 2_011_041_792);
    assert_eq!(projected.required_peak_live_bytes, 425_721_856);
    assert_eq!(projected.projected_active_ns, 978_252_485_952);
    assert_eq!(projected.projected_cost_microusd, 978_253);

    assert!(
        project_v36_supercell_post_count_admission(
            &spec,
            &assignment,
            &[999_998, 1, 0, 0],
            4_096,
            4,
            &post_count_request(),
        )
        .is_err()
    );
    assert!(
        project_v36_supercell_post_count_admission(
            &spec,
            &assignment,
            &[999_997, 1, 1, 1],
            1_000_001,
            4,
            &post_count_request(),
        )
        .is_err()
    );
}

#[test]
fn v36_geometry_post_count_plan_consumes_authenticated_preflight_and_limits() {
    let rows = projected_rows(24);
    let spec = V36SupercellTrainingSpec {
        corpus_rows: 24,
        dimensions: ROUTING_DIMENSIONS,
        maximum_block_rows: 8,
        projected_corpus_sha256: projected_rows_sha256(&rows),
        reservoir_rows: 24,
        super_cell_count: 4,
    };
    let mut source = ProjectedSource {
        block_rows: 8,
        rows,
        scans: 0,
        second_scan_delta: false,
    };
    let model = train_v36_supercells(&spec, &mut source).unwrap();
    let (bytes, identity) = encode_v36_supercell_model_arrow(
        &model,
        &spec,
        "supercell-model",
        "s3://borsuk-v36-test/geometry/supercells.arrow",
    )
    .unwrap();
    let authenticated = decode_v36_supercell_model_arrow(&bytes, &identity, &spec).unwrap();
    let mut assignment_request = external_assignment_request();
    assignment_request.measured_component_terms = 1;
    assignment_request.measured_elapsed_ns = 1;
    assignment_request.measured_cost_microusd = 1;
    assignment_request.measured_external_work_units = 1;
    assignment_request.measured_external_elapsed_ns = 1;
    assignment_request.measured_external_cost_microusd = 1;
    assignment_request.queue_rows_per_worker = 1;
    assignment_request.sort_rows_per_worker = 1;
    let assignment =
        admit_v36_supercell_assignment_preflight(&authenticated, &assignment_request).unwrap();
    let mut request = post_count_request();
    request.measured_component_terms = 1;
    request.measured_elapsed_ns = 1;
    let projection = project_v36_supercell_post_count_admission(
        &spec,
        assignment.projection(),
        &[21, 1, 1, 1],
        6,
        4,
        &request,
    )
    .unwrap();
    assert_eq!(projection.required_peak_live_bytes, 209_724_448);
    request.maximum_active_wall_seconds =
        u64::try_from(projection.projected_active_ns.div_ceil(1_000_000_000)).unwrap();
    request.maximum_cost_microusd = projection.projected_cost_microusd;
    request.maximum_peak_live_bytes = projection.required_peak_live_bytes;
    request.maximum_scratch_bytes = projection.required_scratch_bytes;
    let admitted =
        admit_v36_supercell_post_count(&assignment, &[21, 1, 1, 1], 6, &request).unwrap();
    assert_eq!(admitted.assignment_preflight(), &assignment);
    assert_eq!(admitted.request(), &request);
    assert_eq!(admitted.projection(), &projection);
    assert_eq!(admitted.run_rows(), &[21, 1, 1, 1]);
    assert_eq!(admitted.target_primary_rows(), 6);

    for mutation in 0..4 {
        let mut rejected = request.clone();
        match mutation {
            0 => rejected.maximum_active_wall_seconds -= 1,
            1 => rejected.maximum_cost_microusd -= 1,
            2 => rejected.maximum_peak_live_bytes -= 1,
            3 => rejected.maximum_scratch_bytes -= 1,
            _ => unreachable!(),
        }
        assert!(admit_v36_supercell_post_count(&assignment, &[21, 1, 1, 1], 6, &rejected).is_err());
    }
}

#[test]
fn v36_geometry_supercells_lock_farthest_ties_and_lloyd_centroid_bits() {
    let rows = projected_rows(24);
    let spec = V36SupercellTrainingSpec {
        corpus_rows: 24,
        dimensions: ROUTING_DIMENSIONS,
        maximum_block_rows: 8,
        projected_corpus_sha256: projected_rows_sha256(&rows),
        reservoir_rows: 24,
        super_cell_count: 4,
    };
    let mut source = ProjectedSource {
        block_rows: 8,
        rows,
        scans: 0,
        second_scan_delta: false,
    };
    let model = train_v36_supercells(&spec, &mut source).unwrap();
    assert_eq!(model.initialization_source_ordinals(), [0, 23, 2, 1]);
    let mut expected = vec![[0.0_f32; ROUTING_DIMENSIONS]; 4];
    for (centroid, (x, y)) in
        expected
            .iter_mut()
            .zip([(2.5, 1.25), (32.5, 1.625), (22.5, 1.5), (12.5, 1.375)])
    {
        centroid[0] = x;
        centroid[1] = y;
    }
    assert_eq!(model.centroids(), expected);
    assert_eq!(model.empty_repairs(), 0);

    let duplicate_rows = (0..8)
        .map(|ordinal| {
            let mut vector = vec![0.0_f32; ROUTING_DIMENSIONS];
            vector[0] = 1.0;
            (ordinal, vector)
        })
        .collect::<Vec<_>>();
    let duplicate_spec = V36SupercellTrainingSpec {
        corpus_rows: 8,
        dimensions: ROUTING_DIMENSIONS,
        maximum_block_rows: 8,
        projected_corpus_sha256: projected_rows_sha256(&duplicate_rows),
        reservoir_rows: 8,
        super_cell_count: 4,
    };
    let mut source = ProjectedSource {
        block_rows: 8,
        rows: duplicate_rows,
        scans: 0,
        second_scan_delta: false,
    };
    let model = train_v36_supercells(&duplicate_spec, &mut source).unwrap();
    assert_eq!(model.initialization_source_ordinals(), [0, 1, 2, 3]);
    assert_eq!(model.empty_repairs(), 75);
}

#[test]
fn v36_geometry_supercells_are_block_invariant_and_reject_source_drift() {
    let spec = V36SupercellTrainingSpec {
        corpus_rows: 24,
        dimensions: ROUTING_DIMENSIONS,
        maximum_block_rows: 8,
        projected_corpus_sha256: projected_corpus_sha256(24),
        reservoir_rows: 16,
        super_cell_count: 4,
    };
    let mut single = ProjectedSource {
        block_rows: 1,
        rows: projected_rows(spec.corpus_rows),
        scans: 0,
        second_scan_delta: false,
    };
    let mut blocked = ProjectedSource {
        block_rows: 8,
        rows: projected_rows(spec.corpus_rows),
        scans: 0,
        second_scan_delta: false,
    };
    let expected = train_v36_supercells(&spec, &mut single).unwrap();
    assert_eq!(train_v36_supercells(&spec, &mut blocked).unwrap(), expected);

    let mut reordered = ProjectedSource {
        block_rows: 8,
        rows: projected_rows(spec.corpus_rows),
        scans: 0,
        second_scan_delta: false,
    };
    reordered.rows.swap(4, 5);
    assert!(train_v36_supercells(&spec, &mut reordered).is_err());

    let mut oversized = ProjectedSource {
        block_rows: 9,
        rows: projected_rows(spec.corpus_rows),
        scans: 0,
        second_scan_delta: false,
    };
    assert!(train_v36_supercells(&spec, &mut oversized).is_err());

    let mut changing = ProjectedSource {
        block_rows: 8,
        rows: projected_rows(spec.corpus_rows),
        scans: 0,
        second_scan_delta: true,
    };
    assert!(train_v36_supercells(&spec, &mut changing).is_err());

    let mut wrong_digest = spec.clone();
    wrong_digest.projected_corpus_sha256 = "f".repeat(64);
    let mut source = ProjectedSource {
        block_rows: 8,
        rows: projected_rows(spec.corpus_rows),
        scans: 0,
        second_scan_delta: false,
    };
    assert!(train_v36_supercells(&wrong_digest, &mut source).is_err());

    let mut oversized_reservoir = spec.clone();
    oversized_reservoir.corpus_rows = 1_048_577;
    oversized_reservoir.reservoir_rows = 1_048_577;
    oversized_reservoir.projected_corpus_sha256 = "e".repeat(64);
    assert!(train_v36_supercells(&oversized_reservoir, &mut source).is_err());
}

#[test]
fn v36_geometry_supercell_model_round_trips_strict_authenticated_arrow() {
    // Break caught: construction resumes from an unauthenticated, JSON-expanded,
    // or schema-drifted centroid model before creating external posting runs.
    let rows = projected_rows(24);
    let spec = V36SupercellTrainingSpec {
        corpus_rows: 24,
        dimensions: ROUTING_DIMENSIONS,
        maximum_block_rows: 8,
        projected_corpus_sha256: projected_rows_sha256(&rows),
        reservoir_rows: 24,
        super_cell_count: 4,
    };
    let mut source = ProjectedSource {
        block_rows: 8,
        rows,
        scans: 0,
        second_scan_delta: false,
    };
    let model = train_v36_supercells(&spec, &mut source).unwrap();
    let uri = "s3://borsuk-v36-test/geometry/supercells.arrow";
    let (bytes, identity) =
        encode_v36_supercell_model_arrow(&model, &spec, "supercell-model", uri).unwrap();
    assert!(
        encode_v36_supercell_model_arrow(
            &model,
            &spec,
            "supercell-model",
            "https://borsuk-v36-test/geometry/supercells.arrow",
        )
        .is_err()
    );
    let oversized_uri = format!("s3://borsuk-v36-test/{}", "x".repeat(4_096));
    assert!(
        encode_v36_supercell_model_arrow(&model, &spec, "supercell-model", &oversized_uri).is_err()
    );
    assert_eq!(identity.role, "supercell-model");
    assert_eq!(identity.uri, uri);
    assert_eq!(identity.encoded_bytes, bytes.len() as u64);
    let loaded = decode_v36_supercell_model_arrow(&bytes, &identity, &spec).unwrap();
    assert_eq!(loaded.model(), &model);
    assert_eq!(loaded.identity(), &identity);
    assert_eq!(loaded.training_spec(), &spec);

    let mut changed_identity = identity.clone();
    changed_identity.sha256 = "0".repeat(64);
    assert!(decode_v36_supercell_model_arrow(&bytes, &changed_identity, &spec).is_err());

    let mut changed_bytes = bytes.clone();
    let last = changed_bytes.len() - 1;
    changed_bytes[last] ^= 1;
    assert!(decode_v36_supercell_model_arrow(&changed_bytes, &identity, &spec).is_err());

    let mut changed_spec = spec.clone();
    changed_spec.projected_corpus_sha256 = "f".repeat(64);
    assert!(decode_v36_supercell_model_arrow(&bytes, &identity, &changed_spec).is_err());
}
