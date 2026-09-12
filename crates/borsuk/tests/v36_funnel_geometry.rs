//! V36 geometry construction work and admission contracts.

use std::{io::Cursor, sync::Arc};

use arrow_array::{ArrayRef, FixedSizeListArray, Float32Array, RecordBatch, UInt64Array};
use arrow_ipc::{
    CompressionType, MetadataVersion,
    reader::FileReader,
    writer::{FileWriter, IpcWriteOptions},
};
use arrow_schema::{DataType, Field};
use borsuk::{
    V36ArtifactIdentity, V36ProjectedCorpusBlockVisitor, V36ProjectedCorpusSource,
    V36SupercellAssignmentAdmissionRequest, V36SupercellAssignmentRow,
    V36SupercellAssignmentShardArtifact, V36SupercellAssignmentShardSink,
    V36SupercellPostCountAdmissionRequest, V36SupercellRunChunkArtifact, V36SupercellTrainingSpec,
    admit_v36_supercell_assignment_preflight, admit_v36_supercell_post_count,
    bind_v36_registered_supercell_training_spec, bind_v36_supercell_assignment_shard_context,
    bind_v36_supercell_run_chunk_context, decode_v36_supercell_assignment_shard_arrow,
    decode_v36_supercell_model_arrow, decode_v36_supercell_run_chunk_arrow,
    encode_v36_supercell_assignment_shard_arrow, encode_v36_supercell_model_arrow,
    encode_v36_supercell_run_chunk_arrow, load_v36_prefix_source_feature_ids,
    project_v36_exact_assignment_preflight, project_v36_supercell_assignment_admission,
    project_v36_supercell_post_count_admission, project_v36_supercell_training_preflight,
    run_v36_resident_projected_posting_diagnostic, train_v36_supercells, v36_prefix_source_schema,
    write_v36_prefix_source_parquet, write_v36_supercell_assignment_shards,
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

#[test]
fn v36_resident_projected_posting_diagnostic_runs_the_complete_small_shape() {
    let rows = projected_rows(32);
    let digest = projected_rows_sha256(&rows);
    let source = ProjectedSource {
        block_rows: 7,
        rows: rows.clone(),
        scans: 0,
        second_scan_delta: false,
    };
    let diagnostic =
        run_v36_resident_projected_posting_diagnostic(source, 32, 7, &digest, 8, None, 1).unwrap();
    assert_eq!(diagnostic.postings_per_supercell(), &[4]);
    assert_eq!(
        diagnostic.assignments().source_ordinals(),
        &(0..32).collect::<Vec<_>>()
    );
    assert_eq!(
        diagnostic
            .assignments()
            .primary_occupancy()
            .iter()
            .sum::<u64>(),
        32
    );

    let source = ProjectedSource {
        block_rows: 7,
        rows,
        scans: 0,
        second_scan_delta: false,
    };
    assert!(
        run_v36_resident_projected_posting_diagnostic(source, 32, 7, &"f".repeat(64), 8, None, 1,)
            .is_err()
    );
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
    for (rows, shards, merges, schedule, encoded, scratch, coverage, terms, external) in [
        (
            1_000_000,
            16,
            2,
            vec![(16, 2, 2, 0), (2, 1, 0, 2)],
            813_554_432,
            2_042_344_960,
            125_000,
            768_000_000_u128,
            22_000_000_u128,
        ),
        (
            10_000_000,
            153,
            3,
            vec![(153, 20, 19, 1), (20, 3, 2, 4), (3, 1, 0, 3)],
            8_120_864_256,
            16_782_793_728,
            1_250_000,
            122_880_000_000,
            240_000_000,
        ),
        (
            100_000_000,
            1_526,
            4,
            vec![
                (1_526, 191, 190, 6),
                (191, 24, 23, 7),
                (24, 3, 3, 0),
                (3, 1, 0, 3),
            ],
            81_200_253_952,
            163_881_097_216,
            12_500_000,
            9_830_400_000_000,
            2_600_000_000,
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
        assert_eq!(
            projected
                .merge_schedule()
                .unwrap()
                .iter()
                .map(|generation| (
                    generation.input_run_count,
                    generation.output_run_count,
                    generation.full_group_count,
                    generation.tail_group_size,
                ))
                .collect::<Vec<_>>(),
            schedule
        );
        assert_eq!(projected.uncompressed_assignment_bytes, encoded);
        assert_eq!(projected.required_scratch_bytes, scratch);
        assert_eq!(projected.required_peak_live_bytes, 1_060_110_336);
        assert_eq!(projected.coverage_bitmap_bytes, coverage);
        assert_eq!(projected.publication_row_visits, u128::from(rows));
        assert_eq!(projected.component_terms, terms);
        assert_eq!(projected.external_work_units, external);
        assert_eq!(projected.projected_active_ns, terms + external);
        assert_eq!(
            projected.projected_cost_microusd,
            u64::try_from(terms.div_ceil(1_000_000) + external.div_ceil(1_000_000)).unwrap()
        );
    }

    let spec = bind_v36_registered_supercell_training_spec(
        1_000_000,
        65_536,
        &format!("{:064x}", 1_000_000),
    )
    .unwrap();
    let mut one_worker = external_assignment_request();
    one_worker.worker_count = 1;
    assert_eq!(
        project_v36_supercell_assignment_admission(&spec, 16 * 1_048_576, &one_worker)
            .unwrap()
            .required_peak_live_bytes,
        1_060_110_336
    );
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
    assert_eq!(projected.required_scratch_bytes, 2_011_238_400);
    assert_eq!(projected.required_peak_live_bytes, 1_041_825_792);
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
fn v36_geometry_external_assignment_tail_shard_is_canonical_and_authenticated() {
    // Break caught: provisional construction shards drift from the strict
    // cross-language schema, omit a source ordinal, reorder keys, or detach
    // their bytes from corpus/model authority.
    let rows = projected_rows(4);
    let spec = V36SupercellTrainingSpec {
        corpus_rows: 4,
        dimensions: ROUTING_DIMENSIONS,
        maximum_block_rows: 4,
        projected_corpus_sha256: projected_rows_sha256(&rows),
        reservoir_rows: 4,
        super_cell_count: 4,
    };
    let mut source = ProjectedSource {
        block_rows: 4,
        rows,
        scans: 0,
        second_scan_delta: false,
    };
    let model = train_v36_supercells(&spec, &mut source).unwrap();
    let (model_bytes, model_identity) = encode_v36_supercell_model_arrow(
        &model,
        &spec,
        "supercell-model",
        "s3://borsuk-v36-test/geometry/supercells.arrow",
    )
    .unwrap();
    let authenticated =
        decode_v36_supercell_model_arrow(&model_bytes, &model_identity, &spec).unwrap();
    let context = bind_v36_supercell_assignment_shard_context(
        &authenticated,
        0,
        "s3://borsuk-v36-test/geometry/assignments/shard-000000.arrow",
    )
    .unwrap();
    let mut vector = [0.0_f32; ROUTING_DIMENSIONS];
    vector[0] = 1.0;
    let mut negative_zero = vector;
    negative_zero[1] = -0.0;
    assert!(V36SupercellAssignmentRow::new(0, 0, negative_zero).is_err());
    let assignment_rows = vec![
        V36SupercellAssignmentRow::new(0, 1, vector).unwrap(),
        V36SupercellAssignmentRow::new(0, 3, vector).unwrap(),
        V36SupercellAssignmentRow::new(1, 0, vector).unwrap(),
        V36SupercellAssignmentRow::new(1, 2, vector).unwrap(),
    ];
    let (bytes, artifact) =
        encode_v36_supercell_assignment_shard_arrow(&context, &assignment_rows).unwrap();
    assert_eq!(artifact.context, context);
    assert_eq!(artifact.row_count, 4);
    assert_eq!(artifact.encoded_bytes, bytes.len() as u64);
    assert_eq!(
        decode_v36_supercell_assignment_shard_arrow(&bytes, &artifact).unwrap(),
        assignment_rows
    );
    assert_eq!(
        encode_v36_supercell_assignment_shard_arrow(&context, &assignment_rows)
            .unwrap()
            .0,
        bytes
    );

    let mut reordered = assignment_rows.clone();
    reordered.swap(0, 1);
    assert!(encode_v36_supercell_assignment_shard_arrow(&context, &reordered).is_err());
    let mut duplicate = assignment_rows.clone();
    duplicate[1] = duplicate[0].clone();
    assert!(encode_v36_supercell_assignment_shard_arrow(&context, &duplicate).is_err());
    let mut foreign_cell = assignment_rows.clone();
    foreign_cell[3] = V36SupercellAssignmentRow::new(4, 2, vector).unwrap();
    assert!(encode_v36_supercell_assignment_shard_arrow(&context, &foreign_cell).is_err());
    let mut corrupted = bytes.clone();
    let last = corrupted.len() - 1;
    corrupted[last] ^= 1;
    assert!(decode_v36_supercell_assignment_shard_arrow(&corrupted, &artifact).is_err());
    let mut changed_artifact = artifact.clone();
    changed_artifact.sha256 = "0".repeat(64);
    assert!(decode_v36_supercell_assignment_shard_arrow(&bytes, &changed_artifact).is_err());

    let mut reader = FileReader::try_new(Cursor::new(&bytes), None).unwrap();
    let schema = reader.schema();
    let batch = reader.next().unwrap().unwrap();
    let options = IpcWriteOptions::try_new(8, false, MetadataVersion::V5)
        .unwrap()
        .try_with_compression(Some(CompressionType::ZSTD))
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
    assert!(
        decode_v36_supercell_assignment_shard_arrow(&compressed, &compressed_artifact).is_err()
    );
}

#[derive(Default)]
struct AssignmentShardSink {
    aborted: bool,
    committed: bool,
    provisional: Vec<(Vec<u8>, V36SupercellAssignmentShardArtifact)>,
    root: Option<(Vec<u8>, V36ArtifactIdentity)>,
}

impl V36SupercellAssignmentShardSink for AssignmentShardSink {
    fn write_provisional(
        &mut self,
        bytes: &[u8],
        artifact: &V36SupercellAssignmentShardArtifact,
    ) -> borsuk::Result<()> {
        self.provisional.push((bytes.to_vec(), artifact.clone()));
        Ok(())
    }

    fn commit(
        &mut self,
        artifacts: &[V36SupercellAssignmentShardArtifact],
        root_bytes: &[u8],
        root_identity: &V36ArtifactIdentity,
    ) -> borsuk::Result<()> {
        assert_eq!(
            artifacts,
            self.provisional
                .iter()
                .map(|(_, artifact)| artifact.clone())
                .collect::<Vec<_>>()
        );
        self.root = Some((root_bytes.to_vec(), root_identity.clone()));
        self.committed = true;
        Ok(())
    }

    fn abort(&mut self) -> borsuk::Result<()> {
        self.aborted = true;
        self.provisional.clear();
        self.root = None;
        Ok(())
    }
}

#[test]
fn v36_geometry_external_assignment_writer_is_schedule_invariant_and_transactional() {
    // Break caught: callback or worker scheduling changes logical shard bytes,
    // or provisional shards survive a failed complete-corpus replay check.
    let rows = projected_rows(4);
    let spec = V36SupercellTrainingSpec {
        corpus_rows: 4,
        dimensions: ROUTING_DIMENSIONS,
        maximum_block_rows: 4,
        projected_corpus_sha256: projected_rows_sha256(&rows),
        reservoir_rows: 4,
        super_cell_count: 4,
    };
    let mut training_source = ProjectedSource {
        block_rows: 4,
        rows: rows.clone(),
        scans: 0,
        second_scan_delta: false,
    };
    let model = train_v36_supercells(&spec, &mut training_source).unwrap();
    let (model_bytes, model_identity) = encode_v36_supercell_model_arrow(
        &model,
        &spec,
        "supercell-model",
        "s3://borsuk-v36-test/geometry/supercells.arrow",
    )
    .unwrap();
    let authenticated =
        decode_v36_supercell_model_arrow(&model_bytes, &model_identity, &spec).unwrap();

    let mut baseline = None;
    for worker_count in [1, 2, 4] {
        for block_rows in [1, 2, 4] {
            let mut request = external_assignment_request();
            request.worker_count = worker_count;
            request.queue_rows_per_worker = 4;
            request.sort_rows_per_worker = 4;
            request.measured_component_terms = 1;
            request.measured_external_work_units = 1;
            let admission =
                admit_v36_supercell_assignment_preflight(&authenticated, &request).unwrap();
            let mut source = ProjectedSource {
                block_rows,
                rows: rows.clone(),
                scans: 0,
                second_scan_delta: false,
            };
            let mut sink = AssignmentShardSink::default();
            let committed = write_v36_supercell_assignment_shards(
                &authenticated,
                &admission,
                &mut source,
                "s3://borsuk-v36-test/geometry/assignments",
                &mut sink,
            )
            .unwrap();
            assert_eq!(source.scans, 1);
            assert!(sink.committed);
            assert!(!sink.aborted);
            assert_eq!(committed.admission(), &admission);
            assert_eq!(
                committed.uri_prefix(),
                "s3://borsuk-v36-test/geometry/assignments"
            );
            let (root_bytes, root_identity) = sink.root.as_ref().unwrap();
            assert_eq!(root_bytes.last(), Some(&b'\n'));
            assert_eq!(committed.root_identity(), root_identity);
            assert_eq!(root_identity.encoded_bytes, root_bytes.len() as u64);
            assert_eq!(
                root_identity.sha256,
                format!("{:x}", Sha256::digest(root_bytes))
            );
            assert_eq!(
                root_identity.uri,
                "s3://borsuk-v36-test/geometry/assignments/assignment-root.json"
            );
            let artifacts = committed.artifacts();
            assert_eq!(artifacts.len(), 1);
            assert_eq!(sink.provisional.len(), 1);
            let observed = (
                sink.provisional[0].0.clone(),
                artifacts[0].sha256.clone(),
                artifacts[0].blake3.clone(),
                root_bytes.clone(),
                root_identity.sha256.clone(),
            );
            if let Some(expected) = &baseline {
                assert_eq!(&observed, expected);
            } else {
                baseline = Some(observed);
            }
        }
    }

    let baseline_root_sha256 = baseline.as_ref().unwrap().4.clone();
    let mut alternate_schedule_request = external_assignment_request();
    alternate_schedule_request.worker_count = 1;
    alternate_schedule_request.queue_rows_per_worker = 4;
    alternate_schedule_request.sort_rows_per_worker = 4;
    alternate_schedule_request.merge_fan_in = 4;
    alternate_schedule_request.measured_component_terms = 1;
    alternate_schedule_request.measured_external_work_units = 1;
    let alternate_schedule_admission =
        admit_v36_supercell_assignment_preflight(&authenticated, &alternate_schedule_request)
            .unwrap();
    let mut alternate_schedule_source = ProjectedSource {
        block_rows: 4,
        rows: rows.clone(),
        scans: 0,
        second_scan_delta: false,
    };
    let mut alternate_schedule_sink = AssignmentShardSink::default();
    let alternate_schedule = write_v36_supercell_assignment_shards(
        &authenticated,
        &alternate_schedule_admission,
        &mut alternate_schedule_source,
        "s3://borsuk-v36-test/geometry/assignments",
        &mut alternate_schedule_sink,
    )
    .unwrap();
    assert_ne!(
        alternate_schedule.root_identity().sha256,
        baseline_root_sha256
    );

    let mut request = external_assignment_request();
    request.worker_count = 1;
    request.queue_rows_per_worker = 4;
    request.sort_rows_per_worker = 4;
    request.measured_component_terms = 1;
    request.measured_external_work_units = 1;
    let admission = admit_v36_supercell_assignment_preflight(&authenticated, &request).unwrap();
    let mut changed_rows = rows;
    changed_rows[0].1[0] += 0.25;
    let mut changed_source = ProjectedSource {
        block_rows: 2,
        rows: changed_rows,
        scans: 0,
        second_scan_delta: false,
    };
    let mut sink = AssignmentShardSink::default();
    assert!(
        write_v36_supercell_assignment_shards(
            &authenticated,
            &admission,
            &mut changed_source,
            "s3://borsuk-v36-test/geometry/assignments",
            &mut sink,
        )
        .is_err()
    );
    assert!(sink.aborted);
    assert!(!sink.committed);
    assert!(sink.provisional.is_empty());
    assert!(sink.root.is_none());
}

#[test]
fn v36_geometry_external_merge_chunk_is_canonical_and_cell_bound() {
    // Break caught: the fixed-fan-in merge publishes a chunk detached from the
    // authenticated model/corpus/cell, with reordered rows or noncanonical IPC.
    let rows = projected_rows(4);
    let spec = V36SupercellTrainingSpec {
        corpus_rows: 4,
        dimensions: ROUTING_DIMENSIONS,
        maximum_block_rows: 4,
        projected_corpus_sha256: projected_rows_sha256(&rows),
        reservoir_rows: 4,
        super_cell_count: 4,
    };
    let mut source = ProjectedSource {
        block_rows: 2,
        rows,
        scans: 0,
        second_scan_delta: false,
    };
    let model = train_v36_supercells(&spec, &mut source).unwrap();
    let (model_bytes, model_identity) = encode_v36_supercell_model_arrow(
        &model,
        &spec,
        "supercell-model",
        "s3://borsuk-v36-test/geometry/supercells.arrow",
    )
    .unwrap();
    let authenticated =
        decode_v36_supercell_model_arrow(&model_bytes, &model_identity, &spec).unwrap();
    let context = bind_v36_supercell_run_chunk_context(
        &authenticated,
        0,
        0,
        "s3://borsuk-v36-test/geometry/runs/cell-0000/chunk-000000.arrow",
    )
    .unwrap();
    let mut first = [0.0_f32; ROUTING_DIMENSIONS];
    first[0] = 1.0;
    let mut second = [0.0_f32; ROUTING_DIMENSIONS];
    second[1] = 1.0;
    let chunk_rows = vec![
        V36SupercellAssignmentRow::new(0, 0, first).unwrap(),
        V36SupercellAssignmentRow::new(0, 2, second).unwrap(),
    ];
    let (bytes, artifact) = encode_v36_supercell_run_chunk_arrow(&context, &chunk_rows).unwrap();
    assert_eq!(artifact.context, context);
    assert_eq!(artifact.row_count, 2);
    assert_eq!(artifact.encoded_bytes, bytes.len() as u64);
    assert_eq!(
        decode_v36_supercell_run_chunk_arrow(&bytes, &artifact).unwrap(),
        chunk_rows
    );
    assert_eq!(
        encode_v36_supercell_run_chunk_arrow(&context, &chunk_rows)
            .unwrap()
            .0,
        bytes
    );

    let mut wrong_cell = chunk_rows.clone();
    wrong_cell[1] = V36SupercellAssignmentRow::new(1, 2, second).unwrap();
    assert!(encode_v36_supercell_run_chunk_arrow(&context, &wrong_cell).is_err());
    let mut reversed = chunk_rows.clone();
    reversed.reverse();
    assert!(encode_v36_supercell_run_chunk_arrow(&context, &reversed).is_err());
    assert!(
        bind_v36_supercell_run_chunk_context(
            &authenticated,
            4,
            0,
            "s3://borsuk-v36-test/geometry/runs/cell-0004/chunk-000000.arrow",
        )
        .is_err()
    );

    let mut reader = FileReader::try_new(Cursor::new(&bytes), None).unwrap();
    let schema = reader.schema();
    let batch = reader.next().unwrap().unwrap();
    let options = IpcWriteOptions::try_new(8, false, MetadataVersion::V5)
        .unwrap()
        .try_with_compression(Some(CompressionType::ZSTD))
        .unwrap();
    let mut compressed = Vec::new();
    let mut writer =
        FileWriter::try_new_with_options(&mut compressed, schema.as_ref(), options).unwrap();
    writer.write(&batch).unwrap();
    writer.finish().unwrap();
    drop(writer);
    let mut compressed_artifact: V36SupercellRunChunkArtifact = artifact;
    compressed_artifact.encoded_bytes = compressed.len() as u64;
    compressed_artifact.sha256 = format!("{:x}", Sha256::digest(&compressed));
    compressed_artifact.blake3 = blake3::hash(&compressed).to_hex().to_string();
    assert!(decode_v36_supercell_run_chunk_arrow(&compressed, &compressed_artifact).is_err());
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
