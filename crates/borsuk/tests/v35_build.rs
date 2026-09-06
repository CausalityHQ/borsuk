//! V35 bounded streaming writer and immutable-delta contracts.

use arrow_array::{ArrayRef, FixedSizeListArray, Float32Array, RecordBatch, UInt64Array};
use arrow_schema::{DataType, Field, Schema};
use borsuk::{
    Result, V35ArtifactIdentity, V35BuildAuthority, V35BuildBlock, V35BuildBlockSource,
    V35BuildLeafSink, V35BuildMergeRow, V35BuildMergeSource, V35BuildRow, V35BuildScratchSink,
    V35BuildStorageGroup, V35BuildStorageGroupAssembler, V35BuildStorageGroupSink, V35Dimensions,
    V35MortonModel, V35Projection, build_v35_leaf_patch_from_merge_rows, build_v35_scratch_runs,
    build_v35_srht, decode_v35_build_run_arrow, decode_v35_source_block_parquet,
    merge_v35_build_runs, open_v35_build_run_cursor, project_v35_query_scalar,
    train_v35_morton_model,
};
use bytes::Bytes;
use parquet::arrow::ArrowWriter;
use sha2::{Digest, Sha256};
use std::{collections::VecDeque, sync::Arc};

fn training_rows() -> Vec<Vec<f32>> {
    (0..256)
        .map(|row| {
            (0..384)
                .map(|dimension| {
                    ((row * 37 + dimension * 13 + row * dimension * 3) % 2_047) as f32 / 71.0 - 14.0
                })
                .collect()
        })
        .collect()
}

fn projection() -> V35Projection {
    build_v35_srht(
        V35Dimensions {
            source: 384,
            routing: 64,
        },
        811,
    )
    .unwrap()
}

fn build_authority(projection: &V35Projection) -> V35BuildAuthority {
    V35BuildAuthority::new(
        "attempt-01",
        "deep-image-100m",
        &"ab".repeat(32),
        projection.checksum(),
    )
    .unwrap()
}

fn source_block_parquet(ordinals: Vec<u64>, source_field: Field) -> (V35ArtifactIdentity, Bytes) {
    let rows = ordinals.len();
    let manifest = format!(
        "{{\"first_source_ordinal\":{},\"format\":\"borsuk-v35-source-block-parquet-v1\",\"rows\":{},\"source_archive_sha256\":\"{}\",\"source_dimensions\":384,\"source_id\":\"deep-image-100m\"}}",
        ordinals[0],
        rows,
        "ab".repeat(32),
    );
    let schema = Arc::new(Schema::new_with_metadata(
        vec![
            Field::new("source_ordinal", DataType::UInt64, false),
            Field::new("id", DataType::UInt64, false),
            Field::new("sequence", DataType::UInt64, false),
            source_field,
        ],
        [("borsuk.v35.source-block.manifest".to_owned(), manifest)]
            .into_iter()
            .collect(),
    ));
    let source_values = ordinals
        .iter()
        .flat_map(|ordinal| {
            (0..384).map(move |dimension| (*ordinal * 17 + dimension) as f32 / 31.0)
        })
        .collect::<Vec<_>>();
    let source = FixedSizeListArray::try_new(
        Arc::new(Field::new("element", DataType::Float32, false)),
        384,
        Arc::new(Float32Array::from(source_values)),
        None,
    )
    .unwrap();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(UInt64Array::from(ordinals.clone())) as ArrayRef,
            Arc::new(UInt64Array::from(
                ordinals
                    .iter()
                    .map(|ordinal| 10_000 + ordinal)
                    .collect::<Vec<_>>(),
            )),
            Arc::new(UInt64Array::from(vec![1; rows])),
            Arc::new(source),
        ],
    )
    .unwrap();
    let mut bytes = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut bytes, schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    let identity = V35ArtifactIdentity {
        digest: format!("{:x}", Sha256::digest(&bytes)),
        digest_algorithm: "sha256".to_owned(),
        length: bytes.len() as u64,
        role: "build-source-block".to_owned(),
        uri: "s3://borsuk-source/deep-image-100m/blocks/00000000.parquet".to_owned(),
    };
    (identity, Bytes::from(bytes))
}

fn canonical_source_field() -> Field {
    Field::new(
        "source",
        DataType::FixedSizeList(
            Arc::new(Field::new("element", DataType::Float32, false)),
            384,
        ),
        false,
    )
}

#[test]
fn v35_build_source_parquet_authenticates_and_streams_without_projected_input() {
    // Break caught: construction trusts caller-projected coordinates or cannot
    // consume a strict bounded cross-language Parquet source shard.
    let projection = projection();
    let authority = build_authority(&projection);
    let model = train_v35_morton_model(&training_rows(), &projection, authority.clone()).unwrap();
    let (identity, bytes) = source_block_parquet((0..32).collect(), canonical_source_field());
    let block =
        decode_v35_source_block_parquet(bytes, &identity, &authority, projection.dimensions())
            .unwrap();
    let mut source = Blocks(VecDeque::from([block]));
    let mut scratch = Scratch::default();
    let receipt = build_v35_scratch_runs(&model, &projection, &mut source, &mut scratch).unwrap();
    assert_eq!(receipt.source_rows(), 32);
    let (registered, run) = &scratch.writes[0];
    let mut cursor = open_v35_build_run_cursor(run, registered, &model).unwrap();
    let batch = cursor.next_batch().unwrap().unwrap();
    assert_eq!(batch.source(0).unwrap().len(), 384);
    assert_eq!(batch.projected(0).unwrap().len(), 64);
}

#[test]
fn v35_build_source_parquet_rejects_identity_schema_and_order_drift() {
    // Break caught: a valid source shard can be relabeled, admit nullable or
    // caller-projected schema, or reorder immutable source ordinals.
    let projection = projection();
    let authority = build_authority(&projection);
    let dimensions = projection.dimensions();
    let (identity, bytes) = source_block_parquet((0..4).collect(), canonical_source_field());
    let mut changed_identity = identity.clone();
    changed_identity.digest = "cd".repeat(32);
    assert!(
        decode_v35_source_block_parquet(bytes.clone(), &changed_identity, &authority, dimensions)
            .is_err()
    );

    let nullable_source = Field::new(
        "source",
        DataType::FixedSizeList(
            Arc::new(Field::new("element", DataType::Float32, false)),
            384,
        ),
        true,
    );
    let (nullable_identity, nullable_bytes) =
        source_block_parquet((0..4).collect(), nullable_source);
    assert!(
        decode_v35_source_block_parquet(
            nullable_bytes,
            &nullable_identity,
            &authority,
            dimensions,
        )
        .is_err()
    );

    let (reordered_identity, reordered_bytes) =
        source_block_parquet(vec![0, 2, 1, 3], canonical_source_field());
    assert!(
        decode_v35_source_block_parquet(
            reordered_bytes,
            &reordered_identity,
            &authority,
            dimensions,
        )
        .is_err()
    );
}

#[test]
fn v35_build_scratch_cursor_authenticates_and_streams_bounded_batches() {
    // Break caught: external merge materializes a complete run, drops payload
    // fields, or loses total `(Morton key,source ordinal)` order at a batch edge.
    let projection = projection();
    let model = train_v35_morton_model(&training_rows(), &projection, build_authority(&projection))
        .unwrap();
    let rows = (0..520_u64)
        .map(|source_ordinal| {
            let source = (0..384)
                .map(|dimension| (source_ordinal + dimension as u64) as f32 / 19.0)
                .collect::<Vec<_>>();
            V35BuildRow::new(source_ordinal, 50_000 + source_ordinal, 3, source).unwrap()
        })
        .collect::<Vec<_>>();
    let mut source = Blocks(VecDeque::from([V35BuildBlock::new(rows).unwrap()]));
    let mut scratch = Scratch::default();
    build_v35_scratch_runs(&model, &projection, &mut source, &mut scratch).unwrap();
    let (registered, bytes) = &scratch.writes[0];
    let mut cursor = open_v35_build_run_cursor(bytes, registered, &model).unwrap();
    let mut batch_sizes = Vec::new();
    let mut observed = Vec::new();
    while let Some(batch) = cursor.next_batch().unwrap() {
        batch_sizes.push(batch.len());
        assert!(batch.len() <= 256);
        for row in 0..batch.len() {
            assert_eq!(
                batch.id(row),
                Some(50_000 + batch.source_ordinal(row).unwrap())
            );
            assert_eq!(batch.sequence(row), Some(3));
            assert_eq!(batch.source(row).unwrap().len(), 384);
            assert_eq!(batch.projected(row).unwrap().len(), 64);
            observed.push((
                batch.morton_key(row).unwrap(),
                batch.source_ordinal(row).unwrap(),
            ));
        }
    }
    assert_eq!(batch_sizes, vec![256, 256, 8]);
    assert_eq!(cursor.rows_seen(), 520);
    assert!(observed.windows(2).all(|pair| pair[0] < pair[1]));
}

#[test]
fn v35_build_morton_model_selects_variance_quantiles_and_msb_interleave() {
    // Break caught: build order depends on queries, coordinate ties select the
    // later dimension, equality crosses a quantile, or Morton bits are LSB-first.
    let rows = training_rows();
    let projection = projection();
    let model = train_v35_morton_model(&rows, &projection, build_authority(&projection)).unwrap();
    let projected = rows
        .iter()
        .map(|row| {
            project_v35_query_scalar(&projection, row)
                .unwrap()
                .coordinates()
                .to_vec()
        })
        .collect::<Vec<_>>();
    let mut ranked = (0..64_usize)
        .map(|coordinate| {
            let mean = projected.iter().map(|row| row[coordinate]).sum::<f64>() / 256.0;
            let variance = projected.iter().fold(0.0, |sum, row| {
                let delta = row[coordinate] - mean;
                delta.mul_add(delta, sum)
            }) / 256.0;
            (variance, coordinate as u16)
        })
        .collect::<Vec<_>>();
    ranked.sort_by(|left, right| right.0.total_cmp(&left.0).then(left.1.cmp(&right.1)));
    assert_eq!(
        model.selected_coordinates(),
        &ranked
            .iter()
            .take(16)
            .map(|(_, coordinate)| *coordinate)
            .collect::<Vec<_>>()
    );
    assert_eq!(model.boundaries(0).unwrap().len(), 255);
    let mut exact_boundary = vec![0.0; 64];
    for (slot, coordinate) in model.selected_coordinates().iter().enumerate() {
        exact_boundary[usize::from(*coordinate)] = model.boundaries(slot).unwrap()[0];
    }
    let mut just_above = exact_boundary.clone();
    for coordinate in model.selected_coordinates() {
        let value = &mut just_above[usize::from(*coordinate)];
        *value += f64::EPSILON * value.abs().max(1.0);
    }
    assert_eq!(model.key(&exact_boundary).unwrap(), 0);
    assert_eq!(model.key(&just_above).unwrap(), 0xffff);

    let bytes = model.canonical_bytes().unwrap();
    assert_eq!(V35MortonModel::from_canonical_bytes(&bytes).unwrap(), model);
    let mut noncanonical = bytes.clone();
    noncanonical.push(b' ');
    assert!(V35MortonModel::from_canonical_bytes(&noncanonical).is_err());
}

struct Blocks(VecDeque<V35BuildBlock>);

impl V35BuildBlockSource for Blocks {
    fn next_block(&mut self) -> Result<Option<V35BuildBlock>> {
        Ok(self.0.pop_front())
    }
}

#[derive(Default)]
struct Scratch {
    writes: Vec<(V35ArtifactIdentity, Vec<u8>)>,
}

impl V35BuildScratchSink for Scratch {
    fn write_run(&mut self, run_ordinal: u32, bytes: &[u8]) -> Result<V35ArtifactIdentity> {
        let identity = V35ArtifactIdentity {
            digest: format!("{:x}", Sha256::digest(bytes)),
            digest_algorithm: "sha256".to_owned(),
            length: bytes.len() as u64,
            role: "build-scratch-run".to_owned(),
            uri: format!("s3://borsuk-build/scratch/attempt-01/run-{run_ordinal:04}.arrow"),
        };
        self.writes.push((identity.clone(), bytes.to_vec()));
        Ok(identity)
    }
}

#[test]
fn v35_build_streams_ordered_blocks_into_bounded_authenticated_arrow_runs() {
    // Break caught: construction retains the corpus, sorts by ID instead of
    // Morton locality, exceeds 64 MiB, or writes an unauthenticated private format.
    let projection = projection();
    let model = train_v35_morton_model(&training_rows(), &projection, build_authority(&projection))
        .unwrap();
    let rows = (0..32_u64)
        .map(|source_ordinal| {
            let source = (0..384)
                .map(|dimension| (source_ordinal * 17 + dimension) as f32 / 31.0)
                .collect::<Vec<_>>();
            V35BuildRow::new(source_ordinal, 10_000 + source_ordinal, 1, source).unwrap()
        })
        .collect::<Vec<_>>();
    let blocks = rows
        .chunks(8)
        .map(|rows| V35BuildBlock::new(rows.to_vec()).unwrap())
        .collect::<VecDeque<_>>();
    let mut source = Blocks(blocks);
    let mut scratch = Scratch::default();
    let receipt = build_v35_scratch_runs(&model, &projection, &mut source, &mut scratch).unwrap();
    assert_eq!(receipt.source_rows(), 32);
    assert_eq!(receipt.scratch_runs(), 4);
    assert!(receipt.peak_live_builder_bytes() <= 64 * 1_048_576);
    assert_eq!(
        receipt.scratch_bytes(),
        scratch
            .writes
            .iter()
            .map(|(_, bytes)| bytes.len() as u64)
            .sum::<u64>()
    );
    for (run_ordinal, (registered, bytes)) in scratch.writes.iter().enumerate() {
        let decoded = decode_v35_build_run_arrow(bytes, registered, &model).unwrap();
        assert_eq!(decoded.run_ordinal(), run_ordinal as u32);
        assert_eq!(decoded.rows().len(), 8);
        assert!(decoded.rows().windows(2).all(|pair| {
            (pair[0].morton_key(), pair[0].source_ordinal())
                < (pair[1].morton_key(), pair[1].source_ordinal())
        }));
        assert!(
            decoded
                .rows()
                .iter()
                .all(|row| row.source_dimensions() == 384 && row.projected_dimensions() == 64)
        );
    }
}

#[test]
fn v35_build_block_preflights_live_memory_before_encoding() {
    // Break caught: the builder allocates Morton order, flattened Arrow
    // columns, and the complete output before discovering that the block
    // exceeds its 64-MiB live-memory admission.
    let projection = projection();
    let model = train_v35_morton_model(&training_rows(), &projection, build_authority(&projection))
        .unwrap();
    let small = V35BuildBlock::new(vec![
        V35BuildRow::new(0, 10_000, 1, vec![0.0; 384]).unwrap(),
    ])
    .unwrap();
    assert!(
        small
            .projected_peak_live_bytes(&model, &projection)
            .unwrap()
            < 64 * 1_048_576
    );

    let oversized = V35BuildBlock::new(
        (0..16_000_u64)
            .map(|ordinal| V35BuildRow::new(ordinal, 10_000 + ordinal, 1, vec![0.0; 384]).unwrap())
            .collect(),
    )
    .unwrap();
    assert!(
        oversized
            .projected_peak_live_bytes(&model, &projection)
            .is_err()
    );
}

#[test]
fn v35_build_artifacts_bind_source_projection_attempt_and_morton_model() {
    // Break caught: a valid scratch run can be relabeled across a different
    // source, projection, model, or construction attempt.
    let projection = projection();
    let authority = build_authority(&projection);
    let model = train_v35_morton_model(&training_rows(), &projection, authority.clone()).unwrap();
    assert_eq!(model.authority(), &authority);
    let model_bytes = model.canonical_bytes().unwrap();
    assert_eq!(
        V35MortonModel::from_canonical_bytes(&model_bytes)
            .unwrap()
            .authority(),
        &authority
    );

    let row = V35BuildRow::new(0, 10_000, 1, vec![0.0; 384]).unwrap();
    let mut source = Blocks(VecDeque::from([V35BuildBlock::new(vec![row]).unwrap()]));
    let mut scratch = Scratch::default();
    build_v35_scratch_runs(&model, &projection, &mut source, &mut scratch).unwrap();
    let (registered, bytes) = &scratch.writes[0];
    decode_v35_build_run_arrow(bytes, registered, &model).unwrap();

    for changed in [
        V35BuildAuthority::new(
            "attempt-02",
            "deep-image-100m",
            &"ab".repeat(32),
            projection.checksum(),
        )
        .unwrap(),
        V35BuildAuthority::new(
            "attempt-01",
            "other-source",
            &"ab".repeat(32),
            projection.checksum(),
        )
        .unwrap(),
        V35BuildAuthority::new(
            "attempt-01",
            "deep-image-100m",
            &"ac".repeat(32),
            projection.checksum(),
        )
        .unwrap(),
    ] {
        let changed_model = train_v35_morton_model(&training_rows(), &projection, changed).unwrap();
        assert!(decode_v35_build_run_arrow(bytes, registered, &changed_model).is_err());
    }

    let changed_projection_authority = V35BuildAuthority::new(
        "attempt-01",
        "deep-image-100m",
        &"ab".repeat(32),
        [0x32; 32],
    )
    .unwrap();
    assert!(
        train_v35_morton_model(&training_rows(), &projection, changed_projection_authority,)
            .is_err()
    );

    let mut changed_rows = training_rows();
    changed_rows[0][0] += 1.0;
    let changed_model = train_v35_morton_model(&changed_rows, &projection, authority).unwrap();
    assert!(decode_v35_build_run_arrow(bytes, registered, &changed_model).is_err());
}

struct MergeSource {
    runs: Vec<VecDeque<V35BuildMergeRow>>,
}

impl V35BuildMergeSource for MergeSource {
    fn run_count(&self) -> usize {
        self.runs.len()
    }

    fn next_row(&mut self, run: usize) -> Result<Option<V35BuildMergeRow>> {
        Ok(self.runs.get_mut(run).and_then(VecDeque::pop_front))
    }
}

#[derive(Default)]
struct LeafSink {
    leaves: Vec<Vec<V35BuildMergeRow>>,
}

impl V35BuildLeafSink for LeafSink {
    fn write_leaf(&mut self, rows: Vec<V35BuildMergeRow>) -> Result<()> {
        self.leaves.push(rows);
        Ok(())
    }
}

fn merge_source_with_rows(
    model: &V35MortonModel,
    projection: &V35Projection,
    row_count: u64,
) -> MergeSource {
    let mut runs = vec![Vec::new(), Vec::new(), Vec::new()];
    let rows_per_run = row_count.div_ceil(3);
    for source_ordinal in 0..row_count {
        let source = (0..384)
            .map(|dimension| ((source_ordinal * 31 + dimension as u64 * 7) % 4_093) as f32 / 53.0)
            .collect::<Vec<_>>();
        let projected = project_v35_query_scalar(projection, &source)
            .unwrap()
            .coordinates()
            .to_vec();
        let key = model.key(&projected).unwrap();
        let run = usize::try_from((source_ordinal / rows_per_run).min(2)).unwrap();
        runs[run].push(
            V35BuildMergeRow::new(
                key,
                source_ordinal,
                100_000 + source_ordinal,
                1,
                source,
                projected,
            )
            .unwrap(),
        );
    }
    for run in &mut runs {
        run.sort_by_key(|row| (row.morton_key(), row.source_ordinal()));
    }
    MergeSource {
        runs: runs.into_iter().map(VecDeque::from).collect(),
    }
}

fn merge_source(model: &V35MortonModel, projection: &V35Projection) -> MergeSource {
    merge_source_with_rows(model, projection, 600)
}

#[test]
fn v35_build_merge_streams_one_head_per_run_into_bounded_ordered_leaves() {
    // Break caught: external merge loads complete runs, loses total order at a
    // run boundary, or retains more than one 256-row leaf accumulator.
    let projection = projection();
    let model = train_v35_morton_model(&training_rows(), &projection, build_authority(&projection))
        .unwrap();
    let mut source = merge_source(&model, &projection);
    let mut sink = LeafSink::default();
    let receipt = merge_v35_build_runs(&model, &mut source, &mut sink).unwrap();
    assert_eq!(receipt.rows(), 600);
    assert_eq!(receipt.leaves(), 3);
    assert_eq!(receipt.peak_heads(), 3);
    assert_eq!(
        sink.leaves.iter().map(Vec::len).collect::<Vec<_>>(),
        [256, 256, 88]
    );
    let order = sink
        .leaves
        .iter()
        .flatten()
        .map(|row| (row.morton_key(), row.source_ordinal()))
        .collect::<Vec<_>>();
    assert!(order.windows(2).all(|pair| pair[0] < pair[1]));
    let mut ordinals = order
        .iter()
        .map(|(_, ordinal)| *ordinal)
        .collect::<Vec<_>>();
    ordinals.sort_unstable();
    assert_eq!(ordinals, (0..600).collect::<Vec<_>>());
}

#[test]
fn v35_build_merge_rejects_unbounded_fanin_and_untrusted_rows() {
    // Break caught: merge admits unbounded head state or trusts a scratch key
    // that is inconsistent with the authenticated Morton model and row.
    let projection = projection();
    let model = train_v35_morton_model(&training_rows(), &projection, build_authority(&projection))
        .unwrap();
    let mut source = merge_source(&model, &projection);
    let row = source.runs[0].front().unwrap().clone();
    source.runs[0][0] = V35BuildMergeRow::new(
        row.morton_key() ^ 1,
        row.source_ordinal(),
        row.id(),
        row.sequence(),
        row.source().to_vec(),
        row.projected().to_vec(),
    )
    .unwrap();
    assert!(merge_v35_build_runs(&model, &mut source, &mut LeafSink::default()).is_err());

    let mut too_many = MergeSource {
        runs: (0..33).map(|_| VecDeque::new()).collect(),
    };
    assert!(merge_v35_build_runs(&model, &mut too_many, &mut LeafSink::default()).is_err());
}

#[test]
fn v35_build_merge_leaf_seals_authenticated_projection_and_omitted_energy() {
    // Break caught: construction drops source-space complement energy, seals a
    // caller projection, or assigns patch bounds from Morton position.
    let projection = projection();
    let model = train_v35_morton_model(&training_rows(), &projection, build_authority(&projection))
        .unwrap();
    let mut source = merge_source(&model, &projection);
    let mut sink = LeafSink::default();
    merge_v35_build_runs(&model, &mut source, &mut sink).unwrap();
    let rows = &sink.leaves[0];
    let patch =
        build_v35_leaf_patch_from_merge_rows(rows, projection.dimensions(), 4, 9, 2_048).unwrap();
    assert_eq!(patch.leaf_ordinal(), 9);
    assert_eq!(patch.group_ordinal(), 4);
    assert_eq!(patch.logical_start(), 2_048);
    assert_eq!(patch.population(), 256);
    let expected_bounds = rows.iter().fold((u64::MAX, 0), |(minimum, maximum), row| {
        (
            minimum.min(row.source_ordinal()),
            maximum.max(row.source_ordinal()),
        )
    });
    assert_eq!(patch.assignment_bounds(), expected_bounds);
    let expected_omitted = rows
        .iter()
        .map(|row| {
            let source_energy = row.source().iter().fold(0.0_f64, |sum, value| {
                f64::from(*value).mul_add(f64::from(*value), sum)
            });
            let projected_energy = row
                .projected()
                .iter()
                .fold(0.0_f64, |sum, value| value.mul_add(*value, sum));
            (source_energy - projected_energy).max(0.0)
        })
        .sum::<f64>()
        / rows.len() as f64;
    assert_eq!(patch.omitted_energy(), expected_omitted as f32);
}

#[derive(Default)]
struct GroupSink {
    groups: Vec<V35BuildStorageGroup>,
}

impl V35BuildStorageGroupSink for GroupSink {
    fn write_group(&mut self, group: V35BuildStorageGroup) -> Result<()> {
        self.groups.push(group);
        Ok(())
    }
}

#[test]
fn v35_build_groups_streaming_leaves_by_dimension_bound_code_bytes() {
    // Break caught: grouping uses a fixed 96D row width, buffers every leaf,
    // emits a short nonterminal group, or exceeds the code-object target.
    let projection = projection();
    let model = train_v35_morton_model(&training_rows(), &projection, build_authority(&projection))
        .unwrap();
    let mut source = merge_source_with_rows(&model, &projection, 3_200);
    let mut groups = GroupSink::default();
    let mut assembler =
        V35BuildStorageGroupAssembler::new(projection.dimensions(), 4, &mut groups).unwrap();
    let merge = merge_v35_build_runs(&model, &mut source, &mut assembler).unwrap();
    assert_eq!(merge.rows(), 3_200);
    let receipt = assembler.finish().unwrap();
    assert_eq!(receipt.rows(), 3_200);
    assert_eq!(receipt.groups(), 2);
    assert!(receipt.peak_live_bytes() <= 64 * 1_048_576);
    assert_eq!(groups.groups.len(), 2);
    assert_eq!(groups.groups[0].row_count(), 2_730);
    assert_eq!(groups.groups[0].code_bytes(), 524_160);
    assert_eq!(groups.groups[1].row_count(), 470);
    assert_eq!(groups.groups[1].code_bytes(), 90_240);
    assert!(groups.groups[0].code_bytes() >= 349_526);
    assert!(
        groups
            .groups
            .iter()
            .all(|group| group.code_bytes() <= 524_288)
    );
    assert_eq!(groups.groups[0].group_ordinal(), 0);
    assert_eq!(groups.groups[1].group_ordinal(), 1);
    assert_eq!(groups.groups[0].logical_start(), 0);
    assert_eq!(groups.groups[1].logical_start(), 2_730);
}

fn grouping_row(source_ordinal: u64, source_dimensions: usize) -> V35BuildMergeRow {
    V35BuildMergeRow::new(
        u128::from(source_ordinal),
        source_ordinal,
        source_ordinal + 1,
        source_ordinal + 1,
        vec![source_ordinal as f32; source_dimensions],
        vec![source_ordinal as f64; 64],
    )
    .unwrap()
}

#[test]
fn v35_build_groups_split_merge_leaves_at_high_dimension() {
    // Break caught: treating 256-row merge batches as atomic makes 1,280D SQ8
    // unable to form a legal nonterminal group even though 409 rows fit.
    let dimensions = V35Dimensions {
        source: 1_280,
        routing: 64,
    };
    let mut groups = GroupSink::default();
    let mut assembler = V35BuildStorageGroupAssembler::new(dimensions, 8, &mut groups).unwrap();
    assembler
        .write_leaf((0..256).map(|row| grouping_row(row, 1_280)).collect())
        .unwrap();
    assembler
        .write_leaf((256..512).map(|row| grouping_row(row, 1_280)).collect())
        .unwrap();
    let receipt = assembler.finish().unwrap();
    assert_eq!(receipt.rows(), 512);
    assert_eq!(groups.groups.len(), 2);
    assert_eq!(groups.groups[0].row_count(), 409);
    assert_eq!(groups.groups[0].code_bytes(), 523_520);
    assert_eq!(groups.groups[1].row_count(), 103);
    assert_eq!(groups.groups[1].logical_start(), 409);
    assert!(
        groups
            .groups
            .iter()
            .flat_map(V35BuildStorageGroup::leaves)
            .all(|leaf| leaf.len() <= 256)
    );
}

#[test]
fn v35_build_groups_reject_morton_order_drift() {
    // Break caught: a caller other than the merge core can silently publish a
    // storage group whose rows are not in strict (Morton key,ordinal) order.
    let dimensions = V35Dimensions {
        source: 384,
        routing: 64,
    };
    let mut groups = GroupSink::default();
    let mut assembler = V35BuildStorageGroupAssembler::new(dimensions, 4, &mut groups).unwrap();
    let rows = vec![grouping_row(1, 384), grouping_row(0, 384)];
    assert!(assembler.write_leaf(rows).is_err());
}
