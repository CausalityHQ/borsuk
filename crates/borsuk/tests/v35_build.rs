//! V35 bounded streaming writer and immutable-delta contracts.

use borsuk::{
    Result, V35ArtifactIdentity, V35BuildAuthority, V35BuildBlock, V35BuildBlockSource,
    V35BuildRow, V35BuildScratchSink, V35Dimensions, V35MortonModel, V35Projection,
    build_v35_scratch_runs, build_v35_srht, decode_v35_build_run_arrow, open_v35_build_run_cursor,
    project_v35_query_scalar, train_v35_morton_model,
};
use sha2::{Digest, Sha256};
use std::collections::VecDeque;

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
