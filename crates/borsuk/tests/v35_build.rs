//! V35 bounded streaming writer and immutable-delta contracts.

use borsuk::{
    Result, V35ArtifactIdentity, V35BuildAuthority, V35BuildBlock, V35BuildBlockSource,
    V35BuildRow, V35BuildScratchSink, V35MortonModel, build_v35_scratch_runs,
    decode_v35_build_run_arrow, open_v35_build_run_cursor, train_v35_morton_model,
};
use sha2::{Digest, Sha256};
use std::collections::VecDeque;

fn training_rows() -> Vec<Vec<f64>> {
    (0..256)
        .map(|row| {
            (0..18)
                .map(|dimension| {
                    let centered = f64::from(row) - 127.5;
                    let scale = if dimension < 16 {
                        f64::from(18 - dimension)
                    } else {
                        1.0
                    };
                    centered * scale
                })
                .collect()
        })
        .collect()
}

fn build_authority() -> V35BuildAuthority {
    V35BuildAuthority::new(
        "attempt-01",
        "deep-image-100m",
        &"ab".repeat(32),
        [0x31; 32],
    )
    .unwrap()
}

#[test]
fn v35_build_scratch_cursor_authenticates_and_streams_bounded_batches() {
    // Break caught: external merge materializes a complete run, drops payload
    // fields, or loses total `(Morton key,source ordinal)` order at a batch edge.
    let model = train_v35_morton_model(&training_rows(), build_authority()).unwrap();
    let rows = (0..520_u64)
        .map(|source_ordinal| {
            let source = (0..384)
                .map(|dimension| (source_ordinal + dimension as u64) as f32 / 19.0)
                .collect::<Vec<_>>();
            let projected = (0..18)
                .map(|dimension| ((source_ordinal * 29 + dimension as u64 * 7) % 997) as f64 / 31.0)
                .collect::<Vec<_>>();
            V35BuildRow::new(
                source_ordinal,
                50_000 + source_ordinal,
                3,
                source,
                projected,
            )
            .unwrap()
        })
        .collect::<Vec<_>>();
    let mut source = Blocks(VecDeque::from([V35BuildBlock::new(rows).unwrap()]));
    let mut scratch = Scratch::default();
    build_v35_scratch_runs(&model, &mut source, &mut scratch).unwrap();
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
            assert_eq!(batch.projected(row).unwrap().len(), 18);
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
    let model = train_v35_morton_model(&rows, build_authority()).unwrap();
    assert_eq!(
        model.selected_coordinates(),
        &(0_u16..16).collect::<Vec<_>>()
    );
    assert_eq!(model.boundaries(0).unwrap().len(), 255);
    assert_eq!(model.boundaries(0).unwrap()[0], -2295.0);
    assert_eq!(model.boundaries(0).unwrap()[254], 2277.0);

    let exact_boundary = rows[1].clone();
    let just_above = exact_boundary
        .iter()
        .enumerate()
        .map(|(dimension, value)| {
            if dimension < 16 {
                value + f64::EPSILON * value.abs().max(1.0)
            } else {
                *value
            }
        })
        .collect::<Vec<_>>();
    assert_eq!(model.key(&exact_boundary).unwrap(), 0xffff);
    assert_eq!(model.key(&just_above).unwrap(), 0xffff_0000);

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
    let model = train_v35_morton_model(&training_rows(), build_authority()).unwrap();
    let rows = (0..32_u64)
        .map(|source_ordinal| {
            let source = (0..384)
                .map(|dimension| (source_ordinal * 17 + dimension) as f32 / 31.0)
                .collect::<Vec<_>>();
            let projected = (0..18)
                .map(|dimension| (31_i64 - source_ordinal as i64) as f64 * (dimension + 1) as f64)
                .collect::<Vec<_>>();
            V35BuildRow::new(
                source_ordinal,
                10_000 + source_ordinal,
                1,
                source,
                projected,
            )
            .unwrap()
        })
        .collect::<Vec<_>>();
    let blocks = rows
        .chunks(8)
        .map(|rows| V35BuildBlock::new(rows.to_vec()).unwrap())
        .collect::<VecDeque<_>>();
    let mut source = Blocks(blocks);
    let mut scratch = Scratch::default();
    let receipt = build_v35_scratch_runs(&model, &mut source, &mut scratch).unwrap();
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
                .all(|row| row.source_dimensions() == 384 && row.projected_dimensions() == 18)
        );
    }
}

#[test]
fn v35_build_block_preflights_live_memory_before_encoding() {
    // Break caught: the builder allocates Morton order, flattened Arrow
    // columns, and the complete output before discovering that the block
    // exceeds its 64-MiB live-memory admission.
    let model = train_v35_morton_model(&training_rows(), build_authority()).unwrap();
    let small = V35BuildBlock::new(vec![
        V35BuildRow::new(0, 10_000, 1, vec![0.0; 384], vec![0.0; 18]).unwrap(),
    ])
    .unwrap();
    assert!(small.projected_peak_live_bytes(&model).unwrap() < 64 * 1_048_576);

    let oversized = V35BuildBlock::new(vec![
        V35BuildRow::new(0, 10_000, 1, vec![0.0; 6_000_000], vec![0.0; 18]).unwrap(),
    ])
    .unwrap();
    assert!(oversized.projected_peak_live_bytes(&model).is_err());
}

#[test]
fn v35_build_artifacts_bind_source_projection_attempt_and_morton_model() {
    // Break caught: a valid scratch run can be relabeled across a different
    // source, projection, model, or construction attempt.
    let authority = build_authority();
    let model = train_v35_morton_model(&training_rows(), authority.clone()).unwrap();
    assert_eq!(model.authority(), &authority);
    let model_bytes = model.canonical_bytes().unwrap();
    assert_eq!(
        V35MortonModel::from_canonical_bytes(&model_bytes)
            .unwrap()
            .authority(),
        &authority
    );

    let row = V35BuildRow::new(0, 10_000, 1, vec![0.0; 384], vec![0.0; 18]).unwrap();
    let mut source = Blocks(VecDeque::from([V35BuildBlock::new(vec![row]).unwrap()]));
    let mut scratch = Scratch::default();
    build_v35_scratch_runs(&model, &mut source, &mut scratch).unwrap();
    let (registered, bytes) = &scratch.writes[0];
    decode_v35_build_run_arrow(bytes, registered, &model).unwrap();

    for changed in [
        V35BuildAuthority::new(
            "attempt-02",
            "deep-image-100m",
            &"ab".repeat(32),
            [0x31; 32],
        )
        .unwrap(),
        V35BuildAuthority::new("attempt-01", "other-source", &"ab".repeat(32), [0x31; 32]).unwrap(),
        V35BuildAuthority::new(
            "attempt-01",
            "deep-image-100m",
            &"ac".repeat(32),
            [0x31; 32],
        )
        .unwrap(),
        V35BuildAuthority::new(
            "attempt-01",
            "deep-image-100m",
            &"ab".repeat(32),
            [0x32; 32],
        )
        .unwrap(),
    ] {
        let changed_model = train_v35_morton_model(&training_rows(), changed).unwrap();
        assert!(decode_v35_build_run_arrow(bytes, registered, &changed_model).is_err());
    }

    let mut changed_rows = training_rows();
    changed_rows[0][0] += 1.0;
    let changed_model = train_v35_morton_model(&changed_rows, authority).unwrap();
    assert!(decode_v35_build_run_arrow(bytes, registered, &changed_model).is_err());
}
