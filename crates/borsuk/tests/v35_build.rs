//! V35 bounded streaming writer and immutable-delta contracts.

use borsuk::{
    Result, V35ArtifactIdentity, V35BuildBlock, V35BuildBlockSource, V35BuildRow,
    V35BuildScratchSink, V35MortonModel, build_v35_scratch_runs, decode_v35_build_run_arrow,
    train_v35_morton_model,
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

#[test]
fn v35_build_morton_model_selects_variance_quantiles_and_msb_interleave() {
    // Break caught: build order depends on queries, coordinate ties select the
    // later dimension, equality crosses a quantile, or Morton bits are LSB-first.
    let rows = training_rows();
    let model = train_v35_morton_model(&rows).unwrap();
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
    let model = train_v35_morton_model(&training_rows()).unwrap();
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
        let decoded = decode_v35_build_run_arrow(bytes, registered).unwrap();
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
