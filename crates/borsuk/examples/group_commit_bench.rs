//! Positioned group-commit smoke/qualification harness.
//!
//! The former lane-oriented harness described a protocol that no longer
//! exists. V12 reports the single authoritative source position assigned to
//! each caller batch.

use std::{env, error::Error, time::Duration};

use borsuk::{BorsukIndex, GroupCommitConfig, GroupCommitWriter, VectorRecord};

fn required(name: &str) -> Result<String, Box<dyn Error>> {
    env::var(name).map_err(|_| format!("missing required environment variable {name}").into())
}

fn main() -> Result<(), Box<dyn Error>> {
    let uri = required("BORSUK_GROUP_COMMIT_URI")?;
    let records = env::var("BORSUK_GROUP_COMMIT_RECORDS")
        .ok()
        .map(|value| value.parse())
        .transpose()?
        .unwrap_or(1_000_usize);
    let dimensions = env::var("BORSUK_GROUP_COMMIT_DIMENSIONS")
        .ok()
        .map(|value| value.parse())
        .transpose()?
        .unwrap_or(2_usize);
    let workers = env::var("BORSUK_GROUP_COMMIT_WORKERS")
        .ok()
        .map(|value| value.parse())
        .transpose()?
        .unwrap_or(8_usize);

    let index = BorsukIndex::open(&uri)?;
    let writer = GroupCommitWriter::new(
        index,
        GroupCommitConfig {
            max_delay: Duration::from_millis(2),
            max_records: 1_024,
            workers,
        },
    )?;
    let batch = (0..records)
        .map(|ordinal| {
            VectorRecord::new(
                format!("positioned-bench-{ordinal}"),
                (0..dimensions)
                    .map(|dimension| (ordinal ^ dimension) as f32)
                    .collect(),
            )
        })
        .collect();
    let receipt = writer.append(batch)?;
    writer.drain()?;
    let position = receipt
        .position
        .ok_or("a non-empty positioned group append has no source position")?;
    println!(
        "records={} committed_records={} source_epoch={} shard={} sequence={} envelope_checksum={} encoded_bytes={} requests={}",
        receipt.records,
        receipt.committed_records,
        position.source_epoch,
        position.shard,
        position.sequence,
        receipt.envelope_checksum,
        receipt.encoded_bytes,
        receipt.requests.total(),
    );
    Ok(())
}
