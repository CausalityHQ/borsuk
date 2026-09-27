//! Query-blind source mean and packed two-bit prefix parity.

use std::collections::HashSet;
use std::error::Error;
use std::fs;
use std::io;
use std::path::Path;
use std::time::Instant;

use borsuk::rotated_two_bit::RotatedTwoBitCodec;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

fn checked(path: &Path, hash: &str) -> Result<Vec<u8>, Box<dyn Error>> {
    let bytes = fs::read(path)?;
    if format!("{:x}", Sha256::digest(&bytes)) != hash {
        return Err(invalid("source identity").into());
    }
    Ok(bytes)
}

fn main() -> Result<(), Box<dyn Error>> {
    let args = std::env::args().collect::<Vec<_>>();
    if args.len() != 8 {
        return Err("usage: v294_rust_source_parity RAW SQ8 REFERENCE_DIR RECEIPT_SHA ROW_COUNT OUTPUT_RECORDS RESULT".into());
    }
    let count = args[5].parse::<usize>()?;
    if count != 512 && count != 100_000 {
        return Err(invalid("source sample count").into());
    }
    let directory = Path::new(&args[3]);
    let receipt_raw = checked(&directory.join("receipt.json"), &args[4])?;
    let receipt: Value = serde_json::from_slice(&receipt_raw)?;
    if receipt["schema"] != "borsuk-v294-source-reference-v1"
        || receipt["rows"] != 100_000
        || receipt["dimensions"] != 768
        || receipt["seed"] != 20260923
        || receipt["record_bytes"] != 200
        || receipt["query_or_truth_used"] != false
    {
        return Err(invalid("reference geometry").into());
    }
    let raw = checked(
        Path::new(&args[1]),
        "0f3631d71c105e5ea3d701c96033b362c2f84bd43002a9c8a5c70040801be06e",
    )?;
    let sq8 = checked(
        Path::new(&args[2]),
        "301696df05ca03122951b66ad8a9bedb5d5f1e675c6fc66f6019abbce3fcda58",
    )?;
    let reference = checked(
        &directory.join("reference.bin"),
        receipt["reference_sha256"]
            .as_str()
            .ok_or_else(|| invalid("reference digest"))?,
    )?;
    let declared_mean = checked(
        &directory.join("mean.bin"),
        receipt["mean_sha256"]
            .as_str()
            .ok_or_else(|| invalid("mean digest"))?,
    )?;
    if raw.len() != 100_000 * 768 * 4
        || sq8.len() != 78_000_000
        || reference.len() != 20_000_000
        || declared_mean.len() != 768 * 4
    {
        return Err(invalid("source geometry").into());
    }
    let started = Instant::now();
    let mut sums = vec![0.0_f64; 768];
    for row in raw.chunks_exact(768 * 4) {
        for (coordinate, bytes) in row.chunks_exact(4).enumerate() {
            let value = f32::from_le_bytes(bytes.try_into().unwrap());
            if !value.is_finite() {
                return Err(invalid("nonfinite source").into());
            }
            sums[coordinate] += f64::from(value);
        }
    }
    let mean = sums
        .into_iter()
        .map(|sum| (sum / 100_000.0) as f32)
        .collect::<Vec<_>>();
    let mean_bytes = mean
        .iter()
        .flat_map(|value| value.to_le_bytes())
        .collect::<Vec<_>>();
    if mean_bytes != declared_mean {
        return Err(invalid("source mean parity").into());
    }
    let ids = sq8
        .chunks_exact(780)
        .map(|row| usize::try_from(i64::from_le_bytes(row[..8].try_into().unwrap())))
        .collect::<Result<Vec<_>, _>>()?;
    if ids.iter().copied().collect::<HashSet<_>>().len() != 100_000
        || ids.iter().any(|&id| id >= 100_000)
    {
        return Err(invalid("physical source IDs").into());
    }
    let codec = RotatedTwoBitCodec::new(&mean, 20260923)?;
    let mut records = Vec::with_capacity(count * 200);
    let mut row = vec![0.0_f32; 768];
    for (physical, &id) in ids.iter().take(count).enumerate() {
        for (target, bytes) in row
            .iter_mut()
            .zip(raw[id * 768 * 4..(id + 1) * 768 * 4].chunks_exact(4))
        {
            *target = f32::from_le_bytes(bytes.try_into().unwrap());
        }
        let encoded = codec.encode(&row)?;
        if encoded[..196] != reference[physical * 200..physical * 200 + 196] {
            eprintln!("prefix differs at physical={physical} source={id}");
            return Err(invalid("packed prefix parity").into());
        }
        records.extend_from_slice(&encoded);
    }
    let result = json!({"schema":"borsuk-v294-rust-source-parity-v1",
        "dataset":"CoHere first100k D768 raw source", "rows_checked":count,
        "source_mean_parity":true,"all_prefixes_equal":true,
        "record_bytes":200,"record_plane_sha256":format!("{:x}",Sha256::digest(&records)),
        "reference_receipt_sha256":args[4],"source_wall_us":started.elapsed().as_micros()});
    fs::write(&args[6], &records)?;
    fs::write(&args[7], format!("{result}\n"))?;
    println!("{result}");
    Ok(())
}
