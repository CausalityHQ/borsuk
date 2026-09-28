//! Create SQ8 with the public Rust API from sealed source/order snapshots.
use borsuk::sq8_source::{build_sq8_source, normalize_source};
use sha2::{Digest, Sha256};
use std::{
    error::Error,
    fs::File,
    io::{BufReader, Read},
    path::Path,
};

fn main() -> Result<(), Box<dyn Error>> {
    let args = std::env::args().collect::<Vec<_>>();
    if args.len() != 8 {
        return Err("usage: build_sq8_source SOURCE SOURCE_SHA DIMENSIONS ORDER_LE_U64 ORDER_SHA MAX_PAYLOAD_BYTES NEW_OUTPUT (or normalize SOURCE SOURCE_SHA ROWS DIMENSIONS MAX_PAYLOAD_BYTES NEW_OUTPUT)".into());
    }
    if args[1] == "normalize" {
        let sha = normalize_source(
            Path::new(&args[2]),
            &args[3],
            args[4].parse()?,
            args[5].parse()?,
            Path::new(&args[7]),
            args[6].parse()?,
        )?;
        println!(
            "{}",
            serde_json::json!({"normalized_sha256":sha,"query_or_truth_used":false})
        );
        return Ok(());
    }
    let cap: usize = args[6].parse()?;
    let file = File::open(&args[4])?;
    let length = usize::try_from(file.metadata()?.len())?;
    // Order is caller-owned payload charged again by the library's admission.
    if length == 0 || length % 8 != 0 || length.checked_add(65536).is_none_or(|n| n > cap) {
        return Err("order geometry or payload cap".into());
    }
    let mut input = BufReader::with_capacity(65536, file);
    let mut digest = Sha256::new();
    let mut order = Vec::with_capacity(length / 8);
    for _ in 0..length / 8 {
        let mut bytes = [0_u8; 8];
        input.read_exact(&mut bytes)?;
        digest.update(bytes);
        order.push(u64::from_le_bytes(bytes));
    }
    if input.read(&mut [0])? != 0 || format!("{:x}", digest.finalize()) != args[5] {
        return Err("order identity".into());
    }
    drop(input);
    let receipt = build_sq8_source(
        Path::new(&args[1]),
        &args[2],
        args[3].parse()?,
        &order,
        Path::new(&args[7]),
        cap,
    )?;
    println!(
        "{}",
        serde_json::json!({"low":receipt.low,"step":receipt.step,
        "sq8_sha256":receipt.sha256,"rows":order.len(),"query_or_truth_used":false})
    );
    Ok(())
}
