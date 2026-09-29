//! Create SQ8 with the public Rust API from sealed source/order snapshots.
use borsuk::sq8_source::{build_sq8_source, normalize_source};
use sha2::{Digest, Sha256};
use std::{
    error::Error,
    fs::File,
    io::{BufReader, Read, Write},
    path::Path,
};

fn main() -> Result<(), Box<dyn Error>> {
    let args = std::env::args().collect::<Vec<_>>();
    if args.len() != 8 {
        return Err("usage: build_sq8_source SOURCE SOURCE_SHA DIMENSIONS ORDER_LE_U64 ORDER_SHA MAX_PAYLOAD_BYTES NEW_OUTPUT (or normalize SOURCE SOURCE_SHA ROWS DIMENSIONS MAX_PAYLOAD_BYTES NEW_OUTPUT; fit/hier-fit have the same arguments as normalize)".into());
    }
    if args[1] == "fit" || args[1] == "hier-fit" {
        let output = Path::new(&args[7]);
        if output.exists() {
            return Err("output already exists".into());
        }
        let (order, extents) = if args[1] == "hier-fit" {
            let layout = borsuk::source_order::fit_hierarchical_source_layout(
                Path::new(&args[2]),
                &args[3],
                args[4].parse()?,
                args[5].parse()?,
                args[6].parse()?,
            )?;
            (layout.order, Some(layout.extents))
        } else {
            (
                borsuk::source_order::fit_source_order(
                    Path::new(&args[2]),
                    &args[3],
                    args[4].parse()?,
                    args[5].parse()?,
                    args[6].parse()?,
                )?,
                None,
            )
        };
        let parent = output
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        let mut pending = tempfile::NamedTempFile::new_in(parent)?;
        let mut digest = Sha256::new();
        for ordinal in &order {
            let bytes = ordinal.to_le_bytes();
            pending.write_all(&bytes)?;
            digest.update(bytes);
        }
        pending.as_file().sync_all()?;
        pending.persist_noclobber(output).map_err(|e| e.error)?;
        File::open(parent)?.sync_all()?;
        let mut receipt = serde_json::json!({"order_sha256":format!("{:x}",digest.finalize()),
            "rows":order.len(), "recipe":"borsuk-semantic-order-chacha8-f32-v1", "query_or_truth_used":false});
        if let Some(extents) = extents {
            receipt["recipe"] = "borsuk-hierarchical-extents-chacha8-v3".into();
            receipt["source_cell_order"] =
                "nearest-unvisited-layer0-entry-ordinal-fallback-v1".into();
            receipt["source_cell_target_rows"] = 256.into();
            receipt["sampling_cell_target_rows"] = 1024.into();
            receipt["samples_per_sampling_cell"] = 64.into();
            receipt["extent_row_cap"] = 1024.into();
            receipt["extents"] =
                serde_json::json!(extents.iter().map(|r| [r.start, r.end]).collect::<Vec<_>>());
        }
        println!("{receipt}");
        return Ok(());
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
