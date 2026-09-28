//! Build query-blind nomination metadata through the public Rust library API.
use borsuk::two_bit_source::TwoBitSource;
use std::{error::Error, path::Path};
fn main() -> Result<(), Box<dyn Error>> {
    let args = std::env::args().collect::<Vec<_>>();
    if args.len() != 9 {
        return Err("usage: build_two_bit_source RAW RAW_SHA SQ8 SQ8_SHA ROWS DIMENSIONS MAX_MEMORY_BYTES OUTPUT_DIR".into());
    }
    let input = TwoBitSource {
        raw: Path::new(&args[1]),
        raw_sha256: &args[2],
        sq8: Path::new(&args[3]),
        sq8_sha256: &args[4],
        rows: args[5].parse()?,
        dimensions: args[6].parse()?,
    };
    let result = input.build(Path::new(&args[8]), args[7].parse()?)?;
    println!("{}", serde_json::to_string(&result)?);
    Ok(())
}
