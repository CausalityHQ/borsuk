//! Offline API demonstration and frozen development plan replay; not a service benchmark.
use borsuk::two_bit_generation::{TwoBitGeneration, TwoBitGenerationLimits};
use sha2::{Digest, Sha256};
use std::{error::Error, fs, io::Write, path::Path};
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = std::env::args().collect::<Vec<_>>();
    if args.len() != 6 {
        return Err("usage: two_bit_plan_demo ROOT ROOT_SHA REQUESTS REQUESTS_SHA OUTPUT".into());
    }
    let generation = TwoBitGeneration::open(
        Path::new(&args[1]),
        &args[2],
        TwoBitGenerationLimits {
            max_memory_bytes: 1_073_741_824,
            max_active_queries: 1,
            max_query_bytes: 84 * 256 * 780,
            max_query_gets: 32,
            max_parallel_gets: 32,
            max_query_scratch_bytes: 400_000,
            already_pinned_bytes: 0,
        },
    )?;
    let input_size = fs::metadata(&args[3])?.len();
    if input_size > 64 * 1024 * 1024 {
        return Err("demo request file cap".into());
    }
    // Caller-owned immutable development requests; this is an offline demo.
    let requests = fs::read(&args[3])?;
    if requests.len() as u64 != input_size || format!("{:x}", Sha256::digest(&requests)) != args[4]
    {
        return Err("request identity".into());
    }
    let mut output = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&args[5])?;
    for (ordinal, line) in std::str::from_utf8(&requests)?.lines().enumerate() {
        if ordinal >= 64 {
            return Err("demo limited to 64 development queries".into());
        }
        let request: serde_json::Value = serde_json::from_str(line)?;
        let query: Vec<f32> = serde_json::from_value(request["query"].clone())?;
        let plan = generation.plan(&query).await?;
        writeln!(
            output,
            "{}",
            serde_json::json!({"query_ordinal":ordinal,
            "ranges":plan.ranges.iter().map(|r|[r.start,r.end]).collect::<Vec<_>>(),
            "planned_bytes":plan.planned_bytes})
        )?;
    }
    output.sync_all()?;
    Ok(())
}
