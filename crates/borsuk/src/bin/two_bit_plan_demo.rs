//! Offline API plan replay for frozen quality splits; not a service benchmark.
use borsuk::two_bit_generation::{TwoBitGeneration, TwoBitGenerationLimits, TwoBitPlanTrace};
use sha2::{Digest, Sha256};
use std::{error::Error, fs, io::Write, path::Path};
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = std::env::args().collect::<Vec<_>>();
    let trace = args.len() == 9 && args[8] == "--trace";
    if args.len() != 6 && args.len() != 8 && !trace {
        return Err(
            "usage: two_bit_plan_demo ROOT ROOT_SHA REQUESTS REQUESTS_SHA OUTPUT [FIRST COUNT [--trace]]"
                .into(),
        );
    }
    let (first, count) = if args.len() >= 8 {
        let split = (args[6].parse::<usize>()?, args[7].parse::<usize>()?);
        if !matches!(split, (0, 64) | (256, 64) | (256, 744)) {
            return Err("unsupported frozen split".into());
        }
        split
    } else {
        (0, 64)
    };
    let generation = TwoBitGeneration::open(
        Path::new(&args[1]),
        &args[2],
        TwoBitGenerationLimits {
            max_memory_bytes: 1_073_741_824,
            max_active_queries: 1,
            max_query_bytes: 84 * 256 * 780,
            max_query_gets: 32,
            max_parallel_gets: 32,
            max_query_scratch_bytes: 400_000
                + if trace {
                    TwoBitPlanTrace::scratch_bytes(usize::MAX)
                } else {
                    0
                },
            already_pinned_bytes: 0,
        },
    )?;
    let input_size = fs::metadata(&args[3])?.len();
    if input_size > 64 * 1024 * 1024 {
        return Err("demo request file cap".into());
    }
    // Caller-owned immutable frozen requests; this is an offline demo.
    let requests = fs::read(&args[3])?;
    if requests.len() as u64 != input_size || format!("{:x}", Sha256::digest(&requests)) != args[4]
    {
        return Err("request identity".into());
    }
    let lines = std::str::from_utf8(&requests)?.lines().collect::<Vec<_>>();
    if lines.len() > 1000 || first + count > lines.len() {
        return Err("request roster size".into());
    }
    for (ordinal, line) in lines.iter().enumerate() {
        let request: serde_json::Value = serde_json::from_str(line)?;
        if request["query_ordinal"].as_u64() != Some(ordinal as u64) {
            return Err("request ordinal".into());
        }
    }
    let mut output = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&args[5])?;
    for (ordinal, line) in lines.iter().enumerate().skip(first).take(count) {
        let request: serde_json::Value = serde_json::from_str(line)?;
        let query: Vec<f32> = serde_json::from_value(request["query"].clone())?;
        let (plan, diagnostic) = if trace {
            generation.diagnostic_plan(&query).await?
        } else {
            (generation.plan(&query).await?, Default::default())
        };
        let mut record = serde_json::json!({"query_ordinal":ordinal,
            "ranges":plan.ranges.iter().map(|r|[r.start,r.end]).collect::<Vec<_>>(),
            "planned_bytes":plan.planned_bytes});
        if trace {
            record["ranked_candidate_pages"] = serde_json::json!(diagnostic.ranked_candidate_pages);
            record["seed_page"] = diagnostic.seed_page.into();
            record["primary_page"] = diagnostic.primary_page.into();
            record["seed_evaluated_units"] = serde_json::json!(diagnostic.seed_evaluated_units);
            record["walk_evaluated_units"] = serde_json::json!(diagnostic.walk_evaluated_units);
            record["seed_work_exhausted"] = diagnostic.seed_work_exhausted.into();
            record["walk_work_exhausted"] = diagnostic.walk_work_exhausted.into();
            record["selected_pages"] = serde_json::json!(plan.selected_pages);
        }
        writeln!(output, "{record}")?;
    }
    output.sync_all()?;
    Ok(())
}
