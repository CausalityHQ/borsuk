//! Offline API plan replay for frozen quality splits; not a service benchmark.
use borsuk::two_bit_generation::{TwoBitGeneration, TwoBitGenerationLimits, TwoBitPlanTrace};
use sha2::{Digest, Sha256};
use std::{error::Error, fs, io::Write, path::Path, time::Instant};
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = std::env::args().collect::<Vec<_>>();
    let paired = args.len() == 12 && args[8] == "--trace" && args[9] == "--paired";
    let trace = (args.len() == 9 && args[8] == "--trace") || paired;
    if args.len() != 6 && args.len() != 8 && !trace {
        return Err(
            "usage: two_bit_plan_demo ROOT ROOT_SHA REQUESTS REQUESTS_SHA OUTPUT [FIRST COUNT [--trace [--paired ROOT ROOT_SHA]]]"
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
    let limits = TwoBitGenerationLimits {
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
    };
    if paired && (first, count) != (0, 64) {
        return Err("paired replay restricted to consumed dev0–63".into());
    }
    let generation = TwoBitGeneration::open(Path::new(&args[1]), &args[2], limits)?;
    let candidate = if paired {
        Some(TwoBitGeneration::open(
            Path::new(&args[10]),
            &args[11],
            TwoBitGenerationLimits {
                // Conservatively charge the control's entire1GiB allowance.
                max_memory_bytes: 2_147_483_648,
                already_pinned_bytes: 1_073_741_824,
                ..limits
            },
        )?)
    } else {
        None
    };
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
        let mut roots = vec![("control", &generation, args[2].as_str())];
        if let Some(candidate) = candidate.as_ref() {
            roots.push(("candidate", candidate, args[11].as_str()));
            if ordinal % 2 == 1 {
                roots.reverse();
            }
        }
        for (arm, generation, root_sha) in roots {
            let wall = Instant::now();
            let cpu = if paired { Some(cpu_ns()?) } else { None };
            let (plan, diagnostic) = if trace {
                generation.diagnostic_plan(&query).await?
            } else {
                (generation.plan(&query).await?, Default::default())
            };
            let route_wall_ns = wall.elapsed().as_nanos();
            let route_cpu_ns = cpu
                .map(|start| {
                    cpu_ns().and_then(|end| end.checked_sub(start).ok_or("CPU clock".into()))
                })
                .transpose()?;
            let mut record = serde_json::json!({"query_ordinal":ordinal,
            "ranges":plan.ranges.iter().map(|r|[r.start,r.end]).collect::<Vec<_>>(),
            "planned_bytes":plan.planned_bytes});
            if trace {
                record["ranked_candidate_pages"] =
                    serde_json::json!(diagnostic.ranked_candidate_pages);
                record["seed_page"] = diagnostic.seed_page.into();
                record["primary_page"] = diagnostic.primary_page.into();
                record["seed_evaluated_units"] = serde_json::json!(diagnostic.seed_evaluated_units);
                record["walk_evaluated_units"] = serde_json::json!(diagnostic.walk_evaluated_units);
                record["seed_work_exhausted"] = diagnostic.seed_work_exhausted.into();
                record["walk_work_exhausted"] = diagnostic.walk_work_exhausted.into();
                record["selected_pages"] = serde_json::json!(plan.selected_pages);
            }
            if paired {
                record["arm"] = arm.into();
                record["root_sha256"] = root_sha.into();
                record["route_wall_ns"] = serde_json::json!(route_wall_ns);
                record["route_process_cpu_ns"] = serde_json::json!(route_cpu_ns.unwrap());
            }
            writeln!(output, "{record}")?;
        }
    }
    output.sync_all()?;
    Ok(())
}

fn cpu_ns() -> Result<u64, Box<dyn Error>> {
    let t = rustix::time::clock_gettime(rustix::time::ClockId::ProcessCPUTime);
    Ok(u64::try_from(
        i128::from(t.tv_sec) * 1_000_000_000 + i128::from(t.tv_nsec),
    )?)
}
