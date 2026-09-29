//! Frozen plan replay and development-only native S3 serving integration.
#[path = "../native_development_memory.rs"]
mod native_development_memory;
use borsuk::two_bit_generation::{TwoBitGeneration, TwoBitGenerationLimits, TwoBitPlanTrace};
use borsuk::{
    sq8_s3_range::OneAttemptS3,
    two_bit_store::{publish_two_bit_generation, read_two_bit_head},
};
use object_store::{RetryConfig, aws::AmazonS3Builder, path::Path as ObjectPath};
use sha2::{Digest, Sha256};
use std::{error::Error, fs, io::Write, path::Path, time::Instant};
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = std::env::args().collect::<Vec<_>>();
    let paired = args.len() == 12 && args[8] == "--trace" && args[9] == "--paired";
    let live = args.len() == 12 && args[8] == "--live-s3";
    let trace = (args.len() == 9 && args[8] == "--trace") || paired;
    if args.len() != 6 && args.len() != 8 && !trace && !live {
        return Err(
            "usage: two_bit_plan_demo ROOT ROOT_SHA REQUESTS REQUESTS_SHA OUTPUT [FIRST COUNT [--trace [--paired ROOT ROOT_SHA] | --live-s3 BUCKET REGION PREFIX]]"
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
    if live {
        live_split(first, count)?;
    }
    let limits = TwoBitGenerationLimits {
        max_memory_bytes: native_development_memory::memory_limit()?,
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
    let mut generation = TwoBitGeneration::open(Path::new(&args[1]), &args[2], limits)?;
    let candidate = if paired {
        Some(TwoBitGeneration::open(
            Path::new(&args[10]),
            &args[11],
            TwoBitGenerationLimits {
                // Conservatively charge the control's entire admitted allowance.
                max_memory_bytes: limits
                    .max_memory_bytes
                    .checked_mul(2)
                    .ok_or("paired memory overflow")?,
                already_pinned_bytes: limits.max_memory_bytes,
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
    if live {
        // Release local metadata before publisher admission and remote reopen.
        drop(generation);
        let store = AmazonS3Builder::from_env()
            .with_bucket_name(&args[9])
            .with_region(&args[10])
            .with_retry(RetryConfig {
                max_retries: 0,
                ..Default::default()
            })
            .build()?;
        let prefix = ObjectPath::from(args[11].as_str());
        let wall = Instant::now();
        if read_two_bit_head(&store, &prefix).await?.is_some() {
            return Err("live publication requires a fresh namespace".into());
        }
        let published = publish_two_bit_generation(
            &store,
            &prefix,
            Path::new(&args[1]),
            &args[2],
            limits,
            None,
        )
        .await?;
        let publish_wall_ns = wall.elapsed().as_nanos();
        let wall = Instant::now();
        let head = read_two_bit_head(&store, &prefix)
            .await?
            .ok_or("published head missing")?;
        let head_read_wall_ns = wall.elapsed().as_nanos();
        if head.root_sha256() != args[2]
            || head.root_sha256() != published.root_sha256()
            || head.control_epoch() != published.control_epoch()
            || head.generation() != published.generation()
        {
            return Err("published head authority changed".into());
        }
        let scratch = tempfile::tempdir()?;
        let wall = Instant::now();
        generation = TwoBitGeneration::open_remote(
            &store,
            &head.metadata_prefix(),
            head.root_sha256(),
            limits,
            scratch.path(),
        )
        .await?;
        writeln!(
            output,
            "{}",
            serde_json::json!({"phase":"startup","root_sha256":head.root_sha256(),
            "control_epoch":head.control_epoch(),"generation":head.generation(),"publish_wall_ns":publish_wall_ns,
            "head_read_wall_ns":head_read_wall_ns,"remote_open_wall_ns":wall.elapsed().as_nanos()})
        )?;
        output.sync_all()?;
    }
    let reader = if live {
        Some(
            OneAttemptS3::new(&args[9], &args[10])
                .map_err(|error| format!("S3 reader: {error:?}"))?,
        )
    } else {
        None
    };
    let measurement = Instant::now();
    for (ordinal, line) in lines.iter().enumerate().skip(first).take(count) {
        let request: serde_json::Value = serde_json::from_str(line)?;
        let query: Vec<f32> = serde_json::from_value(request["query"].clone())?;
        if let Some(reader) = reader.as_ref() {
            let wall = Instant::now();
            let cpu = cpu_ns()?;
            let result = match generation.search(reader, &query, 100).await {
                Ok(result) => result,
                Err(error) => {
                    let stats = match &error {
                        borsuk::two_bit_generation::TwoBitGenerationError::Read(failure) => {
                            Some(failure.stats)
                        }
                        _ => None,
                    };
                    writeln!(
                        output,
                        "{}",
                        serde_json::json!({"phase":"query-error","query_ordinal":ordinal,
                        "query_wall_ns":wall.elapsed().as_nanos(),"error":error.to_string(),
                        "submitted_gets":stats.map(|s|s.submitted_gets),"verified_bytes":stats.map(|s|s.verified_bytes),
                        "failed_gets":stats.map(|s|s.failed_gets)})
                    )?;
                    output.sync_all()?;
                    return Err(error.into());
                }
            };
            let query_wall_ns = wall.elapsed().as_nanos();
            let query_cpu_ns = cpu_ns()?.checked_sub(cpu).ok_or("CPU clock")?;
            let stats = result.ranked.stats;
            writeln!(
                output,
                "{}",
                serde_json::json!({"phase":"query","query_ordinal":ordinal,
                "ranges":result.plan.ranges.iter().map(|r|[r.start,r.end]).collect::<Vec<_>>(),
                "planned_bytes":result.plan.planned_bytes,"ids":result.ranked.candidates.iter().map(|r|r.id).collect::<Vec<_>>(),
                "query_wall_ns":query_wall_ns,"query_process_cpu_ns":query_cpu_ns,
                "submitted_gets":stats.submitted_gets,"verified_bytes":stats.verified_bytes,"failed_gets":stats.failed_gets})
            )?;
            continue;
        }
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
                record["primary_page"] = diagnostic.primary_page.into();
                record["discoveries"] = serde_json::json!(diagnostic.discoveries);
                record["nomination_evaluated_units"] =
                    serde_json::json!(diagnostic.nomination_evaluated_units);
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
    if live {
        writeln!(
            output,
            "{}",
            serde_json::json!({"phase":"summary","count":count,"measurement_wall_ns":measurement.elapsed().as_nanos()})
        )?;
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

fn live_split(first: usize, count: usize) -> Result<(), Box<dyn Error>> {
    if (first, count) != (0, 64) {
        return Err("live serving restricted to consumed dev0–63".into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn live_scope_is_development_only() {
        assert!(live_split(0, 64).is_ok(), "live split not implemented");
        assert!(live_split(256, 64).is_err());
        assert!(live_split(256, 744).is_err());
        assert!(live_split(0, 65).is_err());
    }
}
