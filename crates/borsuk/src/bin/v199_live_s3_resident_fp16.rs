//! Live conditional S3 execution of the frozen V198 physical plans.

use std::{
    error::Error,
    fs::{self, File},
    io::{BufRead, BufReader, BufWriter, Write},
    path::Path,
    time::Instant,
};

use borsuk::{
    exact_sq8_nominee::Sq8Geometry,
    native_source_tier::SourceCandidate,
    resident_fp16_tier::ResidentFp16Tier,
    returned_sq8::{ReturnedRange, rank_returned_ranges},
    sq8_page_authority::PageAuthority,
    sq8_s3_range::{OneAttemptS3, VerifiedRange},
};
use futures_util::future::join_all;
use object_store::path::Path as ObjectPath;
use serde::Deserialize;
use serde_json::json;
use sha2::{Digest, Sha256};

const ROWS: usize = 1_000_000;
const DIMENSIONS: usize = 768;
const PAGE_BYTES: usize = 32 * (DIMENSIONS + 12);
const OBJECT_SHA: &str = "aecf0f2704f44906f411a74ab81b36e5e05f81bab35f4c70558e88acbc4d05c9";
const PLANE_SHA: &str = "1bce4288b38d88384503d8cfeae21667f45dbfb62303ce510f676fc0d66d4c47";
const SOURCE_SHA: &str = "2796b579f37afe99ca4aff57e282335a6a79ad30596645957d26326a0560cf86";
const CASES_SHA: &str = "e1ba95fbd0cb58e28f1601740edf8c8775230e2ab5c8954e7f11c522dd94cd39";
const PLANS_SHA: &str = "0a61974457030d2e2ce828e7bbbbaf3d8acd9849e70a7aaafd0cb44b0c5dda00";
const LOW_SHA: &str = "ce036f48f918312f694adbd12994646987eb0e144df20f64ffe583bed0d1f891";
const STEP_SHA: &str = "64d49c7413f163fb3446183629d743f69199156f208fa959523d1bcde27da69c";

#[derive(Deserialize)]
struct Candidate {
    ordinal: u64,
    source_id: u64,
}

#[derive(Deserialize)]
struct Case {
    ordinal: usize,
    query: Vec<f32>,
    candidates: Vec<Candidate>,
    expected: Vec<u64>,
}

#[derive(Deserialize)]
struct Arm {
    feasible: bool,
    intervals: Vec<[usize; 2]>,
    units: usize,
    bytes: usize,
    gets: usize,
}

#[derive(Deserialize)]
struct Plan {
    ordinal: usize,
    unit_cap: usize,
    optional_risk: Arm,
}

fn invalid(message: impl Into<String>) -> Box<dyn Error> {
    std::io::Error::new(std::io::ErrorKind::InvalidData, message.into()).into()
}

fn checked(path: &Path, expected: &str) -> Result<Vec<u8>, Box<dyn Error>> {
    let bytes = fs::read(path)?;
    if format!("{:x}", Sha256::digest(&bytes)) != expected {
        return Err(invalid(format!("SHA-256 differs: {}", path.display())));
    }
    Ok(bytes)
}

fn records<T: for<'de> Deserialize<'de>>(bytes: &[u8]) -> Result<Vec<T>, Box<dyn Error>> {
    BufReader::new(bytes)
        .lines()
        .map(|line| {
            let line = line?;
            Ok(serde_json::from_str::<T>(&line)?)
        })
        .collect()
}

fn coefficients(path: &Path, expected: &str) -> Result<Vec<f32>, Box<dyn Error>> {
    let bytes = checked(path, expected)?;
    if bytes.len() != DIMENSIONS * 4 {
        return Err(invalid("SQ8 coefficient width differs"));
    }
    Ok(bytes
        .chunks_exact(4)
        .map(|part| f32::from_le_bytes(part.try_into().unwrap()))
        .collect())
}

fn percentile(values: &[u64], percent: usize) -> u64 {
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    sorted[(sorted.len() * percent).div_ceil(100) - 1]
}

fn peak_rss_bytes() -> Result<u64, Box<dyn Error>> {
    let status = fs::read_to_string("/proc/self/status")?;
    let value = status
        .lines()
        .find(|line| line.starts_with("VmHWM:"))
        .and_then(|line| line.split_whitespace().nth(1))
        .ok_or_else(|| invalid("VmHWM missing"))?;
    Ok(value.parse::<u64>()? * 1024)
}

async fn execute(args: &[String]) -> Result<(), Box<dyn Error>> {
    if args.len() != 13 {
        return Err(invalid(
            "usage: v199_live_s3_resident_fp16 BUCKET REGION KEY ETAG PLANE CASES PLANS LOW STEP PAGE_MANIFEST PAGE_DIGESTS OUTPUT_PREFIX",
        ));
    }
    let boot = Instant::now();
    let manifest = fs::read(&args[10])?;
    let manifest_sha = format!("{:x}", Sha256::digest(&manifest));
    let sidecar = fs::read(&args[11])?;
    let pages = PageAuthority::load(&manifest, &manifest_sha, &sidecar)
        .map_err(|error| invalid(format!("page authority {error:?}")))?;
    if pages.rows() != ROWS
        || pages.dimensions() != DIMENSIONS
        || pages.page_rows() != 32
        || pages.generation() != 196
        || pages.object_sha256() != OBJECT_SHA
        || pages.object_bytes() != ROWS * (DIMENSIONS + 12)
    {
        return Err(invalid("V199 page authority geometry differs"));
    }
    let cases = records::<Case>(&checked(Path::new(&args[6]), CASES_SHA)?)?;
    let plans = records::<Plan>(&checked(Path::new(&args[7]), PLANS_SHA)?)?;
    if cases.len() != 1000 || plans.len() != cases.len() {
        return Err(invalid("V199 frozen cohort differs"));
    }
    let low = coefficients(Path::new(&args[8]), LOW_SHA)?;
    let step = coefficients(Path::new(&args[9]), STEP_SHA)?;
    let tier = ResidentFp16Tier::open_authenticated(
        Path::new(&args[5]),
        PLANE_SHA,
        SOURCE_SHA,
        ROWS as u64,
        DIMENSIONS,
        196,
        2 * 1024 * 1024 * 1024,
    )?;
    let startup_ns = boot.elapsed().as_nanos();
    let reader = OneAttemptS3::new(&args[1], &args[2])
        .map_err(|error| invalid(format!("S3 reader {error:?}")))?;
    let location = ObjectPath::from(args[3].as_str());
    let prefix = &args[12];
    let mut raw = BufWriter::new(File::create(format!("{prefix}-raw.jsonl"))?);
    let run = Instant::now();
    let mut s3 = Vec::with_capacity(cases.len());
    let mut sq8 = Vec::with_capacity(cases.len());
    let mut fp16 = Vec::with_capacity(cases.len());
    let mut total = Vec::with_capacity(cases.len());
    let mut charged_gets = 0_usize;
    let mut charged_bytes = 0_usize;
    for (index, (case, plan)) in cases.iter().zip(&plans).enumerate() {
        let started = Instant::now();
        let arm = &plan.optional_risk;
        if case.ordinal != index
            || plan.ordinal != index
            || case.query.len() != DIMENSIONS
            || case.candidates.len() != 128
            || case.expected.len() != 100
            || !arm.feasible
            || arm.intervals.is_empty()
            || arm.intervals.len() > 32
            || arm.gets != arm.intervals.len()
            || arm.units > plan.unit_cap
        {
            return Err(invalid(format!("case/plan geometry differs at {index}")));
        }
        if arm.intervals.iter().any(|&[a, b]| a > b || b >= ROWS / 32)
            || arm
                .intervals
                .windows(2)
                .any(|pair| pair[0][1] >= pair[1][0])
        {
            return Err(invalid(format!("interval charge differs at {index}")));
        }
        let units = arm.intervals.iter().map(|&[a, b]| b - a + 1).sum::<usize>();
        if units != arm.units || arm.bytes != units * PAGE_BYTES {
            return Err(invalid(format!("interval charge differs at {index}")));
        }
        let fetch_started = Instant::now();
        let results = join_all(arm.intervals.iter().map(|&[first, last]| {
            reader.fetch_verified_pages(
                &location,
                &pages,
                first,
                last,
                &args[4],
                (last - first + 1) * PAGE_BYTES,
            )
        }))
        .await;
        charged_gets += arm.gets;
        let fetched = results
            .into_iter()
            .collect::<Result<Vec<VerifiedRange>, _>>()
            .map_err(|error| invalid(format!("V199 GET failure at {index}: {error:?}")))?;
        let s3_ns = fetch_started.elapsed().as_nanos() as u64;
        let response_bytes = fetched.iter().map(|range| range.bytes.len()).sum::<usize>();
        charged_bytes += response_bytes;
        if response_bytes != arm.bytes {
            return Err(invalid(format!("response byte charge differs at {index}")));
        }
        let returned = fetched
            .iter()
            .map(|range| ReturnedRange {
                start: range.start,
                bytes: &range.bytes,
            })
            .collect::<Vec<_>>();
        let sq8_started = Instant::now();
        let ranked = rank_returned_ranges(
            Sq8Geometry {
                rows: ROWS,
                dimensions: DIMENSIONS,
            },
            &returned,
            &case.query,
            &low,
            &step,
            128,
            arm.bytes,
        )
        .map_err(|error| invalid(format!("SQ8 score failure at {index}: {error:?}")))?;
        let sq8_ns = sq8_started.elapsed().as_nanos() as u64;
        if ranked
            .iter()
            .zip(&case.candidates)
            .any(|(actual, expected)| {
                actual.ordinal as u64 != expected.ordinal || actual.id != expected.source_id as i64
            })
        {
            return Err(invalid(format!("SQ8 shortlist parity differs at {index}")));
        }
        let shortlist = ranked
            .iter()
            .map(|item| SourceCandidate {
                ordinal: item.ordinal as u64,
                source_id: item.id as u64,
            })
            .collect::<Vec<_>>();
        let fp16_started = Instant::now();
        let ids = tier.rank_cosine(&case.query, &shortlist, 100)?;
        let fp16_ns = fp16_started.elapsed().as_nanos() as u64;
        if ids != case.expected {
            return Err(invalid(format!("FP16 returned parity differs at {index}")));
        }
        let total_ns = started.elapsed().as_nanos() as u64;
        writeln!(
            raw,
            "{}",
            json!({"ordinal":index,"gets":arm.gets,
            "bytes":response_bytes,"s3_ns":s3_ns,"sq8_ns":sq8_ns,
            "fp16_ns":fp16_ns,"total_ns":total_ns,"parity":true})
        )?;
        s3.push(s3_ns);
        sq8.push(sq8_ns);
        fp16.push(fp16_ns);
        total.push(total_ns);
    }
    raw.flush()?;
    let summary = json!({
        "schema":"borsuk-v199-live-s3-resident-fp16-v1",
        "queries":cases.len(),"ordered_sq8_shortlist_parity":cases.len(),
        "ordered_fp16_returned_parity":cases.len(),
        "submitted_gets":charged_gets,"response_bytes":charged_bytes,
        "etag":args[4],"page_manifest_sha256":manifest_sha,
        "startup_ns":startup_ns,"run_wall_ns":run.elapsed().as_nanos(),
        "peak_process_rss_bytes":peak_rss_bytes()?,
        "s3_ns":{"p50":percentile(&s3,50),"p95":percentile(&s3,95),"p99":percentile(&s3,99)},
        "sq8_ns":{"p50":percentile(&sq8,50),"p95":percentile(&sq8,95),"p99":percentile(&sq8,99)},
        "fp16_ns":{"p50":percentile(&fp16,50),"p95":percentile(&fp16,95),"p99":percentile(&fp16,99)},
        "total_ns":{"p50":percentile(&total,50),"p95":percentile(&total,95),"p99":percentile(&total,99)},
    });
    fs::write(
        format!("{prefix}-summary.json"),
        serde_json::to_vec(&summary)?,
    )?;
    if charged_gets != 10_047 || charged_bytes != 7_388_559_360 {
        return Err(invalid("V199 observed transport charge differs"));
    }
    Ok(())
}

fn main() -> Result<(), Box<dyn Error>> {
    let args = std::env::args().collect::<Vec<_>>();
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(execute(&args))
}
