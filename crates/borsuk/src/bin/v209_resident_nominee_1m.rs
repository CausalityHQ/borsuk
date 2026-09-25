//! Frozen ReLAION-1M resident-generation build and whole-query measurement.

use std::{
    env,
    error::Error,
    fs::{self, File},
    io::{BufRead, BufReader, BufWriter, Read, Write},
    path::Path,
    sync::atomic::{AtomicUsize, Ordering},
    time::Instant,
};

use borsuk::{
    physical_row_permutation::{
        PhysicalRowPermutation, RowMapBinding, write_verified_row_permutation,
    },
    pq64_router_artifact::load_source_router,
    resident_fp16_tier::ResidentFp16Tier,
    resident_nominee_generation::{ResidentNomineeAuthority, ResidentNomineeGeneration},
};
use serde::Deserialize;
use serde_json::json;
use sha2::{Digest, Sha256};

const ROWS: u64 = 1_000_000;
const DIMS: usize = 768;
const GENERATION: u64 = 196;
const PLANE_SHA: &str = "1bce4288b38d88384503d8cfeae21667f45dbfb62303ce510f676fc0d66d4c47";
const NEW_SQ8_SHA: &str = "aecf0f2704f44906f411a74ab81b36e5e05f81bab35f4c70558e88acbc4d05c9";
const MANIFEST_SHA: &str = "c188766121a6e77f48cdc705bf579431192f93548d2e322bba0f429506adb36e";

#[derive(Deserialize)]
struct Request {
    query_ordinal: usize,
    query: Vec<f32>,
}

fn digest(path: &Path) -> Result<String, Box<dyn Error>> {
    let mut file = BufReader::new(File::open(path)?);
    let mut buffer = [0_u8; 1024 * 1024];
    let mut hasher = Sha256::new();
    loop {
        let count = file.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        hasher.update(&buffer[..count]);
    }
    Ok(format!("{:x}", hasher.finalize()))
}

fn map_binding<'a>(
    router: &'a borsuk::pq64_router_artifact::SourceRouterArtifact,
) -> RowMapBinding<'a> {
    RowMapBinding {
        generation: GENERATION,
        rows: ROWS,
        source_sha256: &router.source_sha256,
        router_manifest_sha256: &router.manifest_sha256,
        old_sq8_sha256: &router.sq8_sha256,
        new_sq8_sha256: NEW_SQ8_SHA,
    }
}

fn verify_plane_ids(plane: &Path, sq8: &Path) -> Result<(), Box<dyn Error>> {
    let mut plane = BufReader::new(File::open(plane)?);
    let mut sq8 = BufReader::new(File::open(sq8)?);
    let mut header = [0_u8; 64];
    plane.read_exact(&mut header)?;
    if &header[..8] != b"BORSF160" {
        return Err("plane format differs".into());
    }
    let mut plane_row = [0_u8; 1544];
    let mut sq8_row = [0_u8; 780];
    for index in 0..ROWS {
        plane.read_exact(&mut plane_row)?;
        sq8.read_exact(&mut sq8_row)?;
        if plane_row[..8] != sq8_row[..8] {
            return Err(format!("plane/SQ8 ID differs at physical row {index}").into());
        }
    }
    if plane.read(&mut [0])? != 0 || sq8.read(&mut [0])? != 0 {
        return Err("plane/SQ8 trailing bytes".into());
    }
    Ok(())
}

fn prepare(args: &[String]) -> Result<(), Box<dyn Error>> {
    if args.len() != 10 {
        return Err(
            "prepare ROUTER OLD_SQ8 NEW_SQ8 MAP_U32 PLANE MAP_OUT ROOT_OUT PLANE_KEY PLANE_ETAG"
                .into(),
        );
    }
    let router = load_source_router(Path::new(&args[1]), MANIFEST_SHA)?;
    if router.generation != GENERATION
        || router.router.rows() != ROWS as usize
        || router.router.dimensions() != DIMS
    {
        return Err("router geometry differs".into());
    }
    let mut raw = Vec::new();
    File::open(&args[4])?.read_to_end(&mut raw)?;
    if raw.len() != ROWS as usize * 4 {
        return Err("map length differs".into());
    }
    let rows = raw
        .chunks_exact(4)
        .map(|part| u32::from_le_bytes(part.try_into().unwrap()))
        .collect::<Vec<_>>();
    let map_sha = write_verified_row_permutation(
        Path::new(&args[6]),
        Path::new(&args[2]),
        Path::new(&args[3]),
        map_binding(&router),
        DIMS,
        &rows,
    )?;
    if digest(Path::new(&args[5]))? != PLANE_SHA {
        return Err("plane digest differs".into());
    }
    verify_plane_ids(Path::new(&args[5]), Path::new(&args[3]))?;
    if args[8].is_empty() || args[9].is_empty() {
        return Err("S3 object identity absent".into());
    }
    let root = json!({
        "schema":"borsuk-resident-nominee-generation-v1", "generation":GENERATION,
        "rows":ROWS, "dimensions":DIMS, "source_sha256":router.source_sha256,
        "router_manifest_sha256":router.manifest_sha256,
        "layout_sha256":router.layout_sha256, "row_map_sha256":map_sha,
        "resident_fp16_sha256":PLANE_SHA, "plane_object_key":args[8], "plane_etag":args[9],
    });
    fs::write(&args[7], serde_json::to_vec(&root)?)?;
    println!(
        "{}",
        json!({"schema":"borsuk-v209-prepare-v1", "row_map_sha256":map_sha,
        "root_sha256":digest(Path::new(&args[7]))?,"plane_sha256":PLANE_SHA,
        "new_sq8_sha256":NEW_SQ8_SHA,"plane_ids_equal_sq8":true})
    );
    Ok(())
}

fn percentile(sorted: &[u64], value: usize) -> u64 {
    sorted[(sorted.len() * value).div_ceil(100) - 1]
}

fn peak_rss_bytes() -> Result<u64, Box<dyn Error>> {
    let status = fs::read_to_string("/proc/self/status")?;
    let line = status
        .lines()
        .find(|line| line.starts_with("VmHWM:"))
        .ok_or("VmHWM missing")?;
    Ok(line
        .split_whitespace()
        .nth(1)
        .ok_or("VmHWM value missing")?
        .parse::<u64>()?
        * 1024)
}

fn serve(args: &[String]) -> Result<(), Box<dyn Error>> {
    if args.len() != 7 {
        return Err("serve ROUTER MAP PLANE ROOT REQUESTS RAW_OUT".into());
    }
    let started = Instant::now();
    let router = load_source_router(Path::new(&args[1]), MANIFEST_SHA)?;
    let root_raw = fs::read(&args[4])?;
    let authority = ResidentNomineeAuthority::load_authenticated(
        &root_raw,
        &format!("{:x}", Sha256::digest(&root_raw)),
    )?;
    let map_sha = digest(Path::new(&args[2]))?;
    let row_map = PhysicalRowPermutation::open_authenticated(
        Path::new(&args[2]),
        &map_sha,
        map_binding(&router),
    )?;
    let plane = ResidentFp16Tier::open_authenticated(
        Path::new(&args[3]),
        PLANE_SHA,
        &router.source_sha256,
        ROWS,
        DIMS,
        GENERATION,
        2_147_483_648,
    )?;
    let generation = ResidentNomineeGeneration::bind(&router, &row_map, &plane, &authority)?;
    let hydration_ns = started.elapsed().as_nanos() as u64;
    let requests = BufReader::new(File::open(&args[5])?)
        .lines()
        .map(|line| -> Result<Request, Box<dyn Error>> {
            Ok(serde_json::from_str::<Request>(&line?)?)
        })
        .collect::<Result<Vec<_>, Box<dyn Error>>>()?;
    if requests.len() != 1000
        || requests
            .iter()
            .enumerate()
            .any(|(i, row)| row.query_ordinal != i || row.query.len() != DIMS)
    {
        return Err("request panel differs".into());
    }
    let next = AtomicUsize::new(0);
    let mut answers = (0..requests.len())
        .map(|_| None)
        .collect::<Vec<Option<(Vec<u64>, u64)>>>();
    let wall = Instant::now();
    let threads = 8;
    std::thread::scope(|scope| {
        let mut handles = Vec::new();
        for _ in 0..threads {
            let next = &next;
            let requests = &requests;
            let generation = &generation;
            handles.push(
                scope.spawn(move || -> Result<Vec<(usize, Vec<u64>, u64)>, String> {
                    let mut records = Vec::new();
                    loop {
                        let index = next.fetch_add(1, Ordering::Relaxed);
                        if index >= requests.len() {
                            break;
                        }
                        let started = Instant::now();
                        let ids = generation
                            .search_cosine(&requests[index].query, 1024, 512, 100)
                            .map_err(|error| error.to_string())?;
                        records.push((index, ids, started.elapsed().as_nanos() as u64));
                    }
                    Ok(records)
                }),
            );
        }
        for handle in handles {
            for (index, ids, ns) in handle.join().map_err(|_| "worker panic")?? {
                answers[index] = Some((ids, ns));
            }
        }
        Ok::<(), String>(())
    })
    .map_err(|error| format!("query worker: {error}"))?;
    let wall_ns = wall.elapsed().as_nanos() as u64;
    let mut writer = BufWriter::new(File::create(&args[6])?);
    let mut times = Vec::with_capacity(1000);
    for (index, result) in answers.into_iter().enumerate() {
        let (ids, ns) = result.ok_or("missing query result")?;
        if ids.len() != 100 {
            return Err("returned list length differs".into());
        }
        times.push(ns);
        serde_json::to_writer(
            &mut writer,
            &json!({"ordinal":index,"returned_ids":ids,
            "request_ns":ns,"vector_body_gets":0,"vector_body_bytes":0}),
        )?;
        writer.write_all(b"\n")?;
    }
    writer.flush()?;
    times.sort_unstable();
    println!(
        "{}",
        json!({"schema":"borsuk-v209-resident-serving-v1",
        "dataset":"ReLAION-1M D768", "split":"validation-1000-already-used",
        "queries":1000,"threads":threads,"p50_ns":percentile(&times,50),
        "p95_ns":percentile(&times,95),"p99_ns":percentile(&times,99),
        "wall_ns":wall_ns,"throughput_qps":1000.0 / (wall_ns as f64 / 1e9),
        "cold_hydration_ns":hydration_ns,"process_peak_rss_bytes":peak_rss_bytes()?,
        "resident_plane_bytes":plane.resident_bytes(),"vector_body_gets":0,
        "vector_body_bytes":0,"raw_sha256":digest(Path::new(&args[6]))?})
    );
    Ok(())
}

fn main() -> Result<(), Box<dyn Error>> {
    let args = env::args().skip(1).collect::<Vec<_>>();
    match args.first().map(String::as_str) {
        Some("prepare") => prepare(&args),
        Some("serve") => serve(&args),
        _ => Err("usage: v209_resident_nominee_1m prepare|serve ...".into()),
    }
}
