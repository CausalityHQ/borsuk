//! Development transport replay over live, generation-pinned S3 SQ8 ranges.
//! V122 fixes physical routes; this does not time router or planner execution.

use borsuk::exact_sq8_mirror::{ExactSq8Mirror, MirrorManifest, Placement};
use borsuk::exact_sq8_nominee::Sq8Geometry;
use borsuk::native_source_id_map::NativeSourceIdMap;
use borsuk::native_source_tier::NativeSourceTier;
use borsuk::pq64_router_artifact::load_source_router;
use borsuk::returned_sq8::{ReturnedRange, rank_returned_ranges};
use borsuk::serving_generation::{ExactServingGeneration, ServingGeneration};
use borsuk::sq8_page_authority::PageAuthority;
use borsuk::sq8_s3_range::{OneAttemptS3, VerifiedRange};
use futures_util::future::join_all;
use object_store::path::Path as ObjectPath;
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::HashSet,
    error::Error,
    fs::{self, File},
    io::{self, BufRead, BufReader, BufWriter, Write},
    path::Path,
    time::Instant,
};

const AUTHORITY_SHA: &str = "968ef7d795b53e5869400998ca19c6ab20d9b39830295fac3081c419f63f0f20";
const V131_REPLAY_SHA: &str = "9c687edc8ea66a5ef41b1e83d1d51788022b58cf526f2e9dcdb21a38bb5dd7d9";
const MAX_GETS: usize = 32;
const MAX_BYTES: usize = 16_777_216;
const TOP_K: usize = 100;
const EXPANSION: usize = 512;

#[derive(Deserialize)]
struct Query {
    query_ordinal: usize,
    source_query_ordinal: usize,
    query: Vec<f32>,
}

#[derive(Deserialize)]
struct Evidence {
    query_ordinal: usize,
    source_query_ordinal: usize,
    truth_ids: Vec<u64>,
    nominees: Vec<usize>,
    candidate_ranges: Vec<[usize; 2]>,
    baseline_ranges: Vec<[usize; 2]>,
}

#[derive(Deserialize)]
struct ReplayArm {
    returned_ids: Vec<u64>,
    sq8_returned_ids: Vec<i64>,
    hits: usize,
    candidate_count: usize,
    local_read_bytes: u64,
    verified_block_reads: u64,
}

#[derive(Deserialize)]
struct Replay {
    query_ordinal: usize,
    source_query_ordinal: usize,
    candidate: ReplayArm,
    baseline: ReplayArm,
}

fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}

fn sha256(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn field<'a>(value: &'a Value, section: &str, name: &str, key: &str) -> Result<&'a str, io::Error> {
    value[section][name][key]
        .as_str()
        .ok_or_else(|| invalid(format!("generation field {section}.{name}.{key}")))
}

fn checked_lines<T: for<'de> Deserialize<'de>>(
    path: &Path,
    expected_sha: &str,
) -> Result<Vec<T>, Box<dyn Error>> {
    if sha256(&fs::read(path)?) != expected_sha {
        return Err(invalid(format!("sealed input SHA-256: {}", path.display())).into());
    }
    let lines = BufReader::new(File::open(path)?);
    let mut records = Vec::new();
    for line in lines.lines() {
        records.push(serde_json::from_str::<T>(&line?)?);
    }
    Ok(records)
}

fn percentile_ms(values: &[u128], percentage: usize) -> f64 {
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    sorted[(sorted.len() * percentage).div_ceil(100) - 1] as f64 / 1_000_000.0
}

fn exact_page_spans(
    pages: &PageAuthority,
    ranges: &[[usize; 2]],
) -> Result<Vec<(usize, usize, usize)>, io::Error> {
    if ranges.is_empty() || ranges.len() > MAX_GETS {
        return Err(invalid("physical GET count"));
    }
    let full_page_bytes = pages
        .page_rows()
        .checked_mul(pages.dimensions() + 12)
        .ok_or_else(|| invalid("page byte width overflow"))?;
    let mut spans = Vec::with_capacity(ranges.len());
    let mut previous_end = 0;
    let mut total = 0usize;
    for &[start, end] in ranges {
        total = total
            .checked_add(
                end.checked_sub(start)
                    .ok_or_else(|| invalid("range order"))?,
            )
            .ok_or_else(|| invalid("range byte overflow"))?;
        if start < previous_end || end > pages.object_bytes() || total > MAX_BYTES || start == end {
            return Err(invalid("physical range cap or overlap"));
        }
        let first = start / full_page_bytes;
        let last = (end - 1) / full_page_bytes;
        if pages
            .byte_range(first, last)
            .map_err(|error| invalid(format!("page span {error:?}")))?
            != (start..end)
        {
            return Err(invalid("range is not an exact page span"));
        }
        spans.push((first, last, end - start));
        previous_end = end;
    }
    Ok(spans)
}

async fn run_arm(
    query_ordinal: usize,
    arm: &str,
    query: &[f32],
    truth: &[u64],
    nominees: &[usize],
    ranges: &[[usize; 2]],
    expected: &ReplayArm,
    mirror: &ExactSq8Mirror,
    generation: &ExactServingGeneration<'_>,
    reader: &OneAttemptS3,
    location: &ObjectPath,
) -> Result<Value, Box<dyn Error>> {
    let total_started = Instant::now();
    let mirror_started = Instant::now();
    let router_ids = mirror
        .score(nominees, query)?
        .into_iter()
        .map(|row| u64::try_from(row.id).map_err(|_| invalid("negative nominee ID")))
        .collect::<Result<Vec<_>, _>>()?;
    let mirror_ns = mirror_started.elapsed().as_nanos();
    let spans = exact_page_spans(generation.base().pages(), ranges)?;
    let s3_started = Instant::now();
    let received = join_all(spans.iter().map(|&(first, last, bytes)| {
        reader.fetch_verified_pages(
            location,
            generation.base().pages(),
            first,
            last,
            generation.base().etag(),
            bytes,
        )
    }))
    .await;
    let fetched = received
        .into_iter()
        .enumerate()
        .map(|(number, result)| {
            result.map_err(|error| invalid(format!(
                "query {query_ordinal} {arm} GET {number} failed after {} submitted requests and {} expected bytes: {error:?}",
                ranges.len(), spans.iter().map(|span| span.2).sum::<usize>(),
            )))
        })
        .collect::<Result<Vec<VerifiedRange>, _>>()?;
    let s3_ns = s3_started.elapsed().as_nanos();
    let response_bytes = fetched.iter().map(|range| range.bytes.len()).sum::<usize>();
    if response_bytes != spans.iter().map(|span| span.2).sum::<usize>() {
        return Err(invalid("S3 response byte inventory differs").into());
    }
    let returned = fetched
        .iter()
        .map(|range| ReturnedRange {
            start: range.start,
            bytes: &range.bytes,
        })
        .collect::<Vec<_>>();
    let sq8_ranked = rank_returned_ranges(
        generation.base().mirror().geometry,
        &returned,
        query,
        &generation.base().mirror().low,
        &generation.base().mirror().step,
        EXPANSION,
        MAX_BYTES,
    )
    .map_err(|error| invalid(format!("returned SQ8 score: {error:?}")))?;
    let sq8_first = sq8_ranked
        .iter()
        .take(TOP_K)
        .map(|row| row.id)
        .collect::<HashSet<_>>();
    let expected_sq8 = expected
        .sq8_returned_ids
        .iter()
        .copied()
        .collect::<HashSet<_>>();
    if sq8_first.len() != TOP_K || sq8_first != expected_sq8 {
        return Err(invalid(format!(
            "query {query_ordinal} {arm} SQ8 top-100 set differs"
        ))
        .into());
    }
    let expanded_ids = sq8_ranked
        .iter()
        .map(|row| u64::try_from(row.id).map_err(|_| invalid("negative SQ8 ID")))
        .collect::<Result<Vec<_>, _>>()?;
    let candidates =
        generation
            .map()
            .resolve_union(generation.source(), &router_ids, &expanded_ids)?;
    let source_started = Instant::now();
    let (ranked, local) = generation
        .source()
        .rank_exact_with_stats(query, &candidates, TOP_K)?;
    let source_ns = source_started.elapsed().as_nanos();
    if candidates.len() != expected.candidate_count
        || local.local_read_bytes != expected.local_read_bytes
        || local.verified_block_reads != expected.verified_block_reads
    {
        return Err(invalid(format!(
            "query {query_ordinal} {arm} source roster or reads differ"
        ))
        .into());
    }
    let ids = ranked.iter().map(|row| row.source_id).collect::<Vec<_>>();
    let actual = ids.iter().copied().collect::<HashSet<_>>();
    let expected_set = expected
        .returned_ids
        .iter()
        .copied()
        .collect::<HashSet<_>>();
    if actual.len() != TOP_K || actual != expected_set {
        return Err(invalid(format!(
            "query {query_ordinal} {arm} source top-100 set differs"
        ))
        .into());
    }
    let truth_set = truth.iter().copied().collect::<HashSet<_>>();
    let hits = actual.intersection(&truth_set).count();
    if hits != expected.hits {
        return Err(invalid(format!("query {query_ordinal} {arm} GT hits differ")).into());
    }
    Ok(json!({
        "hits": hits, "source_ids": ids, "candidate_count": candidates.len(),
        "gets": ranges.len(), "s3_response_bytes": response_bytes,
        "mirror_ns": mirror_ns, "s3_ns": s3_ns, "source_ns": source_ns,
        "total_ns": total_started.elapsed().as_nanos(),
        "local_read_bytes": local.local_read_bytes,
        "verified_block_reads": local.verified_block_reads,
    }))
}

async fn run() -> Result<(), Box<dyn Error>> {
    let args = std::env::args().collect::<Vec<_>>();
    if args.len() != 4 {
        return Err(invalid(
            "usage: v132_live_s3_transport prepared-dir output.jsonl summary.json",
        )
        .into());
    }
    let root = Path::new(&args[1]);
    let startup = Instant::now();
    let authority_bytes = fs::read(root.join("generation.json"))?;
    if sha256(&authority_bytes) != AUTHORITY_SHA {
        return Err(invalid("generation manifest SHA-256").into());
    }
    let authority: Value = serde_json::from_slice(&authority_bytes)?;
    if authority["schema"] != "borsuk-v132-live-serving-generation-v1"
        || authority["generation"] != 131
    {
        return Err(invalid("generation manifest identity").into());
    }
    let queries = checked_lines::<Query>(
        &root.join("queries.jsonl"),
        field(&authority, "inputs", "queries.jsonl", "sha256")?,
    )?;
    let evidence = checked_lines::<Evidence>(
        &root.join("evidence.jsonl"),
        field(&authority, "inputs", "evidence.jsonl", "sha256")?,
    )?;
    let replay = checked_lines::<Replay>(&root.join("replay.jsonl"), V131_REPLAY_SHA)?;
    if queries.len() != 1_000 || evidence.len() != queries.len() || replay.len() != queries.len() {
        return Err(invalid("sealed cohort count").into());
    }
    let router = load_source_router(
        &root.join("router"),
        field(&authority, "derived", "router_manifest", "sha256")?,
    )?;
    let mirror_manifest = MirrorManifest {
        format_version: 1,
        generation: 131,
        max_nominees: 512,
        geometry: Sq8Geometry {
            rows: 100_000,
            dimensions: 96,
        },
        object_sha256: field(&authority, "inputs", "sq8", "sha256")?.to_owned(),
        block_digest_sha256: field(&authority, "derived", "mirror_digests", "sha256")?.to_owned(),
        low: router.low.clone(),
        step: router.step.clone(),
    };
    let mirror = ExactSq8Mirror::open(
        &root.join("sq8.bin"),
        &root.join("mirror-digests.bin"),
        mirror_manifest.clone(),
        Placement::File,
    )?;
    let pages = PageAuthority::load(
        &fs::read(root.join("page-manifest.json"))?,
        field(&authority, "derived", "page_manifest", "sha256")?,
        &fs::read(root.join("page-digests.bin"))?,
    )
    .map_err(|error| invalid(format!("page authority {error:?}")))?;
    let source = NativeSourceTier::open_authenticated(
        &root.join("source-tier.bin"),
        field(&authority, "inputs", "source_tier", "sha256")?,
        authority["source_sha256"]
            .as_str()
            .ok_or_else(|| invalid("source SHA"))?,
        100_000,
        96,
        131,
        65_536,
    )?;
    let map = NativeSourceIdMap::open_authenticated(
        &root.join("source-id-map.bin"),
        field(&authority, "inputs", "source_id_map", "sha256")?,
        authority["source_sha256"]
            .as_str()
            .ok_or_else(|| invalid("source SHA"))?,
        field(&authority, "inputs", "source_tier", "sha256")?,
        100_000,
        131,
    )?;
    let sq8_uri = field(&authority, "inputs", "sq8", "uri")?;
    let sq8_key = sq8_uri
        .strip_prefix("s3://borsuk-bench-453182569524-euc1/")
        .ok_or_else(|| invalid("SQ8 bucket differs"))?;
    let sq8_etag = field(&authority, "inputs", "sq8", "etag")?;
    let base = ServingGeneration::bind(&router, &mirror_manifest, &pages, sq8_key, sq8_etag)
        .map_err(|error| invalid(format!("serving generation {error:?}")))?;
    let generation = ExactServingGeneration::bind(base, &source, &map)
        .map_err(|error| invalid(format!("exact generation {error:?}")))?;
    let reader = OneAttemptS3::new("borsuk-bench-453182569524-euc1", "eu-central-1")
        .map_err(|error| invalid(format!("S3 reader {error:?}")))?;
    let location = ObjectPath::from(sq8_key);
    let startup_ns = startup.elapsed().as_nanos();
    let mut output = BufWriter::new(File::create(&args[2])?);
    let mut totals = [0usize; 2];
    let mut gets = [0usize; 2];
    let mut bytes = [0usize; 2];
    let mut times = [Vec::<u128>::new(), Vec::<u128>::new()];
    let mut s3_times = [Vec::<u128>::new(), Vec::<u128>::new()];
    let mut paired = [0usize; 3];
    let mut local_bytes = [0u64; 2];
    let mut local_blocks = [0u64; 2];
    for (ordinal, ((query, sealed), historical)) in
        queries.iter().zip(&evidence).zip(&replay).enumerate()
    {
        if query.query_ordinal != ordinal
            || sealed.query_ordinal != ordinal
            || historical.query_ordinal != ordinal
            || query.source_query_ordinal != 9_000 + ordinal
            || sealed.source_query_ordinal != query.source_query_ordinal
            || historical.source_query_ordinal != query.source_query_ordinal
            || query.query.len() != 96
            || sealed.truth_ids.len() != TOP_K
            || sealed.nominees.len() != 512
        {
            return Err(invalid(format!("query {ordinal} identity differs")).into());
        }
        let mut arms = [
            (
                0,
                "candidate",
                &sealed.candidate_ranges,
                &historical.candidate,
            ),
            (1, "baseline", &sealed.baseline_ranges, &historical.baseline),
        ];
        if ordinal % 2 == 1 {
            arms.swap(0, 1);
        }
        let mut records = [Value::Null, Value::Null];
        for (index, name, ranges, expected) in arms {
            let record = run_arm(
                ordinal,
                name,
                &query.query,
                &sealed.truth_ids,
                &sealed.nominees,
                ranges,
                expected,
                &mirror,
                &generation,
                &reader,
                &location,
            )
            .await?;
            totals[index] += record["hits"]
                .as_u64()
                .ok_or_else(|| invalid("hit record"))? as usize;
            gets[index] += record["gets"]
                .as_u64()
                .ok_or_else(|| invalid("GET record"))? as usize;
            bytes[index] += record["s3_response_bytes"]
                .as_u64()
                .ok_or_else(|| invalid("byte record"))? as usize;
            times[index].push(
                record["total_ns"]
                    .as_u64()
                    .ok_or_else(|| invalid("time record"))? as u128,
            );
            s3_times[index].push(
                record["s3_ns"]
                    .as_u64()
                    .ok_or_else(|| invalid("S3 time record"))? as u128,
            );
            local_bytes[index] += record["local_read_bytes"]
                .as_u64()
                .ok_or_else(|| invalid("local byte record"))?;
            local_blocks[index] += record["verified_block_reads"]
                .as_u64()
                .ok_or_else(|| invalid("local block record"))?;
            records[index] = record;
        }
        let candidate_hits = records[0]["hits"]
            .as_u64()
            .ok_or_else(|| invalid("candidate hits"))?;
        let baseline_hits = records[1]["hits"]
            .as_u64()
            .ok_or_else(|| invalid("baseline hits"))?;
        paired[if candidate_hits > baseline_hits {
            0
        } else if candidate_hits == baseline_hits {
            1
        } else {
            2
        }] += 1;
        writeln!(
            output,
            "{}",
            json!({"query_ordinal":ordinal,
            "source_query_ordinal":query.source_query_ordinal,
            "candidate":records[0],"baseline":records[1]})
        )?;
    }
    output.flush()?;
    let candidate_p95 = percentile_ms(&times[0], 95);
    let control_p95 = percentile_ms(&times[1], 95);
    let summary = json!({
        "schema":"borsuk-v132-live-s3-transport-v1",
        "generation_manifest_sha256":AUTHORITY_SHA,
        "query_count":queries.len(),"startup_auth_ns":startup_ns,
        "paired_candidate_wins":paired[0],"paired_ties":paired[1],"paired_baseline_wins":paired[2],
        "qualifies_transport":totals == [99_942,98_827] && candidate_p95 <= 2.0 * control_p95,
        "candidate": {"hits":totals[0],"gets":gets[0],"response_bytes":bytes[0],
            "local_read_bytes":local_bytes[0],"verified_block_reads":local_blocks[0],
            "p50_ms":percentile_ms(&times[0],50),"p95_ms":candidate_p95,
            "p99_ms":percentile_ms(&times[0],99),
            "s3_p50_ms":percentile_ms(&s3_times[0],50),
            "s3_p95_ms":percentile_ms(&s3_times[0],95),
            "s3_p99_ms":percentile_ms(&s3_times[0],99)},
        "baseline": {"hits":totals[1],"gets":gets[1],"response_bytes":bytes[1],
            "local_read_bytes":local_bytes[1],"verified_block_reads":local_blocks[1],
            "p50_ms":percentile_ms(&times[1],50),"p95_ms":control_p95,
            "p99_ms":percentile_ms(&times[1],99),
            "s3_p50_ms":percentile_ms(&s3_times[1],50),
            "s3_p95_ms":percentile_ms(&s3_times[1],95),
            "s3_p99_ms":percentile_ms(&s3_times[1],99)},
    });
    fs::write(&args[3], serde_json::to_vec(&summary)?)?;
    Ok(())
}

fn main() -> Result<(), Box<dyn Error>> {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(run())
}
