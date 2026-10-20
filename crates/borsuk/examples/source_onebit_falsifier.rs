//! Offline fixed-nomination diagnostic. Inputs must remain immutable for the run.
#![recursion_limit = "256"]
#[path = "source_onebit_falsifier/onebit.rs"]
mod onebit;
use borsuk::{rotated_two_bit::RotatedTwoBitCodec, two_bit_source::TwoBitPlane};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    error::Error,
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::Path,
    time::Instant,
};
type Result<T> = std::result::Result<T, Box<dyn Error>>;
const AUTHORITY_CAP: usize = 8 * 1024 * 1024;
const CONTROL: usize = 96 * 1024 * 1024;
fn require(ok: bool, why: &str) -> Result<()> {
    if ok { Ok(()) } else { Err(why.into()) }
}
fn num(v: &Value) -> Result<usize> {
    Ok(usize::try_from(v.as_u64().ok_or("integer")?)?)
}
fn string(v: &Value) -> Result<&str> {
    v.as_str().ok_or_else(|| "string".into())
}
fn sha(b: &[u8]) -> String {
    format!("{:x}", Sha256::digest(b))
}
fn vec_exact<T: Default + Clone>(n: usize) -> Result<Vec<T>> {
    let mut v = Vec::new();
    v.try_reserve_exact(n)?;
    v.resize(n, T::default());
    Ok(v)
}
fn regular(path: &Path) -> Result<File> {
    require(
        path.is_absolute() && std::fs::symlink_metadata(path)?.is_file(),
        "absolute regular input",
    )?;
    let f = File::open(path)?;
    require(f.metadata()?.is_file(), "regular input")?;
    Ok(f)
}
fn read_bound(path: &Path, n: usize, hash: &str, exact: bool) -> Result<Vec<u8>> {
    require(
        hash.len() == 64 && hash.bytes().all(|x| x.is_ascii_hexdigit()),
        "SHA256 syntax",
    )?;
    let mut f = regular(path)?;
    let size = usize::try_from(f.metadata()?.len())?;
    require(size <= n && (!exact || size == n), "input size")?;
    let mut b = vec_exact(size)?;
    f.read_exact(&mut b)?;
    require(
        f.read(&mut [0])? == 0 && sha(&b) == hash,
        "input SHA/length",
    )?;
    Ok(b)
}
fn model(
    rows: usize,
    s: &onebit::Shape,
    requests: usize,
    truth: usize,
    authority: usize,
) -> Result<usize> {
    let row_width = s
        .width
        .checked_add(16 + 8 + 32 + 64)
        .ok_or("memory overflow")?;
    rows.checked_mul(row_width)
        .and_then(|n| n.checked_add(requests.checked_mul(2)?))
        .and_then(|n| n.checked_add(truth.checked_mul(2)?))
        .and_then(|n| n.checked_add(authority.checked_mul(96)?))
        .and_then(|n| n.checked_add(s.m.checked_mul(64)?))
        .and_then(|n| n.checked_add(CONTROL))
        .ok_or_else(|| "memory overflow".into())
}
fn centered_memory(
    rows: usize,
    s: &onebit::Shape,
    queries: usize,
    shortlist: usize,
) -> Result<usize> {
    rows.checked_mul(
        s.width
            .checked_add(16 + std::mem::size_of::<Hit>())
            .ok_or("center memory overflow")?,
    )
    .and_then(|n| n.checked_add(s.d.checked_mul(16)?))
    .and_then(|n| n.checked_add(s.m.checked_mul(8)?))
    .and_then(|n| {
        n.checked_add(
            rows.min(shortlist)
                .checked_mul(queries)?
                .checked_mul(4096)?,
        )
    })
    .ok_or_else(|| "center memory overflow".into())
}
fn report_memory(rows: usize, queries: usize, shortlist: usize) -> Result<usize> {
    rows.min(256)
        .checked_mul(2)
        .and_then(|n| n.checked_add(rows.min(shortlist)))
        .and_then(|n| n.checked_mul(queries))
        .and_then(|n| n.checked_mul(4096))
        .ok_or_else(|| "report memory overflow".into())
}
#[derive(Clone, Copy, Default)]
struct Hit {
    row: usize,
    id: i64,
    score: f64,
}
fn top(h: &mut Vec<Hit>, limit: usize) {
    h.sort_unstable_by(|a, b| b.score.total_cmp(&a.score).then(a.row.cmp(&b.row)));
    h.truncate(limit);
}
fn selected(
    s: &onebit::Shape,
    request: &[u8],
    truth: &[u8],
    raw: &mut [f32],
    transformed: &mut [f64],
) -> Result<Vec<i64>> {
    for (x, b) in raw.iter_mut().zip(request.chunks_exact(4)) {
        *x = f32::from_le_bytes(b.try_into()?);
    }
    s.transform(raw, transformed)?;
    let ids = truth
        .chunks_exact(8)
        .map(|b| i64::try_from(u64::from_le_bytes(b.try_into().unwrap())).map_err(Into::into))
        .collect::<Result<Vec<_>>>()?;
    require(
        ids.iter().enumerate().all(|(i, id)| !ids[..i].contains(id)),
        "duplicate truth IDs",
    )?;
    Ok(ids)
}
fn hits(h: &[Hit]) -> Value {
    json!(
        h.iter()
            .map(|h| json!({"row":h.row,"id":h.id,"score":h.score,"score_bits":h.score.to_bits()}))
            .collect::<Vec<_>>()
    )
}
fn truth_retained(truth: &[i64], h: &[Hit]) -> Vec<i64> {
    truth
        .iter()
        .copied()
        .filter(|id| h.iter().any(|h| h.id == *id))
        .collect()
}
fn parity(expected: &Value, actual: &[Hit]) -> Result<bool> {
    if expected.is_null() {
        return Ok(false);
    }
    let expected = expected.as_array().ok_or("baseline_top256 array")?;
    require(expected.len() == actual.len(), "INVALID baseline length")?;
    for (e, h) in expected.iter().zip(actual) {
        require(
            num(&e["row"])? == h.row && e["score_bits"].as_u64() == Some(h.score.to_bits()),
            "INVALID baseline score/order mismatch",
        )?;
    }
    Ok(true)
}
fn metric() -> Value {
    #[cfg(target_os = "linux")]
    {
        let mut r = std::mem::MaybeUninit::<libc::rusage>::uninit();
        // getrusage initializes the complete output on success.
        if unsafe { libc::getrusage(libc::RUSAGE_SELF, r.as_mut_ptr()) } == 0 {
            let r = unsafe { r.assume_init() };
            return json!({"user_cpu_us":r.ru_utime.tv_sec*1_000_000+r.ru_utime.tv_usec,
                "system_cpu_us":r.ru_stime.tv_sec*1_000_000+r.ru_stime.tv_usec,"peak_rss_kib":r.ru_maxrss});
        }
    }
    json!({"unavailable":true})
}
fn save(path: &Path, value: &Value) -> Result<()> {
    let mut f = OpenOptions::new().write(true).create_new(true).open(path)?;
    serde_json::to_writer_pretty(&mut f, value)?;
    f.write_all(b"\n")?;
    f.sync_all()?;
    Ok(())
}
fn run(args: &[String]) -> Result<()> {
    if args.len() == 2 && args[1] == "--self-check" {
        onebit::self_check()?;
        require(
            model(usize::MAX, &onebit::Shape::new(3)?, 0, 0, 0).is_err(),
            "memory overflow guard",
        )?;
        println!("synthetic self-check passed; no payload opened");
        return Ok(());
    }
    let admit = args.get(1).is_some_and(|s| s == "--admit");
    let start = if admit { 2 } else { 1 };
    require(
        args.len() == start + 4,
        "usage: source_onebit_falsifier [--admit] AUTHORITY AUTHORITY_SHA NEW_REPORT MEMORY_BYTES | --self-check",
    )?;
    let cap = args[start + 3].parse::<usize>()?;
    require(cap >= CONTROL, "memory cap below control allowance")?;
    let started = Instant::now();
    let cpu_start = metric();
    let ab = read_bound(
        Path::new(&args[start]),
        AUTHORITY_CAP,
        &args[start + 1],
        false,
    )?;
    let a: Value = serde_json::from_slice(&ab)?;
    let centered = a["schema"] == "borsuk-source-onebit-falsifier-authority-v3";
    require(
        centered || a["schema"] == "borsuk-source-onebit-falsifier-authority-v2",
        "authority v2 or v3 required; old markers unsupported",
    )?;
    require(
        if centered {
            a["centroid_mode"] == "fixed" && a["center"].is_object()
        } else {
            a["center"].is_null()
                && (a["centroid_mode"].is_null() || a["centroid_mode"] == "origin")
        },
        "authority centroid mode",
    )?;
    let shortlist = a["candidate_shortlist_rows"]
        .as_u64()
        .and_then(|n| usize::try_from(n).ok())
        .filter(|n| (1..=4096).contains(n))
        .ok_or("candidate_shortlist_rows integer in 1..4096 required")?;
    require(
        a["original_result"]["native_exit"] == 0 && a["original_result"]["reader_exit"] == 0,
        "root authority",
    )?;
    let rows = num(&a["rows"])?;
    let d = num(&a["dimensions"])?;
    require(rows > 0, "empty population")?;
    let s = onebit::Shape::new(d)?;
    let queries = a["queries"].as_array().ok_or("queries")?;
    require(
        !queries.is_empty() && queries.len() <= 1000,
        "query roster bound",
    )?;
    let req_bytes = num(&a["requests"]["bytes"])?;
    let truth_bytes = num(&a["truth"]["bytes"])?;
    let k = num(&a["truth"]["k"])?;
    let qw = d.checked_mul(4).ok_or("query geometry overflow")?;
    let tw = k.checked_mul(8).ok_or("truth geometry overflow")?;
    require(
        qw > 0 && tw > 0 && k <= 256 && req_bytes % qw == 0 && truth_bytes % tw == 0,
        "request/truth geometry",
    )?;
    let scratch = RotatedTwoBitCodec::required_query_scratch_bytes(d)?;
    let report_allowance = report_memory(rows, queries.len(), shortlist)?;
    let center_allowance = if centered {
        centered_memory(rows, &s, queries.len(), shortlist)?
    } else {
        0
    };
    let other = model(rows, &s, req_bytes, truth_bytes, ab.len())?
        .checked_add(center_allowance)
        .ok_or("center memory overflow")?
        .checked_add(report_allowance)
        .ok_or("report memory overflow")?
        .checked_add(scratch.checked_mul(3).ok_or("scratch overflow")?)
        .ok_or("memory overflow")?;
    let plane_cap = cap
        .checked_sub(other)
        .ok_or("memory admission before payload")?;
    let row_width = qw.checked_add(8).ok_or("record width overflow")?;
    let canonical_bytes = rows.checked_mul(row_width).ok_or("canonical overflow")?;
    let sidecar_bytes = rows.checked_mul(32).ok_or("sidecar overflow")?;
    let mut map = vec_exact::<usize>(rows)?;
    map.fill(usize::MAX);
    let mut last_ordinal = None;
    for q in queries {
        let ordinal = num(&q["ordinal"])?;
        require(
            last_ordinal.is_none_or(|n| ordinal > n)
                && ordinal < req_bytes / qw
                && ordinal < truth_bytes / tw,
            "ordinal order/range",
        )?;
        last_ordinal = Some(ordinal);
        let units = q["nominated_units"].as_array().ok_or("nominated_units")?;
        require(!units.is_empty(), "empty nominations")?;
        let mut seen = std::collections::HashSet::new();
        for u in units {
            let unit = num(u)?;
            require(
                seen.insert(unit) && unit < rows.div_ceil(32),
                "unit order/range",
            )?;

            let lo = unit.checked_mul(32).ok_or("unit overflow")?;
            for v in &mut map[lo..rows.min(lo + 32)] {
                *v = 0;
            }
        }
    }
    let mut count = 0;
    for v in &mut map {
        if *v != usize::MAX {
            *v = count;
            count += 1;
        }
    }
    let codes_bytes = count.checked_mul(s.width).ok_or("packed code overflow")?;
    let mut codes = vec_exact::<u8>(codes_bytes)?;
    let mut factors = vec_exact::<f64>(count)?;
    let mut ids = vec_exact::<i64>(count)?;
    let mut transformed = vec_exact::<f64>(s.m)?;
    let mut raw = vec_exact::<f32>(d)?;
    let mut record = vec_exact::<u8>(row_width)?;
    // Center authentication and all ranking capacities precede payload reads.
    let center = if centered {
        let bytes = d.checked_mul(8).ok_or("center size overflow")?;
        require(num(&a["center"]["bytes"])? == bytes, "center byte geometry")?;
        let bytes = read_bound(
            Path::new(string(&a["center"]["path"])?),
            bytes,
            string(&a["center"]["sha256"])?,
            true,
        )?;
        let mut c = vec_exact::<f64>(d)?;
        for (x, b) in c.iter_mut().zip(bytes.chunks_exact(8)) {
            *x = f64::from_le_bytes(b.try_into()?);
        }
        let mut tc = vec_exact::<f64>(s.m)?;
        s.linear(&c, &mut tc)?;
        Some((c, tc))
    } else {
        None
    };
    let mut centered_codes = vec_exact::<u8>(if centered { codes_bytes } else { 0 })?;
    let mut centered_factors = vec_exact::<f64>(if centered { count } else { 0 })?;
    let mut centered_additives = vec_exact::<f64>(if centered { count } else { 0 })?;
    let mut baseline = Vec::<Hit>::new();
    baseline.try_reserve_exact(rows)?;
    let mut candidate = Vec::<Hit>::new();
    candidate.try_reserve_exact(rows)?;
    let mut centered_hits = Vec::<Hit>::new();
    if centered {
        centered_hits.try_reserve_exact(rows)?;
    }
    let mut results = Vec::new();
    results.try_reserve_exact(queries.len())?;
    let dir = Path::new(string(&a["generation_dir"])?);
    let root = read_bound(
        &dir.join("manifest.json"),
        128 * 1024,
        string(&a["generation_root_sha256"])?,
        false,
    )?;
    let root: Value = serde_json::from_slice(&root)?;
    require(
        root["plane_manifest_sha256"] == a["plane_manifest_sha256"]
            && root["sq8_object_sha256"] == a["sq8_sha256"],
        "root plane binding",
    )?;
    let canonical = &root["canonical"];
    require(
        num(&canonical["rows"])? == rows
            && num(&canonical["dimensions"])? == d
            && num(&canonical["bytes"])? == canonical_bytes
            && num(&canonical["row_digests_bytes"])? == sidecar_bytes,
        "root canonical geometry",
    )?;
    let pb = read_bound(
        &dir.join("plane/manifest.json"),
        65536,
        string(&a["plane_manifest_sha256"])?,
        false,
    )?;
    let pm: Value = serde_json::from_slice(&pb)?;
    let record_bytes = num(&pm["record_bytes"])?;
    let baseline_padded = record_bytes
        .checked_sub(8)
        .and_then(|n| n.checked_mul(4))
        .ok_or("plane width overflow")?;
    require(
        num(&pm["rows"])? == rows && num(&pm["dimensions"])? == d,
        "plane metadata geometry",
    )?;
    let plane_required = rows
        .checked_mul(record_bytes)
        .and_then(|n| n.checked_add(baseline_padded.checked_mul(128)?))
        .and_then(|n| n.checked_add(131072))
        .and_then(|n| n.checked_add(rows.div_ceil(32).checked_mul(64)?))
        .ok_or("plane admission overflow")?;
    require(
        plane_required <= plane_cap,
        "full working model admission before payload",
    )?;
    eprintln!(
        "admitted explicit non-plane bytes={other}; plane bytes={plane_required}; cap={cap}; union_rows={count}; padded_dim={}",
        s.m
    );
    let requests = read_bound(
        Path::new(string(&a["requests"]["path"])?),
        req_bytes,
        string(&a["requests"]["sha256"])?,
        true,
    )?;
    let truth = read_bound(
        Path::new(string(&a["truth"]["path"])?),
        truth_bytes,
        string(&a["truth"]["sha256"])?,
        true,
    )?;
    // Both modes validate selected requests/truth before canonical or plane payload.
    for q in queries {
        let ordinal = num(&q["ordinal"])?;
        selected(
            &s,
            &requests[ordinal * qw..(ordinal + 1) * qw],
            &truth[ordinal * tw..(ordinal + 1) * tw],
            &mut raw,
            &mut transformed,
        )?;
        if centered {
            onebit::vector_norm(&raw)?;
        }
    }
    let sidecar = read_bound(
        &dir.join("canonical_row_digests.bin"),
        sidecar_bytes,
        string(&canonical["row_digests_sha256"])?,
        true,
    )?;
    let plane = TwoBitPlane::open(
        &dir.join("plane"),
        string(&a["plane_manifest_sha256"])?,
        string(&a["sq8_sha256"])?,
        plane_cap,
    )?;
    require(
        plane.receipt().rows == rows && plane.receipt().dimensions == d,
        "plane geometry",
    )?;
    let mut canonical_file = regular(&dir.join("canonical.bin"))?;
    require(
        canonical_file.metadata()?.len() == canonical_bytes as u64,
        "canonical length",
    )?;
    let encoding_start = Instant::now();
    for (physical, &slot) in map.iter().enumerate() {
        if slot == usize::MAX {
            continue;
        }
        canonical_file.seek(SeekFrom::Start(
            physical
                .checked_mul(row_width)
                .ok_or("row offset overflow")? as u64,
        ))?;
        canonical_file.read_exact(&mut record)?;
        require(
            Sha256::digest(&record)[..] == sidecar[physical * 32..physical * 32 + 32],
            "canonical row authentication",
        )?;
        ids[slot] = i64::from_le_bytes(record[..8].try_into()?);
        for (x, b) in raw.iter_mut().zip(record[8..].chunks_exact(4)) {
            *x = f32::from_le_bytes(b.try_into()?);
        }
        s.transform(&raw, &mut transformed)?;
        factors[slot] = s.encode(
            &transformed,
            &mut codes[slot * s.width..(slot + 1) * s.width],
        )?;
        if let Some((c, tc)) = &center {
            let (alpha, additive) = s.encode_centered(
                &raw,
                c,
                tc,
                &mut transformed,
                &mut centered_codes[slot * s.width..(slot + 1) * s.width],
            )?;
            centered_factors[slot] = alpha;
            centered_additives[slot] = additive;
        }
    }
    let encoding_seconds = encoding_start.elapsed().as_secs_f64();
    let mut baseline_seconds = 0.;
    let mut onebit_seconds = 0.;
    let mut centered_seconds = 0.;
    if !admit {
        for q in queries {
            let ordinal = num(&q["ordinal"])?;
            for (x, b) in raw
                .iter_mut()
                .zip(requests[ordinal * qw..(ordinal + 1) * qw].chunks_exact(4))
            {
                *x = f32::from_le_bytes(b.try_into()?);
            }
            s.transform(&raw, &mut transformed)?;
            let prep = plane.prepare_query(&raw, scratch)?;
            let units = q["nominated_units"].as_array().ok_or("units")?;
            baseline.clear();
            candidate.clear();
            centered_hits.clear();
            let qc = if let Some((c, _)) = &center {
                let norm = onebit::vector_norm(&raw)?;
                raw.iter()
                    .zip(c)
                    .map(|(&x, c)| x as f64 / norm * c)
                    .sum::<f64>()
            } else {
                0.
            };
            let scoring = Instant::now();
            for u in units {
                let lo = num(u)?.checked_mul(32).ok_or("unit overflow")?;
                for row in lo..rows.min(lo + 32) {
                    let slot = map[row];
                    baseline.push(Hit {
                        row,
                        id: ids[slot],
                        score: prep.score(plane.record(row).ok_or("plane row")?)?,
                    });
                }
            }
            let nominees = baseline.len();
            baseline_seconds += scoring.elapsed().as_secs_f64();
            let truth_ids = selected(
                &s,
                &requests[ordinal * qw..(ordinal + 1) * qw],
                &truth[ordinal * tw..(ordinal + 1) * tw],
                &mut raw,
                &mut transformed,
            )?;
            let truth_nominated = truth_retained(&truth_ids, &baseline);
            let sorting = Instant::now();
            top(&mut baseline, 256);
            baseline_seconds += sorting.elapsed().as_secs_f64();
            let reproduced = parity(&q["baseline_top256"], &baseline)?;
            let scoring = Instant::now();
            for u in units {
                let lo = num(u)?.checked_mul(32).ok_or("unit overflow")?;
                for row in lo..rows.min(lo + 32) {
                    let slot = map[row];
                    candidate.push(Hit {
                        row,
                        id: ids[slot],
                        score: s.score(
                            &codes[slot * s.width..(slot + 1) * s.width],
                            factors[slot],
                            &transformed,
                        )?,
                    });
                }
            }
            top(&mut candidate, shortlist.max(256));
            onebit_seconds += scoring.elapsed().as_secs_f64();
            if centered {
                let scoring = Instant::now();
                for u in units {
                    let lo = num(u)?.checked_mul(32).ok_or("unit overflow")?;
                    for row in lo..rows.min(lo + 32) {
                        let slot = map[row];
                        centered_hits.push(Hit {
                            row,
                            id: ids[slot],
                            score: s.score_centered(
                                &centered_codes[slot * s.width..(slot + 1) * s.width],
                                centered_factors[slot],
                                centered_additives[slot],
                                &transformed,
                                qc,
                            )?,
                        });
                    }
                }
                top(&mut centered_hits, shortlist);
                centered_seconds += scoring.elapsed().as_secs_f64();
            }
            let bt = truth_retained(&truth_ids, &baseline);
            let control = &candidate[..candidate.len().min(256)];
            let candidate_topk = &candidate[..candidate.len().min(shortlist)];
            let ot = truth_retained(&truth_ids, control);
            let ct = truth_retained(&truth_ids, candidate_topk);
            let candidate_lost = bt
                .iter()
                .filter(|id| !ct.contains(id))
                .copied()
                .collect::<Vec<_>>();
            let candidate_gained = ct
                .iter()
                .filter(|id| !bt.contains(id))
                .copied()
                .collect::<Vec<_>>();
            let retained = bt
                .iter()
                .filter(|id| ot.contains(id))
                .copied()
                .collect::<Vec<_>>();
            let lost = bt
                .iter()
                .filter(|id| !ot.contains(id))
                .copied()
                .collect::<Vec<_>>();
            let gained = ot
                .iter()
                .filter(|id| !bt.contains(id))
                .copied()
                .collect::<Vec<_>>();
            let c_retained = truth_retained(&truth_ids, &centered_hits);
            let c_lost = bt
                .iter()
                .filter(|id| !c_retained.contains(id))
                .copied()
                .collect::<Vec<_>>();
            let c_gained = c_retained
                .iter()
                .filter(|id| !bt.contains(id))
                .copied()
                .collect::<Vec<_>>();
            results.push(json!({"centered_topk":if centered { hits(&centered_hits) } else { Value::Null },
                "centered_truth_retained":if centered { json!(c_retained) } else { Value::Null },
                "centered_truth_lost":if centered { json!(c_lost) } else { Value::Null },
                "centered_truth_gained":if centered { json!(c_gained) } else { Value::Null },"ordinal":ordinal,"nominated_units":q["nominated_units"],"nominated_rows":nominees,
                "baseline_top256":hits(&baseline),"onebit_top256":hits(control),"candidate_topk":hits(candidate_topk),
                "candidate_truth_retained":ct,"candidate_truth_lost":candidate_lost,"candidate_truth_gained":candidate_gained,
                "baseline_reproduction_verified":reproduced,"baseline_reproduction_authority":if reproduced {"verified"} else {"unavailable"},
                "truth_ids":truth_ids,"truth_nominated":truth_nominated,"baseline_truth_retained":bt,"onebit_truth_retained":ot,
                "truth_retained":retained,"truth_lost":lost,"truth_gained":gained,
                "score_vector_capacity_bytes":(baseline.capacity()+candidate.capacity()+centered_hits.capacity())*std::mem::size_of::<Hit>()}));
        }
    }
    save(
        Path::new(&args[start + 2]),
        &json!({"schema":if centered { "borsuk-source-onebit-falsifier-v3" } else { "borsuk-source-onebit-falsifier-v2" },"status":"VALID",
        "centroid_mode":if centered { "fixed" } else { "origin" }, "origin_control":true,
        "center":a["center"], "centered_estimator":"q.c + r.c + (norm(r)^2/L1(Tr))*sign(Tr).T(q-c); r=normalize(x)-c",
        "centered_code_bytes":centered_codes.len(),"centered_factor_bytes":centered_factors.len()*8,
        "centered_additive_bytes":centered_additives.len()*8,"centered_score_sort_seconds":centered_seconds,
        "mode":if admit {"admission"} else {"fixed_nomination_counterfactual"},
        "fixed_nomination_counterfactual":true,"planner_replayed":false,"physical_io_gain_claim":false,
        "authority_sha256":args[start+1],"generation_root_sha256":a["generation_root_sha256"],
        "transform":"normalized signed Hadamard, zero padded to next power of two; NON-HAAR HEURISTIC; no RaBitQ theory claim",
        "transform_seed":onebit::SEED,"origin_centered":true,"dimensions":d,"padded_dimensions":s.m,
        "query_quantized":false,"estimator":"dot(sign(T normalize(x)), T normalize(q)) * norm(T normalize(x))/L1(T normalize(x))",
        "tie_order":"descending score, ascending physical row","union_rows":count,
        "canonical_full_hash_scanned":false,"canonical_nominated_rows_verified":count,"canonical":canonical,
        "candidate_shortlist_rows":shortlist,
        "onebit_score_sort_scope":"shared onebit_top256 and candidate_topk; population scored and sorted once",
        "onebit_code_bytes":codes.len(),"onebit_factor_bytes":factors.len()*8,
        "onebit_code_factor_bytes":codes.len()+factors.len()*8,
        "baseline_union_code_factor_bytes":count*plane.receipt().record_bytes,
        "memory":{"cap":cap,"center_allowance_bytes":center_allowance,"report_allowance_bytes":report_allowance,"modeled_nonplane_bytes":other,"modeled_plane_bytes":plane_required,"modeled_total_bytes":other+plane_required,"plane_allowance":plane_cap,
            "two_bit_query_scratch_bytes":scratch,"onebit_transform_capacity_bytes":transformed.capacity()*8,
            "code_capacity":codes.capacity(),"factor_capacity_bytes":factors.capacity()*8,
            "id_capacity_bytes":ids.capacity()*8,"map_capacity_bytes":map.capacity()*std::mem::size_of::<usize>(),
            "sidecar_capacity":sidecar.capacity(),"requests_capacity":requests.capacity(),"truth_capacity":truth.capacity(),
            "excludes":"allocator/runtime and filesystem cache; external cgroup bounds RSS"},
        "encoding_seconds":encoding_seconds,"baseline_score_sort_seconds":baseline_seconds,"onebit_score_sort_seconds":onebit_seconds,
        "wall_seconds":started.elapsed().as_secs_f64(),"process_start":cpu_start,"process_end":metric(),"queries":results}),
    )?;
    Ok(())
}
fn main() {
    if let Err(e) = run(&std::env::args().collect::<Vec<_>>()) {
        eprintln!("INVALID: {e}");
        std::process::exit(2);
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn memory_admission_and_overflow() {
        let s = onebit::Shape::new(9).unwrap();
        assert!(model(usize::MAX, &s, 0, 0, 0).is_err());
        assert!(model(100, &s, 200, 80, 500).unwrap() > CONTROL);
    }
    #[test]
    fn ties_and_authority_mismatch() {
        let mut h = vec![
            Hit {
                row: 2,
                id: 2,
                score: 1.,
            },
            Hit {
                row: 1,
                id: 1,
                score: 1.,
            },
        ];
        top(&mut h, 256);
        assert_eq!(h[0].row, 1);
        let mut zeros = vec![
            Hit {
                row: 2,
                id: 2,
                score: 0.,
            },
            Hit {
                row: 1,
                id: 1,
                score: -0.,
            },
        ];
        top(&mut zeros, 256);
        assert_eq!(zeros[0].row, 2);
        let mut cutoff = (0..255)
            .map(|row| Hit {
                row,
                id: row as i64,
                score: 1.,
            })
            .collect::<Vec<_>>();
        cutoff.push(Hit {
            row: 255,
            id: 255,
            score: -0.,
        });
        cutoff.push(Hit {
            row: 256,
            id: 256,
            score: 0.,
        });
        top(&mut cutoff, 256);
        assert!(cutoff.iter().any(|h| h.row == 256));
        assert!(!cutoff.iter().any(|h| h.row == 255));
        assert!(parity(&json!([{"row":2,"score_bits":1}]), &h).is_err());
        assert!(!parity(&Value::Null, &h).unwrap());
        assert!(parity(&json!([{"row":1,"score_bits":1f64.to_bits()},{"row":2,"score_bits":1f64.to_bits()}]),&h).unwrap());
    }
    #[test]
    fn authenticated_file_and_output_guards() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("fixture");
        std::fs::write(&p, b"abc").unwrap();
        assert_eq!(read_bound(&p, 3, &sha(b"abc"), true).unwrap(), b"abc");
        assert!(read_bound(&p, 2, &sha(b"abc"), false).is_err());
        assert!(read_bound(&p, 3, &sha(b"bad"), true).is_err());
        assert!(read_bound(Path::new("relative"), 3, &sha(b"abc"), true).is_err());
        assert!(save(&p, &json!({})).is_err());
        assert_eq!(std::fs::read(&p).unwrap(), b"abc");
    }
    #[test]
    fn low_memory_rejects_before_payload_paths_are_needed() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("authority");
        let a = json!({"schema":"borsuk-source-onebit-falsifier-authority-v2","candidate_shortlist_rows":256,
            "original_result":{"native_exit":0,"reader_exit":0},"rows":1000000,"dimensions":3,
            "requests":{"bytes":12},"truth":{"bytes":8,"k":1},
            "queries":[{"ordinal":0,"nominated_units":[0]}]});
        let b = serde_json::to_vec(&a).unwrap();
        std::fs::write(&p, &b).unwrap();
        let e = run(&[
            "h".into(),
            p.to_str().unwrap().into(),
            sha(&b),
            dir.path().join("out").to_str().unwrap().into(),
            CONTROL.to_string(),
        ])
        .unwrap_err();
        assert!(e.to_string().contains("memory admission before payload"));
        assert!(!dir.path().join("out").exists());
    }
    #[test]
    fn synthetic_smoke_no_payload() {
        run(&["h".into(), "--self-check".into()]).unwrap();
        assert!(run(&["h".into()]).is_err());
    }

    struct Fixture {
        dir: tempfile::TempDir,
        authority: Value,
    }
    impl Fixture {
        fn new() -> Self {
            Self::with_rows(33)
        }
        fn with_rows(rows: usize) -> Self {
            use borsuk::two_bit_source::TwoBitSource;
            let dir = tempfile::tempdir().unwrap();
            let root = dir.path();
            let raw = root.join("raw");
            let sq8 = root.join("sq8");
            let mut raw_bytes = Vec::new();
            let mut sq8_bytes = Vec::new();
            let mut canonical = Vec::new();
            let mut digests = Vec::new();
            for id in 0..rows as i64 {
                let vector = [1f32, 0., 0.].map(f32::to_le_bytes).concat();
                raw_bytes.extend_from_slice(&vector);
                sq8_bytes.extend_from_slice(&id.to_le_bytes());
                sq8_bytes.extend_from_slice(&1f32.to_le_bytes());
                sq8_bytes.extend_from_slice(&[1, 0, 0]);
                let record = [id.to_le_bytes().as_slice(), &vector].concat();
                digests.extend_from_slice(&Sha256::digest(&record));
                canonical.extend_from_slice(&record);
            }
            std::fs::write(&raw, &raw_bytes).unwrap();
            std::fs::write(&sq8, &sq8_bytes).unwrap();
            let sq8_sha = sha(&sq8_bytes);
            TwoBitSource {
                raw: &raw,
                raw_sha256: &sha(&raw_bytes),
                sq8: &sq8,
                sq8_sha256: &sq8_sha,
                rows,
                dimensions: 3,
            }
            .build(&root.join("plane"), 1024 * 1024)
            .unwrap();
            std::fs::write(root.join("canonical.bin"), &canonical).unwrap();
            std::fs::write(root.join("canonical_row_digests.bin"), &digests).unwrap();
            let plane_sha = sha(&std::fs::read(root.join("plane/manifest.json")).unwrap());
            let generation = json!({"plane_manifest_sha256":plane_sha,"sq8_object_sha256":sq8_sha,
                "canonical":{"rows":rows,"dimensions":3,"bytes":canonical.len(),
                "row_digests_bytes":digests.len(),"row_digests_sha256":sha(&digests)}});
            let generation = serde_json::to_vec(&generation).unwrap();
            std::fs::write(root.join("manifest.json"), &generation).unwrap();
            let requests = [1f32, 0., 0.].map(f32::to_le_bytes).concat();
            let truth = [0u64, 1].map(u64::to_le_bytes).concat();
            std::fs::write(root.join("requests"), &requests).unwrap();
            std::fs::write(root.join("truth"), &truth).unwrap();
            let authority = json!({"schema":"borsuk-source-onebit-falsifier-authority-v2","candidate_shortlist_rows":256,
                "original_result":{"native_exit":0,"reader_exit":0},"rows":rows,"dimensions":3,
                "generation_dir":root,"generation_root_sha256":sha(&generation),
                "plane_manifest_sha256":plane_sha,"sq8_sha256":sq8_sha,
                "requests":{"path":root.join("requests"),"bytes":requests.len(),"sha256":sha(&requests)},
                "truth":{"path":root.join("truth"),"bytes":truth.len(),"sha256":sha(&truth),"k":2},
                "queries":[{"ordinal":0,"nominated_units":(0..rows.div_ceil(32)).rev().collect::<Vec<_>>()}]});
            Self { dir, authority }
        }
        fn run(&self, admit: bool, size: usize, cap: usize) -> Result<()> {
            let mut bytes = serde_json::to_vec(&self.authority).unwrap();
            if size > 0 {
                assert!(size >= bytes.len());
                bytes.resize(size, b' ');
            }
            let path = self.dir.path().join("authority");
            std::fs::write(&path, &bytes).unwrap();
            let mut args = vec!["h".into()];
            if admit {
                args.push("--admit".into());
            }
            args.extend([
                path.to_str().unwrap().into(),
                sha(&bytes),
                self.dir.path().join("out").to_str().unwrap().into(),
                cap.to_string(),
            ]);
            run(&args)
        }
        fn remove_payload(&self) {
            for name in [
                "canonical.bin",
                "canonical_row_digests.bin",
                "plane/records.bin",
            ] {
                std::fs::remove_file(self.dir.path().join(name)).unwrap();
            }
        }
        fn no_report(&self) {
            assert!(!self.dir.path().join("out").exists());
        }
    }
    #[test]
    fn candidate_shortlist_small_population_and_score_bits() {
        let mut f = Fixture::new();
        f.authority["candidate_shortlist_rows"] = json!(512);
        f.run(false, 0, 512 * 1024 * 1024).unwrap();
        let report: Value =
            serde_json::from_slice(&std::fs::read(f.dir.path().join("out")).unwrap()).unwrap();
        let q = &report["queries"][0];
        assert_eq!(q["candidate_topk"], q["onebit_top256"]);
        assert_eq!(q["candidate_topk"].as_array().unwrap().len(), 33);
        std::fs::remove_file(f.dir.path().join("out")).unwrap();
        f.authority["candidate_shortlist_rows"] = json!(1);
        f.run(false, 0, 512 * 1024 * 1024).unwrap();
        let narrow: Value =
            serde_json::from_slice(&std::fs::read(f.dir.path().join("out")).unwrap()).unwrap();
        let narrow = &narrow["queries"][0];
        assert_eq!(narrow["onebit_top256"], q["onebit_top256"]);
        assert_eq!(narrow["baseline_top256"], q["baseline_top256"]);
        assert_eq!(narrow["candidate_topk"].as_array().unwrap().len(), 1);
        assert_eq!(narrow["candidate_truth_retained"], json!([0]));
        assert_eq!(narrow["candidate_truth_lost"], json!([1]));
        assert_eq!(narrow["candidate_truth_gained"], json!([]));
    }

    #[test]
    fn candidate_256_control_parity_and_512_extra_truth() {
        let mut f = Fixture::with_rows(513);
        let truth = [0u64, 300].map(u64::to_le_bytes).concat();
        std::fs::write(f.dir.path().join("truth"), &truth).unwrap();
        f.authority["truth"]["sha256"] = json!(sha(&truth));
        f.run(false, 0, 512 * 1024 * 1024).unwrap();
        let first: Value =
            serde_json::from_slice(&std::fs::read(f.dir.path().join("out")).unwrap()).unwrap();
        let q = &first["queries"][0];
        assert_eq!(first["schema"], "borsuk-source-onebit-falsifier-v2");
        assert_eq!(q["candidate_topk"], q["onebit_top256"]);
        assert_eq!(q["candidate_topk"].as_array().unwrap().len(), 256);
        assert_eq!(q["candidate_truth_retained"], json!([0]));
        std::fs::remove_file(f.dir.path().join("out")).unwrap();
        f.authority["candidate_shortlist_rows"] = json!(512);
        f.authority["queries"][0]["baseline_top256"] = q["baseline_top256"].clone();
        f.run(false, 0, 512 * 1024 * 1024).unwrap();
        let second: Value =
            serde_json::from_slice(&std::fs::read(f.dir.path().join("out")).unwrap()).unwrap();
        let next = &second["queries"][0];
        assert_eq!(next["baseline_top256"], q["baseline_top256"]);
        assert_eq!(next["onebit_top256"], q["onebit_top256"]);
        assert_eq!(
            &next["candidate_topk"].as_array().unwrap()[..256],
            q["onebit_top256"].as_array().unwrap()
        );
        assert_eq!(next["candidate_topk"].as_array().unwrap().len(), 512);
        assert_eq!(next["candidate_truth_retained"], json!([0, 300]));
        assert_eq!(next["candidate_truth_gained"], json!([300]));
        assert_eq!(next["candidate_truth_lost"], json!([]));
        assert_eq!(next["baseline_reproduction_verified"], true);
    }

    #[test]
    fn invalid_shortlist_and_old_schema_refused_before_payload() {
        for admit in [false, true] {
            for value in [
                Value::Null,
                json!(0),
                json!(4097),
                json!("512"),
                json!(512.0),
                json!(-1),
                json!(true),
            ] {
                let mut f = Fixture::new();
                f.remove_payload();
                std::fs::remove_file(f.dir.path().join("requests")).unwrap();
                std::fs::remove_file(f.dir.path().join("truth")).unwrap();
                if value.is_null() {
                    f.authority
                        .as_object_mut()
                        .unwrap()
                        .remove("candidate_shortlist_rows");
                } else {
                    f.authority["candidate_shortlist_rows"] = value;
                }
                assert_eq!(
                    f.run(admit, 0, usize::MAX).unwrap_err().to_string(),
                    "candidate_shortlist_rows integer in 1..4096 required"
                );
                f.no_report();
            }
            for marker in [
                "borsuk-progressive-bound-root-authority-v1",
                "borsuk-source-onebit-falsifier-authority-v1",
            ] {
                let mut f = Fixture::new();
                f.remove_payload();
                f.authority["schema"] = json!(marker);
                assert!(
                    f.run(admit, 0, usize::MAX)
                        .unwrap_err()
                        .to_string()
                        .contains("old markers unsupported")
                );
                f.no_report();
            }
        }
    }

    #[test]
    fn report_memory_checked_and_model_minus_one_has_no_payload_io() {
        assert_eq!(
            report_memory(513, 2, 512).unwrap(),
            2 * (2 * 256 + 512) * 4096
        );
        assert_eq!(report_memory(33, 1, 512).unwrap(), 3 * 33 * 4096);
        assert!(report_memory(513, usize::MAX, 512).is_err());
        let mut f = Fixture::new();
        f.authority["candidate_shortlist_rows"] = json!(4096);
        f.run(true, 0, 512 * 1024 * 1024).unwrap();
        let report: Value =
            serde_json::from_slice(&std::fs::read(f.dir.path().join("out")).unwrap()).unwrap();
        let cap = num(&report["memory"]["modeled_total_bytes"]).unwrap() - 1;
        std::fs::remove_file(f.dir.path().join("out")).unwrap();
        f.remove_payload();
        std::fs::remove_file(f.dir.path().join("requests")).unwrap();
        std::fs::remove_file(f.dir.path().join("truth")).unwrap();
        for admit in [false, true] {
            assert_eq!(
                f.run(admit, 0, cap).unwrap_err().to_string(),
                "full working model admission before payload"
            );
            f.no_report();
        }
    }

    #[test]
    fn wider_authority_cap_and_admission() {
        let f = Fixture::new();
        let size = 1_563_062;
        f.run(true, size, 512 * 1024 * 1024).unwrap();
        let report: Value =
            serde_json::from_slice(&std::fs::read(f.dir.path().join("out")).unwrap()).unwrap();
        let s = onebit::Shape::new(3).unwrap();
        let scratch = RotatedTwoBitCodec::required_query_scratch_bytes(3).unwrap();
        let expected = model(33, &s, 12, 16, size).unwrap() + 3 * 33 * 4096 + 3 * scratch;
        assert_eq!(report["memory"]["modeled_nonplane_bytes"], expected);
        assert_eq!(
            model(33, &s, 12, 16, 8 * 1024 * 1024).unwrap() - model(33, &s, 12, 16, 0).unwrap(),
            805_306_368
        );
        assert_eq!(
            f.run(true, size, 512 * 1024 * 1024)
                .unwrap_err()
                .to_string(),
            "File exists (os error 17)"
        );
        let f = Fixture::new();
        f.remove_payload();
        assert_eq!(
            f.run(true, size, CONTROL).unwrap_err().to_string(),
            "memory admission before payload"
        );
        f.no_report();
    }
    #[test]
    fn authority_above_finite_cap_rejected_before_read() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("authority");
        File::create(&path)
            .unwrap()
            .set_len(8 * 1024 * 1024 + 1)
            .unwrap();
        // A deliberately wrong hash distinguishes size rejection from reading/authentication.
        for admit in [false, true] {
            let mut args = vec!["h".into()];
            if admit {
                args.push("--admit".into());
            }
            args.extend([
                path.to_str().unwrap().into(),
                "0".repeat(64),
                dir.path().join("out").to_str().unwrap().into(),
                CONTROL.to_string(),
            ]);
            assert_eq!(run(&args).unwrap_err().to_string(), "input size");
            assert!(!dir.path().join("out").exists());
        }
    }
    #[test]
    fn nomination_roster_order_and_bounds() {
        let f = Fixture::new();
        f.run(false, 0, 512 * 1024 * 1024).unwrap();
        let report: Value =
            serde_json::from_slice(&std::fs::read(f.dir.path().join("out")).unwrap()).unwrap();
        assert_eq!(report["queries"][0]["nominated_units"], json!([1, 0]));
        assert_eq!(report["union_rows"], 33);
        for units in [json!([0, 0]), json!([2])] {
            for admit in [false, true] {
                let mut f = Fixture::new();
                f.remove_payload();
                f.authority["queries"][0]["nominated_units"] = units.clone();
                assert_eq!(
                    f.run(admit, 0, 512 * 1024 * 1024).unwrap_err().to_string(),
                    "unit order/range"
                );
                f.no_report();
            }
        }
    }
    #[test]
    fn truth_k257_rejected_before_payload() {
        for admit in [false, true] {
            let mut f = Fixture::new();
            f.remove_payload();
            std::fs::remove_file(f.dir.path().join("requests")).unwrap();
            std::fs::remove_file(f.dir.path().join("truth")).unwrap();
            f.authority["truth"]["k"] = json!(257);
            f.authority["truth"]["bytes"] = json!(257 * 8);
            assert_eq!(
                f.run(admit, 0, 512 * 1024 * 1024).unwrap_err().to_string(),
                "request/truth geometry"
            );
            f.no_report();
        }
    }
    #[test]
    fn both_modes_authenticate_then_reject_selected_invalid_inputs_before_payload() {
        for admit in [false, true] {
            for (name, bytes, error) in [
                (
                    "requests",
                    [f32::NAN, 0., 0.].map(f32::to_le_bytes).concat(),
                    "vector shape/nonfinite",
                ),
                (
                    "requests",
                    [0f32; 3].map(f32::to_le_bytes).concat(),
                    "zero/nonfinite norm",
                ),
                (
                    "truth",
                    [1u64, 1].map(u64::to_le_bytes).concat(),
                    "duplicate truth IDs",
                ),
                (
                    "truth",
                    [0u64, 1 << 63].map(u64::to_le_bytes).concat(),
                    "out of range integral type conversion attempted",
                ),
            ] {
                let mut f = Fixture::new();
                f.remove_payload();
                std::fs::write(f.dir.path().join(name), &bytes).unwrap();
                assert_eq!(
                    f.run(admit, 0, 512 * 1024 * 1024).unwrap_err().to_string(),
                    "input SHA/length"
                );
                f.no_report();
                f.authority[name]["sha256"] = json!(sha(&bytes));
                assert_eq!(
                    f.run(admit, 0, 512 * 1024 * 1024).unwrap_err().to_string(),
                    error
                );
                f.no_report();
            }
        }
    }
    #[test]
    fn centered_fixed_file_retains_origin_control() {
        let mut f = Fixture::new();
        f.run(false, 0, 512 * 1024 * 1024).unwrap();
        let old: Value =
            serde_json::from_slice(&std::fs::read(f.dir.path().join("out")).unwrap()).unwrap();
        std::fs::remove_file(f.dir.path().join("out")).unwrap();
        let center = [0.25f64, 0.125, 0.];
        let bytes = center.map(f64::to_le_bytes).concat();
        let path = f.dir.path().join("center");
        std::fs::write(&path, &bytes).unwrap();
        f.authority["schema"] = json!("borsuk-source-onebit-falsifier-authority-v3");
        f.authority["centroid_mode"] = json!("fixed");
        f.authority["center"] = json!({"path":path,"bytes":bytes.len(),"sha256":sha(&bytes)});
        f.run(false, 0, 512 * 1024 * 1024).unwrap();
        let new: Value =
            serde_json::from_slice(&std::fs::read(f.dir.path().join("out")).unwrap()).unwrap();
        assert_eq!(new["schema"], "borsuk-source-onebit-falsifier-v3");
        for name in ["baseline_top256", "onebit_top256", "candidate_topk"] {
            assert_eq!(new["queries"][0][name], old["queries"][0][name]);
        }
        assert_eq!(
            new["queries"][0]["centered_topk"].as_array().unwrap().len(),
            33
        );
    }

    fn fixed_center(f: &mut Fixture, values: &[f64]) {
        let bytes = values
            .iter()
            .flat_map(|x| x.to_le_bytes())
            .collect::<Vec<_>>();
        let path = f.dir.path().join("center");
        std::fs::write(&path, &bytes).unwrap();
        f.authority["schema"] = json!("borsuk-source-onebit-falsifier-authority-v3");
        f.authority["centroid_mode"] = json!("fixed");
        f.authority["center"] = json!({"path":path,"bytes":bytes.len(),"sha256":sha(&bytes)});
    }
    #[test]
    fn centered_authentication_geometry_modes_and_scaled_query_parity() {
        for admit in [false, true] {
            for case in 0..8 {
                let mut f = Fixture::new();
                fixed_center(&mut f, &[0.25, 0.125, 0.]);
                f.remove_payload();
                std::fs::remove_file(f.dir.path().join("requests")).unwrap();
                let expected = match case {
                    0 => {
                        f.authority["center"]["bytes"] = json!(16);
                        "center byte geometry"
                    }
                    1 => {
                        f.authority["center"]["sha256"] = json!("0".repeat(64));
                        "input SHA/length"
                    }
                    2 => {
                        fixed_center(&mut f, &[f64::NAN, 0., 0.]);
                        "center shape/nonfinite"
                    }
                    3 => {
                        f.authority["centroid_mode"] = json!("origin");
                        "authority centroid mode"
                    }
                    4 => {
                        f.authority["schema"] =
                            json!("borsuk-source-onebit-falsifier-authority-v2");
                        "authority centroid mode"
                    }
                    5 => {
                        f.authority["center"] = Value::Null;
                        "authority centroid mode"
                    }
                    6 => {
                        std::fs::write(f.dir.path().join("center"), [0u8; 23]).unwrap();
                        "input size"
                    }
                    _ => {
                        fixed_center(&mut f, &[f64::INFINITY, 0., 0.]);
                        "center shape/nonfinite"
                    }
                };
                assert_eq!(
                    f.run(admit, 0, 512 * 1024 * 1024).unwrap_err().to_string(),
                    expected
                );
                f.no_report();
            }
            let mut f = Fixture::new();
            fixed_center(&mut f, &[0.25, 0.125, 0.]);
            f.run(admit, 0, 512 * 1024 * 1024).unwrap();
            let first: Value =
                serde_json::from_slice(&std::fs::read(f.dir.path().join("out")).unwrap()).unwrap();
            std::fs::remove_file(f.dir.path().join("out")).unwrap();
            let bytes = [2f32, 0., 0.].map(f32::to_le_bytes).concat();
            std::fs::write(f.dir.path().join("requests"), &bytes).unwrap();
            f.authority["requests"]["sha256"] = json!(sha(&bytes));
            f.run(admit, 0, 512 * 1024 * 1024).unwrap();
            let scaled: Value =
                serde_json::from_slice(&std::fs::read(f.dir.path().join("out")).unwrap()).unwrap();
            assert_eq!(scaled["queries"], first["queries"]);
        }
    }
    #[test]
    fn centered_exact_memory_boundary() {
        let mut f = Fixture::new();
        fixed_center(&mut f, &[0.25, 0.125, 0.]);
        f.run(true, 0, 512 * 1024 * 1024).unwrap();
        let report: Value =
            serde_json::from_slice(&std::fs::read(f.dir.path().join("out")).unwrap()).unwrap();
        let total = num(&report["memory"]["modeled_total_bytes"]).unwrap();
        assert_eq!(
            report["memory"]["center_allowance_bytes"],
            centered_memory(33, &onebit::Shape::new(3).unwrap(), 1, 256).unwrap()
        );
        std::fs::remove_file(f.dir.path().join("out")).unwrap();
        f.run(false, 0, total).unwrap();
        std::fs::remove_file(f.dir.path().join("out")).unwrap();
        f.remove_payload();
        std::fs::remove_file(f.dir.path().join("requests")).unwrap();
        for admit in [false, true] {
            assert_eq!(
                f.run(admit, 0, total - 1).unwrap_err().to_string(),
                "full working model admission before payload"
            );
            f.no_report();
        }
    }
}
