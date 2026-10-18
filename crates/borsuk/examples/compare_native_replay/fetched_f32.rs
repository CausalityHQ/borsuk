//! Offline, sealed fetched-row comparison; never changes the serving plan.
use super::{CompletedConfig, CompletedInput, HeaderSnapshot, Result, require};
use borsuk::{
    exact_sq8_nominee::{ScoredNominee, Sq8Geometry},
    returned_sq8::{ReturnedRange, rank_returned_ranges},
    two_bit_generation::normalize_two_bit_diagnostic_query,
};
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    fs::File,
    io::{BufReader, Read},
    os::unix::fs::FileExt,
    path::Path,
};

pub(super) const REPORT_SCHEMA: &str = "borsuk-fetched-f32-diagnostic-v1";
const CONFIG_SCHEMA: &str = "borsuk-fetched-f32-diagnostic-config-v1";
const MAX_WORKSPACE: u64 = 512 * 1024 * 1024;
// Covers the bounded completed reader, JSON trees, retained small reports and IO buffers.
const FIXED_BYTES: u64 = 64 * 1024 * 1024;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    schema: String,
    completed: CompletedConfig,
    generation: CompletedInput,
    canonical: CompletedInput,
    sq8: CompletedInput,
    requests: CompletedInput,
    truth: CompletedInput,
    ordinals: Vec<usize>,
    max_workspace_bytes: u64,
}
#[derive(Deserialize)]
struct Plan {
    ranges: Vec<[u64; 2]>,
    bridge_pages: usize,
}
#[derive(Deserialize)]
struct Selected {
    ordinal: usize,
    returned: Vec<super::Hit>,
    returned_physical_ordinals: Vec<usize>,
    plan: Plan,
}
fn n(value: &Value) -> Result<u64> {
    value
        .as_u64()
        .ok_or_else(|| "fetched-f32 unsigned integer".into())
}
fn product(a: u64, b: u64) -> Result<u64> {
    a.checked_mul(b)
        .ok_or_else(|| "fetched-f32 size overflow".into())
}
fn workspace(bytes: u64, rows: u64, dimensions: u64, ranges: u64, cap: u64) -> Result<u64> {
    // Native kernel's internal Vec/HashSet/sort allowance, plus retained rankings.
    // Three body copies conservatively include the native range/score lifetime.
    let bound = [
        product(bytes, 3)?,
        product(rows, 512)?,
        product(dimensions, 64)?,
        product(ranges, 128)?,
    ]
    .into_iter()
    .try_fold(FIXED_BYTES, super::plus)?;
    require(
        cap <= MAX_WORKSPACE && bound <= cap,
        "fetched-f32 workspace cap",
    )?;
    Ok(bound)
}
fn snapshot(pin: &CompletedInput) -> Result<HeaderSnapshot> {
    super::header_pin(pin, i64::MAX as u64)?;
    let file = super::header_file(&pin.path)?;
    let identity = super::file_identity(&file)?;
    require(identity.len == pin.bytes, "fetched-f32 pinned length")?;
    Ok(HeaderSnapshot {
        file,
        identity,
        path: pin.path.clone(),
    })
}
fn authenticate(
    pin: &CompletedInput,
    canonical_width: Option<(usize, &str)>,
) -> Result<HeaderSnapshot> {
    let input = snapshot(pin)?;
    let mut reader = BufReader::with_capacity(65536, &input.file);
    let width = canonical_width.map_or(65536, |(width, _)| width);
    let mut buffer = vec![0u8; width];
    let mut total = 0u64;
    let mut digest = Sha256::new();
    let mut order = Sha256::new();
    loop {
        let size = reader.read(&mut buffer)?;
        if size == 0 {
            break;
        }
        // read_exact is necessary for records because Read can legally return short.
        let size = if canonical_width.is_some() {
            reader.read_exact(&mut buffer[size..])?;
            width
        } else {
            size
        };
        total = super::plus(total, size as u64)?;
        require(total <= pin.bytes, "fetched-f32 input growth")?;
        digest.update(&buffer[..size]);
        if canonical_width.is_some() {
            order.update(&buffer[..8]);
        }
    }
    require(
        total == pin.bytes && format!("{:x}", digest.finalize()) == pin.sha256,
        "fetched-f32 input SHA/EOF",
    )?;
    if let Some((_, sha)) = canonical_width {
        require(
            format!("{:x}", order.finalize()) == sha,
            "fetched-f32 canonical order SHA",
        )?;
    }
    input.recheck()?;
    Ok(input)
}
fn row(file: &File, offset: u64, bytes: &mut [u8]) -> Result<()> {
    file.read_exact_at(bytes, offset)?;
    Ok(())
}
fn floats(bytes: &[u8], values: &mut [f32]) -> Result<()> {
    require(bytes.len() == values.len() * 4, "fetched-f32 row width")?;
    for (v, bytes) in values.iter_mut().zip(bytes.chunks_exact(4)) {
        *v = f32::from_le_bytes(bytes.try_into()?);
        require(v.is_finite(), "fetched-f32 finite coordinate")?;
    }
    Ok(())
}
fn bind(c: &Config, g: &Value) -> Result<(usize, usize, usize)> {
    let i = &c.completed.expected_bound_inputs;
    super::current_v10::validate_completed(&c.completed)?;
    let rows = n(&i["rows"])?;
    let dimensions = n(&i["dimensions"])?;
    let k = n(&i["k"])?;
    require(
        c.schema == CONFIG_SCHEMA
            && rows > 0
            && dimensions > 0
            && k > 0
            && k <= rows
            && i["metric"] == "cosine"
            && i["serving"] == json!({"mode":"baseline"})
            && i["source_layout"] == json!({"layout":"flat_records"})
            && g["schema"] == "borsuk-two-bit-generation-v12"
            && c.generation.sha256 == i["generation_root_sha256"]
            && g["canonical"]["rows"] == rows
            && g["canonical"]["dimensions"] == dimensions
            && g["canonical"]["bytes"] == c.canonical.bytes
            && g["canonical"]["sha256"] == c.canonical.sha256
            && c.canonical.bytes == product(rows, super::plus(product(dimensions, 4)?, 8)?)?
            && g["sq8_object_sha256"] == c.sq8.sha256
            && c.sq8.sha256 == i["native_sq8_sha256"]
            && c.sq8.bytes == product(rows, super::plus(dimensions, 12)?)?
            && g["discovery"]["source_order_sha256"] == i["native_order_sha256"]
            && g["discovery"]["source_sha256"] == i["native_source_sha256"]
            && c.requests.sha256 == i["requests_sha256"]
            && c.requests.bytes == n(&i["requests_bytes"])?
            && c.requests.bytes == product(n(&i["count"])?, product(dimensions, 4)?)?
            && c.truth.sha256 == i["truth_sha256"]
            && c.truth.bytes == n(&i["truth_bytes"])?
            && c.truth.bytes == product(n(&i["count"])?, product(k, 8)?)?,
        "fetched-f32 generation/input authority",
    )?;
    require(
        !c.ordinals.is_empty()
            && c.ordinals.len() <= 32
            && c.ordinals.windows(2).all(|v| v[0] < v[1])
            && c.ordinals
                .iter()
                .all(|o| (*o as u64) < n(&i["count"]).unwrap_or(0)),
        "fetched-f32 selected ordinal roster",
    )?;
    workspace(0, 0, dimensions, 0, c.max_workspace_bytes)?;
    Ok((
        usize::try_from(rows)?,
        usize::try_from(dimensions)?,
        usize::try_from(k)?,
    ))
}
fn replay_matches(scores: &[ScoredNominee], selected: &Selected, k: usize) -> Result<()> {
    require(
        selected.returned.len() == k
            && selected.returned_physical_ordinals.len() == k
            && scores.len() >= k,
        "fetched-f32 replay count",
    )?;
    require(
        scores
            .iter()
            .take(k)
            .zip(&selected.returned)
            .zip(&selected.returned_physical_ordinals)
            .all(|((s, h), o)| {
                u64::try_from(s.id).ok() == Some(h.id)
                    && s.ordinal == *o
                    && s.score.to_bits() == h.score_bits
            }),
        "fetched-f32 native replay discrepancy: attribution refused",
    )
}
#[derive(Clone, Copy)]
struct Exact {
    id: i64,
    ordinal: usize,
    cosine: f64,
}
fn canonical_score(
    bytes: &[u8],
    expected: &ScoredNominee,
    query: &[f32],
    query_norm: f64,
    values: &mut [f32],
) -> Result<Exact> {
    require(
        bytes.len() >= 8 && i64::from_le_bytes(bytes[..8].try_into()?) == expected.id,
        "fetched-f32 canonical/SQ8 physical ID mapping",
    )?;
    floats(&bytes[8..], values)?;
    let norm = super::neighborhood_norm(values)?;
    Ok(Exact {
        id: expected.id,
        ordinal: expected.ordinal,
        cosine: super::neighborhood_cosine(query, query_norm, values, norm),
    })
}
fn exact_order(rows: &mut [Exact]) {
    rows.sort_unstable_by(|a, b| a.cosine.total_cmp(&b.cosine).then(a.id.cmp(&b.id)));
}
fn summary(native: &[ScoredNominee], exact: &[Exact], truth: &[i64], k: usize) -> Value {
    let mut recovered = Vec::new();
    let mut lost = Vec::new();
    let neighbors = truth.iter().map(|id| {
        let nr = native.iter().position(|s| s.id == *id);
        let fr = exact.iter().position(|s| s.id == *id);
        let nh = nr.is_some_and(|r| r < k);
        let fh = fr.is_some_and(|r| r < k);
        if !nh && fh { recovered.push(*id); }
        if nh && !fh { lost.push(*id); }
        json!({"id":id,"fetched":nr.is_some(),"native_rank":nr.map(|r|r+1),
            "canonical_f32_rank":fr.map(|r|r+1),
            "native_score_bits":nr.map(|r|native[r].score.to_bits()),
            "canonical_cosine":fr.map(|r|exact[r].cosine),
            "native_margin_to_k":nr.map(|r| f64::from(native[k-1].score)-f64::from(native[r].score)),
            "canonical_margin_to_k":fr.map(|r|exact[k-1].cosine-exact[r].cosine)})
    }).collect::<Vec<_>>();
    json!({"truth":neighbors,"recovered_ids":recovered,"lost_ids":lost,
        "native_hits":truth.iter().filter(|id|native[..k].iter().any(|s|s.id==**id)).count(),
        "canonical_f32_hits":truth.iter().filter(|id|exact[..k].iter().any(|s|s.id==**id)).count(),
        "native_cutoff_ties":native.iter().filter(|s|s.score==native[k-1].score).count(),
        "canonical_cutoff_ties":exact.iter().filter(|s|s.cosine==exact[k-1].cosine).count(),
        "native_k_to_next_margin":native.get(k).map(|s|f64::from(s.score)-f64::from(native[k-1].score)),
        "canonical_k_to_next_margin":exact.get(k).map(|s|s.cosine-exact[k-1].cosine),
        "native_top":native[..k].iter().map(|s|json!({"id":s.id,"physical_ordinal":s.ordinal,
            "score_bits":s.score.to_bits()})).collect::<Vec<_>>(),
        "canonical_f32_top":exact[..k].iter().map(|s|json!({"id":s.id,
            "physical_ordinal":s.ordinal,"cosine":s.cosine,"score_bits_f64":s.cosine.to_bits()})).collect::<Vec<_>>()})
}
pub(super) fn reduce(path: &Path, sha: &str) -> Result<Value> {
    let (c, config_bytes): (Config, _) = super::read_config(path, sha)?;
    let (g, generation_snapshot) = super::header_json(&c.generation)?;
    let (rows, dimensions, k) = bind(&c, &g)?;
    let mut selected = Vec::with_capacity(c.ordinals.len());
    // No payload opens or scores until the existing reader validates seal, terminal and EOF.
    let run = super::read_run_observed(
        &c.completed.input.path,
        &c.completed.input.sha256,
        Some(&c.completed),
        |line, q, _| {
            if c.ordinals.binary_search(&q.ordinal).is_ok() {
                selected.push(serde_json::from_slice::<Selected>(line)?);
            }
            Ok(())
        },
    )?;
    require(
        run.file_identity.len == c.completed.input.bytes
            && selected
                .iter()
                .map(|s| s.ordinal)
                .eq(c.ordinals.iter().copied()),
        "fetched-f32 complete selected roster and file length",
    )?;
    let low: Vec<f32> = serde_json::from_value(g["low"].clone())?;
    let step: Vec<f32> = serde_json::from_value(g["step"].clone())?;
    require(
        low.len() == dimensions
            && step.len() == dimensions
            && low.iter().all(|v| v.is_finite())
            && step.iter().all(|v| v.is_finite() && *v > 0.),
        "fetched-f32 calibration",
    )?;
    let width = dimensions.checked_add(12).ok_or("SQ8 width")?;
    let canonical_width = dimensions
        .checked_mul(4)
        .and_then(|v| v.checked_add(8))
        .ok_or("canonical width")?;
    let canonical = authenticate(
        &c.canonical,
        Some((
            canonical_width,
            c.completed.expected_bound_inputs["native_order_sha256"]
                .as_str()
                .ok_or("order SHA")?,
        )),
    )?;
    let sq8 = authenticate(&c.sq8, None)?;
    let requests = authenticate(&c.requests, None)?;
    let truth_file = authenticate(&c.truth, None)?;
    let mut reports = Vec::with_capacity(selected.len());
    for s in &selected {
        let count_bytes = s.plan.ranges.iter().try_fold(0u64, |sum, [start, end]| {
            require(
                start < end
                    && start % width as u64 == 0
                    && end % width as u64 == 0
                    && *end <= c.sq8.bytes,
                "fetched-f32 aligned bounded range",
            )?;
            super::plus(sum, end - start)
        })?;
        let count = usize::try_from(count_bytes / width as u64)?;
        let mut admitted = workspace(
            count_bytes,
            count as u64,
            dimensions as u64,
            s.plan.ranges.len() as u64,
            c.max_workspace_bytes,
        )?;
        require(count >= k, "fetched-f32 fetched underfill")?;
        let mut body = vec![0u8; usize::try_from(count_bytes)?];
        let mut offset = 0usize;
        let mut ranges = Vec::with_capacity(s.plan.ranges.len());
        for [start, end] in &s.plan.ranges {
            let len = usize::try_from(end - start)?;
            row(&sq8.file, *start, &mut body[offset..offset + len])?;
            offset += len;
        }
        offset = 0;
        for [start, end] in &s.plan.ranges {
            let len = usize::try_from(end - start)?;
            ranges.push(ReturnedRange {
                start: usize::try_from(*start)?,
                bytes: &body[offset..offset + len],
            });
            offset += len;
        }
        let mut raw_query = vec![0u8; dimensions * 4];
        row(
            &requests.file,
            product(s.ordinal as u64, raw_query.len() as u64)?,
            &mut raw_query,
        )?;
        let mut query = vec![0f32; dimensions];
        floats(&raw_query, &mut query)?;
        let normalized = normalize_two_bit_diagnostic_query(&query).map_err(|e| e.to_string())?;
        let native = rank_returned_ranges(
            Sq8Geometry { rows, dimensions },
            &ranges,
            &normalized,
            &low,
            &step,
            count,
            body.len(),
        )
        .map_err(|e| format!("native SQ8 replay: {e:?}"))?;
        replay_matches(&native, s, k)?;
        let query_norm = super::neighborhood_norm(&normalized)?;
        let mut exact = Vec::with_capacity(count);
        let mut canonical_row = vec![0u8; canonical_width];
        let mut values = vec![0f32; dimensions];
        admitted = admitted.max(workspace(
            body.capacity() as u64,
            native.capacity().max(exact.capacity()) as u64,
            values.capacity().max(query.capacity()) as u64,
            ranges.capacity() as u64,
            c.max_workspace_bytes,
        )?);
        for score in &native {
            row(
                &canonical.file,
                product(score.ordinal as u64, canonical_width as u64)?,
                &mut canonical_row,
            )?;
            exact.push(canonical_score(
                &canonical_row,
                score,
                &normalized,
                query_norm,
                &mut values,
            )?);
        }
        exact_order(&mut exact);
        let mut truth_bytes = vec![0u8; k * 8];
        row(
            &truth_file.file,
            product(s.ordinal as u64, truth_bytes.len() as u64)?,
            &mut truth_bytes,
        )?;
        let truth = truth_bytes
            .chunks_exact(8)
            .map(|b| i64::try_from(u64::from_le_bytes(b.try_into()?)).map_err(Into::into))
            .collect::<Result<Vec<_>>>()?;
        require(
            truth
                .iter()
                .enumerate()
                .all(|(j, id)| !truth[..j].contains(id)),
            "fetched-f32 duplicate truth ID",
        )?;
        let mut report = summary(&native, &exact, &truth, k);
        report["ordinal"] = json!(s.ordinal);
        report["fetched_rows"] = json!(count);
        report["fetched_ranges"] = json!(s.plan.ranges);
        report["bridge_pages_included"] = json!(s.plan.bridge_pages);
        report["checked_workspace_bytes"] = json!(admitted);
        reports.push(report);
    }
    for input in [
        &generation_snapshot,
        &canonical,
        &sq8,
        &requests,
        &truth_file,
    ] {
        input.recheck()?;
    }
    require(
        super::file_identity(&super::open_input(&c.completed.input.path)?)? == run.file_identity,
        "fetched-f32 completed input changed",
    )?;
    Ok(
        json!({"schema":REPORT_SCHEMA,"status":"NATIVE_REPLAY_VERIFIED_F32_DIAGNOSTIC",
        "complete":true,"config_path":path,"config_sha256":sha,"config_bytes":config_bytes,
        "completed":c.completed,"generation":c.generation,"canonical":c.canonical,"sq8":c.sq8,
        "requests":c.requests,"truth":c.truth,"ordinals":c.ordinals,"queries":reports,
        "native_replay_exact_ids_ordinals_scorebits":true,"canonical_order_authenticated":true,
        "max_workspace_bytes":c.max_workspace_bytes,"qualified":false,
        "quantization_attributed":false,"vendor_or_scientific_win_claim":false,
        "score_units":{"canonical":"cosine distance = 1 - dot/(norms), lower is better","native":"SQ8 squared-L2 = stored norm + query norm squared - 2 * decoded dot; lower is better","margins":"Each margin uses its own distance units; magnitudes are not interchangeable"},
        "normalization":"production query normalization; canonical f32 unit rows tolerance 1e-6; cosine f64 ascending accumulation; ties by ID ascending",
        "limits":"Only exactly fetched rows including bridges. Canonical already normalized f32, not original vectors; differences include normalization, precision and ties. No global recall or quantization-only attribution."}),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    fn record(id: i64, values: &[f32]) -> Vec<u8> {
        let mut b = id.to_le_bytes().to_vec();
        for v in values {
            b.extend_from_slice(&v.to_le_bytes());
        }
        b
    }
    #[test]
    fn literal_cosine_nonidentity_ids_ties_and_fetched_recovery() {
        let native = vec![
            ScoredNominee {
                id: 90,
                ordinal: 2,
                score: 0.,
            },
            ScoredNominee {
                id: 41,
                ordinal: 0,
                score: 1.,
            },
            ScoredNominee {
                id: 7,
                ordinal: 1,
                score: 2.,
            },
        ];
        let mut values = vec![0.; 2];
        let mut exact = vec![
            canonical_score(
                &record(90, &[0., 1.]),
                &native[0],
                &[1., 0.],
                1.,
                &mut values,
            )
            .unwrap(),
            canonical_score(
                &record(41, &[1., 0.]),
                &native[1],
                &[1., 0.],
                1.,
                &mut values,
            )
            .unwrap(),
            canonical_score(
                &record(7, &[1., 0.]),
                &native[2],
                &[1., 0.],
                1.,
                &mut values,
            )
            .unwrap(),
        ];
        assert_eq!(
            exact.iter().map(|s| s.cosine).collect::<Vec<_>>(),
            [1., 0., 0.]
        );
        exact_order(&mut exact);
        assert_eq!(exact.iter().map(|s| s.id).collect::<Vec<_>>(), [7, 41, 90]);
        let report = summary(&native, &exact, &[7], 1);
        assert_eq!(report["recovered_ids"], json!([7]));
        assert_eq!(report["canonical_cutoff_ties"], 2);
        assert!(
            canonical_score(
                &record(91, &[0., 1.]),
                &native[0],
                &[1., 0.],
                1.,
                &mut values
            )
            .is_err()
        );
        assert!(
            canonical_score(
                &record(90, &[0., 0.]),
                &native[0],
                &[1., 0.],
                1.,
                &mut values
            )
            .is_err()
        );
    }
    #[test]
    fn native_kernel_includes_bridge_and_refuses_replay_discrepancy() {
        let mut body = Vec::new();
        for (id, code) in [(90i64, 0u8), (7, 1), (41, 0)] {
            body.extend(id.to_le_bytes());
            body.extend(1f32.to_le_bytes());
            body.push(code);
        }
        // Middle row is a bridge: the whole recorded interval must reach the kernel.
        let scores = rank_returned_ranges(
            Sq8Geometry {
                rows: 9,
                dimensions: 1,
            },
            &[ReturnedRange {
                start: 3 * 13,
                bytes: &body,
            }],
            &[1.],
            &[0.],
            &[1.],
            3,
            39,
        )
        .unwrap();
        assert_eq!(
            (scores[0].id, scores[0].ordinal, scores[0].score.to_bits()),
            (7, 4, 0f32.to_bits())
        );
        let mut selected = Selected {
            ordinal: 1,
            returned: vec![super::super::Hit {
                id: 7,
                score_bits: 0,
            }],
            returned_physical_ordinals: vec![4],
            plan: Plan {
                ranges: vec![[39, 78]],
                bridge_pages: 1,
            },
        };
        replay_matches(&scores, &selected, 1).unwrap();
        selected.returned[0].score_bits = 1;
        assert!(replay_matches(&scores, &selected, 1).is_err());
        selected.returned[0].score_bits = 0;
        selected.returned_physical_ordinals[0] = 1;
        assert!(replay_matches(&scores, &selected, 1).is_err());
        selected.returned_physical_ordinals[0] = 4;
        selected.returned[0].id = 90;
        assert!(replay_matches(&scores, &selected, 1).is_err());
    }
    #[test]
    fn strict_config_sha_and_unknown_fields_refuse() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config");
        let pin = json!({"path":"/artifact","bytes":1,"sha256":"0".repeat(64)});
        let mut config = json!({"schema":CONFIG_SCHEMA,
            "completed":{"schema":"fixture","input":pin,"expected_identity":{},"expected_bound_inputs":{}},
            "generation":pin,"canonical":pin,"sq8":pin,"requests":pin,"truth":pin,
            "ordinals":[1,8,14,20,25,30],"max_workspace_bytes":MAX_WORKSPACE});
        let body = serde_json::to_vec(&config).unwrap();
        std::fs::write(&path, &body).unwrap();
        let sha = format!("{:x}", Sha256::digest(&body));
        super::super::read_config::<Config>(&path, &sha).unwrap();
        assert!(super::super::read_config::<Config>(&path, &"0".repeat(64)).is_err());
        config["unexpected"] = json!(true);
        assert!(serde_json::from_value::<Config>(config).is_err());
    }

    #[test]
    fn pins_truncation_order_and_occupied_output_refuse() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("canonical");
        let bytes = record(41, &[1., 0.]);
        File::create(&path).unwrap().write_all(&bytes).unwrap();
        let mut pin = CompletedInput {
            path,
            bytes: bytes.len() as u64,
            sha256: format!("{:x}", Sha256::digest(&bytes)),
        };
        let order = format!("{:x}", Sha256::digest(41i64.to_le_bytes()));
        authenticate(&pin, Some((16, &order))).unwrap();
        assert!(authenticate(&pin, Some((16, &"0".repeat(64)))).is_err());
        pin.sha256 = "0".repeat(64);
        assert!(authenticate(&pin, None).is_err());
        pin.sha256 = format!("{:x}", Sha256::digest(&bytes));
        File::options()
            .write(true)
            .open(&pin.path)
            .unwrap()
            .set_len(15)
            .unwrap();
        assert!(authenticate(&pin, None).is_err());
        let partial = std::fs::read(&pin.path).unwrap();
        pin.bytes = partial.len() as u64;
        pin.sha256 = format!("{:x}", Sha256::digest(&partial));
        let error = authenticate(&pin, Some((16, &order))).unwrap_err();
        assert_eq!(
            error.downcast_ref::<std::io::Error>().unwrap().kind(),
            std::io::ErrorKind::UnexpectedEof
        );
        let output = dir.path().join("report");
        std::fs::write(&output, b"keep").unwrap();
        assert!(
            super::super::execute_report(&output, REPORT_SCHEMA, || panic!(
                "occupied output reduced"
            ))
            .is_err()
        );
        assert_eq!(std::fs::read(output).unwrap(), b"keep");
        assert!(workspace(u64::MAX, 1, 1, 1, MAX_WORKSPACE).is_err());
        assert!(workspace(0, 0, 1, 0, FIXED_BYTES).is_err());
    }
    #[test]
    fn sealed_reducer_bridge_recovery_and_coherent_refusals() {
        use super::super::{scale_native_runner, tests as native};
        let (dir, paths) =
            scale_native_runner::source_layout_reducer_native_fixture_with(96, 2, false, false)
                .unwrap();
        let original = native::assert_current_native(dir.path(), &paths[0]);
        let original_root =
            std::fs::read_to_string(dir.path().join("generation/manifest.json")).unwrap();
        let root: Value = serde_json::from_str(&original_root).unwrap();
        let original_canonical =
            std::fs::read(dir.path().join("generation/canonical.bin")).unwrap();
        let mut canonical = original_canonical.clone();
        let width = 8 + 4 * 1024;
        let physical = canonical
            .chunks_exact(width)
            .position(|r| i64::from_le_bytes(r[..8].try_into().unwrap()) == 40)
            .unwrap();
        assert_eq!(physical, 55); // ID40 lies inside the unselected middle/bridge page.
        canonical[physical * width + 8..(physical + 1) * width].fill(0);
        canonical[physical * width + 8..physical * width + 12].copy_from_slice(&1f32.to_le_bytes());
        let mut truth = std::fs::read(dir.path().join("truth.u64")).unwrap();
        truth[9 * 8..10 * 8].copy_from_slice(&40u64.to_le_bytes());
        let hash = |bytes: &[u8]| format!("{:x}", Sha256::digest(bytes));
        let pin = |path: std::path::PathBuf, body: &[u8]| {
            std::fs::write(&path, body).unwrap();
            json!({"path":path,"bytes":body.len(),"sha256":hash(body)})
        };
        let packet = |tag: &str, mut rows: Vec<Value>, canonical: &[u8]| {
            let order = canonical
                .chunks_exact(width)
                .flat_map(|r| r[..8].iter().copied())
                .collect::<Vec<_>>();
            let root_body = original_root
                .replace(
                    root["canonical"]["sha256"].as_str().unwrap(),
                    &hash(canonical),
                )
                .replace(
                    root["discovery"]["source_order_sha256"].as_str().unwrap(),
                    &hash(&order),
                );
            let substitutions = [
                (
                    original[1]["generation_root_sha256"].as_str().unwrap(),
                    hash(root_body.as_bytes()),
                ),
                (
                    root["canonical"]["sha256"].as_str().unwrap(),
                    hash(canonical),
                ),
                (
                    original[1]["native_order_sha256"].as_str().unwrap(),
                    hash(&order),
                ),
                (original[1]["truth_sha256"].as_str().unwrap(), hash(&truth)),
            ];
            for r in &mut rows {
                let mut text = serde_json::to_string(r).unwrap();
                for (old, new) in &substitutions {
                    text = text.replace(old, new);
                }
                *r = serde_json::from_str(&text).unwrap();
                if r.get("sq8_get_budget").is_some() {
                    r["sq8_get_budget"] = json!(1);
                }
                if r["phase"] == "query" {
                    assert_eq!(r["plan"]["ranges"], json!([[0, 96 * 1036]]));
                    r["plan"]["selected_pages"] = json!([0, 2]);
                    r["plan"]["target_pages"] = json!(2);
                    r["plan"]["target_shortfall"] = json!(0);
                    r["plan"]["bridge_pages"] = json!(1);
                }
                if r["phase"] == "recall" && r["ordinal"] == 0 {
                    r["hits10"] = json!(9);
                    r["recall10"] = json!(0.9);
                }
                if r["phase"] == "terminal" {
                    r["summary"]["total_hits10"] = json!(19);
                    r["summary"]["recall_numerator"] = json!(19);
                    r["summary"]["mean_recall10"] = json!(0.95);
                }
            }
            native::authenticate(&mut rows);
            let completed = pin(
                dir.path().join(format!("{tag}.jsonl")),
                &native::encode(&rows),
            );
            let generation = pin(
                dir.path().join(format!("{tag}-manifest.json")),
                root_body.as_bytes(),
            );
            let canonical = pin(dir.path().join(format!("{tag}-canonical.bin")), canonical);
            let sq8_body = std::fs::read(dir.path().join("sq8")).unwrap();
            let requests = std::fs::read(dir.path().join("requests.f32")).unwrap();
            let config = json!({"schema":CONFIG_SCHEMA,"completed":{"schema":super::super::SCALE_CONFIG_SCHEMA,
                "input":completed,"expected_identity":rows[0],"expected_bound_inputs":rows[1]},
                "generation":generation,"canonical":canonical,
                "sq8":pin(dir.path().join(format!("{tag}-sq8")),&sq8_body),
                "requests":pin(dir.path().join(format!("{tag}-requests")),&requests),
                "truth":pin(dir.path().join(format!("{tag}-truth")),&truth),
                "ordinals":[0],"max_workspace_bytes":MAX_WORKSPACE});
            let path = dir.path().join(format!("{tag}-config.json"));
            let body = serde_json::to_vec(&config).unwrap();
            std::fs::write(&path, &body).unwrap();
            (path, hash(&body), config)
        };
        let run = |tag: &str, path: &Path, sha: &str, expected_error: Option<&str>| {
            let output = dir.path().join(format!("{tag}-report.json"));
            let success =
                super::super::execute_report(&output, REPORT_SCHEMA, || reduce(path, sha)).unwrap();
            let report: Value = serde_json::from_slice(&std::fs::read(output).unwrap()).unwrap();
            assert_eq!(
                if success { 0 } else { 2 },
                if expected_error.is_none() { 0 } else { 2 },
                "{report}"
            );
            if let Some(error) = expected_error {
                assert_eq!(report["status"], "INVALID");
                assert_eq!(report["complete"], false);
                assert!(report.get("queries").is_none());
                assert!(
                    report["error"].as_str().unwrap().contains(error),
                    "{report}"
                );
            }
            report
        };
        let (path, sha, config) = packet("valid", original.clone(), &canonical);
        let report = run("valid", &path, &sha, None);
        let q = &report["queries"][0];
        assert_eq!(q["fetched_rows"], 96);
        assert_eq!(q["bridge_pages_included"], 1);
        assert_eq!(q["recovered_ids"], json!([40]));
        assert_eq!(
            (q["native_hits"].as_u64(), q["canonical_f32_hits"].as_u64()),
            (Some(9), Some(10))
        );
        let ids = |key: &str| {
            q[key]
                .as_array()
                .unwrap()
                .iter()
                .map(|v| v["id"].as_i64().unwrap())
                .collect::<Vec<_>>()
        };
        assert_eq!(ids("native_top"), [20, 0, 1, 2, 3, 4, 5, 6, 7, 8]);
        assert_eq!(ids("canonical_f32_top"), [20, 40, 0, 1, 2, 3, 4, 5, 6, 7]);
        assert_eq!(q["canonical_f32_top"][1]["physical_ordinal"], 55);
        assert_eq!(q["canonical_f32_top"][0]["cosine"], 0.0);
        assert_eq!(q["canonical_f32_top"][2]["cosine"], 0.25);
        let mut mismatch = original.clone();
        let query = mismatch
            .iter_mut()
            .find(|r| r["phase"] == "query" && r["ordinal"] == 0)
            .unwrap();
        query["returned"][0]["score_bits"] =
            json!(query["returned"][0]["score_bits"].as_u64().unwrap() + 1);
        let (p, s, _) = packet("replay", mismatch, &canonical);
        run("replay", &p, &s, Some("native replay discrepancy"));
        let mut mapping = canonical.clone();
        let first: [u8; 8] = mapping[..8].try_into().unwrap();
        let second: [u8; 8] = mapping[width..width + 8].try_into().unwrap();
        mapping[..8].copy_from_slice(&second);
        mapping[width..width + 8].copy_from_slice(&first);
        let (p, s, _) = packet("mapping", original.clone(), &mapping);
        run("mapping", &p, &s, Some("physical ID mapping"));
        let request = Path::new(config["requests"]["path"].as_str().unwrap());
        let mut bytes = std::fs::read(request).unwrap();
        bytes[0] ^= 1;
        std::fs::write(request, &bytes).unwrap();
        run("changed", &path, &sha, Some("input SHA/EOF"));
        std::fs::write(request, &bytes[..bytes.len() - 1]).unwrap();
        run("truncated", &path, &sha, Some("pinned length"));
    }
}
