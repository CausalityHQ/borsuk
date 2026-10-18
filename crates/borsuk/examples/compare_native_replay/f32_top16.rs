//! Offline current-cosine top16 reranking over authenticated FP32 coordinates.
use super::fetched_f32::{
    Exact, authenticate, canonical_score, exact_order, floats, row, workspace,
};
use super::sq8_cosine::{Selected, metadata_charge, replay_matches, with_metadata};
use super::{CompletedConfig, CompletedInput, Result, require};
use borsuk::{
    exact_sq8_nominee::{ScoredNominee, Sq8Geometry},
    returned_sq8::{ReturnedRange, score_returned_ranges_cosine_excluding},
    two_bit_generation::normalize_two_bit_diagnostic_query,
};
use serde::Deserialize;
use serde_json::{Value, json};
use std::{
    fs::File,
    io::{self, Seek, Write},
    os::unix::fs::MetadataExt,
    path::Path,
};

pub(super) const REPORT_SCHEMA: &str = "borsuk-cosine-top16-f32-diagnostic-v1";
const CONFIG_SCHEMA: &str = "borsuk-cosine-top16-f32-config-v1";
const ARITHMETIC: &str = "FP32 coordinates / existing sequential-f64 canonical oracle";
const SHORTLIST: usize = 16;
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
    excluded_ids: Vec<i64>,
    max_workspace_bytes: u64,
}
fn number(v: &Value) -> Result<u64> {
    v.as_u64()
        .ok_or_else(|| "f32-top16 unsigned integer".into())
}
fn product(a: u64, b: u64) -> Result<u64> {
    a.checked_mul(b)
        .ok_or_else(|| "f32-top16 size overflow".into())
}
fn bind(c: &Config, g: &Value) -> Result<(usize, usize, usize)> {
    super::current_v10::validate_completed(&c.completed)?;
    let i = &c.completed.expected_bound_inputs;
    let rows = number(&i["rows"])?;
    let d = number(&i["dimensions"])?;
    let k = number(&i["k"])?;
    let count = number(&i["count"])?;
    require(
        c.schema == CONFIG_SCHEMA
            && rows > 0
            && d > 0
            && k == 10
            && k <= rows
            && count > 0
            && i["metric"] == "cosine"
            && i["serving"] == json!({"mode":"baseline"})
            && i["source_layout"] == json!({"layout":"flat_records"})
            && g["schema"] == "borsuk-two-bit-generation-v12"
            && c.generation.sha256 == i["generation_root_sha256"]
            && g["canonical"]["rows"] == rows
            && g["canonical"]["dimensions"] == d
            && g["canonical"]["bytes"] == c.canonical.bytes
            && g["canonical"]["sha256"] == c.canonical.sha256
            && c.canonical.bytes == product(rows, super::plus(product(d, 4)?, 8)?)?
            && g["sq8_object_sha256"] == c.sq8.sha256
            && c.sq8.sha256 == i["native_sq8_sha256"]
            && c.sq8.bytes == product(rows, super::plus(d, 12)?)?
            && g["discovery"]["source_order_sha256"] == i["native_order_sha256"]
            && g["discovery"]["source_sha256"] == i["native_source_sha256"]
            && c.requests.sha256 == i["requests_sha256"]
            && c.requests.bytes == number(&i["requests_bytes"])?
            && c.requests.bytes == product(count, product(d, 4)?)?
            && c.truth.sha256 == i["truth_sha256"]
            && c.truth.bytes == number(&i["truth_bytes"])?
            && c.truth.bytes == product(count, product(k, 8)?)?,
        "f32-top16 generation/input authority",
    )?;
    require(
        (c.ordinals
            .iter()
            .copied()
            .eq(0..usize::try_from(count.min(32))?)
            || (count <= 1000 && c.ordinals.iter().copied().eq(0..usize::try_from(count)?)))
            && c.excluded_ids.windows(2).all(|v| v[0] < v[1]),
        "f32-top16 exact first32/full<=1000 prefix and sorted unique exclusions",
    )?;
    workspace(
        0,
        c.excluded_ids.capacity() as u64,
        d,
        0,
        c.max_workspace_bytes,
    )?;
    Ok((
        usize::try_from(rows)?,
        usize::try_from(d)?,
        usize::try_from(k)?,
    ))
}
// Charge retained JSON allocation capacities and conservative map-node overhead.
fn value_bytes(v: &Value) -> Result<u64> {
    match v {
        Value::String(s) => Ok(s.capacity() as u64),
        Value::Array(a) => a.iter().try_fold(
            product(a.capacity() as u64, std::mem::size_of::<Value>() as u64)?,
            |sum, v| super::plus(sum, value_bytes(v)?),
        ),
        Value::Object(o) => o.iter().try_fold(0, |sum, (key, v)| {
            super::plus(
                sum,
                super::plus(super::plus(key.capacity() as u64, 256)?, value_bytes(v)?)?,
            )
        }),
        _ => Ok(0),
    }
}
fn rerank(native: &[ScoredNominee], exact: &[Exact], k: usize) -> Vec<Exact> {
    exact
        .iter()
        .filter(|s| native.iter().take(SHORTLIST).any(|n| n.id == s.id))
        .take(k)
        .copied()
        .collect()
}
fn native_top(scores: &[ScoredNominee], k: usize) -> Value {
    json!(
        scores
            .iter()
            .take(k)
            .map(|s| json!({"id":s.id,"physical_ordinal":s.ordinal,
        "score_bits":s.score.to_bits()}))
            .collect::<Vec<_>>()
    )
}
fn exact_top(scores: &[Exact], k: usize) -> Value {
    json!(
        scores
            .iter()
            .take(k)
            .map(|s| json!({"id":s.id,"physical_ordinal":s.ordinal,
        "cosine":s.cosine,"score_bits_f64":s.cosine.to_bits()}))
            .collect::<Vec<_>>()
    )
}
fn query_report(native: &[ScoredNominee], exact: &[Exact], truth: &[i64], k: usize) -> Value {
    let reranked = rerank(native, exact, k);
    let old = native.iter().take(k).map(|s| s.id).collect::<Vec<_>>();
    let shortlist = native
        .iter()
        .take(SHORTLIST)
        .map(|s| s.id)
        .collect::<Vec<_>>();
    let new = reranked.iter().map(|s| s.id).collect::<Vec<_>>();
    let all = exact.iter().take(k).map(|s| s.id).collect::<Vec<_>>();
    let missing = all
        .iter()
        .filter(|id| !shortlist.contains(id))
        .copied()
        .collect::<Vec<_>>();
    let hits = |ids: &[i64]| truth.iter().filter(|id| ids.contains(id)).count();
    json!({"native_top10":native_top(native,k),"cosine_top16":native_top(native,SHORTLIST),
        "reranked_top10":exact_top(&reranked,k),"all_fetched_canonical_top10":exact_top(exact,k),
        "truth_ids":truth,"native_hits":hits(&old),"reranked_hits":hits(&new),
        "all_fetched_canonical_hits":hits(&all),
        "gained_truth_ids":truth.iter().filter(|id|new.contains(id)&&!old.contains(id)).collect::<Vec<_>>(),
        "lost_truth_ids":truth.iter().filter(|id|old.contains(id)&&!new.contains(id)).collect::<Vec<_>>(),
        "canonical_top10_contained_count":all.len()-missing.len(),
        "canonical_top10_missing_from_cosine_top16_ids":missing,
        "canonical_top10_fully_contained":missing.is_empty()&&all.len()==k,
        "visible_fetched_rows":native.len(),"shortlist_underfill":native.len()<SHORTLIST,
        "underfill":reranked.len()<k,"all_fetched_underfill":exact.len()<k})
}
// Same original-descriptor invalidation as finish_selection_publication; only this new mode opts in.
pub(super) fn publish(
    output: &Path,
    report: &Value,
    dir: File,
    mut file: File,
    mut sync: impl FnMut(&File, bool) -> io::Result<()>,
) -> Result<bool> {
    let cleanup_dir = dir.try_clone()?;
    let mut owned = file.try_clone()?;
    let result = (|| -> Result<bool> {
        let created = super::file_identity(&file)?;
        let original_parent = dir.metadata()?;
        let mut bytes = serde_json::to_vec(report)?;
        bytes.push(b'\n');
        require(
            bytes.len() as u64 <= super::report_cap(REPORT_SCHEMA),
            "report byte cap",
        )?;
        file.write_all(&bytes)?;
        sync(&file, false)?;
        let readback = super::header_file(output)?;
        let current = super::file_identity(&readback)?;
        require(
            (current.dev, current.ino, current.len)
                == (created.dev, created.ino, bytes.len() as u64)
                && super::file_identity(&file)? == current,
            "f32-top16 output identity changed",
        )?;
        let snapshot = super::HeaderSnapshot {
            file: readback,
            path: output.to_owned(),
            identity: current,
        };
        sync(&dir, true)?;
        snapshot.recheck()?;
        require(
            file.metadata()?.nlink() == 1 && super::file_identity(&file)? == snapshot.identity,
            "f32-top16 final write descriptor",
        )?;
        let current_parent = super::parent(output)?.metadata()?;
        require(
            (original_parent.dev(), original_parent.ino())
                == (current_parent.dev(), current_parent.ino()),
            "f32-top16 output parent changed",
        )?;
        Ok(report["schema"] == REPORT_SCHEMA
            && report["status"] == "NATIVE_REPLAY_VERIFIED_COSINE_TOP16_F32"
            && report["complete"] == true
            && report["qualified"] == false)
    })();
    match result {
        Ok(published) => Ok(published),
        Err(error) => {
            let cleanup = (|| -> Result<()> {
                owned.set_len(0)?;
                owned.seek(io::SeekFrom::Start(0))?;
                owned.write_all(
                    b"{\"status\":\"INVALID\",\"complete\":false,\"publication_failed\":true}\n",
                )?;
                owned.sync_all()?;
                cleanup_dir.sync_all()?;
                Ok(())
            })();
            match cleanup {
                Ok(()) => Err(error),
                Err(cleanup_error) => Err(format!(
                    "publication failed: {error}; owned output invalidation failed: {cleanup_error}"
                )
                .into()),
            }
        }
    }
}
pub(super) fn reduce(path: &Path, sha: &str) -> Result<Value> {
    let (c, bytes): (Config, _) = super::read_config(path, sha)?;
    reduce_config(c, path, sha, bytes)
}
fn reduce_config(c: Config, path: &Path, sha: &str, config_bytes: u64) -> Result<Value> {
    let (g, generation) = super::header_json(&c.generation)?;
    let (rows, dimensions, k) = bind(&c, &g)?;
    let mut selected = Vec::with_capacity(c.ordinals.len());
    let ordinal_bytes = product(
        c.ordinals.capacity() as u64,
        std::mem::size_of::<usize>() as u64,
    )?;
    // Authenticate complete seal, terminal and EOF before opening/scoring payloads.
    let run = super::read_run_observed(
        &c.completed.input.path,
        &c.completed.input.sha256,
        Some(&c.completed),
        |line, q, _| {
            if c.ordinals.binary_search(&q.ordinal).is_ok() {
                selected.push(serde_json::from_slice::<Selected>(line)?);
                with_metadata(
                    workspace(
                        0,
                        c.excluded_ids.capacity() as u64,
                        dimensions as u64,
                        0,
                        c.max_workspace_bytes,
                    )?,
                    super::plus(
                        metadata_charge(&selected, selected.capacity(), c.ordinals.len(), k)?,
                        ordinal_bytes,
                    )?,
                    c.max_workspace_bytes,
                )?;
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
        "f32-top16 complete selected prefix roster",
    )?;
    let low: Vec<f32> = serde_json::from_value(g["low"].clone())?;
    let step: Vec<f32> = serde_json::from_value(g["step"].clone())?;
    require(
        low.len() == dimensions
            && step.len() == dimensions
            && low.iter().all(|v| v.is_finite())
            && step.iter().all(|v| v.is_finite() && *v > 0.),
        "f32-top16 calibration",
    )?;
    let width = dimensions.checked_add(12).ok_or("f32-top16 SQ8 width")?;
    let canonical_width = dimensions
        .checked_mul(4)
        .and_then(|v| v.checked_add(8))
        .ok_or("f32-top16 canonical width")?;
    let order = c.completed.expected_bound_inputs["native_order_sha256"]
        .as_str()
        .ok_or("f32-top16 order SHA")?;
    let canonical = authenticate(&c.canonical, Some((canonical_width, order)))?;
    let sq8 = authenticate(&c.sq8, Some((width, order)))?;
    let requests = authenticate(&c.requests, None)?;
    let truth = authenticate(&c.truth, None)?;
    let mut reports = Vec::with_capacity(selected.len());
    let metadata = super::plus(
        metadata_charge(&selected, selected.capacity(), reports.capacity(), k)?,
        ordinal_bytes,
    )?;
    let mut retained_reports = 0u64;
    let mut peak = 0u64;
    for s in &selected {
        let count_bytes = s.plan.ranges.iter().try_fold(0u64, |sum, [start, end]| {
            require(
                start < end
                    && start % width as u64 == 0
                    && end % width as u64 == 0
                    && *end <= c.sq8.bytes,
                "f32-top16 bounded aligned range",
            )?;
            super::plus(sum, end - start)
        })?;
        let count = usize::try_from(count_bytes / width as u64)?;
        let mut admitted = with_metadata(
            workspace(
                count_bytes,
                super::plus(count as u64, c.excluded_ids.capacity() as u64)?,
                dimensions as u64,
                s.plan.ranges.capacity() as u64,
                c.max_workspace_bytes,
            )?,
            super::plus(metadata, product(retained_reports, 2)?)?,
            c.max_workspace_bytes,
        )?;
        require(count >= k, "f32-top16 fetched underfill")?;
        let mut body = vec![0u8; usize::try_from(count_bytes)?];
        let mut offset = 0;
        for [start, end] in &s.plan.ranges {
            let len = usize::try_from(end - start)?;
            row(&sq8.file, *start, &mut body[offset..offset + len])?;
            offset += len;
        }
        let mut ranges = Vec::with_capacity(s.plan.ranges.len());
        offset = 0;
        for [start, end] in &s.plan.ranges {
            let len = usize::try_from(end - start)?;
            ranges.push(ReturnedRange {
                start: usize::try_from(*start)?,
                bytes: &body[offset..offset + len],
            });
            offset += len;
        }
        let mut query_bytes = vec![0u8; dimensions * 4];
        row(
            &requests.file,
            product(s.ordinal as u64, query_bytes.len() as u64)?,
            &mut query_bytes,
        )?;
        let mut raw_query = vec![0f32; dimensions];
        floats(&query_bytes, &mut raw_query)?;
        let query = normalize_two_bit_diagnostic_query(&raw_query).map_err(|e| e.to_string())?;
        let mut native = score_returned_ranges_cosine_excluding(
            Sq8Geometry { rows, dimensions },
            &ranges,
            &query,
            &low,
            &step,
            body.len(),
            &[],
        )
        .map_err(|e| format!("f32-top16 current cosine replay: {e:?}"))?;
        replay_matches(&native, s, k)?;
        let control = native_top(&native, k);
        // Visibility applies only after the unfiltered current-cosine control matches.
        native.retain(|n| c.excluded_ids.binary_search(&n.id).is_err());
        let query_norm = super::neighborhood_norm(&query)?;
        let mut exact = Vec::with_capacity(native.len());
        let mut canonical_row = vec![0u8; canonical_width];
        let mut values = vec![0f32; dimensions];
        let dimensions_capacity = [
            raw_query.capacity(),
            query_bytes.capacity().div_ceil(4),
            values.capacity(),
            canonical_row.capacity().div_ceil(4),
            low.capacity(),
            step.capacity(),
            match &query {
                std::borrow::Cow::Owned(q) => q.capacity(),
                std::borrow::Cow::Borrowed(_) => raw_query.capacity(),
            },
        ]
        .into_iter()
        .max()
        .unwrap_or(0);
        let working = workspace(
            body.capacity() as u64,
            super::plus(
                native.capacity().max(exact.capacity()) as u64,
                c.excluded_ids.capacity() as u64,
            )?,
            dimensions_capacity as u64,
            ranges.capacity() as u64,
            c.max_workspace_bytes,
        )?;
        admitted = admitted.max(with_metadata(
            working,
            super::plus(metadata, product(retained_reports, 2)?)?,
            c.max_workspace_bytes,
        )?);
        // Physical mapping is authenticated by both streamed order hashes and each row ID.
        for n in &native {
            row(
                &canonical.file,
                product(n.ordinal as u64, canonical_width as u64)?,
                &mut canonical_row,
            )?;
            exact.push(canonical_score(
                &canonical_row,
                n,
                &query,
                query_norm,
                &mut values,
            )?);
        }
        exact_order(&mut exact);
        let mut truth_bytes = vec![0u8; k * 8];
        row(
            &truth.file,
            product(s.ordinal as u64, truth_bytes.len() as u64)?,
            &mut truth_bytes,
        )?;
        let truth_ids = truth_bytes
            .chunks_exact(8)
            .map(|b| i64::try_from(u64::from_le_bytes(b.try_into()?)).map_err(Into::into))
            .collect::<Result<Vec<_>>>()?;
        require(
            truth_ids
                .iter()
                .enumerate()
                .all(|(j, id)| !truth_ids[..j].contains(id)),
            "f32-top16 unique truth IDs",
        )?;
        let mut report = query_report(&native, &exact, &truth_ids, k);
        report["ordinal"] = json!(s.ordinal);
        report["native_replay_top10"] = control;
        report["fetched_rows"] = json!(count);
        report["fetched_ranges"] = json!(s.plan.ranges);
        report["bridge_pages_included"] = json!(s.plan.bridge_pages);
        report["checked_workspace_bytes"] = json!(0u64);
        retained_reports = super::plus(retained_reports, value_bytes(&report)?)?;
        admitted = admitted.max(with_metadata(
            working,
            super::plus(metadata, product(retained_reports, 2)?)?,
            c.max_workspace_bytes,
        )?);
        report["checked_workspace_bytes"] = json!(admitted);
        peak = peak.max(admitted);
        reports.push(report);
    }
    for snapshot in [&generation, &canonical, &sq8, &requests, &truth] {
        snapshot.recheck()?;
    }
    require(
        super::file_identity(&super::open_input(&c.completed.input.path)?)? == run.file_identity,
        "f32-top16 completed input changed",
    )?;
    let sum = |key: &str| {
        reports
            .iter()
            .map(|q| q[key].as_u64().unwrap_or(0))
            .sum::<u64>()
    };
    let count = |key: &str| reports.iter().filter(|q| q[key] == true).count();
    let summary = json!({"queries":reports.len(),"recall_denominator":reports.len()*k,
        "native_hits":sum("native_hits"),"reranked_hits":sum("reranked_hits"),
        "all_fetched_canonical_hits":sum("all_fetched_canonical_hits"),
        "canonical_top10_contained":sum("canonical_top10_contained_count"),
        "fully_contained_queries":count("canonical_top10_fully_contained"),
        "shortlist_underfilled_queries":count("shortlist_underfill"),"underfilled_queries":count("underfill"),
        "gained_truth_ids":reports.iter().map(|q|q["gained_truth_ids"].as_array().map_or(0,Vec::len)).sum::<usize>(),
        "lost_truth_ids":reports.iter().map(|q|q["lost_truth_ids"].as_array().map_or(0,Vec::len)).sum::<usize>()});
    Ok(
        json!({"schema":REPORT_SCHEMA,"status":"NATIVE_REPLAY_VERIFIED_COSINE_TOP16_F32",
        "complete":true,"qualified":false,"vendor_or_scientific_win_claim":false,
        "config_path":path,"config_sha256":sha,"config_bytes":config_bytes,
        "completed":c.completed,"generation":c.generation,"canonical":c.canonical,"sq8":c.sq8,
        "requests":c.requests,"truth":c.truth,"ordinals":c.ordinals,"excluded_ids":c.excluded_ids,
        "selection":"current cosine SQ8 score ascending then ID ascending; first 16 unique visible fetched rows; truth unused",
        "canonical_arithmetic":ARITHMETIC,
        "canonical_contract":"actual f64 norm and dot; squared-norm tolerance1e-6; clamp, zero and ID tie rules unchanged",
        "native_score_bits_type":"u32","canonical_score_bits_f64_type":"u64",
        "native_replay_exact_ids_ordinals_scorebits":true,"canonical_order_authenticated":true,
        "sq8_order_authenticated":true,"max_workspace_bytes":c.max_workspace_bytes,
        "peak_checked_workspace_bytes":peak,"summary":summary,"queries":reports,
        "limits":"Offline exact first min(count,32) or full count<=1000 prefix over the SAME sealed fetched ranges including bridges. No global recall, serving plane, latency, f32-arithmetic or vendor claim."}),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use sha2::{Digest, Sha256};
    fn hash(bytes: &[u8]) -> String {
        format!("{:x}", Sha256::digest(bytes))
    }
    fn pin(path: &Path) -> Value {
        let b = std::fs::read(path).unwrap();
        json!({"path":path,"bytes":b.len(),"sha256":hash(&b)})
    }
    fn write_config(path: &Path, c: &Value) -> String {
        let b = serde_json::to_vec(c).unwrap();
        std::fs::write(path, &b).unwrap();
        hash(&b)
    }
    fn fixture(count: usize) -> (tempfile::TempDir, Value) {
        let (dir, result) =
            super::super::scale_native_runner::scale_reducer_native_fixture_count(count, None)
                .unwrap();
        let rows = std::fs::read_to_string(&result)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str::<Value>(line).unwrap())
            .collect::<Vec<_>>();
        let native: Value =
            serde_json::from_slice(&std::fs::read(dir.path().join("config.json")).unwrap())
                .unwrap();
        let c = json!({"schema":"borsuk-cosine-top16-f32-config-v1",
            "completed":{"schema":super::super::SCALE_CONFIG_SCHEMA,"input":pin(&result),
                "expected_identity":rows[0],"expected_bound_inputs":rows[1]},
            "generation":pin(&dir.path().join("generation/manifest.json")),
            "canonical":pin(&dir.path().join("generation/canonical.bin")),
            "sq8":pin(&dir.path().join("sq8")),"requests":native["requests"],"truth":native["truth"],
            "ordinals":(0..count).collect::<Vec<_>>(),"excluded_ids":[],"max_workspace_bytes":512*1024*1024u64});
        (dir, c)
    }
    #[test]
    fn sealed_cosine_control_and_canonical_correction() {
        let (dir, c) = fixture(2);
        let path = dir.path().join("top16.json");
        let sha = write_config(&path, &c);
        let report = reduce(&path, &sha).unwrap();
        assert_eq!(report["queries"].as_array().unwrap().len(), 2);
        assert_eq!(report["native_replay_exact_ids_ordinals_scorebits"], true);
        assert_eq!(report["canonical_order_authenticated"], true);
        assert_eq!(
            report["canonical_arithmetic"],
            "FP32 coordinates / existing sequential-f64 canonical oracle"
        );
    }
    #[test]
    fn cosine_not_l2_top16_independent_correction_containment_ties_and_tails() {
        use borsuk::returned_sq8::rank_returned_ranges;
        let mut body = Vec::new();
        let ids = (100i64..116).rev().chain([7]).collect::<Vec<_>>();
        for id in &ids {
            let (x, y) = if *id == 7 { (4u8, 1u8) } else { (1, 0) };
            body.extend(id.to_le_bytes());
            let norm = (f32::from(x) * 0.25).powi(2) + (f32::from(y) * 0.25).powi(2);
            body.extend(norm.to_le_bytes());
            body.extend([x, y]);
        }
        let ranges = [ReturnedRange {
            start: 3 * 14,
            bytes: &body,
        }];
        let geometry = Sq8Geometry {
            rows: 23,
            dimensions: 2,
        };
        let cosine = score_returned_ranges_cosine_excluding(
            geometry,
            &ranges,
            &[1., 0.],
            &[0., 0.],
            &[0.25, 0.25],
            body.len(),
            &[],
        )
        .unwrap();
        let l2 = rank_returned_ranges(
            geometry,
            &ranges,
            &[1., 0.],
            &[0., 0.],
            &[0.25, 0.25],
            17,
            body.len(),
        )
        .unwrap();
        assert_eq!(
            cosine.iter().take(16).map(|s| s.id).collect::<Vec<_>>(),
            (100..116).collect::<Vec<_>>()
        );
        assert_eq!(l2[0].id, 7);
        assert!(!l2.iter().take(16).any(|s| s.id == 115));
        assert_eq!((cosine[0].id, cosine[0].ordinal), (100, 18));
        let mut exact = Vec::new();
        let mut values = [0f32; 2];
        for n in &cosine {
            let coordinates = if n.id == 7 || n.id == 115 {
                [1f32, 0.]
            } else {
                [0., 1.]
            };
            let mut bytes = n.id.to_le_bytes().to_vec();
            for f in coordinates {
                bytes.extend(f.to_le_bytes());
            }
            exact.push(canonical_score(&bytes, n, &[1., 0.], 1., &mut values).unwrap());
        }
        exact_order(&mut exact);
        let report = query_report(&cosine, &exact, &[115, 7], 10);
        let expected = [115, 100, 101, 102, 103, 104, 105, 106, 107, 108];
        assert_eq!(
            report["reranked_top10"]
                .as_array()
                .unwrap()
                .iter()
                .map(|s| s["id"].as_i64().unwrap())
                .collect::<Vec<_>>(),
            expected
        );
        assert_eq!(report["native_hits"], 0);
        assert_eq!(report["reranked_hits"], 1);
        assert_eq!(report["gained_truth_ids"], json!([115]));
        assert_eq!(report["canonical_top10_contained_count"], 9);
        assert_eq!(
            report["canonical_top10_missing_from_cosine_top16_ids"],
            json!([7])
        );
        let changed_truth = query_report(&cosine, &exact, &[100, 101], 10);
        for key in [
            "native_top10",
            "cosine_top16",
            "reranked_top10",
            "all_fetched_canonical_top10",
        ] {
            assert_eq!(report[key], changed_truth[key], "truth affected {key}");
        }
        let selected = super::super::sq8_cosine::Selected {
            ordinal: 0,
            returned: cosine
                .iter()
                .take(10)
                .map(|n| super::super::Hit {
                    id: n.id as u64,
                    score_bits: u64::from(n.score.to_bits()),
                })
                .collect(),
            returned_physical_ordinals: cosine.iter().take(10).map(|n| n.ordinal).collect(),
            plan: super::super::sq8_cosine::Plan {
                ranges: vec![[42, 280]],
                bridge_pages: 1,
            },
        };
        replay_matches(&cosine, &selected, 10).unwrap();
        assert!(replay_matches(&l2, &selected, 10).is_err());
        let filtered = score_returned_ranges_cosine_excluding(
            geometry,
            &ranges,
            &[1., 0.],
            &[0., 0.],
            &[0.25, 0.25],
            body.len(),
            &[100],
        )
        .unwrap();
        assert!(filtered.iter().all(|n| n.id != 100));
        let mut nonunit = cosine[0].id.to_le_bytes().to_vec();
        nonunit.extend(2f32.to_le_bytes());
        nonunit.extend(0f32.to_le_bytes());
        assert!(canonical_score(&nonunit, &cosine[0], &[1., 0.], 1., &mut values).is_err());
        nonunit[8..12].copy_from_slice(&f32::NAN.to_le_bytes());
        assert!(canonical_score(&nonunit, &cosine[0], &[1., 0.], 1., &mut values).is_err());
        let mut invalid = body.clone();
        invalid[8..12].copy_from_slice(&0f32.to_le_bytes());
        assert!(
            score_returned_ranges_cosine_excluding(
                geometry,
                &[ReturnedRange {
                    start: 42,
                    bytes: &invalid
                }],
                &[1., 0.],
                &[0., 0.],
                &[0.25, 0.25],
                invalid.len(),
                &[115]
            )
            .is_err()
        );
        let underfill = query_report(&cosine[..3], &exact, &[115, 7], 10);
        assert_eq!(underfill["underfill"], true);
        assert_eq!(underfill["shortlist_underfill"], true);
    }
    #[test]
    fn sealed_authentication_replay_pins_exclusions_and_capacity_refusals() {
        let (dir, mut c) = fixture(2);
        let path = dir.path().join("top16.json");
        let sha = write_config(&path, &c);
        let report = reduce(&path, &sha).unwrap();
        assert!(reduce(&path, &"0".repeat(64)).is_err());
        c["unexpected"] = json!(true);
        let bad = write_config(&path, &c);
        assert!(reduce(&path, &bad).is_err());
        c.as_object_mut().unwrap().remove("unexpected");
        let modeled = report["peak_checked_workspace_bytes"].as_u64().unwrap();
        c["max_workspace_bytes"] = json!(modeled - 1);
        let bad = write_config(&path, &c);
        assert!(
            reduce(&path, &bad)
                .unwrap_err()
                .to_string()
                .contains("workspace cap")
        );
        c["max_workspace_bytes"] = json!(modeled);
        let exact_sha = write_config(&path, &c);
        reduce(&path, &exact_sha).unwrap();
        let (mut parsed, bytes): (Config, _) =
            super::super::read_config(&path, &exact_sha).unwrap();
        parsed.excluded_ids.reserve_exact(4096);
        assert!(parsed.excluded_ids.capacity() > parsed.excluded_ids.len());
        assert!(
            reduce_config(parsed, &path, &exact_sha, bytes)
                .unwrap_err()
                .to_string()
                .contains("workspace cap")
        );
        c["max_workspace_bytes"] = json!(512 * 1024 * 1024u64);
        c["excluded_ids"] = json!([20]);
        let sha = write_config(&path, &c);
        let excluded = reduce(&path, &sha).unwrap();
        assert!(
            excluded["queries"][0]["cosine_top16"]
                .as_array()
                .unwrap()
                .iter()
                .all(|n| n["id"] != 20)
        );
        c["excluded_ids"] = json!([20, 20]);
        let bad = write_config(&path, &c);
        assert!(reduce(&path, &bad).is_err());
        c["excluded_ids"] = json!([]);
        let result = std::path::PathBuf::from(c["completed"]["input"]["path"].as_str().unwrap());
        let original = std::fs::read(&result).unwrap();
        let mut rows = std::str::from_utf8(&original)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str::<Value>(line).unwrap())
            .collect::<Vec<_>>();
        let q = rows.iter_mut().find(|q| q["phase"] == "query").unwrap();
        q["returned"][0]["score_bits"] =
            json!(q["returned"][0]["score_bits"].as_u64().unwrap() + 1);
        super::super::tests::authenticate(&mut rows);
        std::fs::write(&result, super::super::tests::encode(&rows)).unwrap();
        c["completed"]["input"] = pin(&result);
        let bad = write_config(&path, &c);
        assert!(
            reduce(&path, &bad)
                .unwrap_err()
                .to_string()
                .contains("exact native")
        );
        std::fs::write(&result, &original).unwrap();
        c["completed"]["input"] = pin(&result);
        let sha = write_config(&path, &c);
        std::fs::write(&result, &original[..original.len() - 1]).unwrap();
        assert!(reduce(&path, &sha).is_err());
        std::fs::write(&result, &original).unwrap();
        let canonical = std::path::PathBuf::from(c["canonical"]["path"].as_str().unwrap());
        let bytes = std::fs::read(&canonical).unwrap();
        let mut changed = bytes.clone();
        changed[0] ^= 1;
        std::fs::write(&canonical, &changed).unwrap();
        assert!(reduce(&path, &sha).is_err());
        std::fs::write(&canonical, &bytes).unwrap();
        let output = dir.path().join("report.json");
        assert!(
            super::super::execute_report(&output, REPORT_SCHEMA, || reduce(&path, &sha)).unwrap()
        );
        let saved = std::fs::read(&output).unwrap();
        assert!(
            super::super::execute_report(&output, REPORT_SCHEMA, || panic!("occupied reduced"))
                .is_err()
        );
        assert_eq!(std::fs::read(output).unwrap(), saved);
        let other = dir.path().join("canonical-link");
        std::os::unix::fs::symlink(&canonical, &other).unwrap();
        c["canonical"]["path"] = json!(other);
        let bad = write_config(&path, &c);
        assert!(reduce(&path, &bad).is_err());
    }
    #[test]
    fn exact_first32_full_prefix_selection_and_nested_report_charge() {
        let (dir, mut c) = fixture(35);
        let path = dir.path().join("top16-full.json");
        let sha = write_config(&path, &c);
        let report = reduce(&path, &sha).unwrap();
        assert_eq!(report["queries"].as_array().unwrap().len(), 35);
        assert_eq!(report["summary"]["recall_denominator"], 350);
        assert!(value_bytes(&report["queries"]).unwrap() > 0);
        c["ordinals"] = json!((0..32).collect::<Vec<_>>());
        let sha = write_config(&path, &c);
        assert_eq!(
            reduce(&path, &sha).unwrap()["queries"]
                .as_array()
                .unwrap()
                .len(),
            32
        );
        for ordinals in [
            json!((0..31).collect::<Vec<_>>()),
            json!((0..33).collect::<Vec<_>>()),
            json!((1..35).collect::<Vec<_>>()),
        ] {
            c["ordinals"] = ordinals;
            let bad = write_config(&path, &c);
            assert!(reduce(&path, &bad).is_err());
        }
    }
    #[test]
    fn report_cap_positive_over2_exact16_and_refusal() {
        let dir = tempfile::tempdir().unwrap();
        for (label, size, accepted) in [
            ("over2", 2 * 1024 * 1024 + 1, true),
            ("exact16", 16 * 1024 * 1024, true),
            ("over16", 16 * 1024 * 1024 + 1, false),
        ] {
            let mut r = json!({"schema":REPORT_SCHEMA,"status":"NATIVE_REPLAY_VERIFIED_COSINE_TOP16_F32",
                "complete":true,"qualified":false,"padding":""});
            let overhead = serde_json::to_vec(&r).unwrap().len() + 1;
            r["padding"] = json!("x".repeat(size - overhead));
            let output = dir.path().join(label);
            let result = super::super::execute_report(&output, REPORT_SCHEMA, || Ok(r));
            if accepted {
                assert!(result.unwrap());
                assert_eq!(std::fs::metadata(output).unwrap().len(), size as u64);
            } else {
                assert!(result.unwrap_err().to_string().contains("report byte cap"));
                let invalid: Value =
                    serde_json::from_slice(&std::fs::read(&output).unwrap()).unwrap();
                assert_eq!(invalid["status"], "INVALID");
                assert_eq!(invalid["complete"], false);
            }
        }
    }
    // Root invokes this exact test with a fresh owned path; production has no export seam.
    #[test]
    fn export_cli_fixture() {
        let Some(destination) = std::env::var_os("BORSUK_F32_TOP16_FIXTURE_DIR") else {
            return;
        };
        let destination = std::path::PathBuf::from(destination);
        assert!(destination.is_absolute());
        std::fs::create_dir(&destination).unwrap();
        let (_dir, mut c) = fixture(2);
        for (key, name) in [
            ("generation", "generation.json"),
            ("canonical", "canonical.bin"),
            ("sq8", "sq8"),
            ("requests", "requests.f32"),
            ("truth", "truth.u64"),
        ] {
            std::fs::copy(c[key]["path"].as_str().unwrap(), destination.join(name)).unwrap();
            c[key] = pin(&destination.join(name));
        }
        std::fs::copy(
            c["completed"]["input"]["path"].as_str().unwrap(),
            destination.join("native.jsonl"),
        )
        .unwrap();
        c["completed"]["input"] = pin(&destination.join("native.jsonl"));
        let path = destination.join("config.json");
        let sha = write_config(&path, &c);
        reduce(&path, &sha).unwrap();
        std::fs::write(destination.join("config.sha256"), format!("{sha}\n")).unwrap();
        println!(
            "ROOT_FIXTURE_CONFIG={} ROOT_FIXTURE_CONFIG_SHA={sha}",
            path.display()
        );
    }
    #[test]
    fn publication_failures_invalidate_original_and_preserve_replacements() {
        use std::io;
        for fault in [
            "file_sync",
            "dir_sync",
            "file_replace",
            "dir_replace",
            "parent_replace",
        ] {
            let dir = tempfile::tempdir().unwrap();
            let parent = dir.path().join("work");
            std::fs::create_dir(&parent).unwrap();
            let output = parent.join("report.json");
            let owned = dir.path().join("owned.json");
            let moved_parent = dir.path().join("owned-parent");
            let mut replaced = false;
            let result = super::super::execute_report_with_sync(
                &output,
                REPORT_SCHEMA,
                || {
                    Ok(
                        json!({"schema":REPORT_SCHEMA,"status":"NATIVE_REPLAY_VERIFIED_COSINE_TOP16_F32","complete":true,"qualified":false}),
                    )
                },
                |file, directory| {
                    if (fault == "file_sync" && !directory) || (fault == "dir_sync" && directory) {
                        return Err(io::Error::other("injected publication sync failure"));
                    }
                    if !replaced
                        && ((fault == "file_replace" && !directory)
                            || (fault == "dir_replace" && directory)
                            || (fault == "parent_replace" && directory))
                    {
                        if fault == "parent_replace" {
                            std::fs::rename(&parent, &moved_parent)?;
                            std::fs::create_dir(&parent)?;
                        } else {
                            std::fs::rename(&output, &owned)?;
                        }
                        std::fs::write(&output, b"foreign replacement")?;
                        replaced = true;
                    }
                    file.sync_all()
                },
            );
            assert!(result.is_err(), "{fault}");
            let invalid_path = if fault == "parent_replace" {
                moved_parent.join("report.json")
            } else if replaced {
                owned.clone()
            } else {
                output.clone()
            };
            let invalid: Value =
                serde_json::from_slice(&std::fs::read(invalid_path).unwrap()).unwrap();
            assert_eq!(invalid["status"], "INVALID", "{fault}");
            assert_eq!(invalid["complete"], false, "{fault}");
            if replaced {
                assert_eq!(
                    std::fs::read(&output).unwrap(),
                    b"foreign replacement",
                    "{fault}"
                );
            }
        }
    }
}
