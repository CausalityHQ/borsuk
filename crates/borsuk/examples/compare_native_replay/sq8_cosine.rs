//! Offline first32 SQ8 cosine falsifier over authenticated sealed native ranges.
use super::fetched_f32::{authenticate, floats, row, workspace};
use super::{CompletedConfig, CompletedInput, Result, require};
use borsuk::{
    exact_sq8_nominee::{ScoredNominee, Sq8Geometry},
    returned_sq8::{ReturnedRange, rank_returned_ranges, score_returned_ranges_cosine_excluding},
    two_bit_generation::normalize_two_bit_diagnostic_query,
};
use serde::Deserialize;
use serde_json::{Value, json};
use std::path::Path;

pub(super) const REPORT_SCHEMA: &str = "borsuk-sq8-cosine-falsifier-v1";
const CONFIG_SCHEMA: &str = "borsuk-sq8-cosine-falsifier-config-v1";

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    schema: String,
    completed: CompletedConfig,
    generation: CompletedInput,
    sq8: CompletedInput,
    requests: CompletedInput,
    truth: CompletedInput,
    ordinals: Vec<usize>,
    // Immutable visibility roster, authenticated by the complete config SHA.
    excluded_ids: Vec<i64>,
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
fn number(v: &Value) -> Result<u64> {
    v.as_u64()
        .ok_or_else(|| "sq8-cosine unsigned integer".into())
}
fn product(a: u64, b: u64) -> Result<u64> {
    a.checked_mul(b)
        .ok_or_else(|| "sq8-cosine size overflow".into())
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
            && k > 0
            && k <= rows
            && count > 0
            && i["metric"] == "cosine"
            && i["serving"] == json!({"mode":"baseline"})
            && i["source_layout"] == json!({"layout":"flat_records"})
            && g["schema"] == "borsuk-two-bit-generation-v12"
            && c.generation.sha256 == i["generation_root_sha256"]
            && g["canonical"]["rows"] == rows
            && g["canonical"]["dimensions"] == d
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
        "sq8-cosine generation/input authority",
    )?;
    require(
        c.ordinals
            .iter()
            .copied()
            .eq(0..usize::try_from(count.min(32))?)
            && c.excluded_ids.windows(2).all(|ids| ids[0] < ids[1]),
        "sq8-cosine first32 and sorted unique visibility roster",
    )?;
    workspace(0, c.excluded_ids.len() as u64, d, 0, c.max_workspace_bytes)?;
    Ok((
        usize::try_from(rows)?,
        usize::try_from(d)?,
        usize::try_from(k)?,
    ))
}
fn replay_matches(native: &[ScoredNominee], s: &Selected, k: usize) -> Result<()> {
    require(
        s.returned.len() == k
            && s.returned_physical_ordinals.len() == k
            && native.len() >= k
            && native
                .iter()
                .take(k)
                .zip(&s.returned)
                .zip(&s.returned_physical_ordinals)
                .all(|((a, b), o)| {
                    u64::try_from(a.id).ok() == Some(b.id)
                        && a.ordinal == *o
                        && a.score.to_bits() == b.score_bits
                }),
        "sq8-cosine exact native IDs/ordinals/scorebits: attribution refused",
    )
}
#[derive(Clone, Copy)]
struct Oracle {
    id: i64,
    ordinal: usize,
    score: f64,
}
// Literal decode and f64 coordinate accumulation; no candidate dot/norm reuse.
fn oracle(
    bytes: &[u8],
    ordinal: usize,
    query: &[f32],
    low: &[f32],
    step: &[f32],
) -> Result<Oracle> {
    require(bytes.len() == query.len() + 12, "sq8-cosine oracle width")?;
    let id = i64::from_le_bytes(bytes[..8].try_into()?);
    let stored = f32::from_le_bytes(bytes[8..12].try_into()?);
    let (mut decoded_norm, mut qnorm, mut norm, mut dot) = (0f32, 0f64, 0f64, 0f64);
    for d in 0..query.len() {
        let decoded = low[d] + f32::from(bytes[12 + d]) * step[d];
        require(decoded.is_finite(), "sq8-cosine finite decoded coordinate")?;
        decoded_norm += decoded * decoded;
        let x = f64::from(decoded);
        let q = f64::from(query[d]);
        qnorm += q * q;
        norm += x * x;
        dot += q * x;
    }
    require(
        stored.is_finite() && stored > 0. && decoded_norm.to_bits() == stored.to_bits(),
        "sq8-cosine decoded stored norm bits",
    )?;
    require(
        qnorm.is_finite() && qnorm > 0. && norm.is_finite() && norm > 0.,
        "sq8-cosine oracle norms",
    )?;
    let ratio = dot / (qnorm.sqrt() * norm.sqrt());
    require(ratio.is_finite(), "sq8-cosine oracle finite ratio")?;
    Ok(Oracle {
        id,
        ordinal,
        score: 1. - ratio.clamp(-1., 1.),
    })
}
fn differences(a: &[i64], b: &[i64]) -> Vec<i64> {
    a.iter().copied().filter(|id| !b.contains(id)).collect()
}
pub(super) fn reduce(path: &Path, sha: &str) -> Result<Value> {
    let (c, config_bytes): (Config, _) = super::read_config(path, sha)?;
    let (g, generation) = super::header_json(&c.generation)?;
    let (rows, dimensions, k) = bind(&c, &g)?;
    let mut selected = Vec::with_capacity(c.ordinals.len());
    // Validate complete query, recall, seal, terminal and EOF evidence before payload scoring.
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
        "sq8-cosine complete first32 roster",
    )?;
    let low: Vec<f32> = serde_json::from_value(g["low"].clone())?;
    let step: Vec<f32> = serde_json::from_value(g["step"].clone())?;
    require(
        low.len() == dimensions
            && step.len() == dimensions
            && low.iter().all(|v| v.is_finite())
            && step.iter().all(|v| v.is_finite() && *v > 0.),
        "sq8-cosine calibration",
    )?;
    let width = dimensions.checked_add(12).ok_or("sq8-cosine width")?;
    let order_sha = c.completed.expected_bound_inputs["native_order_sha256"]
        .as_str().ok_or("sq8-cosine order SHA")?;
    let sq8 = authenticate(&c.sq8, Some((width, order_sha)))?;
    let requests = authenticate(&c.requests, None)?;
    let truth = authenticate(&c.truth, None)?;
    let mut reports = Vec::with_capacity(selected.len());
    for s in &selected {
        let count_bytes = s.plan.ranges.iter().try_fold(0u64, |sum, [start, end]| {
            require(
                start < end
                    && start % width as u64 == 0
                    && end % width as u64 == 0
                    && *end <= c.sq8.bytes,
                "sq8-cosine bounded aligned range",
            )?;
            super::plus(sum, end - start)
        })?;
        let count = usize::try_from(count_bytes / width as u64)?;
        let admitted = workspace(
            count_bytes,
            super::plus(count as u64, c.excluded_ids.len() as u64)?,
            dimensions as u64,
            s.plan.ranges.len() as u64,
            c.max_workspace_bytes,
        )?;
        require(count >= k, "sq8-cosine fetched underfill")?;
        let mut body = vec![0u8; usize::try_from(count_bytes)?];
        let mut offset = 0;
        for [start, end] in &s.plan.ranges {
            let len = usize::try_from(end - start)?;
            row(&sq8.file, *start, &mut body[offset..offset + len])?;
            offset += len;
        }
        offset = 0;
        let mut ranges = Vec::with_capacity(s.plan.ranges.len());
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
        let mut query = vec![0f32; dimensions];
        floats(&query_bytes, &mut query)?;
        let query = normalize_two_bit_diagnostic_query(&query).map_err(|e| e.to_string())?;
        let geometry = Sq8Geometry { rows, dimensions };
        let mut native =
            rank_returned_ranges(geometry, &ranges, &query, &low, &step, count, body.len())
                .map_err(|e| format!("native control: {e:?}"))?;
        replay_matches(&native, s, k)?;
        // Candidate/oracle rankings are created only after exact native control.
        let mut exact = Vec::with_capacity(count);
        for range in &ranges {
            for (slot, bytes) in range.bytes.chunks_exact(width).enumerate() {
                exact.push(oracle(
                    bytes,
                    range.start / width + slot,
                    &query,
                    &low,
                    &step,
                )?);
            }
        }
        let candidate = score_returned_ranges_cosine_excluding(
            geometry,
            &ranges,
            &query,
            &low,
            &step,
            body.len(),
            &c.excluded_ids,
        )
        .map_err(|e| format!("cosine candidate: {e:?}"))?;
        native.retain(|s| c.excluded_ids.binary_search(&s.id).is_err());
        exact.retain(|s| c.excluded_ids.binary_search(&s.id).is_err());
        exact.sort_by(|a, b| a.score.total_cmp(&b.score).then(a.id.cmp(&b.id)));
        let ids =
            |scores: &[ScoredNominee]| scores.iter().take(k).map(|s| s.id).collect::<Vec<_>>();
        let old = ids(&native);
        let new = ids(&candidate);
        let reference = exact.iter().take(k).map(|s| s.id).collect::<Vec<_>>();
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
            "sq8-cosine unique truth IDs",
        )?;
        let hits = |ids: &[i64]| truth_ids.iter().filter(|id| ids.contains(id)).count();
        let gained = differences(&new, &old);
        let displaced = differences(&old, &new);
        let top = |scores: &[ScoredNominee]| {
            scores.iter().take(k).map(|s|
            json!({"id":s.id,"physical_ordinal":s.ordinal,"score_bits":s.score.to_bits()})).collect::<Vec<_>>()
        };
        reports.push(json!({"ordinal":s.ordinal,"fetched_rows":count,"visible_rows":candidate.len(),
            "fetched_ranges":s.plan.ranges,"bridge_pages_included":s.plan.bridge_pages,
            "checked_workspace_bytes":admitted,"native_hits":hits(&old),"cosine_hits":hits(&new),
            "oracle_hits":hits(&reference),"truth_ids":truth_ids,
            "gained_truth_ids":gained.iter().filter(|id|truth_ids.contains(id)).collect::<Vec<_>>(),
            "displaced_truth_ids":displaced.iter().filter(|id|truth_ids.contains(id)).collect::<Vec<_>>(),
            "gained_ids":gained,"displaced_ids":displaced,
            "oracle_boundary_gained_ids":differences(&reference,&new),
            "oracle_boundary_displaced_ids":differences(&new,&reference),
            "oracle_order_disagreement":new != reference,
            "native_top":top(&native),"cosine_top":top(&candidate),
            "oracle_top":exact.iter().take(k).map(|s|json!({"id":s.id,"physical_ordinal":s.ordinal,
                "cosine":s.score,"score_bits_f64":s.score.to_bits()})).collect::<Vec<_>>(),
            "cosine_cutoff_ties":candidate.get(k-1).map(|cut|candidate.iter().filter(|s|s.score==cut.score).count()),
            "oracle_cutoff_ties":exact.get(k-1).map(|cut|exact.iter().filter(|s|s.score==cut.score).count()),
            "cosine_k_to_next_margin":candidate.get(k).map(|s|f64::from(s.score)-f64::from(candidate[k-1].score)),
            "oracle_k_to_next_margin":exact.get(k).map(|s|s.score-exact[k-1].score),
            "underfill":candidate.len()<k}));
    }
    for snapshot in [&generation, &sq8, &requests, &truth] {
        snapshot.recheck()?;
    }
    require(
        super::file_identity(&super::open_input(&c.completed.input.path)?)? == run.file_identity,
        "sq8-cosine completed input changed",
    )?;
    Ok(
        json!({"schema":REPORT_SCHEMA,"status":"NATIVE_REPLAY_VERIFIED_SQ8_COSINE",
        "complete":true,"qualified":false,"config_path":path,"config_sha256":sha,"config_bytes":config_bytes,
        "completed":c.completed,"generation":c.generation,"sq8":c.sq8,"requests":c.requests,"truth":c.truth,
        "ordinals":c.ordinals,"excluded_ids":c.excluded_ids,"exclusions_authority":"complete config SHA256; both rankings after unfiltered native control",
        "native_replay_exact_ids_ordinals_scorebits":true,"decoded_stored_norm_bits_verified":true,"sq8_order_authenticated":true,
        "max_workspace_bytes":c.max_workspace_bytes,"queries":reports,
        "candidate":"1-clamp((sequential_f32_code_weight_dot+unshifted_affine_offset)/(sqrt(query_norm2)*sqrt(stored_decoded_norm2)),-1,1)",
        "oracle":"literal f32 decoded coordinates; independent sequential f64 dot and squared norms; cosine distance",
        "limits":"First32 sealed fetched ranges including bridges only. No canonical payload, global recall, serving qualification or vendor claim."}),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use sha2::{Digest, Sha256};
    use std::path::PathBuf;
    fn hash(bytes: &[u8]) -> String {
        format!("{:x}", Sha256::digest(bytes))
    }
    fn pin(path: PathBuf) -> Value {
        let bytes = std::fs::read(&path).unwrap();
        json!({"path":path,"bytes":bytes.len(),"sha256":hash(&bytes)})
    }
    fn config(path: &Path, c: &Value) -> String {
        let bytes = serde_json::to_vec(c).unwrap();
        std::fs::write(path, &bytes).unwrap();
        hash(&bytes)
    }
    #[test]
    fn sq8_cosine_actual_tiny_sealed_end_to_end_and_tampering() {
        let (dir, result) =
            super::super::scale_native_runner::scale_reducer_native_fixture(false, false).unwrap();
        let original = std::fs::read(&result).unwrap();
        let rows = std::str::from_utf8(&original)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str::<Value>(line).unwrap())
            .collect::<Vec<_>>();
        let native: Value =
            serde_json::from_slice(&std::fs::read(dir.path().join("config.json")).unwrap())
                .unwrap();
        let mut c = json!({"schema":CONFIG_SCHEMA,
            "completed":{"schema":super::super::SCALE_CONFIG_SCHEMA,"input":pin(result.clone()),
                "expected_identity":rows[0],"expected_bound_inputs":rows[1]},
            "generation":pin(dir.path().join("generation/manifest.json")),
            "sq8":pin(dir.path().join("sq8")),"requests":native["requests"],"truth":native["truth"],
            "ordinals":[0,1],"excluded_ids":[],"max_workspace_bytes":512*1024*1024u64});
        let path = dir.path().join("cosine-config.json");
        let sha = config(&path, &c);
        let report = reduce(&path, &sha).unwrap();
        assert_eq!(report["queries"].as_array().unwrap().len(), 2);
        assert_eq!(report["native_replay_exact_ids_ordinals_scorebits"], true);
        assert_eq!(report["decoded_stored_norm_bits_verified"], true);
        let ids = report["queries"][0]["cosine_top"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v["id"].as_i64().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(ids, [20, 0, 1, 2, 3, 4, 5, 6, 7, 8]);
        let output = dir.path().join("cosine-report.json");
        assert!(
            super::super::execute_report(&output, REPORT_SCHEMA, || reduce(&path, &sha)).unwrap()
        );
        assert!(
            super::super::execute_report(&output, REPORT_SCHEMA, || reduce(&path, &sha)).is_err()
        );
        assert!(reduce(&path, &"0".repeat(64)).is_err());
        // Exclusions are a separate, SHA-pinned shared visibility operation.
        c["excluded_ids"] = json!([20]);
        let excluded_sha = config(&path, &c);
        let excluded = reduce(&path, &excluded_sha).unwrap();
        assert!(
            excluded["queries"][0]["cosine_top"]
                .as_array()
                .unwrap()
                .iter()
                .all(|s| s["id"] != 20)
        );
        assert!(reduce(&path, &sha).is_err());
        c["excluded_ids"] = json!([20, 20]);
        let bad_sha = config(&path, &c);
        assert!(reduce(&path, &bad_sha).is_err());
        c["excluded_ids"] = json!([]);
        for ordinals in [json!([1]), json!([1, 0]), json!([0, 0])] {
            c["ordinals"] = ordinals;
            let bad_sha = config(&path, &c);
            assert!(reduce(&path, &bad_sha).is_err());
        }
        c["ordinals"] = json!([0, 1]);
        // A completely re-sealed native score mutation must still fail exact replay.
        let mut wrong = rows.clone();
        let q = wrong.iter_mut().find(|r| r["phase"] == "query").unwrap();
        q["returned"][0]["score_bits"] =
            json!(q["returned"][0]["score_bits"].as_u64().unwrap() + 1);
        super::super::tests::authenticate(&mut wrong);
        let encoded = super::super::tests::encode(&wrong);
        std::fs::write(&result, &encoded).unwrap();
        c["completed"]["input"] = pin(result.clone());
        let bad_sha = config(&path, &c);
        assert!(reduce(&path, &bad_sha).is_err());
        // Complete result authentication refuses original-pin body mutation and truncation.
        c["completed"]["input"] =
            json!({"path":result,"bytes":original.len(),"sha256":hash(&original)});
        let original_sha = config(&path, &c);
        assert!(reduce(&path, &original_sha).is_err());
        std::fs::write(&result, &original[..original.len() - 1]).unwrap();
        assert!(reduce(&path, &original_sha).is_err());
        std::fs::write(&result, &original).unwrap();
        let sq8_path = dir.path().join("sq8");
        let mut bytes = std::fs::read(&sq8_path).unwrap();
        bytes[8] ^= 1;
        std::fs::write(&sq8_path, &bytes).unwrap();
        assert!(reduce(&path, &original_sha).is_err());
    }
    #[test]
    fn literal_oracle_norm_bits_nonunit_and_boundary_disagreement() {
        let mut row = 7i64.to_le_bytes().to_vec();
        row.extend_from_slice(&1.0625f32.to_le_bytes());
        row.extend_from_slice(&[8, 5]);
        let value = oracle(&row, 3, &[1., 0.], &[-1., -1.], &[0.25, 0.25]).unwrap();
        assert_eq!((value.id, value.ordinal), (7, 3));
        assert_eq!(value.score, 1. - 1. / 1.0625f64.sqrt());
        row[8] ^= 1;
        assert!(oracle(&row, 3, &[1., 0.], &[-1., -1.], &[0.25, 0.25]).is_err());
        assert_eq!(differences(&[41, 7], &[90, 41]), [7]);
        assert_eq!(differences(&[90, 41], &[41, 7]), [90]);
    }
}
