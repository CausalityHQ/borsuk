//! Strict reader for native authenticated FP32 serving evidence.
use super::{Result, require};
use serde_json::{Value, json};
pub(super) const REPORT_SCHEMA: &str = "borsuk-fp32-rerank-serving-report-v1";
fn number(v: &Value) -> Result<u64> {
    v.as_u64().ok_or_else(|| "FP32 unsigned integer".into())
}
fn fields(v: &Value, names: &str) -> Result<()> {
    let o = v.as_object().ok_or("FP32 object")?;
    require(
        o.len() == names.split_whitespace().count()
            && names.split_whitespace().all(|n| o.contains_key(n)),
        "FP32 exact fields",
    )
}
pub(super) fn enabled(v: &Value) -> bool {
    v["serving"]["mode"] == "fp32_rerank"
}
pub(super) fn limits(v: &Value) -> Result<borsuk::fp32_rerank::Fp32RerankLimits> {
    let v = &v["serving"];
    let mut names = String::from("mode shortlist max_fp32_bytes max_fp32_gets max_parallel_fp32_gets");
    if v.get("coalesce").is_some() { names.push_str(" coalesce"); }
    if v.get("direct").is_some() { names.push_str(" direct"); }
    fields(v, &names)?;
    let limits = borsuk::fp32_rerank::Fp32RerankLimits {
            direct: v.get("direct").filter(|v| !v.is_null()).map(|v| serde_json::from_value(v.clone())).transpose()?,
        coalesce: v.get("coalesce").map(|v| v.as_bool().ok_or("FP32 coalesce boolean")).transpose()?.unwrap_or(false),
        shortlist: usize::try_from(number(&v["shortlist"])?)?,
        max_bytes: usize::try_from(number(&v["max_fp32_bytes"])?)?,
        max_gets: usize::try_from(number(&v["max_fp32_gets"])?)?,
        max_parallel_gets: usize::try_from(number(&v["max_parallel_fp32_gets"])?)?,
    };
    require(
        v["mode"] == "fp32_rerank"
            && limits.shortlist >= super::K
            && limits.shortlist <= 256
            && limits.max_gets > 0
            && (limits.coalesce || limits.max_gets >= limits.shortlist)
            && limits.max_parallel_gets > 0,
        "FP32 explicit limits",
    )?;
    if let Some(direct) = limits.direct {
        require(direct.max_sq8_bytes > 0 && (1..=128).contains(&direct.max_sq8_gets), "FP32 direct limits")?;
    }
    Ok(limits)
}
pub(super) fn direct(v: &Value) -> bool {
    enabled(v) && v["serving"]["direct"].is_object()
}
pub(super) fn sq8_bytes(v: &Value) -> Result<u64> {
    number(if direct(v) { &v["serving"]["direct"]["max_sq8_bytes"] } else { &v["sq8_byte_budget"] })
}
pub(super) fn sq8_gets(v: &Value) -> Result<u64> {
    number(if direct(v) { &v["serving"]["direct"]["max_sq8_gets"] } else { &v["sq8_get_budget"] })
}
pub(super) fn validate_query(raw: &Value, inputs: &Value) -> Result<()> {
    let limits = limits(inputs)?;
    require(
        raw["serving"] == inputs["serving"],
        "FP32 query selector binding",
    )?;
    let d = usize::try_from(number(&inputs["dimensions"])?)?;
    let rows = number(&inputs["rows"])?;
    let stage = &raw["canonical_stage"];
    fields(stage, "score_precision wall_ns record_bytes shortlist ranges covered_records bridge_records")?;
    let width = d.checked_mul(4).and_then(|n| n.checked_add(8)).ok_or("FP32 record overflow")?;
    require(
        stage["score_precision"] == "f64"
            && stage["record_bytes"] == width
            && number(&stage["wall_ns"])? > 0
            && number(&stage["wall_ns"])? <= number(&raw["query_wall_ns"])?,
        "FP32 stage scalar/geometry/time",
    )?;
    let shortlist = stage["shortlist"].as_array().ok_or("FP32 shortlist")?;
    let fetched_rows = raw["plan"]["ranges"].as_array().ok_or("FP32 SQ8 ranges")?
        .iter().try_fold(0_u64, |n, r| -> Result<u64> {
            let bytes = number(&r[1])?.checked_sub(number(&r[0])?).ok_or("FP32 SQ8 range")?;
            require(bytes % (d as u64 + 12) == 0, "FP32 SQ8 record width")?;
            super::plus(n, bytes / (d as u64 + 12))
        })?;
    require(shortlist.len() == limits.shortlist.min(usize::try_from(fetched_rows)?),
        "FP32 complete fetched shortlist")?;
    let mut ids = std::collections::BTreeSet::new();
    let mut ordinals = std::collections::BTreeSet::new();
    let mut previous: Option<(f32, u64)> = None;
    for row in shortlist {
        fields(row, "id ordinal sq8_score_bits")?;
        let id = number(&row["id"])?;
        let ordinal = number(&row["ordinal"])?;
        let score = f32::from_bits(u32::try_from(number(&row["sq8_score_bits"])?)?);
        require(
            id < rows
                && ordinal < rows
                && ids.insert(id)
                && ordinals.insert(ordinal)
                && score.is_finite(),
            "FP32 nominee identity/score",
        )?;
        if let Some((old_score, old_id)) = previous {
            require(
                f32::total_cmp(&old_score, &score)
                    .then(old_id.cmp(&id))
                    .is_le(),
                "FP32 SQ8 shortlist order",
            )?;
        }
        previous = Some((score, id));
        let width = (d + 12) as u64;
        require(
            raw["plan"]["ranges"]
                .as_array()
                .ok_or("FP32 SQ8 ranges")?
                .iter()
                .any(|r| {
                    r[0].as_u64().is_some_and(|first| first <= ordinal * width)
                        && r[1]
                            .as_u64()
                            .is_some_and(|last| last >= (ordinal + 1) * width)
                }),
            "FP32 nominee outside fetched population",
        )?;
    }
    let returned = raw["returned"].as_array().ok_or("FP32 returned")?;
    let returned_ordinals = raw["returned_physical_ordinals"]
        .as_array()
        .ok_or("FP32 ordinals")?;
    require(
        returned.len() == returned_ordinals.len(),
        "FP32 returned ordinal length",
    )?;
    for (hit, ordinal) in returned.iter().zip(returned_ordinals) {
        require(
            shortlist
                .iter()
                .any(|n| n["id"] == hit["id"] && n["ordinal"] == *ordinal),
            "FP32 returned row absent from authenticated shortlist",
        )?;
        let score = f64::from_bits(number(&hit["score_bits"])?);
        require(
            score.is_finite() && (0.0..=2.0).contains(&score),
            "FP32 finite cosine bits",
        )?;
    }
    let charge = &raw["charges"]["canonical"];
    fields(charge, "submitted_gets verified_bytes failed_gets")?;
    let ordinals = ordinals.into_iter().map(usize::try_from).collect::<std::result::Result<Vec<_>,_>>()?;
    let ranges = borsuk::fp32_rerank::plan_ranges(&ordinals, usize::try_from(rows)?, d, limits)
        .ok_or("FP32 canonical plan caps/geometry")?;
    let bytes = ranges.iter().try_fold(0_usize, |n,r| n.checked_add(r.len()))
        .ok_or("FP32 byte overflow")?;
    require(
        stage["ranges"] == json!(ranges.iter().map(|r| [r.start,r.end]).collect::<Vec<_>>())
            && stage["covered_records"] == bytes / width
            && stage["bridge_records"] == bytes / width - shortlist.len()
            && charge["submitted_gets"] == ranges.len()
            && charge["verified_bytes"] == bytes
            && charge["failed_gets"] == 0,
        "FP32 exact covered record/range charge binding",
    )?;
    Ok(())
}
pub(super) fn validate_startup(raw: &Value, inputs: &Value) -> Result<()> {
    let limits = limits(inputs)?;
    let rows = number(&inputs["rows"])?;
    let startup = &raw["fp32_rerank"];
    fields(
        startup,
        "limits modeled_memory_bytes open transport score_precision",
    )?;
    require(
        startup["limits"] == serde_json::to_value(limits)? && startup["score_precision"] == "f64",
        "FP32 startup limits/precision",
    )?;
    fields(
        &startup["open"],
        "head_requests head_wall_ns row_digest_bytes",
    )?;
    require(
        startup["open"]["head_requests"] == 1
            && number(&startup["open"]["head_wall_ns"])? > 0
            && startup["open"]["row_digest_bytes"]
                == rows.checked_mul(32).ok_or("FP32 sidecar overflow")?,
        "FP32 canonical HEAD/sidecar charge",
    )?;
    require(number(&startup["modeled_memory_bytes"])? <= number(&raw["library_cap_bytes"])?,
        "FP32 model cap")
}
pub(super) fn validate_model(raw: &Value, inputs: &Value, authority: &Value) -> Result<()> {
    let u = |key: &str| -> Result<usize> { Ok(usize::try_from(number(&inputs[key])?)?) };
    let metadata = raw["metadata"]["metadata"].as_array().ok_or("FP32 metadata")?
        .iter().try_fold(0_u64, |n, e| super::plus(n, number(&e["bytes"])?))?;
    let limits = borsuk::two_bit_generation::TwoBitGenerationLimits {
        max_memory_bytes: number(&inputs["max_memory_bytes"])?,
        max_active_queries: 1,
        max_query_bytes: u("sq8_byte_budget")?, max_query_gets: u("sq8_get_budget")?,
        max_parallel_gets: u("fetch_parallelism")?,
        max_source_bytes: u("source_byte_budget")?, max_source_gets: u("source_get_budget")?,
        max_parallel_source_gets: u("fetch_parallelism")?,
        max_query_scratch_bytes: usize::try_from(number(&raw["query_scratch_bytes"])?)?,
        already_pinned_bytes: number(&raw["caller_pinned_bytes"])?,
    };
    let policy = match inputs.get("source_authentication") {
        None => borsuk::sq8_s3_range::SourceAuthentication::Inline,
        Some(v) => serde_json::from_value(v.clone())?,
    };
    let model = borsuk::fp32_rerank::modeled_remote_memory(
        authority["root_manifest"].as_str().ok_or("FP32 root bytes")?.as_bytes(),
        metadata, limits, self::limits(inputs)?, policy,
        serde_json::from_value(inputs["sq8_range_split"].clone())?,
        raw.get("shared_byte_window_state_bytes").map(number).transpose()?.unwrap_or(0).try_into()?,
        super::current_v10::semantic_leaf_limit(inputs)?,
    )?;
    let model = super::plus(model, super::source_cache::charged_bytes(inputs)?)?;
    require(raw["fp32_rerank"]["modeled_memory_bytes"] == model,
        "FP32 complete generation/query/pins model")
}

pub(super) fn reduce(path: &std::path::Path, sha: &str) -> Result<Value> {
    let (config, _): (super::CompletedConfig, _) = super::read_config(path, sha)?;
    require(
        enabled(&config.expected_identity) && enabled(&config.expected_bound_inputs),
        "FP32 completed selector",
    )?;
    let run = super::read_run_with(&config.input.path, &config.input.sha256, Some(&config))?;
    let mut charges = super::Charges::default();
    let mut hits = 0_u64;
    let mut wall = 0_u64;
    for sample in &run.samples {
        charges.add(sample.charges)?;
        hits = super::plus(hits, sample.hits10)?;
        wall = super::plus(wall, sample.wall_ns)?;
    }
    Ok(
        json!({"schema":REPORT_SCHEMA,"valid":true,"input_sha256":config.input.sha256,
        "queries":run.samples.len(),"recall_numerator":hits,"recall_denominator":run.samples.len()*super::K,
        "charges":charges,"sum":charges.sum()?,"query_wall_ns":wall,
        "score_precision":"f64","physical_s3_measured":false,"external_gate_required":true}),
    )
}
