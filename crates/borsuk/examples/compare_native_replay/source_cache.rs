//! Independent replay of the generation-local authenticated SOURCE cache.
use super::{Result, fields, plus, require};
use serde_json::{Value, json};
use std::collections::BTreeSet;

fn number(v: &Value) -> Result<u64> {
    v.as_u64()
        .ok_or_else(|| "SOURCE cache unsigned integer".into())
}

pub(super) fn budget(inputs: &Value) -> Result<usize> {
    let Some(value) = inputs.get("source_cache_bytes") else {
        return Ok(0);
    };
    let bytes = usize::try_from(number(value)?)?;
    require(
        bytes > 0
            && inputs["config_schema"] == super::current_v10::FRAMED_CONFIG_SCHEMA
            && super::fp32_serving::enabled(inputs)
            && !super::fp32_serving::direct(inputs)
            && inputs["source_layout"] == json!({"layout":"flat_records"})
            && inputs.get("max_active_queries").is_none()
            && inputs["execution"]["trace"] != true,
        "SOURCE cache requires explicit serial untraced flat FP32 identity",
    )?;
    Ok(bytes)
}

pub(super) fn validate_counters(v: &Value) -> Result<()> {
    fields(
        v,
        "charged_bytes capacity_bytes occupied_bytes hits misses evictions insertions",
    )?;
    for value in v.as_object().ok_or("SOURCE cache counters")?.values() {
        number(value)?;
    }
    Ok(())
}

struct Model {
    block: usize,
    object: usize,
    slots: usize,
    charged: usize,
}
fn model(inputs: &Value) -> Result<Model> {
    let cap = budget(inputs)?;
    let rows = usize::try_from(number(&inputs["rows"])?)?;
    let width = super::source_record_bytes(usize::try_from(number(&inputs["dimensions"])?)?)?;
    require((1..=1_000_000).contains(&rows), "SOURCE cache row bound")?;
    let block = width
        .checked_mul(256)
        .ok_or("SOURCE cache block overflow")?;
    let object = rows
        .checked_mul(width)
        .ok_or("SOURCE cache object overflow")?;
    let gets = usize::try_from(number(&inputs["source_get_budget"])?)?;
    // Frozen x86_64 ABI, checked against private native layouts by the library test:
    // Option<Mutex<SourceCache>>=112, VerifiedRange=40, pair=16, Vec=24.
    require(
        cfg!(target_pointer_width = "64"),
        "SOURCE cache producer ABI",
    )?;
    let query_metadata = gets
        .min(rows.div_ceil(256))
        .checked_mul(56)
        .and_then(|n| n.checked_add(2 * (24 + 64)))
        .ok_or("SOURCE cache query model")?;
    let fixed = 112 + 2 * 64 + query_metadata;
    let slot = block.checked_add(8).ok_or("SOURCE cache slot overflow")?;
    let slots = cap
        .checked_sub(fixed)
        .ok_or("SOURCE cache minimum budget")?
        .checked_div(slot)
        .ok_or("SOURCE cache slot size")?
        .min(rows.div_ceil(256));
    require(slots > 0, "SOURCE cache requires one slot")?;
    let charged = slots
        .checked_mul(slot)
        .and_then(|n| n.checked_add(fixed))
        .ok_or("SOURCE cache model overflow")?;
    Ok(Model {
        block,
        object,
        slots,
        charged,
    })
}
pub(super) fn charged_bytes(inputs: &Value) -> Result<u64> {
    if budget(inputs)? == 0 {
        Ok(0)
    } else {
        Ok(model(inputs)?.charged as u64)
    }
}

pub(super) struct Replay {
    model: Model,
    tags: Vec<Option<usize>>,
    occupied: usize,
    hits: u64,
    misses: u64,
    evictions: u64,
    insertions: u64,
}
impl Replay {
    pub(super) fn open(inputs: &Value, startup: &Value) -> Result<Option<Self>> {
        if budget(inputs)? == 0 {
            require(
                startup.get("source_cache").is_none(),
                "unconfigured SOURCE cache startup",
            )?;
            return Ok(None);
        }
        let model = model(inputs)?;
        let replay = Self {
            tags: vec![None; model.slots],
            model,
            occupied: 0,
            hits: 0,
            misses: 0,
            evictions: 0,
            insertions: 0,
        };
        replay.counters(&startup["source_cache"])?;
        Ok(Some(replay))
    }
    fn counters(&self, actual: &Value) -> Result<()> {
        require(
            *actual
                == json!({
            "charged_bytes":self.model.charged,
            "capacity_bytes":self.model.slots * self.model.block,
            "occupied_bytes":self.occupied,
            "hits":self.hits,"misses":self.misses,
            "evictions":self.evictions,"insertions":self.insertions}),
            "SOURCE cache exact replayed capacity/counters",
        )
    }
    fn block_len(&self, block: usize) -> usize {
        self.model
            .block
            .min(self.model.object - block * self.model.block)
    }
    pub(super) fn query(&mut self, raw: &Value, inputs: &Value) -> Result<()> {
        let rows = usize::try_from(number(&inputs["rows"])?)?;
        let width = self.model.object / rows;
        let mut pages = BTreeSet::new();
        let mut previous = None;
        for unit in raw["trace"]["semantic_units"]
            .as_array()
            .ok_or("SOURCE semantic units")?
        {
            let unit = usize::try_from(number(unit)?)?;
            require(
                unit < rows.div_ceil(32) && previous.is_none_or(|old| old < unit),
                "SOURCE cache ordered bounded semantic units",
            )?;
            previous = Some(unit);
            pages.insert(unit / 8);
        }
        let ranked = raw["trace"]["ranked_candidate_pages"]
            .as_array()
            .ok_or("SOURCE cache ranked closure")?;
        let ranked_set = ranked
            .iter()
            .map(|v| Ok(usize::try_from(number(v)?)?))
            .collect::<Result<BTreeSet<_>>>()?;
        require(
            !pages.is_empty() && ranked.len() == pages.len() && ranked_set == pages,
            "SOURCE cache original closure identity",
        )?;
        let (ranges, full_bytes) = super::source_cover::cover_pages(
            &pages,
            rows,
            width,
            256,
            usize::try_from(number(&inputs["source_get_budget"])?)?,
        )
        .map_err(|e| format!("SOURCE cache independent cover: {e:?}"))?;
        require(full_bytes as u64 <= number(&inputs["source_byte_budget"])?,
            "SOURCE cache original full-cover byte budget")?;
        let mut misses = Vec::new();
        // Native lookup completes for every original range before the successful batch inserts.
        for range in ranges {
            let first = range.start / self.model.block;
            let end = range.end.div_ceil(self.model.block);
            if (first..end).all(|block| self.tags[block % self.tags.len()] == Some(block)) {
                self.hits = plus(self.hits, 1)?;
            } else {
                self.misses = plus(self.misses, 1)?;
                misses.push(range);
            }
        }
        let bytes = misses
            .iter()
            .try_fold(0_u64, |n, r| plus(n, r.len() as u64))?;
        require(
            raw["charges"]["source"]
                == json!({"submitted_gets":misses.len(),
            "verified_bytes":bytes,"failed_gets":0}),
            "SOURCE cache exact independently reconstructed transport",
        )?;
        if !raw["transport"]["before"].is_null() {
            let before = &raw["transport"]["before"];
            let after = &raw["transport"]["after"];
            let mut gets = 0;
            let mut payload = 0;
            for key in ["source", "sq8", "canonical"] {
                gets = plus(gets, number(&raw["charges"][key]["submitted_gets"])?)?;
                payload = plus(payload, number(&raw["charges"][key]["verified_bytes"])?)?;
            }
            require(number(&after["method_counts"][0])?.checked_sub(number(&before["method_counts"][0])?)
                    .is_some_and(|delta| delta >= gets)
                && number(&after["consumed_payload_bytes"])?.checked_sub(number(&before["consumed_payload_bytes"])?)
                    .is_some_and(|delta| delta >= payload),
                "SOURCE cache SDK query conservation")?;
        }
        for range in misses {
            for block in range.start / self.model.block..range.end.div_ceil(self.model.block) {
                let slot = block % self.tags.len();
                if self.tags[slot] == Some(block) {
                    continue;
                }
                if let Some(old) = self.tags[slot] {
                    self.occupied -= self.block_len(old);
                    self.evictions = plus(self.evictions, 1)?;
                }
                self.tags[slot] = Some(block);
                self.occupied += self.block_len(block);
                self.insertions = plus(self.insertions, 1)?;
            }
        }
        self.counters(&raw["source_cache"])
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    fn inputs(cap: usize) -> Value {
        json!({"config_schema":super::super::current_v10::FRAMED_CONFIG_SCHEMA,
            "serving":{"mode":"fp32_rerank","shortlist":16,"max_fp32_bytes":576,"max_fp32_gets":16,"max_parallel_fp32_gets":3},
            "source_layout":{"layout":"flat_records"},"source_cache_bytes":cap,
            "rows":513,"dimensions":7,"source_get_budget":3,"source_byte_budget":100000})
    }
    fn row(replay: &Replay, units: Value, pages: Value, gets: u64, bytes: u64,
        hits: u64, misses: u64, evictions: u64, insertions: u64, occupied: usize) -> Value {
        json!({"trace":{"semantic_units":units,"ranked_candidate_pages":pages},
            "charges":{"source":{"submitted_gets":gets,"verified_bytes":bytes,"failed_gets":0}},
            "source_cache":{"charged_bytes":replay.model.charged,"capacity_bytes":replay.model.slots*replay.model.block,
            "occupied_bytes":occupied,"hits":hits,"misses":misses,"evictions":evictions,"insertions":insertions}})
    }
    fn replay(inputs: &Value) -> Replay {
        let model = model(inputs).unwrap();
        Replay { tags:vec![None;model.slots],model,occupied:0,hits:0,misses:0,evictions:0,insertions:0 }
    }
    #[test]
    fn fp32_source_cache_513_tail_collision_lookup_before_insert() {
        let full = model(&inputs(10000)).unwrap();
        let inputs = inputs(full.charged - (full.slots - 1) * (full.block + 8));
        let mut replay = replay(&inputs);
        assert_eq!(replay.model.slots,1);
        let width = replay.model.object/513;
        let first = row(&replay,json!([0,16]),json!([0,2]),2,(257*width) as u64,0,2,1,2,width);
        replay.query(&first,&inputs).unwrap();
        let second = row(&replay,json!([0,16]),json!([0,2]),1,(256*width) as u64,1,3,2,3,256*width);
        replay.query(&second,&inputs).unwrap();
    }
    #[test]
    fn fp32_source_cache_warm_full_cover_budget_refusal() {
        let mut inputs = inputs(10000);
        let mut replay = replay(&inputs);
        let width = replay.model.object/513;
        let first = row(&replay,json!([0]),json!([0]),1,(256*width) as u64,0,1,0,1,256*width);
        replay.query(&first,&inputs).unwrap();
        let second = row(&replay,json!([16]),json!([2]),1,width as u64,0,2,0,2,257*width);
        replay.query(&second,&inputs).unwrap();
        inputs["source_byte_budget"] = json!(256*width);
        let combined = row(&replay,json!([0,16]),json!([0,2]),0,0,2,2,0,2,257*width);
        let error = replay.query(&combined,&inputs).unwrap_err().to_string();
        assert!(error.contains("original full-cover byte budget"), "{error}");
        assert_eq!(replay.hits,0);
    }
}
