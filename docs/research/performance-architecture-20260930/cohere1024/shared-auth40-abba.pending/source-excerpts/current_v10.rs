crates/borsuk/examples/compare_native_replay/current_v10.rs:143:
crates/borsuk/examples/compare_native_replay/current_v10.rs:144: fn source_authentication_charge(inputs: &Value) -> Result<Option<u64>> {
crates/borsuk/examples/compare_native_replay/current_v10.rs:145:     use borsuk::sq8_s3_range::SourceAuthentication;
crates/borsuk/examples/compare_native_replay/current_v10.rs:146:     let Some(value) = inputs.get("source_authentication") else {
crates/borsuk/examples/compare_native_replay/current_v10.rs:147:         return Ok(None);
crates/borsuk/examples/compare_native_replay/current_v10.rs:148:     };
crates/borsuk/examples/compare_native_replay/current_v10.rs:149:     let policy = match value.as_str() {
crates/borsuk/examples/compare_native_replay/current_v10.rs:150:         Some("inline") => SourceAuthentication::Inline,
crates/borsuk/examples/compare_native_replay/current_v10.rs:151:         Some("offload2") => SourceAuthentication::Offload2,
crates/borsuk/examples/compare_native_replay/current_v10.rs:152:         _ => return Err("SOURCE authentication selector".into()),
crates/borsuk/examples/compare_native_replay/current_v10.rs:153:     };
crates/borsuk/examples/compare_native_replay/current_v10.rs:154:     require(
crates/borsuk/examples/compare_native_replay/current_v10.rs:155:         cpu_profile_config(inputs) && inputs["range_scheduler_owner"] == "library",
crates/borsuk/examples/compare_native_replay/current_v10.rs:156:         "SOURCE authentication versioned library scheduler",
crates/borsuk/examples/compare_native_replay/current_v10.rs:157:     )?;
crates/borsuk/examples/compare_native_replay/current_v10.rs:158:     Ok(Some(
crates/borsuk/examples/compare_native_replay/current_v10.rs:159:         policy
crates/borsuk/examples/compare_native_replay/current_v10.rs:160:             .additional_memory_bytes(
crates/borsuk/examples/compare_native_replay/current_v10.rs:161:                 usize::try_from(number(&inputs["fetch_parallelism"])?)?,
crates/borsuk/examples/compare_native_replay/current_v10.rs:162:                 usize::try_from(number(&inputs["source_get_budget"])?)?,
crates/borsuk/examples/compare_native_replay/current_v10.rs:163:                 1,
crates/borsuk/examples/compare_native_replay/current_v10.rs:164:             )
crates/borsuk/examples/compare_native_replay/current_v10.rs:165:             .ok_or("SOURCE authentication memory overflow")?,
crates/borsuk/examples/compare_native_replay/current_v10.rs:166:     ))
crates/borsuk/examples/compare_native_replay/current_v10.rs:167: }
crates/borsuk/examples/compare_native_replay/current_v10.rs:168:
crates/borsuk/examples/compare_native_replay/current_v10.rs:169: fn validate_source_authentication_charge(raw: &Value, inputs: &Value) -> Result<()> {
crates/borsuk/examples/compare_native_replay/current_v10.rs:170:     match source_authentication_charge(inputs)? {
crates/borsuk/examples/compare_native_replay/current_v10.rs:171:         Some(bytes) => require(
crates/borsuk/examples/compare_native_replay/current_v10.rs:172:             raw.get("source_authentication_extra_library_bytes") == Some(&json!(bytes)),
crates/borsuk/examples/compare_native_replay/current_v10.rs:173:             "SOURCE authentication exact library reservation",
crates/borsuk/examples/compare_native_replay/current_v10.rs:174:         ),
crates/borsuk/examples/compare_native_replay/current_v10.rs:175:         None => require(
crates/borsuk/examples/compare_native_replay/current_v10.rs:176:             raw.get("source_authentication_extra_library_bytes")
crates/borsuk/examples/compare_native_replay/current_v10.rs:177:                 .is_none(),
crates/borsuk/examples/compare_native_replay/current_v10.rs:178:             "historical SOURCE authentication reservation absent",
crates/borsuk/examples/compare_native_replay/current_v10.rs:179:         ),
crates/borsuk/examples/compare_native_replay/current_v10.rs:180:     }
crates/borsuk/examples/compare_native_replay/current_v10.rs:181: }
