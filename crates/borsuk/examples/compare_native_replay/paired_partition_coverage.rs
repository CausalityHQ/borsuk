//! Private SOURCE diagnostic. Original authority never comes from derived publications.
use super::*;
use borsuk::semantic_unit_router::{
    LeafPartition, RouterArtifacts, SemanticProfile, SemanticUnitRouter, SourceIdentity,
    admit_authenticated_root, build_with_partition, preflight, validate_publication,
};

const PREP_CONFIG: &str = "borsuk-spatial-publication-config-v1";
const CONSTRUCTION_APPROVAL: &str = "borsuk-paired-construction-authority-v1";
const SCREEN_CONFIG: &str = "borsuk-paired-source-coverage-config-v1";
const SCREEN_APPROVAL: &str = "borsuk-paired-source-authority-v1";
const PREP_RECEIPT: &str = "borsuk-spatial-publication-receipt-v1";
const REPORT_SCHEMA: &str = "borsuk-paired-source-coverage-v1";
const OWNED_CAP: u64 = 1024 * 1024 * 1024;
const STREAM: u64 = 16 * 1024;
const RESERVE: u64 = 1024 * 1024;

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Original {
    schema: String,
    generation_sha256: String,
    generation: u64,
    base_epoch: u64,
    plane_sha256: String,
    discovery_sha256: String,
    input_schema: String,
    input_root_sha256: String,
    centroids_sha256: String,
    profile: SemanticProfile,
    rows: usize,
    dimensions: usize,
}

impl Original {
    fn identity(&self) -> SourceIdentity<'_> {
        SourceIdentity {
            profile: self.profile,
            schema: &self.input_schema,
            root_sha256: &self.input_root_sha256,
            centroids_sha256: &self.centroids_sha256,
            rows: self.rows,
            dimensions: self.dimensions,
        }
    }
    fn centroid_bytes(&self) -> Result<u64> {
        plus(
            32,
            mul(
                mul(self.rows.div_ceil(32) as u64, self.dimensions as u64)?,
                2,
            )?,
        )
    }
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Constructor {
    config: CompletedInput,
    archive: CompletedInput,
    binary: CompletedInput,
    implementation: Value,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ConstructionApproval {
    schema: String,
    original: Original,
    original_approval: CompletedInput,
    constructor: Constructor,
    reconstruction: CompletedInput,
    radial_parity: CompletedInput,
    qualification: CompletedInput,
    corrected_closure: CompletedInput,
    expected_reconstruction: Value,
    expected_radial_parity: Value,
    expected_qualification: Value,
    expected_corrected_closure: Value,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ConstructionInputs {
    generation_manifest: CompletedInput,
    plane_manifest: CompletedInput,
    centroids: CompletedInput,
    max_owned_bytes: u64,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct PrepConfig {
    schema: String,
    inputs: ConstructionInputs,
    output_directory: PathBuf,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Publication {
    root: CompletedInput,
    membership: CompletedInput,
    leaves: CompletedInput,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Producer {
    result: CompletedInput,
    config: CompletedInput,
    archive: CompletedInput,
    binary: CompletedInput,
    expected_config: Value,
    expected_identity: Value,
    expected_bound_inputs: Value,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ScreenApproval {
    schema: String,
    construction: ConstructionApproval,
    producer: Producer,
    radial: Publication,
    spatial: Publication,
    spatial_receipt: CompletedInput,
    expected_spatial_receipt: Value,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ScreenConfig {
    schema: String,
    inputs: ConstructionInputs,
    radial: Publication,
    spatial: Publication,
    spatial_receipt: CompletedInput,
    requests: CompletedInput,
    sq8: CompletedInput,
    truth: CompletedInput,
}

#[derive(Debug)]
struct ResourceFailure(String);
impl std::fmt::Display for ResourceFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}
impl std::error::Error for ResourceFailure {}

fn mul(a: u64, b: u64) -> Result<u64> {
    a.checked_mul(b)
        .ok_or_else(|| "paired ownership overflow".into())
}

// Inputs cannot understate ownership; every phase is reserved before its allocations.
#[derive(Serialize)]
struct Ledger {
    cap: u64,
    metadata: u64,
    phases: std::collections::BTreeMap<String, u64>,
}
impl Ledger {
    fn new(cap: u64, metadata: u64) -> Result<Self> {
        require(cap > 0 && cap <= OWNED_CAP, "paired owned cap range")?;
        let mut ledger = Self {
            cap,
            metadata,
            phases: Default::default(),
        };
        ledger.phase("metadata", &[])?;
        Ok(ledger)
    }
    fn phase(&mut self, name: &str, parts: &[u64]) -> Result<u64> {
        let total = parts.iter().try_fold(self.metadata, |n, &p| plus(n, p))?;
        if total > self.cap {
            return Err(Box::new(ResourceFailure(format!(
                "{name} owns {total} > {}",
                self.cap
            ))));
        }
        self.phases.insert(name.into(), total);
        Ok(total)
    }
    fn remaining(&self, retained: &[u64]) -> Result<usize> {
        let used = retained
            .iter()
            .try_fold(self.metadata, |n, &p| plus(n, p))?;
        usize::try_from(
            self.cap
                .checked_sub(used)
                .ok_or_else(|| Box::new(ResourceFailure("retained import budget".into())))?,
        )
        .map_err(Into::into)
    }
}

fn digest(body: &[u8]) -> String {
    format!("{:x}", Sha256::digest(body))
}

fn same_pin(a: &CompletedInput, b: &CompletedInput) -> Result<()> {
    require(
        a.bytes == b.bytes && a.sha256 == b.sha256,
        "independent byte/hash pin",
    )
}

// Historical descriptors may contain no local path. The local descriptor is opened separately.
fn matches_descriptor(v: &Value, pin: &CompletedInput) -> Result<()> {
    if v.get("path").is_some() {
        fields(v, "path bytes sha256")?;
        header_path(Path::new(v["path"].as_str().ok_or("descriptor path type")?))?;
    } else {
        fields(v, "bytes sha256")?;
    }
    require(
        v["bytes"].as_u64() == Some(pin.bytes) && v["sha256"] == pin.sha256,
        "receipt byte/hash descriptor",
    )
}

fn distinct(snapshots: &[HeaderSnapshot]) -> Result<()> {
    let mut seen = BTreeSet::new();
    for s in snapshots {
        s.recheck()?;
        require(
            seen.insert((s.identity.dev, s.identity.ino)),
            "paired role alias",
        )?;
    }
    Ok(())
}

fn small<T: DeserializeOwned>(path: &Path, sha: &str) -> Result<(T, HeaderSnapshot)> {
    // Fixed bootstrap bound precedes parsing the caller's cap and all owned documents.
    let _bootstrap = Ledger::new(
        OWNED_CAP,
        plus(mul(64, 2 * HEADER_JSON_CAP)?, RESERVE + 2 * STREAM)?,
    )?;
    let file = header_file(path)?;
    let identity = file_identity(&file)?;
    let (v, _) = read_config_file(&file, path, sha, HEADER_JSON_CAP)?;
    let stamp = HeaderSnapshot {
        file,
        path: path.to_owned(),
        identity,
    };
    stamp.recheck()?;
    Ok((v, stamp))
}

fn binary(pin: &CompletedInput, bytes: u64) -> Result<(Vec<u8>, HeaderSnapshot)> {
    header_pin(pin, bytes)?;
    require(pin.bytes == bytes, "paired binary exact geometry")?;
    let mut file = header_file(&pin.path)?;
    let identity = file_identity(&file)?;
    require(identity.len == bytes, "paired binary length")?;
    let mut body = vec![0_u8; usize::try_from(bytes)?];
    file.read_exact(&mut body)?;
    require(
        file.read(&mut [0])? == 0 && digest(&body) == pin.sha256,
        "paired binary SHA/EOF",
    )?;
    let stamp = HeaderSnapshot {
        file,
        path: pin.path.clone(),
        identity,
    };
    stamp.recheck()?;
    Ok((body, stamp))
}

fn stream(pin: &CompletedInput, cap: u64) -> Result<HeaderSnapshot> {
    header_pin(pin, cap)?;
    let mut file = header_file(&pin.path)?;
    let identity = file_identity(&file)?;
    require(identity.len == pin.bytes, "paired streamed length")?;
    let mut hash = Sha256::new();
    let mut block = [0_u8; STREAM as usize];
    let mut remaining = pin.bytes;
    while remaining > 0 {
        let n = usize::try_from(remaining.min(STREAM))?;
        file.read_exact(&mut block[..n])?;
        hash.update(&block[..n]);
        remaining -= n as u64;
    }
    require(
        file.read(&mut block[..1])? == 0 && format!("{:x}", hash.finalize()) == pin.sha256,
        "paired whole-body SHA/EOF",
    )?;
    let stamp = HeaderSnapshot {
        file,
        path: pin.path.clone(),
        identity,
    };
    stamp.recheck()?;
    Ok(stamp)
}

fn approved_json(
    pin: &CompletedInput,
    expected: &Value,
    stamps: &mut Vec<HeaderSnapshot>,
) -> Result<Value> {
    let (v, s) = header_json(pin)?;
    require(&v == expected, "independently approved complete JSON")?;
    stamps.push(s);
    Ok(v)
}

fn metadata_charge(c: &ConstructionInputs, a: &ConstructionApproval, extra: u64) -> Result<u64> {
    // 64x covers parser/typed/Value copies; the fixed reserve covers paths and receipts.
    let mut bytes = 2 * HEADER_JSON_CAP;
    for p in [
        &c.generation_manifest,
        &c.plane_manifest,
        &a.original_approval,
        &a.constructor.config,
        &a.reconstruction,
        &a.radial_parity,
        &a.qualification,
        &a.corrected_closure,
    ] {
        header_pin(p, HEADER_JSON_CAP)?;
        bytes = plus(bytes, p.bytes)?;
    }
    plus(plus(mul(64, plus(bytes, extra)?)?, RESERVE)?, 2 * STREAM)
}

fn authenticate_construction(
    c: &ConstructionInputs,
    a: &ConstructionApproval,
    stamps: &mut Vec<HeaderSnapshot>,
) -> Result<()> {
    let o = &a.original;
    require(
        a.schema == CONSTRUCTION_APPROVAL
            && o.schema == "borsuk-partition-reconstruction-approval-v1"
            && o.profile == SemanticProfile::Scale1m
            && o.profile.valid_geometry(o.rows, o.dimensions)
            && size_of::<usize>() == 8
            && o.generation > 0
            && (1..=256).contains(&o.input_schema.len())
            && [
                &o.generation_sha256,
                &o.plane_sha256,
                &o.discovery_sha256,
                &o.input_root_sha256,
                &o.centroids_sha256,
            ]
            .iter()
            .all(|s| valid_sha(s))
            && c.generation_manifest.sha256 == o.generation_sha256
            && c.plane_manifest.sha256 == o.plane_sha256
            && c.centroids.sha256 == o.centroids_sha256
            && c.centroids.bytes == o.centroid_bytes()?,
        "approved original construction identity",
    )?;
    let (approval, aps) = header_json(&a.original_approval)?;
    require(
        approval == serde_json::to_value(o)?,
        "original approval full body",
    )?;
    stamps.push(aps);
    let (g, gs) = header_json(&c.generation_manifest)?;
    fields(
        &g,
        "schema generation base_epoch plane_manifest_sha256 page_manifest_sha256 discovery sq8_object_sha256 sq8_object_key sq8_etag canonical low step",
    )?;
    require(
        g["schema"] == "borsuk-two-bit-generation-v8"
            && g["generation"].as_u64() == Some(o.generation)
            && g["base_epoch"].as_u64() == Some(o.base_epoch)
            && g["plane_manifest_sha256"] == o.plane_sha256
            && g["page_manifest_sha256"].as_str().is_some_and(valid_sha),
        "original generation/epoch/plane",
    )?;
    let d = &g["discovery"];
    fields(
        d,
        "mode profile root_sha256 root_bytes membership_sha256 membership_bytes leaves_sha256 leaves_bytes input_schema input_root_sha256 centroids_sha256 source_sha256 source_order_sha256 mean_sha256 records_sha256 sq8_sha256",
    )?;
    require(
        d["mode"] == "semantic"
            && d["profile"] == "scale1m"
            && digest(&serde_json::to_vec(d)?) == o.discovery_sha256
            && d["input_schema"] == o.input_schema
            && d["input_root_sha256"] == o.input_root_sha256
            && d["centroids_sha256"] == o.centroids_sha256,
        "original Discovery unchanged",
    )?;
    let units = o.rows.div_ceil(32) as u64;
    for (role, exact, cap) in [
        ("root", None, o.profile.root_cap() as u64),
        ("membership", Some(mul(units, 4)?), mul(units, 4)?),
        (
            "leaves",
            Some(mul(units, plus(4, mul(o.dimensions as u64, 2)?)?)?),
            mul(units, plus(4, mul(o.dimensions as u64, 2)?)?)?,
        ),
    ] {
        let bytes = d[format!("{role}_bytes")]
            .as_u64()
            .ok_or("Discovery integer descriptor")?;
        require(
            bytes > 0
                && bytes <= cap
                && exact.is_none_or(|n| n == bytes)
                && d[format!("{role}_sha256")].as_str().is_some_and(valid_sha),
            "Discovery descriptor",
        )?;
    }
    let (p, ps) = header_json(&c.plane_manifest)?;
    let plane: borsuk::two_bit_source::SourcePlaneReceipt = serde_json::from_value(p)?;
    require(
        plane.schema == "borsuk-two-bit-plane-v3"
            && plane.rows == o.rows
            && plane.dimensions == o.dimensions
            && plane.seed == 20260923
            && plane.page_rows == 32
            && !plane.query_or_truth_used
            && plane.record_bytes == source_record_bytes(o.dimensions)?
            && valid_sha(&plane.page_digest_sha256)
            && g["sq8_object_sha256"] == plane.sq8_sha256
            && g["sq8_object_key"]
                .as_str()
                .is_some_and(|key| neighborhood_object_key(key, &plane.sq8_sha256))
            && g["sq8_etag"].as_str().is_some_and(|s| {
                !s.is_empty() && !s.starts_with("W/") && !s.chars().any(char::is_control)
            }),
        "original source-only plane",
    )?;
    let mut source = json!({});
    for (name, sha) in [
        ("source_sha256", &plane.source_sha256),
        ("source_order_sha256", &plane.source_order_sha256),
        ("mean_sha256", &plane.mean_sha256),
        ("records_sha256", &plane.records_sha256),
        ("sq8_sha256", &plane.sq8_sha256),
    ] {
        require(
            valid_sha(sha) && d[name] == *sha,
            "original plane/Discovery source identity",
        )?;
        source[name] = json!(sha);
    }
    fields(&g["canonical"], "object_key sha256 bytes rows dimensions")?;
    require(
        g["canonical"]["rows"].as_u64() == Some(o.rows as u64)
            && g["canonical"]["dimensions"].as_u64() == Some(o.dimensions as u64)
            && g["canonical"]["bytes"].as_u64()
                == Some(mul(o.rows as u64, plus(8, mul(o.dimensions as u64, 4)?)?)?)
            && g["canonical"]["sha256"].as_str().is_some_and(valid_sha)
            && g["canonical"]["object_key"]
                .as_str()
                .zip(g["canonical"]["sha256"].as_str())
                .is_some_and(|(key, sha)| neighborhood_object_key(key, sha)),
        "original canonical descriptor",
    )?;
    for (name, positive) in [("low", false), ("step", true)] {
        let values: Vec<f32> = serde_json::from_value(g[name].clone())?;
        require(
            values.len() == o.dimensions
                && values
                    .iter()
                    .all(|v| v.is_finite() && (!positive || *v > 0.)),
            "original calibration",
        )?;
    }
    stamps.extend([gs, ps]);
    let (constructor_config, ccs) = header_json(&a.constructor.config)?;
    fields(
        &constructor_config,
        "schema generation_manifest plane_manifest leaves historical caller_pinned_bytes max_payload_bytes output_directory",
    )?;
    require(
        constructor_config["schema"] == "borsuk-partition-reconstruction-config-v1",
        "original constructor config",
    )?;
    for (name, pin) in [
        ("generation_manifest", &c.generation_manifest),
        ("plane_manifest", &c.plane_manifest),
    ] {
        matches_descriptor(&constructor_config[name], pin)?;
    }
    stamps.push(ccs);
    // Archive and ELF are approved ORIGINAL constructor files, never the running consumer.
    stamps.push(stream(&a.constructor.archive, OWNED_CAP)?);
    stamps.push(stream(&a.constructor.binary, 256 * 1024 * 1024)?);
    fields(
        &a.constructor.implementation,
        "example_sha256 module_sha256 router_sha256 generation_sha256 plane_sha256 centroid_sha256 trainer_sha256",
    )?;
    require(
        a.constructor
            .implementation
            .as_object()
            .ok_or("constructor sources")?
            .values()
            .all(|v| v.as_str().is_some_and(valid_sha)),
        "approved constructor code hashes",
    )?;
    let reconstruction = approved_json(&a.reconstruction, &a.expected_reconstruction, stamps)?;
    let parity = approved_json(&a.radial_parity, &a.expected_radial_parity, stamps)?;
    for (receipt, status) in [
        (&reconstruction, "RECONSTRUCTED"),
        (&parity, "HISTORICAL_RADIAL_PARITY"),
    ] {
        fields(
            receipt,
            "schema status complete requests_opened truth_opened historical_root_opened historical_membership_opened config approval generation plane binary implementation original source leaves unit_rows page_rows unit_count final_unit_rows centroids_bytes centroids_sha256 centroids radial_outputs metadata_charge_bytes reconstruction_charge_bytes max_payload_bytes historical_parity requires_successful_file_sync requires_successful_directory_sync requires_original_successful_terminal_receipt",
        )?;
        require(
            receipt["schema"] == "borsuk-partition-reconstruction-receipt-v1"
                && receipt["status"] == status
                && receipt["complete"] == true
                && receipt["requests_opened"] == false
                && receipt["truth_opened"] == false
                && receipt["original"] == approval
                && receipt["source"] == source
                && receipt["implementation"] == a.constructor.implementation
                && receipt["unit_rows"] == 32
                && receipt["page_rows"] == 256
                && receipt["unit_count"] == units
                && receipt["final_unit_rows"] == o.rows - (o.rows.div_ceil(32) - 1) * 32
                && receipt["centroids_bytes"] == c.centroids.bytes
                && receipt["centroids_sha256"] == c.centroids.sha256
                && receipt["requires_successful_file_sync"] == true
                && receipt["requires_successful_directory_sync"] == true
                && receipt["requires_original_successful_terminal_receipt"] == true,
            "native receipt original identity",
        )?;
        for (role, pin) in [
            ("approval", &a.original_approval),
            ("generation", &c.generation_manifest),
            ("plane", &c.plane_manifest),
            ("binary", &a.constructor.binary),
            ("centroids", &c.centroids),
        ] {
            matches_descriptor(&receipt[role], pin)?;
        }
        require(
            receipt["leaves"]["bytes"] == d["leaves_bytes"]
                && receipt["leaves"]["sha256"] == d["leaves_sha256"],
            "original receipt leaf descriptor",
        )?;
    }
    // Parity may have a different config (historical inputs); its full body is independently pinned.
    matches_descriptor(&parity["config"], &a.constructor.config)?;
    let h = &parity["historical_parity"];
    for name in ["root", "membership", "leaves"] {
        require(
            h[format!("old_{name}")]["bytes"] == d[format!("{name}_bytes")]
                && h[format!("old_{name}")]["sha256"] == d[format!("{name}_sha256")],
            "original historical descriptors",
        )?;
    }
    require(
        h["root_exceptions"]
            == json!([
        {"offset":0,"old":"BORSUSR2","new":"BORSUSR3"}, {"offset":88,"old_le_u32":0,"new_le_u32":1}]),
        "exact qualified historical exceptions",
    )?;
    let q = approved_json(&a.qualification, &a.expected_qualification, stamps)?;
    let closure = approved_json(&a.corrected_closure, &a.expected_corrected_closure, stamps)?;
    fields(
        &closure,
        "schema status original_native_exit original_manager_exit original_stage_disposition original_disposition_preserved native_reexecuted original_receipt_sha256 descriptor_validation derived_outputs_full_body_hash_verified derived_outputs_retained_create_only performance_claim",
    )?;
    require(
        closure["schema"] == "borsuk-reconstruction-closed-parity-recovery-v1"
            && closure["status"] == "CLOSED_NATIVE_PARITY_VALIDATED"
            && closure["original_native_exit"] == 0
            && closure["original_manager_exit"] == 1
            && closure["original_stage_disposition"] == "INVALID_RECEIPT_VALIDATOR"
            && closure["original_disposition_preserved"] == true
            && closure["native_reexecuted"] == false
            && closure["original_receipt_sha256"] == a.radial_parity.sha256
            && closure["descriptor_validation"]
                == "exact bytes and SHA; descriptor deliberately has no local path"
            && closure["derived_outputs_full_body_hash_verified"] == true
            && closure["derived_outputs_retained_create_only"] == true
            && closure["performance_claim"] == false,
        "whole corrected closure proof, original failed wrapper preserved",
    )?;
    require(
        q["schema"] == "borsuk-real1m-construction-qualified-v1"
            && q["status"] == "REAL_RECONSTRUCTION_AND_HISTORICAL_RADIAL_PARITY_QUALIFIED"
            && q["elf_sha256"] == a.constructor.binary.sha256
            && q["original_approval"] == approval
            && q["original_generation_sha256"] == o.generation_sha256
            && q["admission"]["native_receipt_sha256"] == a.reconstruction.sha256
            && q["admission"]["all_closed_body_hashes_verified"] == true
            && q["admission"]["original_ssm_success"] == true
            && q["original_parity"]["native_exit"] == 0
            && q["original_parity"]["manager_exit"] == 1
            && q["original_parity"]["ssm_status"] == "Failed"
            && q["original_parity"]["disposition"] == "INVALID_RECEIPT_VALIDATOR"
            && q["original_parity"]["disposition_preserved"] == true
            && q["original_parity"]["receipt_sha256"] == a.radial_parity.sha256
            && q["closed_output_revalidation"]["status"] == "Success"
            && q["closed_output_revalidation"]["exit"] == 0
            && q["closed_output_revalidation"]["native_reexecuted"] == false
            && q["closed_output_revalidation"]["all_recovery_body_checksums_verified"] == true
            && q["requests_opened"] == false
            && q["truth_opened"] == false
            && q["performance_claim"] == false,
        "preserved original INVALID and corrected closure approval",
    )?;
    matches_descriptor(&q["centroids"], &c.centroids)?;
    require(
        q["historical_parity"] == *h,
        "qualified full historical parity",
    )?;
    distinct(stamps)
}

fn publication_pins(p: &Publication, o: &Original) -> Result<()> {
    header_pin(&p.root, o.profile.root_cap() as u64)?;
    header_pin(&p.membership, mul(o.rows.div_ceil(32) as u64, 4)?)?;
    header_pin(
        &p.leaves,
        mul(
            o.rows.div_ceil(32) as u64,
            plus(4, mul(o.dimensions as u64, 2)?)?,
        )?,
    )?;
    require(
        p.membership.bytes == mul(o.rows.div_ceil(32) as u64, 4)?
            && p.leaves.bytes
                == mul(
                    o.rows.div_ceil(32) as u64,
                    plus(4, mul(o.dimensions as u64, 2)?)?,
                )?,
        "derived publication exact capacities",
    )
}

fn same_publication(a: &Publication, b: &Publication) -> Result<()> {
    same_pin(&a.root, &b.root)?;
    same_pin(&a.membership, &b.membership)?;
    same_pin(&a.leaves, &b.leaves)
}

fn group_map(router: &SemanticUnitRouter) -> Result<Vec<u32>> {
    router
        .unit_membership()
        .iter()
        .map(|&leaf| {
            Ok(u32::try_from(
                router
                    .manifest()
                    .leaves
                    .get(leaf)
                    .ok_or("unit group leaf")?
                    .group_ordinal,
            )?)
        })
        .collect()
}

fn compare_groups(router: &SemanticUnitRouter, radial: &[u32]) -> Result<()> {
    require(
        router.unit_membership().len() == radial.len(),
        "paired group-map length",
    )?;
    for (&leaf, &group) in router.unit_membership().iter().zip(radial) {
        require(
            router
                .manifest()
                .leaves
                .get(leaf)
                .ok_or("unit group leaf")?
                .group_ordinal
                == group as usize,
            "paired per-physical-unit group mismatch",
        )?;
    }
    Ok(())
}

fn import(
    p: &Publication,
    o: &Original,
    blob: &[u8],
    expected: LeafPartition,
    map: Option<&[u32]>,
    ledger: &mut Ledger,
    stamps: &mut Vec<HeaderSnapshot>,
) -> Result<Option<Vec<u32>>> {
    publication_pins(p, o)?;
    let map_bytes = mul(o.rows.div_ceil(32) as u64, 4)?;
    ledger.phase(
        "import root authentication",
        &[p.root.bytes, blob.len() as u64, map_bytes],
    )?;
    let (root, rs) = binary(&p.root, p.root.bytes)?;
    // Root authentication/mode admission precedes membership/payload/decoder allocations.
    let available = ledger.remaining(&[blob.len() as u64, map_bytes])?;
    let (mode, modeled) = admit_authenticated_root(&root, &p.root.sha256, &o.identity(), available)
        .map_err(admission_error)?;
    require(mode == expected, "derived publication partition")?;
    ledger.phase(
        "mode-specific import validation",
        &[modeled as u64, blob.len() as u64, map_bytes],
    )?;
    let (membership, ms) = binary(&p.membership, p.membership.bytes)?;
    let (leaves, ls) = binary(&p.leaves, p.leaves.bytes)?;
    ledger.phase(
        "actual imported capacities",
        &[
            modeled as u64,
            blob.len() as u64,
            map_bytes,
            root.capacity().saturating_sub(root.len()) as u64,
            membership.capacity().saturating_sub(membership.len()) as u64,
            leaves.capacity().saturating_sub(leaves.len()) as u64,
        ],
    )?;
    validate_publication(&root, &membership, &leaves, &o.identity(), blob)?;
    let router = SemanticUnitRouter::open(
        &root,
        &membership,
        &p.root.sha256,
        &o.identity(),
        o.profile.root_cap(),
    )?;
    let result = if let Some(map) = map {
        compare_groups(&router, map)?;
        None
    } else {
        Some(group_map(&router)?)
    };
    stamps.extend([rs, ms, ls]);
    distinct(stamps)?;
    Ok(result)
}

// Mirrors the public builder's three phases; the private library cap is never widened.
fn construction_phases(o: &Original, partition: LeafPartition) -> Result<[u64; 3]> {
    let u = o.rows.div_ceil(32) as u64;
    let d = o.dimensions as u64;
    let b = o.centroid_bytes()?;
    let c = u.div_ceil(64);
    let l = mul(2, c)?.checked_sub(1).ok_or("construction geometry")?;
    let f = plus(mul(mul(u, d)?, 4)?, mul(u, 4)?)?;
    let v = plus(
        mul(mul(u, d)?, 4)?,
        mul(u, (size_of::<Vec<f32>>() + 64) as u64)?,
    )?;
    let i = mul(c, plus(mul(d, 4)?, 128)?)?;
    let r = mul(
        (c - 1).div_ceil(31),
        plus(plus(mul(mul(32, d)?, 16)?, mul(u, 64)?)?, 32 * 128)?,
    )?;
    let p = mul(u, plus(mul(d, 2)?, 4)?)?;
    let w = plus(512, mul(l, plus(64, mul(d, 4)?)?)?)?;
    let prototypes = mul(mul(3, l)?, plus(mul(d, 4)?, 256)?)?;
    let stack = if partition == LeafPartition::WidestAxisQuotaV1 {
        mul(c.next_power_of_two().trailing_zeros() as u64 + 1, 128)?
    } else {
        0
    };
    let replay = if partition == LeafPartition::WidestAxisQuotaV1 {
        mul(u, size_of::<(f64, usize)>() as u64)?
    } else {
        0
    };
    let sum = |parts: &[u64]| parts.iter().try_fold(0, |n, &p| plus(n, p));
    Ok([
        sum(&[2 * 65536, b, f, mul(2, v)?, r, mul(2, i)?, 8 * 1024 * 1024])?,
        sum(&[
            2 * 65536,
            b,
            f,
            v,
            i,
            mul(64, u)?,
            p,
            mul(4, u)?,
            prototypes,
            stack,
            4 * 1024 * 1024,
            8 * 1024 * 1024,
        ])?,
        sum(&[
            2 * 65536,
            b,
            f,
            mul(2, p)?,
            mul(16, u)?,
            mul(3, w)?,
            prototypes,
            replay,
            stack,
            8 * 1024 * 1024,
        ])?,
    ])
}

fn pin_value(pin: &CompletedInput) -> Value {
    json!({"bytes":pin.bytes,"sha256":pin.sha256})
}

fn publication_value(p: &Publication) -> Value {
    json!({"root":pin_value(&p.root),"membership":pin_value(&p.membership),"leaves":pin_value(&p.leaves)})
}

fn output_blob(
    path: &Path,
    body: &[u8],
    inputs: &[HeaderSnapshot],
    sync: &mut impl FnMut(&File, bool) -> io::Result<()>,
) -> Result<HeaderSnapshot> {
    distinct(inputs)?;
    let dir = parent(path)?;
    let dir_id = dir.metadata()?;
    let mut file = File::from(openat(
        &dir,
        path.file_name().ok_or("spatial output name")?,
        OFlags::RDWR
            | OFlags::CREATE
            | OFlags::EXCL
            | OFlags::NOFOLLOW
            | OFlags::NONBLOCK
            | OFlags::CLOEXEC,
        Mode::RUSR | Mode::WUSR,
    )?);
    let created = file_identity(&file)?;
    file.write_all(body)?;
    let identity = file_identity(&file)?;
    require(
        identity.dev == created.dev
            && identity.ino == created.ino
            && identity.len == body.len() as u64,
        "spatial output descriptor",
    )?;
    sync(&file, false)?;
    let mut readback = header_file(path)?;
    require(
        file_identity(&readback)? == identity && file_identity(&file)? == identity,
        "spatial file-sync mutation",
    )?;
    let mut remaining = body;
    let mut block = [0_u8; STREAM as usize];
    while !remaining.is_empty() {
        let n = remaining.len().min(block.len());
        readback.read_exact(&mut block[..n])?;
        require(block[..n] == remaining[..n], "spatial exact readback")?;
        remaining = &remaining[n..];
    }
    require(readback.read(&mut block[..1])? == 0, "spatial output EOF")?;
    distinct(inputs)?;
    sync(&dir, true)?;
    let snapshot = HeaderSnapshot {
        file: readback,
        path: path.to_owned(),
        identity,
    };
    snapshot.recheck()?;
    let now = parent(path)?.metadata()?;
    require(
        now.dev() == dir_id.dev() && now.ino() == dir_id.ino(),
        "spatial output parent replacement",
    )?;
    distinct(inputs)?;
    Ok(snapshot)
}

fn checked_sync(
    file: &File,
    is_dir: bool,
    sync: &mut impl FnMut(&File, bool) -> io::Result<()>,
) -> io::Result<()> {
    let before = if is_dir {
        None
    } else {
        Some(file_identity(file).map_err(io::Error::other)?)
    };
    sync(file, is_dir)?;
    if let Some(before) = before {
        require(
            file_identity(file).map_err(io::Error::other)? == before
                && file.metadata()?.nlink() == 1,
            "paired output changed during sync",
        )
        .map_err(io::Error::other)?;
    }
    Ok(())
}

fn publish_spatial(
    c: &PrepConfig,
    a: &ConstructionApproval,
    config_sha: &str,
    approval_sha: &str,
    artifacts: &RouterArtifacts,
    ledger: &Ledger,
    mut stamps: Vec<HeaderSnapshot>,
    mut sync: impl FnMut(&File, bool) -> io::Result<()>,
) -> Result<bool> {
    header_path(&c.output_directory)?;
    distinct(&stamps)?;
    let outer = parent(&c.output_directory)?;
    let outer_id = outer.metadata()?;
    rustix::fs::mkdirat(
        &outer,
        c.output_directory
            .file_name()
            .ok_or("spatial directory name")?,
        Mode::RUSR | Mode::WUSR | Mode::XUSR,
    )?;
    let dir = directory(&c.output_directory)?;
    let dir_id = dir.metadata()?;
    let check_dir = || -> Result<()> {
        let d = directory(&c.output_directory)?.metadata()?;
        let p = parent(&c.output_directory)?.metadata()?;
        require(
            d.dev() == dir_id.dev()
                && d.ino() == dir_id.ino()
                && p.dev() == outer_id.dev()
                && p.ino() == outer_id.ino(),
            "spatial directory replacement",
        )
    };
    sync(&dir, true)?;
    sync(&outer, true)?;
    check_dir()?;
    let mut pins = Vec::new();
    for (name, body) in [
        ("spatial-root.bin", &artifacts.manifest),
        ("spatial-membership.bin", &artifacts.membership),
        ("spatial-leaves.bin", &artifacts.leaves),
    ] {
        check_dir()?;
        let path = c.output_directory.join(name);
        let s = output_blob(&path, body, &stamps, &mut sync)?;
        pins.push(CompletedInput {
            path,
            bytes: body.len() as u64,
            sha256: digest(body),
        });
        stamps.push(s);
    }
    let publication = Publication {
        root: pins.remove(0),
        membership: pins.remove(0),
        leaves: pins.remove(0),
    };
    let receipt = json!({"schema":PREP_RECEIPT,"status":"SPATIAL_PREPARED","complete":true,
        "config_sha256":config_sha,"authority_sha256":approval_sha,"original":a.original,
        "generation":pin_value(&c.inputs.generation_manifest),"plane":pin_value(&c.inputs.plane_manifest),
        "centroids":pin_value(&c.inputs.centroids),"partition":"widest_axis_quota_v1",
        "constructor_archive":pin_value(&a.constructor.archive),"constructor_binary":pin_value(&a.constructor.binary),
        "constructor_implementation":a.constructor.implementation,
        "reconstruction":pin_value(&a.reconstruction),"radial_parity":pin_value(&a.radial_parity),
        "qualification":pin_value(&a.qualification),"corrected_closure":pin_value(&a.corrected_closure),
        "publication":publication_value(&publication),"ownership":ledger,
        "requests_opened":false,"truth_opened":false,"source_payload_opened":false,
        "requires_successful_file_sync":true,"requires_successful_directory_sync":true,
        "performance_claim":false,"qualified":false});
    let references = stamps.iter().collect::<Vec<_>>();
    check_dir()?;
    publish_selection_seal_with_sync(
        &c.output_directory.join("receipt.json"),
        &receipt,
        &references,
        |file, is_dir| {
            checked_sync(file, is_dir, &mut sync)?;
            if is_dir {
                sync(&outer, true)?;
            }
            check_dir().map_err(io::Error::other)
        },
    )?;
    Ok(true)
}

fn prepare_with(
    config: &Path,
    config_sha: &str,
    approval: &Path,
    approval_sha: &str,
    sync: impl FnMut(&File, bool) -> io::Result<()>,
) -> Result<bool> {
    let (c, cs): (PrepConfig, _) = small(config, config_sha)?;
    let (a, aps): (ConstructionApproval, _) = small(approval, approval_sha)?;
    require(
        c.schema == PREP_CONFIG && a.schema == CONSTRUCTION_APPROVAL,
        "source-only preparation schema",
    )?;
    let mut ledger = Ledger::new(c.inputs.max_owned_bytes, metadata_charge(&c.inputs, &a, 0)?)?;
    let mut stamps = vec![cs, aps];
    authenticate_construction(&c.inputs, &a, &mut stamps)?;
    ledger.phase("centroid authentication", &[c.inputs.centroids.bytes])?;
    let (blob, bs) = binary(&c.inputs.centroids, a.original.centroid_bytes()?)?;
    stamps.push(bs);
    let geometry = preflight(
        &blob,
        a.original.rows,
        a.original.dimensions,
        a.original.profile,
    )?;
    let phases = construction_phases(&a.original, LeafPartition::WidestAxisQuotaV1)?;
    let builder_peak = *phases.iter().max().ok_or("spatial phases")?;
    require(
        builder_peak <= a.original.profile.allocation_cap() as u64,
        "unchanged library construction cap",
    )?;
    // Builder model contains its own centroid ownership; charge retained caller capacity too.
    for (name, modeled) in ["spatial training", "spatial emission", "spatial validation"]
        .into_iter()
        .zip(phases)
    {
        ledger.phase(name, &[modeled, blob.capacity() as u64])?;
    }
    require(
        geometry.blob_bytes == blob.len(),
        "spatial centroid geometry",
    )?;
    let artifacts = build_with_partition(
        &blob,
        &a.original.identity(),
        a.original.profile.allocation_cap(),
        LeafPartition::WidestAxisQuotaV1,
    )?;
    admit_authenticated_root(
        &artifacts.manifest,
        &digest(&artifacts.manifest),
        &a.original.identity(),
        ledger.remaining(&[blob.capacity() as u64])?,
    )?;
    validate_publication(
        &artifacts.manifest,
        &artifacts.membership,
        &artifacts.leaves,
        &a.original.identity(),
        &blob,
    )?;
    let router = SemanticUnitRouter::open(
        &artifacts.manifest,
        &artifacts.membership,
        &digest(&artifacts.manifest),
        &a.original.identity(),
        a.original.profile.root_cap(),
    )?;
    require(
        router.manifest().partition == LeafPartition::WidestAxisQuotaV1,
        "prepared spatial mode",
    )?;
    drop(router);
    ledger.phase(
        "spatial output capacities",
        &[
            builder_peak,
            blob.capacity() as u64,
            artifacts
                .manifest
                .capacity()
                .saturating_sub(artifacts.manifest.len()) as u64,
            artifacts
                .membership
                .capacity()
                .saturating_sub(artifacts.membership.len()) as u64,
            artifacts
                .leaves
                .capacity()
                .saturating_sub(artifacts.leaves.len()) as u64,
            4 * SELECTION_CAP,
        ],
    )?;
    drop(blob);
    publish_spatial(
        &c,
        &a,
        config_sha,
        approval_sha,
        &artifacts,
        &ledger,
        stamps,
        sync,
    )
}

pub(super) fn prepare(
    config: &Path,
    sha: &str,
    authority: &Path,
    authority_sha: &str,
) -> Result<bool> {
    match prepare_with(config, sha, authority, authority_sha, |f, _| f.sync_all()) {
        Ok(value) => Ok(value),
        Err(error) if error.downcast_ref::<ResourceFailure>().is_some() => {
            let (c, cs): (PrepConfig, _) = small(config, sha)?;
            let (_, aps): (ConstructionApproval, _) = small(authority, authority_sha)?;
            header_path(&c.output_directory)?;
            let outer = parent(&c.output_directory)?;
            let outer_id = outer.metadata()?;
            rustix::fs::mkdirat(
                &outer,
                c.output_directory
                    .file_name()
                    .ok_or("spatial failure directory")?,
                Mode::RUSR | Mode::WUSR | Mode::XUSR,
            )?;
            let dir = directory(&c.output_directory)?;
            let dir_id = dir.metadata()?;
            dir.sync_all()?;
            outer.sync_all()?;
            let value = json!({"schema":PREP_RECEIPT,"status":"FAIL","complete":true,"resource_gate_failed":true,
                "config_sha256":sha,"authority_sha256":authority_sha,"requests_opened":false,"truth_opened":false,
                "error":error.to_string(),"qualified":false,"performance_claim":false});
            publish_selection_seal_with_sync(
                &c.output_directory.join("receipt.json"),
                &value,
                &[&cs, &aps],
                |file, is_dir| {
                    checked_sync(file, is_dir, &mut |file, _| file.sync_all())?;
                    if is_dir {
                        outer.sync_all()?;
                    }
                    let now = directory(&c.output_directory)
                        .map_err(io::Error::other)?
                        .metadata()?;
                    let p = parent(&c.output_directory)
                        .map_err(io::Error::other)?
                        .metadata()?;
                    require(
                        now.dev() == dir_id.dev()
                            && now.ino() == dir_id.ino()
                            && p.dev() == outer_id.dev()
                            && p.ino() == outer_id.ino(),
                        "spatial failure directory replacement",
                    )
                    .map_err(io::Error::other)
                },
            )?;
            Ok(false)
        }
        Err(error) => Err(error),
    }
}

fn admission_error(error: Box<dyn Error + Send + Sync>) -> Box<dyn Error + Send + Sync> {
    if error
        .to_string()
        .starts_with("modeled allocation admission:")
    {
        Box::new(ResourceFailure(error.to_string()))
    } else {
        error
    }
}

fn authenticate_producer(
    a: &ScreenApproval,
    c: &ScreenConfig,
    scope: &CoverageScope,
    stamps: &mut Vec<HeaderSnapshot>,
) -> Result<(CompletedConfig, Inputs)> {
    let p = &a.producer;
    let (mut config, config_stamp) = header_json(&p.config)?;
    require(
        config == p.expected_config,
        "approved original producer config BODY",
    )?;
    config
        .as_object_mut()
        .ok_or("producer config object")?
        .entry("fetch_parallelism")
        .or_insert(json!(16));
    fields(
        &config,
        "schema dataset revision metric tie_rule corpus_intervals reserved_query_interval cohort_receipt derivation_receipt producer_authority corpus_source_first query_source_first rows dimensions count k profile backend generation_prefix generation_root_sha256 scratch_parent requests truth native_source max_memory_bytes fetch_parallelism serving execution",
    )?;
    require(
        config["schema"] == "borsuk-cohere-native-baseline-config-v7",
        "original producer config schema",
    )?;
    validate_header_descriptors(&config)?;
    let i: Inputs = serde_json::from_value(p.expected_bound_inputs.clone())?;
    validate_scale_row(&serde_json::to_vec(&p.expected_identity)?, "identity")?;
    validate_scale_row(
        &serde_json::to_vec(&p.expected_bound_inputs)?,
        "bound_inputs",
    )?;
    validate_scale_inputs(&i, &p.expected_bound_inputs)?;
    require(
        i.rows == scope.rows
            && i.count == scope.count
            && i.dimensions == a.construction.original.dimensions
            && i.rows == a.construction.original.rows
            && i.profile == "scale1m"
            && scale_ordinals(&p.expected_bound_inputs, i.count)? == scope.ordinals
            && p.expected_bound_inputs["execution"]["mode"] == "diagnostic_panel"
            && p.expected_identity["config_sha256"] == p.config.sha256
            && p.expected_identity["binary_sha256"] == p.binary.sha256
            && i.generation_root_sha256 == c.inputs.generation_manifest.sha256
            && c.requests.bytes == i.requests_bytes
            && c.requests.sha256 == i.requests_sha256
            && c.truth.bytes == i.truth_bytes
            && c.truth.sha256 == i.truth_sha256
            && c.sq8.bytes == mul(i.rows as u64, plus(i.dimensions as u64, 12)?)?
            && c.sq8.sha256 == i.native_sq8_sha256,
        "independent producer/panel/input authority",
    )?;
    for key in "dataset revision metric tie_rule corpus_intervals reserved_query_interval producer_authority corpus_source_first query_source_first rows dimensions count k profile backend generation_prefix generation_root_sha256 max_memory_bytes fetch_parallelism serving execution".split_whitespace() {
        require(config[key] == p.expected_bound_inputs[key], "producer config/expected bound field")?;
    }
    for key in ["fetch_parallelism", "serving", "execution"] {
        require(
            config[key] == p.expected_identity[key],
            "producer config/identity field",
        )?;
    }
    for (role, sha) in [
        ("source_sha256", &i.native_source_sha256),
        ("sq8_sha256", &i.native_sq8_sha256),
        ("source_order_sha256", &i.native_order_sha256),
    ] {
        require(
            config["native_source"][role] == *sha,
            "producer original source identity",
        )?;
    }
    matches_descriptor(&config["requests"], &c.requests)?;
    matches_descriptor(&config["truth"], &c.truth)?;
    for role in ["cohort_receipt", "derivation_receipt"] {
        require(
            config[role]["sha256"] == p.expected_bound_inputs[format!("{role}_sha256")],
            "producer original receipt binding",
        )?;
    }
    let (generation, _) = header_json(&c.inputs.generation_manifest)?;
    let d = &generation["discovery"];
    require(
        d["source_sha256"] == i.native_source_sha256
            && d["sq8_sha256"] == i.native_sq8_sha256
            && d["source_order_sha256"] == i.native_order_sha256,
        "producer/original Discovery identity",
    )?;
    if config["backend"]["kind"] == "s3" {
        require(
            config["backend"]["sq8_object_key"] == generation["sq8_object_key"]
                && config["backend"]["sq8_etag"] == generation["sq8_etag"],
            "producer original S3 descriptor",
        )?;
    }
    stamps.push(config_stamp);
    stamps.push(stream(&p.archive, OWNED_CAP)?);
    stamps.push(stream(&p.binary, 256 * 1024 * 1024)?);
    // Full producer body is authenticated BEFORE trace parsing/control callbacks.
    stamps.push(stream(&p.result, FILE_CAP)?);
    distinct(stamps)?;
    Ok((
        CompletedConfig {
            schema: SCALE_CONFIG_SCHEMA.into(),
            input: CompletedInput {
                path: p.result.path.clone(),
                bytes: p.result.bytes,
                sha256: p.result.sha256.clone(),
            },
            expected_identity: p.expected_identity.clone(),
            expected_bound_inputs: p.expected_bound_inputs.clone(),
        },
        i,
    ))
}

fn authenticate_spatial_receipt(
    a: &ScreenApproval,
    c: &ScreenConfig,
    stamps: &mut Vec<HeaderSnapshot>,
) -> Result<()> {
    same_publication(&c.radial, &a.radial)?;
    same_publication(&c.spatial, &a.spatial)?;
    same_pin(&c.spatial_receipt, &a.spatial_receipt)?;
    require(
        c.radial.root.sha256 != c.spatial.root.sha256,
        "paired distinct mode roots",
    )?;
    let v = approved_json(&c.spatial_receipt, &a.expected_spatial_receipt, stamps)?;
    fields(
        &v,
        "schema status complete config_sha256 authority_sha256 original generation plane centroids partition constructor_archive constructor_binary constructor_implementation reconstruction radial_parity qualification corrected_closure publication ownership requests_opened truth_opened source_payload_opened requires_successful_file_sync requires_successful_directory_sync performance_claim qualified",
    )?;
    require(
        v["schema"] == PREP_RECEIPT
            && v["status"] == "SPATIAL_PREPARED"
            && v["complete"] == true
            && v["original"] == serde_json::to_value(&a.construction.original)?
            && v["partition"] == "widest_axis_quota_v1"
            && v["requests_opened"] == false
            && v["truth_opened"] == false
            && v["source_payload_opened"] == false
            && v["constructor_implementation"] == a.construction.constructor.implementation
            && v["requires_successful_file_sync"] == true
            && v["requires_successful_directory_sync"] == true
            && v["performance_claim"] == false
            && v["qualified"] == false
            && v["config_sha256"].as_str().is_some_and(valid_sha)
            && v["authority_sha256"].as_str().is_some_and(valid_sha),
        "frozen source-only spatial receipt",
    )?;
    for (name, pin) in [
        ("generation", &c.inputs.generation_manifest),
        ("plane", &c.inputs.plane_manifest),
        ("centroids", &c.inputs.centroids),
        ("constructor_archive", &a.construction.constructor.archive),
        ("constructor_binary", &a.construction.constructor.binary),
        ("reconstruction", &a.construction.reconstruction),
        ("radial_parity", &a.construction.radial_parity),
        ("qualification", &a.construction.qualification),
        ("corrected_closure", &a.construction.corrected_closure),
    ] {
        matches_descriptor(&v[name], pin)?;
    }
    require(
        v["publication"] == publication_value(&c.spatial),
        "frozen spatial receipt artifacts",
    )?;
    let h = &a.construction.expected_radial_parity["historical_parity"];
    for (name, pin) in [
        ("root", &c.radial.root),
        ("membership", &c.radial.membership),
        ("leaves", &c.radial.leaves),
    ] {
        matches_descriptor(
            &a.construction.expected_radial_parity["radial_outputs"][name],
            pin,
        )?;
        require(
            h[format!("radial_{name}_sha256")] == pin.sha256,
            "qualified radial derived authority",
        )?;
    }
    distinct(stamps)
}

fn replay_charge(queries: usize) -> Result<u64> {
    // BTree nodes bounded at128B/entry, covering both arm nominations and serialized integer Values.
    mul(
        mul(queries as u64, 2)?,
        plus(mul(4 * 3072 + 7, 128)?, 64 * 1024)?,
    )
}

fn select_arm(
    p: &Publication,
    o: &Original,
    requests: &[u8],
    ordinals: &[usize],
    expected: LeafPartition,
    ledger: &mut Ledger,
    retained: u64,
) -> Result<Vec<CoverageArm>> {
    ledger.phase(
        "replay root authentication",
        &[p.root.bytes, requests.len() as u64, retained],
    )?;
    let (root, rs) = binary(&p.root, p.root.bytes)?;
    let (mode, modeled) = admit_authenticated_root(
        &root,
        &p.root.sha256,
        &o.identity(),
        ledger.remaining(&[requests.len() as u64, retained])?,
    )
    .map_err(admission_error)?;
    require(mode == expected, "replay frozen partition")?;
    ledger.phase(
        "router requests nominations",
        &[
            modeled as u64,
            requests.len() as u64,
            retained,
            mul(o.rows.div_ceil(32) as u64, 128)?,
        ],
    )?;
    let (membership, ms) = binary(&p.membership, p.membership.bytes)?;
    let router = SemanticUnitRouter::open(
        &root,
        &membership,
        &p.root.sha256,
        &o.identity(),
        o.profile.root_cap(),
    )?;
    drop((root, membership));
    let width = o.dimensions.checked_mul(4).ok_or("request width")?;
    let mut arms = Vec::with_capacity(ordinals.len());
    for &ordinal in ordinals {
        let begin = ordinal.checked_mul(width).ok_or("request ordinal width")?;
        let raw = requests
            .get(begin..plus(begin as u64, width as u64)? as usize)
            .ok_or("request ordinal")?
            .chunks_exact(4)
            .map(|w| f32::from_le_bytes(w.try_into().unwrap()))
            .collect::<Vec<_>>();
        let normalized = borsuk::two_bit_generation::normalize_two_bit_diagnostic_query(&raw)?;
        let ranked =
            router.rank_leaves_by_metric(&normalized, &borsuk::VectorMetric::SquaredEuclidean)?;
        require(ranked.len() >= COVERAGE_LEAVES, "paired exactly48 leaves")?;
        let arm = coverage_selected_arm(&router, &ranked[..COVERAGE_LEAVES], o.rows, o.dimensions)
            .map_err(|e| -> Box<dyn Error + Send + Sync> {
                if e.downcast_ref::<borsuk::two_bit_generation::TwoBitGenerationError>()
                    .is_some_and(|e| {
                        matches!(
                            e,
                            borsuk::two_bit_generation::TwoBitGenerationError::Budget(_)
                        )
                    })
                {
                    Box::new(ResourceFailure(e.to_string()))
                } else {
                    e
                }
            })?;
        arms.push(arm);
    }
    rs.recheck()?;
    ms.recheck()?;
    // Router and request-local vectors are dropped before the other arm is opened.
    Ok(arms)
}

fn control(
    completed: &CompletedConfig,
    radial: &[CoverageArm],
    ordinals: &[usize],
) -> Result<(Run, Vec<LossCoverage>, Vec<Value>)> {
    let mut slot = 0;
    let mut cov = Vec::with_capacity(ordinals.len());
    let mut traces = Vec::with_capacity(ordinals.len());
    let run = read_run_observed(
        &completed.input.path,
        &completed.input.sha256,
        Some(completed),
        |line, q, i| {
            require(
                slot < ordinals.len() && q.ordinal == ordinals[slot],
                "paired exact producer order",
            )?;
            cov.push(coverage_control(line, q, i, &radial[slot])?);
            traces.push(json!({"ordinal":q.ordinal,"trace_sha256":hex_digest(&trace_fingerprint(line)?),
            "returned":q.returned.iter().map(|h| json!({"id":h.id,"score_bits":h.score_bits})).collect::<Vec<_>>(),
            "observed_source":q.charges.source}));
            slot += 1;
            Ok(())
        },
    )?;
    require(
        slot == ordinals.len() && run.samples.len() == slot,
        "paired complete producer controls",
    )?;
    Ok((run, cov, traces))
}

fn hex_digest(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn closure_value(a: &CoverageArm) -> Value {
    json!({"original_pages":a.nomination.page_closure,
        "cover_ranges":a.ranges.iter().map(|r|[r.start,r.end]).collect::<Vec<_>>(),
        "planned_gets":a.ranges.len(),"planned_bytes":a.bytes})
}

fn output_roles(
    c: &ScreenConfig,
    a: &ScreenApproval,
    selection: &Path,
    report: &Path,
) -> Result<()> {
    header_path(selection)?;
    header_path(report)?;
    require(selection != report, "selection/report role alias")?;
    // No filesystem operation on the truth role occurs here.
    let mut paths = vec![
        &c.inputs.generation_manifest.path,
        &c.inputs.plane_manifest.path,
        &c.inputs.centroids.path,
        &c.radial.root.path,
        &c.radial.membership.path,
        &c.radial.leaves.path,
        &c.spatial.root.path,
        &c.spatial.membership.path,
        &c.spatial.leaves.path,
        &c.spatial_receipt.path,
        &c.requests.path,
        &c.sq8.path,
        &c.truth.path,
        &a.producer.result.path,
        &a.producer.config.path,
        &a.producer.archive.path,
        &a.producer.binary.path,
    ];
    paths.extend([
        &a.construction.original_approval.path,
        &a.construction.constructor.config.path,
        &a.construction.constructor.archive.path,
        &a.construction.constructor.binary.path,
        &a.construction.reconstruction.path,
        &a.construction.radial_parity.path,
        &a.construction.qualification.path,
        &a.construction.corrected_closure.path,
    ]);
    let normalized = |p: &Path| {
        p.components()
            .filter(|c| !matches!(c, Component::CurDir))
            .collect::<PathBuf>()
    };
    require(
        normalized(selection) != normalized(report),
        "normalized output alias",
    )?;
    let mut roles = BTreeSet::new();
    for p in paths {
        header_path(p)?;
        let path = normalized(p);
        require(
            path != normalized(selection) && path != normalized(report),
            "output/input role alias",
        )?;
        require(roles.insert(path), "input role path alias")?;
    }
    Ok(())
}

fn seal_then_reduce(
    c: &ScreenConfig,
    i: &Inputs,
    scope: &CoverageScope,
    radial: &[CoverageArm],
    spatial: &[CoverageArm],
    run: &Run,
    control: &[LossCoverage],
    seal: &Value,
    selection: &Path,
    mut stamps: Vec<HeaderSnapshot>,
    ledger: &mut Ledger,
    sync: &mut impl FnMut(&File, bool) -> io::Result<()>,
    open_truth: &mut impl FnMut(&CompletedInput, u64) -> Result<(Vec<u8>, HeaderSnapshot)>,
) -> Result<(Value, Vec<HeaderSnapshot>)> {
    let retained = replay_charge(scope.ordinals.len())?;
    ledger.phase(
        "paired seal serialization readback",
        &[retained, 4 * SELECTION_CAP],
    )?;
    distinct(&stamps)?;
    let refs = stamps.iter().collect::<Vec<_>>();
    let (selection_stamp, selection_sha) =
        publish_selection_seal_with_sync(selection, seal, &refs, |file, is_dir| {
            checked_sync(file, is_dir, sync)
        })?;
    stamps.push(selection_stamp);
    distinct(&stamps)?;
    // FIRST truth stat/open, after BOTH complete arms passed durable readback and identity checks.
    ledger.phase(
        "truth order scan reduction",
        &[
            retained,
            c.truth.bytes,
            i.rows.div_ceil(8) as u64,
            8192,
            i.dimensions as u64 + 12,
            8 * REPORT_CAP,
        ],
    )?;
    let (truth, ts) = open_truth(&c.truth, i.truth_bytes)?;
    stamps.push(ts);
    let mut wanted = BTreeSet::new();
    wanted.extend(
        run.samples
            .iter()
            .flat_map(|s| s.returned.iter().map(|h| h.id)),
    );
    let mut truth_rows = Vec::with_capacity(scope.ordinals.len());
    for &ordinal in &scope.ordinals {
        let start = ordinal.checked_mul(K * 8).ok_or("truth ordinal")?;
        let ids = truth
            .get(start..start + K * 8)
            .ok_or("truth row")?
            .chunks_exact(8)
            .map(|w| u64::from_le_bytes(w.try_into().unwrap()))
            .collect::<Vec<_>>();
        require(
            ids.iter().all(|&id| id < i.rows as u64)
                && ids.iter().copied().collect::<BTreeSet<_>>().len() == K,
            "paired truth IDs",
        )?;
        wanted.extend(ids.iter().copied());
        truth_rows.push(ids);
    }
    let (positions, ss) = loss_positions(
        &c.sq8,
        i.rows,
        i.dimensions,
        &i.native_order_sha256,
        &wanted,
    )?;
    stamps.push(ss);
    let mut totals = [0_usize; 2];
    let mut original_hits = 0_u64;
    let (mut gained, mut lost) = (0_usize, 0_usize);
    let mut bridge = [0_usize; 2];
    let mut queries = Vec::with_capacity(truth_rows.len());
    for (slot, ids) in truth_rows.iter().enumerate() {
        let sample = &run.samples[slot];
        for h in &sample.returned {
            require(
                loss_bucket(&control[slot], positions[&h.id], true)
                    != "INVALID_RETURNED_OUTSIDE_SQ8",
                "paired returned ID outside original SQ8 fetch",
            )?;
        }
        let hits = ids
            .iter()
            .filter(|id| sample.returned.iter().any(|h| h.id == **id))
            .count();
        require(
            hits as u64 == sample.hits10,
            "paired original returned recall control",
        )?;
        original_hits = plus(original_hits, hits as u64)?;
        let mut slots = Vec::with_capacity(K);
        for id in ids {
            let physical = positions[id];
            let states = [
                coverage_slot(&radial[slot], physical, source_record_bytes(i.dimensions)?),
                coverage_slot(&spatial[slot], physical, source_record_bytes(i.dimensions)?),
            ];
            for arm in 0..2 {
                totals[arm] += usize::from(states[arm].0);
                bridge[arm] += usize::from(states[arm].1 && !states[arm].0);
            }
            gained += usize::from(!states[0].0 && states[1].0);
            lost += usize::from(states[0].0 && !states[1].0);
            slots.push(json!({"id":id,"physical_row":physical,"radial_selected":states[0].0,
                "spatial_selected":states[1].0,"radial_fetched":states[0].1,"spatial_fetched":states[1].1,
                "radial_bridge_only":states[0].1&&!states[0].0,"spatial_bridge_only":states[1].1&&!states[1].0,
                "gained":!states[0].0&&states[1].0,"displaced_loss":states[0].0&&!states[1].0}));
        }
        queries.push(json!({"ordinal":scope.ordinals[slot],"truth_slots":slots,
            "radial_physical_closure":closure_value(&radial[slot]),
            "spatial_physical_closure":closure_value(&spatial[slot])}));
    }
    require(
        original_hits == scope.original_hits
            && original_hits == run.terminal.total_hits10
            && totals[0] == scope.original_coverage
            && totals[1] + lost == totals[0] + gained,
        "paired frozen313/314 control/decomposition",
    )?;
    let pass = if scope.ordinals.len() == 32 {
        coverage_survives(totals[0], totals[1], gained, lost)
    } else {
        totals[1] > totals[0] && gained > lost
    };
    distinct(&stamps)?;
    let report = json!({"schema":REPORT_SCHEMA,"status":if pass {"PASS"} else {"FAIL"},"complete":true,
        "selection_seal_sha256":selection_sha,"selection_seal_path":selection,
        "original_returned_hits":original_hits,"radial_source_coverage":totals[0],"spatial_source_coverage":totals[1],
        "truth_slots":scope.ordinals.len()*K,"gained_slots":gained,"displaced_lost_slots":lost,
        "radial_bridge_only_slots":bridge[0],"spatial_bridge_only_slots":bridge[1],"queries":queries,
        "ownership":ledger,"truth_opened_after_durable_paired_selection":true,
        "consumed_panel":true,"planner_only":true,"source_payload_read":false,"candidate_returned_ids_computed":false,
        "cold_s3_claim":false,"default_promotion":false,"qualified":false});
    Ok((report, stamps))
}

fn reduce_with(
    config: &Path,
    sha: &str,
    approval: &Path,
    approval_sha: &str,
    selection: &Path,
    report: &Path,
    scope: &CoverageScope,
    mut sync: impl FnMut(&File, bool) -> io::Result<()>,
    mut open_truth: impl FnMut(&CompletedInput, u64) -> Result<(Vec<u8>, HeaderSnapshot)>,
) -> Result<(Value, Vec<HeaderSnapshot>)> {
    let (c, cs): (ScreenConfig, _) = small(config, sha)?;
    let (a, aps): (ScreenApproval, _) = small(approval, approval_sha)?;
    require(
        c.schema == SCREEN_CONFIG && a.schema == SCREEN_APPROVAL,
        "paired strict schemas",
    )?;
    require(
        config != selection && config != report && approval != selection && approval != report,
        "config/output alias",
    )?;
    output_roles(&c, &a, selection, report)?;
    let mut extra = plus(a.producer.config.bytes, c.spatial_receipt.bytes)?;
    header_pin(&a.producer.config, HEADER_JSON_CAP)?;
    header_pin(&c.spatial_receipt, HEADER_JSON_CAP)?;
    for p in [&c.requests, &c.truth] {
        header_pin(p, 4_096_000)?;
    }
    let mut ledger = Ledger::new(
        c.inputs.max_owned_bytes,
        metadata_charge(&c.inputs, &a.construction, extra)?,
    )?;
    let mut stamps = vec![cs, aps];
    authenticate_construction(&c.inputs, &a.construction, &mut stamps)?;
    authenticate_spatial_receipt(&a, &c, &mut stamps)?;
    let (completed, i) = authenticate_producer(&a, &c, scope, &mut stamps)?;
    ledger.phase("centroid import", &[c.inputs.centroids.bytes])?;
    let (blob, bs) = binary(
        &c.inputs.centroids,
        a.construction.original.centroid_bytes()?,
    )?;
    stamps.push(bs);
    let radial_groups = import(
        &c.radial,
        &a.construction.original,
        &blob,
        LeafPartition::Radial64V1,
        None,
        &mut ledger,
        &mut stamps,
    )?
    .ok_or("radial group map")?;
    import(
        &c.spatial,
        &a.construction.original,
        &blob,
        LeafPartition::WidestAxisQuotaV1,
        Some(&radial_groups),
        &mut ledger,
        &mut stamps,
    )?;
    drop((blob, radial_groups));
    let nominations = replay_charge(scope.ordinals.len())?;
    ledger.phase(
        "requests and retained nominations",
        &[c.requests.bytes, nominations],
    )?;
    let (requests, request_stamp) = binary(&c.requests, i.requests_bytes)?;
    stamps.push(request_stamp);
    let radial = select_arm(
        &c.radial,
        &a.construction.original,
        &requests,
        &scope.ordinals,
        LeafPartition::Radial64V1,
        &mut ledger,
        nominations,
    )?;
    let spatial = select_arm(
        &c.spatial,
        &a.construction.original,
        &requests,
        &scope.ordinals,
        LeafPartition::WidestAxisQuotaV1,
        &mut ledger,
        nominations,
    )?;
    drop(requests);
    ledger.phase(
        "producer control parsing",
        &[nominations, mul(64, LINE_CAP as u64)?, 8 * RESERVE],
    )?;
    let (run, controls, traces) = control(&completed, &radial, &scope.ordinals)?;
    distinct(&stamps)?;
    extra = mul(64, SELECTION_CAP)?;
    ledger.phase(
        "seal Value and reducer ownership",
        &[nominations, extra, 8 * RESERVE],
    )?;
    let seal = json!({"schema":"borsuk-paired-partition-selection-seal-v1","status":"SEALED","complete":true,
        "config_sha256":sha,"authority_sha256":approval_sha,"original":a.construction.original,
        "producer_result":pin_value(&a.producer.result),"producer_config":pin_value(&a.producer.config),
        "producer_archive":pin_value(&a.producer.archive),"producer_binary":pin_value(&a.producer.binary),
        "generation":pin_value(&c.inputs.generation_manifest),"plane":pin_value(&c.inputs.plane_manifest),
        "centroids":pin_value(&c.inputs.centroids),"radial":publication_value(&c.radial),"spatial":publication_value(&c.spatial),
        "spatial_receipt":pin_value(&c.spatial_receipt),"requests":pin_value(&c.requests),"truth":pin_value(&c.truth),
        "sq8":pin_value(&c.sq8),"source_sha256":i.native_source_sha256,"order_sha256":i.native_order_sha256,
        "normalization":"normalize_two_bit_diagnostic_query","metric":"squared_euclidean","leaf_count":48,
        "source_get_cap":128,"source_byte_cap":64*1024*1024,"truth_opened":false,"per_unit_groups_equal":true,
        "ordinals":scope.ordinals,"producer_traces":traces,
        "selections":radial.iter().zip(&spatial).enumerate().map(|(s,(r,c))|
            json!({"ordinal":scope.ordinals[s],"radial":coverage_arm_value(r),"spatial":coverage_arm_value(c)})).collect::<Vec<_>>()});
    seal_then_reduce(
        &c,
        &i,
        scope,
        &radial,
        &spatial,
        &run,
        &controls,
        &seal,
        selection,
        stamps,
        &mut ledger,
        &mut sync,
        &mut open_truth,
    )
}

fn failure(error: &(dyn Error + 'static)) -> Value {
    let resource = error.downcast_ref::<ResourceFailure>().is_some();
    json!({"schema":REPORT_SCHEMA,"status":if resource {"FAIL"} else {"INVALID"},
        "complete":resource,"resource_gate_failed":resource,
        "error":error.to_string().chars().take(512).collect::<String>(),"qualified":false,
        "cold_s3_claim":false,"default_promotion":false,"candidate_returned_ids_computed":false})
}

pub(super) fn execute(
    config: &Path,
    sha: &str,
    approval: &Path,
    approval_sha: &str,
    selection: &Path,
    report: &Path,
) -> Result<bool> {
    // Validate lexical output roles before reserving any output (including an absent truth path).
    let (c, _): (ScreenConfig, _) = small(config, sha)?;
    let (a, _): (ScreenApproval, _) = small(approval, approval_sha)?;
    output_roles(&c, &a, selection, report)?;
    require(
        config != selection && config != report && approval != selection && approval != report,
        "config/output alias",
    )?;
    let dir = parent(report)?;
    let file = File::from(openat(
        &dir,
        report.file_name().ok_or("paired report filename")?,
        OFlags::WRONLY
            | OFlags::CREATE
            | OFlags::EXCL
            | OFlags::NOFOLLOW
            | OFlags::NONBLOCK
            | OFlags::CLOEXEC,
        Mode::RUSR | Mode::WUSR,
    )?);
    drop((c, a));
    let (value, retained) = match reduce_with(
        config,
        sha,
        approval,
        approval_sha,
        selection,
        report,
        &CoverageScope::consumed32(),
        |f, _| f.sync_all(),
        binary,
    ) {
        Ok(value) => value,
        Err(error) => (failure(error.as_ref()), vec![]),
    };
    let refs = retained.iter().collect::<Vec<_>>();
    finish_selection_publication(report, &value, &refs, dir, file, |f, is_dir| {
        if !is_dir {
            require(f.metadata()?.len() <= REPORT_CAP, "paired report cap")
                .map_err(io::Error::other)?;
        }
        checked_sync(f, is_dir, &mut |file, _| file.sync_all())
    })?;
    Ok(value["status"] == "PASS")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn put(path: &Path, bytes: &[u8]) -> CompletedInput {
        std::fs::write(path, bytes).unwrap();
        CompletedInput {
            path: path.to_owned(),
            bytes: bytes.len() as u64,
            sha256: digest(bytes),
        }
    }
    fn put_json(path: &Path, value: &Value) -> CompletedInput {
        put(path, &serde_json::to_vec(value).unwrap())
    }
    fn copy_pin(p: &CompletedInput) -> CompletedInput {
        CompletedInput {
            path: p.path.clone(),
            bytes: p.bytes,
            sha256: p.sha256.clone(),
        }
    }
    fn save_publication(dir: &Path, name: &str, a: &RouterArtifacts) -> Publication {
        Publication {
            root: put(&dir.join(format!("{name}-root")), &a.manifest),
            membership: put(&dir.join(format!("{name}-membership")), &a.membership),
            leaves: put(&dir.join(format!("{name}-leaves")), &a.leaves),
        }
    }
    fn blob(rows: usize, d: usize) -> Vec<u8> {
        let mut b = vec![0_u8; 32 + rows.div_ceil(32) * d * 2];
        b[..8].copy_from_slice(b"BORSUCP1");
        b[8..16].copy_from_slice(&(rows as u64).to_le_bytes());
        for (at, word) in [(16, d as u32), (20, 32), (24, 256)] {
            b[at..at + 4].copy_from_slice(&word.to_le_bytes());
        }
        // Identical finite original FP16 rows, including signed zero and a subnormal.
        for unit in 0..rows.div_ceil(32) {
            for (axis, bits) in [(0, 0x3c00_u16), (1, 0x8000), (2, 0x0001)] {
                if axis < d {
                    let at = 32 + 2 * (unit * d + axis);
                    b[at..at + 2].copy_from_slice(&bits.to_le_bytes());
                }
            }
        }
        b
    }

    struct Fixture {
        dir: tempfile::TempDir,
        c: PrepConfig,
        a: ConstructionApproval,
        radial: Publication,
        spatial: Publication,
        config: CompletedInput,
        approval: CompletedInput,
    }
    impl Fixture {
        fn new(rows: usize, d: usize) -> Self {
            let dir = tempfile::tempdir().unwrap();
            let b = blob(rows, d);
            let centroid_sha = digest(&b);
            let input_root = "1".repeat(64);
            let identity = SourceIdentity {
                profile: SemanticProfile::Scale1m,
                schema: "original-construction-fixture",
                root_sha256: &input_root,
                centroids_sha256: &centroid_sha,
                rows,
                dimensions: d,
            };
            // Actual public builder/trainer: never replace root prototypes.
            let r = build_with_partition(
                &b,
                &identity,
                identity.profile.allocation_cap(),
                LeafPartition::Radial64V1,
            )
            .unwrap();
            let s = build_with_partition(
                &b,
                &identity,
                identity.profile.allocation_cap(),
                LeafPartition::WidestAxisQuotaV1,
            )
            .unwrap();
            let radial = save_publication(dir.path(), "radial", &r);
            let spatial = save_publication(dir.path(), "spatial", &s);
            let mut old_root = r.manifest.clone();
            old_root[..8].copy_from_slice(b"BORSUSR2");
            old_root[88..92].copy_from_slice(&0_u32.to_le_bytes());
            let old_sha = digest(&old_root);
            let plane = json!({"schema":"borsuk-two-bit-plane-v3","rows":rows,"dimensions":d,"seed":20260923,
                "record_bytes":source_record_bytes(d).unwrap(),"source_sha256":"2".repeat(64),
                "sq8_sha256":"3".repeat(64),"source_order_sha256":"4".repeat(64),"mean_sha256":"5".repeat(64),
                "records_sha256":"6".repeat(64),"page_rows":32,"page_digest_sha256":"7".repeat(64),"query_or_truth_used":false});
            let plane_pin = put_json(&dir.path().join("plane"), &plane);
            let source = json!({"source_sha256":plane["source_sha256"],"source_order_sha256":plane["source_order_sha256"],
                "mean_sha256":plane["mean_sha256"],"records_sha256":plane["records_sha256"],"sq8_sha256":plane["sq8_sha256"]});
            let discovery = json!({"mode":"semantic","profile":"scale1m","root_bytes":r.manifest.len(),
                "root_sha256":old_sha,"membership_bytes":r.membership.len(),"membership_sha256":radial.membership.sha256,
                "leaves_bytes":r.leaves.len(),"leaves_sha256":radial.leaves.sha256,"input_schema":identity.schema,
                "input_root_sha256":input_root,"centroids_sha256":centroid_sha,
                "source_sha256":source["source_sha256"],"source_order_sha256":source["source_order_sha256"],
                "mean_sha256":source["mean_sha256"],"records_sha256":source["records_sha256"],"sq8_sha256":source["sq8_sha256"]});
            let generation = json!({"schema":"borsuk-two-bit-generation-v8","generation":1,"base_epoch":0,
                "plane_manifest_sha256":plane_pin.sha256,"page_manifest_sha256":"8".repeat(64),"discovery":discovery,
                "sq8_object_sha256":plane["sq8_sha256"],"sq8_object_key":format!("semantic/objects/{}","3".repeat(64)),
                "sq8_etag":"fixture","canonical":{"object_key":format!("semantic/objects/{}","9".repeat(64)),
                "sha256":"9".repeat(64),"bytes":rows*(8+4*d),"rows":rows,"dimensions":d},
                "low":vec![-1_f32;d],"step":vec![0.125_f32;d]});
            let generation_pin = put_json(&dir.path().join("generation"), &generation);
            let original = Original {
                schema: "borsuk-partition-reconstruction-approval-v1".into(),
                generation_sha256: generation_pin.sha256.clone(),
                generation: 1,
                base_epoch: 0,
                plane_sha256: plane_pin.sha256.clone(),
                discovery_sha256: digest(&serde_json::to_vec(&discovery).unwrap()),
                input_schema: identity.schema.into(),
                input_root_sha256: input_root.clone(),
                centroids_sha256: centroid_sha.clone(),
                profile: identity.profile,
                rows,
                dimensions: d,
            };
            let original_value = serde_json::to_value(&original).unwrap();
            let original_approval =
                put_json(&dir.path().join("original-approval"), &original_value);
            let centroid_pin = put(&dir.path().join("centroids"), &b);
            let ctor_config_value = json!({"schema":"borsuk-partition-reconstruction-config-v1",
                "generation_manifest":generation_pin,"plane_manifest":plane_pin,
                "leaves":radial.leaves,"historical":{"root":{"path":dir.path().join("old-root"),
                    "bytes":r.manifest.len(),"sha256":old_sha},"membership":radial.membership},
                "caller_pinned_bytes":0,"max_payload_bytes":512*1024*1024,"output_directory":dir.path().join("old-output")});
            let ctor_config = put_json(&dir.path().join("constructor-config"), &ctor_config_value);
            // Provenance stand-ins test independent SHA binding, not native execution qualification.
            let ctor_archive = put(
                &dir.path().join("constructor-archive"),
                b"approved original source archive fixture",
            );
            let ctor_binary = put(
                &dir.path().join("constructor-elf"),
                b"approved original ELF fixture",
            );
            let implementation = json!({"example_sha256":"1".repeat(64),"module_sha256":"2".repeat(64),
                "router_sha256":"3".repeat(64),"generation_sha256":"4".repeat(64),"plane_sha256":"5".repeat(64),
                "centroid_sha256":"6".repeat(64),"trainer_sha256":"7".repeat(64)});
            let h = json!({"old_root":{"bytes":r.manifest.len(),"sha256":old_sha},
                "old_membership":pin_value(&radial.membership),"old_leaves":pin_value(&radial.leaves),
                "radial_root_sha256":radial.root.sha256,"radial_membership_sha256":radial.membership.sha256,
                "radial_leaves_sha256":radial.leaves.sha256,"old_allocation_limit_bytes":512*1024*1024,
                "builder_peak_bytes":construction_phases(&original,LeafPartition::Radial64V1).unwrap().into_iter().max().unwrap(),
                "experiment_charge_bytes":512*1024*1024,
                "root_exceptions":[{"offset":0,"old":"BORSUSR2","new":"BORSUSR3"},
                    {"offset":88,"old_le_u32":0,"new_le_u32":1}]});
            let mut rec = json!({"schema":"borsuk-partition-reconstruction-receipt-v1","status":"RECONSTRUCTED",
                "complete":true,"requests_opened":false,"truth_opened":false,
                "historical_root_opened":false,"historical_membership_opened":false,
                "config":ctor_config,"approval":original_approval,"generation":generation_pin,"plane":plane_pin,
                "binary":ctor_binary,"implementation":implementation,"original":original_value,"source":source,
                "leaves":radial.leaves,"unit_rows":32,"page_rows":256,"unit_count":rows.div_ceil(32),
                "final_unit_rows":rows-(rows.div_ceil(32)-1)*32,"centroids_bytes":centroid_pin.bytes,
                "centroids_sha256":centroid_pin.sha256,"centroids":centroid_pin,"radial_outputs":null,
                "metadata_charge_bytes":0,"reconstruction_charge_bytes":0,"max_payload_bytes":512*1024*1024,
                "historical_parity":null,"requires_successful_file_sync":true,"requires_successful_directory_sync":true,
                "requires_original_successful_terminal_receipt":true});
            let reconstruction = put_json(&dir.path().join("reconstruction"), &rec);
            let expected_reconstruction = rec.clone();
            rec["status"] = json!("HISTORICAL_RADIAL_PARITY");
            rec["historical_root_opened"] = json!(true);
            rec["historical_membership_opened"] = json!(true);
            rec["historical_parity"] = h.clone();
            rec["radial_outputs"] = serde_json::to_value(&radial).unwrap();
            let radial_parity = put_json(&dir.path().join("radial-parity"), &rec);
            let q = json!({"schema":"borsuk-real1m-construction-qualified-v1",
                "status":"REAL_RECONSTRUCTION_AND_HISTORICAL_RADIAL_PARITY_QUALIFIED",
                "elf_sha256":ctor_binary.sha256,"original_approval":original_value,
                "original_generation_sha256":generation_pin.sha256,
                "admission":{"native_receipt_sha256":reconstruction.sha256,"all_closed_body_hashes_verified":true,"original_ssm_success":true},
                "original_parity":{"native_exit":0,"manager_exit":1,
                    "ssm_status":"Failed","disposition":"INVALID_RECEIPT_VALIDATOR","disposition_preserved":true,
                    "receipt_sha256":radial_parity.sha256},
                "closed_output_revalidation":{"status":"Success","exit":0,"native_reexecuted":false,
                    "all_recovery_body_checksums_verified":true},"historical_parity":h,
                "centroids":centroid_pin,"requests_opened":false,"truth_opened":false,"performance_claim":false});
            let qualification = put_json(&dir.path().join("qualification"), &q);
            let closure = json!({"schema":"borsuk-reconstruction-closed-parity-recovery-v1","status":"CLOSED_NATIVE_PARITY_VALIDATED",
                "original_native_exit":0,"original_manager_exit":1,"original_stage_disposition":"INVALID_RECEIPT_VALIDATOR",
                "original_disposition_preserved":true,"native_reexecuted":false,"original_receipt_sha256":radial_parity.sha256,
                "descriptor_validation":"exact bytes and SHA; descriptor deliberately has no local path",
                "derived_outputs_full_body_hash_verified":true,"derived_outputs_retained_create_only":true,"performance_claim":false});
            let corrected_closure = put_json(&dir.path().join("corrected-closure"), &closure);
            let c = PrepConfig {
                schema: PREP_CONFIG.into(),
                inputs: ConstructionInputs {
                    generation_manifest: generation_pin,
                    plane_manifest: plane_pin,
                    centroids: centroid_pin,
                    max_owned_bytes: OWNED_CAP,
                },
                output_directory: dir.path().join("prepared"),
            };
            let a = ConstructionApproval {
                schema: CONSTRUCTION_APPROVAL.into(),
                original,
                original_approval,
                constructor: Constructor {
                    config: ctor_config,
                    archive: ctor_archive,
                    binary: ctor_binary,
                    implementation,
                },
                reconstruction,
                radial_parity,
                qualification,
                corrected_closure,
                expected_reconstruction,
                expected_radial_parity: rec,
                expected_qualification: q,
                expected_corrected_closure: closure,
            };
            let config = put_json(
                &dir.path().join("prep-config"),
                &serde_json::to_value(&c).unwrap(),
            );
            let approval = put_json(
                &dir.path().join("authority"),
                &serde_json::to_value(&a).unwrap(),
            );
            Self {
                dir,
                c,
                a,
                radial,
                spatial,
                config,
                approval,
            }
        }
        fn save(&mut self) {
            self.config = put_json(&self.config.path, &serde_json::to_value(&self.c).unwrap());
            self.approval = put_json(&self.approval.path, &serde_json::to_value(&self.a).unwrap());
        }
        fn prepare(&self, sync: impl FnMut(&File, bool) -> io::Result<()>) -> Result<bool> {
            prepare_with(
                &self.config.path,
                &self.config.sha256,
                &self.approval.path,
                &self.approval.sha256,
                sync,
            )
        }
        fn authenticate(&self) -> Result<()> {
            let mut stamps = Vec::new();
            authenticate_construction(&self.c.inputs, &self.a, &mut stamps)
        }
    }

    #[test]
    fn paired_source_only_config_refuses_query_truth_roles() {
        let f = Fixture::new(33, 3);
        for key in ["requests", "truth", "sq8", "source", "producer"] {
            let mut raw = serde_json::to_value(&f.c).unwrap();
            raw[key] = json!({"path":"/must-not-open"});
            assert!(serde_json::from_value::<PrepConfig>(raw).is_err(), "{key}");
            let mut raw = serde_json::to_value(&f.c).unwrap();
            raw["inputs"][key] = json!({"path":"/must-not-open"});
            assert!(
                serde_json::from_value::<PrepConfig>(raw).is_err(),
                "inputs/{key}"
            );
        }
        let raw = format!("{{\"schema\":\"{PREP_CONFIG}\",\"schema\":\"{PREP_CONFIG}\"}}");
        let p = put(&f.dir.path().join("duplicate-config"), raw.as_bytes());
        assert!(small::<PrepConfig>(&p.path, &p.sha256).is_err());
    }

    #[test]
    fn paired_real_d1024_public_build_tail_prepare_independent_reopen() {
        let f = Fixture::new(100353, 1024);
        assert!(f.prepare(|file, _| file.sync_all()).unwrap());
        let receipt: Value = serde_json::from_slice(
            &std::fs::read(f.c.output_directory.join("receipt.json")).unwrap(),
        )
        .unwrap();
        assert_eq!(receipt["status"], "SPATIAL_PREPARED");
        assert_eq!(receipt["truth_opened"], false);
        assert_eq!(
            receipt["constructor_implementation"],
            f.a.constructor.implementation
        );
        let p = Publication {
            root: put(
                &f.dir.path().join("reopen-root"),
                &std::fs::read(f.c.output_directory.join("spatial-root.bin")).unwrap(),
            ),
            membership: put(
                &f.dir.path().join("reopen-membership"),
                &std::fs::read(f.c.output_directory.join("spatial-membership.bin")).unwrap(),
            ),
            leaves: put(
                &f.dir.path().join("reopen-leaves"),
                &std::fs::read(f.c.output_directory.join("spatial-leaves.bin")).unwrap(),
            ),
        };
        same_publication(&p, &f.spatial).unwrap();
        let (b, _) = binary(&f.c.inputs.centroids, f.c.inputs.centroids.bytes).unwrap();
        let mut ledger =
            Ledger::new(OWNED_CAP, metadata_charge(&f.c.inputs, &f.a, 0).unwrap()).unwrap();
        let mut stamps = Vec::new();
        let map = import(
            &f.radial,
            &f.a.original,
            &b,
            LeafPartition::Radial64V1,
            None,
            &mut ledger,
            &mut stamps,
        )
        .unwrap()
        .unwrap();
        import(
            &p,
            &f.a.original,
            &b,
            LeafPartition::WidestAxisQuotaV1,
            Some(&map),
            &mut ledger,
            &mut stamps,
        )
        .unwrap();
        let mut nominations = Vec::new();
        for arm in [&f.radial, &p] {
            let (root, _) = binary(&arm.root, arm.root.bytes).unwrap();
            let (membership, _) = binary(&arm.membership, arm.membership.bytes).unwrap();
            let router = SemanticUnitRouter::open(
                &root,
                &membership,
                &arm.root.sha256,
                &f.a.original.identity(),
                4 * 1024 * 1024,
            )
            .unwrap();
            assert_eq!(router.manifest().leaves.len(), 50);
            assert_eq!(router.manifest().unit_count, 3137);
            assert_eq!(router.manifest().final_unit_rows, 1);
            assert_eq!(router.manifest().algorithm.training_centers, 1);
            let mut query = vec![0.; 1024];
            query[0] = 1.;
            let arm = coverage_arm(
                &router,
                &query,
                &borsuk::VectorMetric::SquaredEuclidean,
                100353,
                1024,
            )
            .unwrap();
            assert_eq!(arm.nomination.leaf_ids, (0..48).collect::<Vec<_>>());
            nominations.push(arm);
        }
        assert_ne!(
            nominations[0].nomination.units,
            nominations[1].nomination.units
        );
        assert_ne!(
            nominations[0].nomination.page_closure,
            nominations[1].nomination.page_closure
        );
    }

    #[test]
    fn paired_independent_and_rehashed_construction_corruptions() {
        let mut f = Fixture::new(4097, 3);
        f.authenticate().unwrap();
        for path in [
            &f.a.constructor.archive.path,
            &f.a.constructor.config.path,
            &f.a.constructor.binary.path,
            &f.a.original_approval.path,
            &f.c.inputs.generation_manifest.path,
            &f.c.inputs.plane_manifest.path,
            &f.a.reconstruction.path,
            &f.a.radial_parity.path,
            &f.a.qualification.path,
            &f.a.corrected_closure.path,
        ] {
            let original = std::fs::read(path).unwrap();
            let mut changed = original.clone();
            changed[0] ^= 1;
            std::fs::write(path, &changed).unwrap();
            assert!(f.authenticate().is_err(), "{}", path.display());
            std::fs::write(path, &original).unwrap();
        }
        let original = std::fs::read(&f.c.inputs.generation_manifest.path).unwrap();
        let mut g: Value = serde_json::from_slice(&original).unwrap();
        g["discovery"]["input_root_sha256"] = json!("f".repeat(64));
        f.c.inputs.generation_manifest = put_json(&f.c.inputs.generation_manifest.path, &g);
        f.save();
        assert!(f.prepare(|file, _| file.sync_all()).is_err());
        assert!(!f.c.output_directory.exists());
        let mut a = serde_json::to_value(&f.a).unwrap();
        a["expected_radial_parity"]["implementation"]["router_sha256"] = json!("f".repeat(64));
        let p = put_json(&f.dir.path().join("rehashed-authority"), &a);
        assert!(
            prepare_with(
                &f.config.path,
                &f.config.sha256,
                &p.path,
                &p.sha256,
                |file, _| file.sync_all()
            )
            .is_err()
        );
    }

    #[test]
    fn paired_centroid_and_arm_full_body_corruptions() {
        let f = Fixture::new(4097, 3);
        for pin in [
            &f.c.inputs.centroids,
            &f.radial.root,
            &f.radial.membership,
            &f.radial.leaves,
            &f.spatial.root,
            &f.spatial.membership,
            &f.spatial.leaves,
        ] {
            let original = std::fs::read(&pin.path).unwrap();
            for how in 0..3 {
                let mut changed = original.clone();
                match how {
                    0 => {
                        changed.pop();
                    }
                    1 => changed.push(0),
                    _ => {
                        let last = changed.len() - 1;
                        changed[last] ^= 1;
                    }
                }
                std::fs::write(&pin.path, &changed).unwrap();
                assert!(binary(pin, pin.bytes).is_err());
                std::fs::write(&pin.path, &original).unwrap();
            }
        }
        let (b, _) = binary(&f.c.inputs.centroids, f.c.inputs.centroids.bytes).unwrap();
        let mut root = std::fs::read(&f.spatial.root.path).unwrap();
        root[88..92].copy_from_slice(&1_u32.to_le_bytes());
        let mut p: Publication =
            serde_json::from_value(serde_json::to_value(&f.spatial).unwrap()).unwrap();
        p.root = put(&f.dir.path().join("rehashed-mode"), &root);
        let mut ledger = Ledger::new(OWNED_CAP, RESERVE).unwrap();
        let mut stamps = Vec::new();
        assert!(
            import(
                &p,
                &f.a.original,
                &b,
                LeafPartition::WidestAxisQuotaV1,
                None,
                &mut ledger,
                &mut stamps
            )
            .is_err()
        );
    }

    #[test]
    fn paired_every_unit_group_mutation_refused() {
        let f = Fixture::new(4097, 3);
        let (root, _) = binary(&f.spatial.root, f.spatial.root.bytes).unwrap();
        let (m, _) = binary(&f.spatial.membership, f.spatial.membership.bytes).unwrap();
        let router = SemanticUnitRouter::open(
            &root,
            &m,
            &f.spatial.root.sha256,
            &f.a.original.identity(),
            4 * 1024 * 1024,
        )
        .unwrap();
        let mut groups = group_map(&router).unwrap();
        compare_groups(&router, &groups).unwrap();
        for unit in 0..groups.len() {
            groups[unit] = 1;
            assert!(compare_groups(&router, &groups).is_err());
            groups[unit] = 0;
        }
        assert!(compare_groups(&router, &groups[..groups.len() - 1]).is_err());
    }

    #[test]
    fn paired_mode_and_cumulative_budget_boundaries() {
        let f = Fixture::new(4097, 1);
        let radial = *construction_phases(&f.a.original, LeafPartition::Radial64V1)
            .unwrap()
            .iter()
            .max()
            .unwrap();
        let spatial = *construction_phases(&f.a.original, LeafPartition::WidestAxisQuotaV1)
            .unwrap()
            .iter()
            .max()
            .unwrap();
        assert!(spatial > radial);
        let (root, _) = binary(&f.spatial.root, f.spatial.root.bytes).unwrap();
        let midpoint = (radial + (spatial - radial) / 2) as usize;
        let error = admit_authenticated_root(
            &root,
            &f.spatial.root.sha256,
            &f.a.original.identity(),
            midpoint,
        )
        .unwrap_err();
        assert_eq!(failure(admission_error(error).as_ref())["status"], "FAIL");
        assert_eq!(
            admit_authenticated_root(
                &root,
                &f.spatial.root.sha256,
                &f.a.original.identity(),
                spatial as usize
            )
            .unwrap()
            .1,
            spatial as usize
        );
        let mut ledger = Ledger::new(100, 20).unwrap();
        assert_eq!(ledger.phase("exact", &[40, 40]).unwrap(), 100);
        assert!(ledger.phase("cumulative", &[40, 41]).is_err());
        assert!(Ledger::new(OWNED_CAP + 1, 0).is_err());
        let (centroids, _) = binary(&f.c.inputs.centroids, f.c.inputs.centroids.bytes).unwrap();
        let cumulative =
            RESERVE + radial + centroids.len() as u64 + (f.a.original.rows.div_ceil(32) * 4) as u64;
        let mut short = Ledger::new(cumulative - 1, RESERVE).unwrap();
        let error = import(
            &f.radial,
            &f.a.original,
            &centroids,
            LeafPartition::Radial64V1,
            None,
            &mut short,
            &mut Vec::new(),
        )
        .unwrap_err();
        assert_eq!(failure(error.as_ref())["status"], "FAIL");
        let mut exact = Ledger::new(cumulative, RESERVE).unwrap();
        assert_eq!(
            import(
                &f.radial,
                &f.a.original,
                &centroids,
                LeafPartition::Radial64V1,
                None,
                &mut exact,
                &mut Vec::new()
            )
            .unwrap()
            .unwrap()
            .len(),
            129
        );
    }

    #[test]
    fn paired_prep_every_sync_failure_has_no_success_receipt() {
        let mut f = Fixture::new(4097, 3);
        let mut calls = 0;
        f.prepare(|file, _| {
            calls += 1;
            file.sync_all()
        })
        .unwrap();
        // Directory+parent, three blob file+directory, receipt file+directory+parent.
        assert_eq!(calls, 11);
        for boundary in 0..calls {
            f.c.output_directory = f.dir.path().join(format!("fail-{boundary}"));
            f.save();
            let mut current = 0;
            assert!(
                f.prepare(|file, _| {
                    let fail = current == boundary;
                    current += 1;
                    if fail {
                        Err(io::Error::other("injected sync"))
                    } else {
                        file.sync_all()
                    }
                })
                .is_err()
            );
            let p = f.c.output_directory.join("receipt.json");
            if p.exists() {
                let v: Value = serde_json::from_slice(&std::fs::read(p).unwrap()).unwrap();
                assert_eq!(v["status"], "INVALID");
            }
        }
    }

    fn inputs(rows: usize, d: usize) -> Inputs {
        Inputs {
            dataset: "CohereLabs/wikipedia-2023-11-embed-multilingual-v3".into(),
            revision: "ade45fb52bd549f5e8c065636fe4160a43c2af36".into(),
            metric: "cosine".into(),
            tie_rule: "corpus_ordinal_ascending".into(),
            rows,
            dimensions: d,
            count: 1,
            k: K,
            corpus_source_first: 0,
            query_source_first: rows + 1,
            profile: "scale1m".into(),
            store_root: String::new(),
            generation_prefix: "fixture".into(),
            generation_root_sha256: "1".repeat(64),
            requests_bytes: (d * 4) as u64,
            requests_sha256: "2".repeat(64),
            truth_bytes: 80,
            truth_sha256: "3".repeat(64),
            native_source_sha256: "2".repeat(64),
            native_sq8_sha256: "3".repeat(64),
            native_order_sha256: "4".repeat(64),
            truth_opened: false,
        }
    }
    fn query_line(a: &CoverageArm, i: &Inputs) -> (Value, Query) {
        let n = &a.nomination;
        let scored = n
            .page_closure
            .iter()
            .flat_map(|p| p * 8..((p + 1) * 8).min(i.rows.div_ceil(32)))
            .collect::<Vec<_>>();
        let page = *n.page_closure.first().unwrap();
        let width = i.dimensions + 12;
        let range = [page * 256 * width, ((page + 1) * 256).min(i.rows) * width];
        let source = Charge {
            submitted_gets: a.ranges.len() as u64,
            verified_bytes: a.bytes as u64,
            failed_gets: 0,
        };
        let sq8 = Charge {
            submitted_gets: 1,
            verified_bytes: (range[1] - range[0]) as u64,
            failed_gets: 0,
        };
        let charges = Charges {
            router: Charge::default(),
            source,
            sq8,
        };
        let line = json!({"ordinal":0,"truth_opened":false,"returned":[],"returned_count":0,"underfill":true,
            "charges":charges,"sum":charges.sum().unwrap(),"stages":{"discovery":{"start_ns":0,"end_ns":1},
                "source":{"start_ns":1,"end_ns":2},"planning":{"start_ns":2,"end_ns":3},
                "sq8":{"start_ns":3,"end_ns":4},"leaf_peak_inflight":1},"query_wall_ns":4,"query_process_cpu_ns":4,
            "serving":{"mode":"baseline"},"source_nomination_skipped":false,
            "trace":{"ranked_candidate_pages":n.page_closure,"nomination_evaluated_units":scored,"primary_page":page,
                "discoveries":[],"semantic_leaves":n.leaf_ids,"semantic_units":n.units,"semantic_seed_additions":n.seed_additions},
            "plan":{"selected_pages":[page],"ranges":[range],"planned_bytes":range[1]-range[0]}});
        let q = serde_json::from_value(line.clone()).unwrap();
        (line, q)
    }

    #[test]
    fn paired_same_count_leaf_unit_seed_and_order_control_mismatch() {
        let f = Fixture::new(100353, 1024);
        let (root, _) = binary(&f.radial.root, f.radial.root.bytes).unwrap();
        let (membership, _) = binary(&f.radial.membership, f.radial.membership.bytes).unwrap();
        let router = SemanticUnitRouter::open(
            &root,
            &membership,
            &f.radial.root.sha256,
            &f.a.original.identity(),
            4 * 1024 * 1024,
        )
        .unwrap();
        // Odd leaf IDs leave a partial semantic seed, so a same-count seed mutation is meaningful.
        let selected = (0..50).filter(|&id| id != 0 && id != 2).collect::<Vec<_>>();
        let mut a = coverage_selected_arm(&router, &selected, 100353, 1024).unwrap();
        // For this contiguous radial leaf fixture, inject an independently valid partial-seed selection
        // through the existing SOURCE seam; the group/public-builder fixture remains unchanged.
        a.nomination.units.remove(&64);
        a.nomination.seed_additions = vec![64];
        let i = inputs(100353, 1024);
        let (line, q) = query_line(&a, &i);
        coverage_control(&serde_json::to_vec(&line).unwrap(), &q, &i, &a).unwrap();
        for field in [
            "semantic_leaves",
            "semantic_units",
            "semantic_seed_additions",
        ] {
            let mut changed = line.clone();
            let values = changed["trace"][field].as_array_mut().unwrap();
            let count = values.len();
            if field == "semantic_leaves" {
                values.swap(0, 1);
            } else if field == "semantic_units" {
                values[0] = json!(66);
            } else {
                values[0] = json!(65);
            }
            assert_eq!(values.len(), count);
            assert!(
                coverage_control(&serde_json::to_vec(&changed).unwrap(), &q, &i, &a).is_err(),
                "{field}"
            );
        }
        let expected = CoverageScope::consumed32();
        let mut changed = expected.ordinals.clone();
        changed.swap(0, 1);
        assert_ne!(changed, expected.ordinals);
        assert!(
            scale_ordinals(
                &json!({"execution":{"mode":"diagnostic_panel","trace":false,"ordinals":changed}}),
                1000
            )
            .is_err()
        );
        let mut changed = line;
        changed["charges"]["source"]["verified_bytes"] = json!(a.bytes - 1);
        let changed_q: Query = serde_json::from_value(changed.clone()).unwrap();
        assert!(
            coverage_control(&serde_json::to_vec(&changed).unwrap(), &changed_q, &i, &a).is_err()
        );
    }

    fn empty_run(i: &Inputs, pin: &CompletedInput) -> Run {
        let identity = Identity {
            schema: String::new(),
            config_sha256: String::new(),
            binary_sha256: String::new(),
            runner_source_sha256: String::new(),
            generation_source_sha256: String::new(),
            router_source_sha256: String::new(),
            codec_source_sha256: String::new(),
            source_plane_source_sha256: String::new(),
            scope: String::new(),
            physical_s3_measured: false,
            io_measurement: String::new(),
            external_gate_required: true,
            truth_opened: false,
        };
        let terminal=serde_json::from_value(json!({"status":"COMPLETE","complete":true,"queries":0,"k":10,
            "total_hits10":0,"recall_numerator":0,"recall_denominator":1,"mean_recall10":0.0,"underfilled_queries":0,
            "all_queries_sealed":true,"prefix_bytes":1,"prefix_sha256":"1".repeat(64),"sealed_bytes":1,
            "sealed_sha256":"1".repeat(64),"requests_sha256":i.requests_sha256,"truth_sha256":i.truth_sha256,
            "generation_root_sha256":i.generation_root_sha256,"charges":Charges::default(),"sum":Charge::default(),
            "query_wall_ns":0,"query_process_cpu_ns":0,"physical_s3_measured":false,"external_gate_required":true,
            "process_wall_ns":0,"process_cpu_ns":0,"observed_process_peak_bytes":null})).unwrap();
        Run {
            identity,
            inputs: serde_json::from_value(serde_json::to_value(i).unwrap()).unwrap(),
            samples: vec![],
            stages: StageTotals::default(),
            terminal,
            file_identity: file_identity(&header_file(&pin.path).unwrap()).unwrap(),
            completed: None,
        }
    }
    fn screen_config(f: &Fixture) -> ScreenConfig {
        ScreenConfig {
            schema: SCREEN_CONFIG.into(),
            inputs: serde_json::from_value(serde_json::to_value(&f.c.inputs).unwrap()).unwrap(),
            radial: serde_json::from_value(serde_json::to_value(&f.radial).unwrap()).unwrap(),
            spatial: serde_json::from_value(serde_json::to_value(&f.spatial).unwrap()).unwrap(),
            spatial_receipt: CompletedInput {
                path: f.dir.path().join("absent-spatial-receipt"),
                bytes: 1,
                sha256: "1".repeat(64),
            },
            requests: CompletedInput {
                path: f.dir.path().join("absent-requests"),
                bytes: 4096,
                sha256: "2".repeat(64),
            },
            sq8: CompletedInput {
                path: f.dir.path().join("absent-sq8"),
                bytes: (f.a.original.rows * 1036) as u64,
                sha256: "3".repeat(64),
            },
            truth: CompletedInput {
                path: f.dir.path().join("absent-truth"),
                bytes: 80,
                sha256: "4".repeat(64),
            },
        }
    }

    #[test]
    fn paired_selection_sync_alias_rewrite_directory_boundaries_truth_spy() {
        let f = Fixture::new(100353, 1024);
        let c = screen_config(&f);
        let i = inputs(100353, 1024);
        let mut arms = Vec::new();
        for p in [&f.radial, &f.spatial] {
            let (root, _) = binary(&p.root, p.root.bytes).unwrap();
            let (m, _) = binary(&p.membership, p.membership.bytes).unwrap();
            let router = SemanticUnitRouter::open(
                &root,
                &m,
                &p.root.sha256,
                &f.a.original.identity(),
                4 * 1024 * 1024,
            )
            .unwrap();
            let mut q = vec![0.; 1024];
            q[0] = 1.;
            arms.push(
                coverage_arm(
                    &router,
                    &q,
                    &borsuk::VectorMetric::SquaredEuclidean,
                    100353,
                    1024,
                )
                .unwrap(),
            );
        }
        let scope = CoverageScope {
            rows: 100353,
            count: 1,
            ordinals: vec![0],
            original_hits: 0,
            original_coverage: 0,
        };
        let run = empty_run(&i, &f.config);
        let seal = json!({"schema":"paired-real-arm-sync-fixture","status":"SEALED","radial":coverage_arm_value(&arms[0]),
            "spatial":coverage_arm_value(&arms[1]),"truth_opened":false});
        for mutation in 0..9 {
            let output_dir = f.dir.path().join(format!("seal-{mutation}"));
            std::fs::create_dir(&output_dir).unwrap();
            let selection = output_dir.join("selection");
            let replacement = output_dir.join("replacement");
            let input = put(
                &f.dir.path().join(format!("input-{mutation}")),
                b"immutable selection input",
            );
            if mutation == 6 {
                std::fs::hard_link(&input.path, &selection).unwrap();
            }
            if mutation == 7 {
                std::os::unix::fs::symlink(&input.path, &selection).unwrap();
            }
            let stamp = stream(&input, FILE_CAP).unwrap_or_else(|_| {
                // Hard-link aliases are rejected before any truth opener is reached.
                HeaderSnapshot {
                    file: open_input(&input.path).unwrap(),
                    path: input.path.clone(),
                    identity: file_identity(&open_input(&input.path).unwrap()).unwrap(),
                }
            });
            let mut opened = 0;
            let mut boundary = 0;
            let mut sync = |file: &File, is_dir: bool| -> io::Result<()> {
                let current = boundary;
                boundary += 1;
                if mutation == current && mutation < 2 {
                    return Err(io::Error::other("injected seal sync"));
                }
                file.sync_all()?;
                if mutation == 2 && !is_dir {
                    let bytes = std::fs::read(&selection)?;
                    std::fs::write(&selection, bytes)?;
                }
                if mutation == 3 && is_dir {
                    let bytes = std::fs::read(&selection)?;
                    std::fs::write(&selection, bytes)?;
                }
                if mutation == 4 && !is_dir {
                    std::fs::rename(&selection, &replacement)?;
                    std::fs::write(&selection, b"replacement")?;
                }
                if mutation == 5 && is_dir {
                    std::fs::rename(&output_dir, output_dir.with_extension("old"))?;
                    std::fs::create_dir(&output_dir)?;
                }
                if mutation == 8 && is_dir {
                    let bytes = std::fs::read(&input.path)?;
                    std::fs::write(&input.path, bytes)?;
                }
                Ok(())
            };
            let mut truth = |_: &CompletedInput, _: u64| -> Result<(Vec<u8>, HeaderSnapshot)> {
                opened += 1;
                Err("truth spy reached".into())
            };
            let mut ledger = Ledger::new(OWNED_CAP, RESERVE).unwrap();
            assert!(
                seal_then_reduce(
                    &c,
                    &i,
                    &scope,
                    &arms[..1],
                    &arms[1..],
                    &run,
                    &[],
                    &seal,
                    &selection,
                    vec![stamp],
                    &mut ledger,
                    &mut sync,
                    &mut truth
                )
                .is_err(),
                "mutation {mutation}"
            );
            assert_eq!(opened, 0, "mutation {mutation}");
            if mutation < 4 {
                let v: Value = serde_json::from_slice(&std::fs::read(&selection).unwrap()).unwrap();
                assert_eq!(v["status"], "INVALID");
            }
        }
        let selection = f.dir.path().join("valid-seal");
        let mut opened = 0;
        let mut truth = |_: &CompletedInput, _: u64| -> Result<(Vec<u8>, HeaderSnapshot)> {
            opened += 1;
            Err("stop after first permitted truth open".into())
        };
        let mut ledger = Ledger::new(OWNED_CAP, RESERVE).unwrap();
        assert!(
            seal_then_reduce(
                &c,
                &i,
                &scope,
                &arms[..1],
                &arms[1..],
                &run,
                &[],
                &seal,
                &selection,
                vec![],
                &mut ledger,
                &mut |file, _| file.sync_all(),
                &mut truth
            )
            .is_err()
        );
        assert_eq!(opened, 1);
        assert!(!c.truth.path.exists());
    }

    #[test]
    fn paired_report_file_directory_sync_and_rewrite_invalidate() {
        let dir = tempfile::tempdir().unwrap();
        for mutation in 0..4 {
            let output = dir.path().join(format!("report-{mutation}"));
            let parent_dir = parent(&output).unwrap();
            let file = File::from(
                openat(
                    &parent_dir,
                    output.file_name().unwrap(),
                    OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL,
                    Mode::RUSR | Mode::WUSR,
                )
                .unwrap(),
            );
            let mut boundary = 0;
            assert!(
                finish_selection_publication(
                    &output,
                    &json!({"schema":REPORT_SCHEMA,"status":"PASS"}),
                    &[],
                    parent_dir,
                    file,
                    |file, is_dir| checked_sync(file, is_dir, &mut |file, is_dir| {
                        let current = boundary;
                        boundary += 1;
                        if mutation == current && mutation < 2 {
                            return Err(io::Error::other("injected report sync"));
                        }
                        file.sync_all()?;
                        if (mutation == 2 && !is_dir) || (mutation == 3 && is_dir) {
                            let bytes = std::fs::read(&output)?;
                            std::fs::write(&output, bytes)?;
                        }
                        Ok(())
                    })
                )
                .is_err()
            );
            let value: Value = serde_json::from_slice(&std::fs::read(&output).unwrap()).unwrap();
            assert_eq!(value["status"], "INVALID");
        }
    }

    #[test]
    fn paired_literal_classifications_and_bridge_only_exclusion() {
        assert_eq!(
            failure(&ResourceFailure("valid budget".into()))["status"],
            "FAIL"
        );
        assert_eq!(failure(&io::Error::other("sync"))["status"], "INVALID");
        assert!(coverage_survives(314, 315, 2, 1));
        assert!(!coverage_survives(314, 314, 1, 1));
        assert!(!coverage_survives(313, 315, 2, 0));
        assert!(!coverage_survives(314, 315, 1, 1));
        let a = CoverageArm {
            nomination: borsuk::semantic_unit_router::Nomination {
                leaf_ids: vec![0],
                units: BTreeSet::from([0, 16]),
                seed_page: 0,
                walk_units: vec![0, 16],
                seed_additions: vec![],
                page_closure: BTreeSet::from([0, 2]),
            },
            ranges: vec![0..3 * 256 * 9],
            bytes: 3 * 256 * 9,
        };
        assert_eq!(coverage_slot(&a, 0, 9), (true, true));
        assert_eq!(coverage_slot(&a, 256, 9), (false, true));
        assert_eq!(coverage_slot(&a, 512, 9), (true, true));
        assert_eq!(coverage_slot(&a, 768, 9), (false, false));
    }

    fn source_authority(f: &Fixture, c: &mut ScreenConfig) -> ScreenApproval {
        f.prepare(|file, _| file.sync_all()).unwrap();
        c.spatial_receipt = put(
            &f.dir.path().join("frozen-spatial-receipt"),
            &std::fs::read(f.c.output_directory.join("receipt.json")).unwrap(),
        );
        let expected_spatial_receipt =
            serde_json::from_slice(&std::fs::read(&c.spatial_receipt.path).unwrap()).unwrap();
        let archive = put(
            &f.dir.path().join("producer-archive"),
            b"independently approved ORIGINAL query producer archive fixture",
        );
        let elf = put(
            &f.dir.path().join("producer-elf"),
            b"ORIGINAL query producer ELF, distinct from constructor fixture",
        );
        let result = put(
            &f.dir.path().join("producer-result"),
            b"whole producer body fixture; deliberately not a qualifying native run",
        );
        let mut i = inputs(f.a.original.rows, 1024);
        i.truth_sha256 = c.truth.sha256.clone();
        let mut bound = serde_json::to_value(&i).unwrap();
        bound.as_object_mut().unwrap().remove("store_root");
        bound["phase"] = json!("bound_inputs");
        bound["generation_root_sha256"] = json!(c.inputs.generation_manifest.sha256);
        bound["backend"] = json!({"kind":"local","store_root":f.dir.path()});
        bound["credential_source"] = Value::Null;
        bound["serving"] = json!({"mode":"baseline"});
        bound["execution"] = json!({"mode":"diagnostic_panel","ordinals":[0],"trace":false});
        bound["selected_count"] = json!(1);
        bound["corpus_intervals"] = json!([{"start":0,"end":i.rows}]);
        bound["reserved_query_interval"] =
            json!({"start":i.query_source_first,"end":i.query_source_first+1});
        bound["reserved_queries_sha256"] = json!(i.requests_sha256);
        bound["cohort_receipt_sha256"] = json!("a".repeat(64));
        bound["derivation_receipt_sha256"] = json!("b".repeat(64));
        bound["max_memory_bytes"] = json!(512 * 1024 * 1024_u64);
        bound["source_cache"] = json!("off");
        bound["fetch_parallelism"] = json!(16);
        bound["producer_authority"] = json!({"source_commit":"c".repeat(40),"executable_sha256":"d".repeat(64),
            "producer_source_sha256":"e".repeat(64),"sq8_source_sha256":"f".repeat(64),"source_order_source_sha256":"1".repeat(64)});
        let mut config = json!({"schema":"borsuk-cohere-native-baseline-config-v7","scratch_parent":f.dir.path(),
            "cohort_receipt":{"path":f.dir.path().join("cohort"),"bytes":1,"sha256":"a".repeat(64)},
            "derivation_receipt":{"path":f.dir.path().join("derivation"),"bytes":1,"sha256":"b".repeat(64)},
            "requests":c.requests,"truth":c.truth,"native_source":{"source_sha256":i.native_source_sha256,
                "sq8_sha256":i.native_sq8_sha256,"source_order_sha256":i.native_order_sha256}});
        for key in "dataset revision metric tie_rule corpus_intervals reserved_query_interval producer_authority corpus_source_first query_source_first rows dimensions count k profile backend generation_prefix generation_root_sha256 max_memory_bytes fetch_parallelism serving execution".split_whitespace() {
            config[key]=bound[key].clone();
        }
        let config_pin = put_json(&f.dir.path().join("producer-config"), &config);
        let mut identity = json!({"phase":"identity","schema":"borsuk-cohere-native-baseline-result-v7",
            "config_sha256":config_pin.sha256,"binary_sha256":elf.sha256,
            "scope":"AUTHENTICATED_NATIVE_QUALITY_CORRECTNESS","physical_s3_measured":false,
            "io_measurement":"logical_GET_charges_separate_from_cumulative_process_native_transport",
            "s3_credential_source":"imds_instance_role_only","native_transport_includes":"S3_and_IMDS_credential_requests_including_PUT",
            "wire_bytes":null,"unread_bytes":null,"billed_bytes":null,"billed_requests":null,
            "external_gate_required":true,"truth_opened":false,"fetch_parallelism":16,
            "serving":bound["serving"],"execution":bound["execution"]});
        for key in "runner_source_sha256 generation_source_sha256 router_source_sha256 codec_source_sha256 source_plane_source_sha256 sq8_range_source_sha256 returned_source_sha256".split_whitespace() {
            identity[key]=json!("9".repeat(64));
        }
        ScreenApproval {
            schema: SCREEN_APPROVAL.into(),
            construction: serde_json::from_value(serde_json::to_value(&f.a).unwrap()).unwrap(),
            producer: Producer {
                result,
                config: config_pin,
                archive,
                binary: elf,
                expected_config: config,
                expected_identity: identity,
                expected_bound_inputs: bound,
            },
            radial: serde_json::from_value(serde_json::to_value(&f.radial).unwrap()).unwrap(),
            spatial: serde_json::from_value(serde_json::to_value(&f.spatial).unwrap()).unwrap(),
            spatial_receipt: copy_pin(&c.spatial_receipt),
            expected_spatial_receipt,
        }
    }

    #[test]
    fn paired_original_producer_archive_config_elf_whole_body_and_rehashed_pins() {
        let f = Fixture::new(4097, 1024);
        let mut c = screen_config(&f);
        let a = source_authority(&f, &mut c);
        let scope = CoverageScope {
            rows: 4097,
            count: 1,
            ordinals: vec![0],
            original_hits: 0,
            original_coverage: 0,
        };
        authenticate_producer(&a, &c, &scope, &mut Vec::new()).unwrap();
        for pin in [
            &a.producer.archive,
            &a.producer.config,
            &a.producer.binary,
            &a.producer.result,
        ] {
            let original = std::fs::read(&pin.path).unwrap();
            for how in 0..3 {
                let mut body = original.clone();
                match how {
                    0 => {
                        body.pop();
                    }
                    1 => body.push(0),
                    _ => {
                        let last = body.len() - 1;
                        body[last] ^= 1;
                    }
                }
                std::fs::write(&pin.path, body).unwrap();
                assert!(authenticate_producer(&a, &c, &scope, &mut Vec::new()).is_err());
                std::fs::write(&pin.path, &original).unwrap();
            }
        }
        let mut changed: ScreenApproval =
            serde_json::from_value(serde_json::to_value(&a).unwrap()).unwrap();
        changed.producer.config = put_json(
            &f.dir.path().join("new-producer-config"),
            &json!({"schema":"changed"}),
        );
        changed.producer.expected_config = json!({"schema":"changed"});
        assert!(authenticate_producer(&changed, &c, &scope, &mut Vec::new()).is_err());
        let mut changed: ScreenApproval =
            serde_json::from_value(serde_json::to_value(&a).unwrap()).unwrap();
        changed.producer.expected_identity["binary_sha256"] = json!(f.a.constructor.binary.sha256);
        assert!(authenticate_producer(&changed, &c, &scope, &mut Vec::new()).is_err());
        let mut changed: ScreenApproval =
            serde_json::from_value(serde_json::to_value(&a).unwrap()).unwrap();
        changed.producer.expected_bound_inputs["execution"]["ordinals"] = json!([1]);
        changed.producer.expected_identity["execution"]["ordinals"] = json!([1]);
        assert!(authenticate_producer(&changed, &c, &scope, &mut Vec::new()).is_err());
        assert!(!c.truth.path.exists());
    }

    #[test]
    fn paired_frozen_spatial_receipt_and_arm_rehash_refused_before_truth() {
        let f = Fixture::new(4097, 1024);
        let mut c = screen_config(&f);
        let a = source_authority(&f, &mut c);
        authenticate_spatial_receipt(&a, &c, &mut Vec::new()).unwrap();
        c.spatial.root.sha256 = "e".repeat(64);
        assert!(authenticate_spatial_receipt(&a, &c, &mut Vec::new()).is_err());
        c.spatial.root.sha256 = f.spatial.root.sha256.clone();
        let mut value = a.expected_spatial_receipt.clone();
        value["centroids"]["sha256"] = json!("f".repeat(64));
        c.spatial_receipt = put_json(&f.dir.path().join("rehashed-spatial-receipt"), &value);
        assert!(authenticate_spatial_receipt(&a, &c, &mut Vec::new()).is_err());
        assert!(!c.truth.path.exists());
    }

    #[test]
    fn paired_output_and_input_aliases_refused_with_absent_truth_spy() {
        let f = Fixture::new(4097, 1024);
        let mut c = screen_config(&f);
        let a = source_authority(&f, &mut c);
        let selection = f.dir.path().join("selection");
        let report = f.dir.path().join("report");
        output_roles(&c, &a, &selection, &report).unwrap();
        assert!(output_roles(&c, &a, &c.truth.path, &report).is_err());
        assert!(output_roles(&c, &a, &selection, &c.truth.path).is_err());
        assert!(output_roles(&c, &a, &selection, &selection).is_err());
        c.truth.path = c.requests.path.clone();
        assert!(output_roles(&c, &a, &selection, &report).is_err());
        assert!(!f.dir.path().join("absent-truth").exists());
    }

    #[test]
    fn paired_preparation_resource_failure_is_durable_fail() {
        let mut f = Fixture::new(33, 3);
        f.c.inputs.max_owned_bytes = 1;
        f.save();
        assert!(
            !prepare(
                &f.config.path,
                &f.config.sha256,
                &f.approval.path,
                &f.approval.sha256
            )
            .unwrap()
        );
        let value: Value = serde_json::from_slice(
            &std::fs::read(f.c.output_directory.join("receipt.json")).unwrap(),
        )
        .unwrap();
        assert_eq!(value["status"], "FAIL");
        assert_eq!(value["resource_gate_failed"], true);
        assert_eq!(value["truth_opened"], false);
        assert!(!f.c.output_directory.join("spatial-root.bin").exists());
    }
}
