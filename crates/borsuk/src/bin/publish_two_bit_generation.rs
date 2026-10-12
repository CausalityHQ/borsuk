//! Publish a fresh local generation or rebind an authenticated retained generation to Local/S3.
use borsuk::{
    semantic_unit_router::{Artifact, LeafPartition, SemanticProfile, SourceIdentity},
    sq8_s3_range::OneAttemptS3,
    two_bit_generation::TwoBitGenerationLimits,
    two_bit_store::{
        RetainedRouterBody, RetainedRouterPublication, RetainedSemanticRouterReplacement,
        RetainedTwoBitApproval, publish_two_bit_generation, read_two_bit_head,
        republish_retained_two_bit_generation,
    },
};
use object_store::{local::LocalFileSystem, path::Path as ObjectPath};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::{
    error::Error,
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    os::unix::fs::{MetadataExt, OpenOptionsExt},
    path::{Path, PathBuf},
};

type Result<T> = std::result::Result<T, Box<dyn Error>>;
const CONFIG_CAP: u64 = 65_536;
const CONFIG_SCHEMA: &str = "borsuk-two-bit-local-publication-config-v1";
const RECEIPT_SCHEMA: &str = "borsuk-two-bit-local-publication-receipt-v1";
const RETAINED_CONFIG_SCHEMA: &str = "borsuk-two-bit-retained-publication-config-v3";
const RETAINED_RECEIPT_SCHEMA: &str = "borsuk-two-bit-retained-publication-receipt-v3";
const REPLACEMENT_APPROVAL_SCHEMA: &str = "borsuk-retained-router-replacement-approval-v1";

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    schema: String,
    root: Root,
    store_root: PathBuf,
    prefix: String,
    limits: Limits,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Root {
    path: PathBuf,
    sha256: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RetainedConfig {
    schema: String,
    backend: RetainedBackend,
    retained_prefix: String,
    original_root_sha256: String,
    original_generation: u64,
    original_control_epoch: u64,
    sq8_object_key: String,
    sq8_etag: String,
    destination_prefix: String,
    scratch_parent: PathBuf,
    max_scratch_bytes: u64,
    limits: Limits,
    #[serde(deserialize_with = "Option::deserialize")]
    router_replacement: Option<RouterReplacementConfig>,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct FilePin {
    path: PathBuf,
    bytes: usize,
    sha256: String,
}
#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct RouterFiles {
    root: FilePin,
    membership: FilePin,
    leaves: FilePin,
}
#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct RouterPins {
    root: Artifact,
    membership: Artifact,
    leaves: Artifact,
}
#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ProofFiles {
    construction: FilePin,
    radial_parity: FilePin,
    corrected_closure: FilePin,
    spatial: FilePin,
    source_qualification: FilePin,
    source_authority: FilePin,
    source_config: FilePin,
}
#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ApprovedProof {
    pin: Artifact,
    expected: serde_json::Value,
}
#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ProofApproval {
    construction: ApprovedProof,
    radial_parity: ApprovedProof,
    corrected_closure: ApprovedProof,
    spatial: ApprovedProof,
    source_qualification: ApprovedProof,
    source_authority: Artifact,
    source_config: Artifact,
}
#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct OriginalConstruction {
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
#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct RouterReplacementConfig {
    centroids: FilePin,
    radial: RouterFiles,
    replacement: RouterFiles,
    proofs: ProofFiles,
}
#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ReplacementApproval {
    schema: String,
    config_sha256: String,
    original: OriginalConstruction,
    original_discovery: serde_json::Value,
    centroids: Artifact,
    radial: RouterPins,
    spatial: RouterPins,
    replacement_partition: LeafPartition,
    proofs: ProofApproval,
}
impl ReplacementApproval {
    fn input(&self) -> SourceIdentity<'_> {
        SourceIdentity {
            profile: self.original.profile,
            schema: &self.original.input_schema,
            root_sha256: &self.original.input_root_sha256,
            centroids_sha256: &self.original.centroids_sha256,
            rows: self.original.rows,
            dimensions: self.original.dimensions,
        }
    }
    fn replacement(&self) -> &RouterPins {
        match self.replacement_partition {
            LeafPartition::Radial64V1 => &self.radial,
            LeafPartition::WidestAxisQuotaV1 => &self.spatial,
        }
    }
}
#[derive(Deserialize, Serialize)]
#[serde(tag = "kind", deny_unknown_fields)]
enum RetainedBackend {
    Local {
        store_root: PathBuf,
    },
    S3 {
        bucket: String,
        region: String,
        namespace: String,
        request_timeout_seconds: u64,
    },
}
// Every field is mandatory: the caller states the admission, nothing is defaulted.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Limits {
    max_memory_bytes: u64,
    max_active_queries: usize,
    max_query_bytes: usize,
    max_query_gets: usize,
    max_parallel_gets: usize,
    max_source_bytes: usize,
    max_source_gets: usize,
    max_parallel_source_gets: usize,
    max_query_scratch_bytes: usize,
    already_pinned_bytes: u64,
}
impl From<&Limits> for TwoBitGenerationLimits {
    fn from(l: &Limits) -> Self {
        Self {
            max_memory_bytes: l.max_memory_bytes,
            max_active_queries: l.max_active_queries,
            max_query_bytes: l.max_query_bytes,
            max_query_gets: l.max_query_gets,
            max_parallel_gets: l.max_parallel_gets,
            max_source_bytes: l.max_source_bytes,
            max_source_gets: l.max_source_gets,
            max_parallel_source_gets: l.max_parallel_source_gets,
            max_query_scratch_bytes: l.max_query_scratch_bytes,
            already_pinned_bytes: l.already_pinned_bytes,
        }
    }
}

fn sha_hex(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}
fn is_sha(s: &str) -> bool {
    s.len() == 64 && s.bytes().all(|c| matches!(c, b'0'..=b'9' | b'a'..=b'f'))
}

// Receipt equality must not silently discard duplicate JSON fields.
struct UniqueJson(serde_json::Value);
impl<'de> Deserialize<'de> for UniqueJson {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        struct Visitor;
        impl<'de> serde::de::Visitor<'de> for Visitor {
            type Value = UniqueJson;
            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("unique JSON")
            }
            fn visit_map<M: serde::de::MapAccess<'de>>(
                self,
                mut m: M,
            ) -> std::result::Result<Self::Value, M::Error> {
                let mut values = serde_json::Map::new();
                while let Some(key) = m.next_key::<String>()? {
                    if values.contains_key(&key) {
                        return Err(serde::de::Error::custom("duplicate JSON field"));
                    }
                    values.insert(key, m.next_value::<UniqueJson>()?.0);
                }
                Ok(UniqueJson(values.into()))
            }
            fn visit_seq<S: serde::de::SeqAccess<'de>>(
                self,
                mut s: S,
            ) -> std::result::Result<Self::Value, S::Error> {
                let mut values = Vec::new();
                while let Some(value) = s.next_element::<UniqueJson>()? {
                    values.push(value.0);
                }
                Ok(UniqueJson(values.into()))
            }
            fn visit_bool<E: serde::de::Error>(
                self,
                v: bool,
            ) -> std::result::Result<Self::Value, E> {
                Ok(UniqueJson(v.into()))
            }
            fn visit_i64<E: serde::de::Error>(self, v: i64) -> std::result::Result<Self::Value, E> {
                Ok(UniqueJson(v.into()))
            }
            fn visit_u64<E: serde::de::Error>(self, v: u64) -> std::result::Result<Self::Value, E> {
                Ok(UniqueJson(v.into()))
            }
            fn visit_f64<E: serde::de::Error>(self, v: f64) -> std::result::Result<Self::Value, E> {
                serde_json::Number::from_f64(v)
                    .map(|n| UniqueJson(n.into()))
                    .ok_or_else(|| E::custom("JSON number"))
            }
            fn visit_str<E: serde::de::Error>(
                self,
                v: &str,
            ) -> std::result::Result<Self::Value, E> {
                Ok(UniqueJson(v.into()))
            }
            fn visit_unit<E: serde::de::Error>(self) -> std::result::Result<Self::Value, E> {
                Ok(UniqueJson(serde_json::Value::Null))
            }
        }
        d.deserialize_any(Visitor)
    }
}
fn unique_json(bytes: &[u8]) -> Result<serde_json::Value> {
    Ok(serde_json::from_slice::<UniqueJson>(bytes)?.0)
}

fn descriptor(value: &serde_json::Value, pin: &Artifact) -> bool {
    value["bytes"].as_u64() == Some(pin.bytes as u64) && value["sha256"] == pin.sha256
}
fn publication_descriptor(value: &serde_json::Value, pins: &RouterPins) -> bool {
    descriptor(&value["root"], &pins.root)
        && descriptor(&value["membership"], &pins.membership)
        && descriptor(&value["leaves"], &pins.leaves)
}
fn file_pin(pin: &FilePin, approved: &Artifact, cap: usize) -> Result<()> {
    if pin.bytes == 0
        || pin.bytes > cap
        || pin.bytes != approved.bytes
        || pin.sha256 != approved.sha256
        || !is_sha(&approved.sha256)
        || pin.path.as_os_str().len() > 4096
    {
        return Err("replacement independent file pin".into());
    }
    Ok(())
}
fn file_identity(meta: &fs::Metadata) -> (u64, u64, u64, i64, i64, i64, i64) {
    (
        meta.dev(),
        meta.ino(),
        meta.len(),
        meta.mtime(),
        meta.mtime_nsec(),
        meta.ctime(),
        meta.ctime_nsec(),
    )
}
fn replacement_body(pin: &FilePin, approved: &Artifact, cap: usize) -> Result<Box<[u8]>> {
    file_pin(pin, approved, cap)?;
    let mut file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(&pin.path)?;
    let before = file.metadata()?;
    if !before.is_file() || before.nlink() != 1 || before.len() != pin.bytes as u64 {
        return Err("replacement regular immutable file".into());
    }
    let mut body = vec![0_u8; pin.bytes].into_boxed_slice();
    file.read_exact(&mut body)?;
    let mut eof = [0_u8; 1];
    if file.read(&mut eof)? != 0
        || sha_hex(&body) != approved.sha256
        || file_identity(&before) != file_identity(&file.metadata()?)
        || file_identity(&before) != file_identity(&fs::symlink_metadata(&pin.path)?)
    {
        return Err("replacement file SHA/length/EOF/identity".into());
    }
    Ok(body)
}

fn replacement_proofs(c: &RouterReplacementConfig, a: &ReplacementApproval) -> Result<()> {
    for (file, proof) in [
        (&c.proofs.construction, &a.proofs.construction),
        (&c.proofs.radial_parity, &a.proofs.radial_parity),
        (&c.proofs.corrected_closure, &a.proofs.corrected_closure),
        (&c.proofs.spatial, &a.proofs.spatial),
        (
            &c.proofs.source_qualification,
            &a.proofs.source_qualification,
        ),
    ] {
        let body = replacement_body(file, &proof.pin, CONFIG_CAP as usize)?;
        if unique_json(&body)? != proof.expected {
            return Err("replacement approved proof body".into());
        }
    }
    let original = serde_json::to_value(&a.original)?;
    let q = &a.proofs.construction.expected;
    let p = &a.proofs.radial_parity.expected;
    let h = &p["historical_parity"];
    for name in ["root", "membership", "leaves"] {
        if h[format!("old_{name}")]["bytes"] != a.original_discovery[format!("{name}_bytes")]
            || h[format!("old_{name}")]["sha256"] != a.original_discovery[format!("{name}_sha256")]
        {
            return Err("replacement historical artifact provenance".into());
        }
    }
    let closure = &a.proofs.corrected_closure.expected;
    let spatial = &a.proofs.spatial.expected;
    let source = &a.proofs.source_qualification.expected;
    // Authenticate the screened configuration/authority themselves: a free SHA
    // label would not bind SOURCE qualification to these two publications.
    let screened_authority = unique_json(&replacement_body(
        &c.proofs.source_authority,
        &a.proofs.source_authority,
        CONFIG_CAP as usize,
    )?)?;
    let screened_config = unique_json(&replacement_body(
        &c.proofs.source_config,
        &a.proofs.source_config,
        CONFIG_CAP as usize,
    )?)?;
    if screened_authority["schema"] != "borsuk-paired-source-authority-v1"
        || screened_authority["construction"]["original"] != original
        || !publication_descriptor(&screened_authority["radial"], &a.radial)
        || !publication_descriptor(&screened_authority["spatial"], &a.spatial)
        || !descriptor(
            &screened_authority["spatial_receipt"],
            &a.proofs.spatial.pin,
        )
        || screened_authority["expected_spatial_receipt"] != *spatial
        || screened_authority["construction"]["expected_qualification"] != *q
        || screened_authority["construction"]["expected_radial_parity"] != *p
        || screened_authority["construction"]["expected_corrected_closure"] != *closure
        || !descriptor(
            &screened_authority["construction"]["qualification"],
            &a.proofs.construction.pin,
        )
        || !descriptor(
            &screened_authority["construction"]["radial_parity"],
            &a.proofs.radial_parity.pin,
        )
        || !descriptor(
            &screened_authority["construction"]["corrected_closure"],
            &a.proofs.corrected_closure.pin,
        )
        || screened_config["schema"] != "borsuk-paired-source-coverage-config-v1"
        || !publication_descriptor(&screened_config["radial"], &a.radial)
        || !publication_descriptor(&screened_config["spatial"], &a.spatial)
        || !descriptor(&screened_config["spatial_receipt"], &a.proofs.spatial.pin)
        || screened_config["inputs"]["generation_manifest"]["sha256"]
            != a.original.generation_sha256
        || screened_config["inputs"]["plane_manifest"]["sha256"] != a.original.plane_sha256
        || !descriptor(&screened_config["inputs"]["centroids"], &a.centroids)
    {
        return Err("replacement screened publication provenance".into());
    }
    if q["schema"] != "borsuk-real1m-construction-qualified-v1"
        || q["status"] != "REAL_RECONSTRUCTION_AND_HISTORICAL_RADIAL_PARITY_QUALIFIED"
        || q["original_approval"] != original
        || q["original_generation_sha256"] != a.original.generation_sha256
        || !descriptor(&q["centroids"], &a.centroids)
        || q["historical_parity"] != *h
        || q["admission"]["all_closed_body_hashes_verified"] != true
        || q["admission"]["original_ssm_success"] != true
        || q["original_parity"]["native_exit"] != 0
        || q["original_parity"]["manager_exit"] != 1
        || q["original_parity"]["disposition"] != "INVALID_RECEIPT_VALIDATOR"
        || q["original_parity"]["disposition_preserved"] != true
        || q["original_parity"]["receipt_sha256"] != a.proofs.radial_parity.pin.sha256
        || q["closed_output_revalidation"]["status"] != "Success"
        || q["closed_output_revalidation"]["exit"] != 0
        || q["closed_output_revalidation"]["native_reexecuted"] != false
        || q["closed_output_revalidation"]["all_recovery_body_checksums_verified"] != true
        || q["requests_opened"] != false
        || q["truth_opened"] != false
        || q["performance_claim"] != false
        || p["schema"] != "borsuk-partition-reconstruction-receipt-v1"
        || p["status"] != "HISTORICAL_RADIAL_PARITY"
        || p["complete"] != true
        || p["original"] != original
        || !descriptor(&p["centroids"], &a.centroids)
        || !publication_descriptor(&p["radial_outputs"], &a.radial)
        || p["requests_opened"] != false
        || p["truth_opened"] != false
        || h["radial_root_sha256"] != a.radial.root.sha256
        || h["radial_membership_sha256"] != a.radial.membership.sha256
        || h["radial_leaves_sha256"] != a.radial.leaves.sha256
        || h["root_exceptions"]
            != json!([{"offset":0,"old":"BORSUSR2","new":"BORSUSR3"},
            {"offset":88,"old_le_u32":0,"new_le_u32":1}])
        || closure["schema"] != "borsuk-reconstruction-closed-parity-recovery-v1"
        || closure["status"] != "CLOSED_NATIVE_PARITY_VALIDATED"
        || closure["original_native_exit"] != 0
        || closure["original_manager_exit"] != 1
        || closure["original_stage_disposition"] != "INVALID_RECEIPT_VALIDATOR"
        || closure["original_disposition_preserved"] != true
        || closure["native_reexecuted"] != false
        || closure["original_receipt_sha256"] != a.proofs.radial_parity.pin.sha256
        || closure["derived_outputs_full_body_hash_verified"] != true
        || closure["derived_outputs_retained_create_only"] != true
        || closure["performance_claim"] != false
        || spatial["schema"] != "borsuk-spatial-publication-receipt-v1"
        || spatial["status"] != "SPATIAL_PREPARED"
        || spatial["complete"] != true
        || spatial["original"] != original
        || spatial["partition"] != "widest_axis_quota_v1"
        || !publication_descriptor(&spatial["publication"], &a.spatial)
        || !descriptor(&spatial["centroids"], &a.centroids)
        || !descriptor(&spatial["radial_parity"], &a.proofs.radial_parity.pin)
        || !descriptor(&spatial["qualification"], &a.proofs.construction.pin)
        || !descriptor(
            &spatial["corrected_closure"],
            &a.proofs.corrected_closure.pin,
        )
        || spatial["requests_opened"] != false
        || spatial["truth_opened"] != false
        || spatial["source_payload_opened"] != false
        || spatial["requires_successful_file_sync"] != true
        || spatial["requires_successful_directory_sync"] != true
        || spatial["performance_claim"] != false
        || spatial["qualified"] != false
        || source["schema"] != "borsuk-paired-source-screen-qualified-v1"
        || source["status"] != "CONSUMED_SOURCE_SCREEN_PASS"
        || source["ssm_status"] != "Success"
        || source["ssm_exit"] != 0
        || source["all_server_sha_and_bytes_verified"] != true
        || source["all_stage_manifests_verified"] != true
        || source["exact_manifest_rosters"] != true
        || source["selection_seal_hash_matches_native"] != true
        || source["native_summary"]["schema"] != "borsuk-paired-source-coverage-v1"
        || source["native_summary"]["status"] != "PASS"
        || source["native_summary"]["complete"] != true
        || source["native_summary"]["planner_only"] != true
        || source["native_summary"]["candidate_returned_ids_computed"] != false
        || source["candidate_returned_quality_claim"] != false
        || source["performance_claim"] != false
        || source["default_promotion"] != false
    {
        return Err("replacement closed construction/parity/spatial/SOURCE provenance".into());
    }
    let stages = source["stages"]
        .as_array()
        .ok_or("replacement SOURCE stages")?;
    if stages.len() != 2
        || stages[0]["phase"] != "canary"
        || stages[1]["phase"] != "screen"
        || stages.iter().any(|s| {
            s["execution_closed"] != true
                || s["owned_cgroup_drained"] != true
                || s["manager_exit"] != 0
                || s["configuration_sha256"] != a.proofs.source_config.sha256
                || s["authority_sha256"] != a.proofs.source_authority.sha256
        })
        || stages[0]["native_exit"] != 2
        || stages[1]["native_exit"] != 0
    {
        return Err("replacement closed SOURCE qualification binding".into());
    }
    Ok(())
}

struct ReplacementBodies {
    centroids: Box<[u8]>,
    radial: [Box<[u8]>; 3],
    replacement: [Box<[u8]>; 3],
}
impl ReplacementBodies {
    fn borrowed<'a>(&'a self, a: &'a ReplacementApproval) -> RetainedSemanticRouterReplacement<'a> {
        let publication =
            |b: &'a [Box<[u8]>; 3], p: &'a RouterPins, partition| RetainedRouterPublication {
                root: RetainedRouterBody {
                    body: &b[0],
                    approved: &p.root,
                },
                membership: RetainedRouterBody {
                    body: &b[1],
                    approved: &p.membership,
                },
                leaves: RetainedRouterBody {
                    body: &b[2],
                    approved: &p.leaves,
                },
                partition,
            };
        RetainedSemanticRouterReplacement {
            original_discovery_sha256: &a.original.discovery_sha256,
            input: a.input(),
            centroids: RetainedRouterBody {
                body: &self.centroids,
                approved: &a.centroids,
            },
            radial: publication(&self.radial, &a.radial, LeafPartition::Radial64V1),
            replacement: publication(&self.replacement, a.replacement(), a.replacement_partition),
        }
    }
}
fn replacement_inputs(
    c: &RetainedConfig,
    config_sha: &str,
    approval_path: &Path,
    approval_sha: &str,
    limits: &mut TwoBitGenerationLimits,
) -> Result<(ReplacementApproval, ReplacementBodies)> {
    // Capped authority/proof bytes, recursive JSON/typed copies and receipt
    // serialization coexist; body buffers are charged again by the library.
    limits.already_pinned_bytes = limits
        .already_pinned_bytes
        .checked_add(64 * CONFIG_CAP * 9)
        .filter(|&n| n < limits.max_memory_bytes)
        .ok_or("replacement CLI proof memory")?;
    let a: ReplacementApproval =
        serde_json::from_value(unique_json(&config_bytes(approval_path, approval_sha)?)?)?;
    let files = c
        .router_replacement
        .as_ref()
        .ok_or("replacement descriptors required")?;
    let o = &a.original;
    if a.schema != REPLACEMENT_APPROVAL_SCHEMA
        || a.config_sha256 != config_sha
        || o.schema != "borsuk-partition-reconstruction-approval-v1"
        || o.generation_sha256 != c.original_root_sha256
        || o.generation != c.original_generation
        || o.base_epoch != 0
        || o.plane_sha256 != o.input_root_sha256
        || sha_hex(&serde_json::to_vec(&a.original_discovery)?) != o.discovery_sha256
        || o.centroids_sha256 != a.centroids.sha256
        || !(1..=256).contains(&o.input_schema.len())
        || !o.profile.valid_geometry(o.rows, o.dimensions)
        || [
            &o.generation_sha256,
            &o.plane_sha256,
            &o.discovery_sha256,
            &o.input_root_sha256,
            &o.centroids_sha256,
        ]
        .into_iter()
        .any(|s| !is_sha(s))
    {
        return Err("replacement independent construction approval".into());
    }
    let units = o.rows.div_ceil(32);
    let centroid_bytes = units
        .checked_mul(o.dimensions)
        .and_then(|n| n.checked_mul(2))
        .and_then(|n| n.checked_add(32))
        .ok_or("replacement geometry")?;
    let membership_bytes = units.checked_mul(4).ok_or("replacement geometry")?;
    let leaves_bytes = units
        .checked_mul(4 + 2 * o.dimensions)
        .ok_or("replacement geometry")?;
    let selected = a.replacement();
    let pins = [
        (&files.centroids, &a.centroids, centroid_bytes),
        (&files.radial.root, &a.radial.root, o.profile.root_cap()),
        (
            &files.radial.membership,
            &a.radial.membership,
            membership_bytes,
        ),
        (&files.radial.leaves, &a.radial.leaves, leaves_bytes),
        (
            &files.replacement.root,
            &selected.root,
            o.profile.root_cap(),
        ),
        (
            &files.replacement.membership,
            &selected.membership,
            membership_bytes,
        ),
        (&files.replacement.leaves, &selected.leaves, leaves_bytes),
    ];
    let mut bytes = 0_u64;
    for (file, pin, cap) in pins {
        file_pin(file, pin, cap)?;
        bytes = bytes
            .checked_add(file.bytes as u64)
            .ok_or("replacement body memory")?;
    }
    // Pre-allocation root/serving reserve; the shared publisher makes the final
    // cumulative mode/group/staging admission before any destination write.
    limits
        .already_pinned_bytes
        .checked_add(bytes)
        .and_then(|n| n.checked_add(CONFIG_CAP * 32))
        .filter(|&n| n < limits.max_memory_bytes)
        .ok_or("replacement CLI body memory")?;
    replacement_proofs(files, &a)?;
    let read_router = |files: &RouterFiles, pins: &RouterPins| -> Result<[Box<[u8]>; 3]> {
        Ok([
            replacement_body(&files.root, &pins.root, o.profile.root_cap())?,
            replacement_body(&files.membership, &pins.membership, membership_bytes)?,
            replacement_body(&files.leaves, &pins.leaves, leaves_bytes)?,
        ])
    };
    let bodies = ReplacementBodies {
        centroids: replacement_body(&files.centroids, &a.centroids, centroid_bytes)?,
        radial: read_router(&files.radial, &a.radial)?,
        replacement: read_router(&files.replacement, selected)?,
    };
    Ok((a, bodies))
}
fn config_bytes(path: &Path, trusted_sha: &str) -> Result<Vec<u8>> {
    let mut file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(path)?;
    let before = file.metadata()?;
    if !is_sha(trusted_sha)
        || path.as_os_str().len() > 4096
        || !before.is_file()
        || before.nlink() != 1
        || !(1..=CONFIG_CAP).contains(&before.len())
    {
        return Err("config identity or cap".into());
    }
    let mut bytes = vec![0_u8; before.len() as usize];
    file.read_exact(&mut bytes)?;
    let mut eof = [0_u8; 1];
    if file.read(&mut eof)? != 0
        || sha_hex(&bytes) != trusted_sha
        || file_identity(&before) != file_identity(&file.metadata()?)
        || file_identity(&before) != file_identity(&fs::symlink_metadata(path)?)
    {
        return Err("config SHA/length/EOF/identity".into());
    }
    Ok(bytes)
}
fn config(path: &Path, trusted_sha: &str) -> Result<Config> {
    let c: Config = serde_json::from_slice(&config_bytes(path, trusted_sha)?)?;
    if c.schema != CONFIG_SCHEMA {
        return Err("config schema".into());
    }
    if !is_sha(&c.root.sha256) {
        return Err("invalid root sha".into());
    }
    if !c.store_root.is_dir() {
        return Err("store_root not a directory".into());
    }
    Ok(c)
}
fn retained_config(path: &Path, trusted_sha: &str) -> Result<RetainedConfig> {
    let c: RetainedConfig = serde_json::from_slice(&config_bytes(path, trusted_sha)?)?;
    if c.schema != RETAINED_CONFIG_SCHEMA {
        return Err("retained config schema".into());
    }
    if !is_sha(&c.original_root_sha256)
        || c.original_generation == 0
        || c.original_control_epoch == 0
    {
        return Err("invalid retained approval".into());
    }
    if !c.scratch_parent.is_dir() {
        return Err("retained scratch not a directory".into());
    }
    match &c.backend {
        RetainedBackend::Local { store_root } if !store_root.is_dir() => {
            return Err("retained store not a directory".into());
        }
        RetainedBackend::S3 {
            bucket,
            region,
            namespace,
            request_timeout_seconds,
        } => {
            if !(3..=63).contains(&bucket.len())
                || !bucket.bytes().all(|c| {
                    c.is_ascii_lowercase() || c.is_ascii_digit() || matches!(c, b'.' | b'-')
                })
                || region.is_empty()
                || !region
                    .bytes()
                    .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == b'-')
                || !(1..=600).contains(request_timeout_seconds)
                || namespace.is_empty()
                || ObjectPath::parse(namespace)?.as_ref() != namespace.as_str()
            {
                return Err("retained S3 descriptor".into());
            }
        }
        _ => {}
    }
    Ok(c)
}
/// Same-directory temp file, fsync, then hard link: atomic and never overwrites.
fn write_receipt(path: &Path, body: &[u8]) -> Result<()> {
    let parent = receipt_parent(path)?;
    let name = path.file_name().ok_or("receipt name")?.to_string_lossy();
    let tmp = parent.join(format!(".{name}.{}.tmp", std::process::id()));
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&tmp)?;
    file.write_all(body)?;
    file.sync_all()?;
    drop(file);
    let linked = fs::hard_link(&tmp, path);
    fs::remove_file(&tmp)?;
    linked.map_err(|_| "receipt exists")?;
    File::open(parent)?.sync_all()?;
    Ok(())
}
fn receipt_parent(path: &Path) -> Result<&Path> {
    let parent = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    if !parent.is_dir() {
        return Err("receipt parent not a directory".into());
    }
    Ok(parent)
}
async fn publish(config_path: &Path, config_sha: &str, receipt: &Path) -> Result<()> {
    let c = config(config_path, config_sha)?;
    receipt_parent(receipt)?;
    if receipt.symlink_metadata().is_ok() {
        return Err("receipt exists".into());
    }
    let prefix = ObjectPath::parse(&c.prefix)?;
    if prefix.as_ref().is_empty() {
        return Err("invalid prefix".into());
    }
    let store = LocalFileSystem::new_with_prefix(&c.store_root)?;
    if read_two_bit_head(&store, &prefix).await?.is_some() {
        return Err("head not fresh".into());
    }
    let published = publish_two_bit_generation(
        &store,
        &prefix,
        &c.root.path,
        &c.root.sha256,
        (&c.limits).into(),
        None,
    )
    .await?;
    let head = read_two_bit_head(&store, &prefix)
        .await?
        .ok_or("published head missing")?;
    if head.root_sha256() != c.root.sha256
        || head.root_sha256() != published.root_sha256()
        || head.generation() != published.generation()
        || head.control_epoch() != published.control_epoch()
        || head.metadata_prefix() != published.metadata_prefix()
    {
        return Err("published head authority changed".into());
    }
    let body = serde_json::to_vec_pretty(&json!({
        "schema": RECEIPT_SCHEMA,
        "config_sha256": config_sha,
        "prefix": c.prefix,
        "metadata_prefix": head.metadata_prefix().to_string(),
        "root_sha256": head.root_sha256(),
        "generation": head.generation(),
        "control_epoch": head.control_epoch(),
    }))?;
    write_receipt(receipt, &body)
}
async fn publish_retained(
    config_path: &Path,
    config_sha: &str,
    receipt: &Path,
    approval: Option<(&Path, &str)>,
) -> Result<()> {
    let c = retained_config(config_path, config_sha)?;
    receipt_parent(receipt)?;
    if receipt.symlink_metadata().is_ok() {
        return Err("receipt exists".into());
    }
    // Fixed schemas/capped config, path clones, escaped receipt Value/Vec copies
    // and independent head readback coexist with the library's caller pins.
    let mut limits = TwoBitGenerationLimits::from(&c.limits);
    limits.already_pinned_bytes = limits
        .already_pinned_bytes
        .checked_add(CONFIG_CAP * 16 + 131072)
        .filter(|&n| n < limits.max_memory_bytes)
        .ok_or("retained CLI memory admission")?;
    if c.router_replacement.is_some() != approval.is_some() {
        return Err("replacement requires independent approval arguments".into());
    }
    let replacement = approval
        .map(|(path, sha)| replacement_inputs(&c, config_sha, path, sha, &mut limits))
        .transpose()?;
    let source = ObjectPath::parse(&c.retained_prefix)?;
    let destination = ObjectPath::parse(&c.destination_prefix)?;
    if source.as_ref().is_empty() || destination.as_ref().is_empty() {
        return Err("invalid retained prefix".into());
    }
    let local;
    let remote;
    let store: &dyn object_store::ObjectStore = match &c.backend {
        RetainedBackend::Local { store_root } => {
            local = LocalFileSystem::new_with_prefix(store_root)?;
            &local
        }
        RetainedBackend::S3 {
            bucket,
            region,
            namespace,
            request_timeout_seconds,
        } => {
            remote = OneAttemptS3::new_with_prefix_and_timeout(
                bucket,
                region,
                ObjectPath::parse(namespace)?,
                std::time::Duration::from_secs(*request_timeout_seconds),
            )
            .map_err(|error| format!("retained S3 transport: {error:?}"))?;
            remote.store()
        }
    };
    let retained = read_two_bit_head(store, &source)
        .await?
        .ok_or("retained head missing")?;
    let published = republish_retained_two_bit_generation(
        store,
        &retained,
        RetainedTwoBitApproval {
            root_sha256: &c.original_root_sha256,
            generation: c.original_generation,
            control_epoch: c.original_control_epoch,
            sq8_object_key: &c.sq8_object_key,
            sq8_etag: &c.sq8_etag,
            router_replacement: replacement.as_ref().map(|(a, bodies)| bodies.borrowed(a)),
        },
        &destination,
        limits,
        &c.scratch_parent,
        c.max_scratch_bytes,
    )
    .await?;
    let head = read_two_bit_head(store, &destination)
        .await?
        .ok_or("published retained head missing")?;
    if head.root_sha256() != published.root_sha256()
        || head.generation() != c.original_generation
        || head.generation() != published.generation()
        || head.control_epoch() != 1
        || head.control_epoch() != published.control_epoch()
        || head.metadata_prefix() != published.metadata_prefix()
    {
        return Err("published retained head authority changed".into());
    }
    let body = serde_json::to_vec_pretty(&json!({
        "schema": RETAINED_RECEIPT_SCHEMA,
        "config_sha256": config_sha,
        "backend": c.backend,
        "retained_prefix": c.retained_prefix,
        "original_root_sha256": c.original_root_sha256,
        "original_generation": c.original_generation,
        "original_control_epoch": c.original_control_epoch,
        "sq8_object_key": c.sq8_object_key,
        "sq8_etag": c.sq8_etag,
        "destination_prefix": c.destination_prefix,
        "head_key": destination.clone().join("head.json").to_string(),
        "metadata_prefix": head.metadata_prefix().to_string(),
        "root_sha256": head.root_sha256(),
        "generation": head.generation(),
        "control_epoch": head.control_epoch(),
        "local_file_only": matches!(&c.backend, RetainedBackend::Local { .. }),
        "credential_mode": match &c.backend { RetainedBackend::Local { .. } => "none", RetainedBackend::S3 { .. } => "instance_role_imds" },
        "performance_claim": false,
        "replacement_approval_sha256": approval.map(|(_, sha)| sha),
        "router_replacement": c.router_replacement,
        "replacement_partition": replacement.as_ref().map(|(a, _)| a.replacement_partition),
    }))?;
    write_receipt(receipt, &body)
}
fn run(args: &[String]) -> Result<()> {
    let replacement = args.get(1).is_some_and(|arg| arg == "--retained-router");
    let retained = replacement || args.get(1).is_some_and(|arg| arg == "--retained");
    if args.len()
        != if replacement {
            7
        } else if retained {
            5
        } else {
            4
        }
    {
        return Err(
            "usage: publish_two_bit_generation [--retained] CONFIG CONFIG_SHA NEW_RECEIPT; --retained-router CONFIG CONFIG_SHA APPROVAL APPROVAL_SHA NEW_RECEIPT".into(),
        );
    }
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    if retained {
        runtime.block_on(publish_retained(
            Path::new(&args[2]),
            &args[3],
            Path::new(&args[if replacement { 6 } else { 4 }]),
            replacement.then(|| (Path::new(&args[4]), args[5].as_str())),
        ))
    } else {
        runtime.block_on(publish(Path::new(&args[1]), &args[2], Path::new(&args[3])))
    }
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
    use borsuk::{
        two_bit_build::TwoBitGenerationBuilder, two_bit_generation::DiscoveryMode,
        two_bit_source::TwoBitSource,
    };
    use object_store::ObjectStoreExt;
    use serde_json::Value;

    const ROWS: usize = 257;
    const D: usize = 768;

    /// Real builder output plus the SQ8 object staged in a real LocalFileSystem store.
    async fn fixture(rows: usize, dimensions: usize) -> (tempfile::TempDir, Value) {
        fixture_with_repeated_units(rows, dimensions, false).await
    }
    async fn fixture_with_repeated_units(
        rows: usize,
        dimensions: usize,
        repeated_units: bool,
    ) -> (tempfile::TempDir, Value) {
        let dir = tempfile::tempdir().unwrap();
        let store_root = dir.path().join("store");
        fs::create_dir(&store_root).unwrap();
        let (mut raw, mut sq8) = (Vec::new(), Vec::new());
        for row in 0..rows {
            let sample_row = if repeated_units { row % 32 } else { row };
            let value = (1 + sample_row % 7) as u8;
            for _ in 0..dimensions {
                raw.extend_from_slice(&(value as f32).to_le_bytes());
            }
            sq8.extend_from_slice(&((rows - row + 1000) as i64).to_le_bytes());
            sq8.extend_from_slice(&(dimensions as f32 * (value as f32).powi(2)).to_le_bytes());
            sq8.extend(std::iter::repeat_n(value, dimensions));
        }
        let (raw_path, sq8_path) = (dir.path().join("raw"), dir.path().join("sq8"));
        fs::write(&raw_path, &raw).unwrap();
        fs::write(&sq8_path, &sq8).unwrap();
        let sq8_sha = sha_hex(&sq8);
        let key = ObjectPath::from(format!("semantic/objects/{sq8_sha}"));
        let store = LocalFileSystem::new_with_prefix(&store_root).unwrap();
        store.put(&key, sq8.into()).await.unwrap();
        let etag = store.head(&key).await.unwrap().e_tag.unwrap();
        let root = dir.path().join("generation");
        let order = (0..rows as u64).collect::<Vec<_>>();
        let root_sha = TwoBitGenerationBuilder {
            source: TwoBitSource {
                raw: &raw_path,
                raw_sha256: &sha_hex(&raw),
                sq8: &sq8_path,
                sq8_sha256: &sq8_sha,
                rows,
                dimensions,
            },
            base_epoch: 0,
            generation: 1,
            low: &vec![0.; dimensions],
            step: &vec![1.; dimensions],
            sq8_object_key: key.as_ref(),
            sq8_etag: &etag,
        }
        .build_with_discovery(Some(&order), DiscoveryMode::Semantic, &root, 128_000_000)
        .unwrap();
        // These fixtures use full rotation blocks. Match prepare_query's exact
        // lookup-table plus temporary rotated-query admission at either width.
        assert_eq!(dimensions % 256, 0);
        let prepare_bytes = (dimensions / 4)
            .checked_mul(256)
            .and_then(|n| n.checked_add(dimensions))
            .and_then(|n| n.checked_mul(std::mem::size_of::<f64>()))
            .unwrap();
        let config = json!({
            "schema": CONFIG_SCHEMA,
            "root": {"path": root, "sha256": root_sha},
            "store_root": store_root,
            "prefix": "semantic/index",
            "limits": {
                "max_memory_bytes": 512 * 1024 * 1024,
                "max_active_queries": 1,
                "max_query_bytes": 16_773_120,
                "max_query_gets": 32,
                "max_parallel_gets": 16,
                "max_source_bytes": 64 * 1024 * 1024,
                "max_source_gets": 128.max(rows.div_ceil(32)),
                "max_parallel_source_gets": 16,
                "max_query_scratch_bytes": prepare_bytes,
                "already_pinned_bytes": 0,
            },
        });
        (dir, config)
    }
    fn write_config(dir: &Path, value: &Value) -> (PathBuf, String) {
        let path = dir.join("config.json");
        let body = serde_json::to_vec(value).unwrap();
        fs::write(&path, &body).unwrap();
        (path, sha_hex(&body))
    }
    async fn head_exists(c: &Value) -> bool {
        let store = LocalFileSystem::new_with_prefix(c["store_root"].as_str().unwrap()).unwrap();
        let prefix = ObjectPath::from(c["prefix"].as_str().unwrap());
        read_two_bit_head(&store, &prefix).await.unwrap().is_some()
    }

    // Real publication, exact retained roster, then remove construction/source.
    async fn retained_fixture() -> (tempfile::TempDir, Value) {
        let (dir, fresh) = fixture(64, 1024).await;
        let (path, sha) = write_config(dir.path(), &fresh);
        publish(&path, &sha, &dir.path().join("original-receipt.json"))
            .await
            .unwrap();
        let source_root = dir.path().join("store");
        let copied_root = dir.path().join("copied");
        fs::create_dir(&copied_root).unwrap();
        let source = LocalFileSystem::new_with_prefix(&source_root).unwrap();
        let copied = LocalFileSystem::new_with_prefix(&copied_root).unwrap();
        let head = read_two_bit_head(&source, &ObjectPath::from("semantic/index"))
            .await
            .unwrap()
            .unwrap();
        let root: Value =
            serde_json::from_slice(&fs::read(dir.path().join("generation/manifest.json")).unwrap())
                .unwrap();
        let metadata = head.metadata_prefix();
        let mut roster = [
            "manifest.json",
            "page_manifest.json",
            "page_digests.bin",
            "plane/manifest.json",
            "plane/mean.bin",
            "plane/records.bin",
            "plane/page_digests.bin",
            "router/root.bin",
            "router/membership.bin",
            "router/leaves.bin",
        ]
        // join() takes a single segment, not a relative path with directories.
        .map(|name| {
            name.split('/')
                .fold(metadata.clone(), |path, segment| path.join(segment))
        })
        .to_vec();
        roster.extend([
            ObjectPath::from("semantic/index/head.json"),
            ObjectPath::from(root["sq8_object_key"].as_str().unwrap()),
            ObjectPath::from(root["canonical"]["object_key"].as_str().unwrap()),
        ]);
        for object in roster {
            let from = source.path_to_filesystem(&object).unwrap();
            let target = copied.path_to_filesystem(&object).unwrap();
            fs::create_dir_all(target.parent().unwrap()).unwrap();
            fs::copy(&from, &target).unwrap_or_else(|error| {
                panic!("copy {object} from {from:?} to {target:?}: {error}")
            });
        }
        assert!(
            !copied_root
                .join(metadata.as_ref())
                .join("centroids.bin")
                .exists()
        );
        let etag = copied
            .head(&ObjectPath::from(root["sq8_object_key"].as_str().unwrap()))
            .await
            .unwrap()
            .e_tag
            .unwrap();
        assert_ne!(etag, root["sq8_etag"].as_str().unwrap());
        let scratch = dir.path().join("scratch");
        fs::create_dir(&scratch).unwrap();
        let c = json!({
            "schema": RETAINED_CONFIG_SCHEMA,
            "router_replacement": null,
            "backend": {"kind": "Local", "store_root": copied_root},
            "retained_prefix": "semantic/index",
            "original_root_sha256": head.root_sha256(),
            "original_generation": head.generation(),
            "original_control_epoch": head.control_epoch(),
            "sq8_object_key": root["sq8_object_key"],
            "sq8_etag": etag,
            "destination_prefix": "rebound/index",
            "scratch_parent": scratch,
            "max_scratch_bytes": 4_000_000,
            "limits": fresh["limits"],
        });
        drop(source);
        for name in ["store", "generation"] {
            fs::remove_dir_all(dir.path().join(name)).unwrap();
        }
        for name in ["raw", "sq8"] {
            fs::remove_file(dir.path().join(name)).unwrap();
        }
        (dir, c)
    }

    fn retained_args(path: &Path, sha: &str, receipt: &Path) -> Vec<String> {
        vec![
            "publish_two_bit_generation".into(),
            "--retained".into(),
            path.to_str().unwrap().into(),
            sha.into(),
            receipt.to_str().unwrap().into(),
        ]
    }

    // Real native publications with independently sealed synthetic provenance
    // fixtures. These receipts exercise authorization, never claim qualification.
    async fn replacement_cli_fixture() -> (tempfile::TempDir, Value, Value) {
        let rows = 129 * 32;
        let (dir, fresh) = fixture_with_repeated_units(rows, 1024, true).await;
        let (path, sha) = write_config(dir.path(), &fresh);
        publish(&path, &sha, &dir.path().join("fresh-receipt.json"))
            .await
            .unwrap();
        let store = LocalFileSystem::new_with_prefix(dir.path().join("store")).unwrap();
        let source = ObjectPath::from("semantic/index");
        let old = read_two_bit_head(&store, &source).await.unwrap().unwrap();
        let mut root: Value =
            serde_json::from_slice(&fs::read(dir.path().join("generation/manifest.json")).unwrap())
                .unwrap();
        let centroid = fs::read(dir.path().join("generation/centroids.bin")).unwrap();
        let radial_root = fs::read(dir.path().join("generation/router/root.bin")).unwrap();
        let radial_membership =
            fs::read(dir.path().join("generation/router/membership.bin")).unwrap();
        let radial_leaves = fs::read(dir.path().join("generation/router/leaves.bin")).unwrap();
        let input = SourceIdentity {
            profile: SemanticProfile::Native100k,
            schema: root["discovery"]["input_schema"].as_str().unwrap(),
            root_sha256: root["discovery"]["input_root_sha256"].as_str().unwrap(),
            centroids_sha256: root["discovery"]["centroids_sha256"].as_str().unwrap(),
            rows,
            dimensions: 1024,
        };
        let spatial = borsuk::semantic_unit_router::build_with_partition(
            &centroid,
            &input,
            128_000_000,
            LeafPartition::WidestAxisQuotaV1,
        )
        .unwrap();
        let radial_router = borsuk::semantic_unit_router::SemanticUnitRouter::open(
            &radial_root,
            &radial_membership,
            &sha_hex(&radial_root),
            &input,
            input.profile.root_cap(),
        )
        .unwrap();
        let spatial_router = borsuk::semantic_unit_router::SemanticUnitRouter::open(
            &spatial.manifest,
            &spatial.membership,
            &sha_hex(&spatial.manifest),
            &input,
            input.profile.root_cap(),
        )
        .unwrap();
        assert!(rows.div_ceil(32) > 64);
        assert!(
            radial_router.manifest().leaves.len() > 1 && spatial_router.manifest().leaves.len() > 1
        );
        assert_eq!(radial_router.manifest().algorithm.training_centers, 1);
        assert_ne!(
            radial_router.unit_membership(),
            spatial_router.unit_membership()
        );
        for (&r, &s) in radial_router
            .unit_membership()
            .iter()
            .zip(spatial_router.unit_membership())
        {
            assert_eq!(
                radial_router.manifest().leaves[r].group_ordinal,
                spatial_router.manifest().leaves[s].group_ordinal
            );
        }
        drop((radial_router, spatial_router));
        let seal = |name: &str, bytes: &[u8]| {
            let path = dir.path().join(name);
            fs::write(&path, bytes).unwrap();
            json!({"path":path,"bytes":bytes.len(),"sha256":sha_hex(bytes)})
        };
        let centroids = seal("approved-centroids.bin", &centroid);
        let radial = json!({"root":seal("radial-root.bin",&radial_root),"membership":seal("radial-membership.bin",&radial_membership),"leaves":seal("radial-leaves.bin",&radial_leaves)});
        let candidate = json!({"root":seal("spatial-root.bin",&spatial.manifest),"membership":seal("spatial-membership.bin",&spatial.membership),"leaves":seal("spatial-leaves.bin",&spatial.leaves)});
        let mut historical = radial_root.clone();
        historical[..8].copy_from_slice(b"BORSUSR2");
        historical[88..92].copy_from_slice(&0_u32.to_le_bytes());
        root["discovery"]["root_sha256"] = sha_hex(&historical).into();
        let body = serde_json::to_vec(&root).unwrap();
        let root_sha = sha_hex(&body);
        let metadata = source.clone().join("generations").join(root_sha.as_str());
        for name in [
            "manifest.json",
            "page_manifest.json",
            "page_digests.bin",
            "plane/manifest.json",
            "plane/mean.bin",
            "plane/records.bin",
            "plane/page_digests.bin",
            "router/root.bin",
            "router/membership.bin",
            "router/leaves.bin",
        ] {
            let key = |prefix: &ObjectPath| name.split('/').fold(prefix.clone(), |p, s| p.join(s));
            let bytes = match name {
                "manifest.json" => bytes::Bytes::from(body.clone()),
                "router/root.bin" => bytes::Bytes::from(historical.clone()),
                _ => store
                    .get(&key(&old.metadata_prefix()))
                    .await
                    .unwrap()
                    .bytes()
                    .await
                    .unwrap(),
            };
            store.put(&key(&metadata), bytes.into()).await.unwrap();
        }
        store.put(&source.clone().join("head.json"),serde_json::to_vec(&json!({"schema":"borsuk-two-bit-head-v2","epoch":1,"generation":1,"root_sha256":root_sha,"mutation":null,"fence":null})).unwrap().into()).await.unwrap();
        let original = json!({"schema":"borsuk-partition-reconstruction-approval-v1","generation_sha256":root_sha,"generation":1,"base_epoch":0,
            "plane_sha256":root["plane_manifest_sha256"],"discovery_sha256":sha_hex(&serde_json::to_vec(&root["discovery"]).unwrap()),
            "input_schema":root["discovery"]["input_schema"],"input_root_sha256":root["discovery"]["input_root_sha256"],
            "centroids_sha256":root["discovery"]["centroids_sha256"],"profile":"native100k","rows":rows,"dimensions":1024});
        let pin = |v: &Value| json!({"bytes":v["bytes"],"sha256":v["sha256"]});
        let router_pins = |v: &Value| json!({"root":pin(&v["root"]),"membership":pin(&v["membership"]),"leaves":pin(&v["leaves"])});
        let h = json!({"old_root":{"bytes":historical.len(),"sha256":sha_hex(&historical)},
            "old_membership":pin(&radial["membership"]),"old_leaves":pin(&radial["leaves"]),
            "radial_root_sha256":radial["root"]["sha256"],"radial_membership_sha256":radial["membership"]["sha256"],"radial_leaves_sha256":radial["leaves"]["sha256"],
            "root_exceptions":[{"offset":0,"old":"BORSUSR2","new":"BORSUSR3"},{"offset":88,"old_le_u32":0,"new_le_u32":1}]});
        let parity = json!({"schema":"borsuk-partition-reconstruction-receipt-v1","status":"HISTORICAL_RADIAL_PARITY","complete":true,
            "original":original,"centroids":centroids,"radial_outputs":radial,"historical_parity":h,"requests_opened":false,"truth_opened":false});
        let parity_file = seal("parity.json", &serde_json::to_vec(&parity).unwrap());
        let q = json!({"schema":"borsuk-real1m-construction-qualified-v1","status":"REAL_RECONSTRUCTION_AND_HISTORICAL_RADIAL_PARITY_QUALIFIED",
            "original_approval":original,"original_generation_sha256":root_sha,"centroids":centroids,"historical_parity":h,
            "admission":{"all_closed_body_hashes_verified":true,"original_ssm_success":true},
            "original_parity":{"native_exit":0,"manager_exit":1,"disposition":"INVALID_RECEIPT_VALIDATOR","disposition_preserved":true,"receipt_sha256":parity_file["sha256"]},
            "closed_output_revalidation":{"status":"Success","exit":0,"native_reexecuted":false,"all_recovery_body_checksums_verified":true},
            "requests_opened":false,"truth_opened":false,"performance_claim":false});
        let q_file = seal("construction.json", &serde_json::to_vec(&q).unwrap());
        let closure = json!({"schema":"borsuk-reconstruction-closed-parity-recovery-v1","status":"CLOSED_NATIVE_PARITY_VALIDATED",
            "original_native_exit":0,"original_manager_exit":1,"original_stage_disposition":"INVALID_RECEIPT_VALIDATOR","original_disposition_preserved":true,
            "native_reexecuted":false,"original_receipt_sha256":parity_file["sha256"],"derived_outputs_full_body_hash_verified":true,"derived_outputs_retained_create_only":true,"performance_claim":false});
        let closure_file = seal("closure.json", &serde_json::to_vec(&closure).unwrap());
        let spatial_receipt = json!({"schema":"borsuk-spatial-publication-receipt-v1","status":"SPATIAL_PREPARED","complete":true,"original":original,
            "partition":"widest_axis_quota_v1","publication":candidate,"centroids":centroids,"radial_parity":parity_file,"qualification":q_file,"corrected_closure":closure_file,
            "requests_opened":false,"truth_opened":false,"source_payload_opened":false,"requires_successful_file_sync":true,"requires_successful_directory_sync":true,"performance_claim":false,"qualified":false});
        let spatial_file = seal(
            "spatial.json",
            &serde_json::to_vec(&spatial_receipt).unwrap(),
        );
        let screened_authority = json!({"schema":"borsuk-paired-source-authority-v1",
            "construction":{"original":original,"qualification":q_file,"radial_parity":parity_file,"corrected_closure":closure_file,
                "expected_qualification":q,"expected_radial_parity":parity,"expected_corrected_closure":closure},
            "radial":radial,"spatial":candidate,"spatial_receipt":spatial_file,"expected_spatial_receipt":spatial_receipt});
        let source_authority_file = seal(
            "screened-authority.json",
            &serde_json::to_vec(&screened_authority).unwrap(),
        );
        let screened_config = json!({"schema":"borsuk-paired-source-coverage-config-v1","radial":radial,"spatial":candidate,"spatial_receipt":spatial_file,
            "inputs":{"generation_manifest":{"sha256":root_sha},"plane_manifest":{"sha256":original["plane_sha256"]},"centroids":centroids}});
        let source_config_file = seal(
            "screened-config.json",
            &serde_json::to_vec(&screened_config).unwrap(),
        );
        let source_config = source_config_file["sha256"].as_str().unwrap();
        let source_authority = source_authority_file["sha256"].as_str().unwrap();
        let source_q = json!({"schema":"borsuk-paired-source-screen-qualified-v1","status":"CONSUMED_SOURCE_SCREEN_PASS","ssm_status":"Success","ssm_exit":0,
            "all_server_sha_and_bytes_verified":true,"all_stage_manifests_verified":true,"exact_manifest_rosters":true,"selection_seal_hash_matches_native":true,
            "native_summary":{"schema":"borsuk-paired-source-coverage-v1","status":"PASS","complete":true,"planner_only":true,"candidate_returned_ids_computed":false},
            "candidate_returned_quality_claim":false,"performance_claim":false,"default_promotion":false,
            "stages":[{"phase":"canary","execution_closed":true,"owned_cgroup_drained":true,"manager_exit":0,"native_exit":2,"configuration_sha256":source_config,"authority_sha256":source_authority},
                {"phase":"screen","execution_closed":true,"owned_cgroup_drained":true,"manager_exit":0,"native_exit":0,"configuration_sha256":source_config,"authority_sha256":source_authority}]});
        let source_file = seal(
            "source-qualification.json",
            &serde_json::to_vec(&source_q).unwrap(),
        );
        let scratch = dir.path().join("scratch");
        fs::create_dir(&scratch).unwrap();
        let config = json!({"schema":RETAINED_CONFIG_SCHEMA,"backend":{"kind":"Local","store_root":dir.path().join("store")},
            "retained_prefix":"semantic/index","original_root_sha256":root_sha,"original_generation":1,"original_control_epoch":1,
            "sq8_object_key":root["sq8_object_key"],"sq8_etag":root["sq8_etag"],"destination_prefix":"replacement-cli/index",
            "scratch_parent":scratch,"max_scratch_bytes":4_000_000,"limits":fresh["limits"],
            "router_replacement":{"centroids":centroids,"radial":radial,"replacement":candidate,
                "proofs":{"construction":q_file,"radial_parity":parity_file,"corrected_closure":closure_file,"spatial":spatial_file,"source_qualification":source_file,
                    "source_authority":source_authority_file,"source_config":source_config_file}}});
        let authority = json!({"schema":REPLACEMENT_APPROVAL_SCHEMA,"config_sha256":sha_hex(&serde_json::to_vec(&config).unwrap()),"original":original,"original_discovery":root["discovery"],
            "centroids":pin(&centroids),"radial":router_pins(&radial),"spatial":router_pins(&candidate),"replacement_partition":"widest_axis_quota_v1",
            "proofs":{"construction":{"pin":pin(&q_file),"expected":q},"radial_parity":{"pin":pin(&parity_file),"expected":parity},
                "corrected_closure":{"pin":pin(&closure_file),"expected":closure},"spatial":{"pin":pin(&spatial_file),"expected":spatial_receipt},"source_qualification":{"pin":pin(&source_file),"expected":source_q},
                "source_authority":pin(&source_authority_file),"source_config":pin(&source_config_file)}});
        (dir, config, authority)
    }

    fn replacement_args(dir: &Path, c: &Value, a: &Value, receipt: &Path) -> Vec<String> {
        let (path, sha) = write_config(dir, c);
        let authority = dir.join("replacement-approval.json");
        let bytes = serde_json::to_vec(a).unwrap();
        fs::write(&authority, &bytes).unwrap();
        vec![
            "publish_two_bit_generation".into(),
            "--retained-router".into(),
            path.to_str().unwrap().into(),
            sha,
            authority.to_str().unwrap().into(),
            sha_hex(&bytes),
            receipt.to_str().unwrap().into(),
        ]
    }

    fn reseal_source_fixture(c: &mut Value, a: &mut Value, name: &str, value: &Value) {
        let file = &mut c["router_replacement"]["proofs"][name];
        let body = serde_json::to_vec(value).unwrap();
        fs::write(file["path"].as_str().unwrap(), &body).unwrap();
        let pin = json!({"bytes":body.len(),"sha256":sha_hex(&body)});
        file["bytes"] = pin["bytes"].clone();
        file["sha256"] = pin["sha256"].clone();
        match name {
            "source_authority" | "source_config" => a["proofs"][name] = pin,
            "source_qualification" => {
                a["proofs"][name]["pin"] = pin;
                a["proofs"][name]["expected"] = value.clone();
            }
            _ => panic!("unexpected SOURCE fixture role"),
        }
    }

    #[test]
    fn retained_router_replacement_cli_requires_independent_pins_and_closed_provenance() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let (dir, good, approved) = runtime.block_on(replacement_cli_fixture());
        let receipt = dir.path().join("replacement-receipt.json");
        // Remote serving requires streamed range bodies; keep the real local
        // publication, adapting its file payloads like the integration fixture.
        let store = object_store::chunked::ChunkedStore::new(
            std::sync::Arc::new(
                LocalFileSystem::new_with_prefix(dir.path().join("store")).unwrap(),
            ),
            8192,
        );
        let destination = ObjectPath::from("replacement-cli/index");
        let source_proof_path = Path::new(
            good["router_replacement"]["proofs"]["source_qualification"]["path"]
                .as_str()
                .unwrap(),
        );
        let source_proof = fs::read(source_proof_path).unwrap();
        let source_inputs = ["source_authority", "source_config"].map(|name| {
            let path = PathBuf::from(
                good["router_replacement"]["proofs"][name]["path"]
                    .as_str()
                    .unwrap(),
            );
            let bytes = fs::read(&path).unwrap();
            (path, bytes)
        });
        for fault in [
            "approval-sha",
            "config-only",
            "pin",
            "proof-pin",
            "provenance",
            "source-binding",
            "source-binding-construction",
            "source-claim",
            "historical",
            "cap",
            "unknown",
        ] {
            let mut c = good.clone();
            let mut a = approved.clone();
            match fault {
                "pin" => {
                    c["router_replacement"]["replacement"]["root"]["sha256"] = "0".repeat(64).into()
                }
                "proof-pin" => a["proofs"]["construction"]["pin"]["sha256"] = "0".repeat(64).into(),
                "provenance" => a["original"]["input_root_sha256"] = "0".repeat(64).into(),
                "historical" => {
                    a["original_discovery"]["root_sha256"] = "0".repeat(64).into();
                    a["original"]["discovery_sha256"] =
                        sha_hex(&serde_json::to_vec(&a["original_discovery"]).unwrap()).into();
                }
                "source-binding" | "source-binding-construction" => {
                    let mut authority: Value = serde_json::from_slice(&source_inputs[0].1).unwrap();
                    let mut screened: Value = serde_json::from_slice(&source_inputs[1].1).unwrap();
                    if fault == "source-binding" {
                        authority["radial"]["root"]["sha256"] = "0".repeat(64).into();
                        screened["radial"]["root"]["sha256"] = "0".repeat(64).into();
                    } else {
                        authority["construction"]["original"]["input_root_sha256"] =
                            "0".repeat(64).into();
                        screened["inputs"]["plane_manifest"]["sha256"] = "0".repeat(64).into();
                    }
                    reseal_source_fixture(&mut c, &mut a, "source_authority", &authority);
                    reseal_source_fixture(&mut c, &mut a, "source_config", &screened);
                    let mut qualified = a["proofs"]["source_qualification"]["expected"].clone();
                    for stage in qualified["stages"].as_array_mut().unwrap() {
                        stage["authority_sha256"] =
                            a["proofs"]["source_authority"]["sha256"].clone();
                        stage["configuration_sha256"] =
                            a["proofs"]["source_config"]["sha256"].clone();
                    }
                    reseal_source_fixture(&mut c, &mut a, "source_qualification", &qualified);
                }
                "source-claim" => {
                    a["proofs"]["source_qualification"]["expected"]["candidate_returned_quality_claim"] =
                        true.into();
                    let body = serde_json::to_vec(&a["proofs"]["source_qualification"]["expected"])
                        .unwrap();
                    fs::write(source_proof_path, &body).unwrap();
                    let pin = json!({"bytes":body.len(),"sha256":sha_hex(&body)});
                    a["proofs"]["source_qualification"]["pin"] = pin.clone();
                    c["router_replacement"]["proofs"]["source_qualification"]["bytes"] =
                        pin["bytes"].clone();
                    c["router_replacement"]["proofs"]["source_qualification"]["sha256"] =
                        pin["sha256"].clone();
                }
                "cap" => c["limits"]["max_memory_bytes"] = 1.into(),
                "unknown" => a["unapproved"] = true.into(),
                _ => {}
            }
            a["config_sha256"] = sha_hex(&serde_json::to_vec(&c).unwrap()).into();
            let mut args = replacement_args(dir.path(), &c, &a, &receipt);
            if fault == "approval-sha" {
                args[5] = "0".repeat(64);
            }
            if fault == "config-only" {
                args = retained_args(Path::new(&args[2]), &args[3], &receipt);
            }
            let error = run(&args).unwrap_err();
            if fault.starts_with("source-binding") {
                assert_eq!(
                    error.to_string(),
                    "replacement screened publication provenance",
                    "{fault}"
                );
            }
            fs::write(source_proof_path, &source_proof).unwrap();
            for (path, body) in &source_inputs {
                fs::write(path, body).unwrap();
            }
            assert!(!receipt.exists());
            assert!(
                runtime
                    .block_on(read_two_bit_head(&store, &destination))
                    .unwrap()
                    .is_none()
            );
        }
        let args = replacement_args(dir.path(), &good, &approved, &receipt);
        run(&args).unwrap();
        let received: Value = serde_json::from_slice(&fs::read(&receipt).unwrap()).unwrap();
        assert_eq!(received["schema"], RETAINED_RECEIPT_SCHEMA);
        assert_eq!(received["replacement_approval_sha256"], args[5]);
        assert_eq!(received["router_replacement"], good["router_replacement"]);
        assert_eq!(received["replacement_partition"], "widest_axis_quota_v1");
        assert_eq!(received["performance_claim"], false);
        let head = runtime
            .block_on(read_two_bit_head(&store, &destination))
            .unwrap()
            .unwrap();
        assert_eq!(received["root_sha256"], head.root_sha256());
        runtime.block_on(async {
            for (name, pin) in [
                ("root", "root.bin"),
                ("membership", "membership.bin"),
                ("leaves", "leaves.bin"),
            ] {
                let expected = fs::read(
                    good["router_replacement"]["replacement"][name]["path"]
                        .as_str()
                        .unwrap(),
                )
                .unwrap();
                let staged = store
                    .get(&head.metadata_prefix().join("router").join(pin))
                    .await
                    .unwrap()
                    .bytes()
                    .await
                    .unwrap();
                assert_eq!(staged.as_ref(), expected.as_slice(), "{name}");
            }
            let generation = borsuk::two_bit_generation::TwoBitGeneration::open_remote_from_head(
                &store,
                &head,
                (&retained_config(Path::new(&args[2]), &args[3])
                    .unwrap()
                    .limits)
                    .into(),
                Path::new(good["scratch_parent"].as_str().unwrap()),
            )
            .await
            .unwrap();
            assert_eq!(generation.rows(), 129 * 32);
            let returned = generation
                .search_with_store(&store, &vec![1.; 1024], 10, None)
                .await
                .unwrap();
            assert_eq!(returned.ranked.candidates.len(), 10);
            assert!(returned.ranked.candidates.iter().all(|h| {
                h.ordinal < 129 * 32
                    && h.id == (129 * 32 - h.ordinal + 1000) as i64
                    && h.score.is_finite()
            }));
            assert_eq!(
                returned
                    .ranked
                    .candidates
                    .iter()
                    .map(|h| h.id)
                    .collect::<std::collections::BTreeSet<_>>()
                    .len(),
                10
            );
        });
        assert!(run(&args).is_err());
    }

    #[test]
    fn retained_router_replacement_files_reject_duplicates_corruption_truncation_and_links() {
        assert!(unique_json(br#"{"proof":{"complete":true,"complete":false}}"#).is_err());
        assert!(unique_json(br#"{"proof":[{"bytes":1,"bytes":2}]}"#).is_err());
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("body.bin");
        fs::write(&path, b"approved").unwrap();
        let approved = Artifact {
            bytes: 8,
            sha256: sha_hex(b"approved"),
        };
        let pin = FilePin {
            path: path.clone(),
            bytes: 8,
            sha256: approved.sha256.clone(),
        };
        assert_eq!(
            replacement_body(&pin, &approved, 8).unwrap().as_ref(),
            b"approved"
        );
        for bytes in [
            b"corrupt!".as_slice(),
            b"short".as_slice(),
            b"approved-extra".as_slice(),
        ] {
            fs::write(&path, bytes).unwrap();
            assert!(replacement_body(&pin, &approved, 8).is_err());
        }
        fs::write(&path, b"approved").unwrap();
        let hard = dir.path().join("hard");
        fs::hard_link(&path, &hard).unwrap();
        assert!(replacement_body(&pin, &approved, 8).is_err());
        fs::remove_file(&hard).unwrap();
        fs::remove_file(&path).unwrap();
        fs::write(&hard, b"approved").unwrap();
        std::os::unix::fs::symlink(&hard, &path).unwrap();
        assert!(replacement_body(&pin, &approved, 8).is_err());
        assert!(replacement_body(&pin, &approved, 7).is_err());
    }

    #[test]
    fn retained_router_replacement_config_and_approval_refuse_unsafe_entrypoints() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let (dir, c, a) = runtime.block_on(replacement_cli_fixture());
        let args = replacement_args(dir.path(), &c, &a, &dir.path().join("unused-receipt.json"));
        let config_path = PathBuf::from(&args[2]);
        let config_sha = args[3].clone();
        let approval_path = PathBuf::from(&args[4]);
        let approval_sha = args[5].clone();
        assert!(retained_config(&config_path, &config_sha).is_ok());
        let config = retained_config(&config_path, &config_sha).unwrap();
        assert!(
            replacement_inputs(
                &config,
                &config_sha,
                &approval_path,
                &approval_sha,
                &mut (&config.limits).into()
            )
            .is_ok()
        );
        for approval in [false, true] {
            let target = if approval {
                &approval_path
            } else {
                &config_path
            };
            let bytes = fs::read(target).unwrap();
            for kind in ["symlink", "hardlink", "fifo"] {
                let path = dir.path().join(format!("unsafe-{approval}-{kind}"));
                match kind {
                    "symlink" => std::os::unix::fs::symlink(target, &path).unwrap(),
                    "hardlink" => fs::hard_link(target, &path).unwrap(),
                    "fifo" => {
                        use std::os::unix::ffi::OsStrExt;
                        let name = std::ffi::CString::new(path.as_os_str().as_bytes()).unwrap();
                        assert_eq!(unsafe { libc::mkfifo(name.as_ptr(), 0o600) }, 0);
                    }
                    _ => unreachable!(),
                }
                let (tx, rx) = std::sync::mpsc::channel();
                let candidate = path.clone();
                let cp = config_path.clone();
                let cs = config_sha.clone();
                let aps = approval_sha.clone();
                let worker = std::thread::spawn(move || {
                    let refused = if approval {
                        let config = retained_config(&cp, &cs).unwrap();
                        replacement_inputs(
                            &config,
                            &cs,
                            &candidate,
                            &aps,
                            &mut (&config.limits).into(),
                        )
                        .is_err()
                    } else {
                        retained_config(&candidate, &cs).is_err()
                    };
                    tx.send(refused).unwrap();
                });
                let result = rx.recv_timeout(std::time::Duration::from_secs(2));
                let prompt = result.is_ok();
                // Release a regressed blocking FIFO open before reporting failure.
                if !prompt && kind == "fifo" {
                    if let Ok(mut writer) = OpenOptions::new()
                        .write(true)
                        .custom_flags(libc::O_NONBLOCK)
                        .open(&path)
                    {
                        let _ = writer.write_all(&bytes);
                    }
                }
                let refused =
                    result.or_else(|_| rx.recv_timeout(std::time::Duration::from_secs(2)));
                if refused.is_ok() {
                    worker.join().unwrap();
                }
                fs::remove_file(&path).unwrap();
                assert!(
                    prompt && refused == Ok(true),
                    "{approval} {kind}: entrypoint must promptly refuse before store operations"
                );
            }
        }
    }

    #[test]
    fn retained_backend_descriptors_are_strict_before_store_operations() {
        let dir = tempfile::tempdir().unwrap();
        let mut value = json!({
            "schema": RETAINED_CONFIG_SCHEMA,
            "router_replacement": null,
            "backend": {"kind": "S3", "bucket": "fixture", "region": "eu-central-1", "namespace": "tenant/import", "request_timeout_seconds": 120},
            "retained_prefix": "semantic/index",
            "original_root_sha256": "1".repeat(64),
            "original_generation": 1, "original_control_epoch": 1,
            "sq8_object_key": "semantic/objects/frozen", "sq8_etag": "\"actual\"",
            "destination_prefix": "semantic/rebound",
            "scratch_parent": dir.path(), "max_scratch_bytes": 4_000_000,
            "limits": {"max_memory_bytes": 16_000_000, "max_active_queries": 1,
                "max_query_bytes": 1, "max_query_gets": 1, "max_parallel_gets": 1,
                "max_source_bytes": 1, "max_source_gets": 1, "max_parallel_source_gets": 1,
                "max_query_scratch_bytes": 1, "already_pinned_bytes": 0}
        });
        let (path, sha) = write_config(dir.path(), &value);
        assert!(retained_config(&path, &sha).is_ok());
        for backend in [
            json!({"kind": "S3", "bucket": "", "region": "eu-central-1", "namespace": "tenant", "request_timeout_seconds": 120}),
            json!({"kind": "S3", "bucket": "fixture", "region": "eu-central-1", "namespace": "", "request_timeout_seconds": 120}),
            json!({"kind": "S3", "bucket": "fixture", "region": "eu-central-1", "namespace": "tenant", "request_timeout_seconds": 120, "store_root": dir.path()}),
            json!({"kind": "Local", "store_root": dir.path(), "bucket": "fixture"}),
            json!({"kind": "S3", "bucket": "fixture", "region": "invalid region", "namespace": "tenant", "request_timeout_seconds": 120}),
            json!({"kind": "S3", "bucket": "fixture", "region": "eu-central-1", "namespace": "tenant", "request_timeout_seconds": 0}),
            json!({"kind": "S3", "bucket": "fixture", "region": "eu-central-1", "namespace": "tenant", "request_timeout_seconds": 601}),
        ] {
            value["backend"] = backend;
            let (path, sha) = write_config(dir.path(), &value);
            assert!(retained_config(&path, &sha).is_err());
        }
    }

    // A wrong dispatch/schema/approval or omitted receipt binding breaks this.
    #[test]
    fn retained_publishes_copied_source_gone_generation_and_bound_receipt() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let (dir, c) = runtime.block_on(retained_fixture());
        let (path, sha) = write_config(dir.path(), &c);
        let receipt = dir.path().join("retained-receipt.json");
        run(&retained_args(&path, &sha, &receipt)).unwrap();
        let body: Value = serde_json::from_slice(&fs::read(&receipt).unwrap()).unwrap();
        assert_eq!(body["schema"], RETAINED_RECEIPT_SCHEMA);
        assert_eq!(body["config_sha256"], sha);
        for field in [
            "backend",
            "retained_prefix",
            "original_root_sha256",
            "original_generation",
            "original_control_epoch",
            "sq8_object_key",
            "sq8_etag",
            "destination_prefix",
        ] {
            assert_eq!(body[field], c[field], "{field}");
        }
        assert_eq!(body["local_file_only"], true);
        assert_eq!(body["performance_claim"], false);
        assert_ne!(body["root_sha256"], c["original_root_sha256"]);
        assert_eq!(body["generation"], 1);
        assert_eq!(body["control_epoch"], 1);
        let store =
            LocalFileSystem::new_with_prefix(c["backend"]["store_root"].as_str().unwrap()).unwrap();
        let prefix = ObjectPath::from("rebound/index");
        runtime.block_on(async {
            let head = read_two_bit_head(&store, &prefix).await.unwrap().unwrap();
            assert_eq!(body["root_sha256"], head.root_sha256());
            assert_eq!(body["metadata_prefix"], head.metadata_prefix().to_string());
            assert_eq!(body["head_key"], "rebound/index/head.json");
            let generation = borsuk::two_bit_generation::TwoBitGeneration::open_remote_from_head(
                &store,
                &head,
                (&retained_config(&path, &sha).unwrap().limits).into(),
                Path::new(c["scratch_parent"].as_str().unwrap()),
            )
            .await
            .unwrap();
            assert_eq!(generation.rows(), 64);
            drop(generation);
        });
        let first = fs::read(&receipt).unwrap();
        assert!(run(&retained_args(&path, &sha, &receipt)).is_err());
        assert_eq!(fs::read(&receipt).unwrap(), first);
        let another = dir.path().join("another.json");
        assert!(run(&retained_args(&path, &sha, &another)).is_err());
        assert!(!another.exists());
    }

    // Removing strict config, output preflight, SHA or pending-head refusal breaks this.
    #[test]
    fn retained_refuses_bad_config_pending_head_and_occupied_output_without_head() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let (dir, good) = runtime.block_on(retained_fixture());
        let receipt = dir.path().join("retained-receipt.json");
        let store =
            LocalFileSystem::new_with_prefix(good["backend"]["store_root"].as_str().unwrap())
                .unwrap();
        let prefix = ObjectPath::from("rebound/index");
        let no_head = || {
            assert!(
                runtime
                    .block_on(read_two_bit_head(&store, &prefix))
                    .unwrap()
                    .is_none()
            )
        };
        let (path, sha) = write_config(dir.path(), &good);
        assert!(run(&retained_args(&path, &"0".repeat(64), &receipt)).is_err());
        fs::write(&receipt, b"occupied").unwrap();
        assert!(run(&retained_args(&path, &sha, &receipt)).is_err());
        assert_eq!(fs::read(&receipt).unwrap(), b"occupied");
        no_head();
        fs::remove_file(&receipt).unwrap();
        let missing_parent = dir.path().join("absent/receipt.json");
        assert!(run(&retained_args(&path, &sha, &missing_parent)).is_err());
        no_head();
        for (field, value) in [
            ("schema", json!(CONFIG_SCHEMA)),
            ("pending", json!(true)),
            ("original_root_sha256", json!("0".repeat(64))),
            ("original_generation", json!(2)),
            ("original_control_epoch", json!(2)),
            ("sq8_etag", json!("unapproved")),
            ("sq8_object_key", json!("unapproved/object")),
            ("destination_prefix", json!("../bad")),
            ("max_scratch_bytes", json!(0)),
        ] {
            let mut bad = good.clone();
            bad[field] = value;
            let (path, sha) = write_config(dir.path(), &bad);
            assert!(
                run(&retained_args(&path, &sha, &receipt)).is_err(),
                "{field}"
            );
            assert!(!receipt.exists());
            no_head();
        }
        for missing_limit in [false, true] {
            let mut bad = good.clone();
            if missing_limit {
                bad["limits"]
                    .as_object_mut()
                    .unwrap()
                    .remove("max_source_gets");
            } else {
                bad["limits"]["max_memory_bytes"] = json!(0);
            }
            let (path, sha) = write_config(dir.path(), &bad);
            assert!(run(&retained_args(&path, &sha, &receipt)).is_err());
            assert!(!receipt.exists());
            no_head();
        }
        for field in good.as_object().unwrap().keys() {
            let mut bad = good.clone();
            bad.as_object_mut().unwrap().remove(field);
            let (path, sha) = write_config(dir.path(), &bad);
            assert!(
                run(&retained_args(&path, &sha, &receipt)).is_err(),
                "missing {field}"
            );
            no_head();
        }
        let (path, sha) = write_config(dir.path(), &good);
        fs::write(&path, [b' '; 65_537]).unwrap();
        assert!(run(&retained_args(&path, &sha_hex(&[b' '; 65_537]), &receipt)).is_err());
        write_config(dir.path(), &good);
        let head_path = dir.path().join("copied/semantic/index/head.json");
        let original = fs::read(&head_path).unwrap();
        let mut pending: Value = serde_json::from_slice(&original).unwrap();
        pending["mutation"] = json!({"revision":1,"sha256":"1".repeat(64),"sealed":false});
        fs::write(&head_path, serde_json::to_vec(&pending).unwrap()).unwrap();
        assert!(run(&retained_args(&path, &sha, &receipt)).is_err());
        no_head();
        assert!(!receipt.exists());
        fs::write(&head_path, original).unwrap();
        let occupied = dir.path().join("copied/rebound/index/occupied");
        fs::create_dir_all(occupied.parent().unwrap()).unwrap();
        fs::write(&occupied, b"occupied").unwrap();
        assert!(run(&retained_args(&path, &sha, &receipt)).is_err());
        no_head();
        assert!(!receipt.exists());
        assert_eq!(fs::read(&occupied).unwrap(), b"occupied");
    }

    #[tokio::test]
    async fn publishes_real_generation_and_receipt_then_refuses_replay() {
        let (dir, c) = fixture(ROWS, D).await;
        let (path, sha) = write_config(dir.path(), &c);
        let receipt = dir.path().join("receipt.json");
        // An existing receipt is refused before anything is published.
        fs::write(&receipt, b"x").unwrap();
        assert!(publish(&path, &sha, &receipt).await.is_err());
        assert!(!head_exists(&c).await);
        fs::remove_file(&receipt).unwrap();

        publish(&path, &sha, &receipt).await.unwrap();
        assert!(head_exists(&c).await);
        let body: Value = serde_json::from_slice(&fs::read(&receipt).unwrap()).unwrap();
        assert_eq!(body["schema"], RECEIPT_SCHEMA);
        assert_eq!(body["config_sha256"], sha);
        assert_eq!(body["root_sha256"], c["root"]["sha256"]);
        assert_eq!(body["generation"], 1);
        assert_eq!(body["prefix"], "semantic/index");
        assert!(
            body["metadata_prefix"]
                .as_str()
                .unwrap()
                .starts_with("semantic/")
        );
        // Independent readback of the head against the receipt and config.
        let store = LocalFileSystem::new_with_prefix(c["store_root"].as_str().unwrap()).unwrap();
        let head = read_two_bit_head(&store, &ObjectPath::from("semantic/index"))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(head.root_sha256(), c["root"]["sha256"].as_str().unwrap());
        assert_eq!(head.generation(), 1);
        assert_eq!(
            head.control_epoch(),
            body["control_epoch"].as_u64().unwrap()
        );
        assert_eq!(head.metadata_prefix().to_string(), body["metadata_prefix"]);

        // Replay: the head is no longer fresh, and the receipt is never overwritten.
        let first = fs::read(&receipt).unwrap();
        let again = dir.path().join("again.json");
        let error = publish(&path, &sha, &again).await.unwrap_err();
        assert_eq!(error.to_string(), "head not fresh");
        assert!(!again.exists());
        assert!(publish(&path, &sha, &receipt).await.is_err());
        assert_eq!(fs::read(&receipt).unwrap(), first);
    }

    #[tokio::test]
    async fn rejects_wrong_root_cap_existing_head_and_bad_config() {
        let (dir, good) = fixture(ROWS, D).await;
        let receipt = dir.path().join("receipt.json");
        let mut wrong_root = good.clone();
        wrong_root["root"]["sha256"] = json!("0".repeat(64));
        let mut capped = good.clone();
        capped["limits"]["max_memory_bytes"] = json!(131_071);
        for bad in [wrong_root, capped] {
            let (path, sha) = write_config(dir.path(), &bad);
            assert!(publish(&path, &sha, &receipt).await.is_err());
            assert!(!receipt.exists());
            assert!(!head_exists(&good).await);
        }

        // Config identity, cap, schema, unknown/missing fields and sha shape.
        let (path, sha) = write_config(dir.path(), &good);
        assert!(config(&path, &sha).is_ok());
        assert!(config(&path, &"0".repeat(64)).is_err());
        let mut edits: Vec<Value> = Vec::new();
        for (field, value) in [
            ("unknown", json!(1)),
            ("schema", json!("other")),
            ("prefix", json!("../index")),
            ("store_root", json!(dir.path().join("missing"))),
        ] {
            let mut v = good.clone();
            v[field] = value;
            edits.push(v);
        }
        let mut v = good.clone();
        v["root"]["sha256"] = json!("A".repeat(64));
        edits.push(v);
        let mut v = good.clone();
        v["limits"]
            .as_object_mut()
            .unwrap()
            .remove("max_source_gets");
        edits.push(v);
        for v in edits {
            let (path, sha) = write_config(dir.path(), &v);
            assert!(config(&path, &sha).is_err() || publish(&path, &sha, &receipt).await.is_err());
            assert!(!receipt.exists());
        }
        fs::write(&path, [b' '; 65_537]).unwrap();
        assert!(config(&path, &sha_hex(&[b' '; 65_537])).is_err());

        // A published head blocks a second publication to the same prefix.
        let (path, sha) = write_config(dir.path(), &good);
        publish(&path, &sha, &receipt).await.unwrap();
        let other = dir.path().join("other.json");
        assert!(publish(&path, &sha, &other).await.is_err());
        assert!(!other.exists());
    }
}
