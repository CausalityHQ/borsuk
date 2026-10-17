use crate::{
    Result, ensure,
    io::{self, CONTROL, add, mul},
};
use borsuk::{
    semantic_unit_router::SemanticProfile, source_capsule::Geometry,
    two_bit_source::SourcePlaneReceipt,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    collections::BTreeSet,
    path::{Path, PathBuf},
};

pub const ORIGINAL_ROOT_SHA: &str =
    "96609071a9186cb3e9a159522f3ee6dfbc5dc81e86f87a8038632e5531b815ef";
pub const V10: &str = "borsuk-two-bit-generation-v10";
pub const V11: &str = "borsuk-two-bit-generation-v11";

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RootPin {
    pub path: PathBuf,
    pub sha256: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FilePin {
    pub path: PathBuf,
    pub bytes: u64,
    pub sha256: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub original_root: RootPin,
    pub old_asset_root: PathBuf,
    pub local_sq8: FilePin,
    pub order: FilePin,
    pub fresh_flat: PathBuf,
    pub fresh_caps: PathBuf,
    pub cap_bytes: usize,
}
impl Config {
    pub fn receipt_path(&self) -> Result<PathBuf> {
        let mut name = self
            .fresh_caps
            .file_name()
            .ok_or("capsule output name")?
            .to_os_string();
        name.push(".construction-receipt.json");
        Ok(self.fresh_caps.with_file_name(name))
    }
    pub fn check(&self) -> Result<()> {
        ensure(
            self.original_root.sha256 == ORIGINAL_ROOT_SHA,
            "unapproved original root",
        )?;
        io::admit(self.cap_bytes, &[CONTROL, 131072])?;
        for p in [
            &self.original_root.path,
            &self.old_asset_root,
            &self.local_sq8.path,
            &self.order.path,
            &self.fresh_flat,
            &self.fresh_caps,
        ] {
            ensure(
                p.is_absolute() && p.to_str().is_some_and(|s| s.len() <= 4096),
                "absolute bounded path",
            )?;
            ensure(
                !p.components()
                    .any(|c| matches!(c, std::path::Component::ParentDir)),
                "parent path component",
            )?;
        }
        for out in [&self.fresh_flat, &self.fresh_caps] {
            for input in [
                &self.old_asset_root,
                &self.original_root.path,
                &self.local_sq8.path,
                &self.order.path,
            ] {
                ensure(
                    !out.starts_with(input) && !input.starts_with(out),
                    "output overlaps historical input",
                )?;
            }
            io::absent(out)?;
        }
        ensure(
            !self.fresh_flat.starts_with(&self.fresh_caps)
                && !self.fresh_caps.starts_with(&self.fresh_flat),
            "output overlap",
        )?;
        io::absent(&self.receipt_path()?)?;
        ensure(
            io::valid_sha(&self.local_sq8.sha256) && io::valid_sha(&self.order.sha256),
            "explicit SQ8/order pins",
        )?;
        Ok(())
    }
}

#[derive(Clone, Serialize)]
pub struct Asset {
    pub path: String,
    pub bytes: usize,
    pub sha256: String,
}
pub struct Authority {
    pub root: Value,
    pub plane: SourcePlaneReceipt,
    pub plane_body: Vec<u8>,
    pub page_body: Vec<u8>,
    pub assets: Vec<Asset>,
    pub profile: SemanticProfile,
    original_snapshots: Vec<(PathBuf, io::Input)>,
}
pub fn text<'a>(v: &'a Value, key: &str) -> Result<&'a str> {
    v.get(key)
        .and_then(Value::as_str)
        .ok_or_else(|| format!("string field: {key}").into())
}
pub fn number(v: &Value, key: &str) -> Result<usize> {
    usize::try_from(
        v.get(key)
            .and_then(Value::as_u64)
            .ok_or_else(|| format!("integer field: {key}"))?,
    )
    .map_err(Into::into)
}
fn fields(v: &Value, names: &[&str]) -> Result<()> {
    let obj = v.as_object().ok_or("object required")?;
    ensure(
        obj.len() == names.len() && names.iter().all(|n| obj.contains_key(*n)),
        "unknown/missing manifest fields",
    )
}
#[derive(Deserialize)]
struct Coefficients {
    low: Vec<f32>,
    step: Vec<f32>,
}
pub fn coefficient_bits(body: &[u8]) -> Result<(Vec<u32>, Vec<u32>)> {
    let c: Coefficients = serde_json::from_slice(body)?;
    ensure(
        c.low.iter().all(|v| v.is_finite()) && c.step.iter().all(|v| v.is_finite() && *v > 0.),
        "coefficient values",
    )?;
    Ok((
        c.low.iter().map(|v| v.to_bits()).collect(),
        c.step.iter().map(|v| v.to_bits()).collect(),
    ))
}

impl Authority {
    pub fn load(c: &Config) -> Result<Self> {
        c.check()?;
        let root_body = io::read(
            &c.original_root.path,
            io::length(&c.original_root.path, 65536)?,
            ORIGINAL_ROOT_SHA,
        )?;
        let mut root: Value = serde_json::from_slice(&root_body)?;
        fields(
            &root,
            &[
                "schema",
                "generation",
                "base_epoch",
                "plane_manifest_sha256",
                "page_manifest_sha256",
                "discovery",
                "source_layout",
                "sq8_object_sha256",
                "sq8_object_key",
                "sq8_etag",
                "canonical",
                "low",
                "step",
            ],
        )?;
        ensure(
            text(&root, "schema")? == V10
                && number(&root, "generation")? == 1
                && number(&root, "base_epoch")? == 0,
            "historical v10 generation1/base_epoch0 required",
        )?;
        fields(&root["source_layout"], &["layout"])?;
        ensure(
            root["source_layout"]["layout"] == "flat_records",
            "historical flat layout required",
        )?;
        fields(
            &root["canonical"],
            &["rows", "dimensions", "bytes", "sha256", "object_key"],
        )?;
        fields(
            &root["discovery"],
            &[
                "mode",
                "profile",
                "root_sha256",
                "root_bytes",
                "membership_sha256",
                "membership_bytes",
                "leaves_sha256",
                "leaves_bytes",
                "input_schema",
                "input_root_sha256",
                "centroids_sha256",
                "source_sha256",
                "source_order_sha256",
                "mean_sha256",
                "records_sha256",
                "sq8_sha256",
            ],
        )?;
        ensure(
            root["discovery"]["mode"] == "semantic",
            "semantic router required",
        )?;
        let profile: SemanticProfile =
            serde_json::from_value(root["discovery"]["profile"].clone())?;
        let plane_path = c.old_asset_root.join("plane/manifest.json");
        let page_path = c.old_asset_root.join("page_manifest.json");
        let plane_body = io::read(
            &plane_path,
            io::length(&plane_path, 65536)?,
            text(&root, "plane_manifest_sha256")?,
        )?;
        let page_body = io::read(
            &page_path,
            io::length(&page_path, 65536)?,
            text(&root, "page_manifest_sha256")?,
        )?;
        let plane: SourcePlaneReceipt = serde_json::from_slice(&plane_body)?;
        let page: Value = serde_json::from_slice(&page_body)?;
        fields(
            &page,
            &[
                "schema",
                "generation",
                "rows",
                "dimensions",
                "page_rows",
                "object_sha256",
                "page_digest_sha256",
            ],
        )?;
        let (n, d) = (plane.rows, plane.dimensions);
        ensure(profile.valid_geometry(n, d), "profile geometry")?;
        let geometry = Geometry::new(n, d, profile)?;
        ensure(
            plane.schema == "borsuk-two-bit-plane-v3"
                && plane.seed == 20260923
                && !plane.query_or_truth_used
                && plane.page_rows == 32
                && plane.record_bytes == geometry.record_bytes(),
            "original source codec/geometry",
        )?;
        ensure(
            number(&root["canonical"], "rows")? == n
                && number(&root["canonical"], "dimensions")? == d,
            "canonical geometry",
        )?;
        ensure(
            page["schema"] == "borsuk-v115-sq8-page-authority-v2"
                && number(&page, "generation")? == 1
                && number(&page, "rows")? == n
                && number(&page, "dimensions")? == d
                && number(&page, "page_rows")? == 32,
            "SQ8 page binding",
        )?;
        let discovery = &root["discovery"];
        ensure(
            text(discovery, "input_schema")? == plane.schema
                && text(discovery, "input_root_sha256")? == text(&root, "plane_manifest_sha256")?,
            "independent plane-bound router input",
        )?;
        for (key, value) in [
            ("source_sha256", plane.source_sha256.as_str()),
            ("source_order_sha256", plane.source_order_sha256.as_str()),
            ("mean_sha256", plane.mean_sha256.as_str()),
            ("records_sha256", plane.records_sha256.as_str()),
            ("sq8_sha256", plane.sq8_sha256.as_str()),
        ] {
            ensure(
                io::valid_sha(value) && text(discovery, key)? == value,
                "discovery/source identity",
            )?;
        }
        ensure(
            c.local_sq8.sha256 == plane.sq8_sha256
                && text(&root, "sq8_object_sha256")? == c.local_sq8.sha256
                && text(&page, "object_sha256")? == c.local_sq8.sha256,
            "SQ8 independent identity",
        )?;
        ensure(
            c.local_sq8.bytes == mul(n, add(d, 12)?)? as u64
                && c.order.bytes == mul(n, 8)? as u64
                && c.order.sha256 == plane.source_order_sha256,
            "order/SQ8 length or digest",
        )?;
        let canonical_bytes = mul(n, add(mul(d, 4)?, 8)?)?;
        ensure(
            number(&root["canonical"], "bytes")? == canonical_bytes,
            "canonical exact length",
        )?;
        ensure(
            number(discovery, "membership_bytes")? == mul(n.div_ceil(32), 4)?
                && number(discovery, "leaves_bytes")? == mul(n.div_ceil(32), add(4, mul(d, 2)?)?)?,
            "router payload lengths",
        )?;
        ensure(
            (512..=profile.root_cap()).contains(&number(discovery, "root_bytes")?),
            "router root bound",
        )?;
        let mut assets = Vec::with_capacity(11);
        for (path, bytes, sha256) in [
            (
                "plane/manifest.json",
                plane_body.len(),
                text(&root, "plane_manifest_sha256")?,
            ),
            ("plane/mean.bin", mul(d, 4)?, plane.mean_sha256.as_str()),
            (
                "plane/records.bin",
                mul(n, plane.record_bytes)?,
                plane.records_sha256.as_str(),
            ),
            (
                "plane/page_digests.bin",
                mul(n.div_ceil(32), 32)?,
                plane.page_digest_sha256.as_str(),
            ),
            (
                "page_manifest.json",
                page_body.len(),
                text(&root, "page_manifest_sha256")?,
            ),
            (
                "page_digests.bin",
                mul(n.div_ceil(32), 32)?,
                text(&page, "page_digest_sha256")?,
            ),
            (
                "router/root.bin",
                number(discovery, "root_bytes")?,
                text(discovery, "root_sha256")?,
            ),
            (
                "router/membership.bin",
                number(discovery, "membership_bytes")?,
                text(discovery, "membership_sha256")?,
            ),
            (
                "router/leaves.bin",
                number(discovery, "leaves_bytes")?,
                text(discovery, "leaves_sha256")?,
            ),
            (
                "centroids.bin",
                add(32, mul(mul(n.div_ceil(32), d)?, 2)?)?,
                text(discovery, "centroids_sha256")?,
            ),
            (
                "canonical.bin",
                canonical_bytes,
                text(&root["canonical"], "sha256")?,
            ),
        ] {
            ensure(io::valid_sha(sha256), "asset digest")?;
            assets.push(Asset {
                path: path.into(),
                bytes,
                sha256: sha256.into(),
            });
        }
        // Normalize only the spelling of coefficients; native f32 bits must survive.
        let coefficients: Coefficients = serde_json::from_slice(&root_body)?;
        ensure(
            coefficients.low.len() == d && coefficients.step.len() == d,
            "coefficient dimensions",
        )?;
        let bits = coefficient_bits(&root_body)?;
        root["low"] = serde_json::to_value(coefficients.low)?;
        root["step"] = serde_json::to_value(coefficients.step)?;
        ensure(
            coefficient_bits(&serde_json::to_vec(&root)?)? == bits,
            "coefficient f32 bit identity",
        )?;
        io::stream(
            &c.old_asset_root.join("manifest.json"),
            root_body.len() as u64,
            ORIGINAL_ROOT_SHA,
            None,
        )?;
        let mut a = Self {
            root,
            plane,
            plane_body,
            page_body,
            assets,
            profile,
            original_snapshots: Vec::new(),
        };
        for pin in &a.assets {
            let path = c.old_asset_root.join(&pin.path);
            a.original_snapshots
                .push((path.clone(), io::Input::open(&path, pin.bytes as u64)?));
        }
        for (path, bytes) in [
            (&c.original_root.path, root_body.len() as u64),
            (
                &c.old_asset_root.join("manifest.json"),
                root_body.len() as u64,
            ),
            (&c.local_sq8.path, c.local_sq8.bytes),
            (&c.order.path, c.order.bytes),
        ] {
            a.original_snapshots
                .push((path.clone(), io::Input::open(path, bytes)?));
        }
        a.check_tree(&c.old_asset_root, false)?;
        Ok(a)
    }
    pub fn asset(&self, name: &str) -> Result<&Asset> {
        self.assets
            .iter()
            .find(|a| a.path == name)
            .ok_or_else(|| "unknown asset path".into())
    }
    pub fn read_asset(&self, root: &Path, name: &str) -> Result<Vec<u8>> {
        let p = self.asset(name)?;
        io::read(&root.join(name), p.bytes, &p.sha256)
    }
    pub fn check_tree(&self, root: &Path, capsules: bool) -> Result<()> {
        io::directory(root)?;
        let mut expected: BTreeSet<String> = self.assets.iter().map(|a| a.path.clone()).collect();
        expected.insert("manifest.json".into());
        if capsules {
            expected.remove("plane/records.bin");
            expected.insert("plane/capsules.bin".into());
            expected.insert("plane/capsule_directory.bin".into());
        }
        let mut found = BTreeSet::new();
        for sub in ["", "plane", "router"] {
            let dir = root.join(sub);
            io::directory(&dir)?;
            let mut count = 0;
            for entry in std::fs::read_dir(&dir)? {
                count += 1;
                ensure(count <= 14, "unexpected asset count")?;
                let entry = entry?;
                let name = entry
                    .file_name()
                    .into_string()
                    .map_err(|_| "non-UTF8 asset name")?;
                let rel = if sub.is_empty() {
                    name.clone()
                } else {
                    format!("{sub}/{name}")
                };
                let kind = entry.file_type()?;
                if sub.is_empty() && (name == "plane" || name == "router") {
                    ensure(kind.is_dir(), "symlink/non-directory asset namespace")?;
                } else {
                    ensure(
                        kind.is_file() && expected.contains(&rel) && found.insert(rel),
                        "unknown/nonregular asset path",
                    )?;
                }
            }
        }
        ensure(found == expected, "incomplete asset allowlist")
    }
    pub fn recheck_original(&self, c: &Config) -> Result<()> {
        self.check_tree(&c.old_asset_root, false)?;
        for (path, snapshot) in &self.original_snapshots {
            snapshot.unchanged(path)?;
        }
        io::stream(
            &c.original_root.path,
            io::length(&c.original_root.path, 65536)? as u64,
            ORIGINAL_ROOT_SHA,
            None,
        )?;
        io::stream(
            &c.old_asset_root.join("manifest.json"),
            io::length(&c.old_asset_root.join("manifest.json"), 65536)? as u64,
            ORIGINAL_ROOT_SHA,
            None,
        )?;
        for p in &self.assets {
            io::stream(
                &c.old_asset_root.join(&p.path),
                p.bytes as u64,
                &p.sha256,
                None,
            )?;
        }
        io::stream(
            &c.local_sq8.path,
            c.local_sq8.bytes,
            &c.local_sq8.sha256,
            None,
        )?;
        io::stream(&c.order.path, c.order.bytes, &c.order.sha256, None)?;
        for (path, snapshot) in &self.original_snapshots {
            snapshot.unchanged(path)?;
        }
        Ok(())
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn strict_authority_and_coefficients() {
        assert!(
            serde_json::from_str::<RootPin>(r#"{"path":"/a","sha256":"x","extra":1}"#).is_err()
        );
        assert!(
            serde_json::from_str::<RootPin>(r#"{"path":"/a","path":"/b","sha256":"x"}"#).is_err()
        );
        let b =
            coefficient_bits(br#"{"low":[-0.0,0.10000000149011612],"step":[1,0.0001]}"#).unwrap();
        assert_eq!(b.0, vec![(-0.0f32).to_bits(), 0.1f32.to_bits()]);
        assert!(coefficient_bits(br#"{"low":[0],"step":[0]}"#).is_err());
    }
}
