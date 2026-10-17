//! Source-only, one-off construction of a separately approved historical input.
//! This package does not change the production reader or authorize publication.
mod authority;
mod io;
mod verify;

use authority::{Authority, Config, ORIGINAL_ROOT_SHA, V11, text};
use borsuk::{
    two_bit_build::repackage_source_layout,
    two_bit_generation::{SourceLayoutMode, TwoBitGeneration},
};
use serde_json::{Value, json};
use std::{error::Error, path::Path};

type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;
fn ensure(ok: bool, message: &str) -> Result<()> {
    if ok {
        Ok(())
    } else {
        Err(message.to_owned().into())
    }
}

fn config() -> Result<(Config, String)> {
    let args = std::env::args_os().collect::<Vec<_>>();
    ensure(
        args.len() == 3,
        "usage: capsule-page-range-v2-construction CONFIG_JSON CONFIG_SHA256",
    )?;
    let expected = args[2].to_str().ok_or("config SHA UTF8")?;
    ensure(
        io::valid_sha(expected),
        "independent config SHA256 required",
    )?;
    let path = Path::new(&args[1]);
    let body = io::read(path, io::length(path, 65536)?, expected)?;
    let config: Config = serde_json::from_slice(&body)?;
    config.check()?;
    Ok((config, expected.into()))
}
fn fresh_flat(a: &mut Authority, c: &Config) -> Result<(String, verify::IdReceipt)> {
    io::new_directory(&c.fresh_flat)?;
    io::new_directory(&c.fresh_flat.join("plane"))?;
    io::new_directory(&c.fresh_flat.join("router"))?;
    for pin in &a.assets {
        io::stream(
            &c.old_asset_root.join(&pin.path),
            pin.bytes as u64,
            &pin.sha256,
            Some(&c.fresh_flat.join(&pin.path)),
        )?;
    }
    // These three tiny/codec assets complete the copy-readback coverage; the
    // remaining eight are fully authenticated by the validators below.
    for name in [
        "plane/manifest.json",
        "plane/mean.bin",
        "page_manifest.json",
    ] {
        let pin = a.asset(name)?;
        io::stream(
            &c.fresh_flat.join(name),
            pin.bytes as u64,
            &pin.sha256,
            None,
        )?;
    }
    verify::router_publication(a, &c.fresh_flat, c.cap_bytes)?;
    verify::flat_source(a, &c.fresh_flat, c.cap_bytes)?;
    let ids = verify::canonical_sq8_ids(
        a,
        c,
        &c.fresh_flat,
        &a.page_body,
        text(&a.root, "page_manifest_sha256")?,
    )?;
    a.root["schema"] = V11.into();
    let root = serde_json::to_vec(&a.root)?;
    ensure(root.len() <= 65536, "fresh flat root size")?;
    io::directory(&c.fresh_flat.join("plane"))?.sync_all()?;
    io::directory(&c.fresh_flat.join("router"))?.sync_all()?;
    io::seal(&c.fresh_flat.join("manifest.json"), &root)?;
    a.check_tree(&c.fresh_flat, false)?;
    Ok((io::sha(&root), ids))
}

fn logical_capsule_assets(a: &Authority, c: &Config, sha: &str) -> Result<Value> {
    let path = c.fresh_caps.join("manifest.json");
    let body = io::read(&path, io::length(&path, 65536)?, sha)?;
    let root: Value = serde_json::from_slice(&body)?;
    ensure(
        root["schema"] == V11 && root["generation"] == 2 && root["base_epoch"] == 0,
        "fresh capsule generation/epoch",
    )?;
    let page_path = c.fresh_caps.join("page_manifest.json");
    let page_body = io::read(
        &page_path,
        io::length(&page_path, 65536)?,
        text(&root, "page_manifest_sha256")?,
    )?;
    let mut expected_page: Value = serde_json::from_slice(&a.page_body)?;
    expected_page["generation"] = 2.into();
    ensure(
        serde_json::from_slice::<Value>(&page_body)? == expected_page,
        "unexpected logical page-manifest change",
    )?;
    ensure(
        authority::coefficient_bits(&body)?
            == authority::coefficient_bits(&serde_json::to_vec(&a.root)?)?,
        "SQ8 coefficient f32 bits changed",
    )?;
    let mut logical = root.clone();
    logical["generation"] = 1.into();
    logical["page_manifest_sha256"] = a.root["page_manifest_sha256"].clone();
    logical["source_layout"] = a.root["source_layout"].clone();
    logical["low"] = a.root["low"].clone();
    logical["step"] = a.root["step"].clone();
    ensure(
        logical == a.root,
        "native repack changed original logical authority",
    )?;
    a.check_tree(&c.fresh_caps, true)?;
    for pin in &a.assets {
        if pin.path == "plane/records.bin" || pin.path == "page_manifest.json" {
            continue;
        }
        io::stream(
            &c.fresh_caps.join(&pin.path),
            pin.bytes as u64,
            &pin.sha256,
            None,
        )?;
    }
    Ok(root)
}

fn run() -> Result<()> {
    let (c, config_sha) = config()?;
    let mut a = Authority::load(&c)?;
    verify::reject_historical(&c)?;
    let (flat_sha, ids) = fresh_flat(&mut a, &c)?;
    // All router payloads, IDs, bitsets and streaming buffers have been dropped.
    // CONTROL charges the still-held config, small authority and receipt metadata.
    // Native repack validates the flat publication completely before creating its
    // output; public full-router/canonical/SQ8 checks above complete that proof.
    let caps_sha = repackage_source_layout(
        &c.fresh_flat,
        &flat_sha,
        &c.fresh_caps,
        2,
        SourceLayoutMode::LeafCapsulesV2,
        verify::limits(c.cap_bytes),
    )?;
    let caps_root = logical_capsule_assets(&a, &c, &caps_sha)?;
    let capsules = verify::capsules(&a, &c, &caps_root)?;
    // Capsule open is paged: this checks native root/codec/page/router admission
    // without hydrating the canonical source or the physical capsule blob.
    drop(TwoBitGeneration::open(
        &c.fresh_caps,
        &caps_sha,
        verify::limits(c.cap_bytes),
    )?);
    a.recheck_original(&c)?;
    a.check_tree(&c.fresh_flat, false)?;
    a.check_tree(&c.fresh_caps, true)?;
    for (path, digest) in [
        (c.fresh_flat.join("manifest.json"), flat_sha.as_str()),
        (c.fresh_caps.join("manifest.json"), caps_sha.as_str()),
    ] {
        io::stream(&path, io::length(&path, 65536)? as u64, digest, None)?;
    }
    let receipt = json!({
        "schema":"borsuk-research-page-range-v2-construction-receipt-v1",
        "status":"complete_source_construction_validation",
        "config_sha256":config_sha,
        "api_contract_source_base":"329fd2227a17c049b6fa9ceeee71f3ea57f24aad",
        "library_revision_requires_external_binding":true,
        "original_root_sha256":ORIGINAL_ROOT_SHA,
        "original_asset_root":c.old_asset_root,
        "original_generation":1,"base_epoch":0,
        "original_assets":a.assets,
        "fresh_flat":{"path":c.fresh_flat,"generation":1,"root_sha256":flat_sha},
        "fresh_capsules":{"path":c.fresh_caps,"generation":2,"root_sha256":caps_sha,"whole_artifact":capsules},
        "ids":ids,
        "sq8":{"path":c.local_sq8.path,"bytes":c.local_sq8.bytes,"sha256":c.local_sq8.sha256},
        "order":{"path":c.order.path,"bytes":c.order.bytes,"sha256":c.order.sha256},
        "checks":{
            "production_rejected_original_v10":true,
            "all_original_asset_full_pins_rechecked":true,
            "all_flat_original_source_units_verified":true,
            "full_native_router_publication_verified":true,
            "canonical_full_sha_and_all_position_ids_equal_sq8":true,
            "sq8_full_sha_and_all_original_pages_verified":true,
            "unique_application_ids_and_raw_order_permutation":true,
            "unchanged_capsule_logical_assets_and_ids_by_full_sha":true,
            "all_capsule_copies_verified_against_original_unit_authority":true,
            "native_flat_publication_admission_via_repackage":true,
            "native_capsule_root_admission":true
        },
        "cap_bytes":c.cap_bytes,
        "query_or_truth_used":false,"s3_writes":false,"performance_claim":false
    });
    let receipt_path = c.receipt_path()?;
    let body = serde_json::to_vec_pretty(&receipt)?;
    ensure(
        body.len() <= io::CONTROL / 2,
        "construction receipt metadata cap",
    )?;
    io::seal(&receipt_path, &body)?;
    println!(
        "{}",
        json!({"receipt":receipt_path,"receipt_sha256":io::sha(&body),"flat_root_sha256":flat_sha,"capsule_root_sha256":caps_sha})
    );
    Ok(())
}
fn main() {
    if let Err(error) = run() {
        eprintln!("construction failed; no complete construction receipt: {error}");
        std::process::exit(1);
    }
}
