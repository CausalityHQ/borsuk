#![allow(missing_docs)]

use std::{fs, path::PathBuf};

#[test]
fn crate_metadata_declares_public_project_urls() {
    let crate_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let workspace_manifest = fs::read_to_string(crate_root.join("../../Cargo.toml")).unwrap();
    let crate_manifest = fs::read_to_string(crate_root.join("Cargo.toml")).unwrap();
    let license = fs::read_to_string(crate_root.join("../../LICENSE")).unwrap();

    assert_contains(
        &workspace_manifest,
        r#"repository = "https://github.com/CausalityHQ/borsuk""#,
    );
    assert_contains(
        &workspace_manifest,
        r#"homepage = "http://causality.pl/borsuk/""#,
    );
    assert_contains(
        &workspace_manifest,
        r#"documentation = "https://docs.rs/borsuk""#,
    );
    assert_contains(&crate_manifest, r#"license-file = "../../LICENSE""#);
    assert_contains(
        &crate_manifest,
        "description = \"Blob-Oriented Retrieval with Segmental Unified KNN\"",
    );
    assert_contains(&license, "Business Source License 1.1");
    assert_contains(&license, "US $100,000");
}

fn assert_contains(haystack: &str, needle: &str) {
    assert!(
        haystack.contains(needle),
        "expected metadata to contain `{needle}`"
    );
}
