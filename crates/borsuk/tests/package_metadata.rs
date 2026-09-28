#![allow(missing_docs)]

use std::{fs, path::PathBuf};

#[test]
fn crate_metadata_declares_public_project_urls() {
    let crate_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let crate_manifest = fs::read_to_string(crate_root.join("Cargo.toml")).unwrap();
    let license = fs::read_to_string(crate_root.join(env!("CARGO_PKG_LICENSE_FILE"))).unwrap();

    assert_eq!(
        env!("CARGO_PKG_REPOSITORY"),
        "https://github.com/CausalityHQ/borsuk"
    );
    assert_eq!(env!("CARGO_PKG_HOMEPAGE"), "http://causality.pl/borsuk/");
    assert_eq!(
        env!("CARGO_PKG_DESCRIPTION"),
        "Blob-Oriented Retrieval with Segmental Unified KNN"
    );
    let documentation_manifest = if crate_manifest.contains("documentation.workspace = true") {
        fs::read_to_string(crate_root.join("../../Cargo.toml")).unwrap()
    } else {
        crate_manifest
    };
    assert_contains(
        &documentation_manifest,
        r#"documentation = "https://docs.rs/borsuk""#,
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
