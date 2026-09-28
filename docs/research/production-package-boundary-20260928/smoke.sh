#!/usr/bin/env bash
set -eu
campaign_dir=$(cd "$1" && pwd)
cargo_bin=/home/riomus/.cargo/bin/cargo
cd "$campaign_dir"
finish() {
    result=$?
    printf '%s\n' "$result" > "$campaign_dir/smoke.exit"
}
trap finish EXIT
sha256sum ./*.crate > received.sha256
for archive in ./*.crate; do tar xf "$archive"; done
cat > smoke-config.toml <<EOF
[patch.crates-io]
borsuk-fma = { path = "$campaign_dir/borsuk-fma-0.1.0" }
borsuk-pq4 = { path = "$campaign_dir/borsuk-pq4-0.1.0" }
EOF
export CARGO_BUILD_JOBS=2
export CARGO_TARGET_DIR="$campaign_dir/../rust-repo/target"
cd borsuk-0.1.0
# Local helper archives supply unpublished dependencies. This is not a registry install.
timeout 300 "$cargo_bin" --config "$campaign_dir/smoke-config.toml" metadata --format-version 1 > "$campaign_dir/resolved-metadata.json"
for test_filter in sq8_source::tests sq8_s3_range::tests; do
    timeout 1200 "$cargo_bin" --config "$campaign_dir/smoke-config.toml" test --locked --offline -p borsuk --lib "$test_filter"
done
timeout 1200 "$cargo_bin" --config "$campaign_dir/smoke-config.toml" test --locked --offline -p borsuk --test package_metadata
timeout 1200 "$cargo_bin" --config "$campaign_dir/smoke-config.toml" build --locked --offline -p borsuk --example build_sq8_source
