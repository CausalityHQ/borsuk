#!/bin/bash
set -euo pipefail
out=/mnt/borsuk-workspace/source-capsule-geometry-source-a0001/root-qualification-a0001
source /mnt/borsuk-workspace/environment.sh
cd /mnt/borsuk-workspace/source-capsule-geometry-source-a0001/source
exec >"$out/stdout" 2>"$out/stderr"
trap 'rc=$?; printf "%s\n" "$rc" > "$out/runner.exit"' EXIT
exec 9>/mnt/borsuk-workspace/cargo-target.lock
flock -n 9
test "$(git rev-parse HEAD)" = a2db885e44f516ae68d379335f126f5187a5225a
git sparse-checkout disable
rustfmt --edition 2024 crates/borsuk/examples/source_capsule_geometry.rs
git add crates/borsuk/examples/source_capsule_geometry.rs
if ! git diff --cached --quiet; then
  git -c core.hooksPath=/dev/null commit -m "Format capsule geometry diagnostic before native qualification"
fi
git rev-parse HEAD > "$out/source.commit"
sha256sum crates/borsuk/examples/source_capsule_geometry.rs crates/borsuk/Cargo.toml > "$out/source.sha256"
cargo test --offline --locked -p borsuk --example source_capsule_geometry > "$out/affected-tests.log" 2>&1
printf '0\n' > "$out/affected-tests.exit"
cargo build --offline --locked --release -p borsuk --example source_capsule_geometry > "$out/release.log" 2>&1
printf '0\n' > "$out/release.exit"
cargo clippy --offline --locked --workspace --all-targets -- -D clippy::correctness -D clippy::suspicious > "$out/clippy.log" 2>&1
printf '0\n' > "$out/clippy.exit"
env -u BORSUK_TEST_BUILD_COMMAND BORSUK_TEST_BUILD_JOBS=4 bash scripts/check_rust_test_build.sh > "$out/testbuild.log" 2>&1
printf '0\n' > "$out/testbuild.exit"
git diff --exit-code
sha256sum "$CARGO_TARGET_DIR/release/examples/source_capsule_geometry" > "$out/binary.sha256"
