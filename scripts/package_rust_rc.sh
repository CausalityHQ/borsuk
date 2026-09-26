#!/usr/bin/env bash
set -euo pipefail
output=${1:?usage: package_rust_rc.sh OUTPUT.tar.gz}
test -z "$(git status --porcelain)"
git archive --format=tar.gz --prefix=borsuk-rust-rc/ -o "$output" HEAD
sha256sum "$output"
