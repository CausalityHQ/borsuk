#!/usr/bin/env bash
# DRAFT: invoke only inside the admitted remote native service, before locked stages.
set -euo pipefail
cd /mnt/borsuk-http/source
readonly evidence=/mnt/borsuk-http/evidence
sha256sum --check /mnt/borsuk-http/source-files.sha256 > "$evidence/source-original-before-resolution.log"
cp Cargo.lock "$evidence/Cargo.lock.original"
# Preserve existing registry versions; add only dependencies absent from the lock.
cargo update --workspace > "$evidence/lock-resolution.log" 2>&1
cp Cargo.lock "$evidence/Cargo.lock.resolved"
# The sole admitted source mutation in this preparatory step is Cargo.lock.
awk '$2 != "Cargo.lock" && $2 != "./Cargo.lock"' /mnt/borsuk-http/source-files.sha256 > "$evidence/source-except-lock.sha256"
sha256sum --check "$evidence/source-except-lock.sha256" > "$evidence/source-except-lock-after-resolution.log"
sha256sum Cargo.lock > "$evidence/resolved-lock.sha256"
cat "$evidence/source-except-lock.sha256" "$evidence/resolved-lock.sha256" > "$evidence/source-resolved.sha256"
sha256sum --check "$evidence/source-resolved.sha256" > "$evidence/source-resolved-before-gates.log"
# Every native test/build/clippy command is --locked; check source-resolved.sha256 again after all stages.
# Root must authenticate and independently review the complete resolved lock and every source byte before integration.
