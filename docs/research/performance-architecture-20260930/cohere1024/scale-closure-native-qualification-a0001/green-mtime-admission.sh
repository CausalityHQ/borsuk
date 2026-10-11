#!/bin/bash
set -euo pipefail
root=/mnt/borsuk-scale-closure-qualification
ev="$root/evidence/green-mtime-admission"
mkdir "$ev"
cd "$root/source"
sha256sum -c ../source-files.sha256 > "$ev/source-before.log"
stat -c '%n %y %s' crates/borsuk/src/semantic_unit_router.rs crates/borsuk/src/two_bit_generation.rs target/debug/deps/borsuk-a25da2756a39dd18 > "$ev/timestamps-before.txt"
sha256sum target/debug/deps/borsuk-a25da2756a39dd18 > "$ev/old-test-elf.sha256"
touch crates/borsuk/src/semantic_unit_router.rs crates/borsuk/src/two_bit_generation.rs
stat -c '%n %y %s' crates/borsuk/src/semantic_unit_router.rs crates/borsuk/src/two_bit_generation.rs target/debug/deps/borsuk-a25da2756a39dd18 > "$ev/timestamps-after.txt"
sha256sum -c ../source-files.sha256 > "$ev/source-after.log"
cmp "$ev/source-before.log" "$ev/source-after.log"
jq -n '{schema:"borsuk-scale-closure-build-mtime-admission-v1",source_commit:"6e78d393b4d7dadf19befeeabe3dc20a77319a26",source_bytes_unchanged:true,native_qualified:false}' > "$ev/terminal.json"
(cd "$ev"; find . -type f ! -name SHA256SUMS -print0 | sort -z | xargs -0 sha256sum) > "$ev/SHA256SUMS"
export AWS_MAX_ATTEMPTS=1 AWS_DEFAULT_REGION=eu-central-1
publication_deadline=$(( $(date +%s) + 120 ))
for file in "$ev"/*; do
remaining=$(( publication_deadline - $(date +%s) - 2 )); (( remaining > 0 )) || exit 124
(( remaining <= 30 )) || remaining=30
timeout --kill-after=2 "$remaining" aws s3api put-object --bucket borsuk-bench-453182569524-euc1 --key research/semantic-router/20261011/scale-closure-native-qualification-a0001/stages/green-mtime-admission/"$(basename "$file")" --body "$file" --if-none-match '*' > /dev/null
done
cat "$ev/terminal.json"
