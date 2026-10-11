#!/bin/bash
set -euo pipefail
root=/mnt/borsuk-scale-closure-qualification
export AWS_MAX_ATTEMPTS=1 AWS_DEFAULT_REGION=eu-central-1
cd "$root"
ev="$root/evidence/green-source-admission"
mkdir "$ev"
(cd source; sha256sum -c ../source-files.sha256) > "$ev/red-source-before.log"
cp source-files.sha256 "$ev/source-files.red.sha256"
cp native-source.json "$ev/native-source.red.json"
cp source/crates/borsuk/src/semantic_unit_router.rs "$ev/semantic_unit_router.red.rs"
cp source/crates/borsuk/src/two_bit_generation.rs "$ev/two_bit_generation.red.rs"
timeout --kill-after=2 30 aws s3 cp s3://borsuk-bench-453182569524-euc1/research/semantic-router/20261011/scale-closure-native-qualification-a0001/green-inputs/semantic_unit_router.candidate.rs semantic_unit_router.candidate.rs --only-show-errors
printf "%s  %s\n" 4a580f2bb3afe2c0c30c5a11b6491b28d4165788d69bb39432b748e67fc68718 semantic_unit_router.candidate.rs | sha256sum -c -
test "$(stat -c %s semantic_unit_router.candidate.rs)" = 81578
timeout --kill-after=2 30 aws s3 cp s3://borsuk-bench-453182569524-euc1/research/semantic-router/20261011/scale-closure-native-qualification-a0001/green-inputs/two_bit_generation.candidate.rs two_bit_generation.candidate.rs --only-show-errors
printf "%s  %s\n" 3cb8d291eeda39bb29f27884f898e34a4a7f444059d52599a36439aa8fe4a0a9 two_bit_generation.candidate.rs | sha256sum -c -
test "$(stat -c %s two_bit_generation.candidate.rs)" = 395012
timeout --kill-after=2 30 aws s3 cp s3://borsuk-bench-453182569524-euc1/research/semantic-router/20261011/scale-closure-native-qualification-a0001/green-inputs/source-files.candidate.sha256 source-files.candidate.sha256 --only-show-errors
printf "%s  %s\n" ab5ceffcc3b1dc99d8af75fb4bc86a7b2c5035c59ac9a163e40fc9316fafd2ea source-files.candidate.sha256 | sha256sum -c -
test "$(stat -c %s source-files.candidate.sha256)" = 50571
timeout --kill-after=2 30 aws s3 cp s3://borsuk-bench-453182569524-euc1/research/semantic-router/20261011/scale-closure-native-qualification-a0001/green-inputs/native-source.candidate.json native-source.candidate.json --only-show-errors
printf "%s  %s\n" ce4671b10f92214be94f03a7ea24f7c46f713a19ed0685bfed7242bcae2418e5 native-source.candidate.json | sha256sum -c -
test "$(stat -c %s native-source.candidate.json)" = 50078
mv semantic_unit_router.candidate.rs source/crates/borsuk/src/semantic_unit_router.rs
mv two_bit_generation.candidate.rs source/crates/borsuk/src/two_bit_generation.rs
mv source-files.candidate.sha256 source-files.sha256
mv native-source.candidate.json native-source.json
(cd source; sha256sum -c ../source-files.sha256) > "$ev/green-source-after.log"
cp source-files.sha256 native-source.json "$ev/"
printf '%s\n' 6e78d393b4d7dadf19befeeabe3dc20a77319a26 > "$ev/green-source.commit"
jq -n '{schema:"borsuk-scale-closure-source-admission-v1",source_commit:"6e78d393b4d7dadf19befeeabe3dc20a77319a26",source_authenticated:true,native_qualified:false}' > "$ev/terminal.json"
(cd "$ev"; find . -type f ! -name SHA256SUMS -print0 | sort -z | xargs -0 sha256sum) > "$ev/SHA256SUMS"
publication_deadline=$(( $(date +%s) + 120 ))
for file in "$ev"/*; do
remaining=$(( publication_deadline - $(date +%s) - 2 )); (( remaining > 0 )) || exit 124
(( remaining <= 30 )) || remaining=30
timeout --kill-after=2 "$remaining" aws s3api put-object --bucket borsuk-bench-453182569524-euc1 --key research/semantic-router/20261011/scale-closure-native-qualification-a0001/stages/green-source-admission/"$(basename "$file")" --body "$file" --if-none-match '*' > /dev/null
done
cat "$ev/terminal.json"
