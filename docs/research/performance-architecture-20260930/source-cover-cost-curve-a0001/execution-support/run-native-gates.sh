#!/usr/bin/env bash
set -euo pipefail
root=/mnt/borsuk-paired-qualification
prefix=research/semantic-router/20261013/source-cover-cost-curve-a0001
export AWS_MAX_ATTEMPTS=1 AWS_DEFAULT_REGION=eu-central-1
mkdir "$root/stage-assets" "$root/evidence/batch"
batch=$root/evidence/batch
fetch() {
 local name=$1 sha=$2
 timeout -k 2 90 aws s3api get-object --bucket borsuk-bench-453182569524-euc1 --key "$prefix/inputs/$name" "$root/stage-assets/$name" > /dev/null
 printf '%s  %s\n' "$sha" "$root/stage-assets/$name" | sha256sum -c -
}
fetch required-tests.json 4f5b306ae40523a83d521333ccb84a37ac2de6447bf4edd4c7c6dbbac8d2b7aa
cp "$root/stage-assets/required-tests.json" "$batch/required-tests.json"
: > "$batch/stages.tsv"
completed=0
batch_status=0
failed_stage=
while read -r stage sha; do
 fetch "$stage.sh" "$sha"
 set +e
 bash "$root/stage-assets/$stage.sh" > "$batch/$stage.driver.log" 2>&1
 status=$?
 set -e
 printf '%s\t%s\n' "$stage" "$status" >> "$batch/stages.tsv"
 if (( status != 0 )); then batch_status=$status; failed_stage=$stage; break; fi
 if jq -e --arg stage "$stage" 'has($stage)' "$batch/required-tests.json" > /dev/null; then
  jq -r --arg stage "$stage" '.[$stage][]' "$batch/required-tests.json" | LC_ALL=C sort > "$batch/$stage.expected"
  sed -n 's/: test$//p' "$root/evidence/$stage/native-list.log" | LC_ALL=C sort > "$batch/$stage.listed"
  awk '$1=="test" && $3=="..." && $4=="ok" {print $2}' "$root/evidence/$stage/native.log" | LC_ALL=C sort > "$batch/$stage.actual"
  count=$(wc -l < "$batch/$stage.expected")
  if ! cmp "$batch/$stage.expected" "$batch/$stage.listed" > "$batch/$stage.list-check.log" 2>&1 \
   || ! cmp "$batch/$stage.expected" "$batch/$stage.actual" > "$batch/$stage.roster-check.log" 2>&1 \
   || [[ $(grep -Ec "^test result: ok[.] $count passed; 0 failed; 0 ignored; 0 measured;" "$root/evidence/$stage/native.log") != 1 ]]; then
   batch_status=42; failed_stage=$stage; break
  fi
 fi
 completed=$((completed+1))
done <<'STAGES'
compile-example 781f6c1a45aba7b1cc67d2b75ba21df04b33686b588049bdb4d16294aa7ef588
curve-closure 081753ca37c334a273fd023732f6943f8f4e876b9716fc648ffe29b5f1fa03fa
curve-byte-cap 87080f2f1aa54050195a8e3f9640427df7ba0274f82e9ff8473945b325fb06cb
curve-status 4b5d0ea81f71b302105a3a57701c3778af864778def73ee1ffbc06590df4721a
curve-full 44d2a9ff12adca12ac27a1dc275da26fee994cf3f028faeaa5e6d4fb3f7bd8d8
release-elf e724965d8c81c68d2585e6c91620bd435ee951ba31974d7679823c1665c40666
workspace-clippy 3259f9eb50cc2d6240463564488ee1c4e6064817fb659c04251b92b6e8feed44
workspace-test-build d52189a0ec1679241754cfe022d26cd58ea5f98a5ab14c78f0c5c830c5d6e4d1
curve-real 71bc46250efb79507c4f779c978f2b76a7134fb8dc0d19f1bd4f246e6fcc8386
STAGES
if (( batch_status == 0 && completed != 9 )); then batch_status=43; failed_stage=roster_incomplete; fi
jq -n --arg schema borsuk-fetch-scheduler-native-batch-v1 --arg source 13eb0bdc84c55db3f3e9433f56a1ee7d6bed7d8c --arg failed "$failed_stage" --argjson exit "$batch_status" --argjson completed "$completed" \
 '{schema:$schema,source_commit:$source,batch_exit:$exit,completed_stages:$completed,required_stages:9,gate_groups:9,failed_stage:$failed,native_qualified:false,performance_claim:false}' > "$batch/terminal.json"
(cd "$batch" && find . -type f ! -name SHA256SUMS -print0 | sort -z | xargs -0 sha256sum) > "$batch/SHA256SUMS"
publication_deadline=$(( $(date +%s)+120 ))
for file in "$batch"/*; do
 left=$((publication_deadline-$(date +%s)-2)); (( left > 0 )) || exit 124
 (( left <= 90 )) || left=90
 timeout -k 2 "$left" aws s3api put-object --bucket borsuk-bench-453182569524-euc1 --key "$prefix/batch/$(basename "$file")" --body "$file" --if-none-match '*' > /dev/null
done
cat "$batch/terminal.json"
exit "$batch_status"
