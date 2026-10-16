#!/usr/bin/env bash
# Remote EC2 only. Downloads/extracts authenticated inputs; does not run ANN queries.
set -euo pipefail
[[ $# == 3 ]] || exit 125
root=$1 manifest=$2 manifest_sha=$3
[[ $root == /mnt/borsuk-sq8-split-panel && $manifest_sha =~ ^[0-9a-f]{64}$ ]]
[[ $manifest == /* && -f $manifest && ! -L $manifest ]]
printf '%s  %s\n' "$manifest_sha" "$manifest" | sha256sum --strict --check -
jq -e '
  .schema == "borsuk-split-panel-remote-assets-draft-v1" and
  .root == "/mnt/borsuk-sq8-split-panel" and
  .bucket == "borsuk-bench-453182569524-euc1" and
  .native_source_commit == "8e7076951936b0434b7947afd5b23e192c98fe70" and
  (.assets|length)==6 and (.members|length)==3 and
  ([.assets[].target]|unique|length)==6 and
  ([.members[].target]|unique|length)==3 and
  all(.assets[]; (.sha256|test("^[0-9a-f]{64}$")) and
    (.bytes|type=="number" and floor==. and .>0 and .<=20000000) and
    (.target|test("^(inputs|archives|bin)/[A-Za-z0-9_.-]+$")) and
    (.s3_key|test("^research/semantic-router/[A-Za-z0-9_./-]+$"))) and
  all(.members[]; (.sha256|test("^[0-9a-f]{64}$")) and
    (.bytes|type=="number" and floor==. and .>0 and .<=2000000) and
    (.target|test("^(inputs|history)/[A-Za-z0-9_.-]+$")) and
    (.archive|test("^archives/[A-Za-z0-9_.-]+$")) and
    (.member|test("^(\\./)?[A-Za-z0-9_./-]+$")))
' "$manifest" >/dev/null
[[ ! -e $root && ! -L $root ]]
mkdir "$root"
mkdir "$root/inputs" "$root/archives" "$root/bin" "$root/history" "$root/setup" "$root/scratch"
cd "$root"
export AWS_MAX_ATTEMPTS=1 AWS_DEFAULT_REGION=eu-central-1 AWS_PAGER=''
[[ $(aws --version) == aws-cli/2.36.11\ * ]]
rows=$(jq -r '.assets[]|[.role,.target,.s3_key,(.bytes|tostring),.sha256]|@tsv' "$manifest")
while IFS=$'\t' read -r role target key bytes sha; do
  [[ $role =~ ^[A-Za-z0-9_]+$ && ! -e $target && ! -L $target ]]
  timeout -k 2 90 aws s3api get-object --bucket borsuk-bench-453182569524-euc1 \
    --key "$key" "$target" > "setup/$role.get.json"
  [[ -f $target && ! -L $target && $(stat -c %s "$target") == "$bytes" ]]
  printf '%s  %s\n' "$sha" "$target" | sha256sum --strict --check -
  chmod 0400 "$target"
done <<< "$rows"
rows=$(jq -r '.members[]|[.archive,.member,.target,(.bytes|tostring),.sha256]|@tsv' "$manifest")
while IFS=$'\t' read -r archive member target bytes sha; do
  [[ -f $archive && ! -L $archive && ! -e $target && ! -L $target ]]
  # The whole archive is authenticated above. Still refuse duplicates and links.
  list=$(timeout -k 2 20 tar -tzf "$archive" -- "$member")
  [[ $list == "$member" ]]
  detail=$(timeout -k 2 20 tar --numeric-owner --full-time -tvzf "$archive" -- "$member")
  read -r mode _owner actual_bytes _day _clock actual_name <<< "$detail"
  [[ $mode == -* && $actual_bytes == "$bytes" && $actual_name == "$member" ]]
  (set -o noclobber; timeout -k 2 20 tar -xOzf "$archive" -- "$member" > "$target")
  [[ -f $target && ! -L $target && $(stat -c %s "$target") == "$bytes" ]]
  printf '%s  %s\n' "$sha" "$target" | sha256sum --strict --check -
  chmod 0400 "$target"
done <<< "$rows"
for executable in check_cohere_native_baseline compare_native_replay; do
  readelf -h "bin/$executable" > "setup/$executable.elf.txt"
  grep -Eq 'Class: +ELF64' "setup/$executable.elf.txt"
  grep -Eq 'Machine: +Advanced Micro Devices X86-64' "setup/$executable.elf.txt"
  chmod 0500 "bin/$executable"
done
printf '%s\n' ASSETS_AUTHENTICATED_NO_NATIVE_EXECUTION
