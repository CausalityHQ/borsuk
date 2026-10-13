#!/usr/bin/env bash
# EC2-only opaque transport; requests/truth/result rows remain Rust-owned.
set -euo pipefail
[[ $# == 3 && $1 == /mnt/borsuk-fetch-scheduler ]] || exit 125
root=$1; manifest=$2; pin=$3
[[ $manifest == "$root/"* && $pin =~ ^[0-9a-f]{64}$ && -f $manifest && ! -L $manifest ]] || exit 125
printf '%s  %s\n' "$pin" "$manifest" | sha256sum -c -
[[ ! -e $root/input-files.sha256 ]] || exit 125
mkdir -m 700 "$root/inputs" "$root/bin" "$root/evidence" "$root/results" "$root/scratch"
mkdir -m 700 "$root/scratch/original" "$root/scratch/candidate"
export AWS_MAX_ATTEMPTS=1 AWS_DEFAULT_REGION=eu-central-1 AWS_PAGER=''
deadline=$(( $(date +%s)+570 ))
: > "$root/input-files.sha256.tmp"
while IFS=$'\t' read -r role bytes sha key target; do
 [[ $role != role ]] || continue
 [[ $bytes =~ ^[0-9]+$ && $bytes -gt 0 && $bytes -le 67108864 && $sha =~ ^[0-9a-f]{64}$ && $key == research/semantic-router/* ]] || exit 125
 [[ $target != /* && $target != *..* && $target != *$'\n'* ]] || exit 125
 case "$target" in inputs/*|bin/check_cohere_native_baseline-original|bin/check_cohere_native_baseline-candidate|bin/compare_native_replay|*.sh) ;; *) exit 125;; esac
 [[ ! -e $root/$target && ! -L $root/$target ]] || exit 125
 left=$((deadline-$(date +%s)-2)); ((left>0)); ((left<=60)) || left=60
 /usr/bin/timeout -k 2 "$left" aws s3api get-object --bucket borsuk-bench-453182569524-euc1 --key "$key" "$root/$target.partial" > "$root/evidence/$role.get.json"
 [[ $(stat -c %s "$root/$target.partial") == "$bytes" ]] || exit 125
 printf '%s  %s\n' "$sha" "$root/$target.partial" | sha256sum -c - > "$root/evidence/$role.sha-check.log"
 mv "$root/$target.partial" "$root/$target"
 chmod 0400 "$root/$target"
 if [[ $target == bin/* ]]; then [[ $(od -An -tx1 -N4 "$root/$target"|tr -d ' \n') == 7f454c46 ]]; chmod 0500 "$root/$target"; fi
 printf '%s  %s\n' "$sha" "$root/$target" >> "$root/input-files.sha256.tmp"
done < "$manifest"
# Extract only exact metadata bodies into owned regular files, never tar paths.
for role in cohort derivation; do
 if [[ $role == cohort ]]; then member=./prepared-parent_cohort_complete.json; name=prepared-parent_cohort_complete.json; bytes=6238; sha=e0503d41493157a6a0a48627515e62ec452386d0496827028bb9571d406e3a17; else member=./prepared-parent_derived_derivation.json; name=prepared-parent_derived_derivation.json; bytes=23128; sha=00e59ee26745c092f1097ba8892b4f205f7c96b48a757311aedc9072fcc3c96e; fi
 [[ ! -e $root/inputs/$name && ! -L $root/inputs/$name ]] || exit 125
 /usr/bin/timeout -k 2 15 tar -xOzf "$root/inputs/cohort-evidence.tar.gz" "$member" > "$root/inputs/$name"
 [[ $(stat -c %s "$root/inputs/$name") == "$bytes" ]] || exit 125
 printf '%s  %s\n' "$sha" "$root/inputs/$name" | sha256sum -c - > "$root/evidence/$role.sha-check.log"
 chmod 0400 "$root/inputs/$name"
 printf '%s  %s\n' "$sha" "$root/inputs/$name" >> "$root/input-files.sha256.tmp"
done
mv "$root/input-files.sha256.tmp" "$root/input-files.sha256"
sha256sum -c "$root/input-files.sha256" > "$root/evidence/transport-final-check.log"
printf '%s\n' INPUTS_AUTHENTICATED_NATIVE_ADMISSION_UNRUN > "$root/evidence/transport.status"
