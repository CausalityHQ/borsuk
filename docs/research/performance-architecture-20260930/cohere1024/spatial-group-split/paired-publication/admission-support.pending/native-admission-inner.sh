#!/usr/bin/env bash
set -euo pipefail
phase=${1:?canary, prefix, or corrupt}
case $phase in canary|prefix|corrupt) ;; *) exit 125;; esac
support=/opt/borsuk-paired-admission
root=/mnt/borsuk-paired-source-screen
ev=$root/evidence/$phase
cd "$support"
sha256sum --check support.sha256 > "$ev/package-check.log"
# shellcheck source=/dev/null
source "$support/admission.env"
: "${CONFIG_SHA256:?}" "${AUTHORITY_SHA256:?}" "${STRACE_REALPATH:?}" "${STRACE_SHA256:?}"
[[ $STRACE_REALPATH == /usr/bin/strace && $STRACE_SHA256 =~ ^[0-9a-f]{64}$ ]]
test "$(realpath "$(command -v strace)")" = "$STRACE_REALPATH"
printf '%s  %s\n' "$STRACE_SHA256" "$STRACE_REALPATH" "$CONFIG_SHA256" "$root/paired.config.json" "$AUTHORITY_SHA256" "$root/paired.authority.json" fd6d36f6bfbb4e370a94707941cec396af59492f832f6d1090c24eacbc1ec748 "$root/bin/compare_native_replay" | sha256sum -c - > "$ev/runtime-pins.log"
cp "$support/tracer.realpath" "$support/tracer.sha256" "$support/tracer.version" "$support/tracer.package" "$support/tracer.owner" "$support/aws.version" "$ev/"
export BORSUK_CPU_THREADS=1 RAYON_NUM_THREADS=1 RUST_TEST_THREADS=1
unset BORSUK_SOURCE_NEIGHBORHOOD_CRASH_DIR
cg=$(awk -F: '$1==0 {print $3}' /proc/self/cgroup)
[[ $cg == /system.slice/borsuk-paired-admission-$phase.service ]]
printf '%s\n' "$cg" > "$ev/cgroup-path.txt"
for f in cpu.max cpuset.cpus.effective memory.max memory.swap.max pids.max; do cat "/sys/fs/cgroup$cg/$f" > "$ev/$f.before"; done
test "$(cat "$ev/cpu.max.before")" = '100000 100000'
test "$(cat "$ev/cpuset.cpus.effective.before")" = 0
test "$(cat "$ev/memory.max.before")" = 4294967296
test "$(cat "$ev/memory.swap.max.before")" = 0
test "$(cat "$ev/pids.max.before")" = 256
test "$(ulimit -c)" = 0
while IFS=$'\t' read -r _key path bytes sha; do
 test "$(stat -c %s "$path")" = "$bytes"
 printf '%s  %s\n' "$sha" "$path" | sha256sum -c -
done < "$support/transport.tsv" > "$ev/inputs-full-authentication.log"
request=$(jq -r '.requests.path' "$root/paired.config.json")
truth=$(jq -r '.truth.path' "$root/paired.config.json")
sq8=$(jq -r '.sq8.path' "$root/paired.config.json")
membership=$(jq -r '.spatial.membership.path' "$root/paired.config.json")
membership_sha=$(jq -r '.spatial.membership.sha256' "$root/paired.config.json")
for path in "$request" "$truth" "$sq8"; do test ! -e "$path" && test ! -L "$path"; done
test ! -e "$ev/selection.json" && test ! -L "$ev/selection.json"
test ! -e "$ev/report.json" && test ! -L "$ev/report.json"
restore() {
 if [[ $phase == corrupt ]] && test -f "$membership.approved-original"; then
  printf '%s  %s\n' "$membership_sha" "$membership.approved-original" | sha256sum -c - > "$ev/backup-authentication.log"
  mv -f "$membership.approved-original" "$membership"
  printf '%s  %s\n' "$membership_sha" "$membership" | sha256sum -c - > "$ev/restoration.log"
  sync -f "$membership"
  sync -f "${membership%/*}"
 fi
}
if [[ $phase == corrupt ]]; then
 printf '%s  %s\n' "$membership_sha" "$membership" | sha256sum -c - > "$ev/original-membership.log"
 test ! -e "$membership.approved-original" && test ! -L "$membership.approved-original"
 trap restore EXIT
 mv "$membership" "$membership.approved-original"
 cp --reflink=never "$membership.approved-original" "$membership"
 chmod 0600 "$membership"
 byte=$(od -An -tu1 -N1 "$membership")
 printf '%b' "\\$(printf '%03o' "$((byte ^ 1))")" | dd of="$membership" bs=1 count=1 conv=notrunc status=none
 chmod 0400 "$membership"
 test "$(stat -c %s "$membership")" = "$(stat -c %s "$membership.approved-original")"
 if cmp -l "$membership.approved-original" "$membership" > "$ev/corrupted-byte.txt"; then exit 1; else test "$?" = 1; fi
 test "$(wc -l < "$ev/corrupted-byte.txt")" = 1
 sha256sum "$membership" > "$ev/corrupted-membership.sha256"
 sync -f "$membership"
fi
config_sha=$CONFIG_SHA256
if [[ $phase == canary ]]; then config_sha=$(printf '%064d' 0); test "$config_sha" != "$CONFIG_SHA256"; fi
command=("$root/bin/compare_native_replay" --paired-partition-source-coverage "$root/paired.config.json" "$config_sha" "$root/paired.authority.json" "$AUTHORITY_SHA256" "$ev/selection.json" "$ev/report.json")
printf '%q ' "$STRACE_REALPATH" -f -yy -s 8192 -e trace=%file -o "$ev/native.trace" -- "${command[@]}" > "$ev/native-command.txt"
printf '\n' >> "$ev/native-command.txt"
date -u +%FT%TZ > "$ev/native-started.txt"
set +e
/usr/bin/time -v -o "$ev/native-time.txt" timeout -k 30 1140 "$STRACE_REALPATH" -f -yy -s 8192 -e trace=%file -o "$ev/native.trace" -- "${command[@]}" > "$ev/native.stdout" 2> "$ev/native.stderr"
status=$?
set -e
printf '%s\n' "$status" > "$ev/traced-command-exit.txt"
awk -v exe="$root/bin/compare_native_replay" 'index($0,"execve(\"" exe "\"") && /= 0$/ {pid=$1} $1==pid && /\+\+\+ exited with [0-9]+ \+\+\+/ {print $(NF-1)}' "$ev/native.trace" > "$ev/native-exit.txt"
date -u +%FT%TZ > "$ev/native-finished.txt"
restore
trap - EXIT
for f in memory.peak memory.events memory.swap.peak pids.peak cpu.stat; do if test -f "/sys/fs/cgroup$cg/$f"; then cat "/sys/fs/cgroup$cg/$f" > "$ev/$f.after"; fi; done
test "$status" = 2
test "$(cat "$ev/native-exit.txt")" = 2
test -s "$ev/native.trace"
if grep -Eq '<unfinished \.\.\.>|resumed>|strace:|\.\.\.' "$ev/native.trace"; then exit 1; fi
# -yy binds openat's resolved directory FD plus basename to the configured absolute role.
awk -v request="$request" -v truth="$truth" -v sq8="$sq8" -v phase="$phase" -v inputs="$root/inputs/" '
 function refers(line,path, parent,base,n,parts) {
  n=split(path,parts,"/"); base=parts[n]; parent=substr(path,1,length(path)-length(base)-1)
  return index(line,"\"" path "\"") || index(line,"<" path ">") || index(line,"<" parent ">, \"" base "\"")
 }
 {
  if (refers($0,truth) || refers($0,sq8)) bad=1
  if (refers($0,request)) {
   print
   count++
   if (phase!="prefix" || $0!~/open(at)?\(/ || $0!~/O_RDONLY/ || $0~/O_WRONLY|O_RDWR|O_CREAT|O_TRUNC/ || $0!~/= -1 ENOENT /) bad=1
  } else if (index($0,inputs) && $0~/= -1 ENOENT /) bad=1
 }
 END {if ((phase=="prefix" && count!=1) || (phase!="prefix" && count!=0)) bad=1; exit bad}
' "$ev/native.trace" > "$ev/request-witness.txt"
test ! -e "$ev/selection.json" && test ! -L "$ev/selection.json"
if [[ $phase == canary ]]; then
 grep -Fx 'INVALID: config SHA/length/identity; external gates required' "$ev/native.stderr" > "$ev/refusal-witness.txt"
 test ! -e "$ev/report.json" && test ! -L "$ev/report.json"
 gate=EXPECTED_CONFIG_SHA_REFUSAL
else
 jq -e '.schema=="borsuk-paired-source-coverage-v1" and .status=="INVALID" and .complete==false and .qualified==false and .resource_gate_failed==false and .cold_s3_claim==false and .candidate_returned_ids_computed==false' "$ev/report.json" > "$ev/native-invalid-check.log"
 if [[ $phase == prefix ]]; then
  jq -e '.error=="No such file or directory (os error 2)"' "$ev/report.json" >> "$ev/native-invalid-check.log"
  gate=VALIDATED_SOURCE_LAYOUT_PREFIX_ONLY
 else
  jq -e '.error=="paired binary SHA/EOF"' "$ev/report.json" >> "$ev/native-invalid-check.log"
  test -s "$ev/restoration.log"
  gate=EXPECTED_PAIRED_BINARY_SHA_REFUSAL
 fi
fi
for path in "$request" "$truth" "$sq8"; do test ! -e "$path" && test ! -L "$path"; done
test "$(cat "$ev/memory.swap.peak.after")" = 0
awk '$1 ~ /^oom/ && $2 != 0 {bad=1} END {exit bad}' "$ev/memory.events.after"
printf '%s\n' "$gate" > "$ev/wrapper-prefix-gate.txt"
