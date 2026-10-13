#!/usr/bin/env bash
# Run only on the disposable EC2 machine. No BORSUK/native data invoked.
set -Eeuo pipefail
root=/mnt/borsuk-source-width64
mkdir -m 700 "$root/perf-admission"
ev=$root/perf-admission
failure() {
 local rc=$1 line=$2 file
 trap - ERR
 set +e
 printf 'PERF_ADMISSION_FAILED exit=%s line=%s\n' "$rc" "$line" >&2
 for file in package-owner.txt package-version.txt package-verify.stdout package-verify.stderr version.txt kernel.txt paranoid.txt canary.stderr canary-report.txt canary-report.stderr; do
  if [[ -f $ev/$file ]]; then
   printf '\nPERF_EVIDENCE %s\n' "$file" >&2
   head -c 65536 "$ev/$file" >&2
  fi
 done
 printf '%s\n' "$rc" > "$ev/failed.exit"
 # Preserve probe evidence before the setup terminal watcher terminates EC2.
 if timeout -k 2 10 tar -czf "$root/perf-admission-failed.tar.gz" -C "$root" perf-admission; then
  timeout -k 2 20 aws s3api put-object --bucket borsuk-bench-453182569524-euc1 --key research/semantic-router/20261013/source-width64-cpu-profile-a0004/setup/perf-admission-failed.tar.gz --body "$root/perf-admission-failed.tar.gz" --if-none-match '*' > "$ev/failure-put.json" 2> "$ev/failure-put.stderr"
 fi
 exit "$rc"
}
trap 'failure "$?" "$LINENO"' ERR
perf_package=linux-tools-6.8.0-147
package_files=$(dpkg-query -L "$perf_package")
bins=()
while IFS= read -r file; do
 if [[ $file == /usr/lib/linux-tools-*/perf ]]; then bins+=("$file"); fi
done <<< "$package_files"
(( ${#bins[@]} == 1 )) || exit 125
perf_bin=${bins[0]}
[[ -f $perf_bin && ! -L $perf_bin && -x $perf_bin ]]
[[ $(od -An -tx1 -N4 "$perf_bin" | tr -d ' \n') == 7f454c46 ]]
dpkg-query -S "$perf_bin" > "$ev/package-owner.txt"
owner=$(sed 's/: .*//' "$ev/package-owner.txt")
[[ $owner == "$perf_package" ]]
[[ $(dpkg-query -W -f='${Version}' "$owner") == 6.8.0-147.147 ]]
dpkg-query -W -f='${Package}\t${Version}\t${Architecture}\n' "$owner" > "$ev/package-version.txt"
timeout -k 2 30 dpkg --verify "$owner" > "$ev/package-verify.stdout" 2> "$ev/package-verify.stderr"
[[ ! -s $ev/package-verify.stdout && ! -s $ev/package-verify.stderr ]]
sha=$(sha256sum "$perf_bin"); sha=${sha%% *}
printf '%s\n' "$perf_bin" > "$root/perf.path"
printf '%s\n' "$sha" > "$root/perf.sha256"
"$perf_bin" --version > "$ev/version.txt"
uname -a > "$ev/kernel.txt"
cat /proc/sys/kernel/perf_event_paranoid > "$ev/paranoid.txt"
# The child Bash owns sum/i; parent expansion would invalidate this probe.
# shellcheck disable=SC2016
timeout -k 2 15 "$perf_bin" record --no-buildid -e cpu-clock -F 99 --call-graph dwarf,4096 -o "$ev/canary.data" -- /bin/bash -c 'sum=0; for ((i=1;i<=1000000;i++)); do sum=$((sum+i)); done; printf "%s\n" "$sum"' > "$ev/canary.stdout" 2> "$ev/canary.stderr"
[[ $(cat "$ev/canary.stdout") == 500000500000 ]]
timeout -k 2 15 "$perf_bin" report --stdio --no-children -i "$ev/canary.data" > "$ev/canary-report.txt" 2> "$ev/canary-report.stderr"
[[ -s $ev/canary.data && -s $ev/canary-report.txt ]]
grep -Eq '[[:space:]][1-9][0-9]*[[:space:]]+samples|[[:space:]][1-9][0-9]*[[:space:]]+of event' "$ev/canary-report.txt"
sha256sum "$ev/canary.data" > "$ev/canary.data.sha256"
printf '%s\n' SOFTWARE_SAMPLING_CANARY_CLOSED > "$ev/status"
