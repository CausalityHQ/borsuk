#!/usr/bin/env bash
# Run only on the disposable EC2 machine. No BORSUK/native data invoked.
set -euo pipefail
root=/mnt/borsuk-source-width64
mkdir -m 700 "$root/perf-admission"
ev=$root/perf-admission
shopt -s nullglob
bins=(/usr/lib/linux-tools/*/perf)
(( ${#bins[@]} > 0 )) || exit 125
perf_bin=${bins[-1]}
[[ -f $perf_bin && ! -L $perf_bin && -x $perf_bin ]]
[[ $(od -An -tx1 -N4 "$perf_bin" | tr -d ' \n') == 7f454c46 ]]
dpkg-query -S "$perf_bin" > "$ev/package-owner.txt"
owner=$(sed 's/: .*//' "$ev/package-owner.txt")
[[ $owner == linux-tools-* && $owner != *$'\n'* ]]
dpkg-query -W -f='${Package}\t${Version}\t${Architecture}\n' "$owner" > "$ev/package-version.txt"
timeout -k 2 30 dpkg --verify "$owner" > "$ev/package-verify.stdout" 2> "$ev/package-verify.stderr"
[[ ! -s $ev/package-verify.stdout && ! -s $ev/package-verify.stderr ]]
sha=$(sha256sum "$perf_bin"); sha=${sha%% *}
printf '%s\n' "$perf_bin" > "$root/perf.path"
printf '%s\n' "$sha" > "$root/perf.sha256"
"$perf_bin" --version > "$ev/version.txt"
uname -a > "$ev/kernel.txt"
cat /proc/sys/kernel/perf_event_paranoid > "$ev/paranoid.txt"
timeout -k 2 15 "$perf_bin" record --no-buildid -e cpu-clock -F 99 --call-graph dwarf,4096 -o "$ev/canary.data" -- /bin/bash -c 'for ((i=0;i<400;i++)); do /usr/bin/sha256sum /bin/bash > /dev/null; done' > "$ev/canary.stdout" 2> "$ev/canary.stderr"
timeout -k 2 15 "$perf_bin" report --stdio --no-children -i "$ev/canary.data" > "$ev/canary-report.txt" 2> "$ev/canary-report.stderr"
[[ -s $ev/canary.data && -s $ev/canary-report.txt ]]
grep -Eq '[[:space:]][1-9][0-9]*[[:space:]]+samples|[[:space:]][1-9][0-9]*[[:space:]]+of event' "$ev/canary-report.txt"
sha256sum "$ev/canary.data" > "$ev/canary.data.sha256"
printf '%s\n' SOFTWARE_SAMPLING_CANARY_CLOSED > "$ev/status"
