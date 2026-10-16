#!/usr/bin/env bash
# Closed archive metadata only. Never extract result.jsonl or fixture query payloads.
set -euo pipefail
out=/data/target/borsuk-sq8-panel-attribution.pending/launch-split-prerequisites-a0002
archive=$out/evidence.tar.gz
destination=$out/collected
[[ $(cat "$out/collection.closed") == OPAQUE_ARCHIVE_COLLECTED_NOT_VERIFIED ]]
[[ ! -e $destination && ! -L $destination ]]
mkdir "$destination"
tar -tzf "$archive" > "$out/archive-members.txt"
extract() {
 local relative=$1 member detail mode _owner bytes _day _clock name count
 member=split-prerequisites.pending/$relative
 count=$(grep -Fxc "$member" "$out/archive-members.txt")
 [[ $count == 1 ]]
 detail=$(tar --numeric-owner --full-time -tvzf "$archive" -- "$member")
 read -r mode _owner bytes _day _clock name <<< "$detail"
 [[ $mode == -* && $name == "$member" && $bytes =~ ^[0-9]+$ ]]
 (( bytes <= 2097152 ))
 mkdir -p "$(dirname "$destination/$relative")"
 (set -o noclobber; tar -xOzf "$archive" -- "$member" > "$destination/$relative")
 [[ $(stat -c %s "$destination/$relative") == "$bytes" ]]
}
for file in terminal.json input-manifest.json machine-deadline.epoch driver-source.txt driver-functions.sh; do extract "$file"; done
for stage in C1/C1 C1/reducer NULL_SMOKE/C0 NULL_SMOKE/reducer MOCKED/reducer PARITY/reducer; do
 for file in execution.json owner.txt invocation-id.txt unit-start-status.txt unit-stop-status.txt systemd-after.txt cgroup.events.drained service-result.txt \
  native-exit.txt timeout-exit.txt time-exit.txt systemd-run-exit.txt manager-envelope-exit.txt worker-exit.txt binary-after-exit.txt \
  cpu.max.before cpu.max.after cpuset.cpus.effective.before cpuset.cpus.effective.after memory.max.before memory.max.after \
  memory.swap.max.before memory.swap.max.after pids.max.before pids.max.after memory.events.after memory.swap.peak.after memory.peak.after pids.peak.after; do
  extract "$stage/$file"
 done
done
for phase in C1 NULL_SMOKE; do extract "$phase/native-report.json"; extract "$phase/validated.txt"; done
extract MOCKED/reducer/stdout.log
for file in .receipt.pending.json historical.native.json null.native.json native-exits.json before.identities after.identities; do
 extract "PARITY/results/$file"
done
# The checker publishes parity.json as an atomic hard link. Resolve ONLY this
# authenticated alias to its explicitly selected regular receipt, never an arbitrary link.
alias_member=split-prerequisites.pending/PARITY/results/parity.json
alias_target=split-prerequisites.pending/PARITY/results/.receipt.pending.json
[[ $(grep -Fxc "$alias_member" "$out/archive-members.txt") == 1 ]]
tar --numeric-owner --full-time -tvzf "$archive" -- "$alias_member" > "$destination/PARITY/receipt-alias.txt"
read -r alias_mode _owner alias_bytes _day _clock alias_name < "$destination/PARITY/receipt-alias.txt"
[[ $alias_mode == h* && $alias_bytes == 0 && $alias_name == "$alias_member link to $alias_target" ]]
cp "$destination/PARITY/results/.receipt.pending.json" "$destination/PARITY/results/parity.json"
cmp "$destination/PARITY/results/.receipt.pending.json" "$destination/PARITY/results/parity.json"
extract PARITY/validated.txt
printf '%s\n' CLOSED_METADATA_ONLY_EXTRACTED > "$out/extraction.closed"
