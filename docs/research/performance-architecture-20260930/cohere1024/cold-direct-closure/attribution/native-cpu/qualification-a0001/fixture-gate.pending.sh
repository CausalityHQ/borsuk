#!/usr/bin/env bash
set -euo pipefail
# Root-owned experiment glue; run only inside the freshly admitted payload cgroup.
# The external controller validates placement/limits and snapshots counters before release.
[[ $# == 6 ]] || exit 125
fifo=$1
helper=$2
config=$3
config_sha=$4
report=$5
mode=$6
[[ "$fifo" == /* && -p "$fifo" && "$helper" == /* && "$config" == /* && "$report" == /* ]] || exit 125
[[ "$config_sha" =~ ^[0-9a-f]{64}$ ]] || exit 125
case "$mode" in
  ordinary) ;;
  context) export BORSUK_NATIVE_CPU_DISPOSABLE_CGROUP=1 ;;
  publication-failure) trap '' XFSZ ;;
  *) exit 125 ;;
esac
IFS= read -r release < "$fifo"
[[ "$release" == release ]] || exit 125
exec "$helper" "$config" "$config_sha" "$report"
