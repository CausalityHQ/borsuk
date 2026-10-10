#!/usr/bin/env bash
# Controlled remote cleanup falsifier ONLY: no ANN, no network, no performance claim.
# shellcheck disable=SC2016 # The child Bash expands these expressions.
set -u -o noclobber
[[ $# == 1 && $1 == /mnt/borsuk-pool-pid/staging-*/native/descendant.identity && ! -e $1 && ! -L $1 ]] || exit 98
command -v /usr/bin/setsid >/dev/null || exit 98
trap '' TERM
# Child escapes the timeout process group, stays in the systemd payload cgroup,
# ignores TERM and exits naturally within60s if cleanup fails. Close logger FDs
# 3..63 (the recipe opens three Bash dynamic descriptors starting at10).
/usr/bin/setsid /bin/bash -c '
set -u -o noclobber
trap "" TERM
printf "pid=%s\n" "$$" > "$1" || exit 98
cat /proc/self/cgroup >> "$1" || exit 98
cat /proc/$$/stat > "$1.procstat" || exit 98
for ((fd=3;fd<64;fd++)); do eval "exec $fd>&-"; done
exec /bin/sleep 60
' _ "$1" </dev/null >/dev/null 2>&1 &
child=$!
wait "$child"
exit "$?"
