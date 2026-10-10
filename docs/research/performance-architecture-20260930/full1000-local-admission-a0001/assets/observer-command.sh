#!/usr/bin/env bash
# Enclosing command inside the retained system observer; not a launch wrapper.
set -u -o noclobber
[[ $# == 5 && $EUID == 0 ]] || exit 97
recipe=$1 config=$2 sha=$3 evidence=$4 actual_exit=$5
[[ $recipe == /* && $config == /* && $evidence == /* && $actual_exit == /* &&
   $sha =~ ^[0-9a-f]{64}$ && ! -e $actual_exit && ! -L $actual_exit ]] || exit 97
# Deliberately collect the real command return, including a signal-derived shell status.
bash "$recipe" "$config" "$sha" "$evidence"
rc=$?
printf '%s\n' "$rc" > "$actual_exit" || exit 97
sync -f "$actual_exit" || exit 97
sync -f "${actual_exit%/*}" || exit 97
exit "$rc"
