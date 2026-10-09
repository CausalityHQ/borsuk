#!/usr/bin/env bash
set -euo pipefail
# ExecStopPost must be admitted in .control, outside the measured payload subtree.
# The external deadline owns cleanup; this hold never kills or retries a native invocation.
[[ $# == 1 && "$1" == /* && -p "$1" ]] || exit 125
IFS= read -r release < "$1"
[[ "$release" == release ]] || exit 125
