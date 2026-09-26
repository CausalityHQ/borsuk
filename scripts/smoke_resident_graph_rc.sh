#!/usr/bin/env bash
set -euo pipefail
root=$(mktemp -d)
trap 'rm -rf "$root"' EXIT
cargo run --locked --quiet -p borsuk --example resident_graph_rc -- create "$root"
first=$(cargo run --locked --quiet -p borsuk --example resident_graph_rc -- search "$root")
second=$(cargo run --locked --quiet -p borsuk --example resident_graph_rc -- search "$root")
python3 -c 'import json,sys; a,b=map(json.loads,sys.argv[1:]); assert len(a["ids"]) == 10 and 1042 in a["ids"] and a["ids"] == b["ids"] and a["hydrate_gets"] == 5 and b["hydrate_gets"] == 0' "$first" "$second"
