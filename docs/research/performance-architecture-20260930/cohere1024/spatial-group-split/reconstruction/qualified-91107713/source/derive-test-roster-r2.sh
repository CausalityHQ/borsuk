#!/usr/bin/env bash
set -euo pipefail
out=/data/target/borsuk-reconstruction-native-gates-a0001.pending/roster-r2
source_dir=/data/target/borsuk-reconstruction-native-gates-a0001.pending/source-package-91107713/frozen-source
test ! -e "$out/required-tests.json"
scan() {
 local prefix=$1 file=$2
 awk -v prefix="$prefix" '
 /^mod [a-z_]+ \{/ { module=$2 }
 /^[[:space:]]*#\[(tokio::)?test/ { pending=1; next }
 pending && /^[[:space:]]*(async )?fn / {
   name=$2; if($1=="async")name=$3; sub(/\(.*/, "", name);
   print prefix module "::" name; pending=0; count++
 }
 END { if (pending || count==0) exit 1 }
 ' "$file"
}
scan '' "$source_dir/crates/borsuk/examples/compare_native_replay.rs" > "$out/example-tests.unsorted"
scan 'partition_reconstruction::' "$source_dir/crates/borsuk/examples/compare_native_replay/partition_reconstruction.rs" > "$out/reconstruction-tests.txt"
scan 'source_cover::' "$source_dir/crates/borsuk/src/budgeted_page_rank.rs" >> "$out/example-tests.unsorted"
scan 'scale_native_runner::' "$source_dir/crates/borsuk/src/bin/check_cohere_native_baseline.rs" >> "$out/example-tests.unsorted"
# build_sq8_source has exactly one nested test module, derive::tests.
awk '/^[[:space:]]*#\[(tokio::)?test/ {pending=1;next} pending && /^[[:space:]]*(async )?fn / {name=$2; if($1=="async")name=$3;sub(/\(.*/,"",name);print "scale_native_runner::scale_derivation_producer::derive::tests::" name;pending=0;count++} END{if(pending || count!=8)exit 1}' "$source_dir/crates/borsuk/examples/build_sq8_source.rs" >> "$out/example-tests.unsorted"
cat "$out/reconstruction-tests.txt" >> "$out/example-tests.unsorted"
LC_ALL=C sort "$out/example-tests.unsorted" > "$out/example-tests.txt"
test "$(wc -l < "$out/reconstruction-tests.txt")" = 10
test -z "$(uniq -d "$out/example-tests.txt")"
sed 's/^partition_reconstruction::tests:://' "$out/reconstruction-tests.txt" > "$out/reconstruction-names.txt"
cmp "$out/mandatory-reconstruction-tests.txt" "$out/reconstruction-names.txt"
jq -Rn '[inputs]' < "$out/reconstruction-tests.txt" > "$out/reconstruction-roster.json"
jq -Rn '[inputs]' < "$out/example-tests.txt" > "$out/example-roster.json"
jq -n --slurpfile reconstruction "$out/reconstruction-roster.json" --slurpfile example "$out/example-roster.json" '{"reconstruction-debug":$reconstruction[0],"reconstruction-release-tests":$reconstruction[0],"reconstruction-example-tests":$example[0]}' > "$out/required-tests.json"
jq 'map_values(length)' "$out/required-tests.json"
