#!/usr/bin/env bash
# UNVERIFIED source-only glue. Root runs on EC2 after BOTH original results close.
# Environment API: REDUCER REDUCER_SHA HIST_COMPLETED_CFG HIST_CFG_SHA
# NULL_COMPLETED_CFG NULL_CFG_SHA NEW_OUTPUT_DIR. All paths absolute/canonical.
# Root qualifies the reducer and freezes headers independently of result content;
# current binary/source pins and old whole-result SHA are root authority, not discovery.
# Native --completed-scale is the fail-closed unique-key decoder for configs/JSONL.
# No historical physical-ordinal, scored-row, cold, quality or performance claim.

# One literal supplies the production guards/extractor and the source-bound fixture.
# shellcheck disable=SC2016
parity_jq=$(cat <<'JQ'
def roster: [0,32,64,96,128,161,193,225,257,290,322,354,386,418,451,483,
 515,547,580,612,644,676,708,741,773,805,837,870,902,934,966,999];
def historical_identity_pins: {
 schema:"borsuk-cohere-native-baseline-result-v9",
 binary_sha256:"912f23d34dd175f365aab2797896da2c45306bf9dd4faf7d44723c8bdce50995",
 config_sha256:"a0a7f642207a151af6e267b281f72a3e499d8b8540f5679a4dc947ab6d8955bd",
 codec_source_sha256:"eddf88c6c8ee23a732751f49291293f1cc081545ebb9a21c149eb757aa5c38ec",
 generation_source_sha256:"84518f01fa2b9625081c7a4c2dbd3eab1987f0cc0893ba77af9c32a997422f7f",
 returned_source_sha256:"a4dcc7f9bc06cdea835f72860f3d2e796fcb6f29276858c349d624884cc9f74b",
 router_source_sha256:"a8b6dbc74ea15b08d15499bcf83cab680c17fcc040840e422818b505a8758cd8",
 runner_source_sha256:"3d1e3f63d88d943615c7155460aeb3c9cc05c19e27d48d9c39d4fb60d6065304",
 source_plane_source_sha256:"dbcc4cdbc4bb5c244354b375afd657f8b5cb1892df3f0ff0b8f41ef580de42e0",
 sq8_range_source_sha256:"bf31d4e0b6e53c9e7da88ab8a5d6d27a748d2492d7a915e7df7eb131c41c45e2"};
def original_inputs: {
 phase:"bound_inputs",dataset:"CohereLabs/wikipedia-2023-11-embed-multilingual-v3",
 revision:"ade45fb52bd549f5e8c065636fe4160a43c2af36",metric:"cosine",
 tie_rule:"corpus_ordinal_ascending",profile:"scale1m",rows:1000000,dimensions:1024,
 count:1000,k:10,selected_count:32,corpus_source_first:0,query_source_first:100000,
 corpus_intervals:[{start:0,end:100000},{start:101000,end:1001000}],
 reserved_query_interval:{start:100000,end:101000},requests_bytes:4096000,truth_bytes:80000,
 requests_sha256:"8460a81ff2f979deff7d82bede874a1301f47dfd3e4589305c9f53e020920d5e",
 reserved_queries_sha256:"8460a81ff2f979deff7d82bede874a1301f47dfd3e4589305c9f53e020920d5e",
 truth_sha256:"0e38d943adaf62f197b98fe8b01b2edec2008010df760717ababfa5054693b7f",
 native_source_sha256:"31f28fa211f9237f0f7e41f569773cd207cefcdce22d4932ec96aee01ff2c40d",
 native_sq8_sha256:"cc4395af5fdbdeffcb596460aceaad0e55bdcc4af074f405e78ab2dd2be002e1",
 native_order_sha256:"1060c919e34e93cc9a3d88b93eda226a8635bba41bfc60e5ba47caafb711d413",
 generation_prefix:"semantic/v9-source-shortlist-actual-a0001/generations/460d095ac98f2dba70ff765907ba89d3bd7bbdb03e1cb4781da637ef5d19baf6",
 generation_root_sha256:"460d095ac98f2dba70ff765907ba89d3bd7bbdb03e1cb4781da637ef5d19baf6",
 cohort_receipt_sha256:"e0503d41493157a6a0a48627515e62ec452386d0496827028bb9571d406e3a17",
 derivation_receipt_sha256:"00e59ee26745c092f1097ba8892b4f205f7c96b48a757311aedc9072fcc3c96e",
 producer_authority:{source_commit:"bc3082a8210c4370ebadae4ba093a7c211072b62",
  executable_sha256:"4f502be7d15a064dba525816c63ad471c3ad2fe616ede14712b67cdb85e3cf66",
  producer_source_sha256:"a7ba542c3ada061002b09e638a68423236d5499d11aae0e0083cd6e52d38adfd",
  source_order_source_sha256:"1393f99e71be1e68916b598256d1200d56879b439c05d8c66c098345856c9eb4",
  sq8_source_sha256:"16f03aa7187b4f18676aa511339b07b6e7b8776323710a60071e6ea0d62eb795"},
 backend:{kind:"s3",bucket:"borsuk-bench-453182569524-euc1",region:"eu-central-1",
  physical_prefix:"research/semantic-router/20261010/full1000-local-admission-a0004/retained-source",
  sq8_object_key:"semantic/objects/cc4395af5fdbdeffcb596460aceaad0e55bdcc4af074f405e78ab2dd2be002e1",
  sq8_etag:"\"4875c438eb819e00fc58ac993227fb2b\""},
 credential_source:"imds_instance_role_only",truth_opened:false,source_cache:"off",
 max_memory_bytes:536870912,fetch_parallelism:64,source_get_budget:128,
 sq8_get_budget:30,sq8_byte_budget:33554432,serving:{mode:"baseline"},
 execution:{mode:"diagnostic_panel",ordinals:roster,trace:false}};
def sha: type == "string" and test("^[0-9a-f]{64}$");
def descriptor:
 keys == ["bytes","path","sha256"] and (.sha256|sha)
 and (.path|type == "string" and startswith("/"))
 and (.bytes|type == "number" and . == floor and . > 0 and . <= 16777216);
def config_guard:
 length == 2 and all(.[]; .schema == "borsuk-completed-native-reduction-config-v4"
  and (.input|descriptor))
 and (.[0].expected_identity|contains(historical_identity_pins))
 and (.[0].expected_identity|has("sq8_range_split")|not)
 and .[0].expected_bound_inputs == original_inputs
 and .[1].expected_identity.schema == "borsuk-cohere-native-baseline-result-v10"
 and (.[1].expected_identity|has("sq8_range_split"))
 and .[1].expected_identity.sq8_range_split == null
 and .[1].expected_bound_inputs == (original_inputs + {sq8_range_split:null});
def report_guard($c):
 .schema == "borsuk-completed-native-reduction-v4" and .status == "MEASURED"
 and .complete == true and .diagnostic_only == true and .diagnostic_query_count == 32
 and .result_path == $c.input.path and .result_bytes == $c.input.bytes
 and .result_sha256 == $c.input.sha256 and .identity == $c.expected_identity
 and .inputs == $c.expected_bound_inputs;
def canonical($current):
 if [.[] | select(.phase == "startup") | .library_cap_bytes] == [536870912]
 then . else error("exact 512MiB library admission required") end
 |
 [.[] | select(.phase == "query")
  | if (has("ordinal") and (.returned|type == "array" and length == 10)
       and (.trace|type == "object") and (.plan|type == "object")
       and (.charges.source|type == "object") and (.charges.sq8|type == "object")
       and (if $current then (.plan|has("logical_range_count"))
            and .plan.logical_range_count == (.plan.ranges|length)
            else (.plan|has("logical_range_count")|not) end))
    then {ordinal,returned,trace,charges:{source:.charges.source,sq8:.charges.sq8},
          plan:(.plan | if $current then del(.logical_range_count) else . end)}
    else error("missing canonical query evidence") end]
 | if map(.ordinal) != roster then error("exact ordered 32-query roster required") else . end
 # Refuse numbers outside jq exact-integer range rather than silently round them.
 | if all(..|numbers; . >= 0 and . <= 9007199254740991 and . == floor)
   then . else error("canonical integer outside exact jq range") end;
JQ
)

parity_die() { printf '%s\n' "$*" >&2; exit 1; }
parity_exit() {
 local rc=$1 owned=$2
 trap - EXIT
 if (( rc != 0 && owned )); then
  printf '{"status":"INVALID","exit":%d}\n' "$rc" > "$NEW_OUTPUT_DIR/INVALID.json" || :
 fi
 exit "$rc"
}
parity_sha() { sha256sum -- "$1" | cut -d ' ' -f 1; }
parity_regular() {
 local path=$1 cap=$2 size
 [[ $path == /* && $path != *[$'\t\r\n']* && -f $path && ! -L $path && $(realpath -e -- "$path") == "$path" ]] || parity_die "Not a canonical regular file: $path"
 size=$(stat -c %s -- "$path")
 (( size > 0 && size <= cap )) || parity_die "File byte cap: $path"
}
parity_snapshot() {
 parity_regular "$1" "$2"
 stat -c '%d:%i:%s:%f:%y:%z' -- "$1" || return $?
 parity_sha "$1"
}
parity_compare_queries() {
 # Only these explicit fields are equalities; native checks stage/transport/seals.
 jq -ceS -s "$parity_jq canonical(false)" "$hist_result" > "$NEW_OUTPUT_DIR/historical.canonical.json" || return $?
 jq -ceS -s "$parity_jq canonical(true)" "$null_result" > "$NEW_OUTPUT_DIR/null.canonical.json" || return $?
 cmp -- "$NEW_OUTPUT_DIR/historical.canonical.json" "$NEW_OUTPUT_DIR/null.canonical.json"
}
parity_validate_compare() {
 local hist_rc=0 null_rc=0
 # Keep independent original exits and logs. Never query either JSONL on failure.
 "$REDUCER" --completed-scale "$HIST_COMPLETED_CFG" "$HIST_CFG_SHA" "$NEW_OUTPUT_DIR/historical.native.json" > "$NEW_OUTPUT_DIR/historical.stdout" 2> "$NEW_OUTPUT_DIR/historical.stderr" || hist_rc=$?
 "$REDUCER" --completed-scale "$NULL_COMPLETED_CFG" "$NULL_CFG_SHA" "$NEW_OUTPUT_DIR/null.native.json" > "$NEW_OUTPUT_DIR/null.stdout" 2> "$NEW_OUTPUT_DIR/null.stderr" || null_rc=$?
 printf '{"historical_exit":%d,"null_exit":%d}\n' "$hist_rc" "$null_rc" > "$NEW_OUTPUT_DIR/native-exits.json"
 (( hist_rc == 0 )) || exit "$hist_rc"
 (( null_rc == 0 )) || exit "$null_rc"
 parity_regular "$NEW_OUTPUT_DIR/historical.native.json" 1048576
 parity_regular "$NEW_OUTPUT_DIR/null.native.json" 1048576
 jq -e --slurpfile c "$HIST_COMPLETED_CFG" "$parity_jq report_guard(\$c[0])" "$NEW_OUTPUT_DIR/historical.native.json" > /dev/null || return $?
 jq -e --slurpfile c "$NULL_COMPLETED_CFG" "$parity_jq report_guard(\$c[0])" "$NEW_OUTPUT_DIR/null.native.json" > /dev/null || return $?
 # Both native decoders have now authenticated unique keys, seals, EOF and headers.
 jq -e -s "$parity_jq config_guard" "$HIST_COMPLETED_CFG" "$NULL_COMPLETED_CFG" > /dev/null || return $?
 parity_compare_queries
}
parity_main() {
 set -Eeuo pipefail
 export LC_ALL=C TZ=UTC
 umask 077
 local output_owned=0 hist_result null_result hist_bytes null_bytes hist_sha null_sha
 local reducer_before hist_cfg_before null_cfg_before hist_before null_before canonical_sha
 trap 'parity_exit "$?" "$output_owned"' EXIT
 [[ $# == 0 ]] || parity_die 'Use the seven named environment variables'
 : "${REDUCER:?}" "${REDUCER_SHA:?}" "${HIST_COMPLETED_CFG:?}" "${HIST_CFG_SHA:?}" "${NULL_COMPLETED_CFG:?}" "${NULL_CFG_SHA:?}" "${NEW_OUTPUT_DIR:?}"
 local pin
 for pin in "$REDUCER_SHA" "$HIST_CFG_SHA" "$NULL_CFG_SHA"; do
  [[ $pin =~ ^[0-9a-f]{64}$ ]] || parity_die 'Invalid SHA256 pin'
 done
 [[ $HIST_COMPLETED_CFG != "$NULL_COMPLETED_CFG" ]] || parity_die 'Independent configs required'
 parity_regular "$REDUCER" 268435456
 [[ -x $REDUCER ]] || parity_die 'Reducer is not executable'
 parity_regular "$HIST_COMPLETED_CFG" 262144
 parity_regular "$NULL_COMPLETED_CFG" 262144
 [[ $(parity_sha "$REDUCER") == "$REDUCER_SHA" && $(parity_sha "$HIST_COMPLETED_CFG") == "$HIST_CFG_SHA" && $(parity_sha "$NULL_COMPLETED_CFG") == "$NULL_CFG_SHA" ]] || parity_die 'Reducer/config pin mismatch'
 [[ $NEW_OUTPUT_DIR == /* && $NEW_OUTPUT_DIR != *[$'\t\r\n']* && ! -e $NEW_OUTPUT_DIR && ! -L $NEW_OUTPUT_DIR && $(realpath -m -- "$NEW_OUTPUT_DIR") == "$NEW_OUTPUT_DIR" ]] || parity_die 'Output must be fresh and canonical'
 [[ -d $(dirname -- "$NEW_OUTPUT_DIR") ]] || parity_die 'Output parent must exist'
 mkdir -- "$NEW_OUTPUT_DIR"
 output_owned=1
 reducer_before=$(parity_snapshot "$REDUCER" 268435456)
 hist_cfg_before=$(parity_snapshot "$HIST_COMPLETED_CFG" 262144)
 null_cfg_before=$(parity_snapshot "$NULL_COMPLETED_CFG" 262144)
 [[ ${reducer_before##*$'\n'} == "$REDUCER_SHA" && ${hist_cfg_before##*$'\n'} == "$HIST_CFG_SHA" && ${null_cfg_before##*$'\n'} == "$NULL_CFG_SHA" ]] || parity_die 'Reducer/config changed before snapshot'
 # Descriptor reads only, no result/query parsing; native rejects duplicate keys.
 jq -e '.input | keys == ["bytes","path","sha256"]' "$HIST_COMPLETED_CFG" > /dev/null
 jq -e '.input | keys == ["bytes","path","sha256"]' "$NULL_COMPLETED_CFG" > /dev/null
 hist_result=$(jq -er '.input.path | strings' "$HIST_COMPLETED_CFG")
 null_result=$(jq -er '.input.path | strings' "$NULL_COMPLETED_CFG")
 hist_bytes=$(jq -er "$parity_jq .input | select(descriptor) | .bytes" "$HIST_COMPLETED_CFG")
 null_bytes=$(jq -er "$parity_jq .input | select(descriptor) | .bytes" "$NULL_COMPLETED_CFG")
 hist_sha=$(jq -er '.input.sha256' "$HIST_COMPLETED_CFG")
 null_sha=$(jq -er '.input.sha256' "$NULL_COMPLETED_CFG")
 [[ $hist_result != "$null_result" ]] || parity_die 'Independent results required'
 hist_before=$(parity_snapshot "$hist_result" 16777216)
 null_before=$(parity_snapshot "$null_result" 16777216)
 [[ $(stat -c %s -- "$hist_result") == "$hist_bytes" && $(parity_sha "$hist_result") == "$hist_sha" && $(stat -c %s -- "$null_result") == "$null_bytes" && $(parity_sha "$null_result") == "$null_sha" ]] || parity_die 'Original result pin mismatch'
 printf '%s\n' "$reducer_before" "$hist_cfg_before" "$null_cfg_before" "$hist_before" "$null_before" > "$NEW_OUTPUT_DIR/before.identities"
 parity_validate_compare
 canonical_sha=$(parity_sha "$NEW_OUTPUT_DIR/historical.canonical.json") || parity_die 'Canonical digest failed'
 [[ $canonical_sha =~ ^[0-9a-f]{64}$ ]] || parity_die 'Invalid canonical digest'
 jq -n --arg reducer "$REDUCER_SHA" --arg hist_cfg "$HIST_CFG_SHA" --arg null_cfg "$NULL_CFG_SHA" --slurpfile h "$HIST_COMPLETED_CFG" --slurpfile n "$NULL_COMPLETED_CFG" --arg canonical_sha "$canonical_sha" \
  '{schema:"borsuk-disabled-historical-parity-v1",status:"EXACT_CANONICAL_PARITY",complete:true,
    reducer_sha256:$reducer,historical_config_sha256:$hist_cfg,null_config_sha256:$null_cfg,
    historical:$h[0],current_null:$n[0],canonical_sha256:$canonical_sha,query_count:32,
    normalization:"remove only current plan.logical_range_count; preserve all array order",
    compared_fields:["ordinal","returned","trace","charges.source","charges.sq8","plan"],
    native_validations:2,originals_unchanged:true,qualified:false,
    prior_access:"real-input smoke S3 reads are prior access; caches uncontrolled",
    historical_physical_ordinal_parity:false,complete_scored_row_roster:false,
    cold_claim:false,quality_claim:false,performance_claim:false,vendor_claim:false,
    root_execution_qualification_and_provenance_gate_required:true}' > "$NEW_OUTPUT_DIR/.receipt.pending.json"
 printf '%s\n' "$(parity_snapshot "$REDUCER" 268435456)" "$(parity_snapshot "$HIST_COMPLETED_CFG" 262144)" "$(parity_snapshot "$NULL_COMPLETED_CFG" 262144)" "$(parity_snapshot "$hist_result" 16777216)" "$(parity_snapshot "$null_result" 16777216)" > "$NEW_OUTPUT_DIR/after.identities"
 cmp -- "$NEW_OUTPUT_DIR/before.identities" "$NEW_OUTPUT_DIR/after.identities" || parity_die 'Original file changed during validation/comparison'
 # Same-filesystem hard link publishes a complete receipt atomically, create-only.
 ln -T -- "$NEW_OUTPUT_DIR/.receipt.pending.json" "$NEW_OUTPUT_DIR/parity.json"
 trap - EXIT
}

if [[ ${BASH_SOURCE[0]} == "$0" ]]; then parity_main "$@"; fi
