#!/usr/bin/env bash
# UNVERIFIED. Root-only execution: synthetic MOCKED native decoder, never science.
# Uses the production canonical extractor and validator-before-query guard verbatim.
# Usage: bash fault-fixture.sh NEW_FIXTURE_DIR (canonical parent must exist).
set -Eeuo pipefail
export LC_ALL=C TZ=UTC
umask 077
[[ $# == 1 && $1 == /* && ! -e $1 && ! -L $1 && $(realpath -m -- "$1") == "$1" ]] || exit 1
mkdir -- "$1"
fixture=$1
here=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)
# shellcheck source=check-disabled-historical-parity.sh
source "$here/check-disabled-historical-parity.sh"
printf '%s\n' 'MOCKED synthetic fixture; no native validation, seals, execution or scientific qualification' > "$fixture/MOCKED.txt"
cat > "$fixture/mock-reducer" <<'MOCK'
#!/usr/bin/env bash
set -Eeuo pipefail
[[ $1 == --completed-scale ]] || exit 72
[[ ${MOCK_FAIL_ROLE:-} != "$2" ]] || exit 73
jq '{schema:"borsuk-completed-native-reduction-v4",status:"MEASURED",complete:true,
 diagnostic_only:true,diagnostic_query_count:32,result_path:.input.path,
 result_bytes:.input.bytes,result_sha256:.input.sha256,identity:.expected_identity,
 inputs:.expected_bound_inputs,mocked:true}' "$2" > "$4"
MOCK
chmod 700 "$fixture/mock-reducer"
REDUCER=$fixture/mock-reducer
HIST_COMPLETED_CFG=$fixture/historical.config.json
NULL_COMPLETED_CFG=$fixture/null.config.json
hist_result=$fixture/historical.jsonl
null_result=$fixture/null.jsonl
# Every query field below is synthetic; ordering and nested contents are deliberate.
jq -nc "$parity_jq"' {phase:"startup",library_cap_bytes:536870912},
 (roster[] as $ordinal | {
 phase:"query",ordinal:$ordinal,returned:[range(10)|{id:.,score_bits:(1065353216+.)}],
 trace:{ranked_candidate_pages:[2,1],nomination_evaluated_units:[4,3],primary_page:2,
 discoveries:[{seed_page:2,seed_evaluated_units:[4,3],walk_evaluated_units:[6,5],
 seed_work_exhausted:false,walk_work_exhausted:true}],semantic_leaves:[8,7],
 semantic_units:[10,9],semantic_seed_additions:[12,11]},
 charges:{source:{submitted_gets:2,verified_bytes:1024,failed_gets:0},
 sq8:{submitted_gets:1,verified_bytes:4096,failed_gets:0}},
 plan:{page_rows:32,selected_pages:[2,1],ranges:[[0,4096]],planned_bytes:4096,
 target_pages:2,target_shortfall:0,primary_pages_retained:1,covered_pages:2,bridge_pages:0}})
 ' > "$hist_result"
jq -c 'if .phase == "query" then .plan.logical_range_count = (.plan.ranges|length) else . end' "$hist_result" > "$fixture/null.original.jsonl"
# Reuse the source-defined authority so drift in production is exercised by fixture.
jq -n "$parity_jq"' {schema:"borsuk-completed-native-reduction-config-v4",
 expected_identity:historical_identity_pins,expected_bound_inputs:original_inputs}' > "$fixture/historical.template.json"
jq '.expected_identity.schema="borsuk-cohere-native-baseline-result-v10"
 | .expected_identity.sq8_range_split=null | .expected_bound_inputs.sq8_range_split=null' \
 "$fixture/historical.template.json" > "$fixture/null.template.json"
fixture_config() {
 jq --arg path "$1" --arg sha "$(parity_sha "$1")" --argjson bytes "$(stat -c %s -- "$1")" \
  '. + {input:{path:$path,bytes:$bytes,sha256:$sha}}' "$2" > "$3"
}
fixture_config "$hist_result" "$fixture/historical.template.json" "$HIST_COMPLETED_CFG"
HIST_CFG_SHA=$(parity_sha "$HIST_COMPLETED_CFG")
# Explicit independent mutations; never sort or deduplicate query/returned arrays.
# shellcheck disable=SC2016
mutation_jq=$(cat <<'JQ'
if $case == "identical" or ($case|startswith("validator-")) then .
elif $case == "score" then .[7].returned[9].score_bits += 1
elif $case == "id" then .[7].returned[9].id = 20
elif $case == "returned-order" then .[7].returned |= reverse
elif $case == "trace" then .[7].trace.discoveries[0].walk_evaluated_units |= reverse
elif $case == "plan" then .[7].plan.selected_pages |= reverse
elif $case == "source-charge" then .[7].charges.source.verified_bytes += 1
elif $case == "sq8-charge" then .[7].charges.sq8.submitted_gets += 1
elif $case == "ordinal" then .[7].ordinal += 1
elif $case == "query-order" then reverse
elif $case == "duplicate-query" then .[8] = .[7]
elif $case == "missing-query" then del(.[7])
elif $case == "missing-field" then del(.[7].trace)
else error("unknown fixture case") end | .[]
JQ
)
for case in identical score id returned-order trace plan source-charge sq8-charge ordinal query-order duplicate-query missing-query missing-field validator-historical validator-null; do
 jq -cs --arg case "$case" "$mutation_jq" "$fixture/null.original.jsonl" > "$null_result"
 fixture_config "$null_result" "$fixture/null.template.json" "$NULL_COMPLETED_CFG"
 NULL_CFG_SHA=$(parity_sha "$NULL_COMPLETED_CFG")
 NEW_OUTPUT_DIR=$fixture/$case
 mkdir -- "$NEW_OUTPUT_DIR"
 MOCK_FAIL_ROLE=
 case "$case" in
  validator-historical) MOCK_FAIL_ROLE=$HIST_COMPLETED_CFG ;;
  validator-null) MOCK_FAIL_ROLE=$NULL_COMPLETED_CFG ;;
 esac
 export MOCK_FAIL_ROLE
 # The extractor creates its output before jq reads any query; absence proves refusal.
 rc=0
 (
  parity_validate_compare
 ) > "$NEW_OUTPUT_DIR/fixture.stdout" 2> "$NEW_OUTPUT_DIR/fixture.stderr" || rc=$?
 if [[ $case == identical ]]; then
  (( rc == 0 )) || parity_die 'MOCKED identical fixture refused'
 elif [[ $case == validator-* ]]; then
  [[ $rc == 73 && ! -e $NEW_OUTPUT_DIR/historical.canonical.json && ! -e $NEW_OUTPUT_DIR/null.canonical.json ]] || parity_die "MOCKED validator-before-parse guard failed: $case"
  jq -e --arg case "$case" 'if $case == "validator-historical" then .historical_exit == 73 and .null_exit == 0 else .historical_exit == 0 and .null_exit == 73 end' "$NEW_OUTPUT_DIR/native-exits.json" > /dev/null
 else
  (( rc != 0 )) || parity_die "MOCKED mutation accepted: $case"
 fi
 printf 'MOCKED %s exit=%d\n' "$case" "$rc"
done
printf '%s\n' 'MOCKED source-bound canonical/guard cases passed; native and real-input gates remain UNRUN'
