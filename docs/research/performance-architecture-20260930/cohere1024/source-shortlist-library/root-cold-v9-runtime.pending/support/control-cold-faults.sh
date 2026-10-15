#!/bin/bash
# EC2-only source-bound control falsifiers. No scientific bodies/native actors.
set -euo pipefail
root=/mnt/borsuk-v9-panel
[[ $EUID == 0 && ! -e $root/bin/original && ! -e $root/bin/candidate && ! -e $root/inputs/queries.f32 && ! -e $root/inputs/truth.u64 ]] || exit 125
ev=$root/evidence/cold-faults
mkdir -m 700 "$ev"
cg=/sys/fs/cgroup$(sed -n 's/^0:://p' /proc/self/cgroup)
[[ $(cat "$cg/memory.max") == 268435456 && $(cat "$cg/memory.swap.max") == 0 && $(cat "$cg/pids.max") == 128 ]] || exit 125
cat "$cg/cpu.max" > "$ev/cpu.max"
sha256sum "$root/run-v9-chain.sh" "$root/reduce-v9-abba.jq" "$root/smoke-v9-object.sh" > "$ev/source.sha256"
# Execute the exact verifier body against one copied, then mutated, installed script.
sand=$ev/code
mkdir -m 700 "$sand" "$sand/evidence"
cp "$root/run-v9-stage.sh" "$sand/run-v9-stage.sh"
(cd "$sand" && sha256sum run-v9-stage.sh > runtime-code.sha256)
pin=$(sha256sum "$sand/runtime-code.sha256" | cut -d ' ' -f 1)
{
 printf 'set -euo pipefail\nroot=%q\ncode_pin=%q\ncode_checks=0\n' "$sand" "$pin"
 awk '/^verify_code\(\) \{/{on=1} on{print} on && /^\}/{exit}' "$root/run-v9-chain.sh"
 printf 'verify_code\ntouch %q\n' "$sand/released"
} > "$sand/check.sh"
/bin/bash "$sand/check.sh"
[[ -f $sand/released ]] || exit 1
rm -- "$sand/released"
chmod u+w "$sand/run-v9-stage.sh"
printf '\n# mutation\n' >> "$sand/run-v9-stage.sh"
set +e
/bin/bash "$sand/check.sh" > "$sand/mutated.out" 2>&1
rc=$?
set -e
[[ $rc != 0 && ! -e $sand/released ]] || exit 1
# Exact formal-cell admission fragment: equality refuses, next second releases.
mkdir -m 700 "$ev/budget-bin"
printf '#!/bin/bash\nprintf "10000\\n"\n' > "$ev/budget-bin/date"
chmod 0500 "$ev/budget-bin/date"
awk '/^ # Reserve worst-case native/{left=2;next} left>0{print;left--}' "$root/run-v9-chain.sh" > "$ev/budget-fragment.sh"
[[ $(wc -l < "$ev/budget-fragment.sh") == 2 ]] || exit 1
for seconds in 1530 1531; do
 dir=$ev/budget-$seconds
 mkdir -m 700 "$dir"
 printf '%s\n' "$((10000+seconds))" > "$dir/machine-stop.epoch"
 { printf 'set -euo pipefail\nroot=%q\n' "$dir"; cat "$ev/budget-fragment.sh"; printf 'touch %q\n' "$dir/released"; } > "$dir/check.sh"
 set +e
 PATH="$ev/budget-bin:$PATH" /bin/bash "$dir/check.sh" > "$dir/out" 2>&1
 rc=$?
 set -e
 if [[ $seconds == 1530 ]]; then [[ $rc == 125 && ! -e $dir/released ]]; else [[ $rc == 0 && -f $dir/released ]]; fi
done
# Aggregate-only synthetic fixtures: never ANN/result-JSONL evidence.
mkdir -m 700 "$ev/aggregate"
n=0
for label in a1 b1 b2 a2; do
 n=$((n+1)); hash=$(printf '%064d' "$n")
 template=$root/config/formal-original.completed.EXPECTED.json
 wall=95000000000; p90=100000000; p95=200000000; underfill=0
 if [[ $label == b* ]]; then template=$root/config/formal-candidate.completed.EXPECTED.json; wall=100000000000; p90=105000000; p95=190000000; fi
 [[ $label != b1 ]] || underfill=1
 jq -n --slurpfile expected "$template" --arg sha "$hash" --argjson wall "$wall" --argjson p90 "$p90" --argjson p95 "$p95" --argjson underfill "$underfill" '{schema:"borsuk-completed-native-reduction-v4",status:"MEASURED",complete:true,population_percentiles_valid:true,diagnostic_only:false,identity:$expected[0].expected_identity,inputs:$expected[0].expected_bound_inputs,result_sha256:$sha,result_bytes:123,statistics:{count:1000,p90_ns:$p90,p95_ns:$p95,sequential_qps:(1000000000000/$wall),query_wall_ns:$wall},native_terminal:{complete:true,selected_count:1000,population_count:1000,recall_denominator:10000,recall_numerator:9780,underfilled_queries:$underfill,observed_process_peak_bytes:1000000,transport_last_boundary:{after:{transport_failures:0,stream_failures:0,dropped_error_bodies:0}},charges:{router:{failed_gets:0,submitted_gets:0,verified_bytes:0},source:{failed_gets:0,submitted_gets:0,verified_bytes:0},sq8:{failed_gets:0,submitted_gets:0,verified_bytes:0}}},synthetic_control_fixture:true}' > "$ev/aggregate/$label.json"
done
reduce() {
 jq -n --slurpfile a1 "$ev/aggregate/a1.json" --slurpfile b1 "$ev/aggregate/b1.json" --slurpfile b2 "$ev/aggregate/b2.json" --slurpfile a2 "$ev/aggregate/a2.json" --slurpfile ea "$root/config/formal-original.completed.EXPECTED.json" --slurpfile eb "$root/config/formal-candidate.completed.EXPECTED.json" -f "$root/reduce-v9-abba.jq"
}
reduce > "$ev/aggregate/equal-five.json"
jq -e '.status=="MEASURED" and .cells[1].underfilled_queries==1 and .candidate_matched_recall==true and .candidate_full_k==false and .modest_performance_screen==true' "$ev/aggregate/equal-five.json" >/dev/null
cp "$ev/aggregate/b1.json" "$ev/aggregate/b1.base.json"
jq '.statistics.p90_ns=105000001' "$ev/aggregate/b1.base.json" > "$ev/aggregate/b1.json"
reduce > "$ev/aggregate/over-p90.json"
jq -e '.status=="MEASURED" and .modest_performance_screen==false' "$ev/aggregate/over-p90.json" >/dev/null
jq '.statistics.query_wall_ns=100000000001 | .statistics.sequential_qps=(1000000000000/.statistics.query_wall_ns)' "$ev/aggregate/b1.base.json" > "$ev/aggregate/b1.json"
reduce > "$ev/aggregate/over-qps.json"
jq -e '.status=="MEASURED" and .modest_performance_screen==false' "$ev/aggregate/over-qps.json" >/dev/null
# Exact smoke source under a single root rewrite; PATH AWS spy cannot hit network.
mkdir -m 700 "$ev/aws-bin" "$ev/aws" "$ev/smoke-root" "$ev/smoke-root/config" "$ev/smoke-root/evidence"
cp "$root/config/candidate-generation.json" "$ev/smoke-root/config/candidate-generation.json"
printf '%s\n' "$(( $(date +%s)+1000 ))" > "$ev/smoke-root/machine-stop.epoch"
sed "s@^root=/mnt/borsuk-v9-panel\$@root=$ev/smoke-root@" "$root/smoke-v9-object.sh" > "$ev/smoke-source.sh"
[[ $(grep -Fxc "root=$ev/smoke-root" "$ev/smoke-source.sh") == 1 ]] || exit 1
if grep -Fq 'root=/mnt/borsuk-v9-panel' "$ev/smoke-source.sh"; then exit 1; fi
cat > "$ev/aws-bin/aws" <<'SPY'
#!/bin/bash
set -euo pipefail
while [[ $# -gt 0 && $1 != s3api ]]; do shift; done
[[ $# -ge 2 ]] || exit 97
shift; op=$1; shift
body=; metadata=; match=
while (($#)); do
 case $1 in --body) body=$2;shift 2;; --metadata) metadata=$2;shift 2;; --if-match) match=$2;shift 2;; *) shift;; esac
done
d=$FAKE_AWS_DIR
case $op in
 head-object)
  if [[ -f $d/object ]]; then jq -n --arg token "$(cat "$d/owner")" '{ETag:"\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\"",Metadata:{"borsuk-smoke-owner":$token}}'
  else printf '%s\n' 'An error occurred (404) when calling HeadObject: Not Found' >&2;exit 254;fi;;
 put-object) cp "$body" "$d/object";printf '%s\n' "$(printf '%s' "$metadata" | cut -d = -f 2)" > "$d/owner";printf '%s\n' '{"ETag":"\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\""}';;
 get-object) printf '%s\n' INJECTED_READBACK_FAILURE >&2;exit 42;;
 delete-object) [[ $match == '"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"' ]] || exit 96;rm -- "$d/object";touch "$d/deleted";printf '{}\n';;
 *) exit 97;;
esac
SPY
chmod 0500 "$ev/aws-bin/aws"
set +e
PATH="$ev/aws-bin:$PATH" FAKE_AWS_DIR="$ev/aws" /bin/bash "$ev/smoke-source.sh" > "$ev/smoke.out" 2>&1
rc=$?
set -e
[[ $rc == 42 && -f $ev/aws/deleted && ! -e $ev/aws/object && $(cat "$ev/smoke-root/evidence/smoke-object/original.exit") == 42 && $(cat "$ev/smoke-root/evidence/smoke-object/cleanup.exit") == 0 ]] || exit 1
jq -n '{schema:"borsuk-v9-cold-fault-canary-v1",status:"FAULT_CANARY_PASS",runtime_script_mutation_refused:true,whole_cell_budget_boundary_verified:true,underfill_remains_measured:true,exact_five_percent_boundaries_verified:true,smoke_failure_cleanup_preserves_original:true,aws_mocked:true,native_actors_executed:false,scientific_bodies_opened:false,performance_claim:false}' > "$root/evidence/cold-fault-canary.json"
