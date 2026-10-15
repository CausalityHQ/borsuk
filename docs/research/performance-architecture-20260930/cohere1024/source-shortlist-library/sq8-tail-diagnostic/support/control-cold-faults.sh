#!/bin/bash
# shellcheck disable=SC2016 # Source anchors intentionally contain literal shell expressions.
# EC2-only source-bound control falsifiers. No scientific bodies/native actors.
set -euo pipefail
root=/mnt/borsuk-v9-panel
[[ $# == 0 && $EUID == 0 && ! -e $root/bin/original && ! -e $root/bin/candidate && ! -e $root/inputs/queries.f32 && ! -e $root/inputs/truth.u64 ]] || exit 125
ev=$root/evidence/cold-faults
mkdir -m 700 "$ev"
cg=/sys/fs/cgroup$(sed -n 's/^0:://p' /proc/self/cgroup)
[[ $(cat "$cg/memory.max") == 268435456 && $(cat "$cg/memory.swap.max") == 0 && $(cat "$cg/pids.max") == 128 ]] || exit 125
cat "$cg/cpu.max" > "$ev/cpu.max"
sha256sum "$root/run-sq8-panel-chain.sh" "$root/run-v9-stage.sh" "$root/smoke-v9-object.sh" > "$ev/source.sha256"
# Execute the exact verifier body against one copied, then mutated, installed script.
sand=$ev/code
mkdir -m 700 "$sand" "$sand/evidence"
cp "$root/run-v9-stage.sh" "$sand/run-v9-stage.sh"
(cd "$sand" && sha256sum run-v9-stage.sh > runtime-code.sha256)
pin=$(sha256sum "$sand/runtime-code.sha256" | cut -d ' ' -f 1)
{
 printf 'set -euo pipefail\nroot=%q\ncode_pin=%q\ncode_checks=0\n' "$sand" "$pin"
 awk '/^verify_code\(\) \{/{on=1} on{print} on && /^\}/{exit}' "$root/run-sq8-panel-chain.sh"
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
# Conditional calls must reject a rewritten manifest despite errexit suppression.
(cd "$sand" && sha256sum run-v9-stage.sh > runtime-code.sha256)
sed 's/^verify_code$/if verify_code; then exit 0; else exit 73; fi/' "$sand/check.sh" > "$sand/conditional.sh"
set +e
/bin/bash "$sand/conditional.sh" > "$sand/manifest-mutation.out" 2>&1
rc=$?
set -e
[[ $rc == 73 && ! -e $sand/released ]] || exit 1
# Extract the current functions only; never source the chain or launch native/systemd.
for function in reserve panel_report; do
 awk -v name="$function" '$0==name"() {"{on=1} on{print} on && /^\}/{exit}' "$root/run-sq8-panel-chain.sh" > "$ev/$function.sh"
 [[ $(grep -Fxc "$function() {" "$ev/$function.sh") == 1 && $(tail -n 1 "$ev/$function.sh") == '}' ]] || exit 1
done
# Exact reserve(), mocked wall clock: equality refuses, one extra second permits.
mkdir -m 700 "$ev/budget-bin"
printf '#!/bin/bash\nprintf "10000\\n"\n' > "$ev/budget-bin/date"
chmod 0500 "$ev/budget-bin/date"
for seconds in 1220 860; do
 for margin in 0 1 bad; do
  dir=$ev/budget-$seconds-$margin
  mkdir -m 700 "$dir"
  if [[ $margin == bad ]]; then printf 'bad-deadline\n' > "$dir/machine-stop.epoch"
  else printf '%s\n' "$((10000+seconds+margin))" > "$dir/machine-stop.epoch"; fi
  {
   printf 'set -euo pipefail\nroot=%q\n' "$dir"
   cat "$ev/reserve.sh"
   printf 'reserve %s\ntouch %q\n' "$seconds" "$dir/released"
  } > "$dir/check.sh"
  set +e
  PATH="$ev/budget-bin:$PATH" /bin/bash "$dir/check.sh" > "$dir/out" 2>&1
  rc=$?
  set -e
  printf '%s\n' "$rc" > "$dir/observed.exit"
  if [[ $margin == 1 ]]; then [[ $rc == 0 && -f $dir/released ]]
  else [[ $rc == 125 && ! -e $dir/released ]]; fi
 done
done
# Source assertions bind reserves to each fixed next native/panel/reader stage.
# Only owned() and reserve() read the deadline; no aggregate remaining-chain gate.
[[ $(grep -Fc 'machine-stop.epoch' "$root/run-sq8-panel-chain.sh") == 2 ]] || exit 1
[[ $(grep -Fc 'machine-stop.epoch' "$ev/reserve.sh") == 1 ]] || exit 1
[[ $(awk '/^owned[(][)] [{]$/{on=1} on{print} on && /^[}]$/{exit}' "$root/run-sq8-panel-chain.sh" | grep -Fc 'machine-stop.epoch') == 1 ]] || exit 1
sed -n '/^[[:space:]]*reserve /s/^[[:space:]]*//p' "$root/run-sq8-panel-chain.sh" > "$ev/reserve-calls.txt"
cat <<'EXPECTED' | cmp - "$ev/reserve-calls.txt"
reserve 1220
reserve 1220
reserve 1220
reserve 860
EXPECTED
for line in 'for kind in admission canary; do' ' for arm in original candidate; do' 'for prefix in canary-panel panel; do' ' for label in a0 b0 a1 b1 b2 a2; do'; do
 [[ $(grep -Fxc "$line" "$root/run-sq8-panel-chain.sh") == 1 ]] || exit 1
done
grep -F -A2 '  phase=$kind-$arm' "$root/run-sq8-panel-chain.sh" > "$ev/native-reserve.txt"
cat <<'EXPECTED' | cmp - "$ev/native-reserve.txt"
  phase=$kind-$arm
  reserve 1220
  run "$phase" "$root/config/$phase.config.json"
EXPECTED
grep -F -A1 ' phase=$prefix-start' "$root/run-sq8-panel-chain.sh" > "$ev/panel-reserve.txt"
cat <<'EXPECTED' | cmp - "$ev/panel-reserve.txt"
 phase=$prefix-start
 reserve 1220
EXPECTED
grep -F -A2 '  phase=$prefix-$label' "$root/run-sq8-panel-chain.sh" > "$ev/panel-native-reserve.txt"
cat <<'EXPECTED' | cmp - "$ev/panel-native-reserve.txt"
  phase=$prefix-$label
  reserve 1220
  run "$phase" "$root/config/$label.runtime.json"
EXPECTED
grep -F -A3 ' phase=$prefix-attribution' "$root/run-sq8-panel-chain.sh" > "$ev/reader-reserve.txt"
cat <<'EXPECTED' | cmp - "$ev/reader-reserve.txt"
 phase=$prefix-attribution
 reserve 860
 [[ $(stat -c %s "$config") -le 32768 ]] || exit 125
 run "$phase" "$config"
EXPECTED
# Execute exact result/config size guards on sparse metadata fixtures.
grep -F 'bytes=$(stat -c %s "$output"); ((bytes>0 && bytes<=67108864)) || exit 125' "$root/run-sq8-panel-chain.sh" > "$ev/result-cap-lines.sh"
[[ $(wc -l < "$ev/result-cap-lines.sh") == 2 ]] || exit 1
sed 's/^ *//' "$ev/result-cap-lines.sh" | sort -u > "$ev/result-cap.sh"
[[ $(wc -l < "$ev/result-cap.sh") == 1 ]] || exit 1
grep -Fx ' [[ $(stat -c %s "$config") -le 32768 ]] || exit 125' "$root/run-sq8-panel-chain.sh" > "$ev/config-cap.sh"
[[ $(wc -l < "$ev/config-cap.sh") == 1 ]] || exit 1
grep -Fxq '  [[ $(stat -c %s "$config") -le 32768 ]];;' "$root/run-v9-stage.sh"
for guard in result config; do
 if [[ $guard == result ]]; then sizes=(0 67108864 67108865); limit=67108864
 else sizes=(32768 32769); limit=32768; fi
 for size in "${sizes[@]}"; do
  file=$ev/$guard-$size
  truncate -s "$size" "$file"
  {
   printf 'set -euo pipefail\noutput=%q\nconfig=%q\n' "$file" "$file"
   cat "$ev/$guard-cap.sh"
   printf 'touch %q\n' "$file.released"
  } > "$file.check.sh"
  set +e
  /bin/bash "$file.check.sh" > "$file.out" 2>&1
  rc=$?
  set -e
  printf '%s\n' "$rc" > "$file.exit"
  if ((size>0 && size<=limit)); then [[ $rc == 0 && -f $file.released ]]
  else [[ $rc == 125 && ! -e $file.released ]]; fi
 done
done
# Synthetic fixed-32/six-run metadata only; no result JSONL or frozen config reads.
panel=$ev/panel
mkdir -m 700 "$panel" "$panel/results"
input_sha=$(printf '%064d' 1)
jq -n --arg sha "$input_sha" '{schema:"borsuk-sq8-panel-attribution-config-v1",runs:(["A0","B0","A1","B1","B2","A2"] | map({label:.,completed:{input:{path:("/synthetic/" + . + ".jsonl"),bytes:123,sha256:$sha}}})),synthetic_control_fixture:true}' > "$panel/config.json"
config_sha=$(sha256sum "$panel/config.json" | cut -d ' ' -f 1)
jq -n --slurpfile config "$panel/config.json" --arg sha "$config_sha" '{schema:"borsuk-sq8-panel-attribution-v1",status:"MEASURED",complete:true,diagnostic_only:true,population_percentiles_valid:false,vendor_or_scientific_win_claim:false,default_adoption:false,reduction_config_sha256:$sha,selected_count:32,within_actor_trace_off_parity:true,runs:[$config[0].runs[]|{label,input:.completed.input}],queries:[range(0;32)|{ordinal:(. * 999 / 31 | floor),reproduced_excess:(.==0)}],reproduced_ordinals:1,synthetic_control_fixture:true}' > "$panel/measured.json"
{
 printf 'set -euo pipefail\nroot=%q\n' "$panel"
 cat "$ev/panel_report.sh"
 printf 'panel_report report %q\n' "$panel/config.json"
} > "$panel/check.sh"
cp "$panel/measured.json" "$panel/results/report.json"
/bin/bash "$panel/check.sh" > "$panel/measured.out" 2>&1
jq '.status="INCONCLUSIVE_FIXED_PANEL" | .reproduced_ordinals=0 | .queries[].reproduced_excess=false' "$panel/measured.json" > "$panel/inconclusive.json"
cp "$panel/inconclusive.json" "$panel/results/report.json"
/bin/bash "$panel/check.sh" > "$panel/inconclusive.out" 2>&1
n=0
while IFS= read -r mutation; do
 n=$((n+1))
 jq "$mutation" "$panel/measured.json" > "$panel/results/report.json"
 set +e
 /bin/bash "$panel/check.sh" > "$panel/mutation-$n.out" 2>&1
 rc=$?
 set -e
 printf '%s %s %s\n' "$n" "$rc" "$mutation" >> "$panel/mutations.txt"
 [[ $rc != 0 ]] || exit 1
done <<'MUTATIONS'
.schema="wrong"
.status="INVALID"
.status="INCONCLUSIVE_FIXED_PANEL"
.status="MEASURED" | .reproduced_ordinals=0 | .queries[].reproduced_excess=false
.reduction_config_sha256=("0"*64)
.runs[0].label="B0"
.runs|=reverse
.runs|=.[0:5]
.runs += [.runs[0]]
.runs[0].input.path="/synthetic/wrong.jsonl"
.runs[0].input.bytes=124
.runs[0].input.sha256=("0"*64)
.queries[0].ordinal=1
.queries[1].ordinal=0
.queries|=reverse
.queries|=.[0:31]
.queries += [.queries[0]]
.selected_count=31
.reproduced_ordinals=2
.queries[0].reproduced_excess=false
.within_actor_trace_off_parity=false
.complete=false
.diagnostic_only=false
.population_percentiles_valid=true
.vendor_or_scientific_win_claim=true
.default_adoption=true
MUTATIONS
# Valid JSON padded with whitespace: exact report cap passes, one byte over refuses 125.
cp "$panel/measured.json" "$panel/results/report.json"
bytes=$(stat -c %s "$panel/results/report.json")
((bytes<524288)) || exit 1
printf '%*s' "$((524288-bytes))" '' >> "$panel/results/report.json"
/bin/bash "$panel/check.sh" > "$panel/report-at-cap.out" 2>&1
printf ' ' >> "$panel/results/report.json"
set +e
/bin/bash "$panel/check.sh" > "$panel/report-over-cap.out" 2>&1
rc=$?
set -e
printf '%s\n' "$rc" > "$panel/report-over-cap.exit"
[[ $rc == 125 ]] || exit 1
# Exact smoke source under a single root rewrite; PATH AWS spy cannot hit network.
mkdir -m 700 "$ev/aws-bin" "$ev/aws" "$ev/smoke-root" "$ev/smoke-root/config" "$ev/smoke-root/evidence"
cp "$root/config/candidate-generation.json" "$ev/smoke-root/config/candidate-generation.json"
printf '%s\n' "$(( $(date +%s)+1000 ))" > "$ev/smoke-root/machine-stop.epoch"
sed "s@^root=/mnt/borsuk-v9-panel\$@root=$ev/smoke-root@" "$root/smoke-v9-object.sh" > "$ev/smoke-source.sh"
[[ $(grep -Fxc "root=$ev/smoke-root" "$ev/smoke-source.sh") == 1 ]] || exit 1
if grep -Fxq 'root=/mnt/borsuk-v9-panel' "$ev/smoke-source.sh"; then exit 1; fi
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
sha256sum --strict -c "$ev/source.sha256" > "$ev/source-after.log"
jq -n '{schema:"borsuk-v9-cold-fault-canary-v1",status:"FAULT_CANARY_PASS",runtime_script_mutation_refused:true,next_stage_reserve_boundaries_verified:true,fixed_panel_report_predicate_verified:true,result_config_report_caps_verified:true,smoke_failure_cleanup_preserves_original:true,aws_mocked:true,native_actors_executed:false,scientific_bodies_opened:false,performance_claim:false}' > "$root/evidence/cold-fault-canary.json"
