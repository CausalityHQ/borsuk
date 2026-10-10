#!/usr/bin/env bash
# Synthetic source-bound checks only; no native executable/corpus/truth.
set -Eeuo pipefail
umask 077
export LC_ALL=C
recipe=${1:?}; out=${2:?}
[[ $# == 2 && $recipe == /* && $out == /* && ! -e $out && ! -L $out ]] || exit 125
recipe_sha=ee8327efb4f744d5acc6d8b2665867ca1a3f8a80908e45a83b0a650168baad65
[[ $(sha256sum "$recipe" | cut -d' ' -f1) == "$recipe_sha" ]] || exit 125
mkdir "$out"
cp "$recipe" "$out/recipe.sh"
[[ $(sha256sum "$out/recipe.sh" | cut -d' ' -f1) == "$recipe_sha" ]] || exit 125
recipe=$out/recipe.sh
for name in die canonical sha_of authenticate descriptor small_json one_object auth_art cfg authenticate_admission_closure; do
    awk -v name="$name" 'index($0,name"() {")==1 {on=1;n++;single=(substr($0,length($0),1)=="}")} on {print} on && ($0=="}" || single) {on=0;single=0} END {if(n!=1 || on) exit 1}' "$recipe" >> "$out/functions.sh"
done
awk '/^readonly JQ_TYPES=/ {on=1;n++} on {print} on && /;\047$/ {on=0} END {if(n!=1 || on) exit 1}' "$recipe" > "$out/types.sh"
awk '/^payload_fs_device=/ {on=1;n++} on {print;lines++} on && /^\[\[ \$payload_fs_device/ {on=0} END {if(n!=1 || on || lines!=3) exit 1}' "$recipe" > "$out/filesystem-guard.sh"
cat > "$out/evaluate.sh" <<'EVAL'
set -Eeuo pipefail
export LC_ALL=C
source "$1/functions.sh"
source "$1/types.sh"
recipe_sha=$2; config_copy=$3/config.json
campaign_evidence_root=$3/campaign; evidence=$3/evaluation
authenticate_admission_closure "$campaign_evidence_root/admission/admission.json"
EVAL
cat > "$out/evaluate-fs.sh" <<'EVAL'
set -Eeuo pipefail
source "$1/functions.sh"
ROOT=$2/payload; campaign_evidence_root=$2/evidence
source "$1/filesystem-guard.sh"
EVAL
# Builders contain fixture data, never a mirrored acceptance predicate.
artifact() {
    jq -cn --arg path "$1" --argjson bytes "$(stat -c %s "$1")" --arg sha "$(sha256sum "$1" | cut -d' ' -f1)" '{path:$path,bytes:$bytes,sha256:$sha}'
}
build_case() {
    local name=$1 d=$out/$1 unit=borsuk-pid128-observer-fixture.service id=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa cfg_sha
    mkdir "$d" "$d/campaign" "$d/campaign/admission" "$d/evaluation"
    local a=$d/campaign/admission cg=/system.slice/$unit
    cfg_sha=$(printf '%064d' 0)
    jq -n --arg c "$cfg_sha" '{config_sha256:$c}' > "$a/admission.json"
    jq -n --arg id "$id" --arg cg "$cg" '{schema:"borsuk-native-pid128-observer-identity-v1",invocation_id:$id,control_group:$cg}' > "$a/observer.identity.json"
    for key in initial before-admission final; do
        jq -cn --arg path "/sys/fs/cgroup$cg" '{path:$path}' > "$a/observer.$key"
    done
    if [[ $name == wrong_snapshot ]]; then
        printf '%s\n' '{"path":"/sys/fs/cgroup/wrong"}' > "$a/observer.before-admission"
    fi
    jq -n --arg c "$cfg_sha" '{schema:"borsuk-native-pid128-terminal-v1",status:"SOURCE_ADMITTED_ROOT_GATE_REQUIRED",stage:"closure",original_exit:0,intended_exit:0,cleanup_exit:0,config_sha256:$c,original_observer_manager_and_outer_exits_required:true,performance_claim:false,cold_claim:false,competitor_claim:false,one_million_claim:false}' > "$a/terminal.json"
    printf '0\n' > "$a/wrapper.exit"
    (cd "$a" && find . -type f ! -path ./closure.sha256 ! -path ./wrapper.exit -print | sort | xargs sha256sum) > "$a/closure.sha256"
    case $name in
      unrelated_observer) unit=borsuk-pid128-observer-other.service; id=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb; cg=/system.slice/$unit ;;
      wrong_invocation) id=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb ;;
      nested_wrapper) mkdir "$a/nested"; printf '0\n' > "$a/nested/wrapper.exit" ;;
      nested_closure) mkdir "$a/nested"; printf 'extra\n' > "$a/nested/closure.sha256" ;;
    esac
    printf 'Id=%s\nDescription=%s\nInvocationID=%s\nControlGroup=%s\nResult=success\nExecMainCode=1\nExecMainStatus=0\n' "$unit" "$unit" "$id" "$cg" > "$d/campaign/manager.show"
    if [[ $name == manager_duplicate ]]; then printf 'ExecMainStatus=0\n' >> "$d/campaign/manager.show"; fi
    if [[ $name == manager_failed ]]; then sed -i 's/Result=success/Result=exit-code/' "$d/campaign/manager.show"; fi
    printf '0\n' > "$d/campaign/outer.exit"
    printf 'populated 0\n' > "$d/campaign/drain.events"
    if [[ $name == wrong_outer_exit ]]; then printf '1\n' > "$d/campaign/outer.exit"; fi
    if [[ $name == populated_one ]]; then printf 'populated 1\n' > "$d/campaign/drain.events"; fi
    artifact "$a/admission.json" > "$d/admission.art.json"
    artifact "$a/terminal.json" > "$d/terminal.art.json"
    artifact "$a/closure.sha256" > "$d/manifest.art.json"
    artifact "$a/wrapper.exit" > "$d/wrapper.art.json"
    artifact "$d/campaign/manager.show" > "$d/manager.art.json"
    artifact "$d/campaign/outer.exit" > "$d/exit.art.json"
    artifact "$d/campaign/drain.events" > "$d/drain.art.json"
    jq -n --arg recipe "$recipe_sha" --arg cfg "$cfg_sha" --arg unit "$unit" --arg id "$id" --arg cg "$cg" \
      --slurpfile a "$d/admission.art.json" --slurpfile t "$d/terminal.art.json" --slurpfile m "$d/manifest.art.json" --slurpfile w "$d/wrapper.art.json" \
      --slurpfile manager "$d/manager.art.json" --slurpfile exit "$d/exit.art.json" --slurpfile drain "$d/drain.art.json" \
      '{schema:"borsuk-native-pid128-outer-closure-v1",status:"CLOSED",recipe_sha256:$recipe,config_sha256:$cfg,admission_sha256:$a[0].sha256,terminal_sha256:$t[0].sha256,manifest_sha256:$m[0].sha256,wrapper_exit_sha256:$w[0].sha256,unit:$unit,invocation_id:$id,control_group:$cg,manager_show:$manager[0],outer_exit_file:$exit[0],drain_events:$drain[0],actual_outer_exit:0,populated_zero:true}' > "$d/campaign/outer.json"
    if [[ $name == multi_document_outer ]]; then printf '{}\n' >> "$d/campaign/outer.json"; fi
    artifact "$d/campaign/outer.json" > "$d/outer.art.json"
    jq -n --slurpfile a "$d/admission.art.json" --slurpfile t "$d/terminal.art.json" --slurpfile m "$d/manifest.art.json" --slurpfile w "$d/wrapper.art.json" --slurpfile o "$d/outer.art.json" \
      '{admission:$a[0],admission_closure:{terminal:$t[0],manifest:$m[0],wrapper_exit:$w[0],outer:$o[0]}}' > "$d/config.json"
}
passed=0
for name in positive unrelated_observer wrong_invocation wrong_snapshot nested_wrapper nested_closure manager_duplicate manager_failed wrong_outer_exit populated_one multi_document_outer; do
    build_case "$name"
    d=$out/$name; rc=0
    bash "$out/evaluate.sh" "$out" "$recipe_sha" "$d" > "$d/check.stdout" 2> "$d/check.stderr" || rc=$?
    printf '%s\n' "$rc" > "$d/check.exit"
    case $name in
      positive) ((rc==0)) || exit 1 ;;
      unrelated_observer|wrong_invocation) ((rc==1)) && grep -Fx false "$d/evaluation/admission-observer-identity.validated" || exit 1 ;;
      wrong_snapshot) ((rc==1)) && grep -Fx false "$d/evaluation/admission-observer-before-admission.validated" || exit 1 ;;
      nested_wrapper|nested_closure)
        ((rc==1)) || exit 1
        ! cmp -s "$d/evaluation/admission-closure.sorted" "$d/evaluation/admission-closure.actual" || exit 1
        if [[ $name == nested_wrapper ]]; then tail_name=wrapper.exit; else tail_name=closure.sha256; fi
        grep -Fx "./nested/$tail_name" "$d/evaluation/admission-closure.actual" || exit 1
        ! grep -Fx "./nested/$tail_name" "$d/evaluation/admission-closure.sorted" || exit 1 ;;
      manager_duplicate|manager_failed) ((rc==98)) && grep -Fx 'INVALID: original observer manager failure' "$d/check.stderr" || exit 1 ;;
      wrong_outer_exit) ((rc==98)) && grep -Fx 'INVALID: actual enclosing observer exit' "$d/check.stderr" || exit 1 ;;
      populated_one) ((rc==98)) && grep -Fx 'INVALID: observer drain evidence' "$d/check.stderr" || exit 1 ;;
      multi_document_outer) ((rc==98)) && grep -Fx 'INVALID: exactly one JSON object required' "$d/check.stderr" || exit 1 ;;
    esac
    passed=$((passed+1))
done
for name in same_device different_device first_stat_nonzero second_stat_nonzero; do
    d=$out/$name; mkdir "$d" "$d/payload" "$d/evidence" "$d/spy"
    if [[ $name != same_device ]]; then
        cat > "$d/spy/stat" <<'SPY'
#!/usr/bin/env bash
[[ $# == 4 && $1 == -c && $2 == %d && $3 == -- ]] || exit 97
case $4 in
  "$FS_CASE/payload") printf '100\n'; printf '100\n' > "$FS_CASE/spy.payload"; [[ $FS_NAME != first_stat_nonzero ]] || exit 7 ;;
  "$FS_CASE/evidence") if [[ $FS_NAME == different_device ]]; then printf '101\n'; printf '101\n' > "$FS_CASE/spy.evidence"; else printf '100\n'; printf '100\n' > "$FS_CASE/spy.evidence"; fi; [[ $FS_NAME != second_stat_nonzero ]] || exit 8 ;;
  *) exit 97 ;;
esac
SPY
        chmod 0500 "$d/spy/stat"
    fi
    rc=0
    FS_CASE=$d FS_NAME=$name PATH="$d/spy:$PATH" bash "$out/evaluate-fs.sh" "$out" "$d" > "$d/check.stdout" 2> "$d/check.stderr" || rc=$?
    printf '%s\n' "$rc" > "$d/check.exit"
    if [[ $name != same_device ]]; then
        grep -Fx 100 "$d/spy.payload"
        if [[ $name == different_device ]]; then grep -Fx 101 "$d/spy.evidence"; fi
        if [[ $name == second_stat_nonzero ]]; then grep -Fx 100 "$d/spy.evidence"; fi
        if [[ $name == first_stat_nonzero ]]; then [[ ! -e $d/spy.evidence ]]; fi
    fi
    case $name in
      same_device) ((rc==0)) || exit 1 ;;
      different_device) ((rc==98)) && grep -Fx 'INVALID: payload and evidence must share filesystem' "$d/check.stderr" || exit 1 ;;
      first_stat_nonzero) ((rc==98)) && grep -Fx 'INVALID: payload filesystem identity unavailable' "$d/check.stderr" || exit 1 ;;
      second_stat_nonzero) ((rc==98)) && grep -Fx 'INVALID: evidence filesystem identity unavailable' "$d/check.stderr" || exit 1 ;;
    esac
    passed=$((passed+1))
done
jq -n --argjson passed "$passed" '{status:"SYNTHETIC_CLOSURE_CHECKS_ONLY",passed:$passed,native_executed:false,corpus_or_truth_opened:false,production_qualification:false,performance_claim:false}' > "$out/result.json"
sync -f "$out"
