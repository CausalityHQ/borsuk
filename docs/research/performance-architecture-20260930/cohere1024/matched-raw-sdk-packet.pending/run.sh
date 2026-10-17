#!/usr/bin/bash
# Parent executes each mode once; native ownership receipts govern deletion.
set -euo pipefail
umask 077
export PATH=/usr/bin:/bin
packet=$(cd -- "$(dirname -- "$0")" && pwd -P)
mode=${1:?usage: run.sh gates|full|cleanup-canary|cleanup-full|verify-canary|verify-full}
case "$mode" in gates|full|cleanup-canary|cleanup-full|verify-canary|verify-full) ;; *) exit 64;; esac
work=/mnt/borsuk-workspace
root=$work/evidence/matched-raw-sdk-experiment-a0001
bin=$work/artifacts/s3vectors-original-raw-release-a0003/check_s3_vectors_baseline
aws=$work/aws/dist/aws
full=$packet/full.json
read -r full_sha < "$packet/full.sha256"
read -r canary_sha < "$packet/canary.sha256"
state=$root/full-state
canary=$state/scale-canary/config.json
cfg=$full
sha=$full_sha
case "$mode" in *canary|gates) cfg=$canary; sha=$canary_sha;; esac

# This does not admit payload bytes: the native admit command does that below.
python3 -I - "$packet" "$mode" <<'PY'
import hashlib,json,pathlib,subprocess,sys,time
p=pathlib.Path(sys.argv[1]); mode=sys.argv[2]
j=json.loads((p/'protocol.json').read_bytes())
subprocess.run(['sha256sum','--check','--strict','manifest.sha256'],cwd=p,check=True)
repo=pathlib.Path(subprocess.check_output(['git','-C',str(p),'rev-parse','--show-toplevel'],text=True).strip())
assert hashlib.sha256((repo/'crates/borsuk/src/bin/check_s3_vectors_baseline.rs').read_bytes()).hexdigest()==j['source_sha256']
with open(j['binary'],'rb') as f:
 assert f.read(4)==b'\x7fELF'; f.seek(0)
 assert hashlib.file_digest(f,'sha256').hexdigest()==j['binary_sha256']
with open(j['aws_cli'],'rb') as f:
 assert hashlib.file_digest(f,'sha256').hexdigest()==j['aws_cli_sha256']
assert pathlib.Path('/sys/devices/virtual/dmi/id/board_asset_tag').read_text().strip()==j['host']
cg=[x.split(':',2)[2] for x in pathlib.Path('/proc/self/cgroup').read_text().splitlines() if x.startswith('0::')]
assert len(cg)==1 and '..' not in pathlib.Path(cg[0]).parts
base=pathlib.Path('/sys/fs/cgroup'+cg[0])
for name,value in [('memory.max','536870912'),('memory.swap.max','0'),('pids.max','128'),('cpuset.cpus.effective','12')]:
 assert (base/name).read_text().strip()==value,(name,'external resource guard')
quota,period=(base/'cpu.max').read_text().split()
assert quota!='max' and int(quota)==int(period),'CPUQuota=100% required'
kind='gates' if mode=='gates' else 'full' if mode=='full' else 'cleanup' if mode.startswith('cleanup') else 'verify'
runtime=j['runtime_seconds'][kind]; stop=j['stop_seconds'][kind]
assert int(time.time())+runtime+stop<=j['guest_horizon_epoch'],'BLOCKED: finite run plus stop/cleanup cannot fit frozen host horizon'
f=json.loads((p/'full.json').read_bytes())
assert hashlib.sha256((p/'full.json').read_bytes()).hexdigest()==j['full_config_sha256']
assert hashlib.sha256((p/'canary.expected.json').read_bytes()).hexdigest()==j['canary_config_sha256']
co=json.loads((p/'cohort.json').read_bytes()); d=json.loads((p/'derivation.json').read_bytes())
n=json.loads((p/'normalization.json').read_bytes()); r=json.loads((p/'rust48-bindings.json').read_bytes())['expected_bound_inputs']
assert co['status']=='COMPLETE' and co['geometry']['corpus_rows']==1000000
assert co['geometry']['query_rows']==1000 and co['geometry']['dimensions']==1024 and co['geometry']['k']==10
for key,name in [('corpus','corpus.f32'),('queries','queries.f32'),('truth','truth.u64')]:
 a=f[key]; o=next(x for x in co['outputs'] if x['name']==name)
 assert (a['bytes'],a['sha256'])==(o['bytes'],o['sha256'])
 path=pathlib.Path(a['path'])
 if mode in ('gates','full'):
  assert not path.is_symlink() and path.is_file() and path.stat().st_size==a['bytes']
assert d['status']=='COMPLETE' and d['query_or_truth_used'] is False
assert d['original_corpus']['sha256']==f['corpus']['sha256']==n['raw_sha256']
assert d['outputs']['normalized']['sha256']==n['normalized_sha256']==r['native_source_sha256']
assert n['status']=='VERIFIED_EXISTING_NATIVE_EXECUTION' and n['original_native_exit']==0
assert n['raw_and_executable_stamps_unchanged'] and n['output_independent_sha256_readback']
assert r['semantic_leaf_limit']==48 and r['rows']==1000000 and r['count']==1000
assert r['requests_sha256']==f['queries']['sha256'] and r['truth_sha256']==f['truth']['sha256']
assert r['corpus_intervals']==co['geometry']['corpus_intervals']==d['corpus_intervals']
assert r['reserved_query_interval']==co['geometry']['reserved_query_interval']
q=json.loads((p/'qualification.json').read_bytes())
assert q['source']['sha256']==j['source_sha256'] and q['release_binary']['sha256']==j['binary_sha256']
assert q['source']['head']==j['qualified_sdk_commit']
assert all(x['exit_code']==0 for x in q['native_qualification']['stages'])
print('Immutable metadata and execution envelope verified; payload admission remains native.')
PY
export AWS_CONFIG_FILE=$packet/aws.config AWS_SHARED_CREDENTIALS_FILE=/dev/null
export AWS_PROFILE=causality AWS_REGION=eu-central-1 AWS_DEFAULT_REGION=eu-central-1
export AWS_MAX_ATTEMPTS=1 AWS_RETRY_MODE=standard AWS_PAGER=
unset AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN AWS_SECURITY_TOKEN
unset AWS_EC2_METADATA_DISABLED AWS_ENDPOINT_URL AWS_ENDPOINT_URL_S3VECTORS AWS_ENDPOINT_URL_S3_VECTORS AWS_ENDPOINT_URL_STS
unset AWS_WEB_IDENTITY_TOKEN_FILE AWS_ROLE_ARN AWS_CONTAINER_CREDENTIALS_RELATIVE_URI AWS_CONTAINER_CREDENTIALS_FULL_URI
mkdir -p "$root"
exec 9>"$root/owner.lock"
flock -n 9
logs=$root/$mode
mkdir "$logs"
printf 'NOT_RUN\n' > "$logs/measurement.exit"
printf 'NOT_RUN\n' > "$logs/cleanup.exit"
printf 'NOT_RUN\n' > "$logs/drain.exit"
active=
active_label=
publish_attempted=0
drain_rc=0
interrupted=0

# GNU timeout owns a fresh process group. Cleanup waits for that whole group.
drain() {
    local pid=$1 i
    if kill -0 "$pid" 2>/dev/null || kill -0 -- "-$pid" 2>/dev/null; then
        kill -TERM -- "-$pid" 2>/dev/null || true
        kill -TERM "$pid" 2>/dev/null || true
        for ((i=0; i<50; i++)); do
            if ! kill -0 "$pid" 2>/dev/null && ! kill -0 -- "-$pid" 2>/dev/null; then return 0; fi
            sleep 0.1
        done
        kill -KILL -- "-$pid" 2>/dev/null || true
        kill -KILL "$pid" 2>/dev/null || true
        for ((i=0; i<50; i++)); do
            if ! kill -0 "$pid" 2>/dev/null && ! kill -0 -- "-$pid" 2>/dev/null; then return 0; fi
            sleep 0.1
        done
        return 70
    fi
}
phase() {
    local label=$1 seconds=$2 rc=0
    shift 2
    (( interrupted == 0 )) || exit "$interrupted"
    active_label=$label
    printf '%q ' timeout --signal=TERM --kill-after=5s "$seconds"s "$@" <&0 > "$logs/$label.argv"
    printf '\n' >> "$logs/$label.argv"
    date -u +%FT%TZ > "$logs/$label.started"
    timeout --signal=TERM --kill-after=5s "$seconds"s "$@" <&0 > "$logs/$label.stdout" 2> "$logs/$label.stderr" &
    active=$!
    (( interrupted == 0 )) || exit "$interrupted"
    printf '%s\n' "$active" > "$logs/$label.pid"
    wait "$active" || rc=$?
    (( interrupted == 0 )) || exit "$interrupted"
    drain "$active" || drain_rc=$?
    active=
    printf '%s\n' "$rc" > "$logs/$label.exit"
    printf '%s\n' "$drain_rc" > "$logs/drain.exit"
    if [[ "$label" == measure ]]; then printf '%s\n' "$rc" > "$logs/measurement.exit"; fi
    (( drain_rc == 0 )) || return "$drain_rc"
    return "$rc"
}
finish() {
    local primary=$? cleanup=NOT_RUN final
    if (( interrupted != 0 )); then primary=$interrupted; fi
    interrupted=0
    trap - EXIT
    trap ':' INT TERM
    set +e
    if [[ -n "$active" ]]; then
        if drain "$active"; then
            # Successful disappearance makes reaping the original handle immediate.
            wait "$active"
            printf '%s\n' "$?" > "$logs/$active_label.exit"
        else
            drain_rc=$?
            printf 'UNREAPED_DRAIN_FAILED\n' > "$logs/$active_label.exit"
        fi
        if [[ "$active_label" == measure ]]; then cp "$logs/measure.exit" "$logs/measurement.exit"; fi
        active=
        printf '%s\n' "$drain_rc" > "$logs/drain.exit"
    fi
    if (( publish_attempted && drain_rc == 0 )); then
        phase cleanup 600 "$bin" cleanup "$cfg" "$sha"
        cleanup=$?
    elif [[ "$mode" == cleanup-* ]]; then
        cleanup=$primary
    fi
    printf '%s\n' "$primary" > "$logs/pipeline.exit"
    printf '%s\n' "$cleanup" > "$logs/cleanup.exit"
    final=$primary
    if (( final == 0 && drain_rc != 0 )); then final=$drain_rc; fi
    if (( final == 0 )) && [[ "$cleanup" != NOT_RUN ]]; then final=$cleanup; fi
    printf '%s\n' "$final" > "$logs/runner.exit"
    printf 'pipeline=%s measurement=%s cleanup=%s drain=%s runner=%s\n' \
        "$primary" "$(cat "$logs/measurement.exit")" "$cleanup" "$drain_rc" "$final"
    exit "$final"
}
trap finish EXIT
trap 'interrupted=130' INT
trap 'interrupted=143' TERM

receipt_check() {
    python3 -I - "$packet" "$1" <<'PY'
import json,pathlib,sys
p=pathlib.Path(sys.argv[1]); action=sys.argv[2]
j=json.loads((p/'protocol.json').read_bytes()); root=pathlib.Path(j['execution_root'])
full=json.loads((p/'full.json').read_bytes()); canary=json.loads((p/'canary.expected.json').read_bytes())
def receipt(c,sha,name):
 r=json.loads((pathlib.Path(c['state_dir'])/name).read_bytes())
 assert r['schema']=='borsuk-s3-vectors-receipt-v1' and r['kind']==name and r['config_sha256']==sha
 return r['data']
fs=j['full_config_sha256']; cs=j['canary_config_sha256']
if action=='prepared':
 assert (pathlib.Path(full['state_dir'])/'scale-canary/config.json').read_bytes()==(p/'canary.expected.json').read_bytes()
 a=receipt(full,fs,'admission.json'); assert a['truth_validated'] is True
 a=receipt(full,fs,'scale-canary-prepared.json')
 assert a['source_config_sha256']==fs and a['config_sha256']==cs and a['logical_ids_preserved'] is True
elif action=='canary':
 a=receipt(canary,cs,'published.json')
 assert a['rows']==a['enumerated_keys']==a['bit_exact_readback_rows']==257
 a=receipt(canary,cs,'reduction.json')
 assert a['status']=='CANARY_VALID' and a['claims_quality'] is False and a['claims_performance'] is False
elif action=='full-gates':
 for label in ['full-admit','prepare','canary-admit','publish','measure','reduce','measurement','cleanup','drain','pipeline','runner']:
  assert (root/'gates'/(label+'.exit')).read_text().strip()=='0',label
 a=receipt(canary,cs,'cleanup.json')
 assert a['bucket_absence_verified'] is True and a['index_absence_verified'] is True
 a=json.loads((root/'verify-canary/absence.json').read_text())
 assert a=={'config_sha256':cs,'bucket':canary['bucket'],'index':canary['index'],'bucket_absent':True,'index_absent':True}
 assert (root/'verify-canary/runner.exit').read_text().strip()=='0'
 for name in ['publish.jsonl','published.json','bucket-create-intent.json','responses.jsonl','measure-started.json','calls.jsonl','cleanup.json']:
  assert not (pathlib.Path(full['state_dir'])/name).exists(),'full attempt already consumed'
elif action=='quality':
 a=receipt(full,fs,'reduction.json')
 assert a['status']=='VALID' and a['query_count']==1000 and a['recall_denominator']==10000
 assert a['all_queries_sealed'] is True and a['truth_opened_after_seal'] is True
 print(json.dumps({'sdk_hits':a['hits'],'denominator':10000,'rust48_reference_hits':9783,
  'sdk_minus_reference_hits':a['hits']-9783,'vendor_win_claim':False}))
PY
}
case "$mode" in
gates)
    mkdir "$state"
    phase full-admit 300 "$bin" admit "$full" "$full_sha"
    phase prepare 600 "$bin" prepare-scale-canary "$full" "$full_sha"
    receipt_check prepared
    phase canary-admit 60 "$bin" admit "$canary" "$canary_sha"
    publish_attempted=1
    phase publish 120 "$bin" publish "$canary" "$canary_sha"
    phase measure 120 "$bin" measure "$canary" "$canary_sha"
    phase reduce 120 "$bin" reduce "$canary" "$canary_sha"
    receipt_check canary
    ;;
full)
    receipt_check full-gates
    publish_attempted=1
    phase publish 7200 "$bin" publish "$full" "$full_sha"
    phase idle 3610 python3 -I - "$state/published.json" "$full_sha" <<'PY'
import json,pathlib,sys,time
r=json.loads(pathlib.Path(sys.argv[1]).read_bytes())
assert r['schema']=='borsuk-s3-vectors-receipt-v1' and r['kind']=='published.json' and r['config_sha256']==sys.argv[2]
t=r['data']['verified_at_ms']; now=time.time_ns()//1000000
assert type(t) is int and 0<t<=now
time.sleep(max(0,(t+3600000-now)/1000))
PY
    phase measure 1860 "$bin" measure "$full" "$full_sha"
    phase reduce 120 "$bin" reduce "$full" "$full_sha"
    receipt_check quality > "$logs/quality-comparison.json"
    ;;
cleanup-*)
    # External recovery is explicit and single-use; it never resumes measurement.
    phase cleanup 600 "$bin" cleanup "$cfg" "$sha"
    ;;
verify-*)
    # Independent read-only service closure; never synthesize ownership receipts.
    mapfile -t names < <(python3 -I - "$cfg" <<'PY'
import json,sys
c=json.load(open(sys.argv[1]))
print(c['bucket'])
print(c['index'])
print('arn:aws:s3vectors:'+c['region']+':'+c['account_id']+':bucket/'+c['bucket']+'/index/'+c['index'])
PY
)
    [[ ${#names[@]} == 3 ]]
    for what in bucket index; do
        if [[ "$what" == bucket ]]; then
            args=(get-vector-bucket --vector-bucket-name "${names[0]}")
        else
            args=(get-index --index-arn "${names[2]}")
        fi
        rc=0
        phase "probe-$what" 40 "$aws" --profile causality --region eu-central-1 \
            --cli-connect-timeout 5 --cli-read-timeout 30 s3vectors "${args[@]}" || rc=$?
        [[ "$rc" == 254 && "$drain_rc" == 0 ]]
        grep -q 'An error occurred (ResourceNotFoundException)' "$logs/probe-$what.stderr"
    done
    python3 -I - "$logs/absence.json" "$sha" "${names[0]}" "${names[1]}" <<'PY'
import json,sys
with open(sys.argv[1],'x') as f:
 json.dump(dict(config_sha256=sys.argv[2],bucket=sys.argv[3],index=sys.argv[4],bucket_absent=True,index_absent=True),f)
 f.write('\n')
PY
    ;;
esac
