#!/bin/bash
set -euo pipefail
mode=$1 prep=$2 gates=$3 out=$4
[[ $mode == gates || $mode == run ]]
if [[ $mode == run ]]; then
[[ $(cat "$gates/terminal") == PASS && $(cat "$gates/manager.exit") == 0 ]]
for stage in admission smoke; do
 for arm in A B; do
  for suffix in exit wrapper.exit native.exit timeout.exit drain.exit cleanup.exit resource.exit; do
   [[ $(cat "$gates/$stage.$arm.$suffix") == 0 ]]
  done
 done
done
[[ $(cat "$gates/checkpoint-probe.checkpoint.exit") == 0 ]]
cmp "$prep/SHA256SUMS" "$gates/preparation.sha256"
fi
(cd "$prep" && sha256sum -c SHA256SUMS > /dev/null && sha256sum -c artifacts.sha256 > /dev/null && sha256sum -c environment.sha256 > /dev/null)
source "$prep/guard.sh"
guard 2400
exec 9>"$prep/owned.lock"
flock -n 9
if pgrep -f '(^|/)(check_cohere_[^ ]*|compare_native_replay|cargo|rustc)( |$)' >/dev/null; then exit 70; fi
mkdir "$out"
exec >"$out/manager.stdout" 2>"$out/manager.stderr"
trap 'rc=$?; printf "%s\n" "$rc" > "$out/manager.exit"; if ((rc==0)); then if [[ $mode == gates ]]; then echo PASS; else echo ABBA_LATENCY_SCREEN_CLOSED; fi > "$out/terminal"; else echo EXECUTION_INVALID > "$out/terminal"; fi' EXIT
source /mnt/borsuk-workspace/environment.sh
export AWS_CONFIG_FILE=/mnt/borsuk-workspace/evidence/s3vectors-full-cell-a0002/native-profile/config
export AWS_SHARED_CREDENTIALS_FILE=/dev/null AWS_PROFILE=causality AWS_SDK_LOAD_CONFIG=1 AWS_MAX_ATTEMPTS=1
unset AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN
cp -a "$prep/artifacts" "$out/artifacts"
cp "$prep/SHA256SUMS" "$out/preparation.sha256"
cp "$prep/header-authority.json" "$out/header-authority.json"
cp "$prep/resource-authority.json" "$out/resource-authority.json"
cp "$prep/protocol.md" "$out/protocol.md"
cp "$prep/config-pins.unselected.json" "$out/config-pins.json"
cp -a "$prep/configs" "$prep/frozen-headers" "$out/"
sha256sum "$out/artifacts/"* > "$out/artifacts.sha256"
python3 - "$out" <<'PY'
import json,pathlib,sys,hashlib
p=pathlib.Path(sys.argv[1]);a=json.loads((p/'header-authority.json').read_bytes())
for arm in ('A','B'):
 assert hashlib.sha256((p/'artifacts'/f'check_cohere_native_baseline.{arm}').read_bytes()).hexdigest()==a['binary_sha256'][arm]
assert hashlib.sha256((p/'artifacts/compare_native_replay').read_bytes()).hexdigest()==a['strict_reader_sha256']
for row in json.loads((p/'config-pins.json').read_bytes())['configs']:
 b=(p/'configs'/row['name']).read_bytes()
 assert len(b)==row['bytes'] and hashlib.sha256(b).hexdigest()==row['sha256']
PY
if [[ $mode == gates ]]; then stages="admission smoke"; else stages="A1 B1 B2 A2"; fi
for stage in $stages; do
 case "$stage" in admission|smoke) arms="A B";; A1|A2) arms=A;; B1|B2) arms=B;; esac
 for arm in $arms; do
  cell="$stage.$arm"
  cfgstage=$stage
  case "$stage" in A1|B1|B2|A2) cfgstage=full;; esac
  cfg="$out/configs/$cfgstage.$arm.json"
  sha=$(sha256sum "$cfg"); sha=${sha%% *}
  case "$stage" in admission) seconds=60;; smoke) seconds=90;; A1|B1|B2|A2) seconds=450;; esac
  printf '%q ' "$out/artifacts/check_cohere_native_baseline.$arm" "$cfg" "$sha" "$out/$cell.jsonl" > "$out/$cell.command"
  guard "$seconds"
  set +e
  NATIVE_EXIT="$out/$cell.native.exit" /usr/bin/time -v -o "$out/$cell.resources" timeout --signal=TERM --kill-after=5s "$seconds" bash -c '"$@"; rc=$?; printf "%s\n" "$rc" >"$NATIVE_EXIT"; exit "$rc"' _ "$out/artifacts/check_cohere_native_baseline.$arm" "$cfg" "$sha" "$out/$cell.jsonl" >"$out/$cell.stdout" 2>"$out/$cell.stderr"
  rc=$?
  set -e
  printf '%s\n' "$rc" >"$out/$cell.exit"
  echo "$rc" >"$out/$cell.wrapper.exit"
  echo 0 >"$out/$cell.timeout.exit"
  if ((rc==124 || rc==137)); then echo "$rc" >"$out/$cell.timeout.exit"; fi
  drain=0
  if pgrep -f '(^|/)(check_cohere_[^ ]*|compare_native_replay|cargo|rustc)( |$)' >/dev/null; then drain=71; fi
  echo "$drain" >"$out/$cell.drain.exit"
  cleanup=0
  if ! residue=$(find "$prep/scratch0000" -mindepth 1 -print -quit) || [[ -n $residue ]]; then cleanup=72; fi
  echo "$cleanup" >"$out/$cell.cleanup.exit"
  ((rc==0 && drain==0 && cleanup==0))
  [[ $(cat "$out/$cell.native.exit") == 0 ]]
  set +e
  python3 "$prep/check-resources.py" "$out/$cell.jsonl" "$cfgstage" "$arm"
  resource_rc=$?
  set -e
  echo "$resource_rc" >"$out/$cell.resource.exit"
  ((resource_rc==0))
  if [[ $mode == gates ]]; then
   python3 - "$out/$cell.jsonl" "$cfg" <<'GATE'
import json,sys
with open(sys.argv[1]) as f:
 rows=[json.loads(line) for line in f]
c=json.load(open(sys.argv[2])); t=rows[-1]
assert t['phase']=='terminal'
t=t['summary']; assert t['complete'] and t['all_queries_sealed']
assert t['executed_count']==len(c['execution']['ordinals'])
assert t['requests_sha256']==c['requests']['sha256'] and t['truth_sha256']==c['truth']['sha256']
assert [r['ordinal'] for r in rows if r.get('phase')=='query']==c['execution']['ordinals']
GATE
  fi
  printf '%s NATIVE_TERMINAL0\n' "$cell" >"$out/$cell.terminal"
  if [[ $stage != admission && $stage != smoke ]]; then
   python3 - "$out" "$arm" "$cell" <<'PY'
import json,pathlib,sys,hashlib
p=pathlib.Path(sys.argv[1]);arm=sys.argv[2];cell=sys.argv[3];f=p/f'{cell}.jsonl'
c=json.loads((p/'frozen-headers'/f'{arm}.json').read_bytes())
with f.open('rb') as inp: h=hashlib.file_digest(inp,'sha256').hexdigest()
c['input']={'path':str(f),'bytes':f.stat().st_size,'sha256':h}
with (p/f'{cell}.reader-config.json').open('x') as out:json.dump(c,out,sort_keys=True,indent=2);out.write('\n')
PY
   readercfg="$out/$cell.reader-config.json"
   readersha=$(sha256sum "$readercfg"); readersha=${readersha%% *}
   set +e
   /usr/bin/time -v -o "$out/$cell.reader.resources" timeout --signal=TERM --kill-after=5s 90 "$out/artifacts/compare_native_replay" --completed-scale "$readercfg" "$readersha" "$out/$cell.reader-report.json" >"$out/$cell.reader.stdout" 2>"$out/$cell.reader.stderr"
   rc=$?
   set -e
   printf '%s\n' "$rc" >"$out/$cell.reader.exit"
   echo 0 >"$out/$cell.reader.timeout.exit"
   if ((rc==124 || rc==137)); then echo "$rc" >"$out/$cell.reader.timeout.exit"; fi
   readerdrain=0
   if pgrep -f '(^|/)(compare_native_replay)( |$)' >/dev/null; then readerdrain=71; fi
   echo "$readerdrain" >"$out/$cell.reader.drain.exit"
   ((readerdrain==0))
   ((rc==0))
   bash "$prep/checkpoint.sh" cell "$out" "$cell" "$arm"
  fi
 done
done
if [[ $mode == gates ]]; then bash "$prep/checkpoint.sh" probe "$out"; fi
sync -f "$out"
