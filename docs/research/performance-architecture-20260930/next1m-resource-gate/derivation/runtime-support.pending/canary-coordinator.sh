#!/usr/bin/env bash
# Borsuk next-1M cloud-final canary coordinator (plan R6 s.4-s.5 + root decision, message 1791585172630928753).
# Started by canary-tail.sh as borsuk-canary-coordinator.service (Type=simple, After=cloud-final.service).
# SOURCE ONLY until the root assembles and launches it. No ANN, no native chain, no scientific or performance claim.
# The live cloud-init trees are only READ. Every fixture runs in a unit COPY of cloud-final.service whose BindPaths
# point at disposable copies under /mnt/borsuk-canary-cases. The verdict is PROVISIONAL until the root replays the
# evidence and proves termination and volume deletion.
# shellcheck disable=SC2015,SC2016,SC2329
# (A && B || C is used deliberately as all-must-pass-else-refuse; fin, put and lst are used through a trap and bash -c)
set -Eeuo pipefail
umask 077
export LC_ALL=C PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
root=/mnt/borsuk-scale1m
cases=/mnt/borsuk-canary-cases
res=$cases/results
tev=$root/evidence-root/canary-tail
bucket=borsuk-bench-453182569524-euc1
stop_sha=6d7fdb12b7d7218c778a403ad8fcf3b55ef7b949f1d9c91402f05f870558da6e
wc_pin=bf2cb012c3880c420bf9c3b80334e42a7d1469caa3c339042cde8607d3093a1f
fifo=/run/cloud-init/share/hook-hotplug-cmd
dropin=/run/systemd/system/cloud-final.service.d/borsuk-exit.conf
t0=$EPOCHSECONDS
instance='' prefix='' frag_sha='' frag='' local_stop=0 iid_dir=''
batch_stop=$((t0 + 180))
step_stop=$batch_stop
hard_stop=$((t0 + 3600))
phase=init
sevno=0
V=QUALIFIED_PROVISIONAL
active=''
T=0
reasons=()
declare -A okstep=()
roster=(original.json admission.json listing-summary.json steps.jsonl)
for n in 0 2 3; do roster+=("case-$n.json" "case-$n.manager.json" "case-$n.result.json" "case-$n.status.json"); done

log() { printf 'canary %s %s\n' "$EPOCHSECONDS" "$*"; }
# sev LEVEL REASON: 1 UNQUALIFIED, 2 NOT_RUN, 3 INVALID (the highest level wins; nothing is ever downgraded)
sev() {
 reasons+=("$phase: $2"); log "$phase: $2"
 (( $1 > sevno )) || return 0
 sevno=$1
 case $1 in 1) V=UNQUALIFIED;; 2) V=NOT_RUN;; *) V=INVALID;; esac
}
# tmo CAP: T = min(CAP, step, whole batch, machine stop) - now - 1 (the 1 s is the kill grace); refuse when <= 0
tmo() {
 local lim=$step_stop r
 (( batch_stop < lim )) && lim=$batch_stop
 (( hard_stop < lim )) && lim=$hard_stop
 r=$((lim - EPOCHSECONDS - 1))
 (( r > 0 )) || return 1
 T=$(( $1 < r ? $1 : r ))
}
run() { local cap=$1; shift; tmo "$cap" || return 125; timeout -k 1 "$T" "$@"; }
rec() { jq -cn --arg s "$phase" --argjson t "$EPOCHSECONDS" --arg m "$*" '{step:$s,t:$t,msg:$m}' >> "$res/steps.jsonl"; }
step_begin() { phase=$1; step_stop=$((EPOCHSECONDS + $2)); rec "begin budget=$2"; }
step_end() { okstep[$phase]=1; rec end; }
dl() { tmo 1 || { sev 2 "step deadline reached"; exit 0; }; }
dig() { local d; d=$(run 8 bash -c 'set -o pipefail; lst "$1" "$2" | sha256sum' _ "$1" "${2:-}") || return 1; d=${d%% *}; [[ $d =~ ^[0-9a-f]{64}$ ]] && printf '%s' "$d"; }
sub() { local lim=$((EPOCHSECONDS + $1)); (( case_stop < lim )) && lim=$case_stop; step_stop=$lim; }
case_stop=0
halt=0
reg() { [[ -f $1 && ! -L $1 ]]; }
sha() { local h; h=$(sha256sum < "$1") || return 1; h=${h%% *}; [[ $h =~ ^[0-9a-f]{64}$ ]] || return 1; printf '%s' "$h"; }
chk_list() { [[ -s $1 ]] && grep -qxF 'd .' -- "$1"; }
cg_empty() { [[ ! -d $1 ]] || grep -qx 'populated 0' "$1/cgroup.events" 2> /dev/null || [[ ! -d $1 ]]; }
# Shared edit rule for EVERY path inside a copied tree or a case root: the path lies under a case directory, its parent is a real directory whose
# canonical path is itself (no symlink component anywhere), and a new file is created exclusively; any pre-existing path of any kind,
# including a dangling symlink, is refused. Nothing is ever written through a copied link.
safe_parent() { local d=${1%/*}; [[ $1 == "$cases"/[023]/* && -d $d && ! -L $d && $(realpath -e -- "$d") == "$d" ]]; }
mkfile() { # mkfile MODE PATH (content on stdin)
 safe_parent "$2" && [[ ! -e $2 && ! -L $2 ]] || return 1
 ( set -o noclobber; cat > "$2" ) || return 1
 chmod "$1" -- "$2" && [[ -f $2 && ! -L $2 ]]
}
rmfile() { # rmfile PATH: remove one copied REGULAR file (absent is fine); links and every other type are refused
 safe_parent "$1" || return 1
 if [[ -e $1 || -L $1 ]]; then [[ -f $1 && ! -L $1 ]] || return 1; rm -f -- "$1" || return 1; fi
 [[ ! -e $1 && ! -L $1 ]]
}
bnd() { local c=$cases/$1; printf '%s' "$c/var-lib-cloud:/var/lib/cloud $c/run-cloud-init:/run/cloud-init $c/etc-cloud:/etc/cloud $c/cloud-init.log:/var/log/cloud-init.log $c/cloud-init-output.log:/var/log/cloud-init-output.log $c/root:$root"; }
# relative listing with content hashes (no mtimes). Every stage is checked: errexit + pipefail, no grep filter (the admitted FIFO is
# excluded by find itself when $2 is set), so a failed enumeration/hash can never be masked by the final sort.
lst() {
 set -Eeuo pipefail
 local -a ex=()
 [[ -z ${2:-} ]] || ex=(! \( -path ./share/hook-hotplug-cmd -type p \))
 cd -- "$1" || return 1
 { find . -xdev -type d -printf 'd %p\n' || exit 1
   find . -xdev ! -type f ! -type d "${ex[@]}" -printf '%y %p -> %l\n' || exit 1
   find . -xdev -type f -print0 | sort -z | xargs -0 -r sha256sum || exit 1; } | sort
}
export -f lst

IFS= read -r -d '' probe_py <<'EOF' || true
import json,re
from cloudinit import stages
from cloudinit.config.modules import Modules
i=stages.Init(ds_deps=[])
i.read_cfg(None)
c=Modules(i,None).cfg
s=json.dumps([c.get("log_cfgs"),c.get("output"),c.get("def_log_file")])
print(json.dumps({"modules":c.get("cloud_final_modules"),"paths":sorted(set(re.findall(r"/[A-Za-z0-9_./-]+",s)))}))
EOF

# drain N: stop the OWNED unit, kill only its exact cgroup subtree, require cgroup.events populated 0 (whole subtree); the actual hook record
# is read BEFORE reset-failed
drain() {
 local u=borsuk-canary-final-$1.service cg=/sys/fs/cgroup/system.slice/borsuk-canary-final-$1.service st
 run 6 systemctl stop "$u" || true
 if [[ -d $cg ]]; then
  [[ -w $cg/cgroup.kill ]] && printf '1' > "$cg/cgroup.kill"
  for _ in 1 2 3 4 5 6; do if cg_empty "$cg"; then break; fi; sleep 0.5; done
 fi
 run 3 systemctl show "$u" -p ExecStopPost -p ActiveState -p Result > "$res/esp.$1.txt" || true
 run 3 systemctl reset-failed "$u" || true
 st=$(run 3 systemctl show "$u" -p ActiveState --value) || return 1
 [[ $st == inactive || $st == failed ]] && cg_empty "$cg"
}

put() { # put NAME FILE: create-only publication of one allowlisted small file; the exit status is recorded
 local rc=0
 if reg "$2" && (( $(stat -c %s "$2") <= 65536 )); then
  run 20 aws s3api put-object --bucket "$bucket" --key "$prefix/canary/$1" --body "$2" --if-none-match '*' > "$res/put.$1.json" 2> "$res/put.$1.err" || rc=$?
  (( rc != 0 )) || jq -e 'type=="object" and (.ETag|type=="string")' "$res/put.$1.json" > /dev/null || rc=1
 else rc=1; fi
 printf '%s %s\n' "$1" "$rc" >> "$res/c5.rc" || rc=1
 return "$rc"
}

fin() {
 local rc=$? f s w=0 vrc=0 sd=0 xit=0 line
 trap - EXIT; set +e
 step_stop=$((EPOCHSECONDS + 25)); batch_stop=$step_stop; hard_stop=$((local_stop > 0 ? local_stop - 5 : t0 + 3600))
 phase=finalize
 if [[ -n $active ]]; then drain "$active" || sev 3 "exit drain of case $active failed"; fi
 (( rc == 0 )) || sev 3 "coordinator exit status $rc"
 if [[ -d $res && -n $prefix ]]; then
  for s in W C0 C1 case0 case2 case3 C4; do [[ ${okstep[$s]:-} == 1 ]] || sev 2 "step $s did not complete"; done
  for f in "${roster[@]}"; do
   case $f in
    *.json) if [[ -f $res/$f ]]; then jq -e . "$res/$f" > /dev/null || sev 3 "roster file $f is not valid JSON"; fi;;
    *.jsonl) if [[ -f $res/$f ]]; then jq -e -s . "$res/$f" > /dev/null || sev 3 "roster file $f is not valid JSON lines"; fi;;
   esac
  done
  step_stop=$((EPOCHSECONDS + 240)); (( local_stop - 30 < step_stop )) && step_stop=$((local_stop - 30)); batch_stop=$step_stop
  : > "$res/c5.rc" || { w=1; sev 3 "c5.rc is not writable"; }
  for f in "${roster[@]}"; do
   if [[ -f $res/$f ]]; then put "$f" "$res/$f" || { w=1; sev 1 "publication of $f failed"; }
   else printf '%s missing\n' "$f" >> "$res/c5.rc" || w=1; [[ $V != QUALIFIED_PROVISIONAL ]] || sev 3 "roster file $f missing"; fi
  done
  if jq -n --arg v "$V" --arg i "$instance" --argjson t "$EPOCHSECONDS" --argjson w "$w" \
   --argjson ok "$(printf '%s\n' "${!okstep[@]}" | jq -R . | jq -sc 'map(select(length>0))|sort')" \
   --argjson reasons "$(jq -nc '$ARGS.positional' --args -- "${reasons[@]}")" \
   --argjson puts "$(jq -Rn '[inputs|split(" ")|{name:.[0],rc:.[1]}]' < "$res/c5.rc")" \
   '{schema:"borsuk-canary-verdict-v1",instance_id:$i,verdict:$v,reasons:$reasons,steps_completed:$ok,roster_publications:$puts,roster_writer_failure:($w == 1),
     verdict_publication:"its own exit status is recorded only locally in verdict.put.rc and on the console; absence of the object means NOT_COMPLETE",
     acceptance:"PROVISIONAL_REQUIRES_ROOT_REPLAY_TERMINATION_AND_VOLUME_DELETION",native_chain_closed:false,scientific_claim:false,performance_claim:false,finished_epoch:$t}' > "$res/verdict.json.tmp" \
   && mv -- "$res/verdict.json.tmp" "$res/verdict.json"; then
   put verdict.json "$res/verdict.json"; vrc=$?
   printf '%s\n' "$vrc" > "$res/verdict.put.rc" || w=1
  else vrc=71; fi
  if (( vrc != 0 )); then w=1; sev 1 "verdict.json was not written or published (rc $vrc)"; fi
 else w=1; sev 3 "no results directory or run prefix"; fi
 line=$(jq -nc --arg v "$V" --arg i "$instance" --argjson n "${#reasons[@]}" --argjson vrc "$vrc" --argjson w "$w" --argjson rc "$rc" '{verdict:$v,instance_id:$i,reasons:$n,verdict_put_rc:$vrc,writer_failure:($w == 1),coordinator_rc:$rc}') || line='{"verdict":"INVALID","reason":"console line not built"}'
 printf 'BORSUK_CANARY_VERDICT %s\n' "$line" > /dev/console || w=1
 shutdown -h now
 sd=$?
 (( sd == 0 )) || w=1
 if (( rc != 0 )); then xit=$rc; elif (( w != 0 )); then xit=70; elif [[ $V != QUALIFIED_PROVISIONAL ]]; then xit=73; fi
 exit "$xit"
}
trap fin EXIT
trap 'exit 143' TERM INT HUP

mkdir -m 0700 -- "$cases" "$res"
: > "$res/steps.jsonl"

# ---- parameters written by the tail ---------------------------------------------------------------------------------
phase=params
reg "$tev/params.json" && (( $(stat -c %s "$tev/params.json") <= 4096 ))
jq -e --arg b "$bucket" '(keys == ["boot_epoch","bucket","coordinator_sha256","fragment_sha256","instance_id","local_stop_epoch","prefix","schema","tail_sha256","wrapper_canary_sha256"]) and .schema == "borsuk-canary-params-v1" and .bucket == $b and (.instance_id|test("^i-[0-9a-f]+$")) and (.prefix|test("^research/semantic-router/[0-9]{8}/[a-z0-9-]+$")) and (.fragment_sha256|test("^[0-9a-f]{64}$")) and ([.boot_epoch,.local_stop_epoch]|all(type == "number" and . > 0 and floor == .))' "$tev/params.json" > /dev/null
instance=$(jq -er .instance_id "$tev/params.json"); prefix=$(jq -er .prefix "$tev/params.json")
frag_sha=$(jq -er .fragment_sha256 "$tev/params.json"); local_stop=$(jq -er .local_stop_epoch "$tev/params.json")
hard_stop=$((local_stop - 20))
tj=$(jq -er .tail_sha256 "$tev/params.json")
[[ $(jq -er .coordinator_sha256 "$tev/params.json") == "$(sha "${BASH_SOURCE[0]}")" && $(jq -er .wrapper_canary_sha256 "$tev/params.json") == "$wc_pin" && $(sha "$root/wrapper-canary.sh") == "$wc_pin" ]] || { sev 2 "coordinator/wrapper pins in params differ from the files"; exit 0; }
reg "$tev/tail.json" && jq -e --arg t "$tj" --arg f "$(jq -er .fragment_sha256 "$tev/params.json")" '.schema == "borsuk-canary-tail-v1" and .tail_sha256 == $t and .fragment_sha256 == $f and .coordinator_queued == true' "$tev/tail.json" > /dev/null || { sev 2 "tail.json does not match params"; exit 0; }

# ---- W: wait for the ORIGINAL cloud-final to finish and authenticate what it wrote -------------------------------------
step_begin W 20
(( local_stop - EPOCHSECONDS >= 480 )) || { sev 2 "less than 480 s before the machine stop"; exit 0; }
show=''
while :; do
 if show=$(run 3 systemctl show cloud-final.service -p ActiveState -p SubState -p Result -p ExecMainCode -p ExecMainStatus -p ExecStopPost); then
  s=$(sed -n 's/^ActiveState=//p' <<< "$show")
  if [[ $s == failed || $s == inactive ]]; then break; fi
 fi
 tmo 3 || { sev 2 "original cloud-final did not reach failed|inactive (last '${s:-unknown}')"; exit 0; }
 sleep 1
done
printf '%s\n' "$show" > "$res/original.show.txt"
pr() { sed -n "s/^$1=//p" <<< "$show"; }
esp=$(pr ExecStopPost)
[[ $(grep -c '^ExecStopPost=' <<< "$show") == 1 && $esp == *"argv[]=/bin/bash $root/service-stop.sh bootstrap $bucket $prefix/bootstrap-manager.json ; ignore_errors=no ;"* && $esp == *' ; code=exited ; status=0 }' ]] || { sev 2 "original ExecStopPost is not exactly the 3-argument hook with code=exited status=0"; exit 0; }
for f in terminal.json final.exit bootstrap-manager.json bootstrap-manager.put.json evidence.put.json manifest.put.json evidence.tar.gz artifacts.sha256; do
 reg "$root/$f" && [[ -s $root/$f ]] || { sev 2 "original writer output $f missing"; exit 0; }
done
tsha=$(sha "$root/terminal.json")
[[ $(< "$root/final.exit") == 99 ]] || { sev 2 "original final.exit is not 99"; exit 0; }
jq -e --arg i "$instance" '(keys == ["acceptance","chain","evidence","exit","instance_id","original_exit","performance_claim","phase","publication_verified","schema","scientific_success_asserted"]) and .schema == "borsuk-native-scale-build-bootstrap-closed-v1" and .instance_id == $i and .phase == "canary" and .original_exit == 99 and .exit == 99 and .chain == {"unit_exit": null, "disposition": null} and .acceptance == "PROVISIONAL_REQUIRES_EXTERNAL_BOOTSTRAP_EXIT" and .publication_verified == false and .scientific_success_asserted == false and .performance_claim == false and (.evidence | keys == ["bytes","sha256"] and (.bytes | type == "number" and . > 0 and floor == .) and (.sha256 | test("^[0-9a-f]{64}$")))' "$root/terminal.json" > /dev/null || { sev 2 "original terminal.json shape"; exit 0; }
jq -e --arg i "$instance" --arg sha "$tsha" '(keys == ["exit_code","exit_status","final_exit","instance_id","schema","service_result","terminal_sha256"]) and .schema == "borsuk-parity-bootstrap-exit-v1" and .instance_id == $i and .terminal_sha256 == $sha and .final_exit == "99"' "$root/bootstrap-manager.json" > /dev/null || { sev 2 "original bootstrap-manager.json shape"; exit 0; }
for f in bootstrap-manager.put.json evidence.put.json manifest.put.json; do
 jq -e 'type == "object" and (.ETag | type == "string")' "$root/$f" > /dev/null || { sev 2 "original publication receipt $f"; exit 0; }
done
jq -e '.exit_code == "exited" and .exit_status == "1" and .service_result == "exit-code"' "$root/bootstrap-manager.json" > /dev/null || sev 1 "original manager tuple differs from the prospective exited/1/exit-code pin (recorded verbatim)"
for t in T1 T2 T3 T4 T5; do
 reg "$tev/$t.exit" || { sev 2 "tail record $t.exit missing"; exit 0; }
 [[ $(< "$tev/$t.exit") == 0 ]] || sev 1 "tail step $t exit status $(< "$tev/$t.exit")"
done
jq -n --arg show "$show" --slurpfile m "$root/bootstrap-manager.json" --argjson t "$(jq -c . "$tev/T1.json")" \
 '{schema:"borsuk-canary-original-v1",show:($show|split("\n")),manager:$m[0],final_exit:"99",tail_t1:$t}' > "$res/original.json"
step_end

# ---- C0: admission, unit copies, ONE daemon-reload, case trees -------------------------------------------------------------
step_begin C0 25
ver=$(systemctl --version | sed -n '1s/^systemd \([0-9][0-9]*\).*/\1/p')
[[ $ver =~ ^[0-9]+$ ]] && (( ver >= 233 )) || { sev 2 "systemd $ver is older than 233"; exit 0; }
frag=$(run 3 systemctl show cloud-final.service -p FragmentPath --value)
reg "$frag" && [[ $(sha "$frag") == "$frag_sha" ]] || { sev 2 "FragmentPath '$frag' is not the pinned file"; exit 0; }
[[ $(run 3 systemctl show cloud-final.service -p DropInPaths --value) == "$dropin" ]] && cmp -s -- "$dropin" <(printf '[Service]\nExecStopPost=/bin/bash %s/service-stop.sh bootstrap %s %s/bootstrap-manager.json\n' "$root" "$bucket" "$prefix") || { sev 2 "drop-in is not exactly the production borsuk-exit.conf"; exit 0; }
reg "$root/service-stop.sh" && [[ $(sha "$root/service-stop.sh") == "$stop_sha" ]] || { sev 2 "service-stop.sh is not the pinned file"; exit 0; }
caps=(/var/lib/cloud:67108864 /run/cloud-init:8388608 /etc/cloud:1048576 /var/log/cloud-init.log:67108864 /var/log/cloud-init-output.log:67108864)
total=0
for e in "${caps[@]}"; do
 [[ -e ${e%:*} ]] || { sev 2 "${e%:*} is missing"; exit 0; }
 n=$(run 5 du -sb -- "${e%:*}" | cut -f1)
 [[ $n =~ ^[0-9]+$ ]] && (( n <= ${e#*:} )) || { sev 2 "${e%:*} exceeds its size cap"; exit 0; }
 total=$((total + n))
done
for l in /var/log/cloud-init.log /var/log/cloud-init-output.log; do reg "$l" || { sev 2 "$l is not a regular file"; exit 0; }; done
avail=$(df -B1 --output=avail /mnt | tail -n1 | tr -d ' ')
[[ $avail =~ ^[0-9]+$ ]] && (( avail >= 3 * total + 1073741824 )) || { sev 2 "free space $avail below 3x copies + 1 GiB"; exit 0; }
run 5 find /var/lib/cloud /run/cloud-init /etc/cloud ! -type f ! -type d ! -type l -print0 > "$res/special.lst"
while IFS= read -r -d '' p; do [[ $p == "$fifo" && -p $p ]] || { sev 2 "unadmitted special file $p"; exit 0; }; done < "$res/special.lst"
run 5 find /var/lib/cloud /run/cloud-init /etc/cloud -type l -printf '%p\0%l\0' > "$res/symlinks.lst"
: > "$res/symlinks.tsv"
while IFS= read -r -d '' p && IFS= read -r -d '' tg; do
 [[ $p != *$'\t'* && $p != *$'\n'* && $tg != *$'\t'* && $tg != *$'\n'* ]] || { sev 2 "symlink name with a control character"; exit 0; }
 if [[ $tg == /* ]]; then ab=$(realpath -sm -- "$tg"); else ab=$(realpath -sm -- "${p%/*}/$tg"); fi
 case $ab in /var/lib/cloud|/var/lib/cloud/*|/run/cloud-init|/run/cloud-init/*|/etc/cloud|/etc/cloud/*) ;; *) sev 2 "symlink $p -> $tg leaves the admitted trees"; exit 0;; esac
 printf '%s\t%s\n' "$p" "$tg" >> "$res/symlinks.tsv"
done < "$res/symlinks.lst"
[[ $(readlink /var/lib/cloud/instance) == "/var/lib/cloud/instances/$instance" ]] || { sev 2 "cached instance link is not instances/$instance"; exit 0; }
iid_dir=/var/lib/cloud/instances/$instance
[[ -d $iid_dir && ! -L $iid_dir ]] && reg "$iid_dir/obj.pkl" && [[ -s $iid_dir/obj.pkl && -d $iid_dir/scripts && ! -L $iid_dir/scripts ]] && [[ $(< /var/lib/cloud/data/instance-id) == "$instance" ]] || { sev 2 "cached identity does not match $instance"; exit 0; }
for n in 0 2 3; do
 f=/run/systemd/system/borsuk-canary-final-$n.service
 [[ ! -e $f && ! -L $f && ! -e $f.tmp && ! -L $f.tmp && ! -e $cases/$n && ! -L $cases/$n ]] || { sev 2 "case $n artifacts already exist"; exit 0; }
 awk '/^\[Install\]/{s=1} s{next} /^TimeoutSec=0$/{print "TimeoutStartSec=10"; print "TimeoutStopSec=5"; c++; next} {print} END{exit (c==1 ? 0 : 1)}' "$frag" > "$f.tmp" || { sev 2 "fragment lacks exactly one TimeoutSec=0 line"; exit 0; }
 [[ $(grep -E '^\[' "$f.tmp" | tail -n1) == '[Service]' ]] || { sev 2 "fragment does not end in [Service] after dropping [Install]"; exit 0; }
 printf 'BindPaths=%s\nExecStopPost=/bin/bash %s/service-stop.sh bootstrap\n' "$(bnd "$n")" "$root" >> "$f.tmp"
 chmod 0644 "$f.tmp"; mv -- "$f.tmp" "$f"
done
dl
run 10 systemctl daemon-reload
: > "$res/unit.show.txt"
for n in 0 2 3; do
 u=borsuk-canary-final-$n.service
 sh=$(run 3 systemctl show "$u" -p LoadState -p Type -p RemainAfterExit -p KillMode -p TimeoutStartUSec -p TimeoutStopUSec -p ExecStart -p ExecStopPost -p BindPaths)
 printf '== %s\n%s\n' "$u" "$sh" >> "$res/unit.show.txt"
 grep -qx 'LoadState=loaded' <<< "$sh" && grep -qx 'Type=oneshot' <<< "$sh" && grep -qx 'TimeoutStartUSec=10s' <<< "$sh" && grep -qx 'TimeoutStopUSec=5s' <<< "$sh" || { sev 2 "unit $u did not load with the expected overrides"; exit 0; }
done
# the ACTIVE coordinator keeps the three case units loaded: systemd.unit(5) UNIT GARBAGE COLLECTION - another loaded unit's After= references them, so a stopped successful
# case unit is not unloaded and its actual ExecStopPost record survives to the show below (After= alone never starts them). Bounded readback of the loaded manager state:
ca=$(run 3 systemctl show borsuk-canary-coordinator.service -p After --value) && (( ${#ca} <= 2048 )) || { sev 2 "coordinator After unreadable or oversized"; exit 0; }
for w in cloud-final.service borsuk-canary-final-0.service borsuk-canary-final-2.service borsuk-canary-final-3.service; do [[ " $ca " == *" $w "* ]] || { sev 2 "coordinator After lacks $w"; exit 0; }; done
# case trees are copied AFTER the reload (the generator rewrites /run/cloud-init); edits touch the copies only
declare -A want=()
for pair in /var/lib/cloud:var-lib-cloud /run/cloud-init:run-cloud-init /etc/cloud:etc-cloud; do
 want[${pair#*:}]=$(dig "${pair%:*}" "$([[ ${pair%:*} == /run/cloud-init ]] && echo x)") || { sev 2 "live listing digest of ${pair%:*} failed"; exit 0; }
done
for n in 0 2 3; do
 dl
 c=$cases/$n
 mkdir -m 0700 -- "$c" "$c/root" "$c/var-lib-cloud" "$c/run-cloud-init" "$c/etc-cloud"
 for pair in /var/lib/cloud:var-lib-cloud /run/cloud-init:run-cloud-init /etc/cloud:etc-cloud; do
  run 8 bash -c 'set -o pipefail; tar -C "$1" --exclude=./share/hook-hotplug-cmd -cf - . | tar -C "$2" -xpf - --numeric-owner' _ "${pair%:*}" "$c/${pair#*:}"
  [[ $(dig "$c/${pair#*:}") == "${want[${pair#*:}]}" ]] || { sev 2 "case $n copy of ${pair%:*} differs from the live tree"; exit 0; }
 done
 mkfile 0640 "$c/cloud-init.log" < /var/log/cloud-init.log && mkfile 0640 "$c/cloud-init-output.log" < /var/log/cloud-init-output.log || { sev 2 "case $n log copies not created"; exit 0; }
 s0=$(stat -c %s "$c/cloud-init.log"); printf '%s\n' "$s0" | mkfile 0600 "$c/log.offset" || { sev 2 "case $n log offset not written"; exit 0; }
 run 5 find "$c/var-lib-cloud" "$c/run-cloud-init" "$c/etc-cloud" ! -type f ! -type d ! -type l -print -quit > "$res/copy.special"
 [[ ! -s $res/copy.special ]] || { sev 2 "case $n copy holds a special file"; exit 0; }
 ic=$c/var-lib-cloud/instances/$instance
 for d in "$c/var-lib-cloud/instances" "$ic" "$ic/scripts" "$c/var-lib-cloud/data" "$c/etc-cloud/cloud.cfg.d"; do [[ -d $d && ! -L $d ]] || { sev 2 "case $n copy path $d is not a real directory"; exit 0; }; done
 [[ -z $(find "$ic/scripts" -mindepth 1 ! -type f) ]] || { sev 2 "case $n scripts directory holds a non-regular entry"; exit 0; }
 for d in "$c/var-lib-cloud/data/status.json" "$c/var-lib-cloud/data/result.json"; do [[ ! -e $d || ( -f $d && ! -L $d ) ]] || { sev 2 "case $n $d is not a regular file"; exit 0; }; done
 run 5 find "$ic/scripts" -mindepth 1 -maxdepth 1 -print0 > "$res/scripts.lst"
 while IFS= read -r -d '' f; do rmfile "$f" || { sev 2 "case $n scripts entry $f is not a removable regular file"; exit 0; }; done < "$res/scripts.lst"
 for f in "$c/var-lib-cloud/data/status.json" "$c/var-lib-cloud/data/result.json"; do rmfile "$f" || { sev 2 "case $n status reset of $f refused"; exit 0; }; done
 printf '#!/bin/bash\nprintf "%%s\\n" %s > %s/final.exit\nprintf "%%s\\n" %s > %s/script.exit\nexit %s\n' "$n" "$root" "$n" "$root" "$n" | mkfile 0700 "$ic/scripts/part-001" || { sev 2 "case $n fixture path refused"; exit 0; }
 printf 'cloud_final_modules:\n - [scripts-user, always]\n' | mkfile 0600 "$c/etc-cloud/cloud.cfg.d/zz-borsuk-canary.cfg" || { sev 2 "case $n override path is occupied (incl. a link) or its parent is not admitted"; exit 0; }
 mkfile 0500 "$c/root/service-stop.sh" < "$root/service-stop.sh" && [[ $(sha "$c/root/service-stop.sh") == "$stop_sha" ]] || { sev 2 "case $n service-stop.sh copy refused"; exit 0; }
 jq -n --arg i "$instance" '{schema:"borsuk-canary-terminal-v1",instance_id:$i,canary:true}' | mkfile 0600 "$c/root/terminal.json" || { sev 2 "case $n terminal.json refused"; exit 0; }
done
mkdir -m 0700 -- "$cases/original"
jq -n --argjson v "$ver" --arg f "$frag" --arg fs "$frag_sha" --argjson total "$total" --arg i "$instance" --rawfile u "$res/unit.show.txt" --rawfile l "$res/symlinks.tsv" --arg ca "$ca" \
 '{schema:"borsuk-canary-admission-v1",systemd:$v,fragment:$f,fragment_sha256:$fs,copied_bytes:$total,instance_id:$i,admitted_special:["/run/cloud-init/share/hook-hotplug-cmd"],symlinks:($l|split("\n")|map(select(length>0))),coordinator_after:($ca|split(" ")|map(select(length>0))|sort),units:($u|split("\n"))}' > "$res/admission.json"
step_end

# ---- C1: preserve the original evidence outside the bound targets and take the live baseline -----------------------------
step_begin C1 15
cp -p "$root/bootstrap-manager.json" "$root/final.exit" "$res/original.show.txt" "$cases/original/"
for f in status.json result.json instance-id; do cp -p -- "/var/lib/cloud/data/$f" "$cases/original/$f"; done
snap_lists() { # one checked listing file per tree, one checked files list, then the digest of exactly those files
 local l=$1 t f h
 for t in /var/lib/cloud /run/cloud-init /etc/cloud "$root/evidence-root"; do
  run 8 bash -c 'lst "$1" "$2"' _ "$t" "$([[ $t == /run/cloud-init ]] && echo x)" > "$res/lst.$l.${t//\//_}" || return 1
  chk_list "$res/lst.$l.${t//\//_}" || { log "empty or partial listing of $t"; return 1; }
 done
 for f in /var/log/cloud-init.log /var/log/cloud-init-output.log "$root"/{terminal.json,final.exit,bootstrap-manager.json,bootstrap-manager.put.json,evidence.put.json,manifest.put.json,evidence.tar.gz,artifacts.sha256}; do
  if reg "$f"; then h=$(sha "$f") || return 1; printf '%s %s\n' "$h" "$f"; else printf 'absent %s\n' "$f"; fi
 done > "$res/lst.$l.files" || return 1
 [[ $(wc -l < "$res/lst.$l.files") == 10 ]] || return 1
 { for t in /var/lib/cloud /run/cloud-init /etc/cloud "$root/evidence-root"; do cat "$res/lst.$l.${t//\//_}" || exit 1; done; cat "$res/lst.$l.files" || exit 1; } | sha256sum | cut -d' ' -f1 > "$res/snap.$l" || return 1
 [[ $(< "$res/snap.$l") =~ ^[0-9a-f]{64}$ ]]
}
snap_lists before || { sev 2 "baseline listing failed or was empty/partial"; exit 0; }
for f in "$res"/lst.before.*; do (( $(stat -c %s "$f") <= 1048576 )) || { sev 2 "baseline listing $f exceeds 1 MiB"; exit 0; }; done
step_end

# ---- C2/C3: the three cases, serial ---------------------------------------------------------------------------------------
run_case() {
 local n=$1 u=borsuk-canary-final-$1.service c=$cases/$1 cls rc mst=1 rsl=exit-code est=1 ecls=EXITED_NONZERO ers=1 act ss rs mc ms out s0 modsran errs ok=1 sh csha
 (( n != 0 )) || { mst=0; rsl=success; est=0; ecls=SUCCEEDED; ers=0; }
 case_stop=$((EPOCHSECONDS + 30)); step_begin "case$n" 30
 # probe: effective merged config under the SAME BindPaths, before any fixture runs
 sub 5
 run 5 systemd-run --quiet --wait --pipe --collect --unit="borsuk-canary-probe-$n" -p "BindPaths=$(bnd "$n")" /usr/bin/python3 -I -c "$probe_py" > "$c/probe.json" 2> "$c/probe.err" || { run 3 systemctl stop "borsuk-canary-probe-$n.service" || :; sev 2 "case $n config probe failed"; halt=1; return 0; }
 jq -e '.modules == [["scripts-user","always"]] and (.paths | all(.[]; . == "/var/log/cloud-init.log" or . == "/var/log/cloud-init-output.log" or . == "/dev/log"))' "$c/probe.json" > /dev/null || { sev 2 "case $n effective modules/output paths not admitted: $(head -c 400 "$c/probe.json")"; halt=1; return 0; }
 active=$n; s0=$(< "$c/log.offset")
 sub 10; rc=0
 run 3 systemctl start --no-block "$u" || rc=$?
 cls=NOT_ACTIVATED; act=unknown; ss=''; rs=''; mc=''; ms=''
 if (( rc != 0 )); then cls=SUBMISSION_ERROR
 else
  while :; do
   sh=$(run 3 systemctl show "$u" -p ActiveState -p SubState -p Result -p ExecMainCode -p ExecMainStatus) || { cls=DEADLINE; break; }
   act=$(sed -n 's/^ActiveState=//p' <<< "$sh"); ss=$(sed -n 's/^SubState=//p' <<< "$sh"); rs=$(sed -n 's/^Result=//p' <<< "$sh")
   mc=$(sed -n 's/^ExecMainCode=//p' <<< "$sh"); ms=$(sed -n 's/^ExecMainStatus=//p' <<< "$sh")
   if [[ $act == active && $ss == exited && $rs == success && $mc == 1 && $ms == 0 ]]; then cls=SUCCEEDED; break
   elif [[ $act == failed ]]; then
    if [[ $rs == timeout ]]; then cls=TIMEOUT
    elif [[ $mc == 2 || $mc == 3 || $rs == signal || $rs == core-dump ]]; then cls=SIGNAL
    elif [[ $rs == exit-code && $mc == 1 ]]; then cls=EXITED_NONZERO
    else cls=UNEXPECTED; fi
    break
   fi
   tmo 3 || { cls=DEADLINE; break; }
   sleep 0.5
  done
 fi
 if [[ $cls == DEADLINE && $act == inactive ]]; then cls=NOT_ACTIVATED; fi
 case $cls in
  SUBMISSION_ERROR|NOT_ACTIVATED) sev 2 "case $n $cls (start rc $rc, state $act/$ss)";;
  TIMEOUT|SIGNAL|DEADLINE) sev 3 "case $n $cls (state $act/$ss result $rs code $mc status $ms)";;
 esac
 case $cls in SUBMISSION_ERROR|NOT_ACTIVATED|TIMEOUT|SIGNAL|DEADLINE) sub 5; drain "$n" || sev 3 "case $n drain failed"; active=''; halt=1; return 0;; esac
 sub 5
 if [[ $cls == SUCCEEDED ]]; then run 5 systemctl stop "$u" || { sev 3 "case $n stop failed"; drain "$n" || :; active=''; halt=1; return 0; }; fi
 # read the hook record BEFORE reset-failed
 sub 3
 esp=$(run 3 systemctl show "$u" -p ExecStopPost) || { sev 3 "case $n ExecStopPost unreadable"; drain "$n" || :; active=''; halt=1; return 0; }
 sub 4
 drain "$n" || { sev 3 "case $n exact-cgroup drain failed"; active=''; halt=1; return 0; }
 active=''
 sub 5
 [[ $(grep -c '^ExecStopPost=' <<< "$esp") == 1 && $esp == *"argv[]=/bin/bash $root/service-stop.sh bootstrap ; ignore_errors=no ;"* && $esp == *' ; code=exited ; status=0 }' ]] || { ok=0; sev 1 "case $n ExecStopPost did not run as the exact 1-argument hook with code=exited status=0"; }
 [[ $cls == "$ecls" ]] || { ok=0; sev 1 "case $n class $cls (expected $ecls)"; }
 [[ $mc == 1 && $ms == "$est" && $rs == "$rsl" ]] || { ok=0; sev 1 "case $n systemd tuple code=$mc status=$ms result=$rs differs from the pin"; }
 out=$c/root
 for f in bootstrap-manager.json final.exit script.exit; do reg "$out/$f" || { ok=0; sev 1 "case $n $f missing"; }; done
 if reg "$out/bootstrap-manager.json"; then
  csha=$(sha "$out/terminal.json") || { ok=0; sev 1 "case $n terminal.json hash unreadable"; csha=none; }
  jq -e --arg i "$instance" --arg sha "$csha" --arg n "$n" --arg ms "$mst" --arg rsl "$rsl" '(keys == ["exit_code","exit_status","final_exit","instance_id","schema","service_result","terminal_sha256"]) and .schema == "borsuk-parity-bootstrap-exit-v1" and .instance_id == $i and .terminal_sha256 == $sha and .exit_code == "exited" and .exit_status == $ms and .service_result == $rsl and .final_exit == $n' "$out/bootstrap-manager.json" > /dev/null || { ok=0; sev 1 "case $n manager record differs from the pin"; }
 fi
 [[ -f $out/script.exit && $(< "$out/script.exit") == "$n" && -f $out/final.exit && $(< "$out/final.exit") == "$n" ]] || { ok=0; sev 1 "case $n script/final exit differs from $n"; }
 for f in result.json status.json; do reg "$c/var-lib-cloud/data/$f" && cp -p "$c/var-lib-cloud/data/$f" "$res/case-$n.$f" || { ok=0; sev 1 "case $n cloud-init $f missing"; }; done
 if [[ -f $res/case-$n.result.json ]]; then
  errs=$(jq -er '.v1.errors | length' "$res/case-$n.result.json") || errs=bad
 fi
 # cloud-init 26.1 with the copy's status.json deleted: ONE stage runs (modules-final) and the four stage records alias ONE errors list (nullstatus.copy()), so the raw result repeats the
 # owner errors four times. The raw files are kept; the exact frozen shape is required, never uniqued: owner errors [] (case 0) or the single scripts-user failure (cases 2/3), ALL four
 # stage lists equal to the owner, only modules-final carries times, and result.errors is exactly the ordered stage concatenation. Both files must hold exactly ONE JSON document
 # (-s on the result, --slurpfile length on the status), so no earlier false document can be masked and no extra document ignored.
 if [[ -f $res/case-$n.result.json && -f $res/case-$n.status.json ]]; then
  jq -e -s --argjson want "$ers" --arg err "('scripts-user', RuntimeError('Runparts: 1 failures (part-001) in 1 attempted commands'))" --slurpfile s "$res/case-$n.status.json" '["init","init-local","modules-config","modules-final"] as $st | (length == 1 and ($s | length) == 1) and (.[0] as $r | $s[0].v1 as $v | (($r.v1 | keys) == ["datasource","errors"]) and (($v | keys) == ["datasource","init","init-local","modules-config","modules-final","stage"]) and ($v.stage == null) and ($st | all(.[]; . as $k | ($v[$k] | type) == "object" and ($v[$k] | keys) == ["errors","finished","recoverable_errors","start"] and ($v[$k].errors | type) == "array" and ($v[$k].recoverable_errors | type) == "object")) and ([$st[] | select($v[.].start != null or $v[.].finished != null)] == ["modules-final"]) and ($v["modules-final"].start | type == "number" and . > 0) and ($v["modules-final"].finished | type == "number" and . >= $v["modules-final"].start) and ($v["modules-final"].errors == (if $want == 0 then [] else [$err] end)) and ($st | all(.[]; . as $k | $v[$k].errors == $v["modules-final"].errors)) and ($r.v1.datasource == $v.datasource) and ($r.v1.errors == [$st[] as $k | $v[$k].errors[]]))' "$res/case-$n.result.json" > /dev/null || { ok=0; sev 1 "case $n cloud-init status/result is not the exact frozen shape (owner errors expected $ers, raw result errors $errs)"; }
 fi
 if reg "$out/bootstrap-manager.json"; then cp -p -- "$out/bootstrap-manager.json" "$res/case-$n.manager.json" || { ok=0; sev 1 "case $n manager record not copied"; }; fi
 modsran=$(tail -c +"$((s0 + 1))" "$c/cloud-init.log" | head -c 4194304 | grep -oE 'Running module [A-Za-z0-9_-]+' | sort -u | tr '\n' ',') || modsran=''
 [[ $modsran == 'Running module scripts-user,' ]] || { ok=0; sev 1 "case $n modules that ran: '$modsran'"; }
 jq -n --argjson n "$n" --arg cls "$cls" --arg act "$act" --arg sub "$ss" --arg rs "$rs" --arg mc "$mc" --arg ms "$ms" --arg esp "$esp" --arg mods "$modsran" --argjson ok "$ok" --slurpfile p "$c/probe.json" \
  '{schema:"borsuk-canary-case-v1",case:$n,class:$cls,systemd:{active:$act,sub:$sub,result:$rs,exec_main_code:$mc,exec_main_status:$ms},exec_stop_post:$esp,modules_ran:$mods,probe:$p[0],all_pinned_tuples_match:($ok==1),modified_copy:true,production_closure:false}' > "$res/case-$n.json"
 step_end
}
for n in 0 2 3; do run_case "$n"; (( halt == 0 )) || break; done

# ---- C4: the live baseline must be byte-identical -------------------------------------------------------------------------
step_begin C4 20
case_stop=0
: > "$res/baseline.diff"
snap_lists after || { sev 3 "after-listing failed or was empty/partial"; exit 1; }
if cmp -s "$res/snap.before" "$res/snap.after"; then st=identical
else
 st=DIFFERENT; sev 1 "live baseline changed during the canary"
 for f in "$res"/lst.before.*; do
  rc=0; diff "$f" "${f/before/after}" > "$res/diff.tmp" || rc=$?
  [[ $rc == 0 || $rc == 1 ]] || { sev 3 "diff of $f failed (rc $rc)"; exit 1; }
  head -n 20 "$res/diff.tmp" >> "$res/baseline.diff"
 done
fi
jq -n --arg b "$(< "$res/snap.before")" --arg a "$(< "$res/snap.after")" --arg st "$st" --rawfile d "$res/baseline.diff" '{schema:"borsuk-canary-listing-v1",before:$b,after:$a,status:$st,diff_head:($d|split("\n")|.[0:60])}' > "$res/listing-summary.json"
jq -e '.schema == "borsuk-canary-listing-v1" and (.before|test("^[0-9a-f]{64}$")) and (.after|test("^[0-9a-f]{64}$")) and (.status == "identical" or .status == "DIFFERENT")' "$res/listing-summary.json" > /dev/null
step_end
exit 0
