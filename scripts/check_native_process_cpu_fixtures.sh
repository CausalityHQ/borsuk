#!/usr/bin/env bash
# Experiment-only supervisor for the native_process_cpu fixture roster (UNVERIFIED source handoff).
#
#   check_native_process_cpu_fixtures.sh ABS_HELPER_ELF HELPER_SHA256 NEW_ABS_EXT4_OUTPUT_DIR
#
# Runs the frozen 12 invocations (15 fixture processes + the already executed compiler-test
# child = cap 16, no retries, no extra cases) one at a time. Each invocation is a fresh
# root-owned systemd service with a delegated `payload` subgroup (CPU1/AllowedCPUs0/256MiB/
# swap0/pids128/no network). The native helper stays blocked behind a FIFO gate until placement,
# limits, core limit, ext4, ELF and config hashes are read back; a root ExecStopPost hold in
# <unit>/.control keeps the payload cgroup and counters alive until evidence is persisted.
# One external 60 s fixture-batch deadline kills only the units this run created. Delegate= makes controllers only AVAILABLE;
# while the gate is blocked the driver enables cpu/cpuset/memory/pids for the payload in the owned unit cgroup (checked, read back).
#
# DECLARED BOUNDS (prospective, not tuned to pass): fixture batch 60 s; cleanup allowance 120 s;
# publication allowance 60 s. A separate hard-stop process kills the driver (start time verified) at
# 240 s after batch start regardless of how far the deadline cleanup got. Its own post-kill cleanup and
# retraction can still stall on a stalled manager or filesystem; that is bounded ONLY by the outer
# transient service the driver MUST run in: admission parses its RuntimeMaxUSec and requires
# 240 s < RuntimeMaxSec <= 300 s (declared ceiling), and refuses a plain shell. No stronger guarantee
# is claimed than this code provides. ACCEPTANCE rule: only a checked TERMINAL.json (final_state, driver exit code 0, and the
# matching result.json + SHA256SUMS hashes) authorizes acceptance; result.json/SHA256SUMS alone never do,
# and a deadline intervention can never be a success.
#
# All fixture work, wait status and accounting come from the native helper's own report and
# collector receipt; this script only orchestrates and reads back. It proves execution
# mechanics ONLY: final states are MECHANICS_VERIFIED / INCONCLUSIVE / INVALID and the claims
# numerical_CPU_accuracy / overhead_GO / performance are always false. Every publication step
# (case.json, evidence hash + sync, result.json, manifest, sync) is checked: any write failure,
# closure failure (timeout/undrained/vanished/not-ended/cleanup) or unsynced evidence is never success.
# Exit: 0 MECHANICS_VERIFIED, 1 INCONCLUSIVE, 2 INVALID, 125 admission refusal.
set -uo pipefail
export LC_ALL=C

readonly RESULT_SCHEMA=borsuk-native-cpu-fixture-batch-result-v1
readonly CASE_SCHEMA=borsuk-native-cpu-fixture-case-v1
readonly ADMISSION_SCHEMA=borsuk-native-cpu-fixture-admission-v1
readonly DEADLINE_S_PROD=60 CLEANUP_ALLOWANCE_S=120 PUBLICATION_ALLOWANCE_S=60 OUTER_RUNTIME_MAX_DECLARED_S=300
readonly SCTL_S=10 STOP_S=20 LAUNCH_S=30
readonly FIXTURE_CAP=16 RESERVED_COMPILER_TEST=1
readonly MEMORY_MAX=268435456 PIDS_MAX=128 PAYLOAD=payload
readonly DELEGATE_CONTROLLERS='cpu cpuset memory pids'
readonly COLLECTOR_SCHEMA=native-process-cpu-collector-v1
readonly TIMEOUT_PROD=/usr/bin/timeout
readonly TERMINAL_SCHEMA=borsuk-native-cpu-fixture-terminal-v1

# Frozen roster (fixture-invocations.pending.json): id gate_mode exp_exit exp_signal procs accounting
readonly -a CASES=(
  'pulse ordinary 0 - 1 UNQUALIFIED_UNTIL_RUNTIME_EVIDENCE'
  'threads ordinary 0 - 1 UNQUALIFIED_UNTIL_RUNTIME_EVIDENCE'
  'waited ordinary 0 - 2 UNQUALIFIED_UNTIL_RUNTIME_EVIDENCE'
  'io ordinary 0 - 1 UNQUALIFIED_UNTIL_RUNTIME_EVIDENCE'
  'late ordinary 0 - 1 UNQUALIFIED_UNTIL_RUNTIME_EVIDENCE'
  'delay ordinary 0 - 1 UNQUALIFIED_UNTIL_RUNTIME_EVIDENCE'
  'nonzero ordinary 7 - 1 UNQUALIFIED_UNTIL_RUNTIME_EVIDENCE'
  'survivor context 0 - 2 REFUSE'
  'unwaited context 0 - 2 REFUSE'
  'sigpipe ordinary - 13 1 UNQUALIFIED_UNTIL_RUNTIME_EVIDENCE'
  'publication-failure publication-failure 0 - 1 REFUSE'
  'timeout-KILL ordinary - 9 1 REFUSE'
)

# Test seams: ONLY NCPU_FIXTURE_MOCK_ROOT enables them, and every output records mock_seams_active.
MOCK=${NCPU_FIXTURE_MOCK_ROOT:-}
if [[ -n $MOCK ]]; then
  CGROOT=$MOCK/cgroup PROC=$MOCK/proc
  DEADLINE_S=${NCPU_FIXTURE_MOCK_DEADLINE_S:-$DEADLINE_S_PROD}
  TIMEOUT_BIN=${NCPU_FIXTURE_MOCK_TIMEOUT:-$TIMEOUT_PROD}
  ROOT_UID=$(id -u)
  MOCK_ACTIVE=true
else
  CGROOT=/sys/fs/cgroup PROC=/proc DEADLINE_S=$DEADLINE_S_PROD TIMEOUT_BIN=$TIMEOUT_PROD ROOT_UID=0
  MOCK_ACTIVE=false
  export PATH=/usr/sbin:/usr/bin:/sbin:/bin
fi

HARD_STOP_S=$((DEADLINE_S + CLEANUP_ALLOWANCE_S + PUBLICATION_ALLOWANCE_S))
die() { printf 'check_native_process_cpu_fixtures: %s\n' "$*" >&2; exit 125; }
span_us() { # systemd human timespan (us ms s min h) -> microseconds; anything else is refused
  local -a toks; local tok total=0
  [[ -n $1 && $1 != infinity ]] || return 1
  read -ra toks <<<"$1"
  for tok in "${toks[@]}"; do
    [[ $tok =~ ^([0-9]+)(us|ms|s|min|h)$ ]] || return 1
    case ${BASH_REMATCH[2]} in
      us) total=$((total + BASH_REMATCH[1])) ;;
      ms) total=$((total + BASH_REMATCH[1] * 1000)) ;;
      s) total=$((total + BASH_REMATCH[1] * 1000000)) ;;
      min) total=$((total + BASH_REMATCH[1] * 60000000)) ;;
      h) total=$((total + BASH_REMATCH[1] * 3600000000)) ;;
    esac
  done
  printf '%s' "$total"
}
# every manager/launcher call is bounded by the ADMITTED exact timeout executable (authenticated before first use)
tmo() { local t=$1 b=$TIMEOUT_BIN; shift; [[ $MOCK_ACTIVE == true ]] && b=timeout; "$b" -k 2 "$t" "$@"; }
sha() { local o; o=$(sha256sum -- "$1") || return 1; printf '%s' "${o%% *}"; }
safe_abs() { [[ $1 =~ ^/[A-Za-z0-9._/-]+$ && $1 != *//* && $1 != */ && $1 != */. && $1 != */.. && $1 != */./* && $1 != */../* ]]; }
owner_ok() { local o m; read -r o m < <(stat -c '%u %a' -- "$1") || return 1; [[ $o == "$ROOT_UID" ]] && (( (8#$m & 8#022) == 0 )); }
elf_ok() { local m=''; IFS= read -rN4 m 2>/dev/null < "$1"; [[ $m == $'\177ELF' ]]; }
fstype() { findmnt -no FSTYPE -T "$1" 2>/dev/null; }
jarr() { if (( $# )); then printf '%s\0' "$@" | jq -Rsc 'split("\u0000")[:-1]'; else printf '[]'; fi; }
rd() { local v; if IFS= read -r v 2>/dev/null < "$1"; then printf '%s' "$v"; else printf '<unreadable>'; fi; }
ev() { local k v; [[ -r $1 ]] || return 1; while read -r k v; do [[ $k == "$2" ]] && { printf '%s' "$v"; return 0; }; done < "$1"; return 1; }
nap() { read -r -t "$1" -u "$NAPFD" || :; }
dl() { [[ -e $OUT/fifo/deadline-fired ]]; }
inv() { INV+=("$1"); }
inc() { INC+=("$1"); }
eq() { [[ "$2" == "$3" ]] || FAIL+=("$1: got [$2] want [$3]"); }

# ---------- admission (nothing is created or released before this passes) ----------
(( $# == 3 )) || die 'usage: ABS_HELPER_ELF HELPER_SHA256 NEW_ABS_EXT4_OUTPUT_DIR'
HELPER=$1 HELPER_SHA=$2 OUT=$3
safe_abs "$HELPER" || die 'helper must be an absolute path [A-Za-z0-9._/-] without ./.. or // components'
safe_abs "$OUT" || die 'output dir must be an absolute path [A-Za-z0-9._/-] without ./.. or // components'
[[ $HELPER_SHA =~ ^[0-9a-f]{64}$ ]] || die 'expected SHA256 must be 64 lowercase hex'

if [[ $MOCK_ACTIVE == false ]]; then
  [[ $EUID == 0 ]] || die 'root required'
  [[ $(stat -fc %T "$CGROOT" 2>/dev/null) == cgroup2fs ]] || die 'unified cgroup2 hierarchy required'
  read -r -a rootctl < "$CGROOT/cgroup.controllers" || die 'cannot read root cgroup.controllers'
  for c in cpu cpuset memory pids; do [[ " ${rootctl[*]} " == *" $c "* ]] || die "root cgroup lacks controller $c"; done
  [[ $(rd /sys/devices/system/cpu/cpu0/online 2>/dev/null) =~ ^(1|<unreadable>)$ ]] || die 'CPU 0 not online'
fi
(( BASH_VERSINFO[0] >= 5 )) || die 'bash >= 5 required (EPOCHREALTIME)'
for t in jq systemctl systemd-run sha256sum md5sum findmnt install mkfifo realpath stat readlink getconf awk tr sort xargs find uname head grep wc cat cp date timeout sync mv cut mktemp; do
  command -v "$t" >/dev/null || die "missing tool: $t"
done
SYSTEMCTL=$(command -v systemctl) SYSTEMD_RUN=$(command -v systemd-run)
jq -e -n '1' >/dev/null || die 'jq unusable'
OUTER_UNIT='mock' OUTER_RUNTIME_MAX='mock' OUTER_US=0
if [[ $MOCK_ACTIVE == false ]]; then
  # cgroup.kill needs kernel >= 5.14; syncfs error reporting >= 5.8
  [[ $(uname -r) =~ ^([0-9]+)\.([0-9]+) ]] && (( BASH_REMATCH[1] > 5 || (BASH_REMATCH[1] == 5 && BASH_REMATCH[2] >= 14) )) || die 'kernel >= 5.14 required (cgroup.kill, syncfs errors)'
  # independent outer supervisor: this driver must run in a transient service with a finite RuntimeMaxSec
  [[ $(rd /proc/self/cgroup) =~ ([^/]+\.service)(/|$) ]] || die 'must run inside an outer transient service (declared RuntimeMaxSec=300), not a plain shell'
  OUTER_UNIT=${BASH_REMATCH[1]}
fi
CLK_TCK=$(getconf CLK_TCK)
[[ -x /bin/bash && -x /usr/bin/env ]] || die '/bin/bash and /usr/bin/env required by the unit command'

# helper: authenticated separately from timeout
[[ $(realpath -e -- "$HELPER" 2>/dev/null) == "$HELPER" && -f $HELPER && -x $HELPER ]] || die 'helper must be a symlink-free regular executable'
owner_ok "$HELPER" || die 'helper must be owned by the operator uid and not group/other-writable'
elf_ok "$HELPER" || die 'helper is not an ELF file'
HELPER_BYTES=$(stat -c %s -- "$HELPER")
(( HELPER_BYTES >= 1 && HELPER_BYTES <= 536870912 )) || die 'helper size outside 1..536870912'
[[ $(sha "$HELPER") == "$HELPER_SHA" ]] || die 'helper SHA256 mismatch'

# timeout: ELF, GNU coreutils, package owner, package verify, hash
[[ $(realpath -e -- "$TIMEOUT_BIN" 2>/dev/null) == "$TIMEOUT_BIN" && -f $TIMEOUT_BIN && -x $TIMEOUT_BIN ]] || die "$TIMEOUT_BIN must be a symlink-free regular executable"
elf_ok "$TIMEOUT_BIN" || die 'timeout is not an ELF file'
TIMEOUT_BYTES=$(stat -c %s -- "$TIMEOUT_BIN")
TIMEOUT_SHA=$(sha "$TIMEOUT_BIN") || die 'cannot hash timeout'
if [[ $MOCK_ACTIVE == true && -n ${NCPU_FIXTURE_MOCK_TIMEOUT_BANNER:-} ]]; then
  TIMEOUT_BANNER=$NCPU_FIXTURE_MOCK_TIMEOUT_BANNER # mock seam: the placeholder file is hashed/ELF-checked but never executed
else
  TIMEOUT_BANNER=$(env -i PATH=/usr/bin:/bin LC_ALL=C "$TIMEOUT_BIN" --version 2>/dev/null | head -n1)
fi
[[ $TIMEOUT_BANNER == 'timeout (GNU coreutils) '* ]] || die "timeout is not GNU coreutils: [$TIMEOUT_BANNER]"
if [[ $MOCK_ACTIVE == true ]]; then
  TIMEOUT_PACKAGE=MOCK
elif command -v dpkg-query >/dev/null; then
  pkgline=$(dpkg-query -S -- "$TIMEOUT_BIN" 2>/dev/null | head -n1; exit "${PIPESTATUS[0]}") || die 'dpkg-query -S failed for timeout'
  [[ $pkgline == *": $TIMEOUT_BIN" ]] || die 'timeout not owned by a dpkg package'
  pkg=${pkgline%%:*}
  pkgver=$(dpkg-query -W -f='${Version}' "$pkg" 2>/dev/null) && [[ -n $pkgver ]] || die 'dpkg-query -W failed or empty version'
  TIMEOUT_PACKAGE="dpkg:$pkg:$pkgver"
  # positive check: the dpkg-recorded md5 of the installed file must match (a verifier error never passes)
  want_md5=$(awk -v f="${TIMEOUT_BIN#/}" '$2 == f { print $1 }' "/var/lib/dpkg/info/$pkg.md5sums" 2>/dev/null) && [[ $want_md5 =~ ^[0-9a-f]{32}$ ]] || die 'no dpkg md5sums entry for timeout'
  got_md5=$(md5sum -- "$TIMEOUT_BIN" | cut -d' ' -f1; exit "${PIPESTATUS[0]}") || die 'md5sum of timeout failed'
  [[ $got_md5 == "$want_md5" ]] || die 'timeout md5 differs from the dpkg-recorded md5'
  verr=$(mktemp) || die 'mktemp failed'
  vout=$(dpkg --verify "$pkg" 2>"$verr"); vrc=$?
  { (( vrc <= 1 )) && [[ ! -s $verr ]]; } || die "dpkg --verify errored (rc=$vrc)"
  rm -f -- "$verr"
  grep -qF "$TIMEOUT_BIN" <<<"$vout" && die 'dpkg --verify reports timeout modified'
elif command -v rpm >/dev/null; then
  pkgnv=$(rpm -qf --qf '%{NAME}-%{VERSION}-%{RELEASE}.%{ARCH}' "$TIMEOUT_BIN" 2>/dev/null) && [[ -n $pkgnv ]] || die 'timeout not owned by an rpm package'
  TIMEOUT_PACKAGE="rpm:$pkgnv"
  rdig=$(rpm -qf --qf '[%{FILENAMES} %{FILEDIGESTS}\n]' "$TIMEOUT_BIN" 2>/dev/null | awk -v f="$TIMEOUT_BIN" '$1 == f { print $2 }') && [[ $rdig =~ ^[0-9a-f]{64}$ ]] || die 'no sha256 rpm file digest for timeout'
  [[ $rdig == "$TIMEOUT_SHA" ]] || die 'timeout sha256 differs from the rpm-recorded digest'
  verr=$(mktemp) || die 'mktemp failed'
  vout=$(rpm -Vf "$TIMEOUT_BIN" 2>"$verr"); vrc=$?
  { (( vrc <= 1 )) && [[ ! -s $verr ]]; } || die "rpm -V errored (rc=$vrc)"
  rm -f -- "$verr"
  grep -qF "$TIMEOUT_BIN" <<<"$vout" && die 'rpm -V reports timeout modified'
else
  die 'neither dpkg nor rpm available to authenticate the timeout package'
fi

# systemd version and the outer supervisor bound (these manager calls use the AUTHENTICATED timeout above)
SDV=$(tmo "$SCTL_S" "$SYSTEMCTL" --version 2>/dev/null | { read -r _ v _; printf '%s' "${v:-}"; })
[[ $SDV =~ ^[0-9]+$ ]] && (( SDV >= 254 )) || die "systemd >= 254 (DelegateSubgroup) required, got [$SDV]"
if [[ $MOCK_ACTIVE == false ]]; then
  OUTER_RUNTIME_MAX=$(tmo "$SCTL_S" "$SYSTEMCTL" show --no-pager --property=RuntimeMaxUSec --value -- "$OUTER_UNIT" 2>/dev/null) || die 'cannot read outer RuntimeMaxUSec'
  OUTER_US=$(span_us "$OUTER_RUNTIME_MAX") || die "outer RuntimeMaxUSec [$OUTER_RUNTIME_MAX] is not a parseable finite timespan"
  (( OUTER_US > HARD_STOP_S * 1000000 && OUTER_US <= OUTER_RUNTIME_MAX_DECLARED_S * 1000000 )) \
    || die "outer RuntimeMaxSec must be > ${HARD_STOP_S}s (driver hard stop) and <= ${OUTER_RUNTIME_MAX_DECLARED_S}s, got ${OUTER_US}us [$OUTER_RUNTIME_MAX]"
fi

# output: NEW directory on ext4; mkdir is the atomic absence check
PARENT=${OUT%/*}; PARENT=${PARENT:-/}
[[ -d $PARENT ]] && owner_ok "$PARENT" || die 'output parent must exist, be operator-owned and not group/other-writable'
[[ $(fstype "$PARENT") == ext4 ]] || die 'output parent is not ext4'
mkdir -m 0700 -- "$OUT" 2>/dev/null || die 'output dir exists or cannot be created (must be NEW)'
[[ $(fstype "$OUT") == ext4 ]] || die 'created output dir is not ext4'
mkdir -m 0700 -- "$OUT/bin" "$OUT/scripts" "$OUT/fifo" "$OUT/cases" "$OUT/closed-artifact-controls" || die 'cannot create output layout'
install -m 0500 -T -- "$HELPER" "$OUT/bin/native_process_cpu" || die 'cannot copy helper'
HELPER_BIN=$OUT/bin/native_process_cpu
[[ $(sha "$HELPER_BIN") == "$HELPER_SHA" ]] || die 'helper copy SHA256 mismatch'

# ---------- fixed gate and hold scripts (embedded; hashed; root-owned 0500) ----------
cat > "$OUT/scripts/gate.sh" <<'GATE'
#!/usr/bin/env bash
set -euo pipefail
# Runs only inside the freshly admitted payload cgroup; the driver validates placement and limits
# BEFORE it writes `release`. The exec is the first native-helper instruction.
[[ $# == 7 ]] || exit 125
fifo=$1 ready=$2 helper=$3 config=$4 config_sha=$5 report=$6 mode=$7
[[ "$fifo" == /* && -p "$fifo" && "$ready" == /* && "$helper" == /* && "$config" == /* && "$report" == /* ]] || exit 125
[[ "$config_sha" =~ ^[0-9a-f]{64}$ ]] || exit 125
case "$mode" in
  ordinary) ;;
  context) export BORSUK_NATIVE_CPU_DISPOSABLE_CGROUP=1 ;;
  publication-failure) trap '' XFSZ ;;
  *) exit 125 ;;
esac
: > "$ready"
IFS= read -r release < "$fifo"
[[ "$release" == release ]] || exit 125
exec "$helper" "$config" "$config_sha" "$report"
GATE
cat > "$OUT/scripts/hold.sh" <<'HOLD'
#!/usr/bin/env bash
set -euo pipefail
# ExecStopPost: runs in <unit>/.control, outside payload. Never kills or retries anything.
# The receipt file proves the hold itself read `release` and terminated normally.
[[ $# == 2 && "$1" == /* && -p "$1" && "$2" == /* ]] || exit 125
IFS= read -r release < "$1"
[[ "$release" == release ]] || exit 125
: > "$2"
HOLD
chmod 0500 "$OUT/scripts/gate.sh" "$OUT/scripts/hold.sh"
GATE_SHA=$(sha "$OUT/scripts/gate.sh") HOLD_SHA=$(sha "$OUT/scripts/hold.sh")
DRIVER_SHA=$(sha "${BASH_SOURCE[0]}")
BATCH=$(date -u +%Y%m%dT%H%M%SZ)-$$

plan_procs=0
for row in "${CASES[@]}"; do read -r _ _ _ _ p _ <<<"$row"; plan_procs=$((plan_procs + p)); done
(( plan_procs + RESERVED_COMPILER_TEST == FIXTURE_CAP )) || die "roster counts $plan_procs + $RESERVED_COMPILER_TEST != $FIXTURE_CAP"

jq -n --arg schema "$ADMISSION_SCHEMA" --arg batch "$BATCH" --arg driver "${BASH_SOURCE[0]}" --arg dsha "$DRIVER_SHA" \
  --arg helper "$HELPER" --arg hbin "$HELPER_BIN" --arg hsha "$HELPER_SHA" --argjson hbytes "$HELPER_BYTES" \
  --arg tpath "$TIMEOUT_BIN" --arg tsha "$TIMEOUT_SHA" --argjson tbytes "$TIMEOUT_BYTES" --arg tpkg "$TIMEOUT_PACKAGE" --arg tban "$TIMEOUT_BANNER" \
  --arg gsha "$GATE_SHA" --arg hosha "$HOLD_SHA" --arg sdv "$SDV" --arg kernel "$(uname -r)" --arg bash "$BASH_VERSION" --arg jqv "$(jq --version)" \
  --arg out "$OUT" --arg fs "$(fstype "$OUT")" --argjson mock "$MOCK_ACTIVE" --argjson deadline "$DEADLINE_S" --argjson procs "$plan_procs" \
  --argjson cleanup "$CLEANUP_ALLOWANCE_S" --argjson pubw "$PUBLICATION_ALLOWANCE_S" --argjson hard "$HARD_STOP_S" --argjson outer "$OUTER_RUNTIME_MAX_DECLARED_S" \
  --arg ounit "$OUTER_UNIT" --arg omax "$OUTER_RUNTIME_MAX" --argjson ous "$OUTER_US" \
  '{schema:$schema, batch:$batch, driver:{path:$driver, sha256:$dsha}, mock_seams_active:$mock,
    helper:{given_path:$helper, immutable_copy:$hbin, sha256:$hsha, bytes:$hbytes},
    timeout:{path:$tpath, sha256:$tsha, bytes:$tbytes, package:$tpkg, banner:$tban},
    scripts:{gate_sha256:$gsha, hold_sha256:$hosha},
    host:{systemd:$sdv, kernel:$kernel, bash:$bash, jq:$jqv}, output:{dir:$out, fstype:$fs},
    plan:{invocations:12, fixture_processes:$procs, reserved_compiler_test_child:1, cap:16, deadline_s:$deadline},
    supervision:{fixture_batch_deadline_s:$deadline, cleanup_allowance_s:$cleanup, publication_allowance_s:$pubw, driver_hard_stop_s:$hard,
                 outer_runtime_max_declared_s:$outer, outer_unit:$ounit, outer_runtime_max_usec_raw:$omax, outer_runtime_max_us_parsed:$ous,
                 outer_bound_enforced:"hard_stop_s < RuntimeMaxSec <= declared ceiling",
                 acceptance:"only a checked TERMINAL.json (exit 0, MECHANICS_VERIFIED, matching hashes) authorizes acceptance"}}' \
  > "$OUT/admission.json" || die 'cannot write admission.json'

# ---------- control plumbing ----------
# The nap FIFO must really be a FIFO (a regular file would make every `read -t` return at once
# and busy-loop the poll/watchdog loops): create it FIRST, verify, then open read/write, then
# prove a timed read actually waits.
NAP=$OUT/fifo/nap
mkfifo -m 0600 "$NAP" || die 'cannot create nap fifo'
[[ -p $NAP && ! -L $NAP ]] || die 'nap path is not a FIFO'
exec {NAPFD}<>"$NAP"
[[ -p /proc/self/fd/$NAPFD ]] || die 'nap fd is not a FIFO'
nap_t0=${EPOCHREALTIME/./}
read -r -t 0.05 -u "$NAPFD"; nap_rc=$?
(( nap_rc > 128 && ${EPOCHREALTIME/./} - nap_t0 >= 40000 )) || die "timed read on nap fifo did not wait (rc=$nap_rc)"
UNITS=$OUT/fifo/units; : > "$UNITS"
declare -A P=()
FINALIZED=0
WD='' WD2=''
declare -a INV=() INC=() FAIL=() CASE_FILES=() CASE_STATES=() PUB_ERR=()
CLOSURE=0
EVID_DURABLE=false

unit_cg() { # unit -> ACTUAL ControlGroup: recorded at pre-release, else asked of systemd (bounded); name-derived path only as last resort
  local u=$1 n=${1#"borsuk-ncpu-$BATCH-"} c=''
  [[ -r $OUT/fifo/$n.cg ]] && c=$(<"$OUT/fifo/$n.cg")
  [[ -n $c ]] || c=$(tmo "$SCTL_S" "$SYSTEMCTL" show --no-pager --property=ControlGroup --value -- "$u.service" 2>/dev/null)
  [[ $c == /*"/$u.service" && $c != *..* ]] || c=/system.slice/$u.service
  printf '%s' "$c"
}

# Drain ONLY units this run named. A killed main still runs ExecStopPost, so each unit's own hold
# FIFO is released first; the OWNED cgroup is killed directly (cgroup.kill) BEFORE any blocking manager
# call; every manager call is bounded; then a bounded re-kill loop until the cgroup is unpopulated.
# Never pkill, never a shared cgroup. Returns 1 (and logs CLEANUP_FAILED) when any named cgroup is
# still populated or unreadable afterwards.
kill_owned_units() { # [unit...]: default every unit this run named
  local -a us=("$@")
  (( ${#us[@]} )) || mapfile -t us < "$UNITS"
  local u cg i rf rfd pop rc=0
  for u in "${us[@]}"; do
    [[ $u == "borsuk-ncpu-$BATCH-"* ]] || continue
    cg=$CGROOT$(unit_cg "$u")
    rf=$OUT/fifo/${u#"borsuk-ncpu-$BATCH-"}.release
    if [[ -p $rf ]] && { exec {rfd}<>"$rf"; } 2>/dev/null; then printf 'release\n' >&"$rfd"; exec {rfd}>&-; fi
    [[ -w $cg/cgroup.kill ]] && printf '1\n' > "$cg/cgroup.kill" 2>/dev/null
    tmo "$SCTL_S" "$SYSTEMCTL" kill --kill-whom=all --signal=SIGKILL -- "$u.service" >/dev/null 2>&1
    [[ -w $cg/cgroup.kill ]] && printf '1\n' > "$cg/cgroup.kill" 2>/dev/null
    tmo "$STOP_S" "$SYSTEMCTL" stop -- "$u.service" >/dev/null 2>&1
    for ((i = 0; i < 100; i++)); do
      [[ -e $cg/cgroup.events && $(ev "$cg/cgroup.events" populated) != 0 ]] || break
      (( i % 10 == 9 )) && tmo "$SCTL_S" "$SYSTEMCTL" kill --kill-whom=all --signal=SIGKILL -- "$u.service" >/dev/null 2>&1
      [[ -w $cg/cgroup.kill ]] && printf '1\n' > "$cg/cgroup.kill" 2>/dev/null
      nap 0.1
    done
    tmo "$SCTL_S" "$SYSTEMCTL" reset-failed -- "$u.service" >/dev/null 2>&1
    pop=gone
    [[ -e $cg/cgroup.events ]] && { pop=$(ev "$cg/cgroup.events" populated) || pop=unreadable; }
    if [[ $pop == gone || $pop == 0 ]]; then printf '%s drained=%s\n' "$u" "$pop" >> "$OUT/fifo/cleanup.log"
    else printf '%s CLEANUP_FAILED populated=%s\n' "$u" "$pop" >> "$OUT/fifo/cleanup.log"; rc=1; fi
  done
  return "$rc"
}
wait_cleanup_outcome() { # deadline intervention: wait (cleanup allowance) for the watchdog's CHECKED cleanup outcome
  local i
  for ((i = 0; i < CLEANUP_ALLOWANCE_S * 10; i++)); do
    [[ -e $OUT/fifo/deadline-cleanup-done ]] && return 0
    [[ -e $OUT/fifo/deadline-cleanup-failed ]] && return 1
    nap 0.1
  done
  return 1
}

# Two independent supervisor processes (separate PIDs, so slow cleanup can never delay the hard stop):
#  (1) deadline process: at the fixture-batch deadline it does the checked cleanup of owned units only,
#      arbitrated atomically (mkdir arbiter) against the driver's end-of-admission;
#  (2) hard-stop process: only sleeps/polls; at HARD_STOP_S = fixture deadline + cleanup allowance +
#      publication allowance it kills the driver FIRST (same process only: start time verified), then
#      best-effort cleans owned units and retracts any success artifact. Its post-kill steps can still stall
#      on a stalled manager/filesystem; only the verified outer service RuntimeMaxSec bounds that.
# Both exit when the driver publishes terminal-done (or the hard stop fired).
start_watchdog() {
  local start_us=${EPOCHREALTIME/./} dpid=$$ dstart='' line rest
  local -a f
  local dl_us=$((start_us + DEADLINE_S * 1000000)) hard_us=$((start_us + HARD_STOP_S * 1000000))
  IFS= read -r line 2>/dev/null < "/proc/$dpid/stat" && { rest=${line##*) }; read -ra f <<<"$rest"; dstart=${f[19]:-}; }
  (
    exec 0</dev/null >> "$OUT/watchdog.log" 2>&1
    while [[ ! -e $OUT/fifo/terminal-done && ! -e $OUT/fifo/hard-stop ]]; do
      if (( ${EPOCHREALTIME/./} >= dl_us )); then
        if mkdir "$OUT/fifo/arbiter" 2>/dev/null; then # the driver has not closed admission: the deadline wins
          : > "$OUT/fifo/deadline-fired"
          if kill_owned_units; then : > "$OUT/fifo/deadline-cleanup-done"; else : > "$OUT/fifo/deadline-cleanup-failed"; fi
        fi
        while [[ ! -e $OUT/fifo/terminal-done && ! -e $OUT/fifo/hard-stop ]]; do read -r -t 0.1 -u "$NAPFD" || :; done # handled once
        exit 0
      fi
      read -r -t 0.1 -u "$NAPFD" || :
    done
  ) &
  WD=$!
  (
    exec 0</dev/null >> "$OUT/hardstop.log" 2>&1
    while [[ ! -e $OUT/fifo/terminal-done ]]; do
      if (( ${EPOCHREALTIME/./} >= hard_us )); then
        : > "$OUT/fifo/hard-stop"
        if [[ -n $dstart ]] && IFS= read -r line 2>/dev/null < "/proc/$dpid/stat"; then
          rest=${line##*) }; read -ra f <<<"$rest"
          [[ ${f[19]:-} == "$dstart" ]] && kill -KILL "$dpid" 2>/dev/null # same process only (start time unchanged)
        fi
        kill_owned_units
        rm -f -- "$OUT/result.json" "$OUT/TERMINAL.json"
        printf '{"schema":"%s","state":"INVALID","mock_seams_active":%s,"hard_stop":true,"claims":{"numerical_CPU_accuracy":false,"overhead_GO":false,"performance":false},"note":"independent hard-stop process killed the driver; no acceptance"}\n' \
          "$RESULT_SCHEMA" "$MOCK_ACTIVE" > "$OUT/result.json"
        : > "$OUT/fifo/terminal-done"
        exit 0
      fi
      read -r -t 0.1 -u "$NAPFD" || :
    done
  ) &
  WD2=$!
}

wait_for() { # seconds predicate...: 0 ok, 1 timeout, 2 batch deadline (checked BEFORE the predicate: nothing succeeds after expiry)
  local limit=$(( ${EPOCHREALTIME/./} + $1 * 1000000 )); shift
  while :; do
    dl && return 2
    "$@" && return 0
    (( ${EPOCHREALTIME/./} < limit )) || return 1
    nap "${POLL:-0.005}" # POLL=0.05 for predicates that fork systemctl
  done
}

stat_fields() { # pid -> SF_STATE SF_PPID SF_START (starttime ticks); fails when unreadable
  local line rest f
  IFS= read -r line 2>/dev/null < "$PROC/$1/stat" || return 1
  rest=${line##*) }
  read -ra f <<<"$rest"
  SF_STATE=${f[0]:-} SF_PPID=${f[1]:-} SF_START=${f[19]:-}
  [[ -n $SF_START ]]
}
pid_live() { stat_fields "$1" && [[ $SF_STATE != [ZX] && $SF_START == "$2" ]]; }
limit_of() { awk -v n="$2" 'index($0, n) == 1 { sub(n, ""); print $1, $2 }' "$PROC/$1/limits" 2>/dev/null; }
show_to() { # unit file: full `systemctl show` into file and P[]
  local k v
  P=()
  tmo "$SCTL_S" "$SYSTEMCTL" show --no-pager -- "$1.service" > "$2" 2>/dev/null || return 1
  while IFS='=' read -r k v; do P[$k]=$v; done < "$2"
}
sub_stop_post() { [[ $(tmo "$SCTL_S" "$SYSTEMCTL" show --no-pager --property=SubState --value -- "$unit.service" 2>/dev/null) == stop-post ]]; }
unit_ended() { [[ $(tmo "$SCTL_S" "$SYSTEMCTL" show --no-pager --property=ActiveState --value -- "$unit.service" 2>/dev/null) =~ ^(inactive|failed)$ ]]; }
main_ended() { ! pid_live "$main" "$main_start"; }
payload_drained() { [[ ! -e $pc/cgroup.events ]] || [[ $(ev "$pc/cgroup.events" populated) == 0 ]]; }

# ---------- delegated payload controllers ----------
# Delegate=<controllers> makes them AVAILABLE in the unit cgroup (cgroup.controllers) but does not enable them for its children:
# the kernel exposes controller files in a child only for controllers enabled in the parent's cgroup.subtree_control, and a cgroup
# may enable them only while it holds no process. So, with the gate BLOCKED and before any limit readback or release, the driver
# enables them for the payload in the OWNED unit cgroup ONLY (never an ancestor, never a global file).
# epc_write is the ONE kernel side effect (cgroup-v2 applies a subtree_control write atomically: all listed controllers or none).
epc_write() { printf '%s\n' "+${DELEGATE_CONTROLLERS// / +}" > "$1/cgroup.subtree_control"; }
epc_has() { local want=$1; shift; [[ " $* " == *" $want "* ]]; }
epc_cpus_allowed() { # pid -> the main's own affinity (Cpus_allowed_list of /proc/<pid>/status), independent of the cgroup files
  local k v
  # bare return: the status of the print, so a failed print is a failed read
  while IFS=$' \t' read -r k v; do [[ $k == Cpus_allowed_list: ]] && { printf '%s' "$v"; return; }; done 2>/dev/null < "$PROC/$1/status"
  return 1
}
epc_snapshot() { # uc pc evdir tag : mandatory evidence copies of available/subtree controls and effective cpusets
  local uc=$1 pc=$2 ev=$3 tag=$4 f
  for f in cgroup.controllers cgroup.subtree_control cgroup.procs; do cat "$uc/$f" > "$ev/unit.$f.$tag" || return 1; done
  for f in cgroup.controllers cgroup.subtree_control cgroup.procs; do cat "$pc/$f" > "$ev/payload.$f.$tag" || return 1; done
  for f in unit:$uc payload:$pc; do
    if [[ -e ${f#*:}/cpuset.cpus.effective ]]; then cat "${f#*:}/cpuset.cpus.effective" > "$ev/${f%%:*}.cpuset.cpus.effective.$tag" || return 1
    else printf 'ABSENT\n' > "$ev/${f%%:*}.cpuset.cpus.effective.$tag" || return 1; fi
  done
}
enable_payload_controllers() { # unit cg main main_start evdir : reasons are appended to FAIL; returns 1 on any failure
  local unit=$1 cg=$2 main=$3 mstart=$4 ev=$5 uc pc c x wrc we=''
  local -a want have on pav miss=() snapf=()
  uc=$CGROOT$cg pc=$CGROOT$cg/$PAYLOAD
  read -ra want <<<"$DELEGATE_CONTROLLERS"
  [[ $cg == "/system.slice/$unit.service" && -d $uc && -d $pc && -d $ev ]] || { FAIL+=("epc: not the owned unit cgroup [$cg]"); return 1; }
  pid_live "$main" "$mstart" || { FAIL+=("epc: gated main $main (starttime $mstart) is not live"); return 1; }
  [[ $(rd "$PROC/$main/cgroup") == "0::$cg/$PAYLOAD" ]] || { FAIL+=("epc: gated main is not in payload"); return 1; }
  [[ -r $uc/cgroup.procs ]] || { FAIL+=("epc: unit cgroup.procs unreadable"); return 1; }
  if IFS= read -r x < "$uc/cgroup.procs"; then FAIL+=("epc: unit cgroup is not empty [$x]"); return 1; fi
  printf 'pid=%s starttime=%s location=%s unit_cgroup=%s\n' "$main" "$mstart" "$(rd "$PROC/$main/cgroup")" "$cg" > "$ev/gated-main.txt" \
    || { FAIL+=("epc: cannot record the gated main"); return 1; }
  epc_snapshot "$uc" "$pc" "$ev" before || { FAIL+=("epc: cannot record the before-state"); return 1; }
  x=$(epc_cpus_allowed "$main") && [[ -n $x ]] || { FAIL+=("epc: cannot read the gated main affinity before the write"); return 1; }
  printf '%s\n' "$x" > "$ev/gated-main.cpus_allowed_list.before" || { FAIL+=("epc: cannot record the before-affinity"); return 1; }
  read -ra have < "$uc/cgroup.controllers" || { FAIL+=("epc: unit cgroup.controllers unreadable"); return 1; }
  for c in "${want[@]}"; do epc_has "$c" "${have[@]}" || miss+=("$c"); done
  (( ${#miss[@]} == 0 )) || { FAIL+=("epc: controllers not available in the unit cgroup [${miss[*]}] (have: ${have[*]})"); return 1; }
  # after EVERY attempted write (failed or not) BOTH after-snapshots are attempted before any validation can return; the original
  # write failure and every snapshot failure are all retained in FAIL
  # the write's own diagnostic (stderr) is evidence: the file is established BEFORE the write (no write if it cannot be) and read back after
  : > "$ev/subtree_control.write.stderr" || { FAIL+=("epc: cannot establish the write-stderr evidence file"); return 1; }
  wrc=0; epc_write "$uc" 2>> "$ev/subtree_control.write.stderr" || wrc=$?
  we=$(tr '\n' ' ' < "$ev/subtree_control.write.stderr") || snapf+=("epc: cannot read the write-stderr evidence")
  epc_snapshot "$uc" "$pc" "$ev" after || snapf+=("epc: cannot record the after-state")
  if x=$(epc_cpus_allowed "$main") && [[ -n $x ]]; then
    printf '%s\n' "$x" > "$ev/gated-main.cpus_allowed_list.after" || snapf+=("epc: cannot record the after-affinity")
  else
    x='<unreadable>'; snapf+=("epc: cannot read the gated main affinity after the write")
    printf '%s\n' "$x" > "$ev/gated-main.cpus_allowed_list.after" || snapf+=("epc: cannot record the unreadable after-affinity")
  fi
  (( wrc == 0 )) || { FAIL+=("epc: checked subtree_control write failed (rc $wrc) [stderr: ${we:0:300}]" "${snapf[@]}"); return 1; }
  (( ${#snapf[@]} == 0 )) || { FAIL+=("${snapf[@]}"); return 1; }
  read -ra on < "$uc/cgroup.subtree_control" || { FAIL+=("epc: subtree_control unreadable after the write"); return 1; }
  read -ra pav < "$pc/cgroup.controllers" || { FAIL+=("epc: payload cgroup.controllers unreadable after the write"); return 1; }
  miss=(); for c in "${want[@]}"; do epc_has "$c" "${on[@]}" || miss+=("$c"); done
  (( ${#miss[@]} == 0 )) || { FAIL+=("epc: not enabled in the unit subtree_control [${miss[*]}] (on: ${on[*]})"); return 1; }
  miss=(); for c in "${want[@]}"; do epc_has "$c" "${pav[@]}" || miss+=("$c"); done
  (( ${#miss[@]} == 0 )) || { FAIL+=("epc: not listed in the payload cgroup.controllers [${miss[*]}]"); return 1; }
  [[ $(rd "$uc/cpuset.cpus.effective") == 0 && $(rd "$pc/cpuset.cpus.effective") == 0 ]] \
    || { FAIL+=("epc: effective cpuset is not exactly 0 (unit [$(rd "$uc/cpuset.cpus.effective")] payload [$(rd "$pc/cpuset.cpus.effective")])"); return 1; }
  # independent of the cgroup files: the still-gated main's own affinity read above (CPU list only; Mems_allowed_list is deliberately
  # NOT frozen, CPU0 may belong to another NUMA node on a supported host)
  [[ $x == 0 ]] || { FAIL+=("epc: gated main Cpus_allowed_list is [$x], not exactly 0"); return 1; }
}

# ---------- validators over closed artifacts (also used by the mutation controls) ----------
receipt_ok() { # report stderr config_sha: exactly one matching REPORT_SYNCED collector receipt
  local r
  r=$(jq -Rc 'try fromjson catch empty | select(type == "object" and .schema == "'"$COLLECTOR_SCHEMA"'")' "$2" 2>/dev/null) || return 1
  [[ -n $r && $(printf '%s\n' "$r" | wc -l) == 1 ]] || return 1
  printf '%s\n' "$r" | jq -e --arg sha "$3" --slurpfile rep "$1" '
    $rep[0] as $r | .collector_status == "REPORT_SYNCED" and .error == null and .config_sha256 == $sha
    and $r.status == "REAPED_ACCOUNTING_ONLY" and $r.config_sha256 == $sha
    and .raw_child_wait_status == $r.child_status.raw_wait_status
    and .child_exit_code == $r.child_status.exit_code and .child_signal == $r.child_status.signal' >/dev/null 2>&1
}
late_ok() { # report stdout: premature fake_terminal_cpu_ns must understate the lifecycle CPU by >=20 ms
  local f
  f=$(jq -rs 'map(select(type == "object" and .fixture == "late" and has("fake_terminal_cpu_ns"))) | first | .fake_terminal_cpu_ns // empty' "$2" 2>/dev/null) || return 1
  [[ $f =~ ^[0-9]+$ ]] || return 1
  jq -e --argjson f "$f" '.child_cpu.cpu_ns - $f >= 20000000' "$1" >/dev/null 2>&1
}
report_binds() { # report config_sha exe_sha exe_bytes argc
  jq -e --arg c "$2" --arg e "$3" --argjson b "$4" --argjson n "$5" \
    '.schema == "native-process-cpu-report-v1" and .status == "REAPED_ACCOUNTING_ONLY" and .config_sha256 == $c
     and .executable_sha256 == $e and .executable_bytes == $b and .argument_count == $n' "$1" >/dev/null 2>&1
}

# ---------- one invocation ----------
# Returns 3 when pre-release admission failed (nothing was released; the batch stops).
run_case() {
  local idx=$1 id=$2 gmode=$3 exp_exit=$4 exp_sig=$5 procs=$6 acct=$7
  local nn name unit cdir scratch cfg report so se go ready rel holdok exe exe_bytes exe_sha cfg_sha rc=0
  printf -v nn '%02d' "$idx"
  name=$nn-$id unit=borsuk-ncpu-$BATCH-$nn-$id
  cdir=$OUT/cases/$name scratch=$cdir/scratch cfg=$cdir/config.json
  report=$cdir/report.json so=$cdir/stdout se=$cdir/stderr
  go=$OUT/fifo/$name.go ready=$OUT/fifo/$name.ready rel=$OUT/fifo/$name.release holdok=$OUT/fifo/$name.hold-ok
  INV=() INC=() FAIL=()
  RELEASED=false REL_US=null CLOSURE=0 EVID_DURABLE=false PRE_VALIDATED=0 CAPTURED=0 OBSERVED=0
  local main='' main_start='' cg='' uc='' pc='' GOFD='' RELFD='' f m
  local -a args=() procs_in ifaces
  mkdir -m 0700 -- "$cdir" "$scratch" "$cdir/cg" || { inv CASE_DIR_CREATE_FAILED; finish_case; return 3; }
  { : > "$so" && : > "$se" && chmod 0600 "$so" "$se"; } || { inv CASE_OUTPUT_FILES_CREATE_FAILED; finish_case; return 3; }
  mkfifo -m 0600 -- "$go" "$rel" || { inv FIFO_CREATE_FAILED; finish_case; return 3; }
  if [[ $id == timeout-KILL ]]; then
    exe=$TIMEOUT_BIN exe_bytes=$TIMEOUT_BYTES exe_sha=$TIMEOUT_SHA
    args=(--signal=KILL 0.2 "$HELPER_BIN" --fixture delay)
  else
    exe=$HELPER_BIN exe_bytes=$HELPER_BYTES exe_sha=$HELPER_SHA
    [[ $id == publication-failure ]] && args=(--fixture pulse) || args=(--fixture "$id")
  fi
  printf '%s\0' "${args[@]}" | jq -Rsc --arg p "$exe" --argjson b "$exe_bytes" --arg s "$exe_sha" \
    '{schema: "native-process-cpu-config-v1", executable: {path: $p, bytes: $b, sha256: $s}, args: (split("\u0000")[:-1])}' > "$cfg" \
    || { inv CONFIG_BUILD_FAILED; finish_case; return 3; }
  chmod 0400 "$cfg" || { inv CONFIG_CHMOD_FAILED; finish_case; return 3; }
  cfg_sha=$(sha "$cfg") && printf '%s\n' "$cfg_sha" > "$cdir/config.sha256" || { inv CONFIG_HASH_FAILED; finish_case; return 3; }
  exec {GOFD}<>"$go" {RELFD}<>"$rel" || { inv FIFO_OPEN_FAILED; finish_case; return 3; }

  local -a sr=(
    --unit="$unit" --service-type=exec
    -p User=root -p Restart=no
    -p "Delegate=$DELEGATE_CONTROLLERS" -p "DelegateSubgroup=$PAYLOAD"
    -p CPUAccounting=yes -p CPUQuota=100% -p AllowedCPUs=0
    -p "MemoryMax=$MEMORY_MAX" -p MemorySwapMax=0 -p "TasksMax=$PIDS_MAX"
    -p PrivateNetwork=yes -p 'SystemCallFilter=~@network-io'
    -p LimitCORE=0 -p KillMode=process -p TimeoutStopSec=60s
  )
  [[ $gmode == publication-failure ]] && sr+=(-p LimitFSIZE=1024)
  sr+=(
    -p "StandardOutput=append:$so" -p "StandardError=append:$se"
    -p "ExecStopPost=/bin/bash --noprofile --norc $OUT/scripts/hold.sh $rel $holdok"
    -- /usr/bin/env -i PATH=/usr/bin:/bin LC_ALL=C HOME=/root SHELL=/bin/bash "TMPDIR=$scratch"
    /bin/bash --noprofile --norc "$OUT/scripts/gate.sh" "$go" "$ready" "$HELPER_BIN" "$cfg" "$cfg_sha" "$report" "$gmode"
  )
  { printf '%s\n' "$SYSTEMD_RUN"; printf '%s\n' "${sr[@]}"; } > "$cdir/systemd-run.argv" || { inv ARGV_RECORD_FAILED; finish_case; return 3; }
  # ownership is registered (checked, synced to the list) BEFORE launch so every actually-created unit is drainable
  printf '%s\n' "$unit" >> "$UNITS" || { inv UNIT_REGISTRATION_FAILED; finish_case; return 3; }
  tmo "$LAUNCH_S" "$SYSTEMD_RUN" "${sr[@]}" > "$cdir/systemd-run.stdout" 2> "$cdir/systemd-run.stderr"; rc=$?
  if (( rc != 0 )); then # a failed/timed-out launcher does not prove nothing was created: drain the exact unit
    inv "LAUNCH_FAILED_rc$rc"
    kill_owned_units "$unit" || inv "CLEANUP_FAILED:$unit"
    finish_case; return 3
  fi

  # ----- pre-release validation: the helper has NOT run; any failure kills only this unit -----
  if ! wait_for 10 test -e "$ready"; then inv GATE_NOT_READY; kill_owned_units "$unit" || inv "CLEANUP_FAILED:$unit"; finish_case; return 3; fi
  show_to "$unit" "$cdir/show.pre-release.txt" || FAIL+=(show_pre_release_unreadable)
  main=${P[MainPID]:-0} cg=${P[ControlGroup]:-}
  [[ $cg == /*"/$unit.service" && $cg != *..* ]] && { printf '%s\n' "$cg" > "$OUT/fifo/$name.cg" || FAIL+=(controlgroup_record_failed); }
  uc=$CGROOT$cg pc=$CGROOT$cg/$PAYLOAD
  eq show.ActiveState "${P[ActiveState]:-}" active
  eq show.SubState "${P[SubState]:-}" running
  eq show.Type "${P[Type]:-}" exec
  eq show.KillMode "${P[KillMode]:-}" process
  eq show.Delegate "${P[Delegate]:-}" yes
  eq show.DelegateSubgroup "${P[DelegateSubgroup]:-}" $PAYLOAD
  eq show.ControlGroup "$cg" "/system.slice/$unit.service"
  [[ $main =~ ^[1-9][0-9]*$ ]] || FAIL+=("MainPID [$main]")
  [[ -d $uc && -d $pc ]] || FAIL+=(payload_subgroup_missing)
  if stat_fields "$main"; then main_start=$SF_START; else FAIL+=(main_stat_unreadable); fi
  eq main.cgroup "$(rd "$PROC/$main/cgroup")" "0::$cg/$PAYLOAD"
  mapfile -t procs_in 2>/dev/null < "$pc/cgroup.procs"; eq payload.procs "${procs_in[*]:-}" "$main"
  mapfile -t procs_in 2>/dev/null < "$uc/cgroup.procs"; eq unit.procs "${procs_in[*]:-}" ""
  # Delegate= only made the controllers AVAILABLE: enable them for the payload (owned unit cgroup only) BEFORE any readback or release
  (( ${#FAIL[@]} )) || enable_payload_controllers "$unit" "$cg" "$main" "$main_start" "$cdir/cg"
  eq cpu.max "$(rd "$uc/cpu.max")" '100000 100000'
  eq cpuset.cpus "$(rd "$uc/cpuset.cpus")" 0
  eq cpuset.effective.unit "$(rd "$uc/cpuset.cpus.effective")" 0
  eq cpuset.effective.payload "$(rd "$pc/cpuset.cpus.effective")" 0
  eq memory.max "$(rd "$uc/memory.max")" $MEMORY_MAX
  eq memory.swap.max "$(rd "$uc/memory.swap.max")" 0
  eq pids.max "$(rd "$uc/pids.max")" $PIDS_MAX
  for f in cpu.stat cgroup.events memory.events pids.events; do [[ -r $pc/$f ]] || FAIL+=("payload/$f unreadable"); done
  eq main.uid "$(awk '/^Uid:/ { print $2, $3, $4, $5 }' "$PROC/$main/status" 2>/dev/null)" "$ROOT_UID $ROOT_UID $ROOT_UID $ROOT_UID"
  [[ $(tr '\0' ' ' < "$PROC/$main/cmdline" 2>/dev/null) == *"$OUT/scripts/gate.sh"* ]] || FAIL+=(main_is_not_gate)
  [[ $(tr '\0' '\n' < "$PROC/$main/environ" 2>/dev/null) == *"TMPDIR=$scratch"* ]] || FAIL+=(tmpdir_not_scratch)
  eq rlimit.core "$(limit_of "$main" 'Max core file size')" '0 0'
  local sigign; sigign=$(awk '/^SigIgn:/ { print $2 }' "$PROC/$main/status" 2>/dev/null)
  if [[ $gmode == publication-failure ]]; then
    eq rlimit.fsize "$(limit_of "$main" 'Max file size')" '1024 1024'
    [[ $sigign =~ ^[0-9a-f]+$ ]] && (( (16#$sigign >> 24) & 1 )) || FAIL+=(sigxfsz_not_ignored)
  else
    eq rlimit.fsize "$(limit_of "$main" 'Max file size')" 'unlimited unlimited'
    [[ $sigign =~ ^[0-9a-f]+$ ]] && (( ((16#$sigign >> 24) & 1) == 0 )) || FAIL+=(sigxfsz_unexpectedly_ignored)
  fi
  [[ $(readlink "$PROC/$main/ns/net") != "$(readlink "$PROC/self/ns/net")" ]] || FAIL+=(network_namespace_shared)
  mapfile -t ifaces < <(awk -F: 'NR > 2 { gsub(/ /, "", $1); print $1 }' "$PROC/$main/net/dev" 2>/dev/null)
  eq net.interfaces "${ifaces[*]:-}" lo
  [[ $(fstype "$cdir") == ext4 && $(fstype "$scratch") == ext4 ]] || FAIL+=(case_not_ext4)
  owner_ok "$cdir" && owner_ok "$scratch" || FAIL+=(case_dir_ownership)
  eq helper.sha "$(sha "$HELPER_BIN")" "$HELPER_SHA"
  elf_ok "$HELPER_BIN" || FAIL+=(helper_not_elf)
  if [[ $id == timeout-KILL ]]; then
    eq timeout.sha "$(sha "$TIMEOUT_BIN")" "$TIMEOUT_SHA"
    elf_ok "$TIMEOUT_BIN" || FAIL+=(timeout_not_elf)
  fi
  eq config.sha "$(sha "$cfg")" "$cfg_sha"
  eq config.sha.binds.executable "$(jq -r .executable.sha256 "$cfg" 2>/dev/null)" "$exe_sha"
  eq gate.sha "$(sha "$OUT/scripts/gate.sh")" "$GATE_SHA"
  eq hold.sha "$(sha "$OUT/scripts/hold.sh")" "$HOLD_SHA"
  [[ ! -e $report ]] || FAIL+=(report_not_absent)
  jq -n --argjson pid "${main:-0}" --arg start "$main_start" --arg cg "$(rd "$PROC/$main/cgroup")" --arg uid "$(awk '/^Uid:/ { print $2, $3, $4, $5 }' "$PROC/$main/status" 2>/dev/null)" \
    --arg core "$(limit_of "$main" 'Max core file size')" --arg fsize "$(limit_of "$main" 'Max file size')" --arg sigign "$sigign" \
    --arg netns "$(readlink "$PROC/$main/ns/net")" --arg ifaces "${ifaces[*]:-}" --arg cmd "$(tr '\0' ' ' < "$PROC/$main/cmdline" 2>/dev/null)" \
    '{pid:$pid, starttime_ticks:$start, cgroup:$cg, uid:$uid, rlimit_core:$core, rlimit_fsize:$fsize, sig_ign:$sigign, netns:$netns, interfaces:$ifaces, cmdline:$cmd}' > "$cdir/proc.main.json" \
    && PRE_VALIDATED=1 || FAIL+=(proc_main_evidence_write_failed)
  cat -- "$pc/cpu.stat" > "$cdir/cg/payload.cpu.stat.initial" 2>/dev/null || FAIL+=(initial_cpu_stat_unreadable)
  if (( ${#FAIL[@]} )); then
    for m in "${FAIL[@]}"; do inv "PRE_RELEASE:$m"; done
    kill_owned_units "$unit" || inv "CLEANUP_FAILED:$unit"; finish_case; return 3
  fi
  dl && { inc BATCH_DEADLINE_BEFORE_RELEASE; kill_owned_units "$unit" || inv "CLEANUP_FAILED:$unit"; finish_case; return 3; }

  # ----- release: first moment the native helper can run -----
  if ! printf 'release\n' >&"$GOFD"; then
    inv RELEASE_WRITE_FAILED; kill_owned_units "$unit" || inv "CLEANUP_FAILED:$unit"; finish_case; return 3
  fi
  REL_US=${EPOCHREALTIME/./} RELEASED=true
  exec {GOFD}>&-; GOFD=''

  wait_for 30 main_ended; rc=$?
  if (( rc == 0 )) && [[ $id == survivor ]]; then observe_survivor; fi
  (( rc == 0 )) && { POLL=0.05 wait_for 10 sub_stop_post; rc=$?; }
  if (( rc == 0 )); then wait_for 10 payload_drained; rc=$?; fi
  if (( rc == 2 )); then
    inc BATCH_DEADLINE
    wait_cleanup_outcome || inv DEADLINE_CLEANUP_NOT_COMPLETED # the watchdog drains every owned unit
  elif (( rc == 1 )); then
    inv 'TIMEOUT_WAITING_FOR_MAIN_STOP_POST_OR_DRAIN'; CLOSURE=1
    kill_owned_units "$unit" || inv "CLEANUP_FAILED:$unit" # KillMode=process leaves leftovers: drain this exact unit only
  else capture_stop_post; fi

  persist_evidence # hash + sync with checked errors BEFORE stop-post is released
  printf 'release\n' >&"$RELFD" || { inv HOLD_RELEASE_WRITE_FAILED; CLOSURE=1; }
  exec {RELFD}>&-; RELFD=''
  if POLL=0.05 wait_for 15 unit_ended; then
    show_to "$unit" "$cdir/show.final.txt" || inv SHOW_FINAL_UNREADABLE
    tmo "$SCTL_S" "$SYSTEMCTL" reset-failed -- "$unit.service" >/dev/null 2>&1
    # independent stop-post termination check: the hold itself must have read `release` and exited normally
    if (( rc == 0 )) && [[ ! -e $holdok ]]; then inv HOLD_DID_NOT_COMPLETE; CLOSURE=1; fi
    if ! payload_drained; then
      inv UNIT_ENDED_WITH_POPULATED_PAYLOAD; CLOSURE=1
      kill_owned_units "$unit" || inv "CLEANUP_FAILED:$unit"
    fi
  elif dl; then # deadline fired while waiting for the unit to end after the hold release: recorded, never swallowed
    inc BATCH_DEADLINE_AFTER_HOLD_RELEASE
    wait_cleanup_outcome || inv DEADLINE_CLEANUP_NOT_COMPLETED
  else
    inv UNIT_DID_NOT_END_AFTER_HOLD_RELEASE; CLOSURE=1
    kill_owned_units "$unit" || inv "CLEANUP_FAILED:$unit"
  fi
  finish_case
  (( CLOSURE )) && return 4 # closure failure: never launch another case on top of an unverified unit
  return 0
}

persist_evidence() { # evidence is not durable until the hash list AND the files/dir are synced with checked errors
  EVID_DURABLE=false
  if ! ( cd "$cdir" && find . -type f ! -name 'evidence.sha256*' -print0 | sort -z | xargs -0 sha256sum ) > "$cdir/evidence.sha256.tmp" 2>/dev/null \
    || ! mv -f -T -- "$cdir/evidence.sha256.tmp" "$cdir/evidence.sha256"; then
    inv EVIDENCE_HASH_FAILED; CLOSURE=1; return 1
  fi
  if ! sync -f -- "$cdir/evidence.sha256" "$cdir" 2>/dev/null; then inv EVIDENCE_SYNC_FAILED; CLOSURE=1; return 1; fi
  EVID_DURABLE=true
}

observe_survivor() { # called right after the unit main PID ended
  local desc comm up dcg pop margin=null
  if ! [[ $(<"$so") =~ \"surviving_descendant_pid\":([0-9]+) ]]; then inv SURVIVOR_PID_NOT_EMITTED; return; fi
  desc=${BASH_REMATCH[1]}
  if stat_fields "$desc" && [[ $SF_STATE != [ZX] ]]; then
    dcg=$(rd "$PROC/$desc/cgroup") comm=$(rd "$PROC/$desc/comm") pop=$(ev "$pc/cgroup.events" populated)
    read -r up _ < /proc/uptime
    [[ $CLK_TCK == 100 ]] && margin=$(( (SF_START + 200 - 10#${up/./}) * 10 ))
    jq -n --argjson pid "$desc" --arg state "$SF_STATE" --argjson ppid "$SF_PPID" --arg start "$SF_START" --arg cg "$dcg" --arg comm "$comm" \
      --arg pop "$pop" --argjson margin "$margin" --arg at "$EPOCHREALTIME" \
      '{pid:$pid, state:$state, ppid:$ppid, starttime_ticks:$start, cgroup:$cg, comm:$comm, payload_populated:$pop, observed_after_main_ended:true,
        observed_at_epoch:$at, natural_exit_margin_ms_10ms_resolution:$margin}' > "$cdir/survivor.json" \
      && OBSERVED=1 || inv EVIDENCE_WRITE_FAILED:survivor
    [[ $dcg == "0::$cg/$PAYLOAD" ]] || inv "SURVIVOR_MISPLACED:$dcg"
    [[ $pop == 1 ]] || inv "SURVIVOR_PAYLOAD_NOT_POPULATED:$pop"
  else
    inc SURVIVOR_NOT_OBSERVED_LIVE
    jq -n --argjson pid "$desc" '{pid:$pid, observed_live:false, observed_after_main_ended:true}' > "$cdir/survivor.json" \
      && OBSERVED=1 || inv EVIDENCE_WRITE_FAILED:survivor
  fi
}

capture_stop_post() {
  local f hp
  show_to "$unit" "$cdir/show.stop-post.txt" || inv SHOW_STOP_POST_UNREADABLE
  if ! [[ -d $pc && -r $pc/cgroup.events ]]; then
    inv PAYLOAD_SUBGROUP_VANISHED; CLOSURE=1
    kill_owned_units "$unit" || inv "CLEANUP_FAILED:$unit"
    return
  fi
  CAPTURED=1
  for f in cpu.stat cgroup.events cgroup.procs memory.events pids.events memory.peak memory.swap.peak; do
    if [[ -e $pc/$f ]]; then cat -- "$pc/$f" > "$cdir/cg/payload.$f.final" 2>/dev/null || inv "READBACK_FAILED:payload/$f"
    elif [[ $f != memory.*peak ]]; then inv "READBACK_FAILED:payload/$f"; fi
    if [[ -e $uc/$f ]]; then cat -- "$uc/$f" > "$cdir/cg/unit.$f.final" 2>/dev/null || inv "READBACK_FAILED:unit/$f"; fi
  done
  if [[ $(ev "$pc/cgroup.events" populated) != 0 ]]; then
    inv PAYLOAD_NOT_DRAINED; CLOSURE=1
    kill_owned_units "$unit" || inv "CLEANUP_FAILED:$unit"
  fi
  for f in oom oom_kill; do [[ $(ev "$pc/memory.events" "$f") == 0 && $(ev "$uc/memory.events" "$f") == 0 ]] || inv "MEMORY_EVENT:$f"; done
  [[ $(ev "$pc/pids.events" max) == 0 && $(ev "$uc/pids.events" max) == 0 ]] || inv 'PIDS_EVENT:max'
  hp=${P[ControlPID]:-0}
  if [[ $hp =~ ^[1-9][0-9]*$ ]]; then
    [[ $(rd "$PROC/$hp/cgroup") == "0::$cg/.control" ]] || inv "HOLD_NOT_IN_CONTROL_CGROUP:$(rd "$PROC/$hp/cgroup")"
  else inv 'HOLD_NOT_RUNNING'; fi
  jq -n --arg pid "${P[ExecMainPID]:-}" --arg code "${P[ExecMainCode]:-}" --arg status "${P[ExecMainStatus]:-}" --arg result "${P[Result]:-}" \
    --arg active "${P[ActiveState]:-}" --arg sub "${P[SubState]:-}" --arg ctl "$hp" --arg ts "${P[ExecMainExitTimestampMonotonic]:-}" \
    '{ExecMainPID:$pid, ExecMainCode:$code, ExecMainStatus:$status, Result:$result, ActiveState:$active, SubState:$sub, ControlPID:$ctl,
      ExecMainExitTimestampMonotonic:$ts, note:"systemd view of the unit main process (the outer helper); distinct from the native report"}' > "$cdir/exec-main.json" \
    || inv EVIDENCE_WRITE_FAILED:exec-main
  FAIL=() # reset BEFORE the PID check (it used to be wiped by the status branches)
  eq exec.main.pid "${P[ExecMainPID]:-}" "$main"
  if [[ $exp_sig != - ]]; then eq ExecMainCode "${P[ExecMainCode]:-}" 2; eq ExecMainStatus "${P[ExecMainStatus]:-}" "$exp_sig"
  else eq ExecMainCode "${P[ExecMainCode]:-}" 1; eq ExecMainStatus "${P[ExecMainStatus]:-}" "$exp_exit"; fi
  for m in "${FAIL[@]}"; do inv "EXEC_MAIN:$m"; done
  check_native_outputs
}

# Oracles are read from the helper's own report/receipt/stdout; nothing is recomputed here.
check_native_outputs() {
  local rcpt
  rcpt=$(jq -Rc 'try fromjson catch empty | select(type == "object" and .schema == "'"$COLLECTOR_SCHEMA"'")' "$se" 2>/dev/null) || rcpt=''
  printf '%s\n' "${rcpt:-null}" > "$cdir/collector-receipt.json" || inv EVIDENCE_WRITE_FAILED:collector-receipt
  jq -s . "$so" > "$cdir/stdout.json" 2>/dev/null || { printf 'null\n' > "$cdir/stdout.json"; inv STDOUT_NOT_JSON_LINES; }
  diagnostic
  if [[ $gmode == publication-failure ]]; then
    local sz; sz=$(stat -c %s -- "$report" 2>/dev/null || echo -1)
    (( sz >= 1 && sz <= 1024 )) || inv "PUBLICATION_FAILURE_PARTIAL_REPORT_SIZE:$sz"
    jq -e . "$report" >/dev/null 2>&1 && inv PUBLICATION_FAILURE_REPORT_UNEXPECTEDLY_COMPLETE
    printf '%s\n' "$rcpt" | jq -e --arg sha "$cfg_sha" '.collector_status == "ACCOUNTING_OR_PUBLICATION_FAILED" and .config_sha256 == $sha
      and .raw_child_wait_status == 0 and .child_exit_code == 0 and .child_signal == null
      and ((.error // "") | test("File too large|os error 27"))' >/dev/null 2>&1 || inv PUBLICATION_FAILURE_RECEIPT_MISMATCH
    jq -e 'any(.[]; .fixture == "pulse" and .injected_cpu_ns >= 2000000)' "$cdir/stdout.json" >/dev/null 2>&1 || inv PUBLICATION_FAILURE_PULSE_NOT_EXECUTED_PRE_SPAWN_FAILURE
    return
  fi
  receipt_ok "$report" "$se" "$cfg_sha" || inv COLLECTOR_RECEIPT_OR_REPORT_MISSING_OR_MISMATCHED
  report_binds "$report" "$cfg_sha" "$exe_sha" "$exe_bytes" "${#args[@]}" || inv REPORT_DOES_NOT_BIND_CONFIG
  if [[ $exp_sig != - ]]; then
    jq -e --argjson s "$exp_sig" '.child_status.signal == $s and .child_status.exit_code == null and .child_status.raw_wait_status == $s' "$report" >/dev/null 2>&1 || inv "CHILD_STATUS_NOT_SIGNAL_$exp_sig"
  else
    jq -e --argjson c "$exp_exit" '.child_status.exit_code == $c and .child_status.signal == null' "$report" >/dev/null 2>&1 || inv "CHILD_STATUS_NOT_EXIT_$exp_exit"
  fi
  local o='.'
  case $id in
    pulse) o='length == 1 and .[0].fixture == "pulse" and .[0].injected_cpu_ns >= 2000000' ;;
    threads) o='length == 1 and .[0].fixture == "threads" and .[0].injected_cpu_ns >= 5000000' ;;
    waited) o='length == 2 and any(.[]; .fixture == "pulse") and any(.[]; .fixture == "waited" and (.waited_child_cpu_ns | type) == "number")' ;;
    io) o='length == 1 and .[0].fixture == "io"' ;;
    late) o='length == 2 and .[0].query_cpu_ns >= 2000000 and (.[0].fake_terminal_cpu_ns | type) == "number" and .[1].fixture == "late" and .[1].injected_cpu_ns >= 20000000'
      late_ok "$report" "$so" || inv LATE_PREMATURE_SNAPSHOT_NOT_UNDERSTATED ;;
    delay) o='length == 1 and .[0].fixture == "delay"'
      jq -e '.lifecycle_wall_bound_ns >= 2000000000' "$report" >/dev/null 2>&1 || inv DELAY_WALL_BELOW_2S ;;
    nonzero) o='length == 1 and .[0].fixture == "nonzero"'
      [[ $(head -n1 "$se") == 'nonzero fixture stderr without LF' ]] || inv NONZERO_UNTERMINATED_STDERR_NOT_PRESERVED ;;
    survivor) o='length == 1 and .[0].fixture == "survivor" and (.[0].surviving_descendant_pid | type) == "number"' ;;
    unwaited) o='length == 3 and any(.[]; .fixture == "late" and .injected_cpu_ns >= 20000000) and any(.[]; .fixture == "unwaited" and .unwaited_event.observed_with == "WEXITED|WNOWAIT" and .unwaited_event.si_status == 0)'
      unwaited_witness ;;
    sigpipe) o='length == 1 and .[0].event == "before_signal" and .[0].expected_signal == 13 and .[0].injected_cpu_ns >= 2000000' ;;
    timeout-KILL) o='length == 0' ;;
  esac
  jq -e "$o" "$cdir/stdout.json" >/dev/null 2>&1 || inv "ORACLE_MISMATCH:$id"
}

cpu_usec() { ev "$1" usage_usec; }
# Observed separation only (explicitly NOT an error bound). The payload CPU is the checked
# FINAL - INITIAL usage_usec delta (the absolute final counter includes pre-release gate work);
# both raw samples are kept; decreasing, malformed or absurd values are refused.
diagnostic() {
  local i f cu why='' d=null ns=null ij=null fj=null cj=null child
  i=$(cpu_usec "$cdir/cg/payload.cpu.stat.initial") f=$(cpu_usec "$cdir/cg/payload.cpu.stat.final")
  cu=$(cpu_usec "$cdir/cg/unit.cpu.stat.final")
  if ! [[ $i =~ ^[0-9]{1,15}$ && $f =~ ^[0-9]{1,15}$ ]]; then why=cpu_stat_missing_or_malformed
  elif (( f < i )); then why=cpu_stat_decreased
  elif (( f - i > 3600000000 )); then why=cpu_stat_delta_over_one_hour
  else ij=$i fj=$f d=$((f - i)) ns=$(( (f - i) * 1000 )); fi
  [[ $cu =~ ^[0-9]{1,15}$ ]] && cj=$cu
  [[ -z $why ]] || inv "CPU_STAT_INVALID:$why"
  child=$(jq -r '.child_cpu.cpu_ns // empty' "$report" 2>/dev/null)
  [[ $child =~ ^[0-9]{1,18}$ ]] || child=null
  jq -n --argjson ij "$ij" --argjson fj "$fj" --argjson d "$d" --argjson ns "$ns" --argjson cj "$cj" --argjson child "$child" \
    --arg why "$why" --argjson mock "$MOCK_ACTIVE" \
    '{payload_usage_usec_initial:$ij, payload_usage_usec_final:$fj, payload_delta_usec:$d, payload_delta_ns:$ns,
      unit_usage_usec_final_includes_hold:$cj, report_child_cpu_ns:$child,
      payload_delta_minus_report_child_ns:(if $ns != null and $child != null then $ns - $child else null end),
      refused:(if $why == "" then null else $why end), mock_seams_active:$mock,
      scope:"observed separation only; includes gate, outer helper and omitted final costs; not an error bound, not completeness"}' \
    > "$cdir/diagnostic.json" 2>/dev/null || { printf 'null\n' > "$cdir/diagnostic.json"; inv DIAGNOSTIC_BUILD_FAILED; }
}
unwaited_witness() { # known >=20ms descendant CPU is in the payload cgroup delta but absent from the wait4 child CPU
  local inj
  inj=$(jq -r 'map(select(.fixture == "late" and has("injected_cpu_ns"))) | first | .injected_cpu_ns // empty' "$cdir/stdout.json" 2>/dev/null)
  [[ $inj =~ ^[0-9]{1,18}$ ]] || inj=0
  jq -e --argjson i "$inj" '.payload_delta_minus_report_child_ns != null and .payload_delta_minus_report_child_ns >= $i and $i >= 20000000' "$cdir/diagnostic.json" >/dev/null 2>&1 \
    || inv UNWAITED_WITNESS_NOT_DEMONSTRATED
}

frag_object() { jq -e 'type == "object"' "$cdir/$1.json" >/dev/null 2>&1; }
finish_case() {
  local state=MECHANICS_VERIFIED f cj=$cdir/case.json ninv ninc
  # mandatory evidence fragments: exactly one JSON value each (an empty file would slurp as [] and hide a missing
  # observation); required fragments must be objects. Done BEFORE the state is derived.
  for f in proc.main exec-main survivor diagnostic collector-receipt; do
    if [[ ! -e $cdir/$f.json ]]; then printf 'null\n' > "$cdir/$f.json" || inv "EVIDENCE_WRITE_FAILED:$f"
    elif ! jq -s -e 'length == 1' "$cdir/$f.json" >/dev/null 2>&1; then
      inv "EVIDENCE_SHAPE:$f"; printf 'null\n' > "$cdir/$f.json" || inv "EVIDENCE_WRITE_FAILED:$f"
    fi
  done
  (( PRE_VALIDATED )) && { frag_object proc.main || inv EVIDENCE_MISSING:proc.main; }
  (( CAPTURED )) && { frag_object exec-main || inv EVIDENCE_MISSING:exec-main; }
  (( CAPTURED )) && { frag_object collector-receipt || inv EVIDENCE_MISSING:collector-receipt; }
  (( CAPTURED )) && { frag_object diagnostic || inv EVIDENCE_MISSING:diagnostic; }
  (( OBSERVED )) && { frag_object survivor || inv EVIDENCE_MISSING:survivor; }
  (( ${#INC[@]} )) && state=INCONCLUSIVE
  (( ${#INV[@]} )) && state=INVALID
  if ! { jq -n --arg schema "$CASE_SCHEMA" --arg id "$id" --arg unit "$unit.service" --arg state "$state" \
    --argjson inv "$(jarr "${INV[@]}")" --argjson inc "$(jarr "${INC[@]}")" --argjson released "$RELEASED" --argjson rel "$REL_US" \
    --arg gmode "$gmode" --arg ee "$exp_exit" --arg es "$exp_sig" --argjson procs "$procs" --arg acct "$acct" \
    --arg cfgsha "${cfg_sha:-}" --arg exe "${exe:-}" --argjson args "$(jarr "${args[@]}")" \
    --argjson mock "$MOCK_ACTIVE" --argjson durable "$EVID_DURABLE" --arg dirname "${name:-$id}" \
    --slurpfile main "$cdir/proc.main.json" --slurpfile exec "$cdir/exec-main.json" --slurpfile sv "$cdir/survivor.json" \
    --slurpfile diag "$cdir/diagnostic.json" --slurpfile rcpt "$cdir/collector-receipt.json" \
    '{schema:$schema, id:$id, unit:$unit, state:$state, reasons:{invalid:$inv, inconclusive:$inc},
      mock_seams_active:$mock, released:$released, release_epoch_us:$rel, evidence_durable:$durable,
      expected:{gate_mode:$gmode, exit_code:$ee, signal:$es, fixture_processes:$procs, accounting_completeness:$acct, config_sha256:$cfgsha, executable:$exe, args:$args},
      observed:{main_process_prerelease:$main[0], exec_main:$exec[0], survivor:$sv[0], diagnostic:$diag[0], collector_receipt:$rcpt[0]},
      evidence_dir:("cases/" + $dirname), accounting_decision:"NOT_QUALIFIED",
      claims:{numerical_CPU_accuracy:false, overhead_GO:false, performance:false, descendant_complete:false}}' \
    > "$cj.tmp"; } 2>"$cj.err" || ! mv -f -T -- "$cj.tmp" "$cj"; then
    # fallback: the recorded state itself becomes INVALID BEFORE it is appended; write errors are retained
    ninv=${#INV[@]} ninc=${#INC[@]}
    state=INVALID; INV+=(CASE_JSON_BUILD_FAILED)
    if ! printf '{"schema":"%s","id":"%s","state":"INVALID","released":%s,"mock_seams_active":%s,"evidence_durable":%s,"reasons":{"invalid":["CASE_JSON_BUILD_FAILED"],"original_invalid_count":%s,"original_inconclusive_count":%s}}\n' \
        "$CASE_SCHEMA" "$id" "$RELEASED" "$MOCK_ACTIVE" "$EVID_DURABLE" "$ninv" "$ninc" > "$cj.fallback" \
      || ! mv -f -T -- "$cj.fallback" "$cj"; then
      PUB_ERR+=("CASE_JSON_FALLBACK_WRITE_FAILED:$id")
    fi
  fi
  CASE_FILES+=("$cj") CASE_STATES+=("$state")
  [[ -n ${GOFD:-} ]] && exec {GOFD}>&-
  [[ -n ${RELFD:-} ]] && exec {RELFD}>&-
  return 0
}

not_run_case() { # index id reason (the JSON id is the plain roster id, like every run case)
  local d cj
  printf -v d '%s/cases/%02d-%s' "$OUT" "$1" "$2"
  cj=$d/case.json
  if ! { mkdir -p -- "$d" && chmod 0700 "$d" && jq -n --arg schema "$CASE_SCHEMA" --arg id "$2" --arg r "$3" --argjson mock "$MOCK_ACTIVE" --arg dirname "${d##*/}" \
    '{schema:$schema, id:$id, state:"INCONCLUSIVE", released:false, mock_seams_active:$mock, reasons:{invalid:[], inconclusive:[$r]}, evidence_dir:("cases/" + $dirname), accounting_decision:"NOT_QUALIFIED",
      claims:{numerical_CPU_accuracy:false, overhead_GO:false, performance:false, descendant_complete:false}}' > "$cj.tmp" \
    && mv -f -T -- "$cj.tmp" "$cj"; } 2>/dev/null; then
    PUB_ERR+=("NOT_RUN_CASE_JSON_WRITE_FAILED:$2")
  fi
  CASE_FILES+=("$cj") CASE_STATES+=(INCONCLUSIVE)
}

# ---------- mutation controls on closed artifacts of THIS batch (no new process) ----------
CONTROL_RESULTS=()
closed_controls() {
  local d=$OUT/closed-artifact-controls pulse late fake st
  pulse=$(compgen -G "$OUT/cases/*-pulse" | head -n1) late=$(compgen -G "$OUT/cases/*-late" | head -n1)
  # (1) remove the collector receipt from a copy of a closed successful pulse artifact
  st=INCONCLUSIVE
  if [[ -r $pulse/report.json && -r $pulse/stderr && -r $pulse/config.sha256 ]] && receipt_ok "$pulse/report.json" "$pulse/stderr" "$(<"$pulse/config.sha256")"; then
    if mkdir -p "$d/missing-receipt" && cp -- "$pulse/report.json" "$d/missing-receipt/report.json" \
      && { grep -vF "$COLLECTOR_SCHEMA" "$pulse/stderr" > "$d/missing-receipt/stderr.mutated"; (( $? <= 1 )); }; then
      if receipt_ok "$d/missing-receipt/report.json" "$d/missing-receipt/stderr.mutated" "$(<"$pulse/config.sha256")"; then st=INVALID; else st=MECHANICS_VERIFIED; fi
    else PUB_ERR+=(CONTROL_MISSING_RECEIPT_WRITE_FAILED); fi
  fi
  CONTROL_RESULTS+=("missing_receipt:$st")
  # (2) replace the late lifecycle CPU by the original premature fake_terminal_cpu_ns
  st=INCONCLUSIVE
  if [[ -r $late/report.json && -r $late/stdout ]] && late_ok "$late/report.json" "$late/stdout"; then
    fake=$(jq -rs 'map(select(.fixture == "late" and has("fake_terminal_cpu_ns"))) | first | .fake_terminal_cpu_ns' "$late/stdout")
    if mkdir -p "$d/premature-late" && jq --argjson f "$fake" '.child_cpu.cpu_ns = $f' "$late/report.json" > "$d/premature-late/report.mutated.json" \
      && cp -- "$late/stdout" "$d/premature-late/stdout"; then
      if late_ok "$d/premature-late/report.mutated.json" "$d/premature-late/stdout"; then st=INVALID; else st=MECHANICS_VERIFIED; fi
    else PUB_ERR+=(CONTROL_PREMATURE_LATE_WRITE_FAILED); fi
  fi
  CONTROL_RESULTS+=("premature_late:$st")
  jq -n --argjson mock "$MOCK_ACTIVE" --argjson c "$(jarr "${CONTROL_RESULTS[@]}")" \
    '{schema:"borsuk-native-cpu-fixture-controls-v1", mock_seams_active:$mock, controls:$c,
      scope:"mutations of copies of this batch closed artifacts; validators only, no new process"}' > "$d/verdict.json" 2>/dev/null \
    || PUB_ERR+=(CONTROLS_VERDICT_WRITE_FAILED)
}

# ---------- finalization (every publication step is checked; no write failure is ever success) ----------
publish_result() { # final extra released_procs
  local tmp=$OUT/result.json.tmp ctmp=$OUT/cases.json.tmp state=$1 f
  local -a existing=()
  for f in "${CASE_FILES[@]}"; do
    if [[ -f $f && -s $f ]]; then existing+=("$f"); else PUB_ERR+=("CASE_FILE_UNREADABLE:$(basename "$(dirname "$f")")"); fi
  done
  # checked intermediate (a process substitution would hide the inner jq exit and publish null cases)
  if (( ${#existing[@]} )); then
    jq -s . "${existing[@]}" > "$ctmp" 2>/dev/null || { PUB_ERR+=(CASES_SERIALIZATION_FAILED); printf '[]\n' > "$ctmp"; }
  else printf '[]\n' > "$ctmp"; fi
  # exact count, frozen-roster id order, schema and per-case states against what this run recorded:
  # a vanished/empty/mismatched case file can never leave a success result over the remainder
  local -a ids=(); local row n=0
  for row in "${CASES[@]}"; do (( n++ < ${#CASE_FILES[@]} )) && ids+=("${row%% *}"); done
  if (( ${#CASE_FILES[@]} > ${#CASES[@]} )) || ! jq -e --argjson n "${#CASE_FILES[@]}" --arg states "${CASE_STATES[*]:-}" --arg ids "${ids[*]:-}" --arg schema "$CASE_SCHEMA" \
    'type == "array" and length == $n and ([.[].state] | join(" ")) == $states and ([.[].id] | join(" ")) == $ids
     and all(.[]; .schema == $schema and (.state | IN("MECHANICS_VERIFIED", "INCONCLUSIVE", "INVALID")))' "$ctmp" >/dev/null 2>&1; then
    PUB_ERR+=(CASES_VALIDATION_FAILED); printf '[]\n' > "$ctmp"
  fi
  (( ${#PUB_ERR[@]} )) && state=INVALID
  if jq -n --arg schema "$RESULT_SCHEMA" --arg state "$state" --arg batch "$BATCH" --arg extra "$2" \
    --argjson mock "$MOCK_ACTIVE" --argjson deadline "$DEADLINE_S" --argjson fired "$([[ -e $OUT/fifo/deadline-fired ]] && echo true || echo false)" \
    --argjson released_procs "$3" --argjson controls "$(jarr "${CONTROL_RESULTS[@]}")" --argjson perr "$(jarr "${PUB_ERR[@]}")" \
    --argjson ncases "${#CASE_FILES[@]}" --argjson nexisting "${#existing[@]}" --arg dsha "$DRIVER_SHA" --arg hsha "$HELPER_SHA" --arg tsha "$TIMEOUT_SHA" \
    --slurpfile cases "$ctmp" \
    '{schema:$schema, state:$state,
      claims:{numerical_CPU_accuracy:false, overhead_GO:false, performance:false, query_wall_gate:false, descendant_complete:false, quantitative_error_bound:"UNQUALIFIED"},
      mock_seams_active:$mock, batch:$batch, abort_note:(if $extra == "" then null else $extra end), publication_errors:$perr,
      driver_sha256:$dsha, helper_sha256:$hsha, timeout_sha256:$tsha,
      plan:{invocations:12, fixture_processes:15, reserved_compiler_test_child:1, cap:16, retries:0, deadline_s:$deadline, deadline_fired:$fired},
      cases_recorded:$ncases, case_files_readable:$nexisting, fixture_processes_released:$released_procs,
      closed_artifact_controls:$controls,
      case_states:($cases[0] | map({id, state})), cases:$cases[0],
      remaining:["independent quantitative accounting error bound and full observer cost", "root protocol review", "new exact-input admission and canary before any performance experiment"]}' \
    > "$tmp" 2>/dev/null && [[ -s $tmp ]] && mv -f -T -- "$tmp" "$OUT/result.json"; then
    rm -f -- "$ctmp"
    return 0
  fi
  rm -f -- "$ctmp"
  PUB_ERR+=(RESULT_JSON_BUILD_OR_PUBLISH_FAILED)
  # minimal jq-free fallback: still INVALID, still mock-marked, still claims false. A previously published
  # success result must not survive a failed replacement: retract it first.
  rm -f -- "$OUT/result.json" || PUB_ERR+=(RESULT_RETRACT_FAILED)
  printf '{"schema":"%s","state":"INVALID","mock_seams_active":%s,"claims":{"numerical_CPU_accuracy":false,"overhead_GO":false,"performance":false},"publication_errors":"%s"}\n' \
    "$RESULT_SCHEMA" "$MOCK_ACTIVE" "${PUB_ERR[*]}" > "$tmp" 2>/dev/null && mv -f -T -- "$tmp" "$OUT/result.json" 2>/dev/null || PUB_ERR+=(RESULT_FALLBACK_WRITE_FAILED)
  return 1
}
publish_manifest() {
  if ! { ( cd "$OUT" && find . -type f ! -name 'SHA256SUMS*' ! -name 'TERMINAL.json*' -print0 | sort -z | xargs -0 sha256sum ) > "$OUT/SHA256SUMS.tmp" \
    && mv -f -T -- "$OUT/SHA256SUMS.tmp" "$OUT/SHA256SUMS"; } 2>/dev/null; then PUB_ERR+=(MANIFEST_FAILED); return 1; fi
  sync -f -- "$OUT/result.json" "$OUT/SHA256SUMS" "$OUT" 2>/dev/null || { PUB_ERR+=(FINAL_SYNC_FAILED); return 1; }
}
close_admission() { # stop admission, arbitrate atomically with the watchdog, join its CHECKED cleanup outcome
  local i
  : > "$OUT/fifo/batch-done" || PUB_ERR+=(BATCH_DONE_MARKER_FAILED)
  mkdir "$OUT/fifo/arbiter" 2>/dev/null && return 0 # the driver won: the batch deadline can no longer fire
  for ((i = 0; i < 50; i++)); do dl && break; nap 0.1; done
  if dl; then wait_cleanup_outcome || PUB_ERR+=(DEADLINE_CLEANUP_NOT_COMPLETED)
  else PUB_ERR+=(ARBITER_MKDIR_FAILED); fi
}
retract_stale_success() { # a non-INVALID result.json must never survive when the final state is INVALID
  jq -e '.state == "INVALID"' "$OUT/result.json" >/dev/null 2>&1 || rm -f -- "$OUT/result.json" || PUB_ERR+=(RESULT_RETRACT_FAILED)
}
final_rc() { case $1 in MECHANICS_VERIFIED) echo 0 ;; INCONCLUSIVE) echo 1 ;; *) echo 2 ;; esac; }
publish_terminal() { # final rc result_sha manifest_sha: the ONLY artifact that can authorize acceptance
  local tmp=$OUT/TERMINAL.json.tmp
  jq -n --arg schema "$TERMINAL_SCHEMA" --arg state "$1" --argjson rc "$2" --arg rsha "$3" --arg msha "$4" --argjson mock "$MOCK_ACTIVE" --arg batch "$BATCH" \
    '{schema:$schema, final_state:$state, driver_exit_code:$rc, result_sha256:$rsha, manifest_sha256:$msha, mock_seams_active:$mock, batch:$batch,
      acceptance:"valid only if this file exists, driver_exit_code is 0, final_state is MECHANICS_VERIFIED and both hashes match the files; result.json and SHA256SUMS alone never authorize acceptance"}' \
    > "$tmp" 2>/dev/null && mv -f -T -- "$tmp" "$OUT/TERMINAL.json" && sync -f -- "$OUT/TERMINAL.json" "$OUT" 2>/dev/null
}
finalize() {
  local final=MECHANICS_VERIFIED s c extra=${1:-} released_procs=0 i=0 row p e perr='' n0 rsha='' msha='' rcode g
  FINALIZED=1
  close_admission
  for s in "${CASE_STATES[@]}"; do [[ $s == INVALID ]] && final=INVALID; done
  if [[ $final != INVALID ]]; then
    for s in "${CASE_STATES[@]}"; do [[ $s == INCONCLUSIVE ]] && final=INCONCLUSIVE; done
    (( ${#CASE_STATES[@]} == ${#CASES[@]} )) || final=INCONCLUSIVE
  fi
  for c in "${CONTROL_RESULTS[@]}"; do
    [[ $c == *:INVALID ]] && final=INVALID
    [[ $c == *:INCONCLUSIVE && $final == MECHANICS_VERIFIED ]] && final=INCONCLUSIVE
  done
  [[ -n $extra && $final == MECHANICS_VERIFIED ]] && final=INCONCLUSIVE
  [[ -e $OUT/fifo/deadline-fired && $final == MECHANICS_VERIFIED ]] && final=INCONCLUSIVE # a deadline intervention is never a success
  [[ -e $OUT/fifo/deadline-cleanup-failed ]] && PUB_ERR+=(CLEANUP_FAILED)
  [[ -e $OUT/fifo/hard-stop ]] && PUB_ERR+=(HARD_STOP_FIRED) # the supervisor already declared this run dead: never a success
  if [[ -e $OUT/fifo/cleanup.log ]]; then
    grep -q CLEANUP_FAILED "$OUT/fifo/cleanup.log"; g=$?
    (( g == 0 )) && PUB_ERR+=(CLEANUP_FAILED)
    (( g > 1 )) && PUB_ERR+=(CLEANUP_LOG_READ_FAILED)
  fi
  for row in "${CASES[@]}"; do
    read -r _ _ _ _ p _ <<<"$row"
    [[ -s ${CASE_FILES[i]:-} ]] && jq -e '.released == true' "${CASE_FILES[i]}" >/dev/null 2>&1 && released_procs=$((released_procs + p))
    i=$((i + 1))
  done
  for _ in 1 2; do # pass 2 only when a publication step failed after a non-INVALID result was written
    (( ${#PUB_ERR[@]} )) && final=INVALID
    n0=${#PUB_ERR[@]}
    publish_result "$final" "$extra" "$released_procs"
    publish_manifest
    rsha=$(sha "$OUT/result.json" 2>/dev/null) || { rsha=''; PUB_ERR+=(RESULT_SHA_FAILED); }
    msha=$(sha "$OUT/SHA256SUMS" 2>/dev/null) || { msha=''; PUB_ERR+=(MANIFEST_SHA_FAILED); }
    (( ${#PUB_ERR[@]} > n0 )) && [[ $final != INVALID ]] || break
  done
  (( ${#PUB_ERR[@]} )) && final=INVALID
  [[ $final == INVALID ]] && retract_stale_success
  for e in "${PUB_ERR[@]}"; do perr+="\"${e//[^A-Za-z0-9_.:\/-]/_}\","; done
  # stdout summary FIRST (checked), terminal receipt LAST: a failed summary can still turn the outcome INVALID
  if ! printf '{"final_state":"%s","result":"%s","result_sha256":"%s","publication_errors":[%s],"mock_seams_active":%s}\n' \
    "$final" "$OUT/result.json" "$rsha" "${perr%,}" "$MOCK_ACTIVE"; then
    PUB_ERR+=(STDOUT_WRITE_FAILED); final=INVALID
    publish_result "$final" "$extra" "$released_procs"; publish_manifest
    rsha=$(sha "$OUT/result.json" 2>/dev/null) || rsha=''; msha=$(sha "$OUT/SHA256SUMS" 2>/dev/null) || msha=''
    retract_stale_success
  fi
  rcode=$(final_rc "$final")
  if ! publish_terminal "$final" "$rcode" "$rsha" "$msha"; then
    PUB_ERR+=(TERMINAL_RECEIPT_FAILED); final=INVALID; rcode=2
    rm -f -- "$OUT/TERMINAL.json"
    publish_result "$final" "$extra" "$released_procs"; publish_manifest; retract_stale_success
  fi
  if ! : > "$OUT/fifo/terminal-done"; then # the watchdog would otherwise hard-stop a finished driver
    rm -f -- "$OUT/TERMINAL.json" "$OUT/result.json"; rcode=2
  fi
  return "$rcode"
}

on_exit() {
  local rc=$? extra=''
  trap - EXIT INT TERM
  if (( ! FINALIZED )); then
    kill_owned_units
    # operator interrupt/terminate is INCONCLUSIVE; any other unexpected abort is a code error => INVALID
    if [[ $rc == 130 || $rc == 143 ]]; then extra="DRIVER_INTERRUPTED_rc$rc"; else PUB_ERR+=("DRIVER_ABORTED_rc$rc"); fi
    finalize "$extra"
    exit $(( $? > 1 ? $? : 1 ))
  fi
  # a finalize that aborted midway never wrote terminal-done, but also never wrote TERMINAL.json (no acceptance):
  # release the watchdog so a finished driver is not hard-stopped by its own supervisor
  [[ -e $OUT/fifo/terminal-done ]] || : > "$OUT/fifo/terminal-done" 2>/dev/null
  [[ -n $WD ]] && wait "$WD" 2>/dev/null
  [[ -n $WD2 ]] && wait "$WD2" 2>/dev/null
  exit "$rc"
}
trap on_exit EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

# ---------- batch ----------
start_watchdog
idx=0 stop_reason=''
for row in "${CASES[@]}"; do
  idx=$((idx + 1))
  read -r cid gmode ee es procs acct <<<"$row"
  if [[ -n $stop_reason ]] || dl; then
    not_run_case "$idx" "$cid" "${stop_reason:-BATCH_DEADLINE}"
    continue
  fi
  run_case "$idx" "$cid" "$gmode" "$ee" "$es" "$procs" "$acct"
  case $? in
    3) stop_reason=NOT_RUN_AFTER_PRE_RELEASE_FAILURE ;;
    4) stop_reason=NOT_RUN_AFTER_CLOSURE_FAILURE ;;
  esac
done
closed_controls
finalize
exit $?
