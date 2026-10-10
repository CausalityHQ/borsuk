#!/usr/bin/env bash
# STATIC, UNRUN fixture for the scratch formatting gate. Source-bound: it extracts the admission section from the committed user-data.sh with exact
# whole-line anchors, applies exactly five documented substitutions (each required once; the reverse mapping must restore the extracted bytes) and runs it
# against shell-function mocks of the host commands. mkfs.ext4 is a counter. No real device, network, AWS or systemd is touched.
# usage: scratch-admission-mock.sh USER_DATA_SH NEW_EMPTY_WORKDIR      exit 0 = every scenario matched its expectation
set -Eeuo pipefail
[[ $# == 2 && -f $1 && ! -L $1 && ! -e $2 ]]
src=$(realpath -e -- "$1"); W=$2; mkdir -m 0700 -- "$W"; W=$(realpath -e -- "$W")
python3 -I - "$src" "$W" <<'PY'
import pathlib, sys
src = pathlib.Path(sys.argv[1]).read_text()
W = pathlib.Path(sys.argv[2])
lines = src.splitlines(keepends=True)
def one(text):
    hits = [i for i, l in enumerate(lines) if l == text]
    assert len(hits) == 1, text[:60]
    return hits[0]
serdef = lines[one('ser() { tr -d \' \\n\' < "/sys/block/${1##*/}/device/serial"; }\n')]
start = one('mapfile -t cand < <(for s in /sys/block/nvme*n1; do [[ $(ser "$s") == "${vol/-/}" ]] && basename "$s"; done)\n')
end = [i for i, l in enumerate(lines) if l.startswith('timeout -k 5 300 mkfs.ext4 -q -m 0 -L borsuk-scratch ')]
assert len(end) == 1 and start < end[0]
section = ''.join(lines[start:end[0] + 1])
original_section, original_serdef = section, serdef
subs = [('for s in /sys/block/nvme*n1; do', 'for s in "$MOCK"/sys/block/nvme*n1; do'),
        ('dev=/dev/${cand[0]}\n', 'dev=$MOCK/dev/${cand[0]}\n'),
        ('[[ -b $dev && ', '[[ -e $dev && '),
        ('/dev/$rd', '$MOCK/dev/$rd')]
ser_sub = ('"/sys/block/${1##*/}/device/serial"', '"$MOCK/sys/block/${1##*/}/device/serial"')
for old, new in subs:
    assert section.count(old) == 1, old
    section = section.replace(old, new)
assert serdef.count(ser_sub[0]) == 1
serdef = serdef.replace(*ser_sub)
inverse = section
for old, new in reversed(subs):
    assert inverse.count(new) == 1, new
    inverse = inverse.replace(new, old)
assert inverse == original_section
assert serdef.replace(ser_sub[1], ser_sub[0]) == original_serdef
(W / 'ser.sh').write_text(serdef)
(W / 'section.sh').write_text(section)
PY
export MOCK=$W/m
mock_env() { # per-scenario defaults; the caller overrides one thing
 VOL='vol-0c4f0ad58d2ba29b9'; RVOL='vol-09d65657769cead73'; SER_DEV='vol0c4f0ad58d2ba29b9'; SER_ROOT='vol09d65657769cead73'
 DEV_SIZE=42949672960; CMP_RC=1; CMP_OUT='/dev/nvme1n1 /dev/zero differ: byte 4097, line 1'; ROWS=1; MOUNTS=''; SOURCE_MOUNTS=''; WIPEFS=''; SWAP=''; BLKID_RC=2; ROOT_PK=nvme0n1; CMPFILE_DIR=0
}
run_case() { # run_case NAME EXPECTED_EXIT EXPECTED_MKFS
 local name=$1 want=$2 mk=$3 rc=0 got
 rm -rf "$W/run" "$MOCK"; mkdir -p "$W/run/evidence-root" "$MOCK/dev" "$MOCK/sys/block/nvme0n1/device" "$MOCK/sys/block/nvme1n1/device"
 : > "$MOCK/dev/nvme1n1"; printf '%s   \n' "$SER_ROOT" > "$MOCK/sys/block/nvme0n1/device/serial"; printf '%s   \n' "$SER_DEV" > "$MOCK/sys/block/nvme1n1/device/serial"
 [[ $CMPFILE_DIR == 1 ]] && mkdir "$W/run/evidence-root/scratch-cmp.txt"
 : > "$W/mkfs.count"; export VOL RVOL DEV_SIZE CMP_RC CMP_OUT ROWS MOUNTS SOURCE_MOUNTS WIPEFS SWAP BLKID_RC ROOT_PK W
 (
  set -Eeuo pipefail; cd "$W/run"; root=$W/run; vol=$VOL; rvol=$RVOL
  # shellcheck disable=SC2034
  : "$root $vol $rvol"
  lsblk() { case "$*" in '-b -J'*) echo '{}';; '-rno NAME'*) for _ in $(seq "$ROWS"); do echo nvme1n1; done;; '-rno MOUNTPOINTS'*) echo "$MOUNTS";; '-rno FSTYPE,PTTYPE'*) echo ' ';; '-no PKNAME'*) echo "$ROOT_PK";; esac; }
  findmnt() { case "$*" in '-J') echo '{}';; '-rno SOURCE') printf '%s\n' /dev/nvme0n1p1 tmpfs "$SOURCE_MOUNTS";; '-no SOURCE /') echo /dev/nvme0n1p1;; esac; }
  wipefs() { printf '%s' "$WIPEFS"; }; swapon() { printf '%s' "$SWAP"; }; blockdev() { echo "$DEV_SIZE"; }
  blkid() { return "$BLKID_RC"; }; cmp() { printf '%s' "$CMP_OUT"; return "$CMP_RC"; }
  timeout() { shift 3; "$@"; }; mkfs.ext4() { echo called >> "$W/mkfs.count"; }
  # shellcheck disable=SC1091
  source "$W/ser.sh"; source "$W/section.sh"
 ) > "$W/run.out" 2>&1 || rc=$?
 got=$(wc -l < "$W/mkfs.count")
 if [[ $rc == "$want" && $got == "$mk" ]]; then echo "PASS $name exit=$rc mkfs=$got"; else echo "FAIL $name exit=$rc (want $want) mkfs=$got (want $mk)"; fails=$((fails + 1)); fi
}
fails=0
mock_env; run_case 'owned volume, cmp rc 1 reaches mkfs once' 0 1
mock_env; CMP_RC=0; CMP_OUT=''; run_case 'owned volume, cmp rc 0 reaches mkfs once' 0 1
mock_env; CMP_RC=2; run_case 'cmp rc 2 refuses' 90 0
mock_env; CMP_RC=124; run_case 'cmp timeout refuses' 90 0
mock_env; SER_DEV=vol0000000000000000a; run_case 'wrong serial: no candidate' 90 0
mock_env; SER_ROOT=vol0000000000000000b; run_case 'root disk serial differs from the binding root volume' 90 0
# Section-only isolation: make the serial predicate true to exercise the projected name guard.
# The full binding validator independently rejects identical root/scratch volume IDs.
mock_env; ROOT_PK=nvme1n1; RVOL=$VOL; run_case 'root backing disk is the chosen device' 90 0
mock_env; DEV_SIZE=1; run_case 'wrong size' 90 0
mock_env; BLKID_RC=0; run_case 'signature present (blkid rc 0)' 90 0
mock_env; WIPEFS='x'; run_case 'wipefs signature' 90 0
mock_env; MOUNTS=/mnt/x; run_case 'mounted' 90 0
mock_env; SOURCE_MOUNTS="$MOCK/dev/nvme1n1"; run_case 'findmnt source uses the device' 90 0
mock_env; SWAP="$MOCK/dev/nvme1n1"; run_case 'swap on the device' 90 0
mock_env; ROWS=2; run_case 'child device rows' 90 0
mock_env; CMPFILE_DIR=1; run_case 'evidence write failure' 90 0
echo "scenarios failed: $fails"
(( fails == 0 ))
