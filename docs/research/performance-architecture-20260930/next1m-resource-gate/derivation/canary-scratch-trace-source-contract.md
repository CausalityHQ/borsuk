# Canary scratch-trace diagnostic contract (SOURCE ONLY; assembly bytes + text assertions; no launch authority)

Authority  root message 1791588807227529492 (one smallest OWN1 source-only slice: edit ONLY assemble-canary.py).
Base       fb0b2ab65e74cb57f88796b3552d4c19535bdcd1 (preserved)        original assembler file sha256 716afcb528cd6d6b9cfe25f7d79b43fbd1c042e358bb4f227cfb1ea19e0cfcb5, 4382 B
Commit     a79a5cff951f31b7e67f5b0f74d4a0ef13ea7cd1  on NEW isolated ref devbox/native-scale-build-gate-scratch-trace (parent fb0b2ab6; author Roman Bartusiak, no trailers; git diff --check OK; 1 file, +31/-2)
Modified   derivation/runtime-support.pending/assemble-canary.py  6148 B  sha256 fbf11d6cd707789b6c61d06529cd373f0bc3bf10c004c2b56bcc8b77ebca87e5
Preserved  own1 f12deb62, own2 d4c1b1b8, own2-canary 36185e2e, own3 9244e2a7 and the physical worktree HEAD 37c4adcf are untouched; nothing else was edited.

## What the assembler now does (after the authenticated prefix assembly and hook replacement, before the stub)
Three exact whole-line anchors in the assembled prefix, each required EXACTLY ONCE (else ValueError, no output):
 1. after `ident before`  insert:  exec 3> evidence-root/scratch-trace.log; BASH_XTRACEFD=3; PS4='+scratch:${LINENO}:${FUNCNAME[0]:-main}: '; set -x
 2. after `ident after`   insert:  set +x; unset BASH_XTRACEFD            (unset closes fd 3)
 3. finish() first line   ` original=$?; trap - EXIT TERM INT HUP PIPE; set +e`  ->  ` original=$?; set +x; unset BASH_XTRACEFD; trap - EXIT TERM INT HUP PIPE; set +e`
The assembler itself refuses unless: removing exactly those tokens reproduces the untraced prefix byte for byte; ON < OFF < `phase=transport`; no `aws ` inside the trace window; `set -x` and `BASH_XTRACEFD=3` each occur once; the finish edit is present once; no PENDING_ marker; raw size <= 16384. Its JSON adds diagnostic_trace:true, untraced_bytes and untraced_sha256 (the untraced assembly = what the base assembler outputs for the same config).
Design points: xtrace goes to fd 3 only (never stdout/stderr), so the `ident` files, run.log and every `$(...)` capture are unchanged; PS4 has no command substitution so `$?` is untouched; the trace is stopped in finish() only AFTER `original=$?` is captured (first command of finish, status semantics identical), and in the normal path before `phase=transport`, so transport/native/AWS are never traced (the window holds no aws/curl). Traced commands are exactly the existing admission commands (nl, mp, ft, ws, mt, sw, blkid, cmp zero-prefix, mkdir, mkfs, mount, chmod, mountpoint/findmnt/stat, and the `ident after` call itself; the disk-admission lines after it are outside the window); no command or read is added, no vectors/credentials/AWS. xtrace prints each `[[ ]]` term with its expanded values, so the last `+scratch:LINE` before `exit 90` identifies the failing check without extra commands.
Canary-only: scratch-trace.log is an extra evidence-root member; the full-chain verify-closed.py roster (exact names) is untouched and would reject it, so this assembler is NOT for a production run.

## Source-only verification (old config metadata only: canary-original-a0001/assembly-config.r2.json sha256 cd9c7ef49d135eb8bf5c6b41a98bb4467f6278ef3fe3b1fce79a410599270ece; no new request/epoch/pins/job)
Bounded user scopes, CPUQuota 100% / AllowedCPUs 0 / MemoryMax 256M / MemorySwapMax 0 / TasksMax 128 / RuntimeMaxSec 120:
 * run-p4065044-i670843915.scope: BASE assembler on the old config -> 12592 B sha256 c12d5460975a8f6b61bbcae9336e1817f693113598f15b5ab9ad06d042398031, byte-identical to the frozen a0001 user-data.r2.sh (cmp OK).
 * run-p4107035-i670888545.scope: MODIFIED assembler on the same config (scope exit 0; raw outputs kept in scratchpad trace/verify.out):
   py_compile 0; assembly rc 0; bash -n (syntax only, nothing executed) 0 for the original and the traced script;
   traced assembly 12763 B (+171 B, <= 16384) sha256 6f72c35e47648f300abe40669e735dd0e2442bc122192c676e20229e92bc05c0;
   independent inverse (not the assembler's function) of the traced assembly == original assembly (sha c12d5460...) TRUE; JSON untraced_sha256 == that sha and untraced_bytes 12592 TRUE;
   diff -U0 original vs traced: exactly 3 hunks (@@ -18 +18 @@ the finish line; @@ -125,0 +126 @@ ON; @@ -141,0 +143 @@ OFF);
   line numbers in the traced script: finish line 18 (first command of finish, `original=$?` before `set +x`), ident before 125, ON 126, ident after 142, OFF 143, phase=transport 152; trace window 16 lines, no aws/curl in it; `set -x` count 1, `BASH_XTRACEFD=3` count 1;
   26 guard lines (every `exit 90`, `cmp -s -n 4194304 "$dev" /dev/zero || exit 90`, mkfs.ext4, mount -o, setup_stop, local_stop_epoch, admit) are identical in both scripts (sha af8c2dcff6b3d81f7aa64ad994eb9f669baab53ee679410c77917c81be8be101).
Not done (as instructed): no shell execution of the script, no native/runtime/cloud/systemd fixture/Cargo/review/child, no launch request, epoch, pins or paid job.

## Open
O1 xtrace output of a guard shows values (device/volume names, lsblk/wipefs/findmnt/swapon results); it contains no credentials but is part of the evidence archive.
O2 `exec 3>` would exit 1 (not 90) only if evidence-root were missing, which the prefix creates first.
O3 The next recorded canary resource admission, launch request, epoch and pins belong to the root; the full canary source set (tail/coordinator) is unchanged by this slice.
