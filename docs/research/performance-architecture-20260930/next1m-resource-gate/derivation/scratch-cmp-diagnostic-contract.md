# Canary scratch zero-prefix cmp status diagnostic (SOURCE ONLY; assembly bytes + text assertions; no launch authority)

Authority  root message 1791590172059530855: smallest source-only slice, ONLY assemble-canary.py, exact base 678f7dbf3012023a20b85803bcabff414dd2073d.
Base       678f7dbf (preserved); its assemble-canary.py is 6148 B sha256 fbf11d6cd707789b6c61d06529cd373f0bc3bf10c004c2b56bcc8b77ebca87e5 (= the a79a5cff / 10d04555 trace assembler, unchanged).
Commit     618efe6975eb9acc5a66b8b96792935f49c07a77 on NEW ref devbox/native-scale-build-gate-scratch-cmp-rc (parent 678f7dbf; Roman Bartusiak, no trailers; git diff --check OK; 1 file, +16/-2).
Modified   derivation/runtime-support.pending/assemble-canary.py  7280 B  sha256 e7625ddc2b0b31c503532ee674f1d16b87a80cf336529fef1f5bfd86e731432e (committed blob == verified file).
Preserved  scratch-trace a79a5cff, own1 f12deb62, own2 d4c1b1b8, own2-canary 36185e2e, own3 9244e2a7, worktree HEAD 37c4adcf; no other file, contract, tail or coordinator touched.

## Exact guard (actual committed spelling, whole line, exactly once; also line 136 of the launched a0002 user-data.sh)
    cmp -s -n 4194304 "$dev" /dev/zero || exit 90
## Canary-only replacement (inside the existing xtrace window, before `mkdir prepared-parent`)
    cmp --version > evidence-root/scratch-cmp-version.txt 2>&1 || exit 94
    exec 4> evidence-root/scratch-cmp.out || exit 94
    cmp_rc=0; cmp -n 4194304 "$dev" /dev/zero >&4 2>&1 || cmp_rc=$?
    exec 4>&-
    printf '%s\n' "$cmp_rc" > evidence-root/scratch-cmp.rc || exit 94
    (( cmp_rc == 0 )) || exit 90
Same -n and inputs; -s removed so cmp prints its diagnostic (differ: byte N / EOF / error text) into scratch-cmp.out; the raw status is kept in cmp_rc BEFORE anything else runs, recorded as a number, and any nonzero status is refused with the SAME exit 90. cmp --version (small) is captured BEFORE the comparison. A failed evidence write (version, fd 4 open, rc file) exits 94 = diagnostic INVALID: 94 is unused by the production prefix and the tail (90 scratch refusal, 91 transport, 92 parity/T3 halt, 93 chain, 95 deadline, 96 bad(), 97/98 signals) and finish() passes 90..98 through unchanged, so INVALID is distinguishable from the guard's 90. The output goes through an already-open fd 4 so a failed redirection can never be mistaken for cmp's status. Nothing else (thresholds, other guards, mkfs/mount, deadlines, prefix, tail, coordinator) is modified.
The assembler refuses (ValueError, no output) unless: the guard appears exactly once as a whole line; none of cmp_rc / exit 94 / fd 4 uses exist beforehand; after the edits the old `cmp -s -n 4194304` is gone, the refusal line is present once and the block lies inside the xtrace ON..OFF window; and the existing assertions hold (all edits' inverse reproduces the untraced prefix byte for byte; ON < OFF < phase=transport; no aws in the window; set -x/BASH_XTRACEFD once). JSON now also reports diagnostic_cmp_rc:true.
Canary-only: scratch-cmp-version.txt, scratch-cmp.out and scratch-cmp.rc are extra evidence-root members that verify-closed.py's exact roster (untouched) would reject; not for a production run.

## Source-only verification (closed a0002 config metadata only: canary-original-a0002/assembly-config.json sha256 be573d56a112b7f65f76f355cdceb5645a4be994bbf07ec7de7e44bb507707d6; nothing new created)
Bounded user scope run-p975131-i671942223.scope (CPUQuota 100%, AllowedCPUs 0, MemoryMax 256M, MemorySwapMax 0, TasksMax 128, RuntimeMaxSec 120), scope exit 0, raw outputs in scratchpad trace2/verify.out; nothing executed beyond py_compile, the assemblers, bash -n (parse) and text assertions:
 * py_compile of the modified assembler 0; assemblers rc 0 for the pre-trace (fb0b2ab6), base (678f7dbf) and modified versions.
 * base assembler on the a0002 config == the launched a0002 user-data.sh byte for byte (12763 B, a4a7182cbff96bd191ea16f0a013097312bc0211d0719747a84b8e4f55122149).
 * modified assembly 13005 B (+242 vs launched, <= 16384) sha256 45dc8132a00fa463f4edfe9d7fa45561290db2f326e04d39c082aa31dde3783f; bash -n 0 for all three.
 * independent inverse (not the assembler's functions): modified minus the cmp block == the launched a0002 bytes TRUE; additionally minus the existing trace tokens == the untraced original assembly TRUE (12592 B, sha256 e856087244c2cbf0d5dd5363f4085e779069f8c104d9acee01e31e1630f88f4b, equal to the JSON untraced_sha256).
 * diff -U0 vs the launched script: exactly one hunk `@@ -136 +136,6 @@` (the guard line replaced by the 6 lines above); block lines 136-141, ON 126, OFF 148, finish 18, mkdir prepared-parent 142, phase=transport 157.
 * order: cmp (rc kept) < record < refusal TRUE; version capture before comparison TRUE; block inside the trace window TRUE; no aws/curl in the window; `cmp -s -n 4194304` absent from the modified script; exit 90 count 18 = 18 (original), exit 94 count 3; the other 25 guard lines (all remaining exit 90, mkfs.ext4, mount -o, setup_stop, local_stop_epoch, admit) are identical (sha256 6688d66a69270899eca0073eba6bfc0e9b88b1247e3413780e4ff1e603436c83); fd 4 used only by the block.
Not done (as instructed): no shell run of the script, no local runtime/mock/raw-disk read/Cargo/network/AWS, no review, child, completion, launch request, epoch, pins or paid job.

## Open
O1 -s removed: for a block-vs-char pair cmp's non-silent and silent modes reach the same read/compare loop and exit statuses; the only observable change is the diagnostic output.
O2 cmp --version proves the target binary's version only if the capture succeeds; a failed capture is exit 94 (INVALID), never a silent pass.
O3 Root owns review and any prospective paid attempt; none is authorized by this slice.
