**BLOCK freeze/launch of this exact package.** Four wrapper defects need repair; none requires changing Rust, the algorithm, or scientific limits.

1. **The guest manifest includes its own checksum.**  
   [build-ssm-request.sh:16](/data/target/borsuk-source-neighborhood-data-gates.pending/build-ssm-request.sh:16) creates `package/support.sha256` before expanding `*.sha256`. That glob includes the newly created output, so the manifest hashes its own unfinished contents. The mandatory guest check at line 28 consequently fails before installation.

   **Smallest repair:** Replace `*.sha256` with the two explicit filenames, `admission.config.sha256 smoke.config.sha256`. Keep `support.sha256` excluded from itself. Regenerate the affected freeze hashes.

2. **Launch failures can bypass the independent external stop.**  
   [launch-once.sh:11](/data/target/borsuk-source-neighborhood-data-gates.pending/launch-once.sh:11) can time out after AWS accepts `RunInstances`. `set -e` then exits before recording the instance or arming the external timer. Likewise, a timer-creation failure at line 17 exits without terminating the known instance. There is no recovery handler. If guest initialization also fails, neither claimed backstop exists.

   **Smallest repair:** Establish the external watchdog before launching, bound to this exact frozen client token until the instance ID is known. Add failure handling that recovers only that original instance and terminates it; never repeat `RunInstances`. A failure to arm or verify protection must trigger immediate cleanup.

3. **The timers do not enforce the stated lifetime arithmetic.**  
   [launch-user-data.txt:9](/data/target/borsuk-source-neighborhood-data-gates.pending/launch-user-data.txt:9) and [launch-once.sh:17](/data/target/borsuk-source-neighborhood-data-gates.pending/launch-once.sh:17) omit `AccuracySec`. Systemd defaults to a firing window extending one minute beyond the requested time. Thus the external action may start near **4680 seconds**, before its additional **25+2-second** command allowance. These settings do not support the stated 4620-second hard lifetime or its exact compute-cost calculation. [Systemd timer semantics](https://raw.githubusercontent.com/systemd/systemd/v255/man/systemd.timer.xml)

   **Smallest repair:** Set timer accuracy explicitly and schedule actions early enough to reserve timer tolerance and shutdown/API/kill grace within the existing ceilings. Distinguish the termination-request deadline from actual terminated time; retain the required termination, volume-deletion and timer-closure receipts. Preserve the $0.25 total ceiling.

4. **Installation failure before AWS availability loses the detailed failure evidence.**  
   [run-data-gates.sh:83](/data/target/borsuk-source-neighborhood-data-gates.pending/package/run-data-gates.sh:83) redirects installation output into `install.phase.log`. If installation fails before `aws` is available, the archival branch at line 43 is skipped. Line 77 prints only status fields; the installation error and raw evidence remain on the disk that must be deleted. Original SSM status preserves failure, but not its cause.

   **Smallest repair:** In the unavailable-publication path, emit a bounded installation-log excerpt plus raw phase/original-exit/cleanup status into the original SSM output. Preserve the original failure status and keep geometry logs opaque.

I read all eight requested documents in full. Both supplied manifest hashes match; every package entry, launch input and immutable `review-input/` entry verifies. `git show fc60c18e…` produces the supplied Rust SHA `239c80db…`.

The direct-I/O, I/O-hierarchy and measured-peak repairs are present. Identity binding, first-failure progression, checksummed terminal-last publication and the separate final closure requirements are coherent. Target support and runtime feasibility remain matters for the disposable gate.

**Verdict: repair 1–4 before launch.** No files changed; no scripts, tests, mocks, native execution, cloud actions, children or consultations were run.
