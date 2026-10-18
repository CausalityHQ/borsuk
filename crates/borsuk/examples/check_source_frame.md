# SOURCE frame falsifier — 2026-10-18

Retain a measured whole-plane byte/CPU/RAM tradeoff: **4.7468% fewer bytes**
after charging frame headers and the proposed directory. Do not adopt a
production format or change defaults from this diagnostic. It establishes no
query-range transfer, GET, ANN quality, latency, QPS, billing or vendor win.

## Frozen method and result

Native source: dfc339906f4f0dd7a3fbbdf330dc9e7f3c8bc870.
Base: c684bd7949acfb46e6eb225aa8757d9157187292.
Independently encode 256 original records per frame, with the actual final tail:
transpose by record-byte position, Zstd1 without dictionary, raw original bytes
when compression is not smaller. The generic [codec](../src/source_frame.rs)
is independent of the fixture. Zstd runtime was 1.5.7 (10507).

Fixture: Cohere Wikipedia embed-multilingual-v3, 1,000,000 rows, D1024,
264-byte encoded SOURCE records, generation v11 flat_records. The root's pinned
generation binds the v3 plane; PageAuthority::load_two_bit validates the roster
and all 31,250 original 32-row units. Full original, codec-input and decoded
SHA256 match. Requests, truth and original f32 vectors were unopened.
No recall was measured.

| Whole-plane byte accounting | Bytes |
| --- | ---: |
| Original records | 264,000,000 |
| Candidate payload | 251,276,843 |
| Actual 25-byte headers, 3,907 frames | 97,675 |
| Candidate framed bytes | 251,374,518 |
| Counterfactual directory, 64 + 24 × 3,907 | 93,832 |
| Framed plus counterfactual directory | 251,468,350 |
| Reduction including both overheads | 12,531,650 |

Raw fallback count was zero. The all-raw synthetic witness separately proves
fallback expands framed bytes by 25N; directory costs remain separate.
Frame sizes including headers: min11,261 / p50 64,411 / p95 64,546 / max64,993
bytes. These are corpus-frame distributions, not query percentiles.

Encode/decode process CPU: 0.479983164 / 0.401212329 seconds; corresponding
wall: 0.481255148 / 0.402388615 seconds. Total execute CPU/wall:
1.932661326 / 1.932675153 seconds. Calls include fresh contexts, zeroing,
transpose and validation. Total includes authentication/hash work and excludes
config loading/publication. These are descriptive whole-call costs, not
steady-state/context-reuse costs or arbitrary rejection gates.

VmHWM was 6,648 KiB; ending VmRSS was 5,724 KiB. Cgroup memory.peak was
4,587,520 bytes; swap/OOM zero. Cgroup accounting includes attributed file
cache and differs from process RSS; its lower peak does not establish a
private or total memory bound. End snapshots cover the whole process.
Output/window caps and metadata admission counts are separate from observations.

Each root mode used a fresh unit: CPU4/100%,1GiB,swap0,pids128,600s runtime,
15s stop. No compiler overlapped native modes.

## Qualification and gates

Final qualifier borsuk-source-frame-qualify-a0002, invocation
8f0105b872ca42339024cf6aa3fe366c, terminated exit0/stageVERIFIED;
MainPID0/inactive and shared lock free. EC2 CPU8-15/40GiB/swap0/pids2048/jobs8.

| Exact command | Result |
| --- | --- |
| cargo test --locked -p borsuk --lib --example check_source_frame --no-run | Exit0 |
| cargo test --locked -p borsuk --lib source_frame_ -- --test-threads=1 | 3 passed, 0 failed |
| cargo test --locked -p borsuk --example check_source_frame -- --test-threads=1 | 5 passed, 0 failed |
| cargo build --locked --release -p borsuk --example check_source_frame | Exit0 |
| cargo clippy --locked --workspace --all-targets -- -Dclippy::correctness -Dclippy::suspicious | Exit0 |
| env -u BORSUK_TEST_BUILD_COMMAND BORSUK_TEST_BUILD_JOBS=8 CARGO_BUILD_JOBS=8 bash scripts/check_rust_test_build.sh | Exit0;162 executable targets;109s |

Tests cover D1/3/257/768/1024 widths;32/256/tails; deterministic exact bytes;
raw fallback; malformed geometry/tags/lengths, output/window/overflow caps,
truncation/trailing/concatenation; authority corruption; strict CLI/create-only
refusal; deterministic file growth stopped after65,537 bytes; all-raw report
accounting. Warnings remain; required Clippy categories passed.

Review group ff671a325dec43ba completed engineering549fcbfb752b4c99 and
researchdc5ab73a6dbc43b4 reviews of original
5be4232174a01dce72ae041c2567f89370546039. Root read both full reports and
reconciled bounded descriptor reads, growth/accounting witnesses, RAM/timing
wording, decode-auth docs and Zstd version. Guards/hashes retained.
Root read full repair diff45ff89be; no further review/unchanged broad gates.
Original revision, passing qualifier and ELF remain retained.

After qualification, root independently ran actual-source admission, separate
synthetic canary and true ELF boundaries, then measurement. All three modes
terminated0 with matching REPORT_SYNCED and create-only report. Unknown and
occupied-report boundaries exited1; occupied sentinel unchanged.
Require exit0, matching marker and report hash; presence alone is insufficient.

## Durable evidence and pins

Evidence directory: /mnt/borsuk-workspace/evidence/source-frame-falsifier-a0001.
It retains original/final bundles, patches, snapshots/source hashes; every
qualifier command/stdout/stderr/exit; full reviews; preregistration/freeze;
wrapper; mode/cgroup/time receipts and reports. Root owns archival,
infrastructure cleanup, integration and push.

| Artifact | SHA256 |
| --- | --- |
| Final ELF artifacts/check_source_frame | a57286ed925769828ac3188cb0c982d770f81021b5fcf65a8636cdf13a58e254 |
| root-only-config.json | 0c8e676847e13fb0baf8f84e8533aa22c9b8eee23463ded39a3b665bc62ba6aa |
| root-preregistration.txt | 6571eb2e66ce54e7896bea328589ea289a263a320a5ec83084e1f33946b0e137 |
| root-mode.sh | cdca7d4fbec21f574c54eca2233a2e2dccdb1e8f152805a8a4f7e71f5872078f |
| root-admission.json | ad66bae006915ae50051058e01e3deef26071188f1a1a82c27c117322e58e73c |
| root-canary.json | 80b39d83a19c2606435740654f6e1db9489da1c8b2562bc04c92e952ad965391 |
| root-measure.json | 6d5a9d920c2db899cf49d60fe554beea678a92be8b2454e5010acb1a74252856 |
| Generation manifest | 824df22c4f3b59ea8b9cd42ca0fbb5e87a5cda1f4887b457695d00940914ace0 |
| Plane manifest | e0e3af73fd40db57df54a453ede016b20e4fb5d63e3a075685c82132474467c6 |
| Original-unit digest roster | 63c07feff1b370753008a4ed6e1bc05cd9844ef7a411b22ccc1e97a76ce9aa41 |
| Original, codec-input and decoded records | e61ffa621fedc44d3cea19ed6c47a73222a9b33c2e567567d21c048cdc826e90 |

Qualification launch SSM124da872-a7d8-4c15-a8d5-8ea1be1c6d32 and collection
0f6b0dc8-3dfc-4917-b4ef-b58bb0bb08d8 succeeded0.
Root mode SSM prefixes: admission109cfd94, canaryd3ecca2c, measureba357787;
terminal collection2ad6e58f succeeded0. Root independentbf39ef8e verified all
six stage exits/test counts; its subsequent hash check failed solely from a
wrong working directory. Corrected read-only0fd961d5 succeeded0, checking
four source hashes, all three reports and sentinel. Preserve both receipts;
no native rerun. Helper read the completed authenticated report through
d2386987-cad6-452d-8a15-2024f717dddf; root executed every real-data mode.

## Decision and minimum next gate

Retain the positive byte result and measured CPU/RAM costs; no production
format adoption. A 256-row frame contains eight authenticated32-row units:
fetching one needed unit may require a whole frame, so whole-plane savings
may disappear or reverse under actual query ranges.

If this modest gain supports continuing, the minimum next step is a separately
frozen real-range replay using actual authenticated SOURCE fetch ranges and
the same inputs. Charge fetched complete frames, headers, proposed directory,
decode work and concurrency/residency; verify exact bytes and existing
scoring/plans. Only supported range-replay evidence can justify a later
serving-format or native ANN experiment.

