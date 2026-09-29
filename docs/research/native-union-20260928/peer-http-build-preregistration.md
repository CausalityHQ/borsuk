# Private-listener ARM qualification, 2026-09-29

One c7g.2xlarge Spot in eu-central-1; no dataset or ANN query. The affected
Rust example changes only its listener guard/usage text: allow loopback or
explicit RFC1918 IPv4, reject wildcard/public addresses. Retain four query
slots and source/scorer/library code. Reuse 2696-pass library assurance.
Preflight authenticates that proof and compares all recorded native dependency
files excluding standalone src/bin tools (not compiled by this example gate).
The only difference within that dependency roster must be two_bit_http.rs.

Actual extracted listener guard std Rust test has observed RED for private
IPv4 before the change and GREEN after it. Execute that check on the ARM host,
all tests for the affected example, then its locked release build. Freeze
compiled example bytes and binary hash, build logs, limits and terminal identity.
Do not repeat full workspace tests. No benchmark result may use the new binary
until its closed artifact/source identities and instance termination are verified.

Spot max $0.30/hour; machine shutdown 2700 s, build timeout2400 s/systemd2430 s,
10 GiB cgroup, zero swap, CPU0–3, encrypted80 GiB gp3 deleted on termination.
Compute cap $0.225 plus $0.15 EBS/S3 allowance; estimates are not billed costs.
Preserve one controller/instance ID. Inspect terminal/infrastructure only until
closure; stop immediately terminal, discard interruptions without automatic
replacement. Always terminate acknowledged instance on failure/deadline.
