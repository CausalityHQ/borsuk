# Frozen validator checks on causality EC2

Operator selected EC2; the previous local execution exception is withdrawn.

Scope: run the nine frozen f6d1871071136d9542ba09ed584454aeaae76b8e cases once. Synthetic validator evidence only; no Rust executable, corpus, truth or performance measurement. Keep the original r2 recipe bytes and harness unchanged. The subsequent r3 filesystem-command-status repair is separate and is not qualified by these nine cases.

Inputs:

- recipe SHA256 3e63380b93320f00dd5888497cd582bc448b5954edce7b8349a66f2dadd5734d
- harness SHA256 ffcc7e26b5c37e519b6368ebdf76582c6f6e1df042b7e394f4018ec3b24011df
- contract SHA256 78cb1fc947c0d8ca46d54fdedcbd1dd291cc056716b9f8f499ae6904d4344a1b

Infrastructure: profile causality, eu-central-1, disposable c7i.large Spot, eu-central-1a, AMI ami-0b8a830d6339a9758, subnet subnet-034528fbd6977848f. Prior AMI availability revalidated. Latest Spot quote returned $0.045300/hour with timestamp 2026-10-10T04:00:00Z. No existing application instance may be used.

Envelope: setup and transport share a checked 500-second deadline; test service CPU0 via taskset plus CPUQuota=100%, MemoryMax=256MiB, MemorySwapMax=0, TasksMax=128, RuntimeMaxSec=120, TimeoutStopSec=5, LimitCORE=0. The root observer takes the launch epoch and starts within 90 seconds. Testing/terminal polling ends at launch+780 seconds, with bounded termination requests and confirmation thereafter. Its independent service RuntimeMaxSec is 1260 seconds. Compute allowance $0.035 covers 1260 seconds at maximum Spot rate $0.10/hour; ancillary allowance $0.015; total $0.05. This is a preregistered spending envelope, not an assertion that an unavailable EC2 termination API enforces billing. Unproven termination must alert the operator immediately; do not silently extend the campaign. Collect available evidence after termination even if volume deletion remains unproven, and retain INVALID closure. No retries of interrupted or failed test cells without a new recorded attempt.

Closure timeline: terminal polling cutoff 780; termination requests up to four bounded 11-second calls plus three two-second backoffs; termination confirmation up to 121 seconds; final instance description up to 17 seconds; volume deletion up to five bounded nine-second calls plus backoffs; terminal/archive metadata and authenticated downloads up to 75 seconds. A cleanup retry on an early failure remains inside the independent service limit or becomes explicit unresolved cleanup. Requests use AWS_MAX_ATTEMPTS=1. Launch sets InstanceInitiatedShutdownBehavior=terminate; guest shutdown at 15 minutes is a backup, not the primary root watchdog.

Before releasing the test service: authenticate each input's exact byte count and SHA, check Bash/jq availability and script syntax, record actual affinity/cgroup limits. Collect original systemd service exit, all nine case exits/intended failure layers, result.json, logs, input identities and resource evidence. A synthetic PASS is never production ANN qualification. Transport/environment/closure errors are INVALID.

Launch remains pending implementation and source verification of the minimal remote wrapper and the independent watchdog. No instance has been launched for this campaign.
