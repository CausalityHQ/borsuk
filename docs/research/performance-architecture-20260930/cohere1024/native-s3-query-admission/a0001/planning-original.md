# S3 native input-admission plan (read-only, HEAD c490dfc7)

## Verdict

**There is no genuine blocker.** The qualified runner (`9a65fd61…`, 16,135,912 B) can run this gate with the template config exactly as committed. The only new pieces are the rendered config bytes and shell substitutions in the existing a0002 user-data and stage scripts. Comparing against the local run needs only `grep`, `sed`, `sha256sum` and `jq` (jq is already used by the wrapper).

**The smallest sound gate is one full serial 1000-query run on S3.** The runner always runs the whole production shape (100k rows, 1000 queries, D1024). The only way to stop early is to fail. So "input admission" and a full correctness panel are the same execution. Its timings are admission telemetry only.

## Facts I checked

- **Same source.** The runner and library files are byte-identical at the qualified commit `4c2e535f` and at HEAD (runner `376366d1…`; generation, router, source plane, `sq8_s3_range` all match). `4c2e535f` is not an ancestor of HEAD, so record that.
- **One codec difference.** The codec file hash differs from the local baseline's identity line (`0f51015f` → `eddf88c6`, commit `a647f215`). That change only touches the scratch-admission formula, and D1024 is still 532,480 B. Scoring code is unchanged, so score-bit parity is a prediction to test, not an assumption.
- **Same generation, same inputs.** The S3 publication's `original_root_sha256` is `4ec270e5…`, the exact generation behind the local 97.23% run. Requests (`8460a81f…`) and truth (`479064239b…`) are the same files, already in S3 under `research/semantic-router/20261006/cohere-native-preflight-a0002/retained/prepared/{queries.f32,truth.u64}`.
- **Template is complete.** It has exactly the 20 `Config` fields, `kind:"s3"`, the SQ8 key suffix equals the SQ8 SHA, and the request/truth sizes are correct. It passes `validate_config` as written.
- **Local reference digests** (from committed `native-preflight-spot/a0002/preflight/baseline-result.jsonl`):
  - The prefix SHA reproduces `3f0ea7b6…`.
  - 1000 query lines, all `truth_opened:false` before the seal.
  - IDs, score bits, count and underfill projection: `cb37f660415dcde0178af84c7fbe8cee2b2c429587ba40d12f36560bb96cef0a`
  - Same plus per-query charges: `57b72b3806d869dbcdf83a3395a7b7e7024b4109b227cab75d4b7edf8afb3f45`
  - The v1 and v2 query records serialize these fields in the same order, so the projections are comparable byte for byte.
- **Local load:** 67,943 GETs in total (43–88 per query), 27.26 GB of verified logical bytes, 75 s of query wall time, 65.7 s CPU. Peak cgroup memory was 72.5 MB at CPU 1 / 512 MiB.

## Proposed envelope (root decides and freezes)

| Item | Value | Basis |
|---|---|---|
| Instance | c7i.2xlarge Spot one-time, max $0.50/h, same AMI, subnet, SG and profile as a0002 | unchanged |
| Native cgroup | `CPUQuota=100% AllowedCPUs=0 MemoryMax=512M MemorySwapMax=0 TasksMax=256` | unchanged |
| Scratch | 64M tmpfs at `/mnt/borsuk-retained-s3/query-scratch` (the template's `scratch_parent`) | staged metadata is about 0.47 MB; must be empty afterwards |
| Output | `$root/evidence/result.jsonl` on EBS, runner cap 64 MiB | about 11 MB expected |
| Native runtime | `RuntimeMaxSec=1500`, inner `timeout --kill-after=10 1470` | estimate 120–300 ms/query (2–5 min); pessimistic about 600 s. The 600 s cap from the publication run is too tight. |
| Instance cap | 2700 s, cutoff and timer at `pendingTime+2640`; refuse to start native unless 1500+10+240 s remain | |
| Reads | about 68k query GETs + 2 binding + 8 metadata + 2 HEAD; about 27.3 GB logical | local charges |
| Cost | compute ≤ $0.375 (0.75 h × $0.50), ancillary ≤ $0.15, **at most 2 attempts**, series ≤ $1.10 | Spot quote $0.21/h; GETs about $0.03 |

## Before freezing (root, cloud reads only)

```bash
B=borsuk-bench-453182569524-euc1; NS=research/semantic-router/20261006/cohere-native-preflight-a0002/retained
aws s3api head-object --bucket $B --key $NS/store/semantic/objects/07a14360b06add9a35f82b97f4e031690ff878ff497ee047902818823992337b  # ETag "f808095a3a2e788df2ae7d7d0e5423fa", 103600000
aws s3api list-objects-v2 --bucket $B --prefix $NS/store/semantic/s3-native-publication-smoke-a0002/ | jq -c '[.Contents[]|[.Key,.Size,.ETag]]'  # equals root-verification roster (11 objects)
aws s3api head-object --bucket $B --key $NS/prepared/queries.f32; aws s3api head-object --bucket $B --key $NS/prepared/truth.u64  # 4096000 / 80000
aws ec2 describe-subnets --subnet-ids subnet-0a12dbed0ca6fac25 --query 'Subnets[0].[VpcId,MapPublicIpOnLaunch]'
aws ec2 describe-route-tables --filters Name=association.subnet-id,Values=subnet-0a12dbed0ca6fac25 --query 'RouteTables[].Routes[]'  # need igw-* or S3 vpce, NOT nat-*
D=docs/research/performance-architecture-20260930/cohere1024/retained-s3-publication/admission-a0002
git show c490dfc7:$D/next-runner-config-template.json | jq -c .template > config.json; sha256sum config.json   # CONFIG_SHA → prereg
```

Get the runner from the `retained-native-publisher-gates/a0001` evidence (`./check_cohere_native_baseline` in `artifacts.sha256`). Check SHA `9a65fd61…` and size 16135912, then upload it create-only under the new attempt prefix as `inputs/runner`.

## Changes to the committed a0002 `user-data.sh` and `stage.sh`

- **Names:** a fresh prefix (for example `.../retained-s3-query-admission-a0001`), fresh unit names, ClientToken and tags.
- **Timers:** on-boot and deadline 1740 → 2640. Refuse check becomes `$(date +%s)+1500+10+240 <= deadline_epoch`. `RuntimeMaxSec=600` → 1500.
- **Scratch:** `mkdir -p $root/query-scratch` and mount the 64M tmpfs there; unmount the same path in `finish`.
- **Staging:** download `runner`, `config.json` and `stage.sh` from `inputs/`. Fetch `queries.f32` and `truth.u64` from `$NS/prepared/` with `s3api get-object --if-match <etag>`. Put all 5 in `pins.sha256` and check sizes 16135912, 4096000 and 80000. Do not copy requests or truth into `evidence/`.
- **`stage.sh` native line:**
  `/usr/bin/time -v -o $evidence/runner.time timeout --kill-after=10 1470 $root/runner $root/config.json $CONFIG_SHA $evidence/result.jsonl > $evidence/runner.stdout 2> $evidence/runner.stderr`
  The scratch-empty check uses `$root/query-scratch`. The cgroup before/after checks stay as they are.
- **Terminal:** schema becomes `borsuk-retained-s3-query-admission-v1`, `binary_source_commit` is `4c2e535f…`, `performance_claim:false`.

## Acceptance (root, in a new empty directory, after fetching the terminal independently and checking the evidence SHA)

```bash
R=ev/result.jsonl; L=<(git show c490dfc7:docs/research/performance-architecture-20260930/cohere1024/native-preflight-spot/a0002/preflight/baseline-result.jsonl)
tail -n1 $R | jq -e '.phase=="terminal" and (.summary|.status=="MEASURED" and .complete and .queries==1000 and .underfilled_queries==0 and .recall_numerator==9723 and .recall_denominator==10000 and .sum.failed_gets==0 and .binding_charge=={"submitted_gets":2,"verified_bytes":28307,"failed_gets":0})'
head -n1 $R | jq -e --arg c "$CONFIG_SHA" '.binary_sha256=="9a65fd6137eaa3929a1ee05fc798ef12c8fb8ab9970099c9b6de3c1031d15f72" and .config_sha256==$c and .runner_source_sha256=="376366d16f50ee3f21d719c443f68bf4c798032c33be77844c26f1407b9d0f0f"'
# library source hashes: compare against `git show 4c2e535f:crates/borsuk/src/{two_bit_generation,semantic_unit_router,rotated_two_bit,two_bit_source}.rs | sha256sum`
for f in prefix sealed; do n=$(tail -n1 $R|jq .summary.${f}_bytes); s=$(tail -n1 $R|jq -r .summary.${f}_sha256); test "$(head -c $n $R|sha256sum|cut -c1-64)" = "$s"; done
head -c "$(tail -n1 $R|jq .summary.prefix_bytes)" $R | jq -s -e 'length==1005 and all(.[]; .truth_opened==false)'
proj(){ /usr/bin/grep -E '^\{"phase":"query","ordinal":' "$1" | sed 's/,"stages":.*$//'; }
test "$(proj $R | wc -l)" = 1000
test "$(proj $R | sed 's/,"charges":.*$//' | sha256sum | cut -c1-64)" = cb37f660415dcde0178af84c7fbe8cee2b2c429587ba40d12f36560bb96cef0a
test "$(proj $R | sha256sum | cut -c1-64)" = 57b72b3806d869dbcdf83a3395a7b7e7024b4109b227cab75d4b7edf8afb3f45
diff <(proj "$L") <(proj $R) | head   # only on mismatch: first divergent ordinal
```

Use `/usr/bin/grep`, not the hook-rewritten `grep` (memory note: the hook has dropped lines before). The wrapper evidence must also show:
- native and final exit 0
- cgroup `memory.events` with oom and oom_kill 0, `memory.swap.peak` 0, `memory.peak` under 512 MiB
- pins unchanged before and after, scratch empty
- the instance terminated

## Falsifier and outcomes

The parity hashes above are the test. The prediction is that all 1000 ordinals are byte-identical with the local run (IDs, score bits, counts, underfill and per-query logical charges), giving 9723/10000.

| Outcome | Condition | What happens |
|---|---|---|
| **ADMITTED** | Every check passes | Proceed to the canary |
| **INVALID (parity)** | MEASURED, but either digest differs | No canary or panel. Find the root cause at the first divergent ordinal. If only the charges digest differs, it's an accounting divergence. If the IDs/scores digest differs, check the codec/SQ8 drift and the S3 read path. |
| **INVALID (execution)** | Non-zero exit, timeout (124/137), `failed_gets>0`, OOM, scratch left behind, pins changed, or terminal missing | Preserve the attempt. At most one new attempt, with the algorithm and limits unchanged, and only after the environment cause is shown. |

Parity failure is never an algorithm KILL. OneAttemptS3 has no retries, so a single transient 5xx across about 70k GETs invalidates the attempt. That is why 2 attempts are budgeted.

## After this gate

- **Canary:** the frozen measured-panel wrapper, run once with a deliberately wrong CONFIG_SHA argument. The runner must stop at stage `config` with 0 completed queries and no S3 query reads, and exit 2. That exercises staging, IMDS, the CLI, exit propagation, terminal upload, cleanup and termination.
  - Real: bootstrap, awscli, input staging with pins, systemd cgroup, terminal and evidence upload.
  - Absent: S3 query reads and truth reduction.
- **Measured panel:** the identical run, frozen. Its scope is serial correctness and latency only. It makes no QPS or vendor claim.

## Risks and housekeeping

1. **NAT cost.** The a0002 launch settings don't pin a public IP. If the subnet routes through NAT, 27.3 GB costs about $1.4 extra per attempt. Confirm igw or an S3 gateway endpoint before launch, or re-budget.
2. **Latency prior is unmeasured.** This is the first real S3 query run. The 1500 s cap is my estimate, not evidence. A timeout would be an environment INVALID, not a verdict on the algorithm.
3. **Stale index in this worktree.** It has the v1 runner (`15f06f8b…`) staged and stages deletions of committed evidence, including all of `retained-s3-publication/` and `compare_native_replay.rs`. Run `git reset` (mixed) before any commit; don't commit from it as is. I read the evidence from HEAD.
4. **Temp quota.** The Claude temp directory hit its quota (EDQUOT) during my session. I removed only the temp dir I had created. Expect lost output on large commands until it's cleaned.

I edited nothing and ran nothing in the cloud. Launch, resources and freezing are yours to decide.
