# SQ8 stage/publication fault canary — UNVERIFIED source handoff

Commit: `58258fe1af451c9c9d090205d00038ec5c517056`.
Exact parent: `c590b2ceecaf9ff157d109fcd0800b38c718d759`.
Worktree: `/home/rb/worktrees/borsuk-sq8-stage-publication-faults`.
Branch: `devbox/sq8-stage-publication-faults`.
Initial clean stale HEAD preserved at
`refs/heads/archive/sq8-stage-publication-faults-opening-37c4adcf`
(`37c4adcfe8eb2cd3d464f7beb81c42bb9bfbfff5`). Support-only non-cone sparse
checkout was configured BEFORE exact-base alignment. No historical full docs checkout.

OWN1 added executable, 222 lines:
`docs/research/performance-architecture-20260930/cohere1024/source-shortlist-library/sq8-tail-diagnostic/support/control-sq8-stage-publication.sh`.
SHA256: `1a54bf8742e86721b5fbfc60d876b3be0e88186fda3c6c635e39efb69de8bb16`.
No existing source, callers, production pins, scientific gates, config, or dependency
files changed. Root owns the later pre-input chain call and manifest integration.
Author and committer remain Roman Bartusiak <riomus@gmail.com>; no trailers,
push, merge, review, consultation, or children.

## API and root execution contract

Install alongside the actual runtime scripts at `/mnt/borsuk-v9-panel` and
include it in the root's pinned runtime manifest/support archive. After root
qualification, before input transport or installing ANN actors, root may add its
single owned call:

```bash
owned stage-publication-faults 180 /bin/bash "$root/control-sq8-stage-publication.sh"
```

No public arguments. The private `--bounded` entry is used only by its own
`timeout -k 25 150` supervisor (175 seconds maximum); invoke the public no-argument
entry. Root retains remote launch, CPU/resource/spend authority and the outer
180-second deadline. Parent cgroup must actually read CPU `100000 100000`, effective
CPU `0`, memory `2147483648`, swap `0`, pids `128`. Nested real system units use
the actual stage's same CPU/memory/swap/pid properties. Needs root, cgroup v2,
systemd system manager, `/usr/bin/time`, coreutils, Bash, Perl, awk, jq, tar.

Fresh directory claimed exclusively:
`/mnt/borsuk-v9-panel/evidence/stage-publication-faults`.
An existing directory or final result refuses a second run; preserve existing
receipts and inspect the original attempt rather than deleting/retrying it.
Root actors `bin/original`, `bin/candidate` and query/truth input paths must be
absent; only sandbox copies of `/bin/bash`, synthetic Bash config/programs and
synthetic setup metadata are used. No scientific bodies or SDK data are opened.

Only after every assertion, source rehash and final exact-unit drain, the script
atomically hard-links its synced summary to
`/mnt/borsuk-v9-panel/evidence/stage-publication-fault-canary.json`, with:
`status=STAGE_PUBLICATION_CANARY_PASS`, `native_actors_executed=false`,
`scientific_bodies_opened=false`, `aws_mocked=true`, `real_systemd=true`,
`production_actor_authentication_claimed=false`. A failing run exits nonzero and
does not publish that final PASS file. Preserve the whole evidence directory.

## Actual source read and source binding

At exact base, unchanged production file SHA256 values are:

| Source | SHA256 |
| --- | --- |
| `run-v9-stage.sh` (both outer and `--owned` branches, lines 1–106) | `cf04b49a593b0e00a6f2f02d8161a976df0e44d07ec11b5f743e4b2235bf1f26` |
| `run-sq8-panel-chain.sh` (`verify_code` lines 14–18; `finish` lines 19–28) | `d02711ae018f3584aeaf31244d7b32510fc9c2f199dff7f49dcb59778001933b` |
| `close-v9-runtime.sh` (lines 1–23) | `c0235148eab60ed23d9b683e750b46b0db486bc8dc8f040b91961757774921af` |

The remote canary hashes the installed originals before work and rechecks them
on exit, including failure. Runtime hashes bind the installed source, rather
than hardcoding this chain hash (root will add the authorized owned call).
The exact `verify_code()` and `finish()` bodies are extracted; no reimplemented
finish predicate, fabricated manager result, or full chain execution. Each
function start must occur exactly once and its extraction must end at the
top-level closing brace. Every literal substitution below counts occurrences
and refuses zero or multiple matches. Adapted stage copies also get before/after
hashes. The close copy is in the manifest verified by the extracted verifier.

## Exact sandbox substitutions

For each full stage copy:

1. One `root=/mnt/borsuk-v9-panel` literal becomes that case's private root.
2. One `unit=borsuk-v9-cold-$phase.service` literal becomes a unique
   `borsuk-v9-stage-canary-<uuid>-<fault>-canary-original.service`.
   Actual owned cgroup membership, token, systemd supervision and drain code
   remain intact, using that exact unit.
3. The unique original-arm two-line block
   `  cap=60; [[ $phase != *panel-* ]] || cap=120` followed by
   `  binary=$root/bin/original; pin=59a22eed2e7d3cb8142e35108171cdcd8c27ec83c8968ba307cd9dc092c78083`
   becomes `  cap=<5|2|15>; binary=$root/bin/original; pin=<copied Bash SHA256>`.
   Phase remains the actual accepted `canary-original`; synthetic config hashes
   are computed and checked by the actual script. No reader/candidate pin changes.
4. ONLY in the client-timeout copy, the exact
   `"$((cap+90))" systemd-run` becomes `"2" systemd-run`.
   Native timeout flags, unit limits, token guards, cleanup and close logic stay exact.

For each close copy, the unique line
`original=$1; phase=$2; root=/mnt/borsuk-v9-panel` has only the root path replaced.
The extracted chain functions are unchanged. Their harness supplies sandbox
`root`, a valid hash pin for a manifest containing that adapted close script,
`code_checks=0`, phase `synthetic-publication`, and an original exit of 0 or 17.
The synthetic setup's support SHA is the installed close-script hash, not an
assertion about a genuine support archive. Fake machine stop epochs are now+1000.

## Case roster and expected remote assertions

| Case | Outer exit | Native / timeout / manager | Other assertions |
| --- | --- | --- | --- |
| Native exit17, cap5 | 1 | 17 / 17 / 1 | actual exact cgroup/token/InvocationID, drained, forced=false, execution_closed=false |
| Native timeout, cap2, synthetic sleep30 | 1 | null / 124 / 1 | absent native receipt stays null, drained, forced=false, execution_closed=false |
| Client timeout2, native cap15/sleep30 | 1 | null / null / 124 | forced=true, exact invocation matches cleanup receipt, drained, execution_closed=false |

All stage cases write a synthetic partial result before failure; no result hash
or bytes success receipts may appear. On-disk native/timeout/manager receipts
must agree with supervision JSON. A duplicate full stage call must refuse 125
and preserve every existing sandbox file hash. Each initial stage call has a
35+2-second external bound; duplicates have 5+2 seconds.

| Publication case | Original | Publication | Extracted finish exit |
| --- | --- | --- | --- |
| Archive succeeds, terminal fails; original success | 0 | 43 | 126 |
| Archive succeeds, terminal fails; original failure | 17 | 43 | 126 |
| Archive upload fails | 17 | 41 | 126 |
| Cleanup-unverified guard before archive construction/upload | 17 | 125 | 126 |

Every publication case checks the exact original/publication/phase log. The
15+2-second bounded harness uses the exact verifier and finish; the unmodified
close source controls the uploads. Strict PATH AWS spies accept only the exact
conditional put-object argv/bucket/keys/body paths, never call another AWS binary,
and make no network request. Terminal-failure cases must record precisely two
calls, a copied successful archive body and a retained partial failure receipt;
archive failure precisely one call, retained partial archive receipt, no terminal
artifacts; pre-archive refusal no calls and no archive/terminal artifacts. Spies
reject repeat attempts. For the three cases that produced partial receipts, a
5+2-second duplicate direct close must refuse 125 and preserve all root/AWS file
hashes and call counts. Nothing retries a successful archive upload.

Cleanup runs from EXIT/TERM/INT handling and checks every registered exact unit.
Active cleanup requires its original evidence token, matching systemd Environment,
and stable InvocationID before cgroup.kill and again before exact-unit stop.
No wildcard kill, reset-failed, shared system changes or unrelated unit control.
Each manager probe/stop has a finite timeout; failed/inactive drained units retain
their receipts. Cleanup uncertainty creates the campaign cleanup-unverified marker
and prevents PASS. Nested unit RuntimeMaxSec remains cap+40: 45/42/55 seconds.

## Static evidence and honest gaps

Final local guarded command receipt `04c921`: `bash -n` exit 0, ShellCheck exit 0,
`git diff --check` exit 0, commit exit 0. All source/Git/static command units used
CPUQuota=100%, AllowedCPUs=0, MemoryMax=256M, MemorySwapMax=0, TasksMax=128,
RuntimeMaxSec=120. Commit hooks were disabled for the commit to preserve the
explicit source-only/no-runtime policy. Final worktree clean, exact parent and
author/committer inspected. Only the new owned file appears in the commit.

UNVERIFIED: no delivered canary, extracted function, generated program, AWS spy,
mock, test, native executable, Cargo, cloud/network operation or review was run
locally. Root must perform qualified remote execution and inspect its original
job, stage receipts, cgroups and final JSON. Dispatch timing/systemd return-code
assumptions are intentionally strict and may fail the canary; they are not yet
measured. No production actor authentication or scientific correctness/performance
claim follows from a passing synthetic control canary.

The actual close source writes local intent records with `execution_closed=true`
when original=0 before terminal publication succeeds. This canary deliberately
retains and checks those partial intent records; its proof of incomplete success
is the failed terminal upload, original/publication pair, finish exit126 and lack
of a successful terminal-publication receipt. It does NOT claim every partial
JSON's execution_closed field becomes false, or repair production behavior.
