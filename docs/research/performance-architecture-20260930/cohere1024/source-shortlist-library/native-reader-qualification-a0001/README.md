# Native v9 fixture and panel-reader qualification

Source `ccbb561bec72e97bc18df363ea307616811af264`, qualified on 2026-10-15.
This delivers the SOURCE-preserving v9 fixture converter, current-format panel
reader, and report conservation fixes. The production library remains the
previously qualified `bf7ada24` implementation. No new default, cold-performance
result, actual-input parity result, or vendor win follows from this qualification.

## Decision table

| Revision / gate | Result | Disposition |
|---|---|---|
| `90508968`, full example suite | 247 passed, 5 failed | Preserved failure: current native producer versus historical reader assumptions; later stages unrun |
| `ccbb561b`, exact native qualification | All 21 stages exited zero; 258 example tests passed, 0 failed, 0 ignored | Qualified fixture/reader increment |
| `1da4f9a`, additional CLI/rank/SOURCE-oracle work | Source-only, not compiled | Not delivered or qualified by this receipt |
| Fixed actual-eight input admission and disposable smoke | Unrun | Next execution gate |
| Fresh cold 1,000-query baseline/candidate comparison | Unrun | Required before selecting a performance winner |

## What changed

- The fixture converter authenticates and copies the original SOURCE/router
  artifacts and streams unchanged SQ8 into 32-row proofs. It neither refits
  SOURCE/SQ8 nor authenticates the canonical corpus body by implication.
- The v9 reader checks its explicit budgets, 32-row canonical coverage,
  partial-tail charges, raw seals, selected ordinals, and traced admission.
  Header serialization and panel lines are bounded before growing parsing work.
- Historical 256-row loss/source-utilization analysis clearly refuses v9.
  Historical fixture envelopes remain labeled and retain their numerical oracles.
- Count/block reports reject gains exceeding the baseline's missing truth hits.

The original engineering and research reader reviews are included. Their
findings were reconciled in the frozen source before native execution.

## Native evidence

- AWS profile `causality`, region `eu-central-1`, Spot `m6a.2xlarge`.
- Instance `i-0383e3cecc88754bc`; original command
  `229fc6ac-5de3-4b30-a936-a9546369c088`, terminal `Success / 0`.
- Native interval 14:41:47–15:19:27 UTC. CPU0–7, 28 GiB, swap0, pids512,
  compiler jobs2. All OOM counters and swap peak zero. The 16,824,418,304-byte
  cgroup peak includes compilation and filesystem cache; it is not query RSS.
- 477 source/support and 444 native files authenticated before and after,
  with identical complete rosters. Independent verifier exited zero;
  qualification JSON SHA256
  `52cc6700a6437a28ea5c864e1297468f393800f55ebf01f9538f9c590616a7c2`.
- Compute independently rechecked: instance terminated, volume
  `vol-0095132d936346fbb` absent, external timer inactive, background marker off.

Stages: affected test compilation; converter and two report tests; five original
regressions; two geometry tests; panel reader, policy, bounded-header and trace
tests; full test listing and all 258 example tests; workspace Clippy; actual
shim-unset workspace test compilation; release build; retained binaries.

Required workspace commands ran on this exact source:

```text
cargo clippy --locked --workspace --all-targets -- -D clippy::correctness -D clippy::suspicious
env -u BORSUK_TEST_BUILD_COMMAND BORSUK_TEST_BUILD_JOBS=2 bash scripts/check_rust_test_build.sh
cargo build --locked --release -p borsuk --bin check_cohere_native_baseline --example compare_native_replay
```

## Retention and limits

Bucket `borsuk-bench-453182569524-euc1`, prefix
`research/semantic-router/20261015/source-shortlist-v9-panel-reviewed-a0001/`.
Complete correctness evidence is under `stages/v9-panel-reviewed/`.
Retained `release.tar` under `retained-release/` is 30,074,880 bytes, SHA256
`abd5fd15ddb53b69e21d76f6ba29a09d33bed610ce944ca36e1d649ce454e8c7`.
Native binary hashes are included. A future deployment must authenticate the
downloaded whole archive and exact members before execution; this local audit
did not download and rehash the remote release body.

The existing 256-candidate diagnostic remains 9,786/10,000 hits versus 9,780,
with 31.1887% fewer planned SQ8 bytes. SOURCE/router traffic is separate.
Those diagnostic figures are historical evidence, not a measured latency gain
for the newly integrated library. The next gate uses the fixed eight queries,
unchanged authenticated SOURCE/router/SQ8/request/truth bytes, exact returned
f32 score bits and independent coverage, then a separately frozen cold run.
