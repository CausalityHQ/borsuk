# Fresh 1M panel construction gate

Freeze campaign config SHA256 `453fadf2ee6aec8a834d0c95c54f9cd85d2ca4e12a7bdc62d66ce760de75737f` and helper config SHA256 `6d0d4e0bd350ecea929db7c478116703f6f52c0eb30a9cc48f1056e7dec0f199`. Exact selected panel is 17,578 bytes, SHA256 `2e2c0507b9bb61468b39aa718d4f5799900ad66724da6ba711fe1f8ec9340530`. Its separate metadata input disposition preserves the original selector resource FAIL and grants no resource, vector or ANN qualification.

Reuse unchanged preparation: authenticate original FIRST1M source, decode fixed selected vectors, reject raw/normalized duplicates against the indexed corpus and consumed queries without replacement, compute exhaustive cosine k100 truth, widen original ordinal IDs unchanged, and conditionally seal outputs with authenticated HEAD/GET readback. Repeat the helper's duplicate audit/oracle replay on the same remote host. No ANN queries or quality selection occur here.

Resource admission: c7i.2xlarge Spot, Ubuntu 24.04, encrypted 80 GiB gp3/delete, configured memory.max 8 GiB, memory.swap.max 0, CPU 200%, Tasks 512, numerical-library threads 2; worker 7200 s/service 7260 s/machine 9000 s. Require exact configured limits, zero OOM/swap/task-overflow events and complete owned-process closure. Record actual cgroup peak and reclaim counters, including possible kernel peak overshoot; this prospective gate does not impose the old selector's observed-peak predicate. Keep old FAIL unchanged. Source and output identities must pass regardless of resource observations.

Spot bid ceiling $0.50/hour gives a maximum 9000-second compute bound of $1.25, plus $0.15 EBS/S3 allowance. These are preregistered bounds, not measured cost. Interruption/failure preserves terminal artifacts and terminates every acknowledged instance; no automatic replacement, query replacement or incomplete-cell reuse.

Root collection authenticates exactly 20 small terminal bodies and the completed remote replay receipt. Multi-GB inputs remain on the disposable host; their exact identities are recorded remotely. Do not rerun the oracle locally. Stop/terminate owned compute before collection. A failed duplicate/oracle/seal/resource/identity gate does not qualify a panel.

Verified before launch: wrapper mocked self-check, actual helper20/code27/ref7 authority preflight, whitespace check, Bash/PYTHONPATH user-data generation (6881 bytes for root proof). Actual construction, oracle and resource attainment remain unmeasured until the original terminal closes.
