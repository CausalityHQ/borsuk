# SOURCE block32: retain a small CPU improvement

Date: 2026-10-17. Candidate d014838fbce1d433b90092f0d20051887422b3ef, control native14aec21db4690c14e0841346f96759a51fe1518c (main a8fd1066d47df6970c4348cc7b35482d44b73ba2).

## Decision
Retain the candidate as a workload-specific CPU improvement with unchanged quality and logical reads. End-to-end wall time is flat and order-sensitive; no reliable latency, throughput, concurrency, memory, lifecycle-cost or vendor win is established.

## Native change
Full32 SOURCE units use byte-major blocked accumulation, preserving each lane's addition order and finalization. Partial units retain the original scalar callback/score loop. Public scalar scorer remains an independent oracle. Conservative admission adds1200 bytes per active query; this is a payload model, not measured stack or RSS. Exact source hashes: rotated_two_bit.rs 1e50138edad0ca4bace95fa367dce02b83a06f2f7eaea248b40432a8638c108a; two_bit_generation.rs 472be3e27c19490f171eda97195b5f514d67301df2f6c20caf3929029e642a86.

## Frozen full1000 ABBA
Cohere Wikipedia multilingual-v3,1M,D1024,cosine,k10; same1000queries repeated twice per arm, not2000 independent queries. Same m6i.4xlarge Spot i-05de5d253e64865e3; serial queries, cacheoff, fetch64, CPU4/16GiB/noSwap operational bounds. Corpus/query/truth/generation authorities remain in frozen packet and original configs.
| Cell | CPU seconds | Wall seconds | p90 ms | p95 ms | Serial QPS | Hits/10000 |
|---|---:|---:|---:|---:|---:|---:|
| A1 |119.731208|167.303373|209.780474|226.238636|5.977166|9783|
| B1 |115.224877|162.277496|203.753001|221.043206|6.162284|9783|
| B2 |115.176093|169.622241|205.010050|220.963709|5.895453|9783|
| A2 |118.543991|165.085357|207.712583|221.964053|6.057472|9783|

Pooled totals B/A: CPU0.9669532183533301 (3.30%lower), planning0.9259730702675224 (7.40%lower), wall0.9985288513735732 (0.15%lower). Forward wall0.9699594995480795, reverse1.0274820486604366. These descriptive observations do not establish significance. Mean run p95 is not pooled p95. Candidate RSS82.5–96.0MB vs control94.1–94.8MB is not a memory-saving claim.

All four canonical semantic streams SHA bed26a50b8902be295b94d753f5974694e232f68c1f24a0ad28a109d5c1c2c9b, retaining ordered IDs/scorebits/rows/plans/logical charges and excluding only clocks/transport counters. Recall97.83% each. Each1000query cell: SOURCE109872GET/28967583744verified bytes, SQ830041GET/10722616576bytes; zero failed logical GETs.

## Verification
Exact candidate source437 files remained byte-identical to Git before/after native gates. Native a0003 stages all exit0: affected debug codec (5passed/1ignored), affected generation (51passed), new release (7passed/1ignored), ignored release primitive, release products, workspace Clippy correctness+suspicious, actual shim-unset workspace testbuild. Compiler concurrency8 on bounded EC2; no local compilation. Runtime actual-input admission and separate twoquery canary each arm plus strict readers0; full ABBA4 native and4 strict readers0. Release query ELF a3678661701d97541c66632bd4b7bb73d5fa0fcd0d392266c523b3b60d9de415; reader0d32db8c47f218200a40e8eee9162dd33a708ff041b6711f3ba807e5fcd92a61.
Original a0001 interrupted by AWS Spot reclamation is execution INVALID despite an EXITtrap0; missing stage completions are not PASS. a0002 retained primitive exposed partial-unit regressions; candidate fallback fixes those. Old four-row/gather historical dispositions are unchanged.
Source reviews af426571da2a4f0a and partial fallback2e57cb9eff304935 reconciled. Final result-only independent audit f4b31e6423f24078 supports CPU retention, not an independent receipt/source verification. Its suggested BAAB repeat is not launched and is not required to describe this completed block honestly.

## Immutable evidence
S3 bucket borsuk-bench-453182569524-euc1, prefix research/semantic-router/20261017/source-block32-closure-a0001/.
source.tar.gz SHA213abab7e9ca126d9f50a10cdd543fa38b8226397b6bcf25293b20a1bffd5bc9; evidence.tar.gz SHA7b46ee8676304ea092e22a75bc69dd75e960cddba19f3a7fd68bfe4e00749d8b.
Full summary SHA2b8fe812b033cbb2b5b292b1cc9f9b07b9f76241e7fb799c68566321f6090e65. Original full unit invocation49dfc6637cff49c0a799a4f2b9dda557; analysis SSMfc17c301-b61a-40b6-a717-fdad7370ffbc Success0. Original gate commands/resources/logs/readers/configs/raw JSONL/ELFs and failed attempts are archived.

## Next falsifiable step
Inspect SOURCE range scheduling and physical cover from closed traces before changing code: about110 SOURCE GETs/query with64 download slots implies multiple waves. Determine whether merging ranges reduces waves without altering selected rows, and quantify extra bytes/RAM before selecting one bounded intervention. No new experiment is authorized by this document; root retains experiment admission/smoke/freeze. Matched S3 Vectors/Turbopuffer comparison and total lifecycle/cold-concurrency gates remain incomplete. Higher recall than an old unpaired vendor run does not establish a matched win.
