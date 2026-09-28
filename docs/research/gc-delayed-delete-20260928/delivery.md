# Epoch-fenced callable reclamation: verified increment

| Gate | Result | Evidence |
|---|---|---|
| Ambiguous DELETE then retry and acknowledged mutation | Reproduced NotFound, fixed | retry-small; generation-green-sealed |
| Same sequence for empty generation | Reproduced NotFound, fixed | generation-red; generation-green-sealed |
| Capped retry leaves claim/SQ8, then recovered compaction | Acknowledged generation survives late metadata DELETE | generation-green-sealed |
| Reject stale prepared root/physical owner before PUT | Pass | application IDs integration |
| Admission before I/O, signed IDs, replay, claims, caps, CAS, HTTP, config | Pass |14 focused tests total |
| ActualS3 conditional publication/GC/reload | Pass; generation2,4 logical deletions, IDs[1,2,99] | authenticated terminal/logs |
| Independent implementation review | No Critical/Important within declared contract | implementation-review.md |
| Frozen source parity and resources closed |13 Rust files exact; worker terminated, scoped active filters empty | source-parity.json, aws-closeout.json |
| Whole workspace/release | OPEN:52 original failures remain unqualified | previous full gate1630 pass/57 fail/6 ignored;5 fixtures separately fixed |
| Matched quality/latency/QPS per lifecycle dollar vs both vendors | OPEN; no new performance or quality measurement | prior immutable research decisions |

MutationBTMUT002 binds snapshot bytes to control epoch. Populatedrootv3 and emptyrootv2 bind prepared epoch. Maintenance owner48hex is16 epoch+32UUID; immutable jobv2 claim binds epoch/index, and direct publication rejects old/foreign physical owners before staging. Epoch-changed compaction jobs rebuild fresh keys. No permanent reclamation records or compatibility readers.

Contract: one host, same trusted maintenance directory, ALL readers registered/coordinated and dropped before GC, writes/publication follow common CAS and prepare from the captured sealed epoch. External immutable application objects outside library GC ownership remain application responsibility. Work caps model payload/SDK operations/logical deleted bytes; SDK/runtime/allocator/versioned retained objects and RSS are separately charged. Bounded prefix scans can require a larger scan cap for progress. No multi-host or service-availability qualification.

Next ordered gates: preserve these immutable receipts; reconcile52 old full-gate failures using current-contract or explicit diagnostic fixtures while preserving security/corruption/admission coverage; narrow failing layers then one final full assurance; then choose/review one distinct source-only routing arm after prior killed arms/Fable reconciliation, preregister paired100k budgets and only scale winners. Freeze defaults/revision only after architecture qualification. Active full goal continues; no operator decision currently required.
