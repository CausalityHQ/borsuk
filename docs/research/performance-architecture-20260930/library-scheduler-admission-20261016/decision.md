# Library-owned range scheduler admission
The library now admits ordinary future/outcome drain state per active query. SOURCE and SQ8 run sequentially, so their scheduler charge is a checked phase maximum. Split configuration replaces its scheduler share; direct closure adds only the excess; tracing adds retained spans/probes. The CLI no longer supplies the library's ordinary scheduler reservation.
The result protocol is v12 with explicit library ownership and exact model checks. Historical full-run validation requires the old scheduler reservation and upper cap. Headers omit some old caller capacities, so this is a checked scheduler floor, not reconstructed equality for every historical caller byte.
Production scoring, routing, physical layout, retry policy and concurrency defaults are unchanged. Memory is a conservative admission model, not a measured heap/RSS guarantee. Remote metadata staging can precede geometry admission.
| Check | Evidence |
|---|---|
| Precursor native regression | 40 source-walk + 37 range + 55 runner + 307 reducer tests, all passed |
| Streaming provenance helper | 13 tests and release build passed |
| Repaired accounting | 4 module + 1 full-run + 1 startup-binding tests passed on the combined source |
| Final exact-source gates | Release, workspace correctness/suspicious Clippy, real shim-unset workspace test-build all exit 0 |
| Build failures | Preserved; explicit release-profile cache invalidation passed without source changes |
The qualified build source is 1ec2d07f414865f32f6369c00aa93a69ed2c7755. Integration verifies changed-file SHA256s against this source; its canonical Git commit includes this decision and verification metadata.
The provenance helper detected that retained canonical vectors reconstruct the normalized derivation rather than the original raw corpus. The original raw corpus was subsequently recovered with exact SHA256. Both outcomes are preserved separately; neither alters the frozen BORSUK ABBA results.
Next gate: qualify the separate S3 Vectors original-raw input contract, run actual full-input admission and a disposable real service canary, then freeze a matched comparison. Turbopuffer credentials remain an external prerequisite. No new ANN performance result or measured vendor win is claimed by this library increment.
