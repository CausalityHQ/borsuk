# SQ8 attribution source review: held for repair

Candidate bc44d3342513feb80391e0bba2d76a8f485c92dd is source-only and not natively qualified. Both independent reviews completed; their review exits are not native test evidence.

Root verified obsolete refusal assertions, duplicate diagnostic memory charging and a mutable/clonable trace reservation. The same worker committed an intermediate repair 7dad7636089e31243ca2aa974bd4fe1d10905044. Remaining repairs cover shared range-state accounting and complete startup admission, payload-release timing, internal trace capacity and explicit host-wide interrupt counters. Source review does not establish timing overhead, compiled layout, native correctness or performance.

Baseline serving stays default. The direct arm remains opt-in after its preserved cold-pair result: GETs decreased 48%, bytes increased 38%, CPU increased 27%, and p99 increased 82%. No vendor win is claimed.

Next gate: verify the final repaired source and exact test roster; remote affected tests, release, workspace Clippy and real shim-unset workspace test compilation; then real-input admission and a separate overhead/cleanup canary before the bounded 128-query diagnostic. No scale campaign, competitor rerun or cache change is authorized by this record.
