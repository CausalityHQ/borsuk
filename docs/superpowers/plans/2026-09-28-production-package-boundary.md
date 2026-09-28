# Production package boundary

Bounded release repair: current BORSUK has no prerelease compatibility contract. Remove only the old V41 learned-router module/export and two unregistered V25/V41 campaign examples from the current crate; preserve historical behavior in original immutable revisions, never rewrite historical receipts. Their private crates remain workspace research members, but the production dependency graph must not include them.

Use the existing native authenticated source/build/open/search APIs unchanged. Give public helper packages accurate descriptions and the existing project license. Refresh the lock graph, package the three production crates together offline, and inspect normalized manifests/fixtures. Build and test from extracted archives on the existing Spark host only, including source normalization/SQ8 and authenticated HTTP failure cases, plus package metadata. Do not publish or start cloud benchmarks.

Success: core archive produced, private dependencies absent, extracted package compiles with packaged helper crates and passes selected lifecycle/search checks. Failure: stop at the first packaging/build causal layer, repair narrowly. No release or vendor-win claim before all remaining product gates.
