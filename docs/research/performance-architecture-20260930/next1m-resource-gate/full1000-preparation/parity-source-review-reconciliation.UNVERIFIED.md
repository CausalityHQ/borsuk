# Parity source review reconciliation — source fixes, runtime UNVERIFIED

Completed dual group2a8383c685114ddb reviewed immutable c9e6f957. Research2b388590e1f94d3d continued via configured Sol fallback after provider limit; engineeringa89db2a780e64aa2 completed. Both full reports are preserved.

1. Exit predicate defect independently verified. Fixed specifically for matching parity caller/report schema, EXACT_PREFIX_PARITY and complete=true; existing modes remain unchanged. Writer success and occupied-output tests authored.
2. Config hash reuse contradiction independently verified. Added pairwise distinct native configuration hashes alongside distinct artifact hash/inode checks. Resealed synthetic contradiction test authored, preserving frozen expected rows except the intentionally conflicting authority.
3. Pathname binding gap independently verified. Keep comparison on original descriptors; reopen via safe traversal and compare exact identity stamps after prefix checks. Repeat run pathname checks before report. Identical-byte pathname replacement test authored.
4. Role-alias truth boundary independently verified. Metadata-only statat with safe parent traversal and nofollow entry checking rejects any of seven role inode aliases before reading run bodies. The authenticated run reader checks its opened inode against admission before reading its first row, rejecting replacement between metadata admission and open. Direct and hard-link role tests authored.

All six tests and compilation are UNRUN; local bounded source formatting only. A shared-writer synthetic dispatch fixture is labelled synthetic. It is not authentic1M producer evidence. The exact release CLI must additionally consume original historical/fresh-local/fresh-S3 sealed artifacts and original process/source/provenance receipts on causality EC2. No production/cold/vendor claim and no measured launch is authorized by this source checkpoint.

Next remote correctness gates: owning example tests including unchanged historical modes, locked release example, locked workspace/all-target Clippy correctness+suspicious, actual env-unset workspace test-build with jobs1. After passing: freeze full1000 recipe, actual-input admission, separate realS3 canary, then preregistered characterization. Production parity/performance/maintenance goals remain incomplete.
