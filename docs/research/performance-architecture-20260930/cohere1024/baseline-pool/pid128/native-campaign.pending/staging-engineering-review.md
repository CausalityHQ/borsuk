What this change does: adds one real ordinal0 staging query using the extracted recipe helpers and a fresh scratch directory. **Three source-level issues need repair before staging execution.**

1. **[P1] A permitted evidence path can invalidate sealed admission evidence.**  
   [Entrypoint line 36](/tmp/borsuk-pid128-native-campaign-next/run-staging-native.sh:36) accepts any descendant of the campaign evidence root. Passing `$E/admission/staging-new` satisfies this check; line 39 creates directories there, and subsequent operations write files before admission closure authentication. The exact roster comparison then rejects the newly modified admission directory at [library line 440](/tmp/borsuk-pid128-native-campaign-next/staging-library.sh:440). Failure leaves those additions behind, invalidating the original closure for subsequent use.  
   **Minimal repair:** reject evidence paths inside the admission directory before the first `mkdir`; preferably require a fresh direct child of the campaign evidence root.

2. **[P2] Final preservation checks trust an admission receipt that is no longer authenticated.**  
   Admission is authenticated initially, but [entrypoint line 80](/tmp/borsuk-pid128-native-campaign-next/run-staging-native.sh:80) rereads the original path. [The helper](/tmp/borsuk-pid128-native-campaign-next/staging-library.sh:387) compares current state against that file without checking its pinned hash. `check_inputs` omits admission and its closure artifacts. Consequently, changing an ignored admission field after initial authentication can still produce staging success despite invalidating the sealed admission hash; changing its expected roster also compromises the later preservation comparison.  
   **Minimal repair:** use an authenticated private admission copy as the comparison baseline, and reauthenticate the original admission and closure roster before success. Repeated closure validation needs separate output directories because the helper’s evidence filenames are create-only.

3. **[P2] Filesystem admission discards both `stat` exit statuses.**  
   [Entrypoint line 38](/tmp/borsuk-pid128-native-campaign-next/run-staging-native.sh:38) compares command-substitution output directly. Two failed reads returning empty output compare equal; equal output accompanied by failure also passes. `set -e` does not make this comparison validate either command’s status. This regresses the explicit checked assignments in [recipe line 395](/tmp/borsuk-root-pid128-repair-r3/run-native-pid128.sh:395). The existing closure test source already contains `first_stat_nonzero` and `second_stat_nonzero` cases for this failure.  
   **Minimal repair:** reuse the recipe’s checked assignments and numeric device comparison exactly. Apply those negative cases to the staging guard too.

The failure-path trace otherwise preserves the intended separation: `run_phase` records manager and payload exits, checks ownership before cleanup, and rejects missing native exit records. `finish` converts execution or cleanup failures to exit 98. However, failures while writing the closure manifest or performing the final sync can leave earlier terminal fields—and, for final-sync failure, `wrapper.exit`—showing zero. The actual externally captured observer exit remains essential. The documented single-read manager-final timing race also remains.

Static verification confirmed that all four temporary files match checkpoint `3eb1379b`, the supplied entrypoint/library/recipe hashes match, and every extracted fragment reconstructs the library exactly. I found no missing caller global on this staging path. The source-config/admission hash binding and scratch-only config comparison are present.

**Verdict: fix 1–3 before real staging.** The acknowledged cleanup falsifier, external collector, transition driver, and canaries remain separate release gates.

Not checked: runtime behavior, AWS, corpus or truth. No reviewed code was executed; no files were edited.
