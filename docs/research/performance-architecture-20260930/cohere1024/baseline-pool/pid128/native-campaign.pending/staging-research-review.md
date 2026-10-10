What this change does: it reuses the admitted diagnostic config and native runner, changes the scratch path, and attempts one real ordinal0 query. Its success status correctly requires a later root gate.

I reviewed checkpoint `3eb1379bda59cc87399f4d82694c6c6fb41c6943`. The supplied entrypoint, library and recipe hashes match. All 32 extracted fragments match their recorded hashes and byte counts; the library is their exact concatenation plus two comments.

1. **[P1] A staging evidence path can invalidate the preserved admission roster.**  
   [Entrypoint line 36](/tmp/borsuk-pid128-native-campaign-next/run-staging-native.sh:36) permits any fresh descendant of the campaign evidence root. Line 39 creates it before authenticating admission closure.

   For admission at `$E/admission/admission.json`, supplying `$E/admission/staging-new` passes these checks. The closure enumeration then includes the newly created staging files and rejects them against the old roster ([library line 439](/tmp/borsuk-pid128-native-campaign-next/staging-library.sh:439)). The attempted smoke leaves admission evidence that no longer satisfies its immutable roster.

   **Minimal repair:** reject overlap with the admission directory before the first `mkdir`. A fresh sibling namespace satisfies the intended plan.

2. **[P2, inherited] Cleanup can return zero despite losing required cleanup proof.**  
   The writes of `cleanup.kill.txt`, `cleanup.drained.txt` and some exit records are unchecked ([library line 99](/tmp/borsuk-pid128-native-campaign-next/staging-library.sh:99), [line 104](/tmp/borsuk-pid128-native-campaign-next/staging-library.sh:104)). `finish` disables `errexit` before calling cleanup.

   If one of these writes fails but later manager capture and stop succeed, `cleanup_owned` can return zero with incomplete evidence. Thus `cleanup_exit==0` alone does not establish the cleanup proof required by the proposed negative test.

   **Minimal repair:** propagate required evidence-write failures into `cleanup_rc` while continuing teardown. If exact r3 extraction must remain unchanged, enforce the required records independently in the staging root collector.

3. **The acknowledged manager-final race remains present.**  
   [Library line 253](/tmp/borsuk-pid128-native-campaign-next/staging-library.sh:253) waits for cgroup drain, then performs one manager-final read. It subsequently requires terminal manager fields. Cgroup emptiness does not itself establish that the manager has finished updating those fields.

   This can reject an otherwise successful query as INVALID; it does not establish a performance failure. **Minimal repair:** bounded waiting for terminal manager state while retaining the same InvocationID and ControlGroup, before stopping the unit. This is an inherited, already-disclosed risk.

The configuration binding is sound **under the stated prerequisite of genuine, externally closed admission**: the source-config SHA must equal admission’s config SHA, the recipe SHA is fixed, and the closure helper binds admission, terminal, manifest, wrapper and external proof bodies. The immutable checks retain file bodies and stamps, parent identity and SQ8 ETag. Deleting only `scratch_parent` for comparison correctly checks the intended native-config transformation. I found no missing global in the current call order.

The error flow mostly fails closed. After traps are installed, ordinary failures and signals enter `finish`, which attempts owned cleanup and exits 98 for failure. If ownership acquisition fails after launch, cleanup explicitly refuses unauthenticated control and requires root reconciliation. The future collector must handle that case.

There is also an important receipt limit: a late manifest or sync failure can change the final exit to 98 after `terminal.json` or `wrapper.exit` recorded zero ([library line 127](/tmp/borsuk-pid128-native-campaign-next/staging-library.sh:127)). The planned external collector must use the actual enclosing process exit. This is already consistent with the declared external-closure design.

Exact helper extraction is a reasonable small alternative to introducing a staging mode into the full recipe. Its hashes establish provenance; they do not establish runtime cleanup behavior. Runtime source authentication additionally depends on protecting the library and its parent directory against replacement between hash checking and `source`.

**Verdict:** repair the namespace defect before execution. Keep this checkpoint UNVERIFIED. The controlled cleanup negative, external collector, canaries and campaign transition remain declared future gates, not newly discovered omissions. No reviewed code, AWS operations, corpus access or children were executed, and no files were edited.
