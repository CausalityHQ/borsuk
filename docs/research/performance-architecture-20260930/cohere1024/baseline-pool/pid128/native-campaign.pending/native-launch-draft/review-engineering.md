# engineering_critic — 98620c79dd5249b8

Group 259d91aacc8c4b8f; immutable input 7cffa1552a62fab65f6c983524d027dd3e57132a.

What this change does: It adds authenticated native-input transport, a finite campaign worker, and an independent termination/collection watcher. **Hold launch: four release blockers remain.**

All locations below refer to commit `7cffa1552a62fab65f6c983524d027dd3e57132a`, under the supplied `native-campaign.pending/` prefix.

1. **[P1 — unsafe failure cleanup] Publication failures bypass immediate shutdown.**  
   `native-launch-draft/native-worker.sh:87–99` exits on receipt, archive, size-limit, or upload failure before reaching `shutdown`. Early setup failures also precede creation of `/mnt/borsuk-pid-evidence` at line 128, so packaging fails even when the reporting tools are available. With no uploaded terminal, the watcher waits until approximately launch+17,900. A failed setup can therefore leave idle compute running for nearly five hours and lose its diagnostic evidence.

   **Smallest repair:** make shutdown unconditional after bounded, best-effort reporting; initialize minimal failure evidence before setup; package only available evidence and exclude installer bodies. Let the watcher detect premature instance termination without waiting for an absent terminal.

2. **[P1 — irreversible evidence loss] The archive omits bytes required for native-result replay.**  
   `native-worker.sh:92` archives only `borsuk-validator` and `borsuk-pid-evidence`. Raw diagnostic/query JSONL and generated native configs remain under `/mnt/borsuk-pool-pid`: see `review-repair-r7/run-native-pid128-r7.sh:924–925,965–970` and `run-staging-native.sh:90–91`. The recipe verifies raw byte offsets and prefix/seal hashes at `run-native-pid128-r7.sh:659–684`; the archived projections and identity records cannot reproduce those checks. `native-watch.sh:68–80` then confirms instance termination and volume absence before collection.

   **Smallest repair:** publish a bounded, explicitly enumerated set of raw result files, exact generated configs, and required receipts before terminal publication. Authenticate that roster and ensure replay works from retained artifacts alone. Preserve the existing volume-absence-before-collection ordering.

3. **[P2 — premature termination/evidence loss] The cleanup reserve uses a later deadline than the watcher.**  
   `native-worker.sh:158` admits work using launch+18,000, while `native-watch.sh:39` uses +17,900. The worker additionally permits parent runtime 16,860 seconds, a later polling deadline, and 90 seconds of cleanup (`native-worker.sh:32,162,167`). Packaging, syncing, and hashing at lines 91–94 have no deadline; uploads receive separate timeout budgets. The stated reserve therefore does not establish that terminal publication finishes before the watcher terminates the instance.

   **Smallest repair:** budget backward from **+17,900**, including controller teardown and bounded packaging/publication. Clamp the worker’s cleanup and publication operations to that shared absolute deadline; refuse native start when the complete allowance cannot fit.

4. **[P2 — resource safety; authentication safely refuses] Support downloads lack a body-size bound.**  
   `native-worker.sh:150–152` downloads each support object completely before checking its expected length and SHA. A wrong oversized object can consume substantial disk space during the 30-second GET even though the later check rejects it. The installer download at lines 112–115 has the same ordering. The bounded ELF transport does not cover these downloads.

   **Smallest repair:** bound support GETs to expected length plus one sentinel byte, retaining exact-length/SHA verification. Add the known installer download-size bound. These failures currently prevent native execution; they are not false authentication successes.

The 19 support bodies and their config references match the committed length/SHA pins. The watcher correctly requires termination and volume absence before collection, and its collection status remains `ROOT_REPLAY_REQUIRED`; I found no demonstrated scientific false-PASS path.

**Verdict: fix 1–4 before launch.** Verification was limited to committed-source inspection and read-only pin checks. No entrypoints, builds, network calls, experiments, or prior review jobs were run; runtime remains unverified.

