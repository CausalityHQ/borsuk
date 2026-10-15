**Hold launch: four source-proven glue issues need repair in `443a0d28`.** References below use that checkpoint.

1. **Smoke can falsely report successful deletion.** `smoke-v9-object.sh:20` uses `[[ $original == 254 ]] && grep ...`. Under `set -e`, failure of the first command does not exit. HEAD returning **0 (object exists), 124 (timeout), or another non-254 error** therefore reaches `CREATED_READBACK_DELETED`. Make both conditions explicit fail-closed checks.

2. **Prep failures bypass cleanup verification.** In `run-v9-chain.sh:21–22`, a nonzero manager/client exit triggers `errexit` before checking the cgroup. Closeout immediately hashes and archives evidence, potentially while a service still writes it. Capture the original exit explicitly, always verify or finish draining the owned service, then return that original failure. Keep cleanup outcome separate.

3. **A second invocation can damage the original run’s evidence.** The chain installs its closeout trap without acquiring exclusive ownership (`run-v9-chain.sh:5–21`). A duplicate invocation truncates `prep-transport-binary.log` before encountering a unit/directory collision, then attempts terminal publication for the same attempt. Acquire a create-only attempt claim **before the trap or any output redirection**; failed claim acquisition must neither write evidence nor close anything. Bind service cleanup to its recorded InvocationID, not merely the fixed unit name.

4. **Stage admission does not reserve enough closeout time.** `run-v9-stage.sh:28` reserves `cap+180`, while its manager client allows `cap+110`, cleanup allows another 12 seconds, and chain closeout allows 155 seconds. These bounds exceed the admitted remaining machine time. The final smoke-object stage also lacks a machine-deadline check. Reserve teardown/publication time consistently before releasing each stage; refuse work that cannot fit.

Native integration checked against `ee598553`: the CLI argument shapes, strict wrapper schemas, payload budgets, three acceptance-status literals, source-hash headers and native no-retry settings agree. The freezer caller matches the supplied contract. Pending setup fields are intentional refusals, not findings.

**Actionable plan:** make these small Bash repairs, freeze the support packet with the separately owned freezer, then perform one bounded control canary before root proceeds with actual8 admission and the separate smoke. Keep cold1000 behind those gates.

**Bounded falsifier, proposed only:** one ≤180-second disposable real-systemd canary, without scientific bodies or ANN execution. Attempt duplicate entry, exercise a prep exit of 17 and a client timeout, and check the smoke predicate with HEAD statuses 0, 124 and 254 plus genuine 404 text. Require unchanged original evidence on duplicate entry, separately preserved exits, authenticated cgroup drain before archiving, and success only for confirmed absence. Any violation blocks freeze.

No files or jobs changed; no scripts, native tools, AWS calls or scientific bodies were executed/opened. Real systemd/AWS behavior, the freezer implementation, corpus admission and quality remain unverified.
