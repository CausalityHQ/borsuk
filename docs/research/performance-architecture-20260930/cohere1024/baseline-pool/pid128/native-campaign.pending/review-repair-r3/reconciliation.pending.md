# Dual review reconciliation: source repair remains incomplete

Original immutable review inputs are preserved in candidate e349ab5be8be8692ce0c5da0844ae3738bcfed56 (its ancestry includes 84cb4bda and196b9164), and /tmp original files are unchanged. Dual group bb4d7d570218480a completed: Astra5496caac and Opus2701a939. Full separately labelled results are retained. No reviewer ran recipes/native/corpus/build/AWS or edited files.

Draft repairs (static only):
- Collector evidence-directory existence check moved after fenced terminal-state capture, while the canonical existing output parent is validated before polling. Fixes early initialization race.
- r5 finish reserves actual return97 for closure write/manifest/sync or cleanup failure. Intended negative98 is retained only if those operations succeeded; actual enclosing return is required. Exact stage closure roster validation remains pending.
- Finite campaign and platform-pair finish explicitly propagate final receipt/sync/required stop failures and preserve distinct final failure return. Campaign teardown has a separate45-second safety grace; it does not extend measurement time.
- Widths now copies/authenticates private admission, uses it as the immutable descriptor/roster baseline, and reauthenticates original admission/full closure in fresh evidence subdirectories after each width. Evidence/admission namespace overlap is refused before first mkdir.
- r5 payload has BindsTo and After bound to its authenticated original observer unit. Separate target observer-death/drain falsifier remains required.
- Negative fixture path is adjacent to entrypoint and finite campaign binds the authenticated timeout_fixture role to that exact path.

Unresolved blockers: terminal cgroup removal proof and matching admission/stage closure schema; negative final payload/observer resource-event validation and witnessed descendant survival/drain; exact stage manifest roster. /bin/true positive fixture may report zero maximum RSS and needs a fixture that satisfies the real runner's positive RSS admission without relaxing the runner. Source wrapper/bootstrap additions are outside the completed review prompt and require integration verification.

Authoritative static scopes: run-p256707/192f4432 (first repairs,2.331s,200.3MiB,exit0); run-p338993/8e181b49 (private widths,1.649s,205.2MiB,exit0); run-p413860 (lifetime/namespace/fixture,2.190s,214.3MiB,exit0). bash-n+ShellCheck only; each CPUQuota100%,AllowedCPUs0 plus explicit taskset0,256MiB,noSwap,Tasks128,RuntimeMax120. No runtime or mock was run. No launch readiness/performance claim.

Systemd255 primary source independently confirms SERVICE_EXITED calls unit_prune_cgroup and pruning removes empty cgroups. See https://raw.githubusercontent.com/systemd/systemd/v255/src/core/service.c and https://raw.githubusercontent.com/systemd/systemd/v255/src/core/cgroup.c. Removal must be recorded as removal, never fabricated populated-zero counters.
