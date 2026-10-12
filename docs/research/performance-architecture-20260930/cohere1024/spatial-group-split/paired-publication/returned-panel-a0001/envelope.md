# Returned panel aggregate envelope r2

Supersedes only protocol.r1's aggregate host/cost limits. Individual native
phase/memory/GET/byte/quality limits remain unchanged. The sum of native caps
is 2100s (four60s preflights, two300s panels, six180s completed reductions,
one180s paired reduction). Each supervisor also has bounded hashing/manager
and drainage overhead. Setup1200 plus native2100 plus evidence/cleanup needs
more than the old3600 aggregate once those overheads are included.

Use host7200s, external watchdog7320s, termination allowance120s, matching the
prior panel host safety envelope. Campaign outer cap5700s after setup; stop
immediately on terminal. No extension after launch. Spot bid0.25/h yields at
most$0.516667 compute over7440s; reserve0.15, ceiling$0.75. If zero-instance
Spot capacity refusal is observed, a separately preregistered On-Demand
exception at observed0.4074/h costs at most$0.84196 compute; reserve0.15,
ceiling$1.10. Confirm price and actual request before freeze.

No native phase cap or scientific threshold is relaxed. This is prospective
host safety arithmetic, not measured execution time or performance.
