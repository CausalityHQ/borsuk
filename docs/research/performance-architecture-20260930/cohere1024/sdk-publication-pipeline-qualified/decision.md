# Qualified future SDK publication pipeline

Source: e808144ee8f00745a2e5d7f4aa48afedc6da281a followed by
21b3f8f94cff7d8db95eebbc314e7a34a396828f. This benchmark adapter change
allows eight publication/readback RPCs with one application compute lane.
PUT starts and signed SDK transmit admissions are spaced by at least250ms
without catchup; listing stays serial. Every response capture is call-owned.
Existing raw bits, complete readback, ownership/transmit accounting,
earliest ordered error and full started-call drain remain required.

The delayed-credentials regression failed the earlier pipeline at36.330906ms
between authenticated admissions. The repair passed all22 native SDK tests,
feature-aware workspace correctness/suspicious Clippy and the actual
shim-unset workspace test build on Causality EC2. See exact receipts.
Non-denied warnings remain; complete workspace runtime tests were not run.

The synchronous hook may block the single polling lane for the remaining
spacing interval. It establishes SDK admission spacing after signing,
not DNS/TLS, connector-entry, wire-send or server-arrival spacing.
No real-service canary or full throughput run was executed for this source.
This is source qualification, not a measured upload speedup or vendor win.

The already-running matched SDK experiment retains its original pinned
source, ELF, configuration, resources and invocation. This revision does
not alter its results. The prior unoptimized canary binary is not a binary
of the final repaired source and must not be used as one.
