# Frozen nomination loss diagnostic

Reuse the existing planner, capture its already computed candidate-page ranking
only on an explicit diagnostic call, expose selected pages from the existing
BudgetedPagePlan. Serving calls do not allocate a trace; diagnostics reserve
at most159 usize IDs from query scratch and use the same semaphore. Returned
trace memory belongs to the caller after completion. Test exact plan equality
and ranking in the existing generation fixture before using the diagnostic.

Run at most64 previously observed validation queries256–319 on the rejected ReLAION100k root. Check
exact equality to the same slice of original terminal validation plans. Compute GT coverage
of discovered159 pages, nominal ranked84 pages ignoring GET bridging, actual
selected pages and fetched ranges; compare existing returned/native-flat hits.
These stage sets are not nested and losses must not be presented as additive.
No parameter change, quality qualification, new fit, cloud request or production
latency claim. A diagnostic result chooses the next material change; it never
qualifies the rejected candidate. Existing required design review is held by
its native cooldown; do not override or start duplicates.
