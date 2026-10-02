# Fixed48 offline evaluation

This is a prospective reducer for the sealed v3 production diagnostics. It never runs search, opens query vectors, constructs truth, or changes the already selected panel. Historical arm outcomes remain unchanged.

Inputs are frozen descriptors (path, bytes, SHA-256) for the scorer config, sealed 68-row JSONL, order.u64, new LEi64 truth (64 by 100), and the root's closed measurement receipt. Require receipt execution exit 0, resource and cleanup gates true, and exact sealed file/config/binary/source identities. The reducer reports only evidence supported by that receipt; it cannot independently establish S3 performance or truth-construction chronology. The scientific driver must seal the measurement and its receipt before invoking the one exhaustive GT construction.

Authenticate and validate measurement inputs and the exact ordered identity/startup/64 frozen_query/all_queries_frozen/terminal roster before opening truth. Require matching FROZEN summaries, no truth use, ordinals 0–63, full source/order/request/root bindings, finite nonnegative timing and transfer charges, bounded unique IDs and ranges, and valid trace unit/page geometry. Fail closed on partial, duplicated, reordered, failed, or tampered rows. Authenticate the full order bijection and the truth's 100 unique in-range logical IDs per query.

Reproduce the six mappings in implementation-gates/combined-source-contract.json → scorer_source_contract.offline_evaluation. Report per-query and aggregate hits at 10 and 100 for nominated units, page closure, SOURCE-scored units, SOURCE-ranked pages, admitted SQ8 ranges, and returned IDs. Never equate coverage with returned recall. Return hit denominators are 640 and 6400. Report observed local wall/process CPU and logical I/O separately from unmeasured physical S3/cold HTTP.

Frozen scientific classification: page-closure hits10 <608 is discovery FAIL; closure passes but returned hits10 <608 is downstream FAIL, identifying the first stage that crosses the floor. Complete and qualified results meeting both floors may proceed to cold measurement. This is an arm-specific gate, not a universal product threshold. Report recall@100 separately. Preserve execution success separately from scientific GO/FAIL; invalid evidence is an error, never a scientific PASS.

One new stdlib Python reducer with an explicit config/hash/output CLI, replay and synthetic self-check is sufficient. No new controller, cloud job, native compilation, plotting, compatibility mode, or framework is needed.
