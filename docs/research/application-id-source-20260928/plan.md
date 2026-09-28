# Separate SQ8 application IDs from source-row addressing

Base7b97ec31. Current SQ8 writer stores ordinal IDs and source builder reuses
those IDs as raw-row seek offsets; arbitrary IDs cannot reach object-native
construction. Separate the two roles, without a second encoder/router.

Reuse common SQ8 writer with optional source-indexed signed-i64 ID slice:
new build_sq8_source_with_ids validates full unique ID roster and order,
charges caller order/IDs plus sorted duplicate-check copy, then writes unchanged
norm/codes and logical ID. Existing ordinal-ID convenience remains useful for
corpus controls, not a legacy format reader or migration fallback.

Reuse source builder with explicit validated physical-position->raw-ordinal
order (build_with_order), unique SQ8 logical IDs validated separately. Existing
ordinal convenience derives the same order from ordinal SQ8 IDs. Charge explicit
caller order plus streamed ID roster/bitset; no full vectors or source-ID map
is resident in the generation. Builder exposes the same explicit-order method.

Source plane marker v2 for both construction methods, mandatory canonical
little-endian-u64 source_order_sha256 in receipt. Reader accepts v2 only; reject
v1 clearly. New root/child/body hashes remain bound by existing immutable
publication/head CAS and conditional page reads; no old artifacts rewritten.
Historical benchmark results stay tied to their archived source and schema.
No new JSON order array, CLI alias/reader or persistence migration is needed;
first document/test the Rust library API. Existing CLI ordinal convenience
naturally emits v2. Non-SQ8 caller state is outside helper payload admission.

One red/green integration fixture:512 D2 unit rows, reversed physical order,
signed IDs including i64 extremes, duplicate-ID and order/cap failures before
publication, source-order SHA, create/open/reopen/publish/remote reload/plan,
exact returned-range scoring yields real logical IDs with deterministic ties,
and old plane marker rejects. Then current source and generation regressions,
focused SQ8 tests, no local build/full suite/cloud. Single original Spark job.
This enables ID binding, not incremental mutation/compaction or ANN quality;
both-vendor/100M/fresh HTTP gates remain OPEN. Next overlay work reuses existing
mutation version and publication machinery, with authenticated logical-ID state.
