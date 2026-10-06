# Publication API review decision

Reviewed source: `9a6d6c63c45a1878afdf190ac3328dd2633e9b9a`. Dual review group: `b27c6c97a0a14cff`; research `aa0d1af7b93f40ad`, engineering `bea132c08e964a61`, both completed with exit 0. The CLI subsequently committed at `93567af1` was outside this review's scope.

Status: native UNVERIFIED; final freeze held for one bounded repair in the same worker. Neither reviewer demonstrated committed-head corruption under the immutable-input assumptions. Formatting checks and reviews do not prove Rust compilation or runtime correctness.

## Required before qualification

- Reparse the serialized replacement manifest and check every original low/step f32 bit. Paired query arms share the replacement root, so their parity alone cannot detect a publication-induced coefficient change.
- Validate shared object ownership against the destination publication, matching the existing fresh publisher's foreign-maintenance rejection.
- Move the existing serving validation to the completed destination root before the head create. Do not add a second serving pass.
- Strengthen the existing negative fixture: precise base-epoch error; fresh mutation-bearing head; identical competing head immediately before native Create; destination nested inside the retained prefix.
- Preserve original errors and metadata after an attempted head create. Do not add implicit success recovery on an ambiguous response.

The root sent these concrete requirements once in message `1791311590350910912-1002214`. Preserve API, CLI and provider-documentation checkpoints; use a separate repair commit and the same mandatory test names.

## Input and backend conditions

Native create-only copies do not pin a source version atomically. The modeled cumulative scratch bound assumes frozen immutable assets; it is not a hard bound against concurrent raw-file replacement. The experiment must seal those assets before publication and prohibit in-place writes or source GC while the destination references shared payloads.

LocalFileSystem Create copies use hard links. Source and destination must share a writable filesystem; EXDEV/EROFS are environment INVALID. Source files remain immutable after linking. S3 requires the provider's create-only copy capability; NotSupported is a setup failure, not an algorithm rejection. Keep host safety limits unchanged.

Deferred: a new error hierarchy, copy-framework redesign, automatic ambiguous-write recovery and format migration. None is needed to run the frozen retained-index comparison. Generic product maintenance and the matched physical S3 benchmark remain unfinished goals.
