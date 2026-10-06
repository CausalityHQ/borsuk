# Root preimplementation audit

Status: hypothesis under independent review; no native implementation or quality result.

The completed research is consultation `44c5af9c05414c41` (effective GPT-6.1 Sol after authentication failures). Independent review group `bf121fb269bb4ea6` is running; research critic fell back to Sol, engineering critic is Astra. This is a new design review, not a repeat of prior placement reviews.

## Independently recomputed arithmetic

For N=100,000,000, D=768, 16-row groups, target 56 groups/object, hidden width64 and four mixtures:

- Label grid side335;112,225 objects.
-223,676 model parameters; complete modeled root4,496,144 bytes.
- Maximum full object798,784 bytes (64-byte header +1024*(768+12)).
- Cold envelope65,536-byte head +4,718,592-byte root ceiling +15 full objects =16,765,888 bytes.
- Remaining16MiB slack is only11,328 bytes. This is an empty-delta envelope, not a qualified update envelope.

Every query must admit head/root/delta and selected bodies together. A nonempty durable delta reduces legal base-object fetches. Opening/parse transients, pinned roots, concurrent queries, maintenance and allocator capacities still require explicit admission; the serialized-root formula alone does not prove resident RSS.

## Existing input seam

`co_selection_layout::SourceSelections` stores authenticated anchor identities, nominees and group membership, but does not retain query vectors. Its anchors are private and have no public precomputed-selection constructor. A new fitter therefore needs an authenticated source-vector streaming adapter, or an explicitly owned API extension with the same source/hash/ID admission. Do not deserialize arbitrary training selections or claim existing metadata alone trains the proposed query-dependent model.

## Decisions pending review

The exact deterministic optimizer, initial membership, move objective, held split, operation ceilings, empty labels and candidate truncation behavior are not frozen. Actual selected-object coverage must govern acceptance; smooth loss improvement and exhaustive-object oracle coverage are separate diagnostics. No parameter sweep after rejection is authorized by this proposal.

The first gate must separate object concentration, candidate-enumeration loss, learned selection loss, and unchanged SQ8 ranking. Source-held agreement is not external ANN recall. Incremental searchable new IDs, stale-row suppression, pinning and in-process compaction remain full product requirements.
