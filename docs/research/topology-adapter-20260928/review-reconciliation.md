# Scoped implementation review

Opus ecbf51f941cd40f2 found no blocker for this synthetic x86 correctness run. The unedited review is retained. This is a narrow code review, not a replacement architecture critique; default group4f5c4d5b61b849e1 and its cadence remain intact.

## Required before corpus use

The paired demo is a generic authenticated-root comparison; authentication alone does not establish a topology-only treatment. The experiment controller must independently hash-check both current-v3 manifests, remove only graph_sha256 and graph_resident_bytes, and require all remaining semantic fields equal **before paired queries**. The adapter already creates that relationship, but the controller must prove it rather than infer it from a filename. It must also check the ordered arm/root-SHA roster on all128 emitted records. These remain launch HOLD items.

The same-root synthetic replay deliberately proves semantic plan/trace parity, alternating labels and unchanged ordinary output. The separate adapter fixture proves a distinct graph with unchanged source metadata and normal reloading. Distinct-root arm/hash association is a corpus-controller gate; the synthetic same-root check does not prove it.

No warm-up query or first-query quality deletion is added. Planning wall/process CPU metadata is exploratory, with alternating order and a control-first initial sample; report both all64 and an ordinal1–63 timing sensitivity from retained records. All64 quality samples and fixed integer gates remain mandatory. These timings are not cold HTTP/vendor measurements.

## Other independently checked limitations

- Source component reads are capped; source is authenticated first, then the fully copied staged root is authenticated again. A changed copied file fails before the no-replace rename. No hard links or original-root overwrites are used.
- Publication is atomic/exclusive through the existing rustix NOREPLACE pattern, but this offline adapter does not fsync or promise crash durability. Spot interruption invalidates the experiment cell. Temp directory permissions remain0700 for the single owning process; production publication/maintenance APIs are separate verified code paths.
- Keeping the TempDir guard makes failed staging self-cleaning. After successful rename its old path is absent; drop cleanup has nothing to remove. No additional keep/orphan path is needed for this offline caller-owned scratch namespace.
- Reported byte/resident counters refer to graphs; they are not total object storage or measured RSS. Control resident declaration is checked by the normal loader against actual graph preflight. The subsequent campaign must record every component's actual byte/SHA identity separately.
- diagnostic_plan only acquires its semaphore and executes synchronous plan_inner; neither it nor unit_centroid_graph spawns detached tasks. Process CPU can include runtime bookkeeping, and is explicitly process CPU, not thread CPU. Wall/CPU clocks stop before JSON serialization.
- This offline adapter uses Linux rustix facilities, matching the declared AWS platform. It is not a new portable production builder option, and graph/default/compaction formats are unchanged.

Source-current assurance subsequently passed: focused2/0/0, full2684/0/26 existing ignored across147 targets;391 Rust/Cargo files and four closed artifacts reconciled, original worker terminated. No corpus quality run is authorized by this review alone.
