# Conditional two-bit generation publication

Reuse the existing streamed hash-checked multipart uploader. SQ8 is already
published by the caller at the immutable key/ETag in the prepared trusted root;
this operation checks its HEAD identity/size and never rewrites or hydrates it.
Validate local generation, stage fixed metadata under generations/ROOT_SHA,
CAS head last. Scope is prepared-generation publication, not source/layout
creation or mutation/GC. Reuse old upload mechanics, no new dependency.

- [x] Focused red: publish/read head/reload, next generation, stale writer and
  wrong namespace refusal; invalid metadata must not advance head.
- [x] Bound multipart payload buffers; hash every uploaded file before completion.
- [x] Opaque namespace-bound head token, monotonic generation/CAS, lost-ack
  readback reconciliation. Read head/root with bounded authenticated reads.
- [x] Focused verification, source/test receipt, docs and fast-forward push.

Missing-API red observed in81311; first integration8583 failed on root
path separator encoding. Corrected metadata path joining at the shared helper
and canonical fixtures; final focused99568 exit0 passes all local/remote/head
cases. No SQ8 rewrite/hydration, paid job or full suite. Receipt in
`docs/research/native-two-bit-publication-check.json`. Lost-ack reconciliation
is implemented from existing publisher mechanics; not fault-injected here.
