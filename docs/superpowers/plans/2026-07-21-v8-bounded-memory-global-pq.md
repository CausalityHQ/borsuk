# V8 bounded-memory global PQ implementation plan

1. Add red tests for 64 MiB chunk flushing, packed locations, zero-copy chunk
   decoding, sidecar record metadata, compact offset tables, honest RAM budget
   accounting, and the Deep-Image shortlist default.
2. Replace the v7 global-PQ Parquet codec with a v8 descriptor/chunk codec and a
   streaming builder that exposes full chunks for immediate persistence.
3. Extend v8 vector sidecars with ID/generation and compact row-range entries;
   update reranking to consume exact records from those row reads.
4. Persist chunks during the second build pass, load them sequentially, validate
   checksums/layouts, and scan chunks in bounded parallel workers.
5. Persist exact resident and sidecar-index estimates in `GlobalPqRef`; enforce
   them in the manifest budget and expose them in reports.
6. Bump the storage format to v8 and update tests/examples that assert the
   unreleased format version.
7. Run focused tests, the crate suite, formatting/lints, and local memory smoke
   tests. Recreate every AWS benchmark index, then regenerate result tables,
   resource charts, recall/latency curves, and tuning documentation.
