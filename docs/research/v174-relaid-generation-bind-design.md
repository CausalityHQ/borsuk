# V174 relaid generation identity fence

The V172 row-map artifact authenticates a permutation and its four
declared identities. V174 makes `ServingGeneration::bind` require that map,
retains the authenticated router manifest digest in `SourceRouterArtifact`,
and checks map generation, source, router manifest, old SQ8, new SQ8 and
row count against the router, mirror and page authority. It permits the
router's old physical page size to differ from the relaid SQ8 page size;
the router summaries remain in old order. A nonidentity two-row binder
test and a router-loader digest test form the narrow compile gate.

The first binder revision `848cffab4cdb6696a069d592e7cc23069586fa34`
passed one Causality Spot `c7i.8xlarge` narrow gate:
`pq64_router_artifact::tests` **2/2** and the
`serving_generation::tests` name filter **8/8** (two new binder tests plus
six `graph_serving_generation` tests). The complete terminal SHA-256 is
`6d59cb80391f4934c404c1efcca73544b9b74b1f3663c953950015d427b3453b`
at `s3://borsuk-bench-453182569524-euc1/research/v174-relaid-bind-compile/848cffab4cdb6696a069d592e7cc23069586fa34/runs/a0001/`.
The controller rehashed all three closed artifacts and confirmed Spot
instance `i-0e74b3a552362bc95` terminated. Compile/tests took 98.19
wall seconds and peaked at 4,991,460 KiB RSS on the build worker.

The next code slice adds bound-generation helpers that convert router
nominees into new SQ8 positions and new candidate rows into old PQ code
positions. This is still not a completed relaid query path. The
generation builder must verify map direction against authenticated old
and new SQ8 row identities and bodies before publication. Exact-source
lookup is by the stable ID
stored in each relaid SQ8 row through `NativeSourceIdMap`; it must not
assume a source-tier ordinal equals either physical SQ8 ordinal. An
end-to-end test must independently check returned IDs and exact-source
reranking. No recall, serving latency,
charged RAM or recovery claim follows from this binder test.
