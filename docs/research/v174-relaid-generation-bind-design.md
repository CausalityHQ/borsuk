# V174 relaid generation identity fence

The V172 row-map artifact authenticates a permutation and its four
declared identities. V174 makes `ServingGeneration::bind` require that map,
retains the authenticated router manifest digest in `SourceRouterArtifact`,
and checks map generation, source, router manifest, old SQ8, new SQ8 and
row count against the router, mirror and page authority. It permits the
router's old physical page size to differ from the relaid SQ8 page size;
the router summaries remain in old order. A nonidentity two-row binder
test and a router-loader digest test form the narrow compile gate.

This is an identity fence, not a completed relaid query path. The
generation builder must verify map direction against authenticated old
and new SQ8 row identities and bodies before publication. Query code must
convert old router nominees to new SQ8 positions and new candidate rows
back to old PQ code positions. An end-to-end test must independently
check returned IDs and exact-source reranking. No recall, serving latency,
charged RAM or recovery claim follows from this binder test.
