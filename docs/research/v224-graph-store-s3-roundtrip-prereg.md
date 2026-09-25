# V224 graph generation S3 round trip

**Decision:** Does the production library publish the frozen V223 ReLAION-1M
graph through a conditional S3 head and load that same generation cold and
warm from one pinned root? This is a persistence gate, not a query benchmark.

- Source: one frozen commit and SHA-256 source archive. Input root is
  `docs/research/v223-relaion-1m-generation.json`, SHA-256
  `c59650ec920d031ff236f5ab47331db88b71fdac0462cd07a568d51c549b7caf`.
  Five input blobs have the byte lengths and hashes in that root. Input keys
  remain the immutable V219/V196/V115 keys used by V223; the worker verifies
  every download before calling the library.
- One c7i.4xlarge Spot instance in eu-central-1c using `causality`, a fresh
  attempt and a fresh publication prefix. One process publishes, reads back
  the head, hydrates cold, then hydrates warm from the same cache.
- Pass only if the returned root digest matches, generation=196,
  N=1,000,000, D=768, cold GETs=5 and bytes=1,879,697,462, warm GETs=0 and
  bytes=0. Record wall time and peak RSS as resource measurements. This gate
  makes no query latency, recall or competitor performance claim; V223 retains
  those measured values for the unchanged search method.
- The worker watches Spot interruption. An interrupted attempt is discarded
  and restarted under a new prefix. Sync terminal artifacts and hashes to S3,
  verify readback from the controller, and terminate the instance immediately
  at the terminal marker. Configure incomplete multipart upload expiry on
  the bucket before production deployment.

On a pass, retain the content-addressed layout and conditional head. Next,
wire publication into a generation switch with pinned readers, mutation and
compaction. The subsequent scale gate remains a frozen 10M build/serve test
under an explicit RAM and recall envelope; 100M RAM may rise with required
recall and is not inferred from this test.
