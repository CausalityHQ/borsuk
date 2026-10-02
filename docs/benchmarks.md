# Experimental measurement evidence

The [latest offered-load measurement](../docs/research/performance-architecture-20260930/semantic-1m/fixed48/offered-http/a0002/decision.md) covers CoHere FIRST1M,
768 dimensions, cosine, top 10, and the same 64 sealed queries at each offered
rate. Each call starts a fresh native process and scratch directory.

At 8 offered queries/s, all 64 calls succeeded with 96.71875% recall@10.
Cold start to first HTTP response p90 was 541.10 ms; scheduled-to-response
p90 was 542.36 ms. Completed throughput over the finite run including drain
was 7.597044 queries/s.

This is a development measurement under its recorded source, hardware,
resource limits, and protocol. Repeating rates does not add independent quality queries. Sustained capacity, saturation, lifecycle billing, broader scale,
and a matched vendor comparison are unmeasured in this run.

Read the [decision](../docs/research/performance-architecture-20260930/semantic-1m/fixed48/offered-http/a0002/decision.md),
[root audit](../docs/research/performance-architecture-20260930/semantic-1m/fixed48/offered-http/a0002/root-audit.json),
and [summary](../docs/research/performance-architecture-20260930/semantic-1m/fixed48/offered-http/a0002/screen/summary.json).
Archived research remains tied to its original source and configuration.
The [research page](web/research.html) provides the public evidence entrypoint.
