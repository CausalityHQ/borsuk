# V225 real S3 graph generation switch

**Decision:** Can a changed graph generation advance the versioned S3 head
with compare-and-swap while an existing query reader keeps its old graph,
and can a stale writer be rejected without moving that head?

- Freeze one source commit and SHA-256 source archive. A fresh S3 attempt
  prefix contains two independently built, valid four-row cosine graph
  generations. Generation 7 maps its first vector to ID 42. Generation 8
  changes that vector and maps it to ID 99. A third valid generation 9 is
  used only as a stale-writer probe.
- One `causality` c7i.4xlarge Spot instance in eu-central-1c. The worker
  builds the Rust example, publishes generation 7, reads and hydrates it,
  pins one reader, publishes generation 8 with generation 7's version token,
  reads and hydrates it, and switches the resident slot. It then attempts
  to publish generation 9 with the stale generation 7 token.
- Pass only if the held reader still returns ID 42 and generation 7, the
  current reader returns ID 99 and generation 8 without ID 42, and S3
  rejects the stale update with a precondition error while its head remains
  at generation 8. Each cold generation must hydrate five blob objects.
  Record wall time, peak RSS, original terminal identity and all artifact
  hashes. This is a transactional correctness probe, not returned recall,
  100k throughput or service latency evidence.
- Monitor Spot interruption; discard the attempt and restart under a new
  prefix if interrupted. Sync terminal artifacts, independently read them
  back, and terminate the instance at the terminal marker.

On a pass, proceed to a 100k mutation and compaction falsifier with a
bounded delta policy and matched quality/resource checks. The live switch
must budget overlapping generations; this probe does not freeze a RAM cap.
