# Existing review reconciliation

Native goal restored verbatim from provider handoff. Last delivered commit
29e578ba; unfinished GC sources matched all six original frozen hashes. All five
compressed logs matched terminal hashes/lengths. Original tests: 2 integration,
1 generation, 4 HTTP and 1 ignored native-S3 invocation passed. Original closeout
records i-047034066527b8886 terminated; current EC2 lookup no longer lists it,
and its active-tag filter is empty. No original job or review duplicated.

Existing dual critique 0122d55c38b34216 completed under Opus5.5 and Astra;
full separately labelled results preserved in review.md.

| Finding | Independent trace | Action |
|---|---|---|
| Invalid mutation memory admitted after GET/fence | admit validates snapshot*12+4096 only inside decode; GC only sums declared allowances | Reuse admit before any lock/fence/I/O, and before validation fetch |
| Validation failure holds fence before deletes | keep_set error exits without end fence; zero delete attempts | Release on keep_set failure, return original validation error when release succeeds |
| Ready namespace claim removed by capped sweep | claim sorts before SQ8; ready checks only SQ8 | Validate claim body against immutable job before reusing ready; rebuild missing claim |
| Ambiguous DELETE may arrive after retry/key reuse | Common CAS fences writes, not unconditional DELETE; content-addressed mutation retries/ready reuploads may reuse keys | OPEN production safety gate; no distributed or general crash-safe reclamation claim |

GC currently requires dropping all coordinated handles, same host, same trusted
maintenance directory. Full release is OPEN including delete ambiguity, service
availability/multihost pinning, quality, scale and both matched vendor axes.
No performance/quality evidence is added by correctness workers.

## Next falsifiable production safety gate

Pause an orphan DELETE before storage executes it, return an ambiguous transport
error, and restart GC. After retry releases its fence, retry the failed mutation
batch on the same root/revision; the current deterministic snapshot bytes reuse
the deleted key. Deliver the paused DELETE only after the new snapshot commits.
Recovery must retain that acknowledged mutation, or the current GC protocol fails.
Repeat for a prepared compaction whose SQ8 remains visible during a capped retry.
The common head epoch rejects stale PUT publication but cannot reject DELETE.
Candidate remedy to test after the falsifier: bind newly staged snapshot identities
and recovered compaction attempts to a reclamation epoch so keys scheduled for
deletion never become live again. Direct external object publication must state
and enforce an equivalent ownership rule; no age-based waiting assumption.
No new benchmark arm, infrastructure launch or design win follows from this note.
