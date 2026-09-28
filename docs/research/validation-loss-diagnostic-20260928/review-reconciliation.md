# Harness review reconciliation

Read-only Opus consultation `254891a6c1f845a7` completed before launch. Findings independently checked against the launcher and historical source.

- Historical host was ARM. Use Amazon Linux ARM64 `ami-03748c04dc81412c6` on c7g.2xlarge Spot; AWS describe-images verified Amazon ownership, available state and arm64. Exact byte gates remain authoritative; matching architecture does not promise reproduction.
- Archive-dependent reservation permitted a changed archive to bypass attempt registration. Use fixed campaign/attempt prefix `research/validation-loss-diagnostic/20260928/a0001`, conditional create before launch, plus active-instance tag and local shared lock. Generated aws-* outputs are excluded from extra untracked archive entries. A registered failure cannot automatically relaunch under a different hash.
- Reduce Spot maximum from $0.45 to $0.30/hour. Extend instance wall to 2700 seconds and controller limit to 3000; this leaves 600 seconds outside compile/replay for installation. Compute estimate ceiling $0.30 excludes EBS/S3 and is not an invoice promise. Observed preflight quote $0.2048/hour at 11:00 UTC; launcher checks again before reservation. Lifecycle closure includes termination confirmation.
- Retain 300-second individual phase limits within the 900-second whole reproduction limit. These are deliberate bounds; any timeout invalidates the cell rather than prompting tuning/replacement.
- Set ARROW_DEFAULT_MEMORY_POOL=system under the 4 GiB process address-space bound. This bound is not an RSS measurement; collect time -v RSS separately.
- Add SHA pins for the two closed score receipts and split the physical gap bonus between candidate pages and pages outside discovery.
- The historical root records content hashes/object references, not local raw/SQ8 filesystem paths. Different worker paths do not by themselves change its identity.
- Shared lock intentionally prevents overlap with the assurance harness. It is not a per-experiment isolation bug.

No new architecture, query source or scoring arm was introduced. Python syntax and rendered shell syntax may be checked locally; executable helper checks and Rust build remain AWS only. No paid compute was launched for the review.
