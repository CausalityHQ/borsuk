# Research reconciliation — 2026-10-13

Original Fable consultation 895ae749a69143a9 completed once, exit0, 433s; no fallback. Its answer is a proposal, not experimental evidence.

Useful directions: independent byte-weighted download admission; explicit network/CPU/active-query tiers; row-granular SQ8 nomination and coalescing; transport stamps plus TCP/ENA counters before attributing the width128 regression. Complete the already-authored SOURCE cover cost curve before selecting a new cover cap.

Corrections required before adopting the recommendations:

- Unchanged aggregate CPU does not rule out event-loop starvation. The same CPU work can be scheduled differently and produce idle gaps or receive-window stalls. The current counters do not distinguish these cases.
- The near-200ms tail signature is consistent with retransmission or other stalls; it does not prove ENA drops, RTO, persist timers or S3 behavior. No retransmission/allowance counters were captured in the closed experiment. Verify the actual instance bandwidth specification from a primary source before claiming a bandwidth ceiling; burst state was not measured.
- Semaphore release after authentication bounds active download/auth bytes, not all retained completed outcomes. Ordered collection retains verified bodies until planning. Charge completed bodies separately, and ensure a range larger than a byte-permit cap is rejected before I/O or admitted by an explicit policy; never let it wait forever for impossible permits.
- A separate CPU tier can improve event-loop responsiveness even on one core. Multi-core speedup is a different resource intervention and needs its own envelope. Neither benefit is proved yet.
- Router query GETs0 and head/binding28154B do not imply cold metadata staging28KB. The retained router root alone is3041472B; other staged sidecars and startup remain separately measured. The old Fable staging verdict is not fully discharged by these two counters.
- Closed native query records do not include per-range SOURCE timing probes or all per-row two-bit scores. SQ8 diagnostic probes existed in separately traced panels; the full ABBA did not trace SOURCE ranges. Do not promise zero-compute localization or free row-rank replay from artifacts that lack those fields.
- Hedging creates additional physical requests and may leave work running after a winner. It needs a separately admitted request/byte envelope and genuine loser drain/cancellation evidence. Do not relabel it to bypass the existing no-retry experiment protocol.
- BLAKE3 may be a permissible future format change, but changes the trust/format domain and requires new native authentication and lifecycle gates. No hash replacement is selected from this consultation.
- Verify SQ8 authentication granularity before claiming row-granular fetch needs no format change. PageAuthority authenticates complete record pages; SOURCE fixes32 rows, and existing SQ8 manifests use256-row pages. Partial-row requests cannot silently bypass the page digest. Any finer authenticator or changed digest sidecar needs explicit layout/resource/native qualification.

Primary AWS DescribeInstanceTypes was checked after the consultation: c7i.2xlarge has16GiB nominal RAM, baseline3.125Gbps and peak12.5Gbps. These specifications support a bandwidth hypothesis, not a measured shaper event or a hard9QPS ceiling while burst is possible.

Parent disposition: preserve width64 and all negative arms. First obtain the authenticated GET/byte curve, then test one bounded pipeline intervention with actual transport/CPU evidence and unchanged numerical results. Full lifecycle and matched-vendor goals remain open.
