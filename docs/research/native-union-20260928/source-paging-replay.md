# Source paging: REPLAYED geometry

ReLAION FIRST1M fits **128 source GETs / 64 MiB for all 64 observed queries**
with either policy. Neither policy fits all queries at 32 or 64 GETs / 64 MiB,
or at any of 32/64/128 GETs / 32 MiB. Both FIRST100k datasets fit
**32 source GETs / 32 MiB for all 64 queries each**. Fresh CoHere 1M is unknown;
the consumed 100k cohort does not establish its feasibility.

Worst source bytes, MiB (GET caps apply to the entire source fetch):

| Dataset | Policy | 8 | 16 | 32 | 64 | 128 | 256 |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| ReLAION 1M | One-wave page closure | 159.229 | 136.963 | 110.400 | 79.932 | 49.316 | 33.447 |
| ReLAION 1M | Two-wave unit path | 166.821 | 145.361 | 119.879 | 89.624 | 56.134 | 31.635 |
| ReLAION 100k | One-wave page closure | 17.950 | 17.072 | 16.144 | 15.900 | 15.900 | 15.900 |
| ReLAION 100k | Two-wave unit path | 18.536 | 18.097 | 17.316 | 16.437 | 15.692 | 15.558 |
| CoHere 100k | One-wave page closure | 15.070 | 13.263 | 12.433 | 12.433 | 12.433 | 12.433 |
| CoHere 100k | Two-wave unit path | 17.706 | 15.753 | 13.947 | 12.628 | 12.433 | 12.433 |

One-wave closure fetches all 256-row pages touched by the initial sorted walk
union. Every recorded completion unit lies in that closure. Fetching extra
records preserves nomination coverage when the scorer, initial sorted scoring,
completion order, and 2544-unit cap remain unchanged. This replay checks coverage;
it does not read source records or recompute scores or selected-page equality.

Two-wave replay fetches initial 32-row units, then actual recorded completion
units, reusing first-wave bytes. Each cover is optimal under its fixed cap by
retaining the largest byte gaps. Shared-cap allocation uses the recorded path;
it is not predictable before scoring or a global optimum over joint schedules.

Authenticated terminal sidecars, pinned terminal hashes, and terminal-listed
trace/manifest/binding body identities establish three closed traces, each with
exactly 64 consumed external development queries 0–63, D768, 200-byte source
records, and exact partial tails. Source payload actually needed on ReLAION 1M
is 16,281,600 bytes/query; closure and bridged gaps account for the overfetch.
The records object identity is checked against the plane manifest; its absent
body is not fetched or reauthenticated.

The single-query buffer bound is source bytes + 16,773,120 SQ8 bytes. At 128 GETs,
ReLAION 1M bounds are 68,485,120 bytes (one-wave) and 75,633,920 bytes (two-wave).
These exclude other runtime allocations and do not project host RSS.

[JSON](source-paging-replay.json) retains p50/p90/p95/max GETs, bytes, needed
payload, overfetch, buffer bounds, per-query geometry, and threshold pass counts.
This is REPLAYED feasibility, with no live latency, recall, or new quality pass.
No quality-tuned arm was selected.

Reproduce locally: `python3 scripts/check_native_source_paging_replay.py --self-check --output docs/research/native-union-20260928/source-paging-replay.json`.
