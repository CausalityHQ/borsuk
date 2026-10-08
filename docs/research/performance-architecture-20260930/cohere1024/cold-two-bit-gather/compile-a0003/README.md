# Gather timing method v2 — prospective compile

Candidate 45456b5168043d5405b7b792ed9360be5e657cfc changes only the private test workload from 4 MiB to 256 MiB per cell and refuses CPU samples below 50 ms. Production scoring is byte-identical to preserved 751b5327571c7fec17af4bbdd7f1b1594f0aaf9d. All acceptance ratios and native safety ceilings remain unchanged.

The original primitive-a0001 report remains REJECT, and its outer execution remains INVALID. Saved-record padding correction reproduces REJECT without another native run. Its 2–3 ms CPU cells are unsuitable for the prospective 5% regression decision. This amendment claims no performance improvement.

Compile assets pass bounded archive admission and synthetic Bash/time/tee checks. Native compilation and timing on this revision remain UNRUN. Eight existing serial compile gates must pass before selecting the exact ELF for one separately frozen timing attempt. No production integration or cold benchmark is authorized by this receipt alone.
