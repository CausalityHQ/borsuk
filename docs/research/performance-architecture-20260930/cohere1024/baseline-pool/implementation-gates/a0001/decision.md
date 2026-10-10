# Bounded query blocking pool

Accepted compiler and synthetic correctness only: 42 baseline tests, 80 replay tests, release, locked workspace correctness/suspicious Clippy and actual environment-unset test-build passed. Exact 426 source hashes match before, after and root candidate. Original instance i-0264e85db8db76a91 terminated before collection; swap/OOM zero.

The current-thread query runtime caps blocking threads at admitted fetch width 16 or 32. Scoring, layout and resource caps are unchanged. Next gate: actual current-format 100k/32-query native execution at both widths under unchanged PID128, with pids.events, peak, cleanup and seal evidence. This does not qualify cold latency, recall at scale, competitor superiority or production readiness.
