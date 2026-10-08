# Four-row native qualification a0002: Spot interruption

Source fd2cb50ee5856a5c43131ca23d38c7be8c8bc73b. The original instance i-072d4039ffff8b196 is terminated; waiter exited 0. AWS Spot request sir-67zzm7ek records instance-terminated-no-capacity at 2026-10-08T15:28:18Z.

All 136 collected artifact hashes match. Twelve recorded stages have native and tee exit 0, including workspace Clippy and actual workspace test compilation. Scalar-control-release was interrupted before its exit record; final source authentication and overall gate exit are absent. The bootstrap exit 0 does not establish native completion.

Disposition: EXECUTION_INVALID_SPOT_INTERRUPTION. No algorithm rejection, complete qualification, production integration, or performance claim. Preserve this attempt and rerun unchanged source and limits in a separately recorded attempt.
