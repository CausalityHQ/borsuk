# Packing diagnostic: environment failure

| Gate | Observed result | Decision |
| --- | --- | --- |
| Qualified binary and six input bodies | All staged bodies authenticated to frozen byte counts and SHA256 pins | Staging passed |
| Native resource setup | Writing `cpu.max` raised `PermissionError` before the native process started | Execution INVALID |
| Packing locality on 128 plans | Native process did not start; no report or permutation output | Unevaluated; retain candidate |
| Instance cleanup | Original instance `i-045de8b2128c20192` terminated and waited; closeout elapsed 58 seconds | Closed |

Original controller session 62981 exited 1. The remote terminal exited 96 and records `original_exit_code: null`; the native exit receipt records `process_started: false`. The eleven terminal-bound artifact bodies are retained as gzip files and independently authenticated against `aws-terminal.json`. Source commit: `3e9fbf989b119967ec2e4443d96ed9a7ebc27960`.

This failure provides no evidence about recall, latency, throughput, or whether graph packing satisfies the locality gate. Repair and validate cgroup delegation, then rerun the unchanged qualified binary, six input identities, and native limits in a separately recorded attempt. Keep the 32-range and 16 MiB per-plan limits. Do not tune the algorithm in response to this setup failure.

The reservation reports an observed Spot quote of $0.216700/hour, a $0.15 compute cap and a $0.15 ancillary allowance. These are reservation/quote values, not measured total cost.
