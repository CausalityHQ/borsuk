# Peer transport preparation,2026-09-29

The existing offered-load reducer accepts an explicit connection factory;
its default remains loopback. The peer CLI authenticates config/code/input
hashes, real native authority/reference geometry and exact GT geometry, then
uses that same reducer over an explicit HTTP endpoint. It includes connection
setup in incoming HTTP timing and retains all offers, parity failures,
physical counters, timeouts, drops and scheduled-to-completion tails.

Run from the repository with frozen config and already authenticated inputs:

```sh
PYTHONPATH=. python3 scripts/run_native_peer_offered_http.py CONFIG CONFIG_SHA256 http://PEER_PRIVATE_IP:8080 NEW_OUTPUT
```

Config schema`borsuk-native-peer-offered-http-v1`;64 or936 offers,k10/k100,
8QPS/eight workers,1M D768, immutable authority and named hashed requests,
native reference and exact GT100 files. The deployment controller must bind
endpoint to the actual server instance/private IP and record both machines.
The serving binary must bind its declared peer-facing address.

Seven real-socket protocol cases passed, including explicit connector parity,
64-offer CLI execution and rejection of a tampered input before dispatch.
They use **synthetic IDs/GT**, not an ANN dataset. The recorded local cgroup
counters are shared-host cumulative counters, not client memory/swap measurements.
No fresh Rust full gate: Rust and serving binaries unchanged.

This is runnable client preparation, **not** a cross-host benchmark or vendor
comparison. First freeze one peer protocol/config after prospective authority
closes; launch no second benchmark while the confirmation is active. Declare
metadata resident/no application SQ8 cache/S3 service cache uncontrolled,
plain private HTTP and physical geography. Report namespace-cold startup
separately and measure it explicitly; this client does not include server
hydration/readiness in query timing. Never label the resident-metadata query
measurement as namespace-cold or infer a matched vendor win.
