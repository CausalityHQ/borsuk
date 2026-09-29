# Original first1M attempt a0001: no Spot capacity, no instance

Native local session51408 exited1 at RunInstances with explicit
InsufficientInstanceCapacity in eu-central-1c. Frozen reservation/userdata/source
archive retained; no launch receipt/instance/measurement exists. EC2 active-tag
query independently confirms zero worker instances. This is capacity rejection,
not scientific KILL or interrupted measurement. No results are retried or mixed.

Explicit manual next attempt a0002 uses already registered eu-central-1a
subnet-034528fbd6977848f, same profile/region/Spot instance/image/CPU/memory/time/
pricecaps/source/scorer/control/quality/physical/HTTP protocol. New immutable
reservation/prefix records actual zone/quote/source. No On-Demand exception,
parameter adjustment, new architecture or overlapping paid job. Zone is disclosed
in serving environment and matched arms run together in the same instance.
