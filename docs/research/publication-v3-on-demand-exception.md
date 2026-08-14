# Publication V3 runtime purchase exception

Publication V3 continues to use EC2 Spot by default. On 2026-08-14, the frozen
SIFT qualification runtime could not obtain `c7g.xlarge` Spot capacity in any
of the campaign VPC's three availability zones (`eu-central-1a`, `1b`, or
`1c`). Each launch failed before an instance existed with AWS
`InsufficientInstanceCapacity`; no partial measurement was produced.

The release campaign may therefore use On-Demand for the SIFT runtime only.
Build and staging workers remain Spot. The exception does not change the
instance type, AMI, storage, cgroup limits, protocol, dataset, index settings,
or measurement method. It only changes the EC2 purchase option.

The exception is fail-closed and auditable:

- the paid launch requires the explicit `on-demand` purchase option;
- EC2 instance and volume tags record `PurchaseOption=on-demand`;
- the worker compares the requested option with the IMDS
  `instance-life-cycle` value before executing the benchmark;
- the immutable runtime terminal receipt records the IMDS-observed value; and
- the controller rejects a tag, lifecycle, request, or terminal-receipt
  mismatch.

The authorized invocation is:

```bash
BORSUK_PUBLICATION_V3_PURCHASE_OPTION=on-demand \
  bash scripts/launch_aws_publication_v3.sh --read-recall-sift
```

Omitting the environment variable selects Spot. The controller's equivalent
direct flag is `--purchase-option on-demand`.

Spot results and On-Demand results are separate attempts and are never merged.
The terminal receipt and frozen source archive identify the method used by
every published measurement.

The exception starts from a new source-bound attempt. It never adopts an
instance launched by code predating the `PurchaseOption` tag and receipt
attestation.
