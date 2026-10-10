# Dual review 59c4693a73814f3c

Both original reviews completed; their full results and original worker/watch inputs are preserved here. No duplicate review or test was started.

Applied: authenticated CLI2.36.11 distribution copied from committed main's established installer pins, CLI version/model admission; short TimeoutStopSec and stop/drain on every worker exit; separate harness exit; discovery retries for only the exact original instance and known propagation error; termination request retries plus original-instance termination confirmation; launch-epoch-anchored polling cutoff; bounded deletion observations; collect opaque evidence despite unproven deletion and exit INVALID; metadata size admission before downloads plus If-Match; exact instance/prefix/bootstrap-hash receipt binding; single-document terminal admission.

The planned complete setup/transport budget is now enforced at 500 seconds. Compute and ancillary allowances are $0.035 and $0.015; the observer has an independent 1260-second service limit. EC2 API failure can leave termination unproven; that alerts the operator rather than claiming closure or automatically extending/repeating the test.

Collection-only status is intentional. Root acceptance still must authenticate the archive, inspect the nine original exits and intended failure layers, service/harness/bootstrap exits, input pins, resource records and cleanup. No collected receipt alone is validator PASS or production/ANN qualification.

Final bootstrap syntax/lint passed. RunInstances dry-run exited254 with exact DryRunOperation and no paid launch. Existing AMI available; root snapshot is eight GiB minimum, requested root is encrypted twelve-GiB gp3/delete-on-termination. Instance shutdown behavior is terminate. IAM profile exists and subnet/security group share the verified VPC. Actual setup, dependencies, service and cases are unrun before a0001.
