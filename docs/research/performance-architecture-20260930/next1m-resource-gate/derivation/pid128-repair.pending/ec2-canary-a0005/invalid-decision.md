# Collector smoke a0005: execution INVALID

Original source ce621dcdc5a0df72b630a2abdebf6d6a83d1bc1d, instance i-095f40cd27aaffaa1, watcher invocation 6d7fea923a2b4cf1a346e8f6831f2adc. Watcher terminal ExecMainCode=1/ExecMainStatus=0. Original smoke and bootstrap exit94; parent cleanup0. Instance terminated, root volume vol-0858acb724788a494 absent before collection.

Opaque collected archive: 17378 bytes, SHA256 7ffd43edaf0d93a3eb17f3aa114dcc36178d8ed40817e01fe82a4d0a6c307ec9. Root authenticated this complete terminal archive and inspected only metadata and logs, not candidate fixture/native/data execution.

Seven completed cases: positive0, positive2, positive3, exit-disagreement, wrong-config, truncated-seal, replaced. Populated case actually reached original MainPID0, active/exited, original invocation c37a179130e44ea9ae2d22192646ced2 and populated1. Collector rejected with98 and cleanup1. Its original stderr identifies line33: cgroup.kill cannot overwrite existing file. Deadline case was not run. No successful smoke seal or qualification claimed.

Demonstrated source defect: collector globally enables noclobber, then uses ordinary redirection on the existing cgroup.kill kernel control file. Minimal repair replaces only that redirection with >|, retaining exact identity, validated owned cgroup path and all stop/drain checks. This is a kernel control write, not an evidence overwrite. The strict smoke correctly refused to turn rescue cleanup into success. Parent cleanup and exact-instance termination remained successful.

Failed attempt and receipts are immutable. No ANN/data execution or performance claim. The repaired collector requires a new recorded causality EC2 smoke before the real1M native gate; no automatic retry, no change to algorithm, dataset, recall threshold or native resource envelopes.
