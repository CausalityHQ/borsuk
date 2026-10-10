# Collector smoke a0006: execution INVALID, deadline observation lost

Original source 0297d99d1f2a56bf71a79991a81f0a5e09cf4b2f, instance i-02be02782373b5b29, watcher invocation 6447e9fe787a4fdcad06fc040b9d5667, terminal1/0. Original bootstrap/test1/1 and parent cleanup0. Instance terminated and root volume absent. Original archive 18684 bytes SHA256 f81e5e4b4e2af58f038504b09469d82fb70cb556cf3ef41fa362b8eac95ccabc; opaque length/hash authenticated before root closed metadata inspection.

Eight completed cases include all three positive exits, three metadata refusals, replacement and populated original child. Populated case independently retained actual MainPID0/populated1, collector exit98, failure-cleanup0, after-cleanup MainPID0/inactive, and harness cleanup0. The a0005 cgroup.kill redirection repair is therefore supported by actual remote execution.

Deadline original invocation 33c8bd6bc26e424e809e5c6f43494ac9 actually had MainPID2750/active/running/populated1. Collector returned98 in two seconds, supplied epoch1791642002; failure-cleanup0 and harness cleanup0. However its poll.tmp is empty. Smoke rejects that missing required progress observation, so no successful seal or gate acceptance.

Source cause: shell truncates poll.tmp before invoking manager. At the final loop manager refuses because deadline-now-2 is exhausted, so no systemctl command runs and the last successful live poll is destroyed. This is an evidence-publication defect, not an ANN/resource/recall failure. Minimal repair writes each prospective poll to poll.next and moves it onto poll.tmp only after manager succeeds. Existing deadline, identity checks, cleanup and qualification assertions are unchanged. Partial failed next poll is retained; last successful snapshot survives. No success is inferred from cleanup alone.

Original failure retained. Repair static-only until a new causality EC2 smoke proves all nine cases. No local runtime/native/fixture execution, ANN or performance claim.
