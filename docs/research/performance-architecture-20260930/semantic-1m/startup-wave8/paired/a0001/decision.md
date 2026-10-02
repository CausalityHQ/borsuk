# Paired startup wave8: comparison unavailable

Execution closed 0; all 24 terminal artifact bodies authenticate; owned instance i-063250ef31b6a55d4 terminated. Frozen control0 offered 64 requests at 8 QPS with six owners: 62 successful, two capacity drops, no native/transport errors. This fails the all64 gate and invokes the frozen escalation stop. Both candidate cells and final control are unstarted/aborted; preserve scientific FAIL and candidate/control ratio UNMEASURED. No measured wave8 speedup or regression follows.

ReLAION FIRST1M D768 cosine fixed64 reservoir1000–1063 k10: successful-response recall@10 605/620=97.580645%; all-offer recall@10 605/640=94.53125% includes dropped offers. Successful-response cold p90 632.1829914 ms, p95 777.30767775 ms; finite full-span successful completion 7.450790933/s. These success-conditioned tails exclude drops; all-offer p99 is UNBOUNDED. Historical offered-a0002 p90 482.202197 ms is stale context, not a matched control.

Next bounded investigation must resolve client owner-capacity admission at 8 QPS under a new prospective concurrency envelope, preserve end-to-end scheduled tails/drop accounting, and avoid treating this control failure as a candidate architecture KILL. No automatic rerun or post-hoc gate relaxation.
