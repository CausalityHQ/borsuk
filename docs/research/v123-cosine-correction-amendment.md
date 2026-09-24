# V123 cosine correction before second attempt

V123 a0001 has terminal SHA-256
`0d29ade93b24a3afbd36114b554c6e3151476c741c09b7fd1ccd6d302e368e6c`.
Its source scorer used a dot product after rounding normalized coordinates to
FP16. Rounding changes a vector's norm, so that implementation did not match
the preregistered cosine score. Its 104-position FP16-versus-float32 loss is
valid for the **unnormalized dot-product scorer only**. It cannot decide the
FP16 cosine fidelity gate. The sealed a0001 artifacts remain immutable.

Before a0002, change both float32 and FP16 branches to compute
`dot(query, decoded_vector) / norm(decoded_vector)`, with a shared positive
query norm omitted because it does not affect ranking. Reject zero or
nonfinite decoded norms. Keep the sealed V121 requests, ranges, GT, source,
layout, fixed K ladder, parity checks, shared-nominee hybrid control and
original ≥99% / p05≥90 / ≤50-hit precision-gap decision rule. A0002 is still
a postmortem on the used test-first-1000 cohort. A pass permits only a new
held-out deep-image and matched ReLAION method test, plus live latency and
resource gates; it does not qualify the product.

No V123 a0002 result was inspected when this amendment was written.
