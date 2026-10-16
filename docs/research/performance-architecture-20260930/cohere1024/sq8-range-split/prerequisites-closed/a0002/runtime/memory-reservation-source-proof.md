# Prospective split-arm memory reservation equality

Status: SOURCE_CHECKED; not a native RSS measurement or experiment admission.
Source: 8e7076951936b0434b7947afd5b23e192c98fe70, crates/borsuk/src/sq8_s3_range.rs, Sq8RangeSplit::scratch_bytes, lines 350–378.

Both arms use max_bytes=33554432, max_physical_gets=46 and parallel=64. Their physical range caps are B=33554432 and C=2121728 bytes.

The reservation is max_bytes + common descriptor bytes + 2*min(min(range_cap,max_bytes)*min(46,64),max_bytes) + common pending-future bytes.
For C, 2121728*46=97599488 exceeds 33554432; B also saturates this minimum. Therefore both reserve the same 67108864-byte transport term and identical total modeled scratch. This equality does not depend on target-specific descriptor sizes because those terms are common.

This is a source-model equality only. Record actual stage RSS, cgroup limits, native admission, successful drain and terminal status independently. It establishes neither a performance result nor lower candidate RSS.
