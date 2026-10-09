# Source-bound build allocation reconciliation

Source: a02c233daca9f2adc1b922853c1f22ef3d5b7699; qualified native source unchanged. Geometry N=1,000,000, D=1024, semantic Scale1m, supplied order.

The audit's B3 claim that centroid buffers are omitted needs correction. semantic_unit_router::admit explicitly includes b=64,000,032 bytes for the retained centroid blob in each training/emit/validate alternative. The generator constructs that blob once and passes it by borrow into router construction. It is not an additional simultaneous 64 MB allocation outside that model.

Canonical construction is sequential before centroid construction. Its explicit heap buffers are raw row 4,096 + decoded values 4,096 + SQ8 row 1,036 + reader 65,536 + writer 65,536 = 140,300 bytes. They drop when write_canonical_source returns. Retained order contributes 8,000,000 bytes. The build's dimension/fixed allowance alone is 128*1024+262144=393216 bytes; it exceeds these canonical buffers. This comparison does not prove allocator/RSS overhead bounds.

Centroid construction explicitly holds output 64,000,032 + SQ8 reader 65,536 + row 1,036 + sums 8,192 = 64,074,796 bytes, plus retained order 8,000,000 and configuration. It precedes router allocation; the row/sums/reader drop before the router build. Router estimate 506,729,928 includes the retained blob. Build base 8,783,456 includes order, page buffer, digests and fixed/dimension allowances; total modeled admission 515,513,384 bytes. The CLI separately charges two 8,000,000-byte buffers while parsing order; the raw byte buffer drops on return, leaving one order vector.

Conclusion: no demonstrated missing large centroid/canonical allocation justifies a Rust change here. Actual 1M process RSS, allocator overhead and duration remain unmeasured; qualify under the independent 8 GiB no-swap host ceiling. Keep the proposed 1 GiB native build model cap distinct from the host ceiling and the fixed 512 MiB serving library cap.

Disk envelope remains pending. Increase the audit's non-data reserve from 1 GiB to the previously used 1.5 GiB before freezing; provisional total becomes 27,581,982,055 bytes. This is an accounting proposal, not measurement, and must include transport/setup evidence explicitly.
