# Fetched-row canonical-f32 diagnostic

Run the release compare_native_replay example with:
```sh
/mnt/borsuk-workspace/target/release/examples/compare_native_replay --fetched-f32 /mnt/borsuk-workspace/evidence/fetched-f32-diagnostic-source-a0001/config.json CONFIG_SHA256 NEW_REPORT
```
CONFIG_SHA256 for the metadata-only recipe below is b7829df86b4026abaa43a484b37c5b04ce90dec4bab46f3cd29329e996296e7f.
The report path must not exist. Failed admission writes a create-only INVALID
report and exits 2. A successful diagnostic exits 0; it is never qualified as a
performance, vendor, global-recall, or quantization-only win.

Config is strict JSON: schema borsuk-fetched-f32-diagnostic-config-v1;
completed is the unchanged arm0 CompletedConfig from canary-pair.json;
generation, canonical, sq8, requests and truth are path/bytes/sha256 pins;
ordinals is a nonempty strictly increasing list (at most 32); max_workspace_bytes
is at most 536870912. The supplied recipe selects 1,8,14,20,25,30.
Dimensions, rows and k come from authenticated metadata, not dataset constants.

The existing completed reader checks identity, input authority, generation,
geometry, query plan, seal, terminal and EOF before payload access. The root
manifest must hash to the recorded generation. Canonical and SQ8 whole-body
SHA/length/EOF, canonical ID-order SHA, query and truth SHA are verified before
scoring. Descriptor identities are rechecked after processing. Fetched rows
come from the exact recorded logical ranges, including bridges. Production
normalization and returned_sq8::rank_returned_ranges perform native scoring.
Every ordered top-k ID, physical ordinal and f32 score bit must match the seal's
recorded query before canonical scores can enter a successful report.

Canonical records are read at native physical ordinals and IDs must agree.
Canonical values are cosine distances, 1 - dot/(norms), lower is better. Native
SQ8 values are squared-L2 distances, stored norm + query norm squared - 2 *
decoded dot. Margin magnitudes are not interchangeable across these units.
Cosine uses the existing f64 ascending-coordinate accumulator over canonical
f32 unit rows (squared-norm tolerance 1e-6); equal scores sort by ID. Report
contains all true-neighbor fetched ranks, top-k recoveries/losses, cutoff ties,
margins, both top-k lists, and checked workspace admission. Unfetched truth has
null ranks. Canonical f32 is already normalized; this does not recreate original
unnormalized vectors or isolate quantization from normalization/precision/ties.

Memory: one-query SQ8 bytes and two rankings, one canonical row, one query.
No whole raw/canonical corpus hydration or global ID table. Checked arithmetic
admits 64 MiB fixed allowance for bounded reader/JSON/IO/report metadata, three
SQ8 byte capacities, 512 bytes per fetched-row capacity for native kernel
Vec/HashSet/sort and retained rankings, 64 bytes per dimension capacity, and
128 bytes per range capacity. Admission is repeated using actual retained
Vec capacities; the native kernel's private scratch uses this conservative
allowance. Runtime/allocator/OS RSS qualification is an external root gate.

Tests: literal_cosine_nonidentity_ids_ties_and_fetched_recovery;
native_kernel_includes_bridge_and_refuses_replay_discrepancy;
pins_truncation_order_and_occupied_output_refuse.
The existing completed-reader tests retain their seal/EOF/authority coverage.
Only synthetic tests run in the child. Root owns the six-query payload replay.

Exact recipe (metadata only, never executed by child):
```json
{"canonical":{"bytes":4104000000,"path":"/mnt/borsuk-workspace/data/source-frame-serving-real1m-a0001/flat/canonical.bin","sha256":"72d58c8e733415c6354ab7f8f28c5b50a816cf6f6aa7bce62d30c1bd9449c93c"},"completed":{"expected_bound_inputs":{"backend":{"bucket":"borsuk-bench-453182569524-euc1","kind":"s3","physical_prefix":"research/native-library-check/source-capsule-pair-a0001-20261016","region":"eu-central-1","sq8_etag":"\"4875c438eb819e00fc58ac993227fb2b\"","sq8_object_key":"semantic/objects/cc4395af5fdbdeffcb596460aceaad0e55bdcc4af074f405e78ab2dd2be002e1"},"cohort_receipt_sha256":"e0503d41493157a6a0a48627515e62ec452386d0496827028bb9571d406e3a17","config_schema":"borsuk-cohere-native-baseline-config-v19","corpus_intervals":[{"end":100000,"start":0},{"end":1001000,"start":101000}],"corpus_source_first":0,"count":1000,"credential_source":"imds_instance_role_only","dataset":"CohereLabs/wikipedia-2023-11-embed-multilingual-v3","derivation_receipt_sha256":"00e59ee26745c092f1097ba8892b4f205f7c96b48a757311aedc9072fcc3c96e","dimensions":1024,"execution":{"mode":"diagnostic_panel","ordinals":[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31],"trace":true},"fetch_parallelism":64,"generation_prefix":"semantic/framed-source-v1-20261018-a0001/flat/generations/19177ccf2ffc1e04bba366c4957daf46a9b418f93c95374d6c5b8d97027c1203","generation_root_sha256":"19177ccf2ffc1e04bba366c4957daf46a9b418f93c95374d6c5b8d97027c1203","k":10,"max_memory_bytes":2147483648,"metric":"cosine","native_order_sha256":"1060c919e34e93cc9a3d88b93eda226a8635bba41bfc60e5ba47caafb711d413","native_source_sha256":"31f28fa211f9237f0f7e41f569773cd207cefcdce22d4932ec96aee01ff2c40d","native_sq8_sha256":"cc4395af5fdbdeffcb596460aceaad0e55bdcc4af074f405e78ab2dd2be002e1","phase":"bound_inputs","producer_authority":{"executable_sha256":"4f502be7d15a064dba525816c63ad471c3ad2fe616ede14712b67cdb85e3cf66","producer_source_sha256":"a7ba542c3ada061002b09e638a68423236d5499d11aae0e0083cd6e52d38adfd","source_commit":"bc3082a8210c4370ebadae4ba093a7c211072b62","source_order_source_sha256":"1393f99e71be1e68916b598256d1200d56879b439c05d8c66c098345856c9eb4","sq8_source_sha256":"16f03aa7187b4f18676aa511339b07b6e7b8776323710a60071e6ea0d62eb795"},"profile":"scale1m","query_source_first":100000,"range_scheduler_owner":"library","requests_bytes":4096000,"requests_sha256":"8460a81ff2f979deff7d82bede874a1301f47dfd3e4589305c9f53e020920d5e","reserved_queries_sha256":"8460a81ff2f979deff7d82bede874a1301f47dfd3e4589305c9f53e020920d5e","reserved_query_interval":{"end":101000,"start":100000},"revision":"ade45fb52bd549f5e8c065636fe4160a43c2af36","rows":1000000,"s3_read_retry":{"backoff_cap_ms":100,"backoff_ms":100,"connect_timeout_ms":5000,"credentials":"existing_zero_retry_imds","data_methods":["GET","HEAD"],"every_206":"exact_attempted_range_and_length_before_body","mutations":"refused_before_reqwest","physical_submission_bound_scope":"S3_data_only_excludes_zero_retry_IMDS","physical_submission_upper_bound_per_logical_read":2,"policy":"one_retry_100ms","query_get_and_verified_byte_budgets":"unchanged_logical_caps","request_timeout_ms":30000,"sdk_max_retries":1,"sdk_retry_window_is_hard_deadline":false,"sdk_retry_window_ms":1000,"transport_counters":"cumulative_process_submissions_and_consumed_payload_not_wire_or_billed"},"selected_count":32,"semantic_leaf_limit":40,"serving":{"mode":"baseline"},"source_authentication":"offload2","source_byte_budget":67108864,"source_cache":"off","source_capsule_verification_policy":"directory_page_ranges_original_units_v2","source_get_budget":128,"source_layout":{"layout":"flat_records"},"sq8_byte_budget":33554432,"sq8_get_budget":32,"sq8_range_split":null,"tie_rule":"corpus_ordinal_ascending","truth_bytes":80000,"truth_opened":false,"truth_sha256":"0e38d943adaf62f197b98fe8b01b2edec2008010df760717ababfa5054693b7f"},"expected_identity":{"billed_bytes":null,"billed_requests":null,"binary_sha256":"73ab82ac75463f48ef92dc629f99f7f0976f4afd7e10796a2b0c223e679860d9","codec_source_sha256":"19fa10d17f52297a5547c54c4545a12937f1ccfc04219b8fbbfcd9fe7524b1e2","config_schema":"borsuk-cohere-native-baseline-config-v19","config_sha256":"cbd6008b97caba08eec216a446859c23579c7dacad00d6293f2074634b292e91","execution":{"mode":"diagnostic_panel","ordinals":[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31],"trace":true},"external_gate_required":true,"fetch_parallelism":64,"generation_source_sha256":"2ca655bdfd16bb49c0e0161484afe0aee304ed890e8c55ccc3ae22a23478e6f5","io_measurement":"logical_GET_charges_separate_from_cumulative_process_native_transport","max_memory_bytes":2147483648,"native_transport_includes":"S3_and_IMDS_credential_requests_including_PUT","phase":"identity","physical_s3_measured":false,"range_scheduler_owner":"library","returned_source_sha256":"a4dcc7f9bc06cdea835f72860f3d2e796fcb6f29276858c349d624884cc9f74b","router_source_sha256":"a8b6dbc74ea15b08d15499bcf83cab680c17fcc040840e422818b505a8758cd8","runner_source_sha256":"153212f19f606437fbf9271df323e34a0f6bf5bc15788d55fbec8d71b559773a","s3_credential_source":"imds_instance_role_only","s3_read_retry":{"backoff_cap_ms":100,"backoff_ms":100,"connect_timeout_ms":5000,"credentials":"existing_zero_retry_imds","data_methods":["GET","HEAD"],"every_206":"exact_attempted_range_and_length_before_body","mutations":"refused_before_reqwest","physical_submission_bound_scope":"S3_data_only_excludes_zero_retry_IMDS","physical_submission_upper_bound_per_logical_read":2,"policy":"one_retry_100ms","query_get_and_verified_byte_budgets":"unchanged_logical_caps","request_timeout_ms":30000,"sdk_max_retries":1,"sdk_retry_window_is_hard_deadline":false,"sdk_retry_window_ms":1000,"transport_counters":"cumulative_process_submissions_and_consumed_payload_not_wire_or_billed"},"schema":"borsuk-cohere-native-baseline-result-v20","scope":"AUTHENTICATED_NATIVE_QUALITY_CORRECTNESS","semantic_leaf_limit":40,"serving":{"mode":"baseline"},"source_authentication":"offload2","source_byte_budget":67108864,"source_capsule_source_sha256":"bac906dd4020caf8e6a092389d3f16bc370d12eadfe7c37b50b982933a88efa0","source_capsule_verification_policy":"directory_page_ranges_original_units_v2","source_frame_source_sha256":"3391ae0cc8729358910f123ce7919e4161a3c3f513dfce8f764c59d42f1b484e","source_get_budget":128,"source_layout":{"layout":"flat_records"},"source_plane_source_sha256":"dbcc4cdbc4bb5c244354b375afd657f8b5cb1892df3f0ff0b8f41ef580de42e0","sq8_byte_budget":33554432,"sq8_get_budget":32,"sq8_range_source_sha256":"ec2e516478c7868edac0d4417e10283574cae83f221384b6208879126d432ed9","sq8_range_split":null,"truth_opened":false,"unread_bytes":null,"wire_bytes":null},"input":{"bytes":1789687,"path":"/mnt/borsuk-workspace/evidence/source-frame-screen-a0002/panel/flat-smoke.jsonl","sha256":"7750328732fb5282d38602af2c53ad635574f2c6f724c2bd87dcd141aff2b622"},"schema":"borsuk-completed-native-reduction-config-v4"},"generation":{"bytes":45350,"path":"/mnt/borsuk-workspace/data/source-frame-serving-real1m-a0001/flat/manifest.json","sha256":"19177ccf2ffc1e04bba366c4957daf46a9b418f93c95374d6c5b8d97027c1203"},"max_workspace_bytes":536870912,"ordinals":[1,8,14,20,25,30],"requests":{"bytes":4096000,"path":"/mnt/borsuk-sq8-shortlist/inputs/queries.f32","sha256":"8460a81ff2f979deff7d82bede874a1301f47dfd3e4589305c9f53e020920d5e"},"schema":"borsuk-fetched-f32-diagnostic-config-v1","sq8":{"bytes":1036000000,"path":"/mnt/borsuk-sq8-shortlist/store/semantic/objects/cc4395af5fdbdeffcb596460aceaad0e55bdcc4af074f405e78ab2dd2be002e1","sha256":"cc4395af5fdbdeffcb596460aceaad0e55bdcc4af074f405e78ab2dd2be002e1"},"truth":{"bytes":80000,"path":"/mnt/borsuk-sq8-shortlist/inputs/truth.u64","sha256":"0e38d943adaf62f197b98fe8b01b2edec2008010df760717ababfa5054693b7f"}}
```
