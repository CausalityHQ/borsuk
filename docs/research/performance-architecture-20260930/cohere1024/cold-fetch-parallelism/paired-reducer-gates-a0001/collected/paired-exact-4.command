timeout 120s cargo test --locked -p borsuk --example compare_native_replay tests::paired_v2_rejects_duplicate_truncated_unsealed_and_unpinned_files -- --exact --test-threads=1
