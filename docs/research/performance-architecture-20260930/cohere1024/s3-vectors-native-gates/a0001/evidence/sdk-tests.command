env CARGO_BUILD_JOBS=1 cargo test --locked -p borsuk --features s3-vectors-bench --bin check_s3_vectors_baseline -- --test-threads=1
