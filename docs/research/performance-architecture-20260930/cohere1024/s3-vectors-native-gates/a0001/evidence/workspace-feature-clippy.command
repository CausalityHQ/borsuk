env CARGO_BUILD_JOBS=1 cargo clippy --locked --workspace --all-targets --features borsuk/s3-vectors-bench -- -D clippy::correctness -D clippy::suspicious
