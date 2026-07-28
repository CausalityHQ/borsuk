use std::sync::OnceLock;

/// Process-wide worker bound shared by every query on every open handle.
///
/// Reusing these workers avoids the per-query allocator arenas and thread stacks
/// that previously made RSS grow with request count. Four workers also keeps an
/// embedded client from consuming every CPU on larger application hosts.
fn query_pool() -> &'static rayon::ThreadPool {
    static POOL: OnceLock<rayon::ThreadPool> = OnceLock::new();
    POOL.get_or_init(|| {
        rayon::ThreadPoolBuilder::new()
            .num_threads(crate::configured_cpu_threads())
            .thread_name(|index| format!("borsuk-query-{index}"))
            .build()
            .expect("fixed BORSUK query worker pool must initialize")
    })
}

/// Shared blocking-I/O pool. Its workers spend most of their time parked in
/// object-store waits, use a 256 KiB stack, and never determine compute
/// parallelism. A single process-wide pool also prevents callers from
/// multiplying thread counts per index or query.
fn io_pool() -> &'static rayon::ThreadPool {
    static POOL: OnceLock<rayon::ThreadPool> = OnceLock::new();
    POOL.get_or_init(|| {
        rayon::ThreadPoolBuilder::new()
            .num_threads(crate::configured_io_threads())
            .stack_size(256 * 1024)
            .thread_name(|index| format!("borsuk-io-{index}"))
            .build()
            .expect("fixed BORSUK I/O worker pool must initialize")
    })
}

pub(crate) fn install<R, F>(work: F) -> R
where
    R: Send,
    F: FnOnce() -> R + Send,
{
    query_pool().install(work)
}

pub(crate) fn install_io<R, F>(work: F) -> R
where
    R: Send,
    F: FnOnce() -> R + Send,
{
    io_pool().install(work)
}
