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
/// object-store waits and never determine compute parallelism. The 1 MiB stack
/// is still bounded process-wide, but leaves enough headroom for the nested
/// object-store futures exercised by concurrent open/refresh and WAL
/// coordination; 256 KiB overflowed in that valid workload. A single
/// process-wide pool also prevents callers from multiplying thread counts per
/// index or query.
fn io_pool() -> &'static rayon::ThreadPool {
    static POOL: OnceLock<rayon::ThreadPool> = OnceLock::new();
    POOL.get_or_init(|| {
        rayon::ThreadPoolBuilder::new()
            .num_threads(crate::configured_io_threads())
            .stack_size(1024 * 1024)
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

/// Queue independent query CPU work without waiting for a worker to become
/// available. The fixed process-wide pool remains the concurrency bound.
pub(crate) fn spawn<F>(work: F)
where
    F: FnOnce() + Send + 'static,
{
    query_pool().spawn(work);
}

pub(crate) fn is_query_worker() -> bool {
    query_pool().current_thread_index().is_some()
}

pub(crate) fn install_io<R, F>(work: F) -> R
where
    R: Send,
    F: FnOnce() -> R + Send,
{
    io_pool().install(work)
}

/// Queue one blocking object-store operation without waiting for a worker.
/// The task itself must never wait on another task in this same fixed pool.
pub(crate) fn spawn_io<F>(work: F)
where
    F: FnOnce() + Send + 'static,
{
    io_pool().spawn(work);
}
