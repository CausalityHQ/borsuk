//! Coordinated background maintenance across multiple index instances.
//!
//! Several processes can open the same object-store index and share the
//! maintenance work (compaction, garbage collection, purge) without stepping on
//! each other. Coordination uses two families of small S3 objects:
//!
//! - **Membership** — each instance heartbeats `maintenance/instances/<id>` with
//!   the current time. The set of instances whose heartbeat is within the lease
//!   TTL is the live membership; that count is how the work is sharded.
//! - **Leases** — a unit of work is claimed by creating `maintenance/leases/<key>`
//!   with a create-if-absent put. An expired lease can be reclaimed. Leases only
//!   avoid *duplicated* work; correctness still rests on the `CURRENT`
//!   compare-and-swap that every publish performs, so a lease race is at worst
//!   wasted effort, never corruption.

use std::time::Duration;

use crate::{error::Result, storage::Storage};

const INSTANCES_PREFIX: &str = "maintenance/instances/";
const LEASES_PREFIX: &str = "maintenance/leases/";

/// Default heartbeat/lease validity window.
pub const DEFAULT_MAINTENANCE_LEASE_TTL: Duration = Duration::from_secs(30);

/// Configuration for coordinated maintenance. Each process must use a distinct
/// `instance_id` (a fresh UUID by default).
#[derive(Debug, Clone)]
pub struct MaintenanceConfig {
    /// Unique id for this process/instance among all instances of the index.
    pub instance_id: String,
    /// How long a heartbeat and a lease stay valid. A crashed instance is
    /// considered dead once its heartbeat is older than this, and its lease can
    /// be reclaimed.
    pub lease_ttl: Duration,
    /// Whether this instance is eligible to run compaction during maintenance.
    pub compaction: bool,
    /// Whether this instance is eligible to run obsolete-object GC.
    pub garbage_collection: bool,
    /// Whether this instance is eligible to run purge of deleted rows.
    pub purge: bool,
}

impl Default for MaintenanceConfig {
    fn default() -> Self {
        Self {
            instance_id: fallback_instance_id(),
            lease_ttl: DEFAULT_MAINTENANCE_LEASE_TTL,
            compaction: true,
            garbage_collection: true,
            purge: false,
        }
    }
}

impl MaintenanceConfig {
    /// Config with an explicit instance id and default lease TTL.
    #[must_use]
    pub fn new(instance_id: impl Into<String>) -> Self {
        Self {
            instance_id: instance_id.into(),
            ..Self::default()
        }
    }
}

/// What a single maintenance pass observed and did on this instance.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct MaintenanceReport {
    /// Number of live instances (including this one) sharing the work.
    pub active_instances: usize,
    /// This instance's rank in the sorted live membership (0-based).
    pub instance_rank: usize,
    /// Whether this instance ran a compaction pass this cycle.
    pub compacted: bool,
    /// Whether this instance ran obsolete-object GC this cycle.
    pub garbage_collected: bool,
    /// Whether this instance ran a purge this cycle.
    pub purged: bool,
    /// Work units this instance skipped because another instance held the lease.
    pub leases_contended: usize,
}

fn instance_path(instance_id: &str) -> String {
    format!("{INSTANCES_PREFIX}{instance_id}")
}

fn lease_path(key: &str) -> String {
    format!("{LEASES_PREFIX}{key}")
}

fn id_from_instance_path(path: &str) -> Option<String> {
    path.strip_prefix(INSTANCES_PREFIX).map(str::to_string)
}

/// Write this instance's heartbeat.
pub(crate) fn heartbeat(storage: &Storage, instance_id: &str, now_ms: i64) -> Result<()> {
    storage.write_bytes(&instance_path(instance_id), now_ms.to_string().as_bytes())
}

/// Live membership: instance ids whose heartbeat is within `ttl_ms`, sorted.
pub(crate) fn active_instances(storage: &Storage, ttl_ms: i64, now_ms: i64) -> Result<Vec<String>> {
    // Collect paths first; reading each object issues its own blocking call, so it
    // must happen outside the listing's async driver to avoid nesting runtimes.
    let mut paths = Vec::new();
    storage.for_each_object(INSTANCES_PREFIX, |object| {
        paths.push(object.path.clone());
        Ok(())
    })?;

    let mut ids = Vec::new();
    for path in paths {
        let Some(id) = id_from_instance_path(&path) else {
            continue;
        };
        if let Some(bytes) = storage.read_object_fresh(&path)?
            && let Ok(timestamp) = String::from_utf8_lossy(&bytes).trim().parse::<i64>()
            && now_ms.saturating_sub(timestamp) <= ttl_ms
        {
            ids.push(id);
        }
    }
    ids.sort();
    ids.dedup();
    Ok(ids)
}

/// This instance's rank and the live instance count, or `None` if it is not a
/// live member (e.g., it has not heartbeat yet).
pub(crate) fn shard_rank(active: &[String], instance_id: &str) -> Option<(usize, usize)> {
    active
        .iter()
        .position(|id| id == instance_id)
        .map(|rank| (rank, active.len()))
}

/// Deterministic sharding: does work `key` belong to this instance's shard?
pub(crate) fn owns_shard(key: &str, rank: usize, count: usize) -> bool {
    if count <= 1 {
        return true;
    }
    (hash_key(key) as usize % count) == rank
}

/// Try to acquire a lease on `key`. Returns `true` if this instance now holds it.
/// Reclaims a lease whose expiry has passed.
pub(crate) fn acquire_lease(
    storage: &Storage,
    key: &str,
    owner: &str,
    ttl_ms: i64,
    now_ms: i64,
) -> Result<bool> {
    let path = lease_path(key);
    let content = format!("{owner}\n{}", now_ms + ttl_ms);
    if storage.try_create_object(&path, content.as_bytes())? {
        return Ok(true);
    }
    // Held by someone — reclaim only if their lease has expired.
    if let Some(bytes) = storage.read_object_fresh(&path)?
        && let Some(expiry) = parse_lease_expiry(&bytes)
        && now_ms > expiry
    {
        storage.delete_object(&path)?;
        return storage.try_create_object(&path, content.as_bytes());
    }
    Ok(false)
}

/// Release a lease this instance holds.
pub(crate) fn release_lease(storage: &Storage, key: &str) -> Result<()> {
    storage.delete_object(&lease_path(key))?;
    Ok(())
}

fn parse_lease_expiry(bytes: &[u8]) -> Option<i64> {
    String::from_utf8_lossy(bytes)
        .lines()
        .nth(1)
        .and_then(|line| line.trim().parse::<i64>().ok())
}

fn hash_key(key: &str) -> u64 {
    let digest = blake3::hash(key.as_bytes());
    let bytes = digest.as_bytes();
    u64::from_le_bytes([
        bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
    ])
}

/// Best-effort unique instance id used when the caller does not supply one.
fn fallback_instance_id() -> String {
    uuid::Uuid::new_v4().to_string()
}

/// Handle to a background maintenance loop. Dropping it, or calling
/// [`MaintenanceHandle::stop`], signals the loop to finish and joins the thread.
pub struct MaintenanceHandle {
    stop: std::sync::Arc<std::sync::atomic::AtomicBool>,
    join: Option<std::thread::JoinHandle<()>>,
}

impl MaintenanceHandle {
    pub(crate) fn new(
        stop: std::sync::Arc<std::sync::atomic::AtomicBool>,
        join: std::thread::JoinHandle<()>,
    ) -> Self {
        Self {
            stop,
            join: Some(join),
        }
    }

    /// Signal the background loop to stop and wait for it to finish.
    pub fn stop(mut self) {
        self.signal_and_join();
    }

    fn signal_and_join(&mut self) {
        self.stop.store(true, std::sync::atomic::Ordering::Relaxed);
        if let Some(join) = self.join.take() {
            let _ = join.join();
        }
    }
}

impl Drop for MaintenanceHandle {
    fn drop(&mut self) {
        self.signal_and_join();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn owns_shard_partitions_keys_across_instances() {
        assert!(owns_shard("compact", 0, 1));
        assert!(owns_shard("gc", 0, 1));
        let count = 3;
        for key in ["compact", "gc", "purge"] {
            let owners = (0..count)
                .filter(|rank| owns_shard(key, *rank, count))
                .count();
            assert_eq!(owners, 1, "key `{key}` must belong to exactly one shard");
        }
    }

    #[test]
    fn shard_rank_locates_member() {
        let active = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        assert_eq!(shard_rank(&active, "b"), Some((1, 3)));
        assert_eq!(shard_rank(&active, "missing"), None);
    }

    #[test]
    fn lease_expiry_reads_the_second_line() {
        assert_eq!(parse_lease_expiry(b"owner\n123456"), Some(123456));
        assert_eq!(parse_lease_expiry(b"owner"), None);
        assert_eq!(parse_lease_expiry(b"owner\nnot-a-number"), None);
    }
}
