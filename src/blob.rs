//! Azure Blob Storage stand-in: durable key/value with conditional writes (ETags).
//! Holds packs, compaction leases, and anything written once to a distinct key.

use crate::util::delay;
use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering::Relaxed},
    },
    time::Duration,
};

pub type Bytes = Arc<Vec<u8>>;

#[derive(Clone, Copy, Debug)]
pub struct BlobLatency {
    pub get: Duration,
    pub put: Duration,
}

impl BlobLatency {
    pub const ZERO: Self = Self {
        get: Duration::ZERO,
        put: Duration::ZERO,
    };

    /// Rough object-store numbers: single-digit-ms reads, slower writes.
    pub fn object_store() -> Self {
        Self {
            get: Duration::from_millis(4),
            put: Duration::from_millis(8),
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub enum Cond {
    Always,
    IfAbsent,
    IfMatch(u64),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PreconditionFailed;

#[derive(Default, Debug)]
pub struct BlobStats {
    pub gets: AtomicU64,
    pub puts: AtomicU64,
    pub deletes: AtomicU64,
}

pub struct BlobStore {
    map: Mutex<HashMap<String, (Bytes, u64)>>,
    etag: AtomicU64,
    lat: BlobLatency,
    pub stats: BlobStats,
}

impl BlobStore {
    pub fn new(lat: BlobLatency) -> Arc<Self> {
        Arc::new(Self {
            map: Mutex::default(),
            etag: AtomicU64::new(0),
            lat,
            stats: BlobStats::default(),
        })
    }

    pub async fn get(&self, key: &str) -> Option<(Bytes, u64)> {
        delay(self.lat.get).await;
        self.stats.gets.fetch_add(1, Relaxed);
        self.map.lock().unwrap().get(key).cloned()
    }

    /// Returns the new ETag. The precondition is checked when the write lands,
    /// so concurrent conditional writers to one key race and all but one fail.
    pub async fn put(&self, key: &str, data: Bytes, cond: Cond) -> Result<u64, PreconditionFailed> {
        delay(self.lat.put).await;
        self.stats.puts.fetch_add(1, Relaxed);
        let mut m = self.map.lock().unwrap();
        let cur = m.get(key).map(|e| e.1);
        let ok = match cond {
            Cond::Always => true,
            Cond::IfAbsent => cur.is_none(),
            Cond::IfMatch(e) => cur == Some(e),
        };
        if !ok {
            return Err(PreconditionFailed);
        }
        let tag = self.etag.fetch_add(1, Relaxed) + 1;
        m.insert(key.to_string(), (data, tag));
        Ok(tag)
    }

    pub async fn delete(&self, key: &str) {
        delay(self.lat.put).await;
        self.stats.deletes.fetch_add(1, Relaxed);
        self.map.lock().unwrap().remove(key);
    }

    /// Instant metric, not a simulated request.
    pub fn count(&self, prefix: &str) -> usize {
        self.map
            .lock()
            .unwrap()
            .keys()
            .filter(|k| k.starts_with(prefix))
            .count()
    }
}
