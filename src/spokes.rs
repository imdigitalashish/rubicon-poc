//! Spokes: routes a repository's traffic across its group of virtual replicas.
//! The same key goes to the same replica when it can (warm caches), but correctness
//! never depends on it: if a replica is down, the next one serves.

use crate::{
    object::Oid,
    refcas::Lsn,
    replica::{FetchError, FetchReply, PushError, RefUpdate, Replica},
};
use std::{
    collections::HashSet,
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
    sync::{
        Arc, RwLock,
        atomic::{AtomicU64, Ordering::Relaxed},
    },
};

pub struct Spokes {
    replicas: RwLock<Vec<Arc<Replica>>>,
    pub failovers: AtomicU64,
}

impl Spokes {
    pub fn new(replicas: Vec<Arc<Replica>>) -> Self {
        Self {
            replicas: RwLock::new(replicas),
            failovers: AtomicU64::new(0),
        }
    }

    pub fn replicas(&self) -> Vec<Arc<Replica>> {
        self.replicas.read().unwrap().clone()
    }

    pub fn add(&self, r: Arc<Replica>) {
        self.replicas.write().unwrap().push(r);
    }

    /// Replicas in preference order for this key.
    pub fn route(&self, key: &str) -> Vec<Arc<Replica>> {
        let rs = self.replicas.read().unwrap();
        let mut h = DefaultHasher::new();
        key.hash(&mut h);
        let start = h.finish() as usize % rs.len();
        (0..rs.len()).map(|i| rs[(start + i) % rs.len()].clone()).collect()
    }

    pub async fn push(
        &self,
        key: &str,
        bytes: Arc<Vec<u8>>,
        updates: Vec<RefUpdate>,
    ) -> Result<(Lsn, usize), PushError> {
        for r in self.route(key) {
            match r.push(bytes.clone(), updates.clone()).await {
                Err(PushError::Unavailable) => {
                    self.failovers.fetch_add(1, Relaxed);
                }
                res => return res.map(|lsn| (lsn, r.id)),
            }
        }
        Err(PushError::Unavailable)
    }

    pub async fn fetch(&self, key: &str, refname: &str, haves: &HashSet<Oid>) -> Result<FetchReply, FetchError> {
        for r in self.route(key) {
            match r.fetch(refname, haves).await {
                Err(FetchError::Unavailable) => {
                    self.failovers.fetch_add(1, Relaxed);
                }
                res => return res,
            }
        }
        Err(FetchError::Unavailable)
    }
}
