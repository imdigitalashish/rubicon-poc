//! Wires the pieces together for one repository.

use crate::{
    blob::{BlobLatency, BlobStore},
    compaction::{self, CompactionConfig, CompactionStats},
    refcas::{RefCas, RefCasConfig},
    replica::Replica,
    spokes::Spokes,
};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering::SeqCst},
};
use tokio::task::JoinHandle;

#[derive(Clone, Copy, Debug)]
pub struct ClusterConfig {
    pub replicas: usize,
    pub blob: BlobLatency,
    pub refcas: RefCasConfig,
}

impl ClusterConfig {
    /// Simulated object-store and in-region network latency.
    pub fn realistic(replicas: usize) -> Self {
        Self {
            replicas,
            blob: BlobLatency::object_store(),
            refcas: RefCasConfig::lan(3),
        }
    }

    /// No simulated latency, for tests.
    pub fn instant(replicas: usize) -> Self {
        Self {
            replicas,
            blob: BlobLatency::ZERO,
            refcas: RefCasConfig::instant(3),
        }
    }
}

struct Fleet {
    stop: Arc<AtomicBool>,
    handles: Vec<JoinHandle<()>>,
}

pub struct Cluster {
    pub blob: Arc<BlobStore>,
    pub refcas: Arc<RefCas>,
    pub spokes: Spokes,
    compaction: Mutex<Option<Fleet>>,
}

impl Cluster {
    pub fn new(cfg: ClusterConfig) -> Arc<Self> {
        let blob = BlobStore::new(cfg.blob);
        let refcas = RefCas::new(cfg.refcas);
        let replicas = (0..cfg.replicas)
            .map(|i| Replica::new(i, blob.clone(), refcas.clone()))
            .collect();
        Arc::new(Self {
            blob,
            refcas,
            spokes: Spokes::new(replicas),
            compaction: Mutex::default(),
        })
    }

    /// When a repository gets busy, add replicas. They start cold.
    pub fn add_replica(&self) -> Arc<Replica> {
        let r = Replica::new(self.spokes.replicas().len(), self.blob.clone(), self.refcas.clone());
        self.spokes.add(r.clone());
        r
    }

    pub fn start_compaction(&self, nodes: usize, cfg: CompactionConfig) -> Arc<CompactionStats> {
        let stop = Arc::new(AtomicBool::new(false));
        let (stats, handles) =
            compaction::spawn_fleet(nodes, self.blob.clone(), self.refcas.clone(), cfg, stop.clone());
        *self.compaction.lock().unwrap() = Some(Fleet { stop, handles });
        stats
    }

    pub async fn stop_compaction(&self) {
        let fleet = self.compaction.lock().unwrap().take();
        if let Some(f) = fleet {
            f.stop.store(true, SeqCst);
            for h in f.handles {
                let _ = h.await;
            }
        }
    }
}
