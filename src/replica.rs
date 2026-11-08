//! Virtual replicas: one fleet of disposable compute serving reads and writes.
//! Their local "disk" is only a cache of packs from Blob Storage; it can be thrown away anytime.

use crate::{
    blob::{BlobStore, Cond},
    object::{Object, Oid, Pack, pack_key},
    refcas::{Lsn, Op, RefCas, TxnError, VersionReply},
};
use std::{
    collections::{HashMap, HashSet},
    fmt,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering::Relaxed, Ordering::SeqCst},
    },
};
use tokio::sync::{Mutex as AsyncMutex, RwLock};

#[derive(Clone, Debug)]
pub struct RefUpdate {
    pub name: String,
    pub old: Option<Oid>,
    pub new: Option<Oid>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum PushError {
    Unavailable,
    BadPack(String),
    MissingObject(Oid),
    Rejected(TxnError),
}

impl fmt::Display for PushError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PushError::Unavailable => write!(f, "no replica available"),
            PushError::BadPack(e) => write!(f, "bad pack: {e}"),
            PushError::MissingObject(o) => write!(f, "missing object {o}"),
            PushError::Rejected(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for PushError {}

#[derive(Debug, Clone, PartialEq)]
pub enum FetchError {
    Unavailable,
    MissingObject(Oid),
}

impl fmt::Display for FetchError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FetchError::Unavailable => write!(f, "no replica available"),
            FetchError::MissingObject(o) => write!(f, "missing object {o}"),
        }
    }
}

impl std::error::Error for FetchError {}

pub struct FetchReply {
    pub tip: Option<Oid>,
    pub objects: Vec<Object>,
    pub version: Lsn,
    /// The version check said "current": no refs or packs were transferred.
    pub was_current: bool,
    pub replica: usize,
}

#[derive(Default, Debug)]
pub struct ReplicaStats {
    pub pushes: AtomicU64,
    pub fetches: AtomicU64,
    pub packs_pulled: AtomicU64,
    /// Per-pack index lookups. Grows with pack count, which is why compaction matters.
    pub pack_probes: AtomicU64,
}

#[derive(Default)]
struct Cache {
    lsn: Lsn,
    refs: HashMap<String, Oid>,
    packs: Vec<Arc<Pack>>,
    ids: HashSet<Oid>,
}

impl Cache {
    /// Like Git, look the object up in each pack's index in turn (newest first).
    fn find(&self, oid: &Oid, probes: &AtomicU64) -> Option<&Object> {
        let mut n = 0;
        let found = self.packs.iter().rev().find_map(|p| {
            n += 1;
            p.objects.get(oid)
        });
        probes.fetch_add(n, Relaxed);
        found
    }

    fn add(&mut self, p: Arc<Pack>) {
        if self.ids.insert(p.id) {
            self.packs.push(p);
        }
    }
}

pub struct Replica {
    pub id: usize,
    up: AtomicBool,
    blob: Arc<BlobStore>,
    refcas: Arc<RefCas>,
    cache: RwLock<Cache>,
    sync_lock: AsyncMutex<()>,
    /// Single-flight bookkeeping: syncs started, and the latest finished one.
    sync_starts: AtomicU64,
    sync_done: AtomicU64,
    sync_done_current: AtomicBool,
    pub stats: ReplicaStats,
}

impl Replica {
    pub fn new(id: usize, blob: Arc<BlobStore>, refcas: Arc<RefCas>) -> Arc<Self> {
        Arc::new(Self {
            id,
            up: AtomicBool::new(true),
            blob,
            refcas,
            cache: RwLock::default(),
            sync_lock: AsyncMutex::new(()),
            sync_starts: AtomicU64::new(0),
            sync_done: AtomicU64::new(0),
            sync_done_current: AtomicBool::new(false),
            stats: ReplicaStats::default(),
        })
    }

    pub fn is_up(&self) -> bool {
        self.up.load(SeqCst)
    }

    /// Lose the node, and its disk with it.
    pub async fn kill(&self) {
        self.up.store(false, SeqCst);
        *self.cache.write().await = Cache::default();
    }

    /// A replacement comes up with a cold cache.
    pub fn start(&self) {
        self.up.store(true, SeqCst);
    }

    pub async fn cached_packs(&self) -> usize {
        self.cache.read().await.packs.len()
    }

    /// Write path: index → connectivity check → upload to Blob Storage → one Ref-CAS transaction.
    pub async fn push(&self, bytes: Arc<Vec<u8>>, updates: Vec<RefUpdate>) -> Result<Lsn, PushError> {
        if !self.is_up() {
            return Err(PushError::Unavailable);
        }
        let pack = Arc::new(Pack::index(&bytes).map_err(PushError::BadPack)?);

        if self.missing(&pack, &updates).await.is_some() {
            // The base may have been pushed through another replica.
            let _one_at_a_time = self.sync_lock.lock().await;
            if self.missing(&pack, &updates).await.is_some() {
                self.sync_locked().await;
            }
        }
        if let Some(oid) = self.missing(&pack, &updates).await {
            return Err(PushError::MissingObject(oid));
        }

        // Content-addressed and immutable: no agreement needed with other writers.
        let _ = self.blob.put(&pack_key(&pack.id), bytes, Cond::Always).await;

        // Refs only ever point at objects that are already durable.
        let mut ops = vec![Op::AddPack {
            id: pack.id,
            objects: pack.len() as u32,
        }];
        ops.extend(updates.into_iter().map(|u| Op::Ref {
            name: u.name,
            old: u.old,
            new: u.new,
        }));
        let lsn = self.refcas.commit(ops).await.map_err(PushError::Rejected)?;

        self.cache.write().await.add(pack);
        self.stats.pushes.fetch_add(1, Relaxed);
        Ok(lsn)
    }

    /// Read path: version check → pull missing packs into the cache → serve from the cache.
    pub async fn fetch(&self, refname: &str, haves: &HashSet<Oid>) -> Result<FetchReply, FetchError> {
        if !self.is_up() {
            return Err(FetchError::Unavailable);
        }
        let was_current = self.sync().await;
        let c = self.cache.read().await;
        if !self.is_up() {
            return Err(FetchError::Unavailable);
        }
        let tip = c.refs.get(refname).copied();
        let (mut objects, mut seen, mut stack) = (Vec::new(), HashSet::new(), tip.into_iter().collect::<Vec<_>>());
        while let Some(oid) = stack.pop() {
            if haves.contains(&oid) || !seen.insert(oid) {
                continue;
            }
            let obj = c
                .find(&oid, &self.stats.pack_probes)
                .ok_or(FetchError::MissingObject(oid))?;
            stack.extend(obj.links());
            objects.push(obj.clone());
        }
        self.stats.fetches.fetch_add(1, Relaxed);
        Ok(FetchReply {
            tip,
            objects,
            version: c.lsn,
            was_current,
            replica: self.id,
        })
    }

    /// Bring the cache up to Ref-CAS's version. Returns true if it already was current.
    /// Concurrent callers share one sync, but only one that started after they arrived,
    /// so a fetch always sees every push that finished before it began.
    async fn sync(&self) -> bool {
        let arrived = self.sync_starts.load(SeqCst);
        let _one_at_a_time = self.sync_lock.lock().await;
        if self.sync_done.load(SeqCst) > arrived {
            return self.sync_done_current.load(SeqCst);
        }
        self.sync_locked().await
    }

    async fn sync_locked(&self) -> bool {
        let id = self.sync_starts.fetch_add(1, SeqCst) + 1;
        let current = self.sync_once().await;
        self.sync_done_current.store(current, SeqCst);
        self.sync_done.store(id, SeqCst);
        current
    }

    async fn sync_once(&self) -> bool {
        for _ in 0..8 {
            let have = self.cache.read().await.lsn;
            let (lsn, all_refs, ref_changes, add, remove) = match self.refcas.check(have).await {
                VersionReply::Current(_) => return true,
                VersionReply::Delta { lsn, entries } => {
                    let (mut refs, mut add, mut remove) = (Vec::new(), HashSet::new(), HashSet::new());
                    for op in entries.iter().flat_map(|e| &e.ops) {
                        match op {
                            Op::Ref { name, new, .. } => refs.push((name.clone(), *new)),
                            Op::AddPack { id, .. } => {
                                remove.remove(id);
                                add.insert(*id);
                            }
                            Op::RemovePack { id } => {
                                add.remove(id);
                                remove.insert(*id);
                            }
                        }
                    }
                    (lsn, None, refs, add, remove)
                }
                VersionReply::Snapshot { lsn, state } => {
                    let c = self.cache.read().await;
                    let add = state.packs.keys().filter(|id| !c.ids.contains(id)).copied().collect();
                    let remove = c
                        .ids
                        .iter()
                        .filter(|id| !state.packs.contains_key(id))
                        .copied()
                        .collect();
                    (lsn, Some(state.refs), vec![], add, remove)
                }
            };

            let need: Vec<Oid> = {
                let c = self.cache.read().await;
                add.iter().filter(|id| !c.ids.contains(id)).copied().collect()
            };
            let mut jobs = tokio::task::JoinSet::new();
            for id in need {
                let blob = self.blob.clone();
                jobs.spawn(async move {
                    let (bytes, _) = blob.get(&pack_key(&id)).await?;
                    Pack::open(&bytes).ok().map(Arc::new)
                });
            }
            let (mut got, mut lost) = (vec![], false);
            while let Some(r) = jobs.join_next().await {
                match r {
                    Ok(Some(p)) => got.push(p),
                    _ => lost = true,
                }
            }
            if lost {
                // Compacted and deleted since; a newer version won't list it.
                continue;
            }

            self.stats.packs_pulled.fetch_add(got.len() as u64, Relaxed);
            let mut c = self.cache.write().await;
            if let Some(refs) = all_refs {
                c.refs = refs;
            }
            for (name, new) in ref_changes {
                match new {
                    Some(o) => c.refs.insert(name, o),
                    None => c.refs.remove(&name),
                };
            }
            c.packs.retain(|p| !remove.contains(&p.id));
            c.ids.retain(|id| !remove.contains(id));
            for p in got {
                c.add(p);
            }
            c.lsn = lsn;
            return false;
        }
        false
    }

    async fn missing(&self, pack: &Pack, updates: &[RefUpdate]) -> Option<Oid> {
        let c = self.cache.read().await;
        let has = |o: &Oid| pack.objects.contains_key(o) || c.find(o, &self.stats.pack_probes).is_some();
        pack.objects
            .values()
            .flat_map(Object::links)
            .chain(updates.iter().filter_map(|u| u.new))
            .find(|o| !has(o))
    }
}
