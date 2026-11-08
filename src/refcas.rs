//! Ref-CAS: a small replicated log that holds references and the pack manifest.
//!
//! * Each transaction locks only the refs (and pack IDs) it touches, checks every
//!   expected old value, gets a log sequence number (LSN), and is replicated to all
//!   log nodes in parallel. It commits once a majority has recorded it.
//! * The repository version is the highest LSN below which everything is resolved.
//!   Readers send the version they hold; if it's current, the reply is one comparison.
//! * Ref-CAS never stores object data, only pointers to packs already durable in Blob Storage.
//!
//! Simplification: one fixed leader per repository (no leader election).

use crate::{object::Oid, util::delay};
use std::{
    collections::{BTreeMap, HashMap},
    fmt,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering::Relaxed, Ordering::SeqCst},
    },
    time::Duration,
};
use tokio::sync::{Mutex as AsyncMutex, mpsc, watch};

pub type Lsn = u64;

#[derive(Clone, Debug, PartialEq)]
pub enum Op {
    /// Compare-and-swap one reference. `None` means "absent".
    Ref {
        name: String,
        old: Option<Oid>,
        new: Option<Oid>,
    },
    /// List a pack (already in Blob Storage) in the manifest.
    AddPack {
        id: Oid,
        objects: u32,
    },
    RemovePack {
        id: Oid,
    },
}

impl Op {
    fn lock_key(&self) -> String {
        match self {
            Op::Ref { name, .. } => format!("ref:{name}"),
            Op::AddPack { id, .. } | Op::RemovePack { id } => format!("pack:{}", id.hex()),
        }
    }
}

#[derive(Debug)]
pub struct Entry {
    pub lsn: Lsn,
    pub ops: Vec<Op>,
}

#[derive(Clone, Debug, Default)]
pub struct State {
    pub refs: HashMap<String, Oid>,
    /// The pack manifest: pack ID → object count.
    pub packs: HashMap<Oid, u32>,
}

impl State {
    pub fn apply(&mut self, ops: &[Op]) {
        for op in ops {
            match op {
                Op::Ref { name, new: Some(n), .. } => {
                    self.refs.insert(name.clone(), *n);
                }
                Op::Ref { name, new: None, .. } => {
                    self.refs.remove(name);
                }
                Op::AddPack { id, objects } => {
                    self.packs.insert(*id, *objects);
                }
                Op::RemovePack { id } => {
                    self.packs.remove(id);
                }
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum TxnError {
    Conflict {
        name: String,
        expected: Option<Oid>,
        actual: Option<Oid>,
    },
    PackAlreadyListed(Oid),
    PackNotListed(Oid),
    NoQuorum {
        acks: usize,
        needed: usize,
    },
}

impl fmt::Display for TxnError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let show = |o: &Option<Oid>| o.map_or("nothing".to_string(), |o| o.to_string());
        match self {
            TxnError::Conflict { name, expected, actual } => {
                write!(f, "{name} moved: expected {}, found {}", show(expected), show(actual))
            }
            TxnError::PackAlreadyListed(id) => write!(f, "pack {id} already listed"),
            TxnError::PackNotListed(id) => write!(f, "pack {id} not listed"),
            TxnError::NoQuorum { acks, needed } => write!(f, "no quorum ({acks} of {needed} needed acks)"),
        }
    }
}

impl std::error::Error for TxnError {}

pub enum VersionReply {
    /// The caller's version is current: no refs sent.
    Current(Lsn),
    /// Everything committed after the caller's version.
    Delta { lsn: Lsn, entries: Vec<Arc<Entry>> },
    /// Full state, for callers with no version or far behind.
    Snapshot { lsn: Lsn, state: State },
}

/// One replica of the log.
pub struct LogNode {
    pub id: usize,
    up: AtomicBool,
    entries: Mutex<BTreeMap<Lsn, Arc<Entry>>>,
    latency: Duration,
}

impl LogNode {
    /// Network hop plus a durable write.
    async fn append(&self, e: Arc<Entry>) -> bool {
        delay(self.latency).await;
        if !self.up.load(SeqCst) {
            return false;
        }
        self.entries.lock().unwrap().insert(e.lsn, e);
        true
    }

    pub fn is_up(&self) -> bool {
        self.up.load(SeqCst)
    }

    pub fn len(&self) -> usize {
        self.entries.lock().unwrap().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[derive(Clone, Copy, Debug)]
pub struct RefCasConfig {
    pub nodes: usize,
    /// Client ↔ leader round trip.
    pub rtt: Duration,
    /// Leader → log node append (network + fsync).
    pub append: Duration,
}

impl RefCasConfig {
    pub fn lan(nodes: usize) -> Self {
        Self {
            nodes,
            rtt: Duration::from_millis(1),
            append: Duration::from_millis(1),
        }
    }

    pub fn instant(nodes: usize) -> Self {
        Self {
            nodes,
            rtt: Duration::ZERO,
            append: Duration::ZERO,
        }
    }
}

#[derive(Default, Debug)]
pub struct RefCasStats {
    pub commits: AtomicU64,
    pub conflicts: AtomicU64,
    pub no_quorum: AtomicU64,
    pub checks: AtomicU64,
    pub checks_current: AtomicU64,
}

struct Visible {
    /// Every LSN ≤ watermark is resolved (committed or aborted).
    watermark: Lsn,
    pending: BTreeMap<Lsn, Option<Arc<Entry>>>,
    state: State,
    log: Vec<Arc<Entry>>,
}

pub struct RefCas {
    nodes: Vec<Arc<LogNode>>,
    locks: Mutex<HashMap<String, Arc<AsyncMutex<()>>>>,
    /// Leader's view including commits not yet visible; CAS checks run against it.
    tentative: Mutex<State>,
    next_lsn: AtomicU64,
    visible: Mutex<Visible>,
    watermark: watch::Sender<Lsn>,
    cfg: RefCasConfig,
    pub stats: RefCasStats,
}

const SNAPSHOT_AFTER: usize = 2048;

impl RefCas {
    pub fn new(cfg: RefCasConfig) -> Arc<Self> {
        let nodes = (0..cfg.nodes)
            .map(|id| {
                Arc::new(LogNode {
                    id,
                    up: AtomicBool::new(true),
                    entries: Mutex::default(),
                    latency: cfg.append,
                })
            })
            .collect();
        Arc::new(Self {
            nodes,
            locks: Mutex::default(),
            tentative: Mutex::default(),
            next_lsn: AtomicU64::new(0),
            visible: Mutex::new(Visible {
                watermark: 0,
                pending: BTreeMap::new(),
                state: State::default(),
                log: vec![],
            }),
            watermark: watch::channel(0).0,
            cfg,
            stats: RefCasStats::default(),
        })
    }

    pub fn nodes(&self) -> &[Arc<LogNode>] {
        &self.nodes
    }

    pub async fn commit(&self, ops: Vec<Op>) -> Result<Lsn, TxnError> {
        delay(self.cfg.rtt / 2).await;

        // Lock only what this transaction touches, in a fixed order (no deadlocks).
        let mut keys: Vec<String> = ops.iter().map(Op::lock_key).collect();
        keys.sort();
        keys.dedup();
        let mut guards = Vec::with_capacity(keys.len());
        for k in keys {
            let m = self.locks.lock().unwrap().entry(k).or_default().clone();
            guards.push(m.lock_owned().await);
        }

        {
            let t = self.tentative.lock().unwrap();
            for op in &ops {
                let err = match op {
                    Op::Ref { name, old, .. } => {
                        let actual = t.refs.get(name).copied();
                        (actual != *old).then(|| TxnError::Conflict {
                            name: name.clone(),
                            expected: *old,
                            actual,
                        })
                    }
                    Op::AddPack { id, .. } => t.packs.contains_key(id).then_some(TxnError::PackAlreadyListed(*id)),
                    Op::RemovePack { id } => (!t.packs.contains_key(id)).then_some(TxnError::PackNotListed(*id)),
                };
                if let Some(e) = err {
                    self.stats.conflicts.fetch_add(1, Relaxed);
                    return Err(e);
                }
            }
        }

        let lsn = self.next_lsn.fetch_add(1, SeqCst) + 1;
        let entry = Arc::new(Entry { lsn, ops });

        // Replicate to every log node in parallel; commit on a majority.
        let (n, needed) = (self.nodes.len(), self.nodes.len() / 2 + 1);
        let (tx, mut rx) = mpsc::channel(n);
        for node in &self.nodes {
            let (node, e, tx) = (node.clone(), entry.clone(), tx.clone());
            tokio::spawn(async move {
                let ok = node.append(e).await;
                let _ = tx.send(ok).await;
            });
        }
        drop(tx);
        let mut acks = 0;
        while let Some(ok) = rx.recv().await {
            acks += ok as usize;
            if acks >= needed {
                break;
            }
        }

        if acks < needed {
            // Every node has answered. Drop the minority copies and skip this LSN.
            for node in &self.nodes {
                node.entries.lock().unwrap().remove(&lsn);
            }
            self.resolve(lsn, None);
            self.stats.no_quorum.fetch_add(1, Relaxed);
            return Err(TxnError::NoQuorum { acks, needed });
        }

        self.tentative.lock().unwrap().apply(&entry.ops);
        drop(guards);
        self.resolve(lsn, Some(entry));
        self.stats.commits.fetch_add(1, Relaxed);

        // Read-your-writes: return once this LSN is visible to readers.
        let mut rx = self.watermark.subscribe();
        while *rx.borrow_and_update() < lsn {
            if rx.changed().await.is_err() {
                break;
            }
        }
        delay(self.cfg.rtt / 2).await;
        Ok(lsn)
    }

    fn resolve(&self, lsn: Lsn, entry: Option<Arc<Entry>>) {
        let mut guard = self.visible.lock().unwrap();
        let v = &mut *guard;
        v.pending.insert(lsn, entry);
        let start = v.watermark;
        while let Some(e) = v.pending.remove(&(v.watermark + 1)) {
            v.watermark += 1;
            if let Some(e) = e {
                v.state.apply(&e.ops);
                v.log.push(e);
            }
        }
        if v.watermark != start {
            self.watermark.send_replace(v.watermark);
        }
    }

    /// The read path's first step: "I have version `have`. Anything new?"
    pub async fn check(&self, have: Lsn) -> VersionReply {
        delay(self.cfg.rtt).await;
        self.stats.checks.fetch_add(1, Relaxed);
        let v = self.visible.lock().unwrap();
        if have == v.watermark {
            self.stats.checks_current.fetch_add(1, Relaxed);
            return VersionReply::Current(have);
        }
        let start = v.log.partition_point(|e| e.lsn <= have);
        if have == 0 || have > v.watermark || v.log.len() - start > SNAPSHOT_AFTER {
            return VersionReply::Snapshot {
                lsn: v.watermark,
                state: v.state.clone(),
            };
        }
        VersionReply::Delta {
            lsn: v.watermark,
            entries: v.log[start..].to_vec(),
        }
    }

    /// The pack manifest, as compaction reads it.
    pub async fn manifest(&self) -> HashMap<Oid, u32> {
        delay(self.cfg.rtt).await;
        self.visible.lock().unwrap().state.packs.clone()
    }

    pub fn version(&self) -> Lsn {
        self.visible.lock().unwrap().watermark
    }

    /// Instant metric, not a simulated request.
    pub fn manifest_len(&self) -> usize {
        self.visible.lock().unwrap().state.packs.len()
    }

    /// Take a log node down, or bring it back and let it catch up from the leader.
    pub fn set_node_up(&self, i: usize, up: bool) {
        let node = &self.nodes[i];
        if up {
            let log = self.visible.lock().unwrap().log.clone();
            let mut entries = node.entries.lock().unwrap();
            for e in log {
                entries.entry(e.lsn).or_insert(e);
            }
        }
        node.up.store(up, SeqCst);
    }
}
