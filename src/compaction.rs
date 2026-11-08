//! Compaction nodes: merge packs in the background, directly against Blob Storage and
//! Ref-CAS. Never on a virtual replica.
//!
//! Each node leases shards of the pack space (pack ID → shard) through a conditional write
//! in Blob Storage, so only one node compacts a shard at a time. Per shard:
//! collect new packs into an open segment → seal it → merge → verify the output holds
//! exactly the input objects → publish the new pack → retire the inputs → delete them later.
//! Packs merge with packs of a similar size ("tiers"), so big packs aren't rewritten
//! every time a small one arrives.

use crate::{
    blob::{BlobStore, Cond},
    object::{Object, Oid, Pack, pack_key},
    refcas::{Op, RefCas, TxnError},
    util::now_ms,
};
use rand::Rng;
use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering::Relaxed, Ordering::SeqCst},
    },
    time::{Duration, Instant},
};
use tokio::task::JoinHandle;

#[derive(Clone, Debug)]
pub struct CompactionConfig {
    pub shards: u8,
    /// Seal a segment once it holds this many packs of one tier…
    pub seal_count: usize,
    /// …or once its oldest pack has waited this long.
    pub seal_age: Duration,
    /// Packs this big are done; they stop merging.
    pub max_objects: u32,
    pub max_inputs: usize,
    pub lease_ttl: Duration,
    /// Inputs stay in Blob Storage this long after removal, for in-flight readers.
    pub delete_grace: Duration,
    pub idle_tick: Duration,
}

impl Default for CompactionConfig {
    fn default() -> Self {
        Self {
            shards: 16,
            seal_count: 8,
            seal_age: Duration::from_millis(300),
            max_objects: 1_000_000,
            max_inputs: 64,
            lease_ttl: Duration::from_secs(3),
            delete_grace: Duration::from_secs(1),
            idle_tick: Duration::from_millis(25),
        }
    }
}

#[derive(Default, Debug)]
pub struct CompactionStats {
    pub merges: AtomicU64,
    pub packs_in: AtomicU64,
    pub objects_verified: AtomicU64,
    pub verify_failures: AtomicU64,
    pub lease_takeovers: AtomicU64,
}

pub struct Compactor {
    pub id: usize,
    blob: Arc<BlobStore>,
    refcas: Arc<RefCas>,
    cfg: CompactionConfig,
    stats: Arc<CompactionStats>,
    held: AtomicUsize,
    fair_share: usize,
}

pub fn spawn_fleet(
    nodes: usize,
    blob: Arc<BlobStore>,
    refcas: Arc<RefCas>,
    cfg: CompactionConfig,
    stop: Arc<AtomicBool>,
) -> (Arc<CompactionStats>, Vec<JoinHandle<()>>) {
    let stats = Arc::new(CompactionStats::default());
    let fair_share = (cfg.shards as usize).div_ceil(nodes);
    let mut handles = vec![];
    for id in 0..nodes {
        let node = Arc::new(Compactor {
            id,
            blob: blob.clone(),
            refcas: refcas.clone(),
            cfg: cfg.clone(),
            stats: stats.clone(),
            held: AtomicUsize::new(0),
            fair_share,
        });
        for shard in 0..cfg.shards {
            handles.push(tokio::spawn(node.clone().shard_loop(shard, stop.clone())));
        }
    }
    (stats, handles)
}

impl Compactor {
    async fn shard_loop(self: Arc<Self>, shard: u8, stop: Arc<AtomicBool>) {
        let ttl = self.cfg.lease_ttl.as_millis() as u64;
        let (mut held, mut lease_until) = (false, 0u64);
        let mut seen: HashMap<Oid, Instant> = HashMap::new();
        let jitter = Duration::from_millis(rand::thread_rng().gen_range(0..50));
        tokio::time::sleep(jitter).await; // spread the initial lease grab across nodes

        while !stop.load(SeqCst) {
            if !held || now_ms() + ttl / 2 >= lease_until {
                let fresh_ok = self.held.load(SeqCst) < self.fair_share;
                match self.take_lease(shard, fresh_ok).await {
                    Some(until) => {
                        if !held {
                            held = true;
                            self.held.fetch_add(1, SeqCst);
                        }
                        lease_until = until;
                    }
                    None => {
                        if held {
                            held = false;
                            self.held.fetch_sub(1, SeqCst);
                            seen.clear();
                        }
                        tokio::time::sleep(self.cfg.lease_ttl / 3).await;
                        continue;
                    }
                }
            }
            if !self.compact_once(shard, &mut seen, lease_until).await {
                tokio::time::sleep(self.cfg.idle_tick).await;
            }
        }
    }

    /// Acquire or renew the shard's lease. A lease nobody holds yet is only taken while
    /// this node is under its fair share; an expired lease can always be taken over.
    async fn take_lease(&self, shard: u8, fresh_ok: bool) -> Option<u64> {
        let key = format!("leases/shard-{shard:02}");
        let until = now_ms() + self.cfg.lease_ttl.as_millis() as u64;
        let val = Arc::new(format!("{}:{until}", self.id).into_bytes());
        match self.blob.get(&key).await {
            None if fresh_ok => self.blob.put(&key, val, Cond::IfAbsent).await.ok().map(|_| until),
            None => None,
            Some((cur, etag)) => {
                let (owner, expiry) = String::from_utf8_lossy(&cur)
                    .split_once(':')
                    .and_then(|(o, e)| Some((o.parse::<usize>().ok()?, e.parse::<u64>().ok()?)))
                    .unwrap_or((usize::MAX, 0));
                if owner != self.id && expiry > now_ms() {
                    return None;
                }
                if owner != self.id {
                    self.stats.lease_takeovers.fetch_add(1, Relaxed);
                }
                self.blob.put(&key, val, Cond::IfMatch(etag)).await.ok().map(|_| until)
            }
        }
    }

    /// Returns true if it merged something.
    async fn compact_once(&self, shard: u8, seen: &mut HashMap<Oid, Instant>, lease_until: u64) -> bool {
        let now = Instant::now();
        let candidates: Vec<(Oid, u32)> = self
            .refcas
            .manifest()
            .await
            .into_iter()
            .filter(|(id, n)| id.shard(self.cfg.shards) == shard && *n < self.cfg.max_objects)
            .collect();
        let live: HashSet<Oid> = candidates.iter().map(|c| c.0).collect();
        seen.retain(|id, _| live.contains(id));
        for (id, _) in &candidates {
            seen.entry(*id).or_insert(now);
        }

        // Open segments, one per size tier (×4 per tier), smallest first.
        let mut tiers: BTreeMap<u32, Vec<(Oid, u32)>> = BTreeMap::new();
        for c in candidates {
            tiers.entry(c.1.max(1).ilog(4)).or_default().push(c);
        }
        for (_, mut segment) in tiers {
            if segment.len() < 2 {
                continue;
            }
            segment.sort_by_key(|(id, _)| seen[id]);
            let oldest = seen[&segment[0].0];
            if segment.len() < self.cfg.seal_count && now.duration_since(oldest) < self.cfg.seal_age {
                continue;
            }
            // Seal: take the oldest packs, bounded by count and size.
            let (mut inputs, mut total) = (vec![], 0u64);
            for (id, n) in segment {
                if inputs.len() >= self.cfg.max_inputs
                    || (inputs.len() >= 2 && total + n as u64 > self.cfg.max_objects as u64)
                {
                    break;
                }
                inputs.push(id);
                total += n as u64;
            }
            if now_ms() + 1000 > lease_until {
                return false; // not enough lease left to finish safely
            }
            return self.merge(&inputs, seen).await;
        }
        false
    }

    async fn merge(&self, inputs: &[Oid], seen: &mut HashMap<Oid, Instant>) -> bool {
        let mut jobs = tokio::task::JoinSet::new();
        for &id in inputs {
            let blob = self.blob.clone();
            jobs.spawn(async move { blob.get(&pack_key(&id)).await.map(|(b, _)| b) });
        }
        let mut blobs = vec![];
        while let Some(r) = jobs.join_next().await {
            match r {
                Ok(Some(b)) => blobs.push(b),
                _ => return false,
            }
        }

        // CPU-heavy: index the inputs, write one pack, re-index it and verify.
        let merged = tokio::task::spawn_blocking(move || -> Result<(Vec<u8>, Oid, u32), String> {
            let mut all: BTreeMap<Oid, Object> = BTreeMap::new();
            for b in &blobs {
                all.extend(Pack::open(b)?.objects);
            }
            let want: BTreeSet<Oid> = all.keys().copied().collect();
            let bytes = Pack::encode_indexed(&all.into_values().collect::<Vec<_>>());
            let out = Pack::index(&bytes)?; // hash everything again: never trust our own index
            let got: BTreeSet<Oid> = out.objects.keys().copied().collect();
            if got != want {
                return Err("merged pack doesn't hold exactly the input objects".into());
            }
            Ok((bytes, out.id, got.len() as u32))
        })
        .await;
        let Ok(Ok((bytes, id, n))) = merged else {
            self.stats.verify_failures.fetch_add(1, Relaxed);
            return false;
        };
        if inputs.contains(&id) {
            return false;
        }

        let _ = self.blob.put(&pack_key(&id), Arc::new(bytes), Cond::Always).await;
        // Transaction 1 publishes the merged pack; transaction 2 retires the inputs.
        // In between, objects are listed twice, which is harmless.
        match self.refcas.commit(vec![Op::AddPack { id, objects: n }]).await {
            Ok(_) | Err(TxnError::PackAlreadyListed(_)) => {}
            Err(_) => return false,
        }
        let retire = inputs.iter().map(|&id| Op::RemovePack { id }).collect();
        if self.refcas.commit(retire).await.is_err() {
            return false;
        }

        self.stats.merges.fetch_add(1, Relaxed);
        self.stats.packs_in.fetch_add(inputs.len() as u64, Relaxed);
        self.stats.objects_verified.fetch_add(n as u64, Relaxed);
        for id in inputs {
            seen.remove(id);
        }
        let (blob, grace, old) = (self.blob.clone(), self.cfg.delete_grace, inputs.to_vec());
        tokio::spawn(async move {
            tokio::time::sleep(grace).await;
            for id in old {
                blob.delete(&pack_key(&id)).await;
            }
        });
        true
    }
}
