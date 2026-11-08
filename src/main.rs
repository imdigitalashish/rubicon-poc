use clap::{Args, Parser};
use rand::Rng;
use rubicon_poc::{
    agent::Agent,
    cluster::{Cluster, ClusterConfig},
    compaction::{CompactionConfig, CompactionStats},
    object::{Object, Oid},
    refcas::TxnError,
    replica::PushError,
};
use std::{
    collections::{HashMap, HashSet},
    error::Error,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering::Relaxed, Ordering::SeqCst},
    },
    time::{Duration, Instant},
};
use tokio::task::JoinSet;

#[derive(Parser)]
#[command(about = "Proof of concept of GitHub's Rubicon Git storage architecture")]
enum Cli {
    /// Narrated walkthrough of every component
    Demo,
    /// Many agents pushing to one repository while CI reads from it
    Bench(BenchArgs),
}

#[derive(Args, Clone)]
struct BenchArgs {
    /// Concurrent agents, each pushing to its own branch
    #[arg(long, default_value_t = 1000)]
    agents: usize,
    #[arg(long, default_value_t = 10)]
    secs: u64,
    #[arg(long, default_value_t = 6)]
    replicas: usize,
    #[arg(long, default_value_t = 4)]
    compactors: usize,
    /// Concurrent CI readers
    #[arg(long, default_value_t = 200)]
    readers: usize,
    /// Average pause between one agent's pushes, in ms
    #[arg(long, default_value_t = 150)]
    think_ms: u64,
    /// Agents racing to move `main` itself
    #[arg(long, default_value_t = 4)]
    main_writers: usize,
    #[arg(long)]
    no_compaction: bool,
    /// Kill a replica and a Ref-CAS node a third of the way in; restore them at two thirds
    #[arg(long)]
    chaos: bool,
}

type Res<T> = Result<T, Box<dyn Error>>;

#[tokio::main]
async fn main() -> Res<()> {
    match Cli::parse() {
        Cli::Demo => demo().await,
        Cli::Bench(a) => bench(a).await,
    }
}

fn ms(t: Instant) -> f64 {
    t.elapsed().as_secs_f64() * 1000.0
}

fn step(n: u32, s: &str) {
    println!("\n\x1b[1m{n}. {s}\x1b[0m");
}

fn say(s: impl AsRef<str>) {
    println!("   {}", s.as_ref());
}

async fn tip(cl: &Cluster, refname: &str) -> Res<Option<Oid>> {
    Ok(cl.spokes.fetch(refname, refname, &HashSet::new()).await?.tip)
}

async fn demo() -> Res<()> {
    let cl = Cluster::new(ClusterConfig::realistic(3));
    let none = HashSet::new();
    println!("Spokes → 3 virtual replicas │ Blob Storage (4 ms get, 8 ms put) │ Ref-CAS (3-node quorum log)");

    step(
        1,
        "A push: index the pack → upload it to Blob Storage → one Ref-CAS transaction",
    );
    let mut alice = Agent::new("alice", "main");
    let c = alice.change(None, None);
    let t = Instant::now();
    let (lsn, r) = cl.spokes.push("main", c.bytes, vec![c.update]).await?;
    say(format!(
        "main → {} (commit + tree + blob) via replica {r}, LSN {lsn}, {:.1} ms",
        c.commit,
        ms(t)
    ));
    say(format!(
        "Blob Storage holds {} pack; Ref-CAS lists it in the manifest",
        cl.blob.count("packs/")
    ));

    step(2, "A fetch starts with one version check against Ref-CAS");
    for i in 1..=2 {
        let t = Instant::now();
        let f = cl.spokes.fetch("ci", "main", &none).await?;
        let how = if f.was_current {
            "version current → served straight from cache"
        } else {
            "behind → caught up, pulled packs"
        };
        say(format!(
            "fetch #{i} (replica {}): {how}; {} objects, {:.1} ms",
            f.replica,
            f.objects.len(),
            ms(t)
        ));
    }

    step(
        3,
        "Two agents race to move main from the same commit: compare-and-swap lets one win",
    );
    let base = tip(&cl, "main").await?;
    let (mut bob, mut eve) = (Agent::new("bob", "main"), Agent::new("eve", "main"));
    let (cb, ce) = (bob.change(base, base), eve.change(base, base));
    let (rb, re) = tokio::join!(
        cl.spokes.push("bob", cb.bytes, vec![cb.update]),
        cl.spokes.push("eve", ce.bytes, vec![ce.update])
    );
    for (who, r) in [("bob", rb), ("eve", re)] {
        say(match r {
            Ok((lsn, rep)) => format!("{who} (replica {rep}): accepted at LSN {lsn}"),
            Err(e) => format!("{who}: rejected, {e}"),
        });
    }

    step(4, "Agents on their own branches don't contend: 200 pushes at once");
    let base = tip(&cl, "main").await?;
    let t = Instant::now();
    let mut set = JoinSet::new();
    for i in 0..200 {
        let cl = cl.clone();
        set.spawn(async move {
            let mut a = Agent::new(format!("agent-{i}"), format!("agents/{i}"));
            let c = a.change(base, None);
            cl.spokes
                .push(&a.branch, c.bytes, vec![c.update])
                .await
                .map(|_| (a.branch, c.commit))
        });
    }
    let mut branches = vec![];
    while let Some(r) = set.join_next().await {
        branches.extend(r?.ok());
    }
    say(format!(
        "{}/200 accepted in {:.1} ms total; refs lock independently, appends replicate in parallel",
        branches.len(),
        ms(t)
    ));

    step(5, "Virtual replicas are disposable");
    let victim = cl.spokes.route("ci")[0].clone();
    victim.kill().await;
    let f = cl.spokes.fetch("ci", "main", &none).await?;
    say(format!(
        "replica {} lost (and its disk) → Spokes sent the fetch to replica {}",
        victim.id, f.replica
    ));
    victim.start();
    let t = Instant::now();
    victim.fetch("main", &none).await?;
    say(format!(
        "replacement starts cold → rebuilt from a Ref-CAS snapshot + Blob Storage in {:.1} ms ({} packs cached)",
        ms(t),
        victim.cached_packs().await
    ));
    let extra = cl.add_replica();
    say(format!(
        "busy repo? add a replica: replica {} joins, cold, and fills its cache as traffic arrives",
        extra.id
    ));

    step(6, "Ref-CAS is a quorum log: 2 of 3 nodes must record each commit");
    let try_push = |label: &'static str| {
        let cl = cl.clone();
        async move {
            let old = tip(&cl, "main").await.ok().flatten();
            let c = Agent::new("alice", "main").change(old, old);
            match cl.spokes.push("main", c.bytes, vec![c.update]).await {
                Ok((lsn, _)) => say(format!("{label} → push accepted at LSN {lsn}")),
                Err(e) => say(format!("{label} → push rejected: {e}")),
            }
        }
    };
    cl.refcas.set_node_up(2, false);
    try_push("node 2 down").await;
    cl.refcas.set_node_up(1, false);
    try_push("nodes 1 and 2 down").await;
    cl.refcas.set_node_up(1, true);
    cl.refcas.set_node_up(2, true);
    try_push("both back (caught up from the leader)").await;
    let lens: Vec<usize> = cl.refcas.nodes().iter().map(|n| n.len()).collect();
    say(format!("entries per log node: {lens:?}"));

    step(7, "Compaction merges packs in the background, never on a replica");
    let before = cl.refcas.manifest_len();
    let stats = cl.start_compaction(2, CompactionConfig::default());
    let t = Instant::now();
    let mut last = before;
    while t.elapsed() < Duration::from_secs(6) {
        tokio::time::sleep(Duration::from_millis(500)).await;
        let now = cl.refcas.manifest_len();
        say(format!(
            "t={:.1}s  manifest: {now} packs  ({} merges so far)",
            t.elapsed().as_secs_f64(),
            stats.merges.load(Relaxed)
        ));
        if now == last && now < before {
            break;
        }
        last = now;
    }
    cl.stop_compaction().await;
    let mut ok = 0;
    for (branch, commit) in &branches {
        if cl.spokes.fetch(branch, branch, &none).await?.tip == Some(*commit) {
            ok += 1;
        }
    }
    say(format!(
        "{before} → {} packs; {} objects verified identical before swapping; {ok}/{} branches still fetch their commit",
        cl.refcas.manifest_len(),
        stats.objects_verified.load(Relaxed),
        branches.len()
    ));

    step(8, "Objects are real Git objects");
    let id = Object::blob(b"hello\n").id().hex();
    let git = std::process::Command::new("sh")
        .args(["-c", "printf 'hello\\n' | git hash-object --stdin"])
        .output();
    match git {
        Ok(o) if o.status.success() => {
            let g = String::from_utf8_lossy(&o.stdout).trim().to_string();
            say(format!(
                "POC: {id}\n   git: {g}  {}",
                if g == id { "✓ match" } else { "✗ differ" }
            ));
        }
        _ => say(format!("blob \"hello\\n\" → {id}")),
    }
    Ok(())
}

#[derive(Default)]
struct Window {
    push: Vec<u32>,
    fetch: Vec<u32>,
    fetch_current: u64,
}

#[derive(Default)]
struct Metrics {
    win: Mutex<Window>,
    conflicts: AtomicU64,
    push_errors: AtomicU64,
    fetch_errors: AtomicU64,
}

fn pct(v: &mut [u32], p: f64) -> f64 {
    if v.is_empty() {
        return 0.0;
    }
    v.sort_unstable();
    v[((v.len() - 1) as f64 * p) as usize] as f64 / 1000.0
}

fn us(t: Instant) -> u32 {
    t.elapsed().as_micros() as u32
}

async fn think(ms: u64) {
    if ms > 0 {
        let d = rand::thread_rng().gen_range(ms / 2..=ms + ms / 2);
        tokio::time::sleep(Duration::from_millis(d)).await;
    }
}

async fn bench(a: BenchArgs) -> Res<()> {
    let cl = Cluster::new(ClusterConfig::realistic(a.replicas));
    let comp: Option<Arc<CompactionStats>> =
        (!a.no_compaction).then(|| cl.start_compaction(a.compactors, CompactionConfig::default()));
    println!(
        "{} agents (+{} on main) · {} CI readers · {} replicas · 3 Ref-CAS nodes · {} · {}s",
        a.agents,
        a.main_writers,
        a.readers,
        a.replicas,
        if a.no_compaction {
            "NO compaction".to_string()
        } else {
            format!("{} compaction nodes", a.compactors)
        },
        a.secs
    );

    let mut root = Agent::new("root", "main");
    let c = root.change(None, None);
    cl.spokes.push("main", c.bytes, vec![c.update]).await?;
    let base = Some(c.commit);

    let stop = Arc::new(AtomicBool::new(false));
    let m = Arc::new(Metrics::default());
    let mut agents = JoinSet::new();
    let mut others = JoinSet::new();

    for i in 0..a.agents {
        let (cl, m, stop, think_ms) = (cl.clone(), m.clone(), stop.clone(), a.think_ms);
        agents.spawn(async move {
            let mut ag = Agent::new(format!("agent-{i}"), format!("agents/{i}"));
            think(think_ms).await;
            while !stop.load(SeqCst) {
                let c = ag.change(ag.tip.or(base), ag.tip);
                let t = Instant::now();
                match cl.spokes.push(&ag.branch, c.bytes, vec![c.update]).await {
                    Ok(_) => {
                        ag.tip = Some(c.commit);
                        m.win.lock().unwrap().push.push(us(t));
                    }
                    Err(_) => {
                        m.push_errors.fetch_add(1, Relaxed);
                    }
                }
                think(think_ms).await;
            }
            (ag.branch, ag.tip)
        });
    }

    for i in 0..a.main_writers {
        let (cl, m, stop, think_ms) = (cl.clone(), m.clone(), stop.clone(), a.think_ms);
        others.spawn(async move {
            let mut ag = Agent::new(format!("merger-{i}"), "main");
            let mut known = HashSet::new();
            while !stop.load(SeqCst) {
                let Ok(f) = cl.spokes.fetch("main", "main", &known).await else {
                    continue;
                };
                known = f.tip.into_iter().collect();
                let c = ag.change(f.tip, f.tip);
                let t = Instant::now();
                match cl.spokes.push("main", c.bytes, vec![c.update]).await {
                    Ok(_) => m.win.lock().unwrap().push.push(us(t)),
                    Err(PushError::Rejected(TxnError::Conflict { .. })) => {
                        m.conflicts.fetch_add(1, Relaxed);
                        continue; // someone moved main first: refetch and retry
                    }
                    Err(_) => {
                        m.push_errors.fetch_add(1, Relaxed);
                    }
                }
                think(think_ms).await;
            }
        });
    }

    for r in 0..a.readers {
        let (cl, m, stop, n, think_ms) = (cl.clone(), m.clone(), stop.clone(), a.agents, a.think_ms);
        others.spawn(async move {
            let mut haves: HashMap<String, Oid> = HashMap::new();
            think(think_ms).await;
            while !stop.load(SeqCst) {
                let branch = if r % 4 == 0 || n == 0 {
                    "main".to_string()
                } else {
                    format!("agents/{}", rand::thread_rng().gen_range(0..n))
                };
                let have: HashSet<Oid> = haves.get(&branch).copied().into_iter().collect();
                let t = Instant::now();
                match cl.spokes.fetch(&branch, &branch, &have).await {
                    Ok(f) => {
                        let mut w = m.win.lock().unwrap();
                        w.fetch.push(us(t));
                        w.fetch_current += f.was_current as u64;
                        if let Some(tip) = f.tip {
                            haves.insert(branch, tip);
                        }
                    }
                    Err(_) => {
                        m.fetch_errors.fetch_add(1, Relaxed);
                    }
                }
                think(think_ms).await;
            }
        });
    }

    println!(
        "{:>4}  {:>9} {:>8} {:>8}  {:>9} {:>8} {:>8} {:>8}  {:>9} {:>9}",
        "t", "pushes/s", "p50 ms", "p99 ms", "fetches/s", "p50 ms", "p99 ms", "current", "manifest", "rep.cache"
    );
    let (mut all_push, mut all_fetch, mut all_current) = (vec![], vec![], 0u64);
    let start = Instant::now();
    for sec in 1..=a.secs {
        tokio::time::sleep_until((start + Duration::from_secs(sec)).into()).await;
        if a.chaos && sec == a.secs / 3 {
            cl.spokes.replicas()[0].kill().await;
            cl.refcas.set_node_up(2, false);
            println!("      ⚡ killed replica 0 and Ref-CAS node 2");
        }
        if a.chaos && sec == 2 * a.secs / 3 {
            cl.spokes.replicas()[0].start();
            cl.refcas.set_node_up(2, true);
            println!("      ⚡ replica 0 back (cold), Ref-CAS node 2 back (caught up)");
        }
        let mut w = std::mem::take(&mut *m.win.lock().unwrap());
        let mut cached = 0;
        let reps = cl.spokes.replicas();
        for r in &reps {
            cached += r.cached_packs().await;
        }
        let fetches = w.fetch.len().max(1) as f64;
        println!(
            "{:>3}s  {:>9} {:>8.1} {:>8.1}  {:>9} {:>8.1} {:>8.1} {:>7.0}%  {:>9} {:>9}",
            sec,
            w.push.len(),
            pct(&mut w.push, 0.5),
            pct(&mut w.push, 0.99),
            w.fetch.len(),
            pct(&mut w.fetch, 0.5),
            pct(&mut w.fetch, 0.99),
            w.fetch_current as f64 * 100.0 / fetches,
            cl.refcas.manifest_len(),
            cached / reps.len()
        );
        all_push.append(&mut w.push);
        all_fetch.append(&mut w.fetch);
        all_current += w.fetch_current;
    }

    stop.store(true, SeqCst);
    let mut tips = vec![];
    while let Some(r) = agents.join_next().await {
        tips.push(r?);
    }
    while others.join_next().await.is_some() {}

    // Integrity: every agent branch must still resolve to the commit its agent last pushed.
    let none = HashSet::new();
    let (mut ok, mut objects) = (0, 0);
    for (branch, tip) in &tips {
        if let Ok(f) = cl.spokes.fetch(branch, branch, &none).await
            && f.tip == *tip
        {
            ok += 1;
            objects += f.objects.len();
        }
    }

    let secs = a.secs as f64;
    println!("\nsummary");
    println!(
        "  pushes   {:>8}  ({:.0}/s)  p50 {:.1} ms  p99 {:.1} ms  · conflicts on main {} · errors {}",
        all_push.len(),
        all_push.len() as f64 / secs,
        pct(&mut all_push, 0.5),
        pct(&mut all_push, 0.99),
        m.conflicts.load(Relaxed),
        m.push_errors.load(Relaxed)
    );
    println!(
        "  fetches  {:>8}  ({:.0}/s)  p50 {:.1} ms  p99 {:.1} ms  · version already current {:.0}% · errors {}",
        all_fetch.len(),
        all_fetch.len() as f64 / secs,
        pct(&mut all_fetch, 0.5),
        pct(&mut all_fetch, 0.99),
        all_current as f64 * 100.0 / all_fetch.len().max(1) as f64,
        m.fetch_errors.load(Relaxed)
    );
    println!(
        "  ref-cas  version {} · commits {} · per-node log {:?} · failovers {}",
        cl.refcas.version(),
        cl.refcas.stats.commits.load(Relaxed),
        cl.refcas.nodes().iter().map(|n| n.len()).collect::<Vec<_>>(),
        cl.spokes.failovers.load(Relaxed)
    );
    println!(
        "  blob     {} packs stored · {} gets · {} puts · {} deletes",
        cl.blob.count("packs/"),
        cl.blob.stats.gets.load(Relaxed),
        cl.blob.stats.puts.load(Relaxed),
        cl.blob.stats.deletes.load(Relaxed)
    );
    if let Some(s) = &comp {
        println!(
            "  compact  {} merges · {} packs merged · {} objects verified · manifest now {} packs",
            s.merges.load(Relaxed),
            s.packs_in.load(Relaxed),
            s.objects_verified.load(Relaxed),
            cl.refcas.manifest_len()
        );
    }
    println!(
        "  verify   {ok}/{} agent branches fetch their latest commit ({objects} objects walked)",
        tips.len()
    );
    cl.stop_compaction().await;
    Ok(())
}
