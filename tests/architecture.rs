use rubicon_poc::{
    agent::Agent,
    cluster::{Cluster, ClusterConfig},
    compaction::CompactionConfig,
    object::{Kind, Object, Oid, Pack},
    refcas::TxnError,
    replica::PushError,
};
use std::{collections::HashSet, sync::Arc, time::Duration};

fn cluster() -> Arc<Cluster> {
    Cluster::new(ClusterConfig::instant(3))
}

async fn push(cl: &Cluster, a: &mut Agent, parent: Option<Oid>, old: Option<Oid>) -> Result<Oid, PushError> {
    let c = a.change(parent, old);
    cl.spokes.push(&a.branch, c.bytes, vec![c.update]).await?;
    a.tip = Some(c.commit);
    Ok(c.commit)
}

/// Next commit on the agent's own branch.
async fn advance(cl: &Cluster, a: &mut Agent) -> Result<Oid, PushError> {
    let t = a.tip;
    push(cl, a, t, t).await
}

async fn tip(cl: &Cluster, r: &str) -> Option<Oid> {
    cl.spokes.fetch(r, r, &HashSet::new()).await.unwrap().tip
}

#[test]
fn object_ids_match_git() {
    assert_eq!(
        Object::blob(b"hello\n").id().hex(),
        "ce013625030ba8dba906f756967f9e9ca394464a"
    );
    assert_eq!(Object::tree(&[]).id().hex(), "4b825dc642cb6eb9a060e54bf8d69288fbee4904");
    let tree = Object::tree(&[("hello.txt", Object::blob(b"hello\n").id(), Kind::Blob)]);
    assert_eq!(tree.id().hex(), "aaa96ced2d9a1c8e72c56b253a0e2fe78393feb7");
}

#[test]
fn shipped_index_matches_a_full_reindex() {
    let bytes = Pack::encode_indexed(&[Object::blob(b"a"), Object::blob(b"b")]);
    let ids = |p: Pack| p.objects.keys().copied().collect::<HashSet<_>>();
    assert_eq!(ids(Pack::open(&bytes).unwrap()), ids(Pack::index(&bytes).unwrap()));
}

#[tokio::test]
async fn compare_and_swap_lets_exactly_one_racer_win() {
    let cl = cluster();
    let base = push(&cl, &mut Agent::new("root", "main"), None, None).await.unwrap();
    let mut set = tokio::task::JoinSet::new();
    for i in 0..16 {
        let cl = cl.clone();
        set.spawn(async move {
            let c = Agent::new(format!("racer-{i}"), "main").change(Some(base), Some(base));
            cl.spokes
                .push(&format!("racer-{i}"), c.bytes, vec![c.update])
                .await
                .map(|_| c.commit)
        });
    }
    let mut results = vec![];
    while let Some(r) = set.join_next().await {
        results.push(r.unwrap());
    }
    let winners: Vec<_> = results.iter().filter_map(|r| r.as_ref().ok()).collect();
    assert_eq!(winners.len(), 1);
    assert!(
        results
            .iter()
            .filter(|r| r.is_err())
            .all(|r| matches!(r, Err(PushError::Rejected(TxnError::Conflict { .. }))))
    );
    assert_eq!(tip(&cl, "main").await, Some(*winners[0]));
}

#[tokio::test]
async fn fetch_sees_pushes_made_through_other_replicas() {
    let cl = cluster();
    let mut a = Agent::new("a", "feature");
    for _ in 0..5 {
        let c = advance(&cl, &mut a).await.unwrap();
        for r in cl.spokes.replicas() {
            assert_eq!(r.fetch("feature", &HashSet::new()).await.unwrap().tip, Some(c));
        }
    }
}

#[tokio::test]
async fn unchanged_repo_costs_one_version_check() {
    let cl = cluster();
    push(&cl, &mut Agent::new("root", "main"), None, None).await.unwrap();
    let first = cl.spokes.fetch("ci", "main", &HashSet::new()).await.unwrap();
    let second = cl.spokes.fetch("ci", "main", &HashSet::new()).await.unwrap();
    assert!(!first.was_current);
    assert!(second.was_current);
    assert_eq!(second.objects.len(), 3);
}

#[tokio::test]
async fn push_with_unknown_parent_is_rejected() {
    let cl = cluster();
    let ghost = Object::blob(b"never pushed").id();
    let err = push(&cl, &mut Agent::new("a", "main"), Some(ghost), None)
        .await
        .unwrap_err();
    assert_eq!(err, PushError::MissingObject(ghost));
    assert_eq!(cl.refcas.manifest_len(), 0);
}

#[tokio::test]
async fn ref_cas_needs_a_majority() {
    let cl = cluster();
    let mut a = Agent::new("a", "main");
    cl.refcas.set_node_up(0, false);
    push(&cl, &mut a, None, None).await.expect("2 of 3 is a quorum");
    cl.refcas.set_node_up(1, false);
    let err = advance(&cl, &mut a).await.unwrap_err();
    assert!(matches!(
        err,
        PushError::Rejected(TxnError::NoQuorum { acks: 1, needed: 2 })
    ));
    cl.refcas.set_node_up(0, true);
    cl.refcas.set_node_up(1, true);
    advance(&cl, &mut a).await.unwrap();
    let lens: Vec<_> = cl.refcas.nodes().iter().map(|n| n.len()).collect();
    assert_eq!(
        lens,
        vec![2, 2, 2],
        "recovered nodes catch up; the failed entry is in no log"
    );
}

#[tokio::test]
async fn replicas_are_disposable() {
    let cl = cluster();
    let c = push(&cl, &mut Agent::new("a", "main"), None, None).await.unwrap();
    for r in cl.spokes.replicas().iter().take(2) {
        r.kill().await;
    }
    assert_eq!(tip(&cl, "main").await, Some(c), "the last replica serves");
    let fresh = cl.add_replica();
    assert_eq!(
        fresh.fetch("main", &HashSet::new()).await.unwrap().tip,
        Some(c),
        "a cold replica rebuilds"
    );
}

#[tokio::test]
async fn compaction_shrinks_the_manifest_and_keeps_every_object() {
    let cl = cluster();
    let base = push(&cl, &mut Agent::new("root", "main"), None, None).await.unwrap();
    let mut agents: Vec<Agent> = (0..60)
        .map(|i| Agent::new(format!("a{i}"), format!("agents/{i}")))
        .collect();
    for _ in 0..5 {
        for a in &mut agents {
            let t = a.tip;
            push(&cl, a, t.or(Some(base)), t).await.unwrap();
        }
    }
    let before = cl.refcas.manifest_len();
    let cfg = CompactionConfig {
        seal_age: Duration::from_millis(20),
        delete_grace: Duration::from_millis(50),
        ..Default::default()
    };
    cl.start_compaction(3, cfg);
    tokio::time::sleep(Duration::from_millis(1500)).await;
    cl.stop_compaction().await;
    let after = cl.refcas.manifest_len();
    assert!(after * 5 < before, "{before} → {after}");

    let fresh = cl.add_replica();
    for a in &agents {
        let f = fresh.fetch(&a.branch, &HashSet::new()).await.unwrap();
        assert_eq!(f.tip, a.tip);
        assert_eq!(
            f.objects.len(),
            5 * 3 + 3,
            "5 commits of (commit, tree, blob) + the base"
        );
    }
}
