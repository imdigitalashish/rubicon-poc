# rubicon-poc

A runnable Rust proof of concept of GitHub's new Git storage architecture ("Project Rubicon"), built from the post [Inside GitHub's new Git storage architecture](https://x.com/github/status/2118830274431873379) by Karthik Raman and Brian Celenza.

It's one process. Blob Storage, the Ref-CAS log nodes and the network are simulated in memory with injected latency. The data model, the protocols and the failure behavior are real code you can run, break and test.

```text
  git client ──► Spokes ──► virtual replicas (disposable compute; disk = cache)
                                │ packs                 │ refs + pack manifest
                                ▼                       ▼
                         Blob Storage  ◄──────  Ref-CAS (quorum log, per-ref CAS)
                                ▲                       ▲
                                └──── compaction nodes ─┘  (background, leased shards)
```

## Run it

```sh
cargo run --release -- demo                     # narrated walkthrough of every component
cargo run --release -- bench                    # 1,000 agents + 200 CI readers for 10 s
cargo run --release -- bench --no-compaction    # watch pack count and latency blow up
cargo run --release -- bench --chaos            # kill a replica and a Ref-CAS node mid-run
cargo run --release -- bench --agents 5000 --think-ms 700 --readers 500
cargo test --release
```

## Article → code

| In the article | Here |
|---|---|
| Git objects are immutable and named by their contents | `object.rs`: real Git encoding; IDs match `git hash-object` (tested) |
| Packs and read-acceleration data live in Azure Blob Storage | `blob.rs`: key/value with ETag conditional writes and simulated latency (4 ms get, 8 ms put) |
| Ref-CAS: a replicated log of refs and the pack manifest; each update is a compare-and-swap committed by a quorum; locks only the refs it touches | `refcas.rs`: per-ref async locks, LSNs, parallel append to 3 log nodes, commit on 2 of 3 |
| Reads start with a version check; if current, the reply carries no refs | `RefCas::check` → `Current` / `Delta` / `Snapshot` |
| Ref-CAS never holds object data; refs only point at durable objects | Replicas upload the pack **before** committing the transaction |
| Virtual replicas: one fleet of disposable compute; local disk is a cache | `replica.rs`: push = index → connectivity check → upload → one transaction; fetch = version check → pull missing packs → serve from cache |
| Spokes routes a repo's traffic to the same replicas when it can, but correctness doesn't depend on it | `spokes.rs`: sticky hash routing with failover |
| Compaction nodes lease shards, collect packs into a segment, seal, merge, verify the same objects, publish, retire the inputs, delete later | `compaction.rs`: Blob-Storage leases, size tiers, two Ref-CAS transactions, delayed delete |

## What a run looks like

These numbers are from a laptop (10 cores) with simulated latency. They show the architecture's *shape*, not GitHub's real performance.

| Run | Pushes/s | Push p50 / p99 | Fetch p50 / p99 | Manifest packs |
|---|---|---|---|---|
| `bench` (1,000 agents) | ~6,000 | 15 / 20 ms | 14 / 22 ms | flat at ~450 |
| `bench --agents 5000 --think-ms 700` | ~7,000 | 15 / 22 ms | 13 / 22 ms | flat at ~700 |
| `bench --no-compaction` | falls to ~1,500 | up to 380 / 870 ms | up to 810 / 1,450 ms | 35,000 and climbing |
| `bench --chaos` | ~6,000 | 15 / 23 ms | 14 / 28 ms | flat; 0 errors, 3,771 failovers |

Each run ends by fetching every agent branch through Spokes and checking that it resolves to the commit its agent last pushed.

Push latency is mostly the simulated Blob Storage upload (8 ms) plus a Ref-CAS quorum round trip. Compaction is what keeps fetches fast: without it, each replica looks objects up across tens of thousands of packs.

## One thing the bench exposes

The `current` column stays near 0%. The repository has a single version (the latest LSN), so while agents push anywhere in the repo, a CI job fetching `main` is almost never "current" and has to pull a delta. Versioning per ref (or per group of refs) would keep the cheap path useful under heavy agent traffic.

## Simplifications

- **One fixed Ref-CAS leader**, with no election or terms. A lost leader isn't handled; lost followers are.
- **A simplified pack format** with no deltas or zlib, named by SHA-1. Merged packs ship their object IDs (like a `.idx`), so replicas don't re-hash them. Client packs are always re-hashed, and compaction re-hashes its own output before publishing it.
- **No garbage collection.** Packs from rejected pushes stay in Blob Storage, and unreachable objects are never removed.
- **Fetches send all objects reachable from the wanted tip**, stopping at the client's `have` commits. Real negotiation is smarter.
- **Replicas pull every new pack on catch-up** rather than only the packs a request needs.
- **One repository per cluster.** The horizontal axis (many repos, tenants, placement) is out of scope.
