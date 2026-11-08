//! Git objects and packs.
//!
//! Objects use Git's real encoding and hashing, so their IDs match `git hash-object`.
//! The pack container is a simplified format (no deltas, no zlib), named by the SHA-1 of its bytes.

use sha1_smol::Sha1;
use std::{collections::HashMap, fmt, sync::Arc};

#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Oid(pub [u8; 20]);

impl Oid {
    pub fn hash_object(kind: Kind, data: &[u8]) -> Oid {
        let mut h = Sha1::new();
        h.update(format!("{} {}\0", kind.name(), data.len()).as_bytes());
        h.update(data);
        Oid(h.digest().bytes())
    }

    pub fn hash_bytes(data: &[u8]) -> Oid {
        Oid(Sha1::from(data).digest().bytes())
    }

    pub fn hex(&self) -> String {
        self.0.iter().map(|b| format!("{b:02x}")).collect()
    }

    pub fn from_hex(s: &str) -> Option<Oid> {
        if s.len() != 40 {
            return None;
        }
        let mut o = [0u8; 20];
        for (i, b) in o.iter_mut().enumerate() {
            *b = u8::from_str_radix(s.get(2 * i..2 * i + 2)?, 16).ok()?;
        }
        Some(Oid(o))
    }

    /// Which compaction shard a pack with this ID belongs to.
    pub fn shard(&self, shards: u8) -> u8 {
        self.0[0] % shards
    }
}

impl fmt::Display for Oid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.hex()[..7])
    }
}

impl fmt::Debug for Oid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Kind {
    Commit,
    Tree,
    Blob,
}

impl Kind {
    pub fn name(self) -> &'static str {
        match self {
            Kind::Commit => "commit",
            Kind::Tree => "tree",
            Kind::Blob => "blob",
        }
    }

    fn code(self) -> u8 {
        match self {
            Kind::Commit => 1,
            Kind::Tree => 2,
            Kind::Blob => 3,
        }
    }

    fn from_code(c: u8) -> Option<Kind> {
        match c {
            1 => Some(Kind::Commit),
            2 => Some(Kind::Tree),
            3 => Some(Kind::Blob),
            _ => None,
        }
    }
}

#[derive(Clone, Debug)]
pub struct Object {
    pub kind: Kind,
    pub data: Arc<[u8]>,
}

impl Object {
    pub fn new(kind: Kind, data: Vec<u8>) -> Self {
        Object {
            kind,
            data: data.into(),
        }
    }

    pub fn id(&self) -> Oid {
        Oid::hash_object(self.kind, &self.data)
    }

    pub fn blob(content: &[u8]) -> Self {
        Self::new(Kind::Blob, content.to_vec())
    }

    pub fn tree(entries: &[(&str, Oid, Kind)]) -> Self {
        let mut es = entries.to_vec();
        es.sort_by(|a, b| a.0.cmp(b.0));
        let mut d = Vec::new();
        for (name, oid, kind) in es {
            let mode: &[u8] = if kind == Kind::Tree { b"40000" } else { b"100644" };
            d.extend_from_slice(mode);
            d.push(b' ');
            d.extend_from_slice(name.as_bytes());
            d.push(0);
            d.extend_from_slice(&oid.0);
        }
        Self::new(Kind::Tree, d)
    }

    pub fn commit(tree: Oid, parents: &[Oid], author: &str, time: u64, msg: &str) -> Self {
        let mut s = format!("tree {}\n", tree.hex());
        for p in parents {
            s += &format!("parent {}\n", p.hex());
        }
        let who = format!("{author} <{author}@agents.local> {time} +0000");
        s += &format!("author {who}\ncommitter {who}\n\n{msg}\n");
        Self::new(Kind::Commit, s.into_bytes())
    }

    /// Objects this one points at: a commit's tree and parents, a tree's entries.
    pub fn links(&self) -> Vec<Oid> {
        match self.kind {
            Kind::Blob => vec![],
            Kind::Tree => {
                let (d, mut i, mut out) = (&self.data[..], 0, vec![]);
                while i < d.len() {
                    let Some(nul) = d[i..].iter().position(|&b| b == 0) else {
                        break;
                    };
                    let s = i + nul + 1;
                    if s + 20 > d.len() {
                        break;
                    }
                    let mut o = [0u8; 20];
                    o.copy_from_slice(&d[s..s + 20]);
                    out.push(Oid(o));
                    i = s + 20;
                }
                out
            }
            Kind::Commit => String::from_utf8_lossy(&self.data)
                .lines()
                .take_while(|l| !l.is_empty())
                .filter_map(|l| l.strip_prefix("tree ").or_else(|| l.strip_prefix("parent ")))
                .filter_map(Oid::from_hex)
                .collect(),
        }
    }
}

/// An indexed pack: every object hashed and addressable by ID.
pub struct Pack {
    pub id: Oid,
    pub objects: HashMap<Oid, Object>,
    pub bytes: usize,
}

const MAGIC: &[u8; 4] = b"RPK1";
/// Same layout, but each object is preceded by its ID: an index shipped with the pack.
const MAGIC_INDEXED: &[u8; 4] = b"RPK2";

impl Pack {
    pub fn encode(objects: &[Object]) -> Vec<u8> {
        Self::write(objects, false)
    }

    /// Encode with object IDs included, so trusted readers can skip hashing.
    pub fn encode_indexed(objects: &[Object]) -> Vec<u8> {
        Self::write(objects, true)
    }

    fn write(objects: &[Object], indexed: bool) -> Vec<u8> {
        let mut out = Vec::with_capacity(8 + objects.iter().map(|o| o.data.len() + 25).sum::<usize>());
        out.extend_from_slice(if indexed { MAGIC_INDEXED } else { MAGIC });
        out.extend_from_slice(&(objects.len() as u32).to_le_bytes());
        for o in objects {
            if indexed {
                out.extend_from_slice(&o.id().0);
            }
            out.push(o.kind.code());
            out.extend_from_slice(&(o.data.len() as u32).to_le_bytes());
            out.extend_from_slice(&o.data);
        }
        out
    }

    /// Indexing: parse the pack and hash every object to learn its name.
    /// In real Git this (plus the connectivity check) is the CPU-heavy part of a push.
    pub fn index(bytes: &[u8]) -> Result<Pack, String> {
        Self::read(bytes, false)
    }

    /// Open a pack from Blob Storage. Packs written by compaction carry their index,
    /// already verified when written, so this skips re-hashing them.
    pub fn open(bytes: &[u8]) -> Result<Pack, String> {
        Self::read(bytes, true)
    }

    fn read(bytes: &[u8], trust_index: bool) -> Result<Pack, String> {
        let indexed = match bytes.get(..4) {
            Some(m) if m == MAGIC => false,
            Some(m) if m == MAGIC_INDEXED => true,
            _ => return Err("not a pack".into()),
        };
        if bytes.len() < 8 {
            return Err("not a pack".into());
        }
        let n = u32::from_le_bytes(bytes[4..8].try_into().unwrap()) as usize;
        let (mut objects, mut i) = (HashMap::with_capacity(n), 8);
        for _ in 0..n {
            let mut stored = None;
            if indexed {
                let raw = bytes.get(i..i + 20).ok_or("truncated pack")?;
                stored = Some(Oid(raw.try_into().unwrap()));
                i += 20;
            }
            if i + 5 > bytes.len() {
                return Err("truncated pack".into());
            }
            let kind = Kind::from_code(bytes[i]).ok_or("bad object type")?;
            let len = u32::from_le_bytes(bytes[i + 1..i + 5].try_into().unwrap()) as usize;
            i += 5;
            let data = bytes.get(i..i + len).ok_or("truncated pack")?;
            i += len;
            let o = Object::new(kind, data.to_vec());
            let id = match stored {
                Some(id) if trust_index => id,
                _ => o.id(),
            };
            objects.insert(id, o);
        }
        Ok(Pack {
            id: Oid::hash_bytes(bytes),
            objects,
            bytes: bytes.len(),
        })
    }

    pub fn len(&self) -> usize {
        self.objects.len()
    }

    pub fn is_empty(&self) -> bool {
        self.objects.is_empty()
    }
}

pub fn pack_key(id: &Oid) -> String {
    format!("packs/pack-{}.pack", id.hex())
}
