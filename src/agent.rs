//! A simulated agent: makes small commits and turns them into pushes.

use crate::{
    object::{Kind, Object, Oid, Pack},
    replica::RefUpdate,
};
use std::{
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

pub struct Agent {
    pub name: String,
    pub branch: String,
    pub tip: Option<Oid>,
    edits: u64,
}

/// One push: a pack of (commit, tree, blob) plus the ref update.
pub struct Change {
    pub bytes: Arc<Vec<u8>>,
    pub update: RefUpdate,
    pub commit: Oid,
}

impl Agent {
    pub fn new(name: impl Into<String>, branch: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            branch: branch.into(),
            tip: None,
            edits: 0,
        }
    }

    /// A commit with `parent`, moving the agent's branch from `old`.
    pub fn change(&mut self, parent: Option<Oid>, old: Option<Oid>) -> Change {
        self.edits += 1;
        let salt: u64 = rand::random();
        let file = format!("{}.txt", self.name);
        let blob = Object::blob(format!("{} edit #{} ({salt:016x})\n", self.name, self.edits).as_bytes());
        let tree = Object::tree(&[(file.as_str(), blob.id(), Kind::Blob)]);
        let now = SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |d| d.as_secs());
        let commit = Object::commit(
            tree.id(),
            parent.as_slice(),
            &self.name,
            now,
            &format!("edit #{}", self.edits),
        );
        let id = commit.id();
        Change {
            bytes: Arc::new(Pack::encode(&[commit, tree, blob])),
            update: RefUpdate {
                name: self.branch.clone(),
                old,
                new: Some(id),
            },
            commit: id,
        }
    }
}
