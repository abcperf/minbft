//! Defines the collector of messages of type Commit.
//! After a sufficient amount (t + 1) of Commits are received and collected, the respective batch of client-requests is accepted.
//! The Commits must share the same next [crate::View].

use crate::Prepare;
use std::collections::{hash_map::Entry, BTreeMap};

use tracing::debug;
use usig::{Count, Counter};

use crate::{config::Config, peer_message::usig_message::view_peer_message::ViewPeerMessage};

use super::CollectorBools;

/// Collects received Commits.
#[derive(Debug, Clone)]
pub(crate) struct CollectorCommits<P, Sig> {
    /// For each Prepare received, a counter and a vector of bools is created.
    /// If the element i (index) in the vector is set to true, a Commit has been received by the replica with ID = i.
    /// [crate::Prepare]s are seen as Commits, too.
    /// In other words, if i is the ID of the primary, element i (index) in the vector is set to true upon receival of the [crate::Prepare].
    /// The receival of the [crate::Prepare] may be either indirect (through a Commit) or direct (actual [crate::Prepare] broadcast by primary).
    recv_commits: CollectorBools<Count>,
    prepare: BTreeMap<Count, Prepare<P, Sig>>,
    t: u64,
}

/// Defines the key for the collector.
/// The key must be the counter of the [crate::Prepare] to which the Commit belongs to.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct KeyCommits(Count);

impl<P: Clone, Sig: Counter + Clone> CollectorCommits<P, Sig> {
    /// Creates a new collector of Commits.
    pub(crate) fn new(t: u64) -> CollectorCommits<P, Sig> {
        CollectorCommits {
            recv_commits: CollectorBools::new(),
            prepare: BTreeMap::new(),
            t,
        }
    }
    /// Collects a [ViewPeerMessage] (Prepare or Commit) and returns the amount of valid
    /// Commits received for the Prepare to which the received Commit belongs to.
    pub(crate) fn collect(
        &mut self,
        msg: ViewPeerMessage<P, Sig>,
        config: &Config,
    ) -> Vec<Prepare<P, Sig>> {
        match msg {
            ViewPeerMessage::Prepare(prepare) => {
                debug!(
                    "Collecting Prepare (origin: {:?}, view: {:?}, counter: {:?}) ...",
                    prepare.origin,
                    prepare.view,
                    prepare.counter(),
                );

                self.recv_commits
                    .collect(prepare.counter(), prepare.origin, config);
                self.prepare.insert(prepare.counter(), prepare);
            }
            ViewPeerMessage::Commit(commit) => {
                debug!(
                    "Collecting Commit (origin: {:?}, counter: {:?}, Prepare: [origin: {:?}, view: {:?}, counter: {:?}]) ...",
                    commit.origin,
                    commit.counter(),
                    commit.prepare.origin,
                    commit.prepare.view,
                    commit.prepare.counter(),
                );
                self.recv_commits
                    .collect(commit.prepare.counter(), commit.origin, config);
            }
        }

        let mut vec = Vec::new();

        while let Some(entry) = self.prepare.first_entry() {
            let amount = self.recv_commits.0.entry(*entry.key());
            let amount = match amount {
                Entry::Occupied(amount) => amount,
                Entry::Vacant(_) => unreachable!(),
            };
            if amount.get().counter <= self.t {
                break;
            }
            amount.remove();
            vec.push(entry.remove());
        }

        vec
    }
}
