//! Defines the collector of messages of type Commit.
//! After a sufficient amount (t + 1) of Commits are received and collected, the respective batch of client-requests is accepted.
//! The Commits must share the same next [crate::View].

use std::marker::PhantomData;

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
    phantom_data: PhantomData<(P, Sig)>,
}

/// Defines the key for the collector.
/// The key must be the counter of the [crate::Prepare] to which the Commit belongs to.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct KeyCommits(Count);

impl<P: Clone, Sig: Counter + Clone> CollectorCommits<P, Sig> {
    /// Creates a new collector of Commits.
    pub(crate) fn new() -> CollectorCommits<P, Sig> {
        CollectorCommits {
            recv_commits: CollectorBools::new(),
            phantom_data: PhantomData,
        }
    }
    /// Collects a [ViewPeerMessage] (Prepare or Commit) and returns the amount of valid
    /// Commits received for the Prepare to which the received Commit belongs to.
    pub(crate) fn collect(&mut self, msg: ViewPeerMessage<P, Sig>, config: &Config) -> u64 {
        match msg {
            ViewPeerMessage::Prepare(prepare) => {
                debug!(
                    "Collecting Prepare (origin: {:?}, view: {:?}, counter: {:?}) ...",
                    prepare.origin,
                    prepare.view,
                    prepare.counter(),
                );
                let amount_collected =
                    self.recv_commits
                        .collect(prepare.counter(), prepare.origin, config);
                debug!(
                    "Successfully inserted Prepare (origin: {:?}, view: {:?}, counter: {:?}).",
                    prepare.origin,
                    prepare.view,
                    prepare.counter(),
                );
                amount_collected
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
                self.collect(ViewPeerMessage::Prepare(commit.prepare.clone()), config);
                let amount_collected =
                    self.recv_commits
                        .collect(commit.prepare.counter(), commit.origin, config);
                debug!("Successfully inserted Commit (origin: {:?}, counter: {:?}, Prepare: [origin: {:?}, view: {:?}, counter: {:?}]).",
                    commit.origin,
                    commit.counter(),
                    commit.prepare.origin,
                    commit.prepare.view,
                    commit.prepare.counter(),
                );
                amount_collected
            }
        }
    }

    /// Cleans the collection up by retaining only
    /// entries which have a counter higher than the provided one.
    pub(crate) fn clean_up(&mut self, counter_prepare: Count) {
        debug!("Cleaning up collector of Commits: Removing Commits for Prepares with counter less than or equal to {:?}", counter_prepare);
        self.recv_commits.clean_up(counter_prepare);
    }
}
