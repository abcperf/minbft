//! Defines the collector of messages of type Commit.
//! After a sufficient amount (t + 1) of Commits are received and collected, the respective batch of client-requests is accepted.
//! The Commits must share the same next [crate::View].

use crate::Prepare;
use std::collections::{BTreeMap, HashMap, HashSet};

use tracing::trace;
use usig::{Count, Counter, ReplicaId};

use crate::peer_message::usig_message::view_peer_message::ViewPeerMessage;

/// Collects received Commits.
#[derive(Debug, Clone)]
pub(crate) struct CollectorCommits<P, Sig> {
    /// For each Prepare received, a counter and a vector of bools is created.
    /// If the element i (index) in the vector is set to true, a Commit has been received by the replica with ID = i.
    /// [crate::Prepare]s are seen as Commits, too.
    /// In other words, if i is the ID of the primary, element i (index) in the vector is set to true upon receival of the [crate::Prepare].
    /// The receival of the [crate::Prepare] may be either indirect (through a Commit) or direct (actual [crate::Prepare] broadcast by primary).
    recv_commits: HashMap<Count, HashSet<ReplicaId>>,
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
            recv_commits: HashMap::new(),
            prepare: BTreeMap::new(),
            t,
        }
    }
    /// Collects a [ViewPeerMessage] (Prepare or Commit) and returns the amount of valid
    /// Commits received for the Prepare to which the received Commit belongs to.
    pub(crate) fn collect(&mut self, msg: ViewPeerMessage<P, Sig>) -> Vec<Prepare<P, Sig>> {
        match msg {
            ViewPeerMessage::Prepare(prepare) => {
                trace!(
                    "Collecting Prepare (origin: {:?}, view: {:?}, counter: {:?}) ...",
                    prepare.origin,
                    prepare.view,
                    prepare.counter(),
                );

                match self.recv_commits.get_mut(&prepare.counter()) {
                    Some(collected_commit_origins) => {
                        collected_commit_origins.insert(prepare.origin);
                    }
                    None => {
                        let mut collected_commit_origins = HashSet::new();
                        collected_commit_origins.insert(prepare.origin);
                        self.recv_commits
                            .insert(prepare.counter(), collected_commit_origins);
                    }
                }
                self.prepare.insert(prepare.counter(), prepare);
            }
            ViewPeerMessage::Commit(commit) => {
                trace!(
                    "Collecting Commit (origin: {:?}, counter: {:?}, Prepare: [origin: {:?}, view: {:?}, counter: {:?}]) ...",
                    commit.origin,
                    commit.counter(),
                    commit.prepare.origin,
                    commit.prepare.view,
                    commit.prepare.counter(),
                );

                match self.recv_commits.get_mut(&commit.prepare.counter()) {
                    Some(collected_commit_origins) => {
                        collected_commit_origins.insert(commit.origin);
                    }
                    None => {
                        let mut collected_commit_origins = HashSet::new();
                        collected_commit_origins.insert(commit.origin);
                        self.recv_commits
                            .insert(commit.prepare.counter(), collected_commit_origins);
                    }
                }

                self.prepare
                    .insert(commit.prepare.counter(), commit.prepare.clone());
            }
        }

        let mut vec = Vec::new();

        while let Some(entry) = self.prepare.first_entry() {
            let commits = self.recv_commits.get_mut(entry.key()).unwrap();

            if commits.len() <= self.t.try_into().unwrap() {
                break;
            }

            self.recv_commits.remove_entry(entry.key());
            vec.push(entry.remove());
        }

        vec
    }
}

#[cfg(test)]
mod test {
    use rstest::rstest;
    use usig::Counter;

    use std::num::NonZeroU64;

    use rand::thread_rng;
    use usig::{noop::UsigNoOp, AnyId};

    use crate::{
        client_request::test::create_batch,
        peer_message::usig_message::view_peer_message::{
            commit::test::create_commit, prepare::test::create_prepare, ViewPeerMessage,
        },
        peer_message_processor::collector::collector_commits::CollectorCommits,
        tests::{
            create_attested_usigs_for_replicas, create_config_default,
            create_default_configs_for_replicas, get_random_included_replica_id,
            get_random_replica_id, get_shuffled_remaining_replicas,
        },
        View,
    };

    #[rstest]
    fn collect_commit_single(#[values(3, 4, 5, 6, 7, 8, 9, 10)] n: u64) {
        let n_parsed = NonZeroU64::new(n).unwrap();
        let mut rng = thread_rng();
        let t = n / 2;

        let primary_id = get_random_replica_id(n_parsed, &mut rng);
        let view = View(primary_id.as_u64());
        let mut usig_primary = UsigNoOp::default();
        let config_primary = create_config_default(n_parsed, t, primary_id);
        let request_batch = create_batch();
        let prepare = create_prepare(view, request_batch, &config_primary, &mut usig_primary);

        let backup_id = get_random_included_replica_id(n_parsed, primary_id, &mut rng);
        let mut usig_backup = UsigNoOp::default();
        let commit = create_commit(backup_id, prepare.clone(), &mut usig_backup);

        let vp_msg = ViewPeerMessage::from(commit.clone());

        let mut collector = CollectorCommits::new(t);

        let acceptable_prepares = collector.collect(vp_msg);

        assert!(acceptable_prepares.is_empty());
        assert!(collector
            .recv_commits
            .get(&commit.prepare.counter())
            .is_some());
        let collected_commit_origins = collector
            .recv_commits
            .get(&commit.prepare.counter())
            .unwrap();
        assert!(collected_commit_origins.contains(&commit.origin));
        assert_eq!(collector.prepare.len(), 1);
        assert!(collector.prepare.contains_key(&prepare.counter()));
        let collected_prepare = collector.prepare.get(&prepare.counter()).unwrap();
        assert_eq!(collected_prepare.counter(), prepare.counter());
        assert_eq!(collected_prepare.origin, prepare.origin);
        assert_eq!(collected_prepare.view, prepare.view);
        assert_eq!(collected_prepare.request_batch, prepare.request_batch);
    }

    #[rstest]
    fn collect_sufficient_commits(#[values(3, 4, 5, 6, 7, 8, 9, 10)] n: u64) {
        let n_parsed = NonZeroU64::new(n).unwrap();
        let mut rng = thread_rng();
        let t = n / 2;

        let primary_id = get_random_replica_id(n_parsed, &mut rng);
        let view = View(primary_id.as_u64());
        let request_batch = create_batch();

        let configs = create_default_configs_for_replicas(n_parsed, t);
        let mut usigs = create_attested_usigs_for_replicas(n_parsed, Vec::new());

        let usig_primary = usigs.get_mut(&primary_id).unwrap();
        let config_primary = configs.get(&primary_id).unwrap();

        let prepare = create_prepare(view, request_batch, config_primary, usig_primary);

        let shuffled_backup_reps =
            get_shuffled_remaining_replicas(n_parsed, Some(primary_id), &mut rng);

        let shuffled_set = shuffled_backup_reps.iter().take((t).try_into().unwrap());

        let mut collector = CollectorCommits::new(t);
        let mut acceptable_prepares = collector.collect(ViewPeerMessage::Prepare(prepare.clone()));
        assert!(acceptable_prepares.is_empty());

        let mut counter_collected_commits = 1;
        for backup_rep_id in shuffled_set {
            let usig_backup = usigs.get_mut(backup_rep_id).unwrap();
            let commit = create_commit(*backup_rep_id, prepare.clone(), usig_backup);

            let vp_msg = ViewPeerMessage::from(commit.clone());

            acceptable_prepares = collector.collect(vp_msg);
            counter_collected_commits += 1;

            if counter_collected_commits <= t {
                assert!(acceptable_prepares.is_empty())
            }
        }

        assert_eq!(collector.prepare.len(), 0);
        assert!(!acceptable_prepares.is_empty());
        assert_eq!(acceptable_prepares.len(), 1);
        assert!(acceptable_prepares.contains(&prepare));
    }
}
