//! Defines the collector of messages of type ReqViewChange.\
//! After a sufficient amount (t + 1) of ReqViewChanges are received and
//! collected, a ViewChange is broadcast.\
//! The ReqViewChanges must share the same previous and next [crate::View]s.

use serde::{Deserialize, Serialize};
use shared_ids::ReplicaId;
use std::{
    cmp::Ordering,
    collections::{HashMap, HashSet},
    hash::Hash,
};
use tracing::{debug, trace};

use crate::{peer_message::req_view_change::ReqViewChange, View};

/// The purpose of the struct is to collect messages of type ReqViewChange.
#[derive(Debug, Clone)]
pub(crate) struct CollectorReqViewChanges(HashMap<KeyRVC, HashSet<ReplicaId>>);

/// Defines the key for the collector.\
/// The key must be the previous and next [crate::View]s.
#[derive(Debug, Serialize, Deserialize, Clone)]
struct KeyRVC {
    // The previous View that turned out to be faulty for the sender of the
    // received ReqViewChange.
    prev_view: View,
    // The next View to which the sender of the received ReqViewChange message
    // wants to change to.
    next_view: View,
}

impl PartialEq for KeyRVC {
    /// Returns true if both the previous and the next [View]s are equal.
    fn eq(&self, other: &Self) -> bool {
        self.prev_view.eq(&other.prev_view) && self.next_view.eq(&other.next_view)
    }
}

impl Hash for KeyRVC {
    /// Computes the hash of the previous and the next [View].
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.prev_view.hash(state);
        self.next_view.hash(state);
    }
}

impl Eq for KeyRVC {}

impl PartialOrd for KeyRVC {
    /// Partially compares the previous Views with each other.\
    /// If the previous Views are equal, then the next Views are partially
    /// compared with each other.
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match self.prev_view.partial_cmp(&other.prev_view) {
            Some(Ordering::Less) => Some(Ordering::Less),
            Some(Ordering::Equal) => self.next_view.partial_cmp(&other.next_view),
            Some(Ordering::Greater) => Some(Ordering::Greater),
            None => None,
        }
    }
}

impl Ord for KeyRVC {
    /// Compares the previous Views with each other.
    /// If the previous Views are equal, then the next Views are compared with
    /// each other.
    fn cmp(&self, other: &Self) -> Ordering {
        match self.prev_view.cmp(&other.prev_view) {
            Ordering::Less => Ordering::Less,
            Ordering::Equal => self.next_view.cmp(&other.next_view),
            Ordering::Greater => Ordering::Greater,
        }
    }
}

impl CollectorReqViewChanges {
    /// Creates a new collector of ReqViewChanges.
    pub(crate) fn new() -> Self {
        Self(HashMap::new())
    }

    /// Inserts a ReqViewChange message and
    ///
    /// # Arguments
    ///
    /// * `msg` - The ReqViewChange message to be collected.
    /// * `from` - The origin of the message to be collected.
    ///
    /// # Return Value
    ///
    /// The amount of collected ReqViewChanges received for the same previous
    /// and next [View] as the input.
    pub(crate) fn collect(&mut self, msg: &ReqViewChange, from: ReplicaId) -> u64 {
        trace!(
            "Collecting ReqViewChange (origin: {from:?}, previous view: {:?}, next view: {:?}) ...",
            msg.prev_view,
            msg.next_view,
        );
        let key = KeyRVC {
            prev_view: msg.prev_view,
            next_view: msg.next_view,
        };

        match self.0.get_mut(&key) {
            Some(collected_rvc) => {
                collected_rvc.insert(from);
            }
            None => {
                let mut collected_rvc = HashSet::new();
                collected_rvc.insert(from);
                self.0.insert(key.clone(), collected_rvc);
            }
        }
        let amount_collected = self.0.get(&key).unwrap().len();
        trace!(
            "Successfully collected ReqViewChange (origin: {from:?}, previous view: {:?}, next view: {:?}).",
            msg.prev_view, msg.next_view,
        );
        amount_collected.try_into().unwrap()
    }

    /// Cleans up the collection by retaining only ReqViewChanges with
    /// their previous [View] set higher than the provided one, or
    /// the same but their next [View] is set higher.
    ///
    /// # Arguments
    ///
    /// * `prev_view` - The previous [View] that should be used to filter out
    ///                 old, no longer necessary messages.
    /// * `next_view` - The next [View] that should be used to filter out old,
    ///                 no longer necessary messages.
    pub(crate) fn clean_up(&mut self, prev_view: View, next_view: View) {
        debug!("Cleaning up collector of ReqViewChanges: Removing ReqViewChanges with previous view less than {:?} or equal to it and next view less than {:?}", prev_view, next_view);
        let key = KeyRVC {
            prev_view,
            next_view,
        };
        self.0.retain(|k, _| k > &key);
    }
}

#[cfg(test)]

mod test {
    use rstest::rstest;

    use std::num::NonZeroU64;

    use rand::{thread_rng, Rng};

    use crate::{
        peer_message::req_view_change::ReqViewChange,
        peer_message_processor::collector::collector_req_view_changes::CollectorReqViewChanges,
        tests::{
            create_random_valid_req_vc_next_dir_subsequent, get_random_included_replica_id,
            get_random_replica_id,
        },
    };

    /// Tests if the collection of a single ReqViewChange succeeds.
    ///
    /// # Arguments
    ///
    /// * `n` - The number of replicas.
    #[rstest]
    fn collect_req_vc_single(#[values(3, 4, 5, 6, 7, 8, 9, 10)] n: u64) {
        let n_parsed = NonZeroU64::new(n).unwrap();
        let mut rng = thread_rng();

        let req_view_change = create_random_valid_req_vc_next_dir_subsequent(n_parsed, &mut rng);

        let mut collector = CollectorReqViewChanges::new();

        let n_parsed = NonZeroU64::new(n).unwrap();
        let mut rng = thread_rng();

        let rep_id = get_random_replica_id(n_parsed, &mut rng);

        let retrieved = collector.collect(&req_view_change, rep_id);
        assert_eq!(retrieved, 1);
    }

    /// Tests if the collection of multiple ReqViewChanges with different
    /// origins succeeds.
    ///
    /// # Arguments
    ///
    /// * `n` - The number of replicas.
    #[rstest]
    fn collect_req_vc_multiple_diff_origins(#[values(3, 4, 5, 6, 7, 8, 9, 10)] n: u64) {
        let n_parsed = NonZeroU64::new(n).unwrap();
        let mut rng = thread_rng();

        let first_req_view_change =
            create_random_valid_req_vc_next_dir_subsequent(n_parsed, &mut rng);

        let mut collector = CollectorReqViewChanges::new();

        let n_parsed = NonZeroU64::new(n).unwrap();
        let mut rng = thread_rng();

        let first_rep_id = get_random_replica_id(n_parsed, &mut rng);

        let retrieved = collector.collect(&first_req_view_change, first_rep_id);
        assert_eq!(retrieved, 1);

        let second_req_view_change = first_req_view_change.clone();
        let second_rep_id = get_random_included_replica_id(n_parsed, first_rep_id, &mut rng);
        let retrieved = collector.collect(&second_req_view_change, second_rep_id);
        assert_eq!(retrieved, 2);
    }

    /// Tests if the collection of multiple ReqViewChanges with same
    /// origins behaves as expected.
    /// That is, they should not be grouped together.
    ///
    /// # Arguments
    ///
    /// * `n` - The number of replicas.
    #[rstest]
    fn collect_req_vc_multiple_same_origin(#[values(3, 4, 5, 6, 7, 8, 9, 10)] n: u64) {
        let n_parsed = NonZeroU64::new(n).unwrap();
        let mut rng = thread_rng();

        let first_req_view_change =
            create_random_valid_req_vc_next_dir_subsequent(n_parsed, &mut rng);

        let mut collector = CollectorReqViewChanges::new();

        let n_parsed = NonZeroU64::new(n).unwrap();
        let mut rng = thread_rng();

        let rep_id = get_random_replica_id(n_parsed, &mut rng);

        let retrieved = collector.collect(&first_req_view_change, rep_id);
        assert_eq!(retrieved, 1);

        let second_req_view_change = first_req_view_change.clone();
        let retrieved = collector.collect(&second_req_view_change, rep_id);
        assert_eq!(retrieved, 1);
    }

    /// Tests if the collection of multiple ReqViewChanges for different
    /// views behaves as expected.
    /// That is, they should not be grouped together.
    ///
    /// # Arguments
    ///
    /// * `n` - The number of replicas.
    #[rstest]
    fn collect_req_vc_multiple_diff_views(#[values(3, 4, 5, 6, 7, 8, 9, 10)] n: u64) {
        let n_parsed = NonZeroU64::new(n).unwrap();
        let mut rng = thread_rng();

        let first_req_view_change =
            create_random_valid_req_vc_next_dir_subsequent(n_parsed, &mut rng);

        let mut collector = CollectorReqViewChanges::new();

        let n_parsed = NonZeroU64::new(n).unwrap();
        let mut rng = thread_rng();

        let first_rep_id = get_random_replica_id(n_parsed, &mut rng);

        let retrieved = collector.collect(&first_req_view_change, first_rep_id);
        assert_eq!(retrieved, 1);

        let rand_summand = rng.gen_range(1..=n * 10);

        let second_req_view_change = ReqViewChange {
            prev_view: first_req_view_change.prev_view + rand_summand,
            next_view: first_req_view_change.next_view + rand_summand,
        };
        let second_rep_id = get_random_included_replica_id(n_parsed, first_rep_id, &mut rng);
        let retrieved = collector.collect(&second_req_view_change, second_rep_id);
        assert_eq!(retrieved, 1);

        let retrieved = collector.collect(&first_req_view_change, first_rep_id);
        assert_eq!(retrieved, 1);
    }

    /// Tests if the collection of multiple ReqViewChanges for different
    /// previous views behaves as expected.
    /// That is, they should not be grouped together.
    ///
    /// # Arguments
    ///
    /// * `n` - The number of replicas.
    #[rstest]
    fn collect_req_vc_multiple_diff_prev_view(#[values(3, 4, 5, 6, 7, 8, 9, 10)] n: u64) {
        let n_parsed = NonZeroU64::new(n).unwrap();
        let mut rng = thread_rng();

        let first_req_view_change =
            create_random_valid_req_vc_next_dir_subsequent(n_parsed, &mut rng);

        let mut collector = CollectorReqViewChanges::new();

        let n_parsed = NonZeroU64::new(n).unwrap();
        let mut rng = thread_rng();

        let first_rep_id = get_random_replica_id(n_parsed, &mut rng);

        let retrieved = collector.collect(&first_req_view_change, first_rep_id);
        assert_eq!(retrieved, 1);

        let rand_summand = rng.gen_range(1..=n * 10);

        let second_req_view_change = ReqViewChange {
            prev_view: first_req_view_change.prev_view + rand_summand,
            next_view: first_req_view_change.next_view,
        };
        let second_rep_id = get_random_included_replica_id(n_parsed, first_rep_id, &mut rng);
        let retrieved = collector.collect(&second_req_view_change, second_rep_id);
        assert_eq!(retrieved, 1);

        let retrieved = collector.collect(&first_req_view_change, first_rep_id);
        assert_eq!(retrieved, 1);
    }

    /// Tests if the collection of multiple ReqViewChanges for different next
    /// views behaves as expected.
    /// That is, they should not be grouped together.
    ///
    /// # Arguments
    ///
    /// * `n` - The number of replicas.
    #[rstest]
    fn collect_req_vc_multiple_diff_next_view(#[values(3, 4, 5, 6, 7, 8, 9, 10)] n: u64) {
        let n_parsed = NonZeroU64::new(n).unwrap();
        let mut rng = thread_rng();

        let first_req_view_change =
            create_random_valid_req_vc_next_dir_subsequent(n_parsed, &mut rng);

        let mut collector = CollectorReqViewChanges::new();

        let n_parsed = NonZeroU64::new(n).unwrap();
        let mut rng = thread_rng();

        let first_rep_id = get_random_replica_id(n_parsed, &mut rng);

        let retrieved = collector.collect(&first_req_view_change, first_rep_id);
        assert_eq!(retrieved, 1);

        let rand_summand = rng.gen_range(1..=n * 10);

        let second_req_view_change = ReqViewChange {
            prev_view: first_req_view_change.prev_view,
            next_view: first_req_view_change.next_view + rand_summand,
        };
        let second_rep_id = get_random_included_replica_id(n_parsed, first_rep_id, &mut rng);
        let retrieved = collector.collect(&second_req_view_change, second_rep_id);
        assert_eq!(retrieved, 1);

        let retrieved = collector.collect(&first_req_view_change, first_rep_id);
        assert_eq!(retrieved, 1);
    }
}
