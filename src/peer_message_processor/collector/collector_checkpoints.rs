//! Defines the collector of messages of type [Checkpoint].\
//! A [CheckpointCertificate] is generated when sufficient valid [Checkpoint]s
//! have been collected.\
//! For further explanation, see the paper "Efficient Byzantine Fault Tolerance"
//! by Veronese et al.

use std::cmp::Ordering;

use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use tracing::trace;

use crate::peer_message::usig_message::checkpoint::CheckpointHash;
use crate::{
    peer_message::usig_message::checkpoint::{Checkpoint, CheckpointCertificate},
    Config,
};

use super::CollectorMessages;

/// [Checkpoint]s (collection of messages of type [Checkpoint]) are unstable
/// until the replica's own message and `t` (see [crate::Config]) other messages
/// of type Checkpoint with equal state hash and amount of accepted batches are
/// successfully received.\
/// Additionally, all messages of type [Checkpoint] must originate from
/// different replicas.\
/// The struct allows to save received messages of type [Checkpoint].\

pub(crate) type CollectorCheckpoints<Sig> = CollectorMessages<KeyCheckpoints, Checkpoint<Sig>>;

/// Defines the key for the collector.\
/// The key must be the state hash and the counter of the last accepted prepare.
#[serde_as]
#[derive(Debug, Clone, Hash, PartialEq, Serialize, Deserialize, Eq)]
pub(crate) struct KeyCheckpoints {
    #[serde_as(as = "serde_with::Bytes")]
    state_hash: CheckpointHash,
    total_amount_accepted_batches: u64,
}

impl PartialOrd for KeyCheckpoints {
    /// Partially compares the total amount of accepted batches of the
    /// KeyCheckpoints.
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.total_amount_accepted_batches
            .partial_cmp(&other.total_amount_accepted_batches)
    }
}

impl Ord for KeyCheckpoints {
    /// Compares the amount of accepted batches of the KeyCheckpoints.
    fn cmp(&self, other: &Self) -> Ordering {
        self.total_amount_accepted_batches
            .cmp(&other.total_amount_accepted_batches)
    }
}

impl<Sig: Clone> CollectorCheckpoints<Sig> {
    /// Inserts a message of type [Checkpoint] to the collector.
    pub(crate) fn collect_checkpoint(&mut self, msg: Checkpoint<Sig>) -> u64 {
        trace!("Collecting Checkpoint (origin: {:?}, counter latest accepted Prepare: {:?}, amount accepted batches: {:?}) ...", msg.origin, msg.counter_latest_prep, msg.total_amount_accepted_batches);
        let key = KeyCheckpoints {
            state_hash: msg.state_hash,
            total_amount_accepted_batches: msg.total_amount_accepted_batches,
        };
        let amount_collected = self.collect(msg.clone(), msg.origin, key);
        trace!("Successfully collected Checkpoint (origin: {:?}, counter of latest accepted Prepare: {:?}, amount accepted batches: {:?}).", msg.origin, msg.counter_latest_prep, msg.total_amount_accepted_batches);
        amount_collected
    }

    /// Generates a new checkpoint certificate.\
    /// It only remains to be checked if at least `t + 1` messages have already
    /// been received (one being implicitly the replica's own message).\
    /// This is because the struct field's type choice and the insert method
    /// already guarantee ...\
    ///
    /// 1. that the replica's own message was already received,
    /// 2. that all other messages have the same state hash,
    /// 3. that all other messages have the same amount of accepted batches
    /// as the replica's own message\
    ///
    /// If all these requirements are met, a new checkpoint certificate is
    /// generated.
    ///
    /// # Arguments
    ///
    /// * `msg` - One of or, normally, the last received [Checkpoint] message
    /// that belongs to the same group of collected checkpoints until now.
    /// * `config` - The [Config] of the replica.
    ///
    /// # Return Value
    ///
    /// If the collected checkpoints of the same group (same state hash,
    /// same amount of accepted batches) are stable (`t + 1` messages
    /// originating from different replicas), a checkpoint certificate
    /// consisting of the checkpoints is returned.
    /// Otherwise, [None] is returned.
    ///
    pub(crate) fn retrieve_collected_checkpoints(
        &mut self,
        msg: &Checkpoint<Sig>,
        config: &Config,
    ) -> Option<CheckpointCertificate<Sig>> {
        trace!(
            "Retrieving Checkpoints (amount accepted batches: {:?}) from collector ...",
            msg.total_amount_accepted_batches
        );
        let key = KeyCheckpoints {
            state_hash: msg.state_hash,
            total_amount_accepted_batches: msg.total_amount_accepted_batches,
        };
        let retrieved = self.retrieve(key, config)?;

        let cert = CheckpointCertificate {
            my_checkpoint: retrieved.0,
            other_checkpoints: retrieved.1,
        };
        Some(cert)
    }
}

#[cfg(test)]
mod test {
    use rstest::rstest;

    use super::CollectorCheckpoints;
    use std::num::NonZeroU64;

    use rand::Rng;
    use usig::{Count, ReplicaId};

    use crate::peer_message::usig_message::checkpoint::test::create_rand_number_diff;
    use crate::peer_message_processor::collector::collector_checkpoints::KeyCheckpoints;
    use crate::tests::{
        create_default_configs_for_replicas, get_random_included_index,
        get_shuffled_remaining_replicas,
    };
    use crate::{
        peer_message::usig_message::checkpoint::test::{
            create_checkpoint, create_rand_state_hash_diff,
        },
        tests::{
            create_attested_usigs_for_replicas, create_random_state_hash, get_random_replica_id,
        },
    };

    /// Tests if the collection of a single Checkpoint succeeds.
    ///
    /// # Arguments
    ///
    /// * `n` - The number of replicas.
    #[rstest]
    fn collect_checkpoint_single(#[values(3, 4, 5, 6, 7, 8, 9, 10)] n: u64) {
        let n_parsed = NonZeroU64::new(n).unwrap();

        let mut rng = rand::thread_rng();
        let origin = get_random_replica_id(n_parsed, &mut rng);
        let counter_latest_prep = Count(rng.gen());
        let total_amount_accepted_batches: u64 = rng.gen();
        let state_hash = create_random_state_hash();

        let mut usigs = create_attested_usigs_for_replicas(n_parsed, Vec::new());

        let usig_origin = usigs.get_mut(&origin).unwrap();

        let checkpoint = create_checkpoint(
            origin,
            state_hash,
            counter_latest_prep,
            total_amount_accepted_batches,
            usig_origin,
        );

        let mut collector = CollectorCheckpoints::new();
        collector.collect_checkpoint(checkpoint.clone());

        assert_eq!(collector.0.len(), 1);

        let key = KeyCheckpoints {
            state_hash,
            total_amount_accepted_batches,
        };

        assert!(collector.0.get(&key).is_some());
        let collected_checkpoints = collector.0.get(&key).unwrap();
        assert!(collected_checkpoints.get(&checkpoint.origin).is_some());
        let collected_checkpoint = collected_checkpoints.get(&checkpoint.origin).unwrap();
        assert_eq!(collected_checkpoint.origin, checkpoint.origin);
        assert_eq!(collected_checkpoint.state_hash, checkpoint.state_hash);
        assert_eq!(
            collected_checkpoint.counter_latest_prep,
            checkpoint.counter_latest_prep
        );
        assert_eq!(
            collected_checkpoint.total_amount_accepted_batches,
            checkpoint.total_amount_accepted_batches
        );
    }

    /// Tests if the retrieval of Checkpoints is as expected.
    /// Both the case of the checkpoint certificate not being stable yet as well
    /// as when it is stable are tested.
    ///
    /// # Arguments
    ///
    /// * `n` - The number of replicas.
    #[rstest]
    fn retrieve_checkpoint(#[values(3, 4, 5, 6, 7, 8, 9, 10)] n: u64) {
        let n_parsed = NonZeroU64::new(n).unwrap();
        let t = n / 2;

        let mut rng = rand::thread_rng();
        let counter_latest_prep = Count(rng.gen());
        let total_amount_accepted_batches: u64 = rng.gen();
        let state_hash = create_random_state_hash();

        let configs = create_default_configs_for_replicas(n_parsed, t);
        let mut usigs = create_attested_usigs_for_replicas(n_parsed, Vec::new());

        let shuffled_replicas = get_shuffled_remaining_replicas(n_parsed, None, &mut rng);
        let shuffled_iter = shuffled_replicas.iter().take((t + 1).try_into().unwrap());
        let shuffled_set: Vec<ReplicaId> = shuffled_iter.clone().cloned().collect();

        let origin_index = get_random_included_index(shuffled_iter.len(), None, &mut rng);
        let origin = shuffled_set[origin_index];
        let config_origin = configs.get(&origin).unwrap();

        let mut collector = CollectorCheckpoints::new();

        let mut last_collected_checkpoint = None;

        let mut counter_collected = 0;
        for rep_id in shuffled_iter {
            let usig_rep_id = usigs.get_mut(rep_id).unwrap();

            let checkpoint = create_checkpoint(
                *rep_id,
                state_hash,
                counter_latest_prep,
                total_amount_accepted_batches,
                usig_rep_id,
            );

            collector.collect_checkpoint(checkpoint.clone());
            counter_collected += 1;
            last_collected_checkpoint = Some(checkpoint.clone());

            if counter_collected <= t.try_into().unwrap() {
                let cp_cert = collector.retrieve_collected_checkpoints(
                    &last_collected_checkpoint.clone().unwrap(),
                    config_origin,
                );
                assert!(cp_cert.is_none());
            }
        }

        assert!(last_collected_checkpoint.is_some());

        let cp_cert = collector
            .retrieve_collected_checkpoints(&last_collected_checkpoint.unwrap(), config_origin);
        assert!(cp_cert.is_some());
        let cp_cert = cp_cert.unwrap();

        assert_eq!(cp_cert.my_checkpoint.origin, origin);
        assert_eq!(cp_cert.my_checkpoint.state_hash, state_hash);
        assert_eq!(
            cp_cert.my_checkpoint.counter_latest_prep,
            counter_latest_prep
        );
        assert_eq!(
            cp_cert.my_checkpoint.total_amount_accepted_batches,
            total_amount_accepted_batches
        );
    }

    /// Tests if the collection of different Checkpoints (not belonging to the
    /// same group of state hashes and amount of accepted batches) behaves as
    /// expectedly, i.e., the checkpoint certificate should not be stable
    /// as not enough checkpoints have been collected.\
    /// Here, the state hashes differ.
    ///
    /// # Arguments
    ///
    /// * `n` - The number of replicas.
    #[rstest]
    fn collect_diff_checkpoints_state_hash(#[values(3, 4, 5, 6, 7, 8, 9, 10)] n: u64) {
        let n_parsed = NonZeroU64::new(n).unwrap();

        let mut rng = rand::thread_rng();
        let origin = get_random_replica_id(n_parsed, &mut rng);
        let counter_latest_prep = Count(rng.gen());
        let total_amount_accepted_batches: u64 = rng.gen();
        let state_hash = create_random_state_hash();

        let mut usigs = create_attested_usigs_for_replicas(n_parsed, Vec::new());
        let usig_origin = usigs.get_mut(&origin).unwrap();

        let mut collector = CollectorCheckpoints::new();

        let checkpoint = create_checkpoint(
            origin,
            state_hash,
            counter_latest_prep,
            total_amount_accepted_batches,
            usig_origin,
        );
        collector.collect_checkpoint(checkpoint.clone());

        let state_hash_diff = create_rand_state_hash_diff(state_hash, &mut rng);

        let checkpoint_diff = create_checkpoint(
            origin,
            state_hash_diff,
            counter_latest_prep,
            total_amount_accepted_batches,
            usig_origin,
        );
        collector.collect_checkpoint(checkpoint_diff.clone());

        assert_eq!(collector.0.len(), 2);

        // Check if first created checkpoint was collected successfully.
        let key = KeyCheckpoints {
            state_hash,
            total_amount_accepted_batches,
        };
        assert!(collector.0.get(&key).is_some());
        let collected_checkpoints = collector.0.get(&key).unwrap();
        assert_eq!(collected_checkpoints.len(), 1);
        assert!(collected_checkpoints.get(&checkpoint.origin).is_some());
        let collected_checkpoint = collected_checkpoints.get(&checkpoint.origin).unwrap();
        assert_eq!(collected_checkpoint.origin, checkpoint.origin);
        assert_eq!(collected_checkpoint.state_hash, checkpoint.state_hash);
        assert_eq!(
            collected_checkpoint.counter_latest_prep,
            checkpoint.counter_latest_prep
        );
        assert_eq!(
            collected_checkpoint.total_amount_accepted_batches,
            checkpoint.total_amount_accepted_batches
        );

        // Check if second created checkpoint was collected successfully.
        let key_diff = KeyCheckpoints {
            state_hash: state_hash_diff,
            total_amount_accepted_batches,
        };
        assert!(collector.0.get(&key_diff).is_some());
        let collected_checkpoints = collector.0.get(&key_diff).unwrap();
        assert_eq!(collected_checkpoints.len(), 1);
        assert!(collected_checkpoints.get(&checkpoint_diff.origin).is_some());
        let collected_checkpoint = collected_checkpoints.get(&checkpoint_diff.origin).unwrap();
        assert_eq!(collected_checkpoint.origin, checkpoint_diff.origin);
        assert_eq!(collected_checkpoint.state_hash, checkpoint_diff.state_hash);
        assert_eq!(
            collected_checkpoint.counter_latest_prep,
            checkpoint_diff.counter_latest_prep
        );
        assert_eq!(
            collected_checkpoint.total_amount_accepted_batches,
            checkpoint_diff.total_amount_accepted_batches
        );
    }

    /// Tests if the collection of different Checkpoints (not belonging to the
    /// same group of state hashes and amount of accepted batches) behaves as
    /// expectedly, i.e., the checkpoint certificate should not be stable
    /// as not enough checkpoints have been collected.\
    /// Here, the amount of accepted batches differ.
    ///
    /// # Arguments
    ///
    /// * `n` - The number of replicas.
    #[rstest]
    fn collect_diff_checkpoints_amount_accepted_batches(#[values(3, 4, 5, 6, 7, 8, 9, 10)] n: u64) {
        let n_parsed = NonZeroU64::new(n).unwrap();

        let mut rng = rand::thread_rng();
        let origin = get_random_replica_id(n_parsed, &mut rng);
        let counter_latest_prep = Count(rng.gen());
        let total_amount_accepted_batches: u64 = rng.gen();
        let state_hash = create_random_state_hash();

        let mut usigs = create_attested_usigs_for_replicas(n_parsed, Vec::new());
        let usig_origin = usigs.get_mut(&origin).unwrap();

        let mut collector = CollectorCheckpoints::new();

        let checkpoint = create_checkpoint(
            origin,
            state_hash,
            counter_latest_prep,
            total_amount_accepted_batches,
            usig_origin,
        );
        collector.collect_checkpoint(checkpoint.clone());

        let total_amount_accepted_batches_diff =
            create_rand_number_diff(total_amount_accepted_batches, &mut rng);

        let checkpoint_diff = create_checkpoint(
            origin,
            state_hash,
            counter_latest_prep,
            total_amount_accepted_batches_diff,
            usig_origin,
        );
        collector.collect_checkpoint(checkpoint_diff.clone());

        assert_eq!(collector.0.len(), 2);

        // Check if first created checkpoint was collected successfully.
        let key = KeyCheckpoints {
            state_hash,
            total_amount_accepted_batches,
        };
        assert!(collector.0.get(&key).is_some());
        let collected_checkpoints = collector.0.get(&key).unwrap();
        assert_eq!(collected_checkpoints.len(), 1);
        assert!(collected_checkpoints.get(&checkpoint.origin).is_some());
        let collected_checkpoint = collected_checkpoints.get(&checkpoint.origin).unwrap();
        assert_eq!(collected_checkpoint.origin, checkpoint.origin);
        assert_eq!(collected_checkpoint.state_hash, checkpoint.state_hash);
        assert_eq!(
            collected_checkpoint.counter_latest_prep,
            checkpoint.counter_latest_prep
        );
        assert_eq!(
            collected_checkpoint.total_amount_accepted_batches,
            checkpoint.total_amount_accepted_batches
        );

        // Check if second created checkpoint was collected successfully.
        let key_diff = KeyCheckpoints {
            state_hash,
            total_amount_accepted_batches: total_amount_accepted_batches_diff,
        };
        assert!(collector.0.get(&key_diff).is_some());
        let collected_checkpoints = collector.0.get(&key_diff).unwrap();
        assert_eq!(collected_checkpoints.len(), 1);
        assert!(collected_checkpoints.get(&checkpoint_diff.origin).is_some());
        let collected_checkpoint = collected_checkpoints.get(&checkpoint_diff.origin).unwrap();
        assert_eq!(collected_checkpoint.origin, checkpoint_diff.origin);
        assert_eq!(collected_checkpoint.state_hash, checkpoint_diff.state_hash);
        assert_eq!(
            collected_checkpoint.counter_latest_prep,
            checkpoint_diff.counter_latest_prep
        );
        assert_eq!(
            collected_checkpoint.total_amount_accepted_batches,
            checkpoint_diff.total_amount_accepted_batches
        );
    }

    /// Tests if the collection of different Checkpoints (not belonging to the
    /// same group of state hashes and amount of accepted batches) behaves as
    /// expectedly, i.e., the checkpoint certificate should not be stable
    /// as not enough checkpoints have been collected.\
    /// Here, both the state hash as well as the amount of accepted batches
    /// differ.
    ///
    /// # Arguments
    ///
    /// * `n` - The number of replicas.
    #[rstest]
    fn collect_diff_checkpoints_all_state(#[values(3, 4, 5, 6, 7, 8, 9, 10)] n: u64) {
        let n_parsed = NonZeroU64::new(n).unwrap();

        let mut rng = rand::thread_rng();
        let origin = get_random_replica_id(n_parsed, &mut rng);
        let counter_latest_prep = Count(rng.gen());
        let total_amount_accepted_batches: u64 = rng.gen();
        let state_hash = create_random_state_hash();

        let mut usigs = create_attested_usigs_for_replicas(n_parsed, Vec::new());
        let usig_origin = usigs.get_mut(&origin).unwrap();

        let mut collector = CollectorCheckpoints::new();

        let checkpoint = create_checkpoint(
            origin,
            state_hash,
            counter_latest_prep,
            total_amount_accepted_batches,
            usig_origin,
        );
        collector.collect_checkpoint(checkpoint.clone());

        let state_hash_diff = create_rand_state_hash_diff(state_hash, &mut rng);
        let total_amount_accepted_batches_diff =
            create_rand_number_diff(total_amount_accepted_batches, &mut rng);

        let checkpoint_diff = create_checkpoint(
            origin,
            state_hash_diff,
            counter_latest_prep,
            total_amount_accepted_batches_diff,
            usig_origin,
        );
        collector.collect_checkpoint(checkpoint_diff.clone());

        assert_eq!(collector.0.len(), 2);

        // Check if first created checkpoint was collected successfully.
        let key = KeyCheckpoints {
            state_hash,
            total_amount_accepted_batches,
        };
        assert!(collector.0.get(&key).is_some());
        let collected_checkpoints = collector.0.get(&key).unwrap();
        assert_eq!(collected_checkpoints.len(), 1);
        assert!(collected_checkpoints.get(&checkpoint.origin).is_some());
        let collected_checkpoint = collected_checkpoints.get(&checkpoint.origin).unwrap();
        assert_eq!(collected_checkpoint.origin, checkpoint.origin);
        assert_eq!(collected_checkpoint.state_hash, checkpoint.state_hash);
        assert_eq!(
            collected_checkpoint.counter_latest_prep,
            checkpoint.counter_latest_prep
        );
        assert_eq!(
            collected_checkpoint.total_amount_accepted_batches,
            checkpoint.total_amount_accepted_batches
        );

        // Check if second created checkpoint was collected successfully.
        let key_diff = KeyCheckpoints {
            state_hash: state_hash_diff,
            total_amount_accepted_batches: total_amount_accepted_batches_diff,
        };
        assert!(collector.0.get(&key_diff).is_some());
        let collected_checkpoints = collector.0.get(&key_diff).unwrap();
        assert_eq!(collected_checkpoints.len(), 1);
        assert!(collected_checkpoints.get(&checkpoint_diff.origin).is_some());
        let collected_checkpoint = collected_checkpoints.get(&checkpoint_diff.origin).unwrap();
        assert_eq!(collected_checkpoint.origin, checkpoint_diff.origin);
        assert_eq!(collected_checkpoint.state_hash, checkpoint_diff.state_hash);
        assert_eq!(
            collected_checkpoint.counter_latest_prep,
            checkpoint_diff.counter_latest_prep
        );
        assert_eq!(
            collected_checkpoint.total_amount_accepted_batches,
            checkpoint_diff.total_amount_accepted_batches
        );
    }
}
