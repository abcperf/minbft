//! Defines the collector of messages of type [Checkpoint].
//! A [CheckpointCertificate] is generated when sufficient valid [Checkpoint]s have been collected.
//! The [Checkpoint]s must share the same state hash and the same counter of the latest [crate::Prepare] accepted.
//! For further explanation, see the paper "Efficient Byzantine Fault Tolerance" by Veronese et al.

use std::cmp::Ordering;

use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use tracing::debug;

use crate::peer_message::usig_message::checkpoint::CheckpointHash;
use crate::{
    peer_message::usig_message::checkpoint::{Checkpoint, CheckpointCertificate},
    Config,
};

use super::CollectorMessages;

/// [Checkpoint]s (collection of messages of type [Checkpoint]) are unstable
/// until the Replica's own message and t (see [crate::Config]) other messages of type Checkpoint
/// with equal state hash are successfully received.
/// Additionally, all messages of type [Checkpoint] must originate from different replicas.
/// The struct allows to save received messages of type [Checkpoint].
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct CollectorCheckpoints<Sig>(CollectorMessages<KeyCheckpoints, Checkpoint<Sig>>);

/// Defines the key for the collector.
/// The key must be the state hash and the counter of the last accepted prepare.
#[serde_as]
#[derive(Debug, Clone, Hash, PartialEq, Serialize, Deserialize)]
struct KeyCheckpoints {
    #[serde_as(as = "serde_with::Bytes")]
    state_hash: CheckpointHash,
    total_amount_accepted_batches: u64,
}

impl Eq for KeyCheckpoints {}

impl PartialOrd for KeyCheckpoints {
    /// Partially compares the counters of the KeyCheckpoints.
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.total_amount_accepted_batches
            .partial_cmp(&other.total_amount_accepted_batches)
    }
}

impl Ord for KeyCheckpoints {
    /// Compares the counters of the KeyCheckpoints.
    fn cmp(&self, other: &Self) -> Ordering {
        self.total_amount_accepted_batches
            .cmp(&other.total_amount_accepted_batches)
    }
}

impl<Sig: Clone> CollectorCheckpoints<Sig> {
    /// Creates a new [CollectorCheckpoints].
    pub(crate) fn new() -> Self {
        CollectorCheckpoints(CollectorMessages::new())
    }

    /// Inserts a message of type [Checkpoint] to the collector.
    pub(crate) fn collect(&mut self, config: &Config, msg: Checkpoint<Sig>) -> u64 {
        debug!("Collecting Checkpoint (origin: {:?}, counter latest accepted Prepare: {:?}, amount accepted batches: {:?}) ...", msg.origin, msg.counter_latest_prep, msg.total_amount_accepted_batches);
        let key = KeyCheckpoints {
            state_hash: msg.state_hash,
            total_amount_accepted_batches: msg.total_amount_accepted_batches,
        };
        let amount_collected = self.0.collect(msg.clone(), msg.origin, key, config);
        debug!("Successfully collected Checkpoint (origin: {:?}, counter of latest accepted Prepare: {:?}, amount accepted batches: {:?}).", msg.origin, msg.counter_latest_prep, msg.total_amount_accepted_batches);
        amount_collected
    }

    /// Generate a new checkpoint certificate.
    /// Due to the struct field's type choice and the insert method
    /// already guaranteeing that the replica's own message was already received,
    /// and that all other messages have the same state hash and counter of last accepted [crate::Prepare]
    /// as the replica's own message, it only remains to be checked
    /// if at least t + 1 messages have already been received (one being implicitly the replica's own message).
    /// If all these requirements are met, a new checkpoint certificate is generated.
    pub(crate) fn retrieve(
        &mut self,
        msg: &Checkpoint<Sig>,
        config: &Config,
    ) -> Option<CheckpointCertificate<Sig>> {
        debug!(
            "Retrieving Checkpoints (amount accepted batches: {:?}) from collector ...",
            msg.total_amount_accepted_batches
        );
        let key = KeyCheckpoints {
            state_hash: msg.state_hash,
            total_amount_accepted_batches: msg.total_amount_accepted_batches,
        };
        let retrieved = self.0.retrieve(key, config)?;

        let cert = CheckpointCertificate {
            my_checkpoint: retrieved.0,
            other_checkpoints: retrieved.1,
        };
        Some(cert)
    }
}
