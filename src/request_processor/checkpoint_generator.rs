use std::marker::PhantomData;

use blake2::{Blake2b512, Digest};
use tracing::trace;
use usig::{Counter, Usig};

use crate::{
    peer_message::usig_message::{
        checkpoint::{CheckpointContent, CheckpointHash},
        view_peer_message::prepare::Prepare,
    },
    Config, RequestPayload,
};

/// The purpose of the struct is to generate Checkpoints.
/// In addition, it keeps track of the hash of the last Checkpoint generated and
/// of the total amount of accepted batches.
#[derive(Debug, Clone)]
pub(crate) struct CheckpointGenerator<P: RequestPayload, U: Usig> {
    /// The hash of the last Checkpoint generated.
    pub(crate) last_hash: CheckpointHash,
    /// The log of accepted batches.
    /// The total amount of batches have been accepted since the last Checkpoint generated.
    pub(crate) total_amount_accepted_batches: u64,
    phantom_data: PhantomData<(P, U)>,
}

impl<P: RequestPayload, U: Usig> CheckpointGenerator<P, U> {
    /// Create a new CheckpointGenerator.
    pub(crate) fn new() -> Self {
        CheckpointGenerator {
            last_hash: [0; 64],
            total_amount_accepted_batches: 0,
            phantom_data: PhantomData,
        }
    }

    /// Decides if a Checkpoint should be generated.
    ///
    /// # Arguments
    ///
    /// * `prepare` - The [Prepare] that has just been accepted.
    /// * `config` - The configuration of the replica.
    ///
    /// # Return Value
    ///
    /// Returns a Checkpoint if it decides in favor, otherwise [None] is
    /// returned.
    pub(crate) fn generate_checkpoint(
        &mut self,
        prepare: &Prepare<P, U::Signature>,
        config: &Config,
    ) -> Option<CheckpointContent> {
        self.total_amount_accepted_batches += 1;
        self.last_hash = self.next_state_hash(prepare);

        trace!(
            "Accepted in total {:?} batches.",
            self.total_amount_accepted_batches
        );
        if self.total_amount_accepted_batches % config.checkpoint_period != 0 {
            return None;
        };
        let checkpoint = CheckpointContent {
            origin: config.id,
            state_hash: self.last_hash,
            counter_latest_prep: prepare.counter(),
            total_amount_accepted_batches: self.total_amount_accepted_batches,
        };
        Some(checkpoint)
    }

    /// Hashes the state for the next Checkpoint using the hash of the last
    /// Checkpoint generated and the request batch of the [Prepare].
    ///
    /// # Arguments
    ///
    /// * `prepare` - The [Prepare] that has just been accepted.
    ///
    /// # Return Value
    ///
    /// The new [CheckpointHash].
    fn next_state_hash(&mut self, prepare: &Prepare<P, U::Signature>) -> CheckpointHash {
        let mut hasher = Blake2b512::new();

        hasher.update(self.last_hash);
        hasher.update(bincode::serialize(&prepare.request_batch).unwrap());

        self.last_hash = hasher.finalize().into();
        self.last_hash
    }
}
