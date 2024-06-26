use std::fmt::Debug;

use serde::Serialize;
use tracing::trace;
use usig::Counter;
use usig::Usig;

use crate::peer_message::usig_message::checkpoint::Checkpoint;
use crate::MinBft;
use crate::RequestPayload;

impl<P: RequestPayload, U: Usig> MinBft<P, U>
where
    U::Attestation: Clone,
    U::Signature: Clone + Serialize,
    U::Signature: Debug,
{
    /// Process a received message of type [Checkpoint].
    /// The steps are as following:
    ///
    /// 1. Collect the received [Checkpoint] with the collector of Checkpoints,
    /// namely [CollectorCheckpoints](crate::peer_message_processor::collector::collector_checkpoints::CollectorCheckpoints).
    /// 2. Retrieve a checkpoint certificate of the collected Checkpoints if
    /// they can be retrieved (see the documentation of collector).
    /// 3. Discard all entries in the message log of the replica that have
    /// a counter value less than its own [Checkpoint] using the checkpoint
    /// certificate.
    /// 4. Update the inner state of the replica by updating the last checkpoint
    /// cert generated.
    ///
    /// # Arguments
    ///
    /// * `checkpoint` - The Checkpoint message to be processed.
    pub(crate) fn process_checkpoint(&mut self, checkpoint: Checkpoint<U::Signature>) {
        let amount_collected = self
            .collector_checkpoints
            .collect_checkpoint(checkpoint.clone());
        if amount_collected <= self.config.t {
            trace!("Processing Checkpoint (origin: {:?}, counter latest accepted Prepare: {:?}, amount accepted batches: {:?}) resulted in ignoring creation of Certificate: A sufficient amount of Checkpoints has not been collected yet (collected: {:?}, required: {:?}).", checkpoint.origin, checkpoint.counter_latest_prep, checkpoint.total_amount_accepted_batches, amount_collected, self.config.t + 1);
            return;
        }
        if let Some(cert) = self
            .collector_checkpoints
            .retrieve_collected_checkpoints(&checkpoint, &self.config)
        {
            // The Replica can discard all entries in its log with a sequence number less than the counter value of its own Checkpoint.
            trace!(
                "Clearing message log by removing messages with a counter less than {:?}...",
                cert.my_checkpoint.counter()
            );
            self.sent_usig_msgs
                .retain(|x| x.counter() >= cert.my_checkpoint.counter());

            trace!(
                "Successfully cleared message log by removing messages with a counter less than {:?}.",
                cert.my_checkpoint.counter()
            );
            self.last_checkpoint_cert = Some(cert);
        }
    }
}
