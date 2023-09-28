use std::fmt::Debug;
use tracing::debug;

use serde::Serialize;
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
    pub(crate) fn process_checkpoint(&mut self, checkpoint: Checkpoint<U::Signature>) {
        let amount_collected = self
            .collector_checkpoints
            .collect(&self.config, checkpoint.clone());
        if amount_collected <= self.config.t {
            return;
        }
        if let Some(cert) = self
            .collector_checkpoints
            .retrieve(&checkpoint, &self.config)
        {
            // The Replica can discard all entries in its log with a sequence number less than the counter value of its own Checkpoint.
            debug!("clearing message log ...");
            self.sent_usig_msgs
                .retain(|x| x.counter() >= cert.my_checkpoint.counter());

            debug!(
                "cleared message log by removing messages with counter less than {:?}",
                cert.my_checkpoint.counter()
            );
            self.last_checkpoint_cert = Some(cert);
        }
    }
}
