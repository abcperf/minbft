pub(crate) mod collector;
mod hello_processor;
mod req_view_change_processor;
mod usig_message_processor;

use serde::Serialize;
use shared_ids::ReplicaId;
use std::fmt::Debug;
use tracing::trace;

use usig::Usig;

use crate::{
    output::{self, NotReflectedOutput, OutputRestricted, ViewInfo},
    peer_message::ValidatedPeerMessage,
    MinBft, RequestPayload,
};

impl<P: RequestPayload, U: Usig> MinBft<P, U>
where
    U::Attestation: Clone,
    U::Signature: Clone + Serialize,
    U::Signature: Debug,
{
    /// Process messages of type [crate::PeerMessage].
    ///
    /// # Arguments
    ///
    /// * `from` - The ID of the replica from which the message originates.
    /// * `message` - The validated [crate::PeerMessage] to process.
    /// * `output` - The output struct to be adjusted in case of, e.g., errors
    ///              or responses.
    pub(crate) fn process_peer_message(
        &mut self,
        from: ReplicaId,
        message: ValidatedPeerMessage<U::Attestation, P, U::Signature>,
        output: &mut NotReflectedOutput<P, U>,
    ) {
        let msg_type = message.msg_type();
        trace!(
            "Processing message (origin: {from:?}, type: {:?}) ...",
            msg_type
        );
        match message {
            ValidatedPeerMessage::ReqViewChange(req_view_change) => {
                self.process_req_view_change(from, req_view_change, output)
            }
            ValidatedPeerMessage::Usig(usig_message) => {
                self.process_usig_message(usig_message, output)
            }
            ValidatedPeerMessage::Hello(attestation) => {
                self.process_hello_message(from, attestation, output)
            }
        };
        trace!(
            "Successfully processed message (origin: {from:?}, type: {:?}).",
            msg_type
        );
    }
}

impl<P: RequestPayload, U: Usig> output::Reflectable<P, U> for MinBft<P, U>
where
    U::Attestation: Clone,
    U::Signature: Clone + Serialize,
    U::Signature: Debug,
{
    /// Process a reflected message (i.e. own broadcasted messages) of type
    /// PeerMessage.
    ///
    /// # Arguments
    ///
    /// * `peer_message` - The validated [crate::PeerMessage] to process.
    /// * `output` - The output struct to be adjusted in case of, e.g., errors
    ///              or responses.
    /// * `_restricted` - The restricted output.
    fn process_reflected_peer_message(
        &mut self,
        peer_message: ValidatedPeerMessage<U::Attestation, P, U::Signature>,
        output: &mut NotReflectedOutput<P, U>,
        _restricted: OutputRestricted,
    ) {
        self.process_peer_message(self.config.me(), peer_message, output)
    }

    /// Returns the current primary.
    ///
    /// # Arguments
    ///
    /// * `_restricted` - The restricted output.
    fn current_primary(&self, _restricted: OutputRestricted) -> Option<ReplicaId> {
        self.primary()
    }

    /// Returns the View information.
    ///
    /// # Arguments
    ///
    /// * `_restricted` - The restricted output.
    fn view_info(&self, _restricted: OutputRestricted) -> ViewInfo {
        match &self.view_state {
            crate::ViewState::InView(s) => ViewInfo::InView(s.view.0),
            crate::ViewState::ChangeInProgress(s) => ViewInfo::ViewChange {
                from: s.prev_view.0,
                to: s.next_view.0,
            },
        }
    }

    /// Returns the information on the round.
    ///
    /// # Arguments
    ///
    /// * `_restricted` - The restricted output.
    fn round(&self, _restricted: OutputRestricted) -> u64 {
        self.request_processor.round()
    }
}
