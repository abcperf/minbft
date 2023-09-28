pub(crate) mod collector;
mod hello_processor;
mod req_view_change_processor;
mod usig_message_processor;

use serde::Serialize;
use shared_ids::ReplicaId;
use std::fmt::Debug;
use tracing::debug;

use usig::Usig;

use crate::{
    output::{self, NotReflectedOutput, OutputRestricted},
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
    pub(crate) fn process_peer_message(
        &mut self,
        from: ReplicaId,
        message: ValidatedPeerMessage<U::Attestation, P, U::Signature>,
        output: &mut NotReflectedOutput<P, U>,
    ) {
        debug!(
            "process_peer_message from {from:?} of type {:?}",
            message.msg_type()
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
    }
}

impl<P: RequestPayload, U: Usig> output::Reflectable<P, U> for MinBft<P, U>
where
    U::Attestation: Clone,
    U::Signature: Clone + Serialize,
    U::Signature: Debug,
{
    /// Process a reflected message (i.e. own broadcasted messages) of type PeerMessage.
    fn process_reflected_peer_message(
        &mut self,
        peer_message: ValidatedPeerMessage<U::Attestation, P, U::Signature>,
        output: &mut NotReflectedOutput<P, U>,
        _restricted: OutputRestricted,
    ) {
        self.process_peer_message(self.config.me(), peer_message, output)
    }

    fn current_primary(&self, _restricted: OutputRestricted) -> Option<ReplicaId> {
        self.primary()
    }
}
