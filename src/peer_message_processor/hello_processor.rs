use serde::Serialize;
use shared_ids::ReplicaId;
use std::fmt::Debug;
use usig::Usig;

use crate::{output::NotReflectedOutput, Error, MinBft, RequestPayload};

impl<P: RequestPayload, U: Usig> MinBft<P, U>
where
    U::Attestation: Clone,
    U::Signature: Clone + Serialize,
    U::Signature: Debug,
{
    /// Process a message of type Hello.
    pub(crate) fn process_hello_message(
        &mut self,
        from: ReplicaId,
        attestation: U::Attestation,
        output: &mut NotReflectedOutput<P, U>,
    ) {
        if self.usig.add_remote_party(from, attestation) {
            if self.recv_hellos.insert(from) && self.recv_hellos.len() as u64 == self.config.n.get()
            {
                output.ready_for_client_requests()
            }
        } else {
            let output_error = Error::Attestation {
                receiver: self.config.id,
                origin: from,
            };
            output.error(output_error);
        }
    }
}
