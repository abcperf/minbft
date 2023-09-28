use serde::Serialize;
use std::fmt::Debug;
use usig::Usig;

use crate::{
    output::NotReflectedOutput,
    peer_message::usig_message::{
        new_view::{NewView, NewViewCertificate, NewViewContent},
        view_change::ViewChange,
    },
    Error, MinBft, RequestPayload, ViewState,
};

impl<P: RequestPayload, U: Usig> MinBft<P, U>
where
    U::Attestation: Clone,
    U::Signature: Clone + Serialize,
    U::Signature: Debug,
{
    /// Process a message of type ViewChange.
    pub(crate) fn process_view_change(
        &mut self,
        msg: ViewChange<P, U::Signature>,
        output: &mut NotReflectedOutput<P, U>,
    ) {
        match &mut self.view_state {
            ViewState::InView(_) => {}
            ViewState::ChangeInProgress(in_progress) => {
                // Only consider messages consistent to the system state.
                // Automatically fulfilled at this point, as messages of type ViewChange are validated when received.

                if !self.config.me_primary(in_progress.next_view) {
                    return;
                }

                // Only the new primary runs following code.
                let amount_collected = self.collector_vc.collect(msg.clone(), &self.config);

                if msg.next_view != in_progress.next_view || amount_collected <= self.config.t {
                    return;
                }

                let view_changes = self.collector_vc.retrieve(&msg, &self.config);

                if view_changes.is_none() {
                    return;
                }

                let view_changes = view_changes.unwrap();
                let new_view_cert = NewViewCertificate { view_changes };

                // Create the NewView message.
                let next_view = in_progress.next_view;
                let origin = self.config.me();
                let new_view = match NewView::sign(
                    NewViewContent {
                        origin,
                        next_view,
                        certificate: new_view_cert,
                    },
                    &mut self.usig,
                ) {
                    Ok(new_view) => new_view,
                    Err(usig_error) => {
                        let output_error = Error::Usig {
                            replica: origin,
                            msg_type: "NewView",
                            usig_error,
                        };
                        output.error(output_error);
                        return;
                    }
                };
                output.broadcast(new_view, &mut self.sent_usig_msgs);
            }
        }
    }
}
