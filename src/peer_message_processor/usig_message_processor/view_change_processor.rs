use serde::Serialize;
use std::fmt::Debug;
use tracing::{debug, info};
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
            ViewState::InView(in_view) => {
                debug!("Processing ViewChange (origin: {:?}, next view: {:?}) resulted in ignoring it: Replica is in view ({:?}).", msg.origin, msg.next_view, in_view.view);
            }
            ViewState::ChangeInProgress(in_progress) => {
                // Only consider messages consistent to the system state.
                // Automatically fulfilled at this point, as messages of type ViewChange are validated when received.

                if !self.config.me_primary(in_progress.next_view) {
                    return;
                }

                // Only the new primary runs following code.
                let amount_collected = self.collector_vc.collect_view_change(msg.clone());

                if msg.next_view != in_progress.next_view {
                    debug!("Processing ViewChange (origin: {:?}, next view: {:?}) resulted in ignoring creation of NewView: Next view set in message is not the same as the current (to become) next view.", msg.origin, msg.next_view);
                    return;
                }

                if amount_collected <= self.config.t {
                    debug!("Processing ViewChange (origin: {:?}, next view: {:?}) resulted in ignoring creation of NewView: A sufficient amount of ViewChanges has not been collected yet (collected: {:?}, required: {:?}).", msg.origin, msg.next_view, amount_collected, self.config.t + 1);
                    return;
                }

                let view_changes = self
                    .collector_vc
                    .retrieve_collected_view_changes(&msg, &self.config);

                if view_changes.is_none() {
                    return;
                }

                let view_changes = view_changes.unwrap();
                let new_view_cert = NewViewCertificate { view_changes };

                // Create the NewView message.
                let next_view = in_progress.next_view;
                let origin = self.config.me();
                debug!(
                    "Creating NewView for ViewChanges (next view: {:?}) ...",
                    msg.next_view
                );
                let new_view = match NewView::sign(
                    NewViewContent {
                        origin,
                        next_view,
                        certificate: new_view_cert,
                    },
                    &mut self.usig,
                ) {
                    Ok(new_view) => {
                        debug!(
                            "Successfully created NewView for ViewChanges (next view: {:?}).",
                            msg.next_view
                        );
                        new_view
                    }
                    Err(usig_error) => {
                        debug!("Failed to create NewView for ViewChanges (next view: {:?}): Signing NewView failed. For further information see output.", msg.next_view);
                        let output_error = Error::Usig {
                            replica: origin,
                            msg_type: "NewView",
                            usig_error,
                        };
                        output.error(output_error);
                        return;
                    }
                };

                info!("Broadcast NewView (next view: {:?}).", new_view.next_view);
                output.broadcast(new_view, &mut self.sent_usig_msgs);
            }
        }
    }
}
