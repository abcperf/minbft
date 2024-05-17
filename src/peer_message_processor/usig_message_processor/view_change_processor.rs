use serde::Serialize;
use std::fmt::Debug;
use tracing::{debug, error, info, trace};
use usig::Usig;

use crate::{
    output::{NotReflectedOutput, TimeoutRequest},
    peer_message::usig_message::{
        new_view::{NewView, NewViewCertificate, NewViewContent},
        view_change::ViewChange,
    },
    ChangeInProgress, Error, MinBft, RequestPayload, ViewState,
};

impl<P: RequestPayload, U: Usig> MinBft<P, U>
where
    U::Attestation: Clone,
    U::Signature: Clone + Serialize,
    U::Signature: Debug,
{
    /// Process a message of type ViewChange.
    ///
    /// Steps for processing the message:
    ///
    /// 1. If the [ViewState] of the replica is not in the state of changing
    ///    views, ignore the message and return.
    /// 2. If the replica is not the next primary, skip the processing, too.
    /// 3. Collect the [ViewChange].
    /// 4. If `t + 1` [ViewChange]s with the same next View and from different
    ///    origins (see the collector for details) have not been collected yet,
    ///    return.
    /// 5. If the next View set in the [ViewChange] does not correspond to
    ///    the inner [ViewState] of the replica, ignore it (for now).
    /// 6. Create the [NewViewCertificate] and the [NewView] message.
    /// 7. Broadcast the [NewView].
    ///
    /// # Arguments
    ///
    /// * `msg` - The [ViewChange] to process.
    /// * `output` - The output struct to be adjusted in case of, e.g., errors
    ///              or responses.
    pub(crate) fn process_view_change(
        &mut self,
        msg: ViewChange<P, U::Signature>,
        output: &mut NotReflectedOutput<P, U>,
    ) {
        // Only consider messages consistent to the system state.
        // Automatically fulfilled at this point, as messages of type ViewChange are validated when received.
        match &mut self.view_state {
            ViewState::InView(in_view) => {
                if msg.next_view <= in_view.view {
                    debug!("Received ViewChange message to change to View less than or equal to the current View set (request to change to: {}, currently stable in View: {}", msg.next_view, in_view.view);
                    return;
                }
                let amount_collected = self.collector_vc.collect_view_change(msg.clone());

                if amount_collected <= self.config.t {
                    trace!("Processing ViewChange (origin: {:?}, next view: {:?}) resulted in ignoring creation of NewView: A sufficient amount of ViewChanges has not been collected yet (collected: {:?}, required: {:?}).", msg.origin, msg.next_view, amount_collected, self.config.t + 1);
                    return;
                }

                self.view_state = ViewState::ChangeInProgress(ChangeInProgress {
                    prev_view: in_view.view,
                    next_view: msg.next_view,
                    has_broadcast_view_change: false,
                });

                let _ = self
                    .collector_vc
                    .retrieve_collected_view_changes(&msg, &self.config);

                let start_new_timeout =
                    TimeoutRequest::new_start_view_change(self.current_timeout_duration);
                output.timeout_request(start_new_timeout);
            }

            ViewState::ChangeInProgress(in_progress) => {
                let amount_collected = self.collector_vc.collect_view_change(msg.clone());

                if msg.next_view < in_progress.next_view {
                    debug!("Processing ViewChange (origin: {:?}, next view: {:?}) resulted in ignoring creation of NewView: Next view set in message is smaller than the expected (to become) next view ({}).", msg.origin, msg.next_view, in_progress.next_view);
                    return;
                }

                if amount_collected <= self.config.t {
                    trace!("Processing ViewChange (origin: {:?}, next view: {:?}) resulted in ignoring creation of NewView: A sufficient amount of ViewChanges has not been collected yet (collected: {:?}, required: {:?}).", msg.origin, msg.next_view, amount_collected, self.config.t + 1);
                    return;
                }

                let view_changes = self
                    .collector_vc
                    .retrieve_collected_view_changes(&msg, &self.config);

                if !self.config.me_primary(in_progress.next_view) {
                    let start_new_timeout =
                        TimeoutRequest::new_start_view_change(self.current_timeout_duration);
                    output.timeout_request(start_new_timeout);
                    return;
                }

                // Only the new primary runs following code.
                assert!(view_changes.is_some());

                let view_changes = view_changes.unwrap();
                let new_view_cert = NewViewCertificate { view_changes };

                // Create the NewView message.
                let next_view = in_progress.next_view;
                let origin = self.config.me();
                trace!(
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
                        trace!(
                            "Successfully created NewView for ViewChanges (next view: {:?}).",
                            msg.next_view
                        );
                        new_view
                    }
                    Err(usig_error) => {
                        error!("Failed to create NewView for ViewChanges (next view: {:?}): Signing NewView failed. For further information see output.", msg.next_view);
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
