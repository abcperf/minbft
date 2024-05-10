use serde::Serialize;
use shared_ids::ReplicaId;
use std::fmt::Debug;
use tracing::{debug, info, warn};
use usig::Usig;

use crate::{
    output::{NotReflectedOutput, TimeoutRequest},
    peer_message::{
        req_view_change::ReqViewChange,
        usig_message::view_change::{ViewChange, ViewChangeContent},
    },
    ChangeInProgress, Error, MinBft, RequestPayload, ViewState,
};

impl<P: RequestPayload, U: Usig> MinBft<P, U>
where
    U::Attestation: Clone,
    U::Signature: Clone + Serialize,
    U::Signature: Debug,
{
    /// Process a message of type [ReqViewChange].
    ///
    /// The steps are as follows:
    ///
    /// 1. Collect the [ReqViewChange] if it is relevant for the replica
    ///    regarding its inner state.
    ///     1.1. If the replica's state is [ViewState::InView], the
    ///          [ReqViewChange] is ignored if the previous [crate::View] set in
    ///          the message is smaller than the [crate::View] set in the inner
    ///          state of the replica.
    ///     1.2. If the replica's state is [ViewState::ChangeInProgress], the
    ///          [ReqViewChange] is ignored if the previous [crate::View] set in
    ///          the message is smaller than the previous [crate::View] set in
    ///          the inner state of the replica OR if they are equal but the
    ///          next [crate::View] is greater in the inner state.
    /// 2. If the previous [crate::View] in the [ReqViewChange] is not the same
    ///    as the one set in the inner state, return.
    /// 3. If `t + 1` [ReqViewChange]s with the same previous and next
    ///    [crate::View] have not been collected yet, return.
    /// 4. If the replica has already previously broadcast a [ReqViewChange]
    ///    return.
    /// 5. Stop the current timeout for the view-change, and start a new one.
    /// 6. Update the inner state of the replica.
    /// 7. Stop any client request.
    /// 8. Create a [ViewChange] and broadcast it.
    ///
    /// # Arguments
    ///
    /// * `from` - The ID of the replica from which the [ReqViewChange].
    /// * `req` - The [ReqViewChange] to be processed.
    /// * `output` - The output struct to be adjusted in case of, e.g., errors
    ///              or responses.
    pub(crate) fn process_req_view_change(
        &mut self,
        from: ReplicaId,
        req: ReqViewChange,
        output: &mut NotReflectedOutput<P, U>,
    ) {
        let (amount_collected, to_become_prev_view) = match &mut self.view_state {
            ViewState::InView(in_view) => {
                if req.prev_view < in_view.view {
                    warn!("Processing ReqViewChange (origin: {from:?}, previous view: {:?}, next view: {:?}) resulted in ignoring it (whilst in view): Previous view set in message is smaller than current view.", req.prev_view, req.next_view);
                    return;
                };
                let amount_collected = self.collector_rvc.collect(&req, from);
                let to_become_prev_view = in_view.view;
                (amount_collected, to_become_prev_view)
            }
            ViewState::ChangeInProgress(in_progress) => {
                if req.prev_view < in_progress.prev_view
                    || req.prev_view == in_progress.prev_view
                        && req.next_view < in_progress.next_view
                {
                    warn!("Processing ReqViewChange (origin: {from:?}, previous view: {:?}, next view: {:?}) (whilst in view-change): Previous view set in message is smaller than previous view set in state or they are equal but next view in message is smaller than next view set in state.", req.prev_view, req.next_view);
                    return;
                }
                let amount_collected = self.collector_rvc.collect(&req, from);
                let to_become_prev_view = in_progress.prev_view;
                (amount_collected, to_become_prev_view)
            }
        };
        if req.prev_view != to_become_prev_view {
            debug!("Processing ReqViewChange (origin: {from:?}, previous view: {:?}, next view: {:?}) resulted in ignoring creation of ViewChange: Previous view set in message is not the same as the current (to become) previous view.", req.prev_view, req.next_view);
            return;
        }
        if amount_collected <= self.config.t {
            debug!("Processing ReqViewChange (origin: {from:?}, previous view: {:?}, next view: {:?}) resulted in ignoring creation of ViewChange: A sufficient amount of ReqViewChanges has not been collected yet (collected: {:?}, required: {:?}).", req.prev_view, req.next_view, amount_collected, self.config.t + 1);
            return;
        }

        match &self.view_state {
            ViewState::InView(_) => {}
            ViewState::ChangeInProgress(in_progress) => {
                if in_progress.has_broadcast_view_change {
                    debug!("Processing ReqViewChange (origin: {from:?}, previous view: {:?}, next view: {:?}) resulted in ignoring creation of ViewChange: ViewChange has already been broadcast for this view.", req.prev_view, req.next_view);
                    return;
                }
            }
        }

        self.view_state = ViewState::ChangeInProgress(ChangeInProgress {
            prev_view: req.prev_view,
            next_view: req.next_view,
            has_broadcast_view_change: true,
        });

        output.timeout_request(TimeoutRequest::new_stop_any_client_req());

        let origin = self.config.me();
        debug!(
            "Creating ViewChange for ReqViewChanges (previous view: {:?}, next view: {:?}) ...",
            req.prev_view, req.next_view
        );
        let view_change = match ViewChange::sign(
            ViewChangeContent::new(
                self.config.me(),
                req.next_view,
                self.last_checkpoint_cert.clone(),
                self.sent_usig_msgs.iter(),
            ),
            &mut self.usig,
        ) {
            Ok(view_change) => {
                debug!("Successfully created ViewChange for ReqViewChanges (previous view: {:?}, next view: {:?}).", req.prev_view, req.next_view);
                view_change
            }
            Err(usig_error) => {
                debug!("Failed to create ViewChange for ReqViewChanges (previous view: {:?}, next view: {:?}): Signing ViewChange failed. For further information see output.", req.prev_view, req.next_view);
                let output_error = Error::Usig {
                    replica: origin,
                    msg_type: "ViewChange",
                    usig_error,
                };
                output.error(output_error);
                return;
            }
        };
        info!(
            "Broadcast ViewChange (next view: {:?}).",
            view_change.next_view
        );
        output.broadcast(view_change, &mut self.sent_usig_msgs);
    }
}
