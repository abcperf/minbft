use serde::Serialize;
use shared_ids::ReplicaId;
use std::fmt::Debug;
use usig::Usig;

use crate::{
    output::{NotReflectedOutput, TimeoutRequest},
    peer_message::{
        req_view_change::ReqViewChange,
        usig_message::view_change::{ViewChange, ViewChangeContent},
    },
    ChangeInProgress, Error, MinBft, RequestPayload, ViewState, BACKOFF_MULTIPLIER,
};

impl<P: RequestPayload, U: Usig> MinBft<P, U>
where
    U::Attestation: Clone,
    U::Signature: Clone + Serialize,
    U::Signature: Debug,
{
    /// Process a message of type [ReqViewChange].
    pub(crate) fn process_req_view_change(
        &mut self,
        from: ReplicaId,
        req: ReqViewChange,
        output: &mut NotReflectedOutput<P, U>,
    ) {
        let (amount_collected, to_become_prev_view) = match &mut self.view_state {
            ViewState::InView(in_view) => {
                if req.prev_view < in_view.view {
                    return;
                };
                let amount_collected = self.collector_rvc.collect(req.clone(), from, &self.config);
                let to_become_prev_view = in_view.view;
                (amount_collected, to_become_prev_view)
            }
            ViewState::ChangeInProgress(in_progress) => {
                if req.prev_view < in_progress.prev_view
                    || req.prev_view == in_progress.prev_view
                        && req.next_view < in_progress.next_view
                {
                    return;
                }
                let amount_collected = self.collector_rvc.collect(req.clone(), from, &self.config);
                let to_become_prev_view = in_progress.prev_view;
                (amount_collected, to_become_prev_view)
            }
        };
        if req.prev_view != to_become_prev_view || amount_collected <= self.config.t {
            return;
        }

        match &self.view_state {
            ViewState::InView(_) => {}
            ViewState::ChangeInProgress(in_progress) => {
                if in_progress.has_broadcast_view_change {
                    return;
                }
            }
        }

        output.timeout_request(TimeoutRequest::new_stop_vc_req());
        self.current_timeout_duration *= BACKOFF_MULTIPLIER as u32;
        let start_new_timeout = TimeoutRequest::new_start_vc_req(self.current_timeout_duration);

        self.view_state = ViewState::ChangeInProgress(ChangeInProgress {
            prev_view: req.prev_view,
            next_view: req.next_view,
            has_broadcast_view_change: true,
        });

        let origin = self.config.me();
        let view_change = match ViewChange::sign(
            ViewChangeContent::new(
                self.config.me(),
                req.next_view,
                self.last_checkpoint_cert.clone(),
                self.sent_usig_msgs.iter(),
            ),
            &mut self.usig,
        ) {
            Ok(view_change) => view_change,
            Err(usig_error) => {
                let output_error = Error::Usig {
                    replica: origin,
                    msg_type: "ViewChange",
                    usig_error,
                };
                output.error(output_error);
                return;
            }
        };
        output.broadcast(view_change, &mut self.sent_usig_msgs);
        output.timeout_request(start_new_timeout);
    }
}
