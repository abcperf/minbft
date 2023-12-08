use serde::Serialize;
use shared_ids::AnyId;
use std::cmp::Reverse;
use std::collections::VecDeque;
use std::fmt::Debug;
use tracing::debug;
use tracing::error;
use tracing::info;
use usig::Count;
use usig::Counter;
use usig::Usig;

use crate::client_request::ClientRequest;
use crate::client_request::RequestBatch;
use crate::output::TimeoutRequest;
use crate::peer_message::usig_message::checkpoint::Checkpoint;
use crate::peer_message::usig_message::new_view::NewView;
use crate::peer_message::usig_message::new_view::NewViewCertificate;
use crate::peer_message::usig_message::view_change::ViewChange;
use crate::peer_message::usig_message::view_change::ViewChangeContent;
use crate::peer_message::usig_message::view_peer_message::prepare::Prepare;
use crate::peer_message::usig_message::view_peer_message::prepare::PrepareContent;
use crate::peer_message::usig_message::view_peer_message::ViewPeerMessage;
use crate::peer_message::usig_message::UsigMessageV;
use crate::peer_message_processor::collector::collector_commits::CollectorCommits;
use crate::Error;
use crate::InView;
use crate::MinBft;
use crate::MinHeap;
use crate::BACKOFF_MULTIPLIER;
use crate::{output::NotReflectedOutput, ChangeInProgress, RequestPayload, ViewState};

type NewViewState<P, Sig> = (Option<Checkpoint<Sig>>, VecDeque<Prepare<P, Sig>>);

impl<P: RequestPayload, U: Usig> MinBft<P, U>
where
    U::Attestation: Clone,
    U::Signature: Clone + Serialize,
    U::Signature: Debug,
{
    /// Process a message of type [NewView].
    pub(crate) fn process_new_view(
        &mut self,
        new_view: NewView<P, U::Signature>,
        output: &mut NotReflectedOutput<P, U>,
    ) {
        match &mut self.view_state {
            ViewState::InView(_) => {}
            ViewState::ChangeInProgress(in_progress) => {
                // Assure the NewViewCertificate is valid.
                match new_view.certificate.validate(&self.config, &mut self.usig) {
                    Ok(_) => {}
                    Err(_) => {
                        output.timeout_request(TimeoutRequest::new_stop_vc_req());
                        let next_view = new_view.next_view + 1;
                        self.current_timeout_duration *= BACKOFF_MULTIPLIER as u32;
                        let start_new_timeout =
                            TimeoutRequest::new_start_vc_req(self.current_timeout_duration);

                        let view_change = match ViewChange::sign(
                            ViewChangeContent::new(
                                self.config.me(),
                                new_view.next_view + 1,
                                self.last_checkpoint_cert.clone(),
                                self.sent_usig_msgs.iter(),
                            ),
                            &mut self.usig,
                        ) {
                            Ok(view_change) => view_change,
                            Err(usig_error) => {
                                let output_error = Error::Usig {
                                    replica: self.config.me(),
                                    msg_type: "ViewChange",
                                    usig_error,
                                };
                                output.error(output_error);
                                return;
                            }
                        };

                        self.view_state = ViewState::ChangeInProgress(ChangeInProgress {
                            prev_view: in_progress.prev_view,
                            next_view,
                            has_broadcast_view_change: false,
                        });
                        info!(
                            "Broadcast ViewChange (next view: {:?}).",
                            view_change.next_view
                        );
                        output.broadcast(view_change, &mut self.sent_usig_msgs);
                        output.timeout_request(start_new_timeout);
                        return;
                    }
                }
                // The NewViewCertificate is valid.

                // Stops the current timeout of type ViewChange.
                output.timeout_request(TimeoutRequest::new_stop_vc_req());

                let (last_cp, mut unique_preps) = Self::compute_new_view_state(
                    self.counter_last_accepted_prep,
                    &new_view.certificate,
                );

                if !unique_preps.is_empty() {
                    let counter_oldest_prep_recv = unique_preps.front().unwrap().counter();
                    let is_transfer_required = match self.counter_last_accepted_prep {
                        Some(counter_last_accepted) => {
                            counter_last_accepted + 1 < counter_oldest_prep_recv
                        }
                        None => counter_oldest_prep_recv != Count(0),
                    };
                    if is_transfer_required {
                        assert!(last_cp.is_some());
                        unimplemented!("State transfer needs to be performed");
                    }
                }

                debug!("Accepting unique Prepares contained in NewViewCertificate ...");
                while !unique_preps.is_empty() {
                    let unique_prep = unique_preps.pop_front().unwrap();
                    let from = unique_prep.origin;
                    let counter = unique_prep.counter();
                    self.request_processor.accept_prepare(
                        &self.config,
                        unique_prep,
                        self.current_timeout_duration,
                        output,
                    );
                    // Update the last seen counter in the replica state of the
                    // origin of the prepare.
                    let replica_state = &mut self.replicas_state[from.as_u64() as usize];
                    replica_state
                        .usig_message_order_enforcer
                        .update_in_new_view(counter);
                }
                debug!("Accepted unique Prepares contained in NewViewCertificate.");

                // Clean up the collection of ReqViewChanges.
                self.collector_rvc
                    .clean_up(new_view.next_view, new_view.next_view);

                self.view_state = ViewState::InView(InView {
                    view: new_view.next_view,
                    has_requested_view_change: false,
                    collector_commits: CollectorCommits::new(self.config.t),
                });

                output.timeout_request(TimeoutRequest::new_stop_any_client_req());
                if let Some((_, req)) = self.request_processor.currently_processing_all().next() {
                    output.timeout_request(TimeoutRequest::new_start_client_req(
                        req.client,
                        self.current_timeout_duration,
                    ));
                }

                if !self.config.me_primary(new_view.next_view) {
                    // Set the counter of the last accepted Prepare temporarily
                    // as the counter of the last sent UsigMessage by the new View.
                    // This makes sure all replicas are synced correctly upon changing views.
                    self.counter_last_accepted_prep = Some(new_view.counter());

                    info!(
                        "Successfully transitioned to next view ({:?})",
                        new_view.next_view
                    );

                    // Relay the NewView message.
                    debug!(
                        "Relayed NewView (origin: {:?}, next view: {:?}).",
                        new_view.origin, new_view.next_view
                    );
                    output.broadcast(new_view, &mut Vec::new());
                } else {
                    let mut requests_to_batch: Vec<ClientRequest<P>> = Vec::new();

                    // After the new View sends and receives the message of type NewView,
                    // it sends Prepares for the client requests that had not yet been accepted by the previous View.
                    for (_, req) in self.request_processor.currently_processing_all() {
                        if !requests_to_batch.iter().any(|e| e.id() == req.id()) {
                            requests_to_batch.push(req.clone());
                        }
                    }

                    // Send Prepares in a batch.
                    debug!("Creating Prepare for client requests that have yet to be accepted ...");
                    let origin = self.config.me();
                    if !requests_to_batch.is_empty() {
                        match Prepare::sign(
                            PrepareContent {
                                view: new_view.next_view,
                                origin,
                                request_batch: RequestBatch {
                                    batch: requests_to_batch.into_boxed_slice(),
                                },
                            },
                            &mut self.usig,
                        ) {
                            Ok(prepare) => {
                                debug!("Successfully created Prepare for client requests that have yet to be accepted.");
                                debug!("Broadcast Prepare for client requests that have yet to be accepted.");
                                output.broadcast(prepare, &mut self.sent_usig_msgs);
                                info!(
                                    "Successfully transitioned to new view ({:?}).",
                                    new_view.next_view
                                );
                            }
                            Err(usig_error) => {
                                error!("Failed to create Prepare for client requests that have yet to be accepted. For further information see output.");
                                let output_error = Error::Usig {
                                    replica: origin,
                                    msg_type: "Prepare",
                                    usig_error,
                                };
                                output.error(output_error);
                            }
                        };
                    }

                    // Set the counter of the last accepted Prepare temporarily
                    // as the counter of the last sent UsigMessage by the new View.
                    // This makes sure all replicas are synced correctly upon changing views.
                    debug!(
                        "Set counter of last accepted Prepare to counter of NewView ({:?}).",
                        new_view.counter()
                    );
                    self.counter_last_accepted_prep = Some(new_view.counter());
                }
            }
        }
    }

    /// Computes the new [crate::View] state.
    /// That is to say, it returns the latest [Checkpoint] and the unique [Prepare]s contained in the [NewViewCertificate].
    /// Only the [Prepare]s with a counter higher than the given counter are gathered.
    fn compute_new_view_state(
        counter_last_accepted_prep: Option<Count>,
        new_view_cert: &NewViewCertificate<P, U::Signature>,
    ) -> NewViewState<P, U::Signature> {
        let mut preps: MinHeap<Prepare<P, U::Signature>> = MinHeap::default();
        let mut last_cp: Option<Checkpoint<U::Signature>> = None;

        for view_change in &new_view_cert.view_changes {
            for m in &view_change.variant.message_log {
                match m {
                    UsigMessageV::View(view) => match view {
                        ViewPeerMessage::Prepare(prepare) => {
                            if Some(prepare.counter()) > counter_last_accepted_prep {
                                preps.push(Reverse(prepare.clone()));
                            };
                        }
                        ViewPeerMessage::Commit(commit) => {
                            if Some(commit.prepare.counter()) > counter_last_accepted_prep {
                                preps.push(Reverse(commit.prepare.clone()))
                            }
                        }
                    },
                    UsigMessageV::ViewChange(_) => {}
                    UsigMessageV::NewView(_) => {}
                    UsigMessageV::Checkpoint(checkpoint) => match &last_cp {
                        Some(latest) => {
                            if latest.counter_latest_prep < checkpoint.counter_latest_prep {
                                last_cp = Some(checkpoint.clone());
                            }
                        }
                        None => {
                            last_cp = Some(checkpoint.clone());
                        }
                    },
                }
            }
        }

        let mut unique_preps: VecDeque<Prepare<P, U::Signature>> = VecDeque::new();
        while let Some(min_prep) = preps.pop() {
            if unique_preps.back().is_none()
                || unique_preps.back().unwrap().counter() != min_prep.0.counter()
            {
                unique_preps.push_back(min_prep.0);
            }
        }
        (last_cp, unique_preps)
    }
}
