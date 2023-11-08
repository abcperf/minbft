use std::fmt::Debug;
use tracing::debug;

use serde::Serialize;
use tracing::error;
use tracing::info;
use usig::Counter;
use usig::Usig;

use crate::peer_message::usig_message::checkpoint::Checkpoint;
use crate::peer_message::usig_message::view_peer_message::commit::Commit;
use crate::peer_message::usig_message::view_peer_message::commit::CommitContent;
use crate::peer_message::usig_message::view_peer_message::prepare::Prepare;
use crate::peer_message_processor::usig_message_processor::ViewPeerMessage;
use crate::Error;
use crate::MinBft;
use crate::{output::NotReflectedOutput, RequestPayload, ViewState};

impl<P: RequestPayload, U: Usig> MinBft<P, U>
where
    U::Attestation: Clone,
    U::Signature: Clone + Serialize,
    U::Signature: Debug,
{
    /// Process messages of type [Prepare].
    pub(crate) fn process_prepare(
        &mut self,
        prepare: Prepare<P, U::Signature>,
        output: &mut NotReflectedOutput<P, U>,
    ) {
        match &mut self.view_state {
            ViewState::InView(in_view) => {
                assert!(Some(prepare.counter()) > self.counter_last_accepted_prep, "Failed to process Prepare (origin: {:?}, view: {:?}, counter: {:?}): Counter of Prepare is less than or equal to counter of last accepted Prepare ({:?}).", prepare.origin, prepare.view, prepare.counter(), self.counter_last_accepted_prep);
                // no Commit for own Prepare since Prepares already count as Commit
                if prepare.origin != self.config.me() {
                    let origin = self.config.me();
                    debug!(
                        "Creating Commit for Prepare (origin: {:?}, view: {:?}, counter: {:?}) ...",
                        prepare.origin,
                        prepare.view,
                        prepare.counter()
                    );
                    let commit = match Commit::sign(
                        CommitContent {
                            origin,
                            prepare: prepare.clone(),
                        },
                        &mut self.usig,
                    ) {
                        Ok(commit) => {
                            debug!("Successfully created Commit for Prepare (origin: {:?}, view: {:?}, counter: {:?}) ...", prepare.origin, prepare.view, prepare.counter());
                            commit
                        }
                        Err(usig_error) => {
                            error!("Failed to process Prepare (origin: {:?}, view: {:?}, counter: {:?}): Failed to create and sign Commit in response to Prepare. For further information see output.", prepare.origin, prepare.view, prepare.counter());
                            let output_error = Error::Usig {
                                replica: origin,
                                msg_type: "Prepare",
                                usig_error,
                            };
                            output.error(output_error);
                            return;
                        }
                    };
                    info!(
                        "Broadcast Commit for Prepare (origin: {:?}, view: {:?}, counter: {:?}) ...",
                        commit.prepare.origin,
                        commit.prepare.view,
                        commit.prepare.counter()
                    );
                    output.broadcast(commit, &mut self.sent_usig_msgs);
                }

                let amount_collected = in_view
                    .collector_commits
                    .collect(ViewPeerMessage::Prepare(prepare.clone()), &self.config);
                if amount_collected <= self.config.t {
                    debug!("Processing Prepare (origin: {:?}, view: {:?}, counter: {:?}) resulted in not accepting it yet: A sufficient amount of Commits has not been collected yet (collected: {:?}, required: {:?}).", prepare.origin, prepare.view, prepare.counter(), amount_collected, self.config.t + 1);
                    return;
                }
                in_view.collector_commits.clean_up(prepare.counter());
                if let Some(checkpoint_content) = self.request_processor.accept_prepare(
                    &self.config,
                    prepare.clone(),
                    self.current_timeout_duration,
                    output,
                ) {
                    debug!("Generating new Checkpoint since checkpoint period ({:?}) has been reached by accepting Prepare ...", self.config.checkpoint_period);
                    let checkpoint = match Checkpoint::sign(checkpoint_content, &mut self.usig) {
                        Ok(checkpoint) => {
                            debug!("Successfully generated new Checkpoint.");
                            checkpoint
                        }
                        Err(usig_error) => {
                            error!("Failed to process Prepare (origin: {:?}, view: {:?}, counter: {:?}): Failed to create and sign Checkpoint in response to accepting Prepare. For further information see output.", prepare.origin, prepare.view, prepare.counter());
                            let output_error = Error::Usig {
                                replica: self.config.id,
                                msg_type: "Prepare",
                                usig_error,
                            };
                            output.error(output_error);
                            return;
                        }
                    };
                    info!("Broadcast Checkpoint (counter of latest accepted Prepare: {:?}, amount accepted batches: {:?}).", checkpoint.counter_latest_prep, checkpoint.total_amount_accepted_batches);
                    output.broadcast(checkpoint, &mut self.sent_usig_msgs);
                }
                self.counter_last_accepted_prep = Some(prepare.counter());
            }
            ViewState::ChangeInProgress(_) => unreachable!(),
        }
    }

    /// Process a message of type [Commit].
    pub(crate) fn process_commit(
        &mut self,
        commit: Commit<P, U::Signature>,
        output: &mut NotReflectedOutput<P, U>,
    ) {
        match &mut self.view_state {
            ViewState::InView(in_view) => {
                if Some(commit.prepare.counter()) <= self.counter_last_accepted_prep {
                    return;
                }
                assert!(Some(commit.prepare.counter()) > self.counter_last_accepted_prep);
                let amount_collected = in_view
                    .collector_commits
                    .collect(ViewPeerMessage::Commit(commit.clone()), &self.config);
                if amount_collected <= self.config.t {
                    debug!("Processing Commit (origin: {:?}, counter: {:?}, Prepare: [origin: {:?}, view: {:?}, counter: {:?}]) resulted in not accepting it yet: A sufficient amount of Commits has not been collected yet (collected: {:?}, required: {:?}).", commit.origin, commit.counter(), commit.prepare.origin, commit.prepare.view, commit.prepare.counter(), amount_collected, self.config.t + 1);
                    return;
                }
                in_view.collector_commits.clean_up(commit.prepare.counter());
                if let Some(checkpoint_content) = self.request_processor.accept_prepare(
                    &self.config,
                    commit.prepare.clone(),
                    self.current_timeout_duration,
                    output,
                ) {
                    debug!("Generating new Checkpoint since checkpoint period ({:?}) has been reached by accepting Prepare ...", self.config.checkpoint_period);
                    let checkpoint = match Checkpoint::sign(checkpoint_content, &mut self.usig) {
                        Ok(checkpoint) => {
                            debug!("Successfully generated new Checkpoint.");
                            checkpoint
                        }
                        Err(usig_error) => {
                            error!("Failed to process Commit (origin: {:?}, counter: {:?}, Prepare: [origin: {:?}, view: {:?}, counter: {:?}]): Failed to create and sign Checkpoint in response to accepting Prepare. For further information see output.", commit.origin, commit.counter(), commit.prepare.origin, commit.prepare.view, commit.prepare.counter());
                            let output_error = Error::Usig {
                                replica: self.config.id,
                                msg_type: "Prepare",
                                usig_error,
                            };
                            output.error(output_error);
                            return;
                        }
                    };
                    info!("Broadcast Checkpoint (counter of latest accepted Prepare: {:?}, amount accepted batches: {:?}).", checkpoint.counter_latest_prep, checkpoint.total_amount_accepted_batches);
                    output.broadcast(checkpoint, &mut self.sent_usig_msgs);
                }
                self.counter_last_accepted_prep = Some(commit.prepare.counter());
            }
            ViewState::ChangeInProgress(_) => unreachable!(),
        }
    }
}
