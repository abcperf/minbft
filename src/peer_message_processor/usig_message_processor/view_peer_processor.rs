use std::fmt::Debug;
use tracing::debug;

use serde::Serialize;
use tracing::error;
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
    /// Process a message of type [Prepare].
    ///
    /// The steps for the processing are as follows:
    ///
    /// 1. If the [ViewState] of the replica is not stable (in a valid View),
    ///    ignore the [Prepare] and return right away.
    /// 2. Assert that the counter of the [Prepare] is not greater than the last
    ///    accepted [Prepare].
    /// 3. If the replica is the primary, no [Commit] is created nor broadcast.
    ///    [Prepare]s count as [Commit]s.
    /// 4. If the replica is not the primary, create and broadcast the
    ///    respective [Commit].
    /// 5. Collect the [Prepare], and accept it if `t` [Commit]s have been
    ///    collected for it - update the counter of the last accepted [Prepare]
    ///    in the inner state.
    /// 6. Create and broadcast a [Checkpoint] if one should be generated (see
    ///    CheckpointGenerator).
    ///
    /// # Arguments
    ///
    /// * `prepare` - The [Prepare] to be processed.
    /// * `output` - The output struct to be adjusted in case of, e.g., errors
    ///              or responses.
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
                    debug!(
                        "Broadcast Commit for Prepare (origin: {:?}, view: {:?}, counter: {:?}) ...",
                        commit.prepare.origin,
                        commit.prepare.view,
                        commit.prepare.counter()
                    );
                    output.broadcast(commit, &mut self.sent_usig_msgs);
                }

                let acceptable_prepares = in_view
                    .collector_commits
                    .collect(ViewPeerMessage::Prepare(prepare), &self.config);
                for acceptable_prepare in acceptable_prepares {
                    let count = acceptable_prepare.counter();
                    if let Some(checkpoint_content) = self.request_processor.accept_prepare(
                        &self.config,
                        acceptable_prepare,
                        self.current_timeout_duration,
                        output,
                    ) {
                        debug!("Generating new Checkpoint since checkpoint period ({:?}) has been reached by accepting Prepare ...", self.config.checkpoint_period);
                        match Checkpoint::sign(checkpoint_content, &mut self.usig) {
                            Ok(checkpoint) => {
                                debug!("Successfully generated new Checkpoint.");
                                debug!("Broadcast Checkpoint (counter of latest accepted Prepare: {:?}, amount accepted batches: {:?}).", checkpoint.counter_latest_prep, checkpoint.total_amount_accepted_batches);
                                output.broadcast(checkpoint, &mut self.sent_usig_msgs);
                            }
                            Err(usig_error) => {
                                error!("Failed to process Prepare: Failed to create and sign Checkpoint in response to accepting Prepare. For further information see output.");
                                let output_error = Error::Usig {
                                    replica: self.config.id,
                                    msg_type: "Prepare",
                                    usig_error,
                                };
                                output.error(output_error);
                                return;
                            }
                        }
                    }
                    self.counter_last_accepted_prep = Some(count);
                }
            }
            ViewState::ChangeInProgress(_) => unreachable!(),
        }
    }

    /// Process a message of type [Commit].
    ///
    /// The steps for the processing are as follows:
    ///
    /// 1. If the [ViewState] of the replica is not stable (in a valid View),
    ///    ignore the [Prepare] and return right away.
    /// 2. If the counter of the [Prepare] in the [Commit] is not greater than
    ///    the last accepted [Prepare], return.
    /// 3. Collect the [Commit], and accept the respective [Prepare] if `t`
    ///    [Commit]s have been collected for it - update the counter of the last
    ///    accepted [Prepare] in the inner state.
    /// 4. Create and broadcast a [Checkpoint] if one should be generated (see
    ///    CheckpointGenerator).
    ///
    /// # Arguments
    ///
    /// * `commit` - The [Commit] to be processed.
    /// * `output` - The output struct to be adjusted in case of, e.g., errors
    ///              or responses.
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
                let acceptable_prepares = in_view
                    .collector_commits
                    .collect(ViewPeerMessage::Commit(commit), &self.config);
                for acceptable_prepare in acceptable_prepares {
                    let count = acceptable_prepare.counter();
                    if let Some(checkpoint_content) = self.request_processor.accept_prepare(
                        &self.config,
                        acceptable_prepare,
                        self.current_timeout_duration,
                        output,
                    ) {
                        debug!("Generating new Checkpoint since checkpoint period ({:?}) has been reached by accepting Prepare ...", self.config.checkpoint_period);
                        match Checkpoint::sign(checkpoint_content, &mut self.usig) {
                            Ok(checkpoint) => {
                                debug!("Successfully generated new Checkpoint.");
                                debug!("Broadcast Checkpoint (counter of latest accepted Prepare: {:?}, amount accepted batches: {:?}).", checkpoint.counter_latest_prep, checkpoint.total_amount_accepted_batches);
                                output.broadcast(checkpoint, &mut self.sent_usig_msgs);
                            }
                            Err(usig_error) => {
                                error!("Failed to process Commit: Failed to create and sign Checkpoint in response to accepting Prepare. For further information see output.");
                                let output_error = Error::Usig {
                                    replica: self.config.id,
                                    msg_type: "Prepare",
                                    usig_error,
                                };
                                output.error(output_error);
                            }
                        }
                    }
                    self.counter_last_accepted_prep = Some(count);
                }
            }
            ViewState::ChangeInProgress(_) => unreachable!(),
        }
    }
}
