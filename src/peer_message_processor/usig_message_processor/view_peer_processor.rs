use std::fmt::Debug;
use tracing::debug;

use serde::Serialize;
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
                // no Commit for own Prepare since Prepares already count as Commit
                if prepare.origin != self.config.me() {
                    let origin = self.config.me();
                    let commit = match Commit::sign(
                        CommitContent {
                            origin,
                            prepare: prepare.clone(),
                        },
                        &mut self.usig,
                    ) {
                        Ok(commit) => commit,
                        Err(usig_error) => {
                            let output_error = Error::Usig {
                                replica: origin,
                                msg_type: "Prepare",
                                usig_error,
                            };
                            output.error(output_error);
                            return;
                        }
                    };
                    output.broadcast(commit, &mut self.sent_usig_msgs);
                }
                if Some(prepare.counter()) <= self.counter_last_accepted_prep {
                    panic!();
                }
                let amount_collected = in_view
                    .collector_commits
                    .collect(ViewPeerMessage::Prepare(prepare.clone()), &self.config);
                if amount_collected <= self.config.t {
                    return;
                }
                in_view.collector_commits.clean_up(prepare.counter());
                if let Some(checkpoint_content) = self.request_processor.accept_prepare(
                    &self.config,
                    prepare.clone(),
                    self.current_timeout_duration,
                    output,
                ) {
                    let checkpoint = Checkpoint::sign(checkpoint_content, &mut self.usig).unwrap();
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
        debug!(
            "process_commit with counter {:?} from {:?} for prepare with counter {:?}",
            commit.counter(),
            commit.origin,
            commit.prepare.counter()
        );
        match &mut self.view_state {
            ViewState::InView(in_view) => {
                if Some(commit.prepare.counter()) <= self.counter_last_accepted_prep {
                    return;
                }
                let amount_collected = in_view
                    .collector_commits
                    .collect(ViewPeerMessage::Commit(commit.clone()), &self.config);
                if amount_collected <= self.config.t {
                    return;
                }
                in_view.collector_commits.clean_up(commit.prepare.counter());
                if let Some(checkpoint_content) = self.request_processor.accept_prepare(
                    &self.config,
                    commit.prepare.clone(),
                    self.current_timeout_duration,
                    output,
                ) {
                    let checkpoint = Checkpoint::sign(checkpoint_content, &mut self.usig).unwrap();
                    output.broadcast(checkpoint, &mut self.sent_usig_msgs);
                }
                self.counter_last_accepted_prep = Some(commit.prepare.counter());
            }
            ViewState::ChangeInProgress(_) => unreachable!(),
        }
    }
}
