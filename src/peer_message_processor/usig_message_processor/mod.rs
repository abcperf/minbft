use std::{borrow::Cow, fmt::Debug};

use serde::Serialize;
use shared_ids::AnyId;
use tracing::debug;
use usig::Counter;
use usig::Usig;

use crate::output::NotReflectedOutput;
use crate::peer_message::usig_message::view_peer_message::ViewPeerMessage;
use crate::peer_message::usig_message::{UsigMessage, UsigMessageV};
use crate::MinBft;
use crate::{RequestPayload, ViewState};

mod checkpoint_processor;
mod new_view_processor;
mod view_change_processor;
mod view_peer_processor;

impl<P: RequestPayload, U: Usig> MinBft<P, U>
where
    U::Attestation: Clone,
    U::Signature: Clone + Serialize,
    U::Signature: Debug,
{
    /// Process a [UsigMessage].
    // At this stage, it is not guaranteed that previous messages of type UsigMessage have already been processed.
    pub(crate) fn process_usig_message(
        &mut self,
        usig_message: UsigMessage<P, U::Signature>,
        output: &mut NotReflectedOutput<P, U>,
    ) {
        // case 1: The UsigMessage contains other messages (i.e. a nested UsigMessage is to be processed).

        // case 1.1: The UsigMessage is a Commit.
        if let UsigMessage::View(ViewPeerMessage::Commit(commit)) = &usig_message {
            debug!(
                "process_usig_message of type {:?} from {:?} with counter {:?} for prepare from {:?}",
                usig_message.msg_type(),
                *usig_message.as_ref(),
                usig_message.counter(),
                commit.prepare.origin
            );
            let usig_prepare = &commit.prepare;
            let from = *usig_prepare.as_ref();
            let replica_state = &mut self.replicas_state[from.as_u64() as usize];
            let messages: Vec<_> = replica_state
                .usig_message_handler_state
                .push_to_handle(Cow::Borrowed(usig_prepare))
                .collect();
            for usig_inner_message in messages {
                self.process_usig_message_ordered(usig_inner_message, output);
            }
        }

        // case 1.2: The UsigMessage is a ViewChange.
        if let UsigMessage::ViewChange(view_change_message) = &usig_message {
            debug!(
                "process_usig_message of type {:?} from {:?} with counter {:?}",
                usig_message.msg_type(),
                *usig_message.as_ref(),
                usig_message.counter(),
            );
            view_change_message
                .variant
                .message_log
                .iter()
                .for_each(|usig_inner_message_of_log| {
                    let from = *usig_inner_message_of_log.as_ref();
                    let replica_state = &mut self.replicas_state[from.as_u64() as usize];
                    let messages: Vec<_> = match usig_inner_message_of_log {
                        UsigMessageV::View(view_peer_message) => replica_state
                            .usig_message_handler_state
                            .push_to_handle(Cow::Borrowed(view_peer_message))
                            .collect(),
                        UsigMessageV::ViewChange(_) => return,
                        UsigMessageV::NewView(new_view_message) => replica_state
                            .usig_message_handler_state
                            .push_to_handle(Cow::Borrowed(new_view_message))
                            .collect(),
                        UsigMessageV::Checkpoint(checkpoint_message) => replica_state
                            .usig_message_handler_state
                            .push_to_handle(Cow::Borrowed(checkpoint_message))
                            .collect(),
                    };
                    for usig_inner_message in messages {
                        self.process_usig_message_ordered(usig_inner_message, output);
                    }
                });
        }

        // case 1.3: The UsigMessage is a NewView.
        if let UsigMessage::NewView(new_view_message) = &usig_message {
            debug!(
                "process_usig_message of type {:?} from {:?}",
                usig_message.msg_type(),
                *usig_message.as_ref()
            );
            // process the other messages of type ViewChange
            new_view_message
                .data
                .certificate
                .view_changes
                .iter()
                .for_each(|usig_inner_view_change_message| {
                    let from = *usig_inner_view_change_message.as_ref();
                    let replica_state = &mut self.replicas_state[from.as_u64() as usize];
                    let messages: Vec<_> = replica_state
                        .usig_message_handler_state
                        .push_to_handle(Cow::Borrowed(usig_inner_view_change_message))
                        .collect();
                    for usig_inner_message in messages {
                        self.process_usig_message_ordered(usig_inner_message, output);
                    }
                });
        }

        // case 2: The UsigMessage does not contain other messages (i.e. a non-nested UsigMessage is to be processed).
        let from = *usig_message.as_ref();
        let replica_state = &mut self.replicas_state[from.as_u64() as usize];
        let messages: Vec<_> = replica_state
            .usig_message_handler_state
            .push_to_handle(Cow::<'_, UsigMessage<P, U::Signature>>::Owned(usig_message))
            .collect();

        for usig_inner_message_non_nested in messages {
            self.process_usig_message_ordered(usig_inner_message_non_nested, output);
        }
    }

    /// Process a [UsigMessage] in an ordered way.
    // Now, it is guaranteed that previous messages of type UsigMessage have already been processed.
    fn process_usig_message_ordered(
        &mut self,
        usig_message: UsigMessage<P, U::Signature>,
        output: &mut NotReflectedOutput<P, U>,
    ) {
        debug!(
            "process_usig_message_ordered from {:?} of type {:?} with counter {:?}",
            *usig_message.as_ref(),
            usig_message.msg_type(),
            usig_message.counter()
        );
        match usig_message {
            UsigMessage::View(view) => match &mut self.view_state {
                ViewState::InView(in_view) => {
                    if view.view() == in_view.view {
                        match view {
                            ViewPeerMessage::Prepare(prepare) => {
                                self.process_prepare(prepare, output)
                            }
                            ViewPeerMessage::Commit(commit) => self.process_commit(commit, output),
                        }
                    }
                }
                ViewState::ChangeInProgress(_) => {}
            },
            UsigMessage::ViewChange(view_change) => self.process_view_change(view_change, output),
            UsigMessage::NewView(new_view) => self.process_new_view(new_view, output),
            UsigMessage::Checkpoint(checkpoint) => self.process_checkpoint(checkpoint),
        }
    }
}
