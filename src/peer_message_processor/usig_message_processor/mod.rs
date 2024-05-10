use std::{borrow::Cow, fmt::Debug};

use serde::Serialize;
use tracing::trace;
use tracing::warn;
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
    /// At this stage, it is not guaranteed that previous messages of type
    /// UsigMessage have already been processed.
    /// This is ensured with a proceeding call.
    ///
    /// The steps are as follows:
    ///
    /// 1. If the [UsigMessage] is a nested one, process the inner messages
    /// first.
    /// 2. If it is not a nested message, use the UsigMsgOrderEnforcer to
    /// process the messages in order, i.e., from lowest USIG counter to highest
    /// and without a hole.
    ///
    /// # Arguments
    ///
    /// * `usig_message` - The UsigMessage to be processed.
    /// * `output` - The output struct to be adjusted in case of, e.g., errors
    ///              or responses.
    pub(crate) fn process_usig_message(
        &mut self,
        usig_message: UsigMessage<P, U::Signature>,
        output: &mut NotReflectedOutput<P, U>,
    ) {
        // case 1: The UsigMessage contains other messages (i.e. a nested UsigMessage is to be processed).

        // case 1.1: The UsigMessage is a Commit.
        if let UsigMessage::View(ViewPeerMessage::Commit(commit)) = &usig_message {
            trace!(
                "Processing inner message Prepare (origin: {:?}, view: {:?}, counter {:?}) by first passing it to the order enforcer ... (outer message is Commit [origin: {:?}, counter: {:?}])",
                commit.prepare.origin,
                commit.prepare.view,
                commit.prepare.counter(),
                commit.origin,
                commit.counter(),
            );
            let usig_prepare = &commit.prepare;
            let from = *usig_prepare.as_ref();
            let replica_state = &mut self.replicas_state[from.as_u64() as usize];
            let messages: Vec<_> = replica_state
                .usig_message_order_enforcer
                .push_to_handle(Cow::Borrowed(usig_prepare))
                .collect();
            for usig_inner_message in messages {
                self.process_usig_message_ordered(usig_inner_message, output);
            }
        }

        // case 1.2: The UsigMessage is a ViewChange.
        if let UsigMessage::ViewChange(view_change) = &usig_message {
            trace!("Processing inner messages of outer message ViewChange (origin: {:?}, next view: {:?}, counter: {:?}) by first passing them to the order enforcer ...", view_change.origin, view_change.next_view, view_change.counter());
            view_change
                .variant
                .message_log
                .iter()
                .for_each(|usig_inner_message_of_log| {
                    let from = *usig_inner_message_of_log.as_ref();
                    let replica_state = &mut self.replicas_state[from.as_u64() as usize];
                    let messages: Vec<_> = match usig_inner_message_of_log {
                        UsigMessageV::View(view_peer_message) => replica_state
                            .usig_message_order_enforcer
                            .push_to_handle(Cow::Borrowed(view_peer_message))
                            .collect(),
                        UsigMessageV::ViewChange(_) => return,
                        UsigMessageV::NewView(new_view_message) => replica_state
                            .usig_message_order_enforcer
                            .push_to_handle(Cow::Borrowed(new_view_message))
                            .collect(),
                        UsigMessageV::Checkpoint(checkpoint_message) => replica_state
                            .usig_message_order_enforcer
                            .push_to_handle(Cow::Borrowed(checkpoint_message))
                            .collect(),
                    };
                    for usig_inner_message in messages {
                        self.process_usig_message_ordered(usig_inner_message, output);
                    }
                });
        }

        // case 1.3: The UsigMessage is a NewView.
        if let UsigMessage::NewView(new_view) = &usig_message {
            trace!("Processing inner messages of outer message NewView (origin: {:?}, next view: {:?}, counter: {:?}) by first passing them to the order enforcer ...", new_view.origin, new_view.next_view, new_view.counter());
            // process the other messages of type ViewChange
            new_view.data.certificate.view_changes.iter().for_each(
                |usig_inner_view_change_message| {
                    let from = *usig_inner_view_change_message.as_ref();
                    let replica_state = &mut self.replicas_state[from.as_u64() as usize];
                    let messages: Vec<_> = replica_state
                        .usig_message_order_enforcer
                        .push_to_handle(Cow::Borrowed(usig_inner_view_change_message))
                        .collect();
                    for usig_inner_message in messages {
                        self.process_usig_message_ordered(usig_inner_message, output);
                    }
                },
            );
        }

        trace!(
            "Process message (origin: {:?}, type: {:?}, counter: {:?}) by first passing it to the order enforcer ...",
            *usig_message.as_ref(),
            usig_message.msg_type(),
            usig_message.counter()
        );

        // case 2: The UsigMessage does not contain other messages (i.e. a non-nested UsigMessage is to be processed).
        let from = *usig_message.as_ref();
        let replica_state = &mut self.replicas_state[from.as_u64() as usize];
        let messages: Vec<_> = replica_state
            .usig_message_order_enforcer
            .push_to_handle(Cow::<'_, UsigMessage<P, U::Signature>>::Owned(usig_message))
            .collect();

        for usig_inner_message_non_nested in messages {
            self.process_usig_message_ordered(usig_inner_message_non_nested, output);
        }
    }

    /// Process a [UsigMessage] in an ordered way.
    /// The messages passed are assumed to be ordered.
    fn process_usig_message_ordered(
        &mut self,
        usig_message: UsigMessage<P, U::Signature>,
        output: &mut NotReflectedOutput<P, U>,
    ) {
        let origin = *usig_message.as_ref();
        let msg_type = usig_message.msg_type();
        let counter = usig_message.counter();
        trace!("Processing message (origin: {:?}, type: {:?}, counter: {:?}) completely as it has been selected by the order enforcer as the next one to be processed ...", origin, msg_type, counter);
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
                ViewState::ChangeInProgress(in_progress) => {
                    warn!("Processing message (origin: {:?}, type: {:?}, counter: {:?}) resulted in ignoring it: Replica is in progress of changing views (from {:?} to {:?}).", origin, msg_type, counter, in_progress.prev_view, in_progress.next_view);
                    return;
                }
            },
            UsigMessage::ViewChange(view_change) => self.process_view_change(view_change, output),
            UsigMessage::NewView(new_view) => self.process_new_view(new_view, output),
            UsigMessage::Checkpoint(checkpoint) => self.process_checkpoint(checkpoint),
        }
        trace!(
            "Successfully processed message (origin: {:?}, type: {:?}, counter: {:?}) completely.",
            origin,
            msg_type,
            counter
        );
    }
}
