//! Defines the collector of messages of type ViewChange.
//! After a sufficient amount (t + 1) of ViewChanges are received and collected, the next [View] broadcasts a NewView message.
//! The Commits must share the same next [crate::View].

use tracing::debug;

use crate::{peer_message::usig_message::view_change::ViewChange, Config, View};

use super::CollectorMessages;

pub(crate) type CollectorViewChanges<P, Sig> = CollectorMessages<View, ViewChange<P, Sig>>;

impl<P: Clone, Sig: Clone> CollectorViewChanges<P, Sig> {
    /// Inserts a ViewChange message and returns the amount of so far collected
    /// ViewChanges for the same next [View] as the given message.
    pub(crate) fn collect_view_change(&mut self, msg: ViewChange<P, Sig>) -> u64 {
        let origin = msg.origin;
        let next_view = msg.next_view;
        debug!(
            "Collecting ViewChange (origin: {:?}, next_view: {:?}) ...",
            origin, next_view
        );
        let amount_collected = self.collect(msg, origin, next_view);
        debug!(
            "Successfully collected ViewChange (origin: {:?}, next view: {:?}).",
            origin, next_view
        );
        amount_collected
    }

    /// Retrieves a collection of at least t + 1 ViewChanges if they are valid and
    /// if already at least t + 1 ViewChanges have been received for the same next [View].
    /// Is this the case, then the collection only retains ViewChanges which are for a higher next [View].
    pub(crate) fn retrieve_collected_view_changes(
        &mut self,
        msg: &ViewChange<P, Sig>,
        config: &Config,
    ) -> Option<Vec<ViewChange<P, Sig>>> {
        debug!(
            "Retrieving ViewChanges (next view: {:?}) from collector ...",
            msg.next_view,
        );
        let mut retrieved = self.retrieve(msg.next_view, config)?;
        let mut retrieved_all = Vec::new();
        retrieved_all.push(retrieved.0);
        retrieved_all.append(&mut retrieved.1);
        debug!(
            "Successfully retrieved ViewChanges (next view: {:?}).",
            msg.next_view
        );
        Some(retrieved_all)
    }
}
