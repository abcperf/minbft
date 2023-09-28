//! Defines the collector of messages of type ReqViewChange.
//! After a sufficient amount (t + 1) of ReqViewChanges are received and collected, a ViewChange is broadcast.
//! The ReqViewChanges must share the same previous and next [crate::View]s.

use serde::{Deserialize, Serialize};
use shared_ids::ReplicaId;
use std::{cmp::Ordering, hash::Hash};
use tracing::debug;

use crate::{peer_message::req_view_change::ReqViewChange, Config, View};

use super::CollectorBools;

/// The purpose of the struct is to collect messages of type ReqViewChange.
#[derive(Debug, Clone)]
pub(crate) struct CollectorReqViewChanges(CollectorBools<KeyRVC>);

/// Defines the key for the collector.
/// The key must be the previous and next [crate::View]s.
#[derive(Debug, Serialize, Deserialize, Clone)]
struct KeyRVC {
    // The previous View that turned out to be faulty for the sender of the received ReqViewChange.
    prev_view: View,
    // The next View to which the sender of the received ReqViewChange message wants to change to.
    next_view: View,
}

impl PartialEq for KeyRVC {
    /// Returns true if both the previous and the next [View]s are equal.
    fn eq(&self, other: &Self) -> bool {
        self.prev_view.eq(&other.prev_view) && self.next_view.eq(&other.next_view)
    }
}

impl Hash for KeyRVC {
    /// Computes the hash of the previous and the next [View].
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.prev_view.hash(state);
        self.next_view.hash(state);
    }
}

impl Eq for KeyRVC {}

impl PartialOrd for KeyRVC {
    /// Partially compares the previous Views with each other.
    /// If the previous Views are equal, then the next Views are partially compared with each other.
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match self.prev_view.partial_cmp(&other.prev_view) {
            Some(Ordering::Less) => Some(Ordering::Less),
            Some(Ordering::Equal) => self.next_view.partial_cmp(&other.next_view),
            Some(Ordering::Greater) => Some(Ordering::Greater),
            None => None,
        }
    }
}

impl Ord for KeyRVC {
    /// Compares the previous Views with each other.
    /// If the previous Views are equal, then the next Views are compared with each other.
    fn cmp(&self, other: &Self) -> Ordering {
        match self.prev_view.cmp(&other.prev_view) {
            Ordering::Less => Ordering::Less,
            Ordering::Equal => self.next_view.cmp(&other.next_view),
            Ordering::Greater => Ordering::Greater,
        }
    }
}

impl CollectorReqViewChanges {
    /// Creates a new collector of ReqViewChanges.
    pub(crate) fn new() -> Self {
        Self(CollectorBools::new())
    }
    /// Inserts a ReqViewChange message and returns the amount of collected ReqViewChanges
    /// received for the same previous and next [View] as the input.
    pub(crate) fn collect(&mut self, msg: ReqViewChange, from: ReplicaId, config: &Config) -> u64 {
        debug!(
            "Insert message ReqViewChange (origin: {:?}, prev_view: {:?}, next_view: {:?})",
            from, msg.prev_view, msg.next_view,
        );
        let key = KeyRVC {
            prev_view: msg.prev_view,
            next_view: msg.next_view,
        };
        self.0.collect(key, from, config)
    }

    /// Cleans up the collection by retaining only ReqViewChanges with
    /// their previous [View] set as higher than the provided one, or
    /// the same but their next [View] is set higher.
    pub(crate) fn clean_up(&mut self, prev_view: View, next_view: View) {
        debug!("Cleaning up collector of ReqViewChanges by removing ReqViewChanges with prev_view less than {:?} or equal to it and next_view less than {:?}", prev_view, next_view);
        let key = KeyRVC {
            prev_view,
            next_view,
        };
        self.0.clean_up(key)
    }
}
