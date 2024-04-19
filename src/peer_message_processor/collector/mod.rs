//! Defines the abstract collector type for collecting messages.
//! The type is [CollectorMessages].´

pub(crate) mod collector_checkpoints;
pub(crate) mod collector_commits;
pub(crate) mod collector_req_view_changes;
pub(crate) mod collector_view_changes;

use std::{collections::HashMap, hash::Hash};

use serde::{Deserialize, Serialize};
use shared_ids::ReplicaId;
use tracing::debug;

use crate::Config;

/// Collects messages and keeps track of how many messages have been received.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct CollectorMessages<K: Eq + Hash + PartialOrd + Clone, M>(
    HashMap<K, HashMap<ReplicaId, M>>,
);

impl<K: Eq + Hash + PartialOrd + Clone, M> CollectorMessages<K, M> {
    pub(crate) fn new() -> Self {
        Self(HashMap::new())
    }
    /// Collects a given message.
    ///
    /// # Arguments
    ///
    /// * `msg` - The message to be colleted.
    /// * `from` - The ID of the replica from which the message originates.
    ///
    /// # Return Value
    ///
    /// The amount of so far collected messages with the same key as the given
    /// message.
    fn collect(&mut self, msg: M, from: ReplicaId, key: K) -> u64 {
        match self.0.get_mut(&key) {
            Some(messages) => {
                // At least one message with the same key has been received before.
                if messages.get(&from).is_some() {
                    debug!("Skipped inserting message (origin: {from:?}) into collector: Message was a duplicate.");
                    return messages.len() as u64;
                }
                // The given message is new.
                messages.insert(from, msg);
                debug!("Inserted message (origin: {from:?}) into collector.");
            }
            None => {
                // No message with the same key has been received before
                let mut messages = HashMap::new();
                messages.insert(from, msg);
                self.0.insert(key.clone(), messages);
                debug!("Inserted message (origin: {from:?}) into collector.");
            }
        }
        self.0.get_mut(&key).unwrap().len() as u64
    }

    /// Retrieves a collection of at least t + 1 messages if they are valid and
    /// with the same given key.
    /// One of the messages must have been broadcast by the replica itself.
    /// If this is the case, then the collection only retains messages which
    /// have a "higher" key, as the "lower" ones are no longer necessary.
    ///
    /// # Arguments
    ///
    /// * `key` - The key from which the collected messages should be retrieved.
    /// * `config` - The config of the replica.
    fn retrieve(&mut self, key: K, config: &Config) -> Option<(M, Vec<M>)> {
        let messages = self.0.get_mut(&key);

        // Check if at least `t + 1` messages have been received.
        messages.as_ref()?;
        let messages = messages.unwrap();
        if messages.len() <= config.t.try_into().unwrap() {
            return None;
        }

        // Check if the replica's own message has been received.
        if !messages.contains_key(&config.id) {
            return None;
        }

        // The replica's own message has been received, too.
        // Collect messages and clean up collector.
        let mut my_retrieved_message = None;
        let mut other_retrieved_messages = Vec::new();
        let mut messages = self.0.remove(&key).unwrap();

        for (rep_id, msg) in messages.drain() {
            if rep_id == config.id {
                my_retrieved_message = Some(msg);
            } else {
                other_retrieved_messages.push(msg);
            }
        }

        self.0.retain(|n, _| n > &key);
        let my_retrieved_message = my_retrieved_message?;
        Some((my_retrieved_message, other_retrieved_messages))
    }
}
