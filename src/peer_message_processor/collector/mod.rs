//! Defines types of collectors for collecting messages.
//! One type is [CollectorMessages].
//! The other type is [CollectorBools].
//! The difference between the types is that the first one stores the actual messages
//! whereas the second one only stores booleans.
//! The booleans indicate if for a given replica a message has been received or not.
//! Both types keep count of how many valid messages have been received.

pub(crate) mod collector_checkpoints;
pub(crate) mod collector_commits;
pub(crate) mod collector_req_view_changes;
pub(crate) mod collector_view_changes;

use std::{collections::HashMap, hash::Hash, mem, num::NonZeroU64};

use serde::{Deserialize, Serialize};
use shared_ids::{AnyId, ReplicaId};
use tracing::debug;

use crate::Config;

/// Collects messages and keeps track of how many messages have been received.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct CollectorMessages<K: Eq + Hash + PartialOrd, M>(
    HashMap<K, (u64, Vec<Option<M>>)>,
);

impl<K: Eq + Hash + PartialOrd, M> CollectorMessages<K, M> {
    pub(crate) fn new() -> Self {
        Self(HashMap::new())
    }
    /// Inserts a message and returns the amount of so far collected
    /// messages with the same key as the given message.
    pub(crate) fn collect(&mut self, msg: M, from: ReplicaId, key: K, config: &Config) -> u64 {
        let index = from.as_u64() as usize;
        let counter;
        match self.0.get_mut(&key) {
            Some((actual_counter, messages)) => {
                // At least one message with the same key has been received before.
                if messages[index].is_some() {
                    debug!("Skipped inserting message (origin: {from:?}) into collector: Message was a duplicate.");
                    return *actual_counter;
                }
                // The given message is new.
                let mut messages = mem::take(messages);
                messages[index] = Some(msg);
                counter = *actual_counter + 1;
                self.0.insert(key, (counter, messages));
                debug!("Inserted message (origin: {from:?}) into collector.");
            }
            None => {
                // No message with the same key has been received before.
                let mut messages = Vec::with_capacity(config.n.get() as usize);
                let mut amount_initialized = 0;
                while amount_initialized < config.n.get() {
                    messages.push(None);
                    amount_initialized += 1;
                }
                messages[index] = Some(msg);
                counter = 1;
                self.0.insert(key, (counter, messages));
                debug!("Inserted message (origin: {from:?}) into collector.");
            }
        }
        counter
    }

    /// Retrieves a collection of at least t + 1 messages if they are valid and
    /// if already at least t + 1 messages have been received with the same given key.
    /// One of the messages must have been broadcast by the replica itself.
    /// If this is the case, then the collection only retains messages which have a higher key.
    pub(crate) fn retrieve(&mut self, key: K, config: &Config) -> Option<(M, Vec<M>)> {
        let collected = self.0.get(&key);
        let collected = collected?;
        if collected.0 <= config.t {
            return None;
        }
        // At least t + 1 received.
        if collected
            .1
            .get(config.id.as_u64() as usize)
            .unwrap()
            .is_none()
        {
            return None;
        }
        // The replica's own message has been received, too.
        // Collect messages and clean up collector.
        let mut my_retrieved_message = None;
        let mut other_retrieved_messages = Vec::new();
        let (_, mut messages) = self.0.remove(&key).unwrap();
        for (from, msg) in messages.iter_mut().enumerate() {
            if let Some(msg) = msg.take() {
                if config.id.as_u64() == from as u64 {
                    my_retrieved_message = Some(msg);
                } else {
                    other_retrieved_messages.push(msg);
                }
            }
        }
        self.0.retain(|n, _| n > &key);
        let my_retrieved_message = my_retrieved_message?;
        Some((my_retrieved_message, other_retrieved_messages))
    }
}

/// Defines a boolean array.
/// Sets specific entries to true.
/// Keeps count of how many entries are set to true.
#[derive(Clone, Default, Debug, Serialize, Deserialize)]
pub(super) struct BoolArray {
    // The counter to keep count of how many entries are set to true in the vec.
    pub(crate) counter: u64,
    // The vec of bools
    bools: Vec<bool>,
}

impl BoolArray {
    /// Creates a new [BoolArray] of the given size.
    pub fn new(size: NonZeroU64) -> Self {
        let mut bools = Vec::with_capacity(size.get() as usize);
        let mut amount_initialized = 0;
        while amount_initialized < size.get() {
            bools.push(false);
            amount_initialized += 1;
        }
        Self { counter: 0, bools }
    }

    /// Updates the boolean array according to the provided index.
    /// That is to say, it sets the entry at the index to true
    /// and increments the counter if the entry was false before.
    pub(super) fn update(&mut self, index: usize) -> bool {
        if !self.bools[index] {
            self.bools[index] = true;
            self.counter += 1;
            return true;
        }
        false
    }
}

/// Keeps track of how many messages have been received and from which replica.
#[derive(Debug, Clone)]
pub(crate) struct CollectorBools<K: Eq + Hash + PartialOrd>(HashMap<K, BoolArray>);

impl<K: Eq + Hash + PartialOrd> CollectorBools<K> {
    /// Creates a new [CollectorBools].
    pub(crate) fn new() -> Self {
        Self(HashMap::new())
    }
    /// Collects a message by updating the [BoolArray] accordingly and returns the amount of collected messages
    /// received with the same key as the input.
    pub(crate) fn collect(&mut self, key: K, from: ReplicaId, config: &Config) -> u64 {
        let mut bool_array = BoolArray::new(config.n);
        match self.0.get_mut(&key) {
            Some(actual_bool_array) => {
                bool_array = mem::take(actual_bool_array);
                let index = from.as_u64() as usize;
                if bool_array.update(index) {
                    debug!("Inserted message (origin: {from:?}) into collector.");
                } else {
                    debug!("Skipped inserting message (origin: {from:?}) into collector: Message was a duplicate.");
                };
            }
            None => {
                let index = from.as_u64() as usize;
                assert!(bool_array.update(index));
                debug!("Inserted message (origin: {from:?}) into collector.");
            }
        }
        let amount_collected = bool_array.counter;
        self.0.insert(key, bool_array);
        amount_collected
    }

    /// Cleans up the collection by retaining only messages with a higher key.
    pub(crate) fn clean_up(&mut self, key: K) {
        self.0.retain(|k, _| k > &key);
    }
}
