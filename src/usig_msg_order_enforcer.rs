use std::{
    borrow::Cow,
    cmp::Ordering,
    collections::{binary_heap::PeekMut, BinaryHeap},
    iter,
};

use either::Either;
use usig::{Count, Counter};

use crate::peer_message::usig_message::UsigMessage;

/// Defines a Wrapper for messages of type UsigMessage.
#[derive(Debug, Clone)]
#[repr(transparent)]
struct UsigMessageWrapper<P, Sig>(UsigMessage<P, Sig>);

impl<P, Sig> From<UsigMessageWrapper<P, Sig>> for UsigMessage<P, Sig> {
    /// Convert the given UsigMessageWrapper to a UsigMessage.
    fn from(usig_message_wrapper: UsigMessageWrapper<P, Sig>) -> Self {
        usig_message_wrapper.0
    }
}

impl<P, Sig> From<UsigMessage<P, Sig>> for UsigMessageWrapper<P, Sig> {
    /// Convert the given UsigMessage to a UsigMessageWrapper.
    fn from(usig_message: UsigMessage<P, Sig>) -> Self {
        Self(usig_message)
    }
}

impl<P, Sig: Counter> Counter for UsigMessageWrapper<P, Sig> {
    /// Returns the counter of the UsigMessage.
    fn counter(&self) -> Count {
        self.0.counter()
    }
}

impl<P, Sig: Counter> PartialEq for UsigMessageWrapper<P, Sig> {
    /// Returns true if the counters of the UsigMessageWrappers are equal, otherwise false.
    fn eq(&self, other: &Self) -> bool {
        self.counter().eq(&other.counter())
    }
}

impl<P, Sig: Counter> Eq for UsigMessageWrapper<P, Sig> {}

impl<P, Sig: Counter> PartialOrd for UsigMessageWrapper<P, Sig> {
    /// Partially compares the counters of the UsigMessageWrappers.
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.counter()
            .partial_cmp(&other.counter())
            .map(|c| c.reverse())
    }
}

impl<P, Sig: Counter> Ord for UsigMessageWrapper<P, Sig> {
    /// Compares the counters of the UsigMessageWrappers.
    fn cmp(&self, other: &Self) -> Ordering {
        self.counter().cmp(&other.counter()).reverse()
    }
}

/// Defines errors regarding the counter of UsigMessages.
enum CountCheckerError {
    /// Already seen UsigMessage based on its counter.
    AlreadySeen,
    /// Not the next expected UsigMessage based on its counter.
    NotNext,
}

/// The checker for counters of messages of type UsigMessage.
#[derive(Debug, Clone)]
struct CountChecker {
    /// The next expected Counter.
    next_count: Count,
}

impl CountChecker {
    /// Creates a new CountChecker.
    fn new() -> Self {
        Self {
            next_count: Count(0),
        }
    }

    /// Increments the next expected count if the given count is the expected one.
    fn next(&mut self, count: Count) -> Result<(), CountCheckerError> {
        match count.cmp(&self.next_count) {
            Ordering::Less => Err(CountCheckerError::AlreadySeen),
            Ordering::Equal => {
                self.next_count += 1;
                Ok(())
            }
            Ordering::Greater => Err(CountCheckerError::NotNext),
        }
    }
}

/// Defines the state of the UsigMessageHandler.
#[derive(Debug, Clone)]
pub(super) struct UsigMsgOrderEnforcer<P, Sig> {
    /// Used for assuring the UsigMessages are handled and processed in the right order.
    count_checker: CountChecker,
    /// Collects already received, but yet to process UsigMessages
    /// (UsigMessages that have a smaller Counter are yet to be received).
    unprocessed: BinaryHeap<UsigMessageWrapper<P, Sig>>, // TODO limit
}

impl<P, Sig: Counter> Default for UsigMsgOrderEnforcer<P, Sig> {
    /// Creates a new default UsigMessageHandlerState.
    fn default() -> Self {
        Self {
            count_checker: CountChecker::new(),
            unprocessed: BinaryHeap::new(),
        }
    }
}

impl<P: Clone, Sig: Counter + Clone> UsigMsgOrderEnforcer<P, Sig> {
    /// Check if the given UsigMessage is the next one expected based on its counter.
    /// case 1: If the given UsigMessage is the next one expected, an Iterator over the given UsigMessage and
    /// all other received messages of type UsigMessage that have yet to be processed and have counters that follow directly.
    /// case 2: If the given UsigMessage is not the one expected and was already seen, it is ignored.
    /// case 3: If the given UsigMessage is not the one expected and was not yet seen, it is stored as unprocessed.
    /// In cases 2 and 3 an empty Iterator is returned.
    pub(super) fn push_to_handle<'a>(
        &'a mut self,
        msg: Cow<'_, impl Into<UsigMessage<P, Sig>> + Clone + Counter>,
    ) -> impl Iterator<Item = UsigMessage<P, Sig>> + 'a {
        match self.count_checker.next(msg.counter()) {
            Ok(()) => {
                // we have the next message, so yield it and any messages that follow directly
                Either::Left(
                    iter::once(msg.into_owned().into()).chain(self.yield_to_be_processed()),
                )
            }
            Err(e) => {
                match e {
                    CountCheckerError::AlreadySeen => {
                        // the given UsigMessage is an old one, so it is ignored
                    }
                    CountCheckerError::NotNext => {
                        // the given UsigMessage is not the next expected message, so it is put in the unprocessed heap
                        // (we might add a message mulitple times to the heap, but those get filtered out in yield_processed)
                        self.unprocessed.push(msg.into_owned().into().into())
                    }
                }
                Either::Right(iter::empty())
            }
        }
    }

    /// Returns to-be-processed UsigMessages.
    fn yield_to_be_processed(&mut self) -> impl Iterator<Item = UsigMessage<P, Sig>> + '_ {
        iter::from_fn(|| {
            while let Some(head) = self.unprocessed.peek_mut() {
                match self.count_checker.next(head.counter()) {
                    Ok(()) => {
                        // we found the next message, so return it
                        return Some(PeekMut::pop(head).into());
                    }
                    Err(CountCheckerError::AlreadySeen) => {
                        // the message is a duplicate, i.e. it was already seen
                        // pop, but ignore it
                        PeekMut::pop(head);
                        continue;
                    }
                    Err(CountCheckerError::NotNext) => {
                        // a hole was found, i.e. there are still missing UsigMessages
                        // therefore, end the iterator
                        return None;
                    }
                }
            }
            // no (more) to-be-processed messages, so end the iterator
            None
        })
    }
}
