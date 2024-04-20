//! Defines the required logic for creating timeouts.
//! Timeouts must be set for messages that need to be handled until a specific
//! time.
//! Those messages are client-requests, batches, and view-changes.
//! The timeouts must be explicitly handled.
//! For an example see the documentation of [crate::MinBft].

use std::time::Duration;

use shared_ids::{AnyId, ClientId};

/// Defines the types a Timeout can have.
/// Timeouts of different type are handled differently, see [crate::MinBft].
#[derive(Debug, Clone, Copy, Hash, Eq, PartialEq, PartialOrd, Ord)]
pub enum TimeoutType {
    /// The Timeout is for a Batch.
    Batch,
    /// The Timeout is for a Client.
    Client,
    /// The Timeout is for a ViewChange.
    ViewChange,
}

/// Defines a Timeout consisting of a type, duration, and stop class.\
/// Timeouts ensure view-changes occur when they are necessary.\
/// [crate::MinBft] outputs timeout requests when a timeout may possibly have to
/// be set.\
/// Timeout requests and timeouts must be handled explicitly, see [crate::MinBft].
///
/// Contains a [StopClass], making sure running timeouts can only be stopped
/// if the [StopClass] is equal to them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Timeout {
    /// The type a timeout can have.
    pub timeout_type: TimeoutType,
    /// The time until the Timeout is triggered.
    pub duration: Duration,
    /// The identifying class of a timeout for when a request for stopping it is
    /// received.
    pub stop_class: StopClass,
}

/// Defines a Timeout consisting of a type, duration, and stop class.\
/// Timeouts ensure view-changes occur when they are necessary.\
/// [crate::MinBft] outputs timeout requests when a timeout may possibly have to be set.\
/// Timeout requests and timeouts must be handled explicitly, see [crate::MinBft].
///
/// Does not contain a [StopClass].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TimeoutAny {
    /// The type a timeout can have.
    pub timeout_type: TimeoutType,
    /// The time until the Timeout is triggered.
    pub duration: Duration,
}

impl TimeoutAny {
    /// Crates a [Timeout] of [TimeoutType::Client] with the specified [ClientId] and duration.
    pub(super) fn client(duration: Duration) -> Self {
        Self {
            timeout_type: TimeoutType::Client,
            duration,
        }
    }
}

/// Identifies a timeout when a request for stopping it is received.
/// A pending timeout is only stopped if the [StopClass] of the timeout of the
/// received TimeoutRequest is equal to its stop class.
#[derive(Default, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Copy)]
#[repr(transparent)]
pub struct StopClass(pub(crate) u64);

impl Timeout {
    /// Creates a [Timeout] of [TimeoutType::Batch] with the specified duration.
    ///
    /// # Arguments
    ///
    /// * `duration` - The duration of the timeout.
    ///
    /// # Return Value
    ///
    /// The created [Timeout].
    pub fn batch(duration: Duration) -> Self {
        Self {
            timeout_type: TimeoutType::Batch,
            duration,
            stop_class: StopClass::default(),
        }
    }

    /// Creates a [Timeout] of [TimeoutType::Client] with the specified
    /// [ClientId] and duration.
    ///
    /// # Arguments
    ///
    /// * `client_id` - The ID of the client.
    /// * `duration` - The duration of the timeout.
    ///
    /// # Return Value
    ///
    /// The created [Timeout].
    pub fn client(client_id: ClientId, duration: Duration) -> Self {
        Self {
            timeout_type: TimeoutType::Client,
            duration,
            stop_class: StopClass(client_id.as_u64()),
        }
    }

    /// Creates a [Timeout] of [TimeoutType::ViewChange] with the specified
    /// duration.
    ///
    /// # Arguments
    ///
    /// * `duration` - The duration of the timeout.
    ///
    /// # Return Value
    ///
    /// The created [Timeout].
    pub fn view_change(duration: Duration) -> Self {
        Self {
            timeout_type: TimeoutType::ViewChange,
            duration,
            stop_class: StopClass::default(),
        }
    }
}
