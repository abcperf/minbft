//! Defines the required logic for creating timeouts.
//! Timeouts must be set for messages that need to be handled until a specific time.
//! Those messages are client-requests, batches, and view-changes.
//! The timeouts must be explicitly handled.
//! For an example see the documentation of [crate::MinBft].

use std::time::Duration;

use shared_ids::ClientId;

/// Defines the types a Timeout can have.
/// Timeouts of different type are handled differently.
#[derive(Debug, Clone, Copy, Hash, Eq, PartialEq, PartialOrd, Ord)]
pub enum TimeoutType {
    /// The Timeout is for a Batch.
    Batch,
    /// The Timeout is for a Client.
    Client,
    /// The Timeout is for a ViewChange.
    ViewChange,
}

/// Defines a Timeout consisting of a type, duration, and stop class.
/// Timeouts ensure view-changes occur when they are necessary.
/// [crate::MinBft] outputs timeout requests when a timeout may possibly have to be set.
/// Timeout requests and timeouts must be handled explicitly, see [crate::MinBft].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Timeout {
    /// The type a timeout can have.
    pub timeout_type: TimeoutType,
    /// The time until the Timeout is triggered.
    pub duration: Duration,
    /// The identifying class of a timeout for when a request for stopping it is received.
    pub stop_class: StopClass,
}

/// Defines a Timeout consisting of a type, duration, and stop class.
/// Timeouts ensure view-changes occur when they are necessary.
/// [crate::MinBft] outputs timeout requests when a timeout may possibly have to be set.
/// Timeout requests and timeouts must be handled explicitly, see [crate::MinBft].
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
/// Only set timeouts with the same [StopClass] as the timeout of the received TimeoutRequest should be stopped.
#[derive(Default, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Copy)]
#[repr(transparent)]
pub struct StopClass(pub(crate) u64);

impl Timeout {
    /// Crates a [Timeout] of [TimeoutType::Batch] with the specified duration.
    pub(super) fn batch(duration: Duration) -> Self {
        Self {
            timeout_type: TimeoutType::Batch,
            duration,
            stop_class: StopClass::default(),
        }
    }

    /// Crates a [Timeout] of [TimeoutType::Client] with the specified [ClientId] and duration.
    pub(super) fn client(client_id: ClientId, duration: Duration) -> Self {
        Self {
            timeout_type: TimeoutType::Client,
            duration,
            stop_class: StopClass(client_id.as_u64()),
        }
    }

    /// Crates a [Timeout] of [TimeoutType::ViewChange] with the specified duration.
    pub(super) fn view_change(duration: Duration) -> Self {
        Self {
            timeout_type: TimeoutType::ViewChange,
            duration,
            stop_class: StopClass::default(),
        }
    }
}
