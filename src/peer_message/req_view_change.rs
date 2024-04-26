//! Defines a message of type [ReqViewChange].
//! A [ReqViewChange] consists of a previous and of a next [View].
//! The previous [View] is the [View] that turned out to be faulty.
//! The next [View] is the [View] to which is to be changed to.
//! A replica should broadcast a [ReqViewChange] when the current primary turns out to be faulty.
//! For further explanation, see the paper "Efficient Byzantine Fault Tolerance" by Veronese et al.

use anyhow::Result;
use serde::{Deserialize, Serialize};
use shared_ids::ReplicaId;
use tracing::debug;

use crate::{error::InnerError, Config, View};

/// Defines a message of type [ReqViewChange].
/// Contains the previous [View] and the next [View] ([View] to be changed to).
#[derive(Serialize, Deserialize, Clone, Debug)]
pub(crate) struct ReqViewChange {
    /// The previous [View] that turned out to be faulty.
    pub(crate) prev_view: View,
    /// The next [View] to which is to be changed to.
    pub(crate) next_view: View,
}

impl ReqViewChange {
    /// Validates the [ReqViewChange].
    /// The previous [View] must be smaller than the next [View].
    pub(crate) fn validate(&self, origin: ReplicaId, config: &Config) -> Result<(), InnerError> {
        debug!(
            "Validating ReqViewChange (previous view: {:?}, next view: {:?}) ...",
            self.prev_view, self.next_view
        );
        if self.prev_view < self.next_view {
            debug!(
                "Successfully validated ReqViewChange (previous view: {:?}, next view: {:?}).",
                self.prev_view, self.next_view
            );
            Ok(())
        } else {
            debug!(
                "Failed validating ReqViewChange (previous view: {:?}, next view: {:?}): Previous view set is not smaller than next view set.",
                self.prev_view, self.next_view
            );
            Err(InnerError::ReqViewChangeIncompatiblePrevNextView {
                receiver: config.id,
                origin,
            })
        }
    }
}

#[cfg(test)]

mod tests {
    use std::num::NonZeroU64;
    use std::time::Duration;

    use shared_ids::ReplicaId;

    use crate::Config;
    use crate::View;

    use super::ReqViewChange;

    /// Tests if a [ReqViewChange], in which the next [View] is smaller
    /// and subsequent to the previous [View], is validated to false.
    #[test]
    fn validate_invalid_req_view_change_subsequent() {
        let req_view_change = ReqViewChange {
            prev_view: View(2),
            next_view: View(1),
        };
        let config = Config {
            n: NonZeroU64::new(3).unwrap(),
            t: 1,
            id: ReplicaId::from_u64(0),
            batch_timeout: Duration::from_secs(2),
            max_batch_size: None,
            initial_timeout_duration: Duration::from_secs(2),
            checkpoint_period: NonZeroU64::new(2).unwrap(),
        };
        assert!(req_view_change
            .validate(ReplicaId::from_u64(0), &config)
            .is_err());
    }

    /// Tests if the validation of a [ReqViewChange], in which the next [View] is smaller
    /// and subsequent to the previous [View], results in an error.
    #[test]
    fn validate_invalid_req_view_change_jump() {
        let req_view_change = ReqViewChange {
            prev_view: View(3),
            next_view: View(0),
        };
        let config = Config {
            n: NonZeroU64::new(3).unwrap(),
            t: 1,
            id: ReplicaId::from_u64(0),
            batch_timeout: Duration::from_secs(2),
            max_batch_size: None,
            initial_timeout_duration: Duration::from_secs(2),
            checkpoint_period: NonZeroU64::new(2).unwrap(),
        };
        assert!(req_view_change
            .validate(ReplicaId::from_u64(0), &config)
            .is_err());
    }

    /// Tests if the validation of a [ReqViewChange], in which the next [View] is bigger
    /// and subsequent to the previous [View], succeeds.
    #[test]
    fn validate_valid_req_view_change_subsequent() {
        let req_view_change = ReqViewChange {
            prev_view: View(2),
            next_view: View(3),
        };
        let config = Config {
            n: NonZeroU64::new(3).unwrap(),
            t: 1,
            id: ReplicaId::from_u64(0),
            batch_timeout: Duration::from_secs(2),
            max_batch_size: None,
            initial_timeout_duration: Duration::from_secs(2),
            checkpoint_period: NonZeroU64::new(2).unwrap(),
        };
        assert!(req_view_change
            .validate(ReplicaId::from_u64(0), &config)
            .is_ok());
    }

    /// Tests if the validation of a [ReqViewChange], in which the next [View] is bigger
    /// but not subsequent to the previous [View], succeeds.
    #[test]
    fn validate_valid_req_view_change_jump() {
        let req_view_change = ReqViewChange {
            prev_view: View(2),
            next_view: View(7),
        };
        let config = Config {
            n: NonZeroU64::new(3).unwrap(),
            t: 1,
            id: ReplicaId::from_u64(0),
            batch_timeout: Duration::from_secs(2),
            max_batch_size: None,
            initial_timeout_duration: Duration::from_secs(2),
            checkpoint_period: NonZeroU64::new(2).unwrap(),
        };
        assert!(req_view_change
            .validate(ReplicaId::from_u64(0), &config)
            .is_ok());
    }
}
