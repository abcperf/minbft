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

    use crate::View;
    use rand::Rng;
    use rstest::rstest;
    use shared_ids::AnyId;
    use shared_ids::ReplicaId;

    use super::ReqViewChange;

    /// Tests if the validation of a [ReqViewChange], in which the next [View] is bigger
    /// and directly subsequent to the previous [View], succeeds.
    #[rstest]
    fn validate_valid_req_view_change_next_dir_subsequent(
        #[values(3, 4, 5, 6, 7, 8, 9, 10)] n: u64,
    ) {
        use rand::thread_rng;

        use crate::tests::{
            create_default_configs_for_replicas, get_random_included_index, get_random_replica_id,
        };

        let n_parsed = NonZeroU64::new(n).unwrap();
        let mut rng = thread_rng();
        let origin = get_random_replica_id(n_parsed, &mut rng);

        let rand_factor_0 = get_random_included_index(n as usize * 10, None, &mut rng);

        let prev_view_nr = rng.gen_range(0..=rand_factor_0 as u64 * n);
        let next_view_nr = prev_view_nr + 1;

        let t = n / 2;

        let prev_view = View(prev_view_nr);
        let next_view = View(next_view_nr);

        let configs = create_default_configs_for_replicas(n_parsed, t);

        let req_view_change = ReqViewChange {
            prev_view,
            next_view,
        };

        for i in 0..n {
            let rep_id = ReplicaId::from_u64(i);
            let config = configs.get(&rep_id).unwrap();
            assert!((req_view_change.validate(origin, config)).is_ok());
        }
    }

    /// Tests if the validation of a [ReqViewChange], in which the next [View] is bigger
    /// but not subsequent to the previous [View], succeeds.
    #[rstest]
    fn validate_valid_req_view_change_next_jump(#[values(3, 4, 5, 6, 7, 8, 9, 10)] n: u64) {
        use rand::thread_rng;

        use crate::tests::{
            create_default_configs_for_replicas, get_random_included_index, get_random_replica_id,
        };

        let n_parsed = NonZeroU64::new(n).unwrap();
        let mut rng = thread_rng();
        let origin = get_random_replica_id(n_parsed, &mut rng);

        let rand_factor_0 = get_random_included_index(n as usize * 10, None, &mut rng);
        let rand_summand = rng.gen_range(1..=n * 10);

        let prev_view_nr = rng.gen_range(0..=rand_factor_0 as u64 * n);
        let next_view_nr = prev_view_nr + rand_summand;

        let t = n / 2;

        let prev_view = View(prev_view_nr);
        let next_view = View(next_view_nr);

        let configs = create_default_configs_for_replicas(n_parsed, t);

        let req_view_change = ReqViewChange {
            prev_view,
            next_view,
        };

        for i in 0..n {
            let rep_id = ReplicaId::from_u64(i);
            let config = configs.get(&rep_id).unwrap();
            assert!((req_view_change.validate(origin, config)).is_ok());
        }
    }

    /// Tests if a [ReqViewChange], in which the next [View] is smaller
    /// and subsequent to the previous [View], is validated to false.
    #[rstest]
    fn validate_invalid_req_view_change_prev_dir_subsequent(
        #[values(3, 4, 5, 6, 7, 8, 9, 10)] n: u64,
    ) {
        use rand::thread_rng;

        use crate::tests::{
            create_default_configs_for_replicas, get_random_included_index, get_random_replica_id,
        };

        let n_parsed = NonZeroU64::new(n).unwrap();
        let mut rng = thread_rng();
        let origin = get_random_replica_id(n_parsed, &mut rng);

        let rand_factor_0 = get_random_included_index(n as usize * 10, None, &mut rng);

        let next_view_nr = rng.gen_range(0..=rand_factor_0 as u64 * n);
        let prev_view_nr = next_view_nr + 1;

        let t = n / 2;

        let prev_view = View(prev_view_nr);
        let next_view = View(next_view_nr);

        let configs = create_default_configs_for_replicas(n_parsed, t);

        let req_view_change = ReqViewChange {
            prev_view,
            next_view,
        };

        for i in 0..n {
            let rep_id = ReplicaId::from_u64(i);
            let config = configs.get(&rep_id).unwrap();
            assert!((req_view_change.validate(origin, config)).is_err());
        }
    }

    /// Tests if the validation of a [ReqViewChange], in which the next [View] is smaller
    /// and subsequent to the previous [View], results in an error.
    #[rstest]
    fn validate_invalid_req_view_change_jump(#[values(3, 4, 5, 6, 7, 8, 9, 10)] n: u64) {
        use rand::thread_rng;

        use crate::tests::{
            create_default_configs_for_replicas, get_random_included_index, get_random_replica_id,
        };

        let n_parsed = NonZeroU64::new(n).unwrap();
        let mut rng = thread_rng();
        let origin = get_random_replica_id(n_parsed, &mut rng);

        let rand_factor_0 = get_random_included_index(n as usize * 10, None, &mut rng);
        let rand_summand = rng.gen_range(1..=n * 10);

        let next_view_nr = rng.gen_range(0..=rand_factor_0 as u64 * n);
        let prev_view_nr = next_view_nr + rand_summand;

        let t = n / 2;

        let prev_view = View(prev_view_nr);
        let next_view = View(next_view_nr);

        let configs = create_default_configs_for_replicas(n_parsed, t);

        let req_view_change = ReqViewChange {
            prev_view,
            next_view,
        };

        for i in 0..n {
            let rep_id = ReplicaId::from_u64(i);
            let config = configs.get(&rep_id).unwrap();
            assert!((req_view_change.validate(origin, config)).is_err());
        }
    }
}
