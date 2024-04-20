//! Models the configuration parameters of a replica in the partially
//! asynchronous Byzantine fault-tolerant atomic broadcast (BFT) algorithm.

use anyhow::Result;
use serde::{Deserialize, Serialize};
use shared_ids::{AnyId, IdIter};
use std::{
    num::{NonZeroU64, NonZeroUsize},
    ops::MulAssign,
    time::Duration,
};

use super::{ReplicaId, View};

/// Contains the configuration parameters for the
/// partially asynchronous Byzantine fault-tolerant atomic broadcast (BFT)
/// algorithm.
///
/// The configuration belongs to one specific replica
/// of a system of multiple replicas that together form the atomic broadcast.
#[derive(Debug)]
pub struct Config {
    /// The amount of replicas (n > 2t).
    pub n: NonZeroU64,
    /// The amount of faulty replicas.
    pub t: u64,
    /// The id of the Replica to which this Config belongs to.
    pub id: ReplicaId,
    /// The duration set for a batch to timeout.
    pub batch_timeout: Duration,
    /// The maximum amount of client-requests a batch may contain.
    /// [None] for unlimited.
    pub max_batch_size: Option<NonZeroUsize>,
    /// The initial duration set for a view-change message or client request to
    /// timeout.
    pub initial_timeout_duration: Duration,
    /// The amount of accepted batches it is needed for a checkpoint
    /// to be generated each time after generating the last one.
    pub checkpoint_period: NonZeroU64,
}

/// Defines the multiplier used to calculate the duration
/// of the next timeout of a client or view-change message in case requests
/// are not handled in-time.
#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
pub struct BackoffMultiplier(f64);

impl TryFrom<f64> for BackoffMultiplier {
    type Error = ();

    /// Tries to create a [BackoffMultiplier] from a [f64].
    /// The provided multiplier has to be greater than or equal to 1.
    fn try_from(multiplier: f64) -> Result<Self, Self::Error> {
        if multiplier >= 1.0 {
            Ok(Self(multiplier))
        } else {
            Err(())
        }
    }
}

impl MulAssign<BackoffMultiplier> for Duration {
    /// Defines the operation *= when providing a [BackoffMultiplier].
    fn mul_assign(&mut self, rhs: BackoffMultiplier) {
        *self = Duration::from_secs_f64(self.as_secs_f64() * rhs.0);
    }
}

impl Config {
    /// Validates itself, i.e. the Config of the Replica.
    /// Returns therefore true if the Config is valid, otherwise false.
    pub(super) fn validate(&self) {
        assert!(self.id.as_u64() < self.n.get());
        assert!(2 * self.t < self.n.get());
    }

    /// Returns the Id of the Replica to which the Config belongs to.
    pub(super) fn me(&self) -> ReplicaId {
        self.id
    }

    /// Returns true if the given Replica is the primary of the given View.
    pub(super) fn is_primary(&self, view: View, replica: ReplicaId) -> bool {
        assert!(replica.as_u64() < self.n.get());
        replica == self.primary(view)
    }

    /// Returns the Replica of the given View (primary).
    pub(super) fn primary(&self, view: View) -> ReplicaId {
        ReplicaId::from_u64(view.0 % self.n)
    }

    /// Returns true if the Replica to which the Config belongs to is the primary of the given View.
    pub(super) fn me_primary(&self, view: View) -> bool {
        self.is_primary(view, self.me())
    }

    /// Returns all Replicas.
    pub(super) fn all_replicas(&self) -> impl Iterator<Item = ReplicaId> {
        IdIter::default().take(self.n.get().try_into().unwrap())
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Not;

    use super::*;

    /// Creates and returns a Config with the given parameters.
    fn new(n: u64, t: u64, id: u64) -> Config {
        let config = Config {
            n: n.try_into().unwrap(),
            t,
            id: ReplicaId::from_u64(id),
            max_batch_size: None,
            batch_timeout: Duration::from_secs(1),
            initial_timeout_duration: Duration::from_secs(1),
            checkpoint_period: NonZeroU64::new(2).unwrap(),
        };
        config.validate();
        config
    }

    #[test]
    #[should_panic]
    /// Test if creating a Config for an invalid ReplicaId (i.e. n <= id) results in an error.
    /// More precisely, test if creating a Config with n = 1 and id = 1 results in an error.
    fn fail_id_1() {
        new(1, 0, 1);
    }

    #[test]
    #[should_panic]
    /// Test if creating a Config for an invalid ReplicaId (i.e. n <= id) results in an error.
    /// More precisely, test if creating a Config with n = 1 and id = 2 results in an error.
    fn fail_id_1_higher() {
        new(1, 0, 2);
    }

    #[test]
    #[should_panic]
    /// Test if creating a Config for an invalid ReplicaId (i.e. n <= id) results in an error.
    /// More precisely, test if creating a Config with n = 2 and id = 2 results in an error.
    fn fail_id_2() {
        new(2, 0, 2);
    }

    #[test]
    #[should_panic]
    /// Test if creating a Config for an invalid ReplicaId (i.e. n <= id) results in an error.
    /// More precisely, test if creating a Config with n = 3 and id = 3 results in an error.
    fn fail_id_3() {
        new(3, 0, 3);
    }

    #[test]
    #[should_panic]
    /// Test if creating a Config for an invalid ReplicaId (i.e. n <= id) results in an error.
    /// More precisely, test if creating a Config with n = 4 and id = 4 results in an error.
    fn fail_id_4() {
        new(4, 0, 4);
    }

    #[test]
    #[should_panic]
    /// Test if creating a Config for an invalid ReplicaId (i.e. n <= id) results in an error.
    /// More precisely, test if creating a Config with n = 5 and id = 5 results in an error.
    fn fail_id_5() {
        new(5, 0, 5);
    }

    #[test]
    #[should_panic]
    /// Test if creating a Config for an invalid ReplicaId (i.e. n <= id) results in an error.
    /// More precisely, test if creating a Config with n = 100 and id = 100 results in an error.
    fn fail_id_100() {
        new(100, 0, 100);
    }

    #[test]
    #[should_panic]
    /// Test if creating a Config with an invalid n and t combination (i.e. n <= 2t) results in an error.
    /// More precisely, test if creating a Config with n = 1 and t = 1 results in an error.
    fn fail_t_1() {
        new(1, 1, 0);
    }

    #[test]
    #[should_panic]
    /// Test if creating a Config with an invalid n and t combination (i.e. n <= 2t) results in an error.
    /// More precisely, test if creating a Config with n = 1 and t = 2 results in an error.
    fn fail_t_1_higher() {
        new(1, 2, 0);
    }

    #[test]
    #[should_panic]
    /// Test if creating a Config with an invalid n and t combination (i.e. n <= 2t) results in an error.
    /// More precisely, test if creating a Config with n = 2 and t = 1 results in an error.
    fn fail_t_2() {
        new(2, 1, 0);
    }

    #[test]
    #[should_panic]
    /// Test if creating a Config with an invalid n and t combination (i.e. n <= 2t) results in an error.
    /// More precisely, test if creating a Config with n = 3 and t = 2 results in an error.
    fn fail_t_3() {
        new(3, 2, 0);
    }

    #[test]
    #[should_panic]
    /// Test if creating a Config with an invalid n and t combination (i.e. n <= 2t) results in an error.
    /// More precisely, test if creating a Config with n = 4 and t = 2 results in an error.
    fn fail_t_4() {
        new(4, 2, 0);
    }

    #[test]
    #[should_panic]
    /// Test if creating a Config with an invalid n and t combination (i.e. n <= 2t) results in an error.
    /// More precisely, test if creating a Config with n = 5 and t = 3 results in an error.
    fn fail_t_5() {
        new(5, 3, 0);
    }

    #[test]
    #[should_panic]
    /// Test if creating a Config with an invalid n and t combination (i.e. n <= 2t) results in an error.
    /// More precisely, test if creating a Config with n = 100 and t = 50 results in an error.
    fn fail_t_100() {
        new(100, 50, 0);
    }

    #[test]
    /// Test if creating a Config with an valid n, t and id combination (i.e. n > 2t, n > id) succeeds.
    /// More precisely, test if creating a Config with n = 1, t = 0 and id = 0 succeeds in an error.
    fn success_id_1() {
        new(1, 0, 0);
    }

    #[test]
    /// Test if creating a Config with a valid n, t and id combination (i.e. n > 2t, n > id) succeeds.
    /// More precisely, test if creating a Config with n = 2, t = 0 and id = 1 succeeds.
    fn success_id_2() {
        new(2, 0, 1);
    }

    #[test]
    /// Test if creating a Config with a valid n, t and id combination (i.e. n > 2t, n > id) succeeds.
    /// More precisely, test if creating a Config with n = 3, t = 0 and id = 2 succeeds.
    fn success_id_3() {
        new(3, 0, 2);
    }

    #[test]
    /// Test if creating a Config with a valid n, t and id combination (i.e. n > 2t, n > id) succeeds.
    /// More precisely, test if creating a Config with n = 4, t = 0 and id = 3 succeeds.
    fn success_id_4() {
        new(4, 0, 3);
    }

    #[test]
    /// Test if creating a Config with a valid n, t and id combination (i.e. n > 2t, n > id) succeeds.
    /// More precisely, test if creating a Config with n = 5, t = 0 and id = 4 succeeds.
    fn success_id_5() {
        new(5, 0, 4);
    }

    #[test]
    /// Test if creating a Config with a valid n, t and id combination (i.e. n > 2t, n > id) succeeds.
    /// More precisely, test if creating a Config with n = 100, t = 0 and id = 99 succeeds.
    fn success_id_100() {
        new(100, 0, 99);
    }

    #[test]
    /// Test if creating a Config with a valid n, t and id combination (i.e. n > 2t, n > id) succeeds.
    /// More precisely, test if creating a Config with n = 1, t = 0 and id = 0 succeeds.
    fn success_t_1() {
        new(1, 0, 0);
    }

    #[test]
    /// Test if creating a Config with a valid n, t and id combination (i.e. n > 2t, n > id) succeeds.
    /// More precisely, test if creating a Config with n = 2, t = 0 and id = 0 succeeds.
    fn success_t_2() {
        new(2, 0, 0);
    }

    #[test]
    /// Test if creating a Config with a valid n, t and id combination (i.e. n > 2t, n > id) succeeds.
    /// More precisely, test if creating a Config with n = 3, t = 1 and id = 0 succeeds.
    fn success_t_3() {
        new(3, 1, 0);
    }

    #[test]
    /// Test if creating a Config with a valid n, t and id combination (i.e. n > 2t, n > id) succeeds.
    /// More precisely, test if creating a Config with n = 4, t = 1 and id = 0 succeeds.
    fn success_t_4() {
        new(4, 1, 0);
    }

    #[test]
    /// Test if creating a Config with a valid n, t and id combination (i.e. n > 2t, n > id) succeeds.
    /// More precisely, test if creating a Config with n = 5, t = 2 and id = 0 succeeds.
    fn success_t_5() {
        new(5, 2, 0);
    }

    #[test]
    /// Test if creating a Config with a valid n, t and id combination (i.e. n > 2t, n > id) succeeds.
    /// More precisely, test if creating a Config with n = 100, t = 49 and id = 0 succeeds.
    fn success_t_100() {
        new(100, 49, 0);
    }

    #[test]
    /// Test if config.me() returns the correct ReplicaId set when creating a valid Config for the Replica.
    fn me() {
        for id in 0..100 {
            let config = new(100, 0, id);
            assert_eq!(config.me(), ReplicaId::from_u64(id))
        }
    }

    #[test]
    /// Test if is_primary() returns the expected primary Replica for a given View and a valid Config.
    /// More precisely, test if is_primary() returns the expected primary Replica
    /// for all possible valid Views < n. Test it with valid Configs for all possible valid variations of t
    /// with n ranging from 1 to 5 and id = 0.
    fn is_primary() {
        let iters = 100;

        let n = 1;
        for t in 0..=((n - 1) / 2) {
            let config = new(n, t, 0);
            for view in (0..iters).map(|i| n * i) {
                assert!(config.is_primary(View(view), ReplicaId::from_u64(0)));
            }
        }

        let n = 2;
        for t in 0..=((n - 1) / 2) {
            let config = new(n, t, 0);
            for view in (0..iters).map(|i| n * i) {
                assert!(config.is_primary(View(view), ReplicaId::from_u64(0)));
                assert!(config.is_primary(View(view), ReplicaId::from_u64(1)).not());

                assert!(config
                    .is_primary(View(view + 1), ReplicaId::from_u64(0))
                    .not());
                assert!(config.is_primary(View(view + 1), ReplicaId::from_u64(1)));
            }
        }

        let n = 3;
        for t in 0..=((n - 1) / 2) {
            let config = new(n, t, 0);
            for view in (0..iters).map(|i| n * i) {
                assert!(config.is_primary(View(view), ReplicaId::from_u64(0)));
                assert!(config.is_primary(View(view), ReplicaId::from_u64(1)).not());
                assert!(config.is_primary(View(view), ReplicaId::from_u64(2)).not());

                assert!(config
                    .is_primary(View(view + 1), ReplicaId::from_u64(0))
                    .not());
                assert!(config.is_primary(View(view + 1), ReplicaId::from_u64(1)));
                assert!(config
                    .is_primary(View(view + 1), ReplicaId::from_u64(2))
                    .not());

                assert!(config
                    .is_primary(View(view + 2), ReplicaId::from_u64(0))
                    .not());
                assert!(config
                    .is_primary(View(view + 2), ReplicaId::from_u64(1))
                    .not());
                assert!(config.is_primary(View(view + 2), ReplicaId::from_u64(2)));
            }
        }

        let n = 4;
        for t in 0..=((n - 1) / 2) {
            let config = new(n, t, 0);
            for view in (0..iters).map(|i| n * i) {
                assert!(config.is_primary(View(view), ReplicaId::from_u64(0)));
                assert!(config.is_primary(View(view), ReplicaId::from_u64(1)).not());
                assert!(config.is_primary(View(view), ReplicaId::from_u64(2)).not());
                assert!(config.is_primary(View(view), ReplicaId::from_u64(3)).not());

                assert!(config.is_primary(View(view + 1), ReplicaId::from_u64(1)));
                assert!(config
                    .is_primary(View(view + 1), ReplicaId::from_u64(0))
                    .not());
                assert!(config
                    .is_primary(View(view + 1), ReplicaId::from_u64(2))
                    .not());
                assert!(config
                    .is_primary(View(view + 1), ReplicaId::from_u64(3))
                    .not());

                assert!(config
                    .is_primary(View(view + 2), ReplicaId::from_u64(0))
                    .not());
                assert!(config
                    .is_primary(View(view + 2), ReplicaId::from_u64(1))
                    .not());
                assert!(config.is_primary(View(view + 2), ReplicaId::from_u64(2)));
                assert!(config
                    .is_primary(View(view + 2), ReplicaId::from_u64(3))
                    .not());

                assert!(config
                    .is_primary(View(view + 3), ReplicaId::from_u64(0))
                    .not());
                assert!(config
                    .is_primary(View(view + 3), ReplicaId::from_u64(1))
                    .not());
                assert!(config
                    .is_primary(View(view + 3), ReplicaId::from_u64(2))
                    .not());
                assert!(config.is_primary(View(view + 3), ReplicaId::from_u64(3)));
            }
        }

        let n = 5;
        for t in 0..=((n - 1) / 2) {
            let config = new(n, t, 0);
            for view in (0..iters).map(|i| n * i) {
                assert!(config.is_primary(View(view), ReplicaId::from_u64(0)));
                assert!(config.is_primary(View(view), ReplicaId::from_u64(1)).not());
                assert!(config.is_primary(View(view), ReplicaId::from_u64(2)).not());
                assert!(config.is_primary(View(view), ReplicaId::from_u64(3)).not());
                assert!(config.is_primary(View(view), ReplicaId::from_u64(4)).not());

                assert!(config.is_primary(View(view + 1), ReplicaId::from_u64(1)));
                assert!(config
                    .is_primary(View(view + 1), ReplicaId::from_u64(2))
                    .not());
                assert!(config
                    .is_primary(View(view + 1), ReplicaId::from_u64(3))
                    .not());
                assert!(config
                    .is_primary(View(view + 1), ReplicaId::from_u64(0))
                    .not());
                assert!(config
                    .is_primary(View(view + 1), ReplicaId::from_u64(4))
                    .not());

                assert!(config
                    .is_primary(View(view + 2), ReplicaId::from_u64(0))
                    .not());
                assert!(config
                    .is_primary(View(view + 2), ReplicaId::from_u64(1))
                    .not());
                assert!(config.is_primary(View(view + 2), ReplicaId::from_u64(2)));
                assert!(config
                    .is_primary(View(view + 2), ReplicaId::from_u64(3))
                    .not());
                assert!(config
                    .is_primary(View(view + 2), ReplicaId::from_u64(4))
                    .not());

                assert!(config
                    .is_primary(View(view + 3), ReplicaId::from_u64(0))
                    .not());
                assert!(config
                    .is_primary(View(view + 3), ReplicaId::from_u64(1))
                    .not());
                assert!(config
                    .is_primary(View(view + 3), ReplicaId::from_u64(2))
                    .not());
                assert!(config.is_primary(View(view + 3), ReplicaId::from_u64(3)));
                assert!(config
                    .is_primary(View(view + 3), ReplicaId::from_u64(4))
                    .not());

                assert!(config
                    .is_primary(View(view + 4), ReplicaId::from_u64(0))
                    .not());
                assert!(config
                    .is_primary(View(view + 4), ReplicaId::from_u64(1))
                    .not());
                assert!(config
                    .is_primary(View(view + 4), ReplicaId::from_u64(2))
                    .not());
                assert!(config
                    .is_primary(View(view + 4), ReplicaId::from_u64(3))
                    .not());
                assert!(config.is_primary(View(view + 4), ReplicaId::from_u64(4)));
            }
        }
    }

    #[test]
    /// Test if me_primary() returns the expected bool for a given View and a valid Config.
    /// More precisely, test if me_primary() returns true if the the Replica
    /// to which the Config belongs to is the primary for the given View.
    /// Test it with valid Configs for all possible valid variations of t
    /// with n ranging from 1 to 5.
    fn me_primary() {
        let iters = 100;

        let n = 1;
        for t in 0..=((n - 1) / 2) {
            for view in (0..iters).map(|i| n * i) {
                assert!(new(n, t, 0).me_primary(View(view)));
            }
        }

        let n = 2;
        for t in 0..=((n - 1) / 2) {
            for view in (0..iters).map(|i| n * i) {
                assert!(new(n, t, 0).me_primary(View(view)));
                assert!(new(n, t, 1).me_primary(View(view)).not());

                assert!(new(n, t, 0).me_primary(View(view + 1)).not());
                assert!(new(n, t, 1).me_primary(View(view + 1)));
            }
        }

        let n = 3;
        for t in 0..=((n - 1) / 2) {
            for view in (0..iters).map(|i| n * i) {
                assert!(new(n, t, 0).me_primary(View(view)));
                assert!(new(n, t, 1).me_primary(View(view)).not());
                assert!(new(n, t, 2).me_primary(View(view)).not());

                assert!(new(n, t, 0).me_primary(View(view + 1)).not());
                assert!(new(n, t, 1).me_primary(View(view + 1)));
                assert!(new(n, t, 2).me_primary(View(view + 1)).not());

                assert!(new(n, t, 0).me_primary(View(view + 2)).not());
                assert!(new(n, t, 1).me_primary(View(view + 2)).not());
                assert!(new(n, t, 2).me_primary(View(view + 2)));
            }
        }

        let n = 4;
        for t in 0..=((n - 1) / 2) {
            for view in (0..iters).map(|i| n * i) {
                assert!(new(n, t, 0).me_primary(View(view)));
                assert!(new(n, t, 1).me_primary(View(view)).not());
                assert!(new(n, t, 2).me_primary(View(view)).not());
                assert!(new(n, t, 3).me_primary(View(view)).not());

                assert!(new(n, t, 1).me_primary(View(view + 1)));
                assert!(new(n, t, 0).me_primary(View(view + 1)).not());
                assert!(new(n, t, 2).me_primary(View(view + 1)).not());
                assert!(new(n, t, 3).me_primary(View(view + 1)).not());

                assert!(new(n, t, 0).me_primary(View(view + 2)).not());
                assert!(new(n, t, 1).me_primary(View(view + 2)).not());
                assert!(new(n, t, 2).me_primary(View(view + 2)));
                assert!(new(n, t, 3).me_primary(View(view + 2)).not());

                assert!(new(n, t, 0).me_primary(View(view + 3)).not());
                assert!(new(n, t, 1).me_primary(View(view + 3)).not());
                assert!(new(n, t, 2).me_primary(View(view + 3)).not());
                assert!(new(n, t, 3).me_primary(View(view + 3)));
            }
        }

        let n = 5;
        for t in 0..=((n - 1) / 2) {
            for view in (0..iters).map(|i| n * i) {
                assert!(new(n, t, 0).me_primary(View(view)));
                assert!(new(n, t, 1).me_primary(View(view)).not());
                assert!(new(n, t, 2).me_primary(View(view)).not());
                assert!(new(n, t, 3).me_primary(View(view)).not());
                assert!(new(n, t, 4).me_primary(View(view)).not());

                assert!(new(n, t, 1).me_primary(View(view + 1)));
                assert!(new(n, t, 2).me_primary(View(view + 1)).not());
                assert!(new(n, t, 3).me_primary(View(view + 1)).not());
                assert!(new(n, t, 0).me_primary(View(view + 1)).not());
                assert!(new(n, t, 4).me_primary(View(view + 1)).not());

                assert!(new(n, t, 0).me_primary(View(view + 2)).not());
                assert!(new(n, t, 1).me_primary(View(view + 2)).not());
                assert!(new(n, t, 2).me_primary(View(view + 2)));
                assert!(new(n, t, 3).me_primary(View(view + 2)).not());
                assert!(new(n, t, 4).me_primary(View(view + 2)).not());

                assert!(new(n, t, 0).me_primary(View(view + 3)).not());
                assert!(new(n, t, 1).me_primary(View(view + 3)).not());
                assert!(new(n, t, 2).me_primary(View(view + 3)).not());
                assert!(new(n, t, 3).me_primary(View(view + 3)));
                assert!(new(n, t, 4).me_primary(View(view + 3)).not());

                assert!(new(n, t, 0).me_primary(View(view + 4)).not());
                assert!(new(n, t, 1).me_primary(View(view + 4)).not());
                assert!(new(n, t, 2).me_primary(View(view + 4)).not());
                assert!(new(n, t, 3).me_primary(View(view + 4)).not());
                assert!(new(n, t, 4).me_primary(View(view + 4)));
            }
        }
    }

    #[test]
    /// Test if all_replicas() returns all expected ReplicaIds when creating
    /// valid Configurations.
    /// More precisely, test if all_replicas() returns all expected ReplicaIds
    /// when creating Configurations with n ranging from 1 to 5 and t, id = 0.
    fn all_replicas() {
        let n = 1;
        let config = new(n, 0, 0);
        let mut iter = config.all_replicas();
        assert_eq!(iter.next(), Some(ReplicaId::from_u64(0)));
        assert_eq!(iter.next(), None);

        let n = 2;
        let config = new(n, 0, 0);
        let mut iter = config.all_replicas();
        assert_eq!(iter.next(), Some(ReplicaId::from_u64(0)));
        assert_eq!(iter.next(), Some(ReplicaId::from_u64(1)));
        assert_eq!(iter.next(), None);

        let n = 3;
        let config = new(n, 0, 0);
        let mut iter = config.all_replicas();
        assert_eq!(iter.next(), Some(ReplicaId::from_u64(0)));
        assert_eq!(iter.next(), Some(ReplicaId::from_u64(1)));
        assert_eq!(iter.next(), Some(ReplicaId::from_u64(2)));
        assert_eq!(iter.next(), None);

        let n = 4;
        let config = new(n, 0, 0);
        let mut iter = config.all_replicas();
        assert_eq!(iter.next(), Some(ReplicaId::from_u64(0)));
        assert_eq!(iter.next(), Some(ReplicaId::from_u64(1)));
        assert_eq!(iter.next(), Some(ReplicaId::from_u64(2)));
        assert_eq!(iter.next(), Some(ReplicaId::from_u64(3)));
        assert_eq!(iter.next(), None);

        let n = 5;
        let config = new(n, 0, 0);
        let mut iter = config.all_replicas();
        assert_eq!(iter.next(), Some(ReplicaId::from_u64(0)));
        assert_eq!(iter.next(), Some(ReplicaId::from_u64(1)));
        assert_eq!(iter.next(), Some(ReplicaId::from_u64(2)));
        assert_eq!(iter.next(), Some(ReplicaId::from_u64(3)));
        assert_eq!(iter.next(), Some(ReplicaId::from_u64(4)));
        assert_eq!(iter.next(), None);
    }
}
