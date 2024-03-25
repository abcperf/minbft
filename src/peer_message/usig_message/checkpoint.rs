//! Defines a message of type [Checkpoint].
//! A [Checkpoint] consists of two main parts.
//! The first part is its content.
//! It contains the origin of the [Checkpoint], i.e., the ID of the replica
//! ([ReplicaId]) which created the [Checkpoint].
//! Moreover, it contains the hash of the [crate::MinBft]'s state, i.e. the
//! [CheckpointHash].
//! Furthermore, it contains the counter of the most recently accepted
//! [crate::Prepare] by the replica.
//! It also keeps track of the total amount of accepted batches until this
//! [Checkpoint].
//! The second part is its USIG signature.
//! In our implementation, [Checkpoint]s are USIG signed - this seems to differ
//! from the paper "Efficient Byzantine Fault Tolerance" by Veronese et al.
//! A [Checkpoint] is broadcast by a replica when enough client requests have
//! been accepted.
//! For further explanation, refer to the documentation in [crate::MinBft] or
//! the paper "Efficient Byzantine Fault-Tolerance" by Veronese et al.

use core::fmt;
use std::collections::HashSet;

use anyhow::Result;
use blake2::digest::Update;
use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use tracing::{error, trace};
use usig::{Count, Usig};

use crate::{error::InnerError, Config, ReplicaId};

use super::signed::{UsigSignable, UsigSigned};

pub(crate) type CheckpointHash = [u8; 64];

/// The content of a message of type [Checkpoint].
/// Consists of the hash of the state of the [crate::MinBft].
/// Furthermore, the origin that created the [Checkpoint] is necessary.
#[serde_as]
#[derive(Serialize, Deserialize, Clone, Debug)]
pub(crate) struct CheckpointContent {
    /// Used for keeping track of which replica created the message of type
    /// Checkpoint.
    pub(crate) origin: ReplicaId,
    /// The counter of the most recently accepted [crate::Prepare].
    pub(crate) counter_latest_prep: Count,
    /// The hash of the [crate::MinBft]'s state.
    /// All replicas must have equal state.
    #[serde_as(as = "serde_with::Bytes")]
    pub(crate) state_hash: CheckpointHash,
    /// Keeps count of the total amount of accepted batches until this
    /// [Checkpoint].
    pub(crate) total_amount_accepted_batches: u64,
}

impl AsRef<ReplicaId> for CheckpointContent {
    /// Referencing [CheckpointContent] returns a reference to the origin in the
    /// CheckpointContent.
    fn as_ref(&self) -> &ReplicaId {
        &self.origin
    }
}

impl UsigSignable for CheckpointContent {
    /// Hashes the content of a message of type [Checkpoint].
    /// Required for signing and verifying a message of type [Checkpoint].
    fn hash_content<H: Update>(&self, hasher: &mut H) {
        let encoded = bincode::serialize(self).unwrap();
        hasher.update(&encoded);
    }
}

/// The message of type [Checkpoint].
/// [Checkpoint]s consist of their content and must be signed by a USIG.
/// Such a message is broadcast by a replica in response to having accepted a
/// sufficient amount of client requests (for further explanation, refer to
/// [crate::Config], [crate::request_processor::RequestProcessor]).
/// [Checkpoint]s can and should be validated.
pub(crate) type Checkpoint<Sig> = UsigSigned<CheckpointContent, Sig>;

impl<Sig> fmt::Display for Checkpoint<Sig> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "(origin: {0}, counter of latest prepare: {1}, total amount accepted 
            batches: {2})",
            self.origin, self.counter_latest_prep.0, self.total_amount_accepted_batches
        )
    }
}

impl<Sig: Serialize> Checkpoint<Sig> {
    /// Validates a message of type [Checkpoint].
    /// To validate it, its USIG signature must be verified.
    ///
    /// # Arguments
    ///
    /// * `config` - The [Config] of the algorithm.
    /// * `usig` - The [USIG] signature that should be a valid one for the
    ///            [Checkpoint] message.
    pub(crate) fn validate(
        &self,
        config: &Config,
        usig: &mut impl Usig<Signature = Sig>,
    ) -> Result<(), InnerError> {
        trace!(
            "Validating Checkpoint (origin: {:?}, counter of latest accepted 
            Prepare: {:?}, amount accepted batches: {:?}) ...",
            self.origin,
            self.counter_latest_prep,
            self.total_amount_accepted_batches
        );
        trace!(
            "Verifying signature of Checkpoint (origin: {:?}, counter of 
            latest accepted Prepare: {:?}, amount accepted batches: {:?}) ...",
            self.origin,
            self.counter_latest_prep,
            self.total_amount_accepted_batches
        );
        self.verify(usig).map_or_else(
            |usig_error| {
                error!(
                    "Failed validating Checkpoint (origin: {:?}, counter of 
                latest accepted Prepare: {:?}, amount accepted batches: {:?}): 
                Verification of the signature failed.",
                    self.origin, self.counter_latest_prep, self.total_amount_accepted_batches
                );
                Err(InnerError::parse_usig_error(
                    usig_error,
                    config.id,
                    "Checkpoint",
                    self.origin,
                ))
            },
            |v| {
                trace!(
                    "Successfully verified signature of Checkpoint (origin: {:?}, 
                counter of latest accepted Prepare: {:?}, amount accepted 
                batches: {:?}).",
                    self.origin,
                    self.counter_latest_prep,
                    self.total_amount_accepted_batches
                );
                trace!(
                    "Successfully validated Checkpoint (origin: {:?}, counter of 
                latest accepted Prepare: {:?}, amount accepted batches: {:?}) ...",
                    self.origin,
                    self.counter_latest_prep,
                    self.total_amount_accepted_batches
                );
                Ok(v)
            },
        )
    }
}

/// The (stable) certificate containing a set of valid messages of type
/// [Checkpoint].
/// Following conditions must be met for the certificate to become stable:
///     (1) The certificate must contain at least `t + 1` [Checkpoint]s
///         (for further explanation regarding `t`, see [crate::Config]).
///     (2) They have to originate from different replicas.
///     (3) They have to share the same [CheckpointHash].
///     (4) They have to share the same counter of the latest accepted [crate::Prepare].
/// If a certificate does not (yet) meet all aforementioned conditions, it is
/// refered to as non-stable until the conditions are met.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub(crate) struct CheckpointCertificate<Sig> {
    /// The message of type [Checkpoint] created by the replica itself.
    /// In its details, the struct differs from the paper.
    /// Reason: We have to clear all messages from the replica's log of sent
    ///         messages that have a counter lower than the counter of its
    ///         [Checkpoint].
    ///         By saving at this stage the replica's own [Checkpoint],
    ///         we can safely remove all aforementioned messages.
    pub(crate) my_checkpoint: Checkpoint<Sig>,
    /// All other messages of type [Checkpoint] apart from the replica's own
    /// [Checkpoint].
    pub(crate) other_checkpoints: Vec<Checkpoint<Sig>>,
}

impl<Sig: Serialize> CheckpointCertificate<Sig> {
    /// Validates the [CheckpointCertificate].
    /// Following conditions must be met for the certificate to be considered
    /// valid:
    ///     (1) The certificate must contain at least `t + 1` [Checkpoint]s
    ///         (for further explanation regarding `t`, see [crate::Config]).
    ///     (2) They have to originate from different replicas.
    ///     (3) They have to share the same [CheckpointHash].
    ///     (4) They have to share the same counter of the latest accepted
    ///         prepare.
    ///     (5) Their USIG signatures have to be valid.
    ///
    /// # Arguments
    ///
    /// * `config` - The [Config] of the algorithm.
    /// * `usig` - The [USIG] signature that should be a valid one for the
    ///            [Checkpoint] messages.
    pub(crate) fn validate(
        &self,
        config: &Config,
        usig: &mut impl Usig<Signature = Sig>,
    ) -> Result<(), InnerError> {
        trace!(
            "Validating checkpoint certificate (origin: {:?}, counter of 
            latest accepted Prepare: {:?}, amount accepted batches: {:?}) ...",
            self.my_checkpoint.origin,
            self.my_checkpoint.counter_latest_prep,
            self.my_checkpoint.total_amount_accepted_batches
        );

        // Check for condition (1).
        // Assures that the CheckpointCertificate contains at least `t + 1`
        // messages of type Checkpoint, (one of them is implicitly the
        // Checkpoint of the origin of the CheckpointCertificate).
        if (self.other_checkpoints.len() as u64) < config.t {
            error!(
                "Failed validating checkpoint certificate (origin: {:?}, 
                counter of latest accepted Prepare: {:?}, amount accepted 
                batches: {:?}): Checkpoint certificate does not contain 
                sufficient checkpoints. For further information see output.",
                self.my_checkpoint.origin,
                self.my_checkpoint.counter_latest_prep,
                self.my_checkpoint.total_amount_accepted_batches
            );
            return Err(InnerError::CheckpointCertNotSufficientMsgs {
                receiver: config.id,
                origin: self.my_checkpoint.origin,
            });
        }

        // Check for condition (2).
        // Assures that all Checkpoints originate from different replicas.
        let mut origins = HashSet::new();
        origins.insert(self.my_checkpoint.origin);
        for msg in &self.other_checkpoints {
            if !origins.insert(msg.origin) {
                error!(
                    "Failed validating checkpoint certificate (origin: {:?}, 
                    counter of latest accepted Prepare: {:?}, amount accepted 
                    batches: {:?}): Not all checkpoints contained in certificate 
                    originate from different replicas. For further information 
                    see output.",
                    self.my_checkpoint.origin,
                    self.my_checkpoint.counter_latest_prep,
                    self.my_checkpoint.total_amount_accepted_batches
                );
                return Err(InnerError::CheckpointCertNotAllDifferentOrigin {
                    receiver: config.id,
                    origin: self.my_checkpoint.origin,
                });
            }
        }

        // Check for condition (3).
        // Assures that the CheckpointHash of the messages of type Checkpoint
        // is equal to the CheckpointHash of the replica's own Checkpoint
        // (and are therefore all equal).
        for other in &self.other_checkpoints {
            if self.my_checkpoint.state_hash != other.state_hash {
                error!(
                    "Failed validating checkpoint certificate ({0}): Not all 
                checkpoints contained in certificate agree on the same state 
                hash. For further information see output.",
                    self.my_checkpoint
                );
                return Err(InnerError::CheckpointCertNotAllSameStateHash {
                    receiver: config.id,
                    origin: self.my_checkpoint.origin,
                });
            }
        }

        // Check for condition (4).
        // Assures that the all checkpoints agree on the counter of the latest
        // accepted prepare.
        for other in &self.other_checkpoints {
            if self.my_checkpoint.counter_latest_prep != other.counter_latest_prep {
                error!(
                    "Failed validating checkpoint certificate ({0}): Not all 
                checkpoints contained in certificate agree on the same counter 
                of the latest accepted prepare. For further information see 
                output.",
                    self.my_checkpoint
                );
                return Err(InnerError::CheckpointCertNotAllSameLatestPrep {
                    receiver: config.id,
                    origin: self.my_checkpoint.origin,
                });
            }
        }

        // Check for condition (5).
        // Assures the signatures of all Checkpoints are valid.
        trace!(
            "Validating checkpoints contained in certificate (origin: {:?}, 
            counter of latest accepted Prepare: {:?}, amount accepted batches: {:?}) ...",
            self.my_checkpoint.origin,
            self.my_checkpoint.counter_latest_prep,
            self.my_checkpoint.total_amount_accepted_batches
        );
        self.my_checkpoint.validate(config, usig)?;
        for msg in &self.other_checkpoints {
            match msg.validate(config, usig) {
                Ok(_) => (),
                Err(e) => return Err(e),
            }
        }
        trace!(
            "Successfully validated checkpoints contained in certificate 
        (origin: {:?}, counter of latest accepted Prepare: {:?}, amount accepted 
            batches: {:?}).",
            self.my_checkpoint.origin,
            self.my_checkpoint.counter_latest_prep,
            self.my_checkpoint.total_amount_accepted_batches
        );
        trace!(
            "Successfully validated checkpoint certificate (origin: {:?}, 
            counter of latest accepted Prepare: {:?}, amount accepted batches: {:?}).",
            self.my_checkpoint.origin,
            self.my_checkpoint.counter_latest_prep,
            self.my_checkpoint.total_amount_accepted_batches
        );
        Ok(())
    }
}

#[cfg(test)]

mod test {
    use std::{num::NonZeroU64, time::Duration};

    use rand::Rng;
    use shared_ids::{AnyId, ReplicaId};
    use usig::{noop::UsigNoOp, Count, Usig};

    use crate::{
        error::InnerError,
        tests::{create_default_checkpoint, create_random_state_hash},
        Config,
    };

    use super::{Checkpoint, CheckpointCertificate};

    /// Tests if the validation of a [CheckpointCertificate], which does not
    /// contain a sufficient amount of [Checkpoint]s, results in an error.
    #[test]
    fn validate_cert_not_enough_msgs() {
        let mut usig_0 = UsigNoOp::default();

        let mut rng = rand::thread_rng();
        let rand_counter_last_prep: u64 = rng.gen();
        let rand_total_accepted_batches: u64 = rng.gen();

        let state_hash = create_random_state_hash();

        let my_checkpoint = Checkpoint::sign(
            super::CheckpointContent {
                origin: ReplicaId::from_u64(0),
                state_hash,
                counter_latest_prep: Count(rand_counter_last_prep),
                total_amount_accepted_batches: rand_total_accepted_batches,
            },
            &mut usig_0,
        )
        .unwrap();

        let mut usig_1 = UsigNoOp::default();

        usig_0.add_remote_party(ReplicaId::from_u64(0), ());
        usig_0.add_remote_party(ReplicaId::from_u64(1), ());
        usig_1.add_remote_party(ReplicaId::from_u64(0), ());
        usig_1.add_remote_party(ReplicaId::from_u64(1), ());

        let other_checkpoint = Checkpoint::sign(
            super::CheckpointContent {
                origin: ReplicaId::from_u64(1),
                state_hash,
                counter_latest_prep: Count(rand_counter_last_prep),
                total_amount_accepted_batches: rand_total_accepted_batches,
            },
            &mut usig_1,
        )
        .unwrap();

        let other_checkpoints = vec![other_checkpoint];

        let config_origin = Config {
            n: NonZeroU64::new(5).unwrap(),
            t: 2,
            id: ReplicaId::from_u64(0),
            batch_timeout: Duration::from_secs(2),
            max_batch_size: None,
            initial_timeout_duration: Duration::from_secs(2),
            checkpoint_period: NonZeroU64::new(2).unwrap(),
        };

        let config_other = Config {
            n: NonZeroU64::new(5).unwrap(),
            t: 2,
            id: ReplicaId::from_u64(1),
            batch_timeout: Duration::from_secs(2),
            max_batch_size: None,
            initial_timeout_duration: Duration::from_secs(2),
            checkpoint_period: NonZeroU64::new(2).unwrap(),
        };

        let cert = CheckpointCertificate {
            my_checkpoint,
            other_checkpoints,
        };

        assert!(cert.validate(&config_origin, &mut usig_0).is_err());

        assert!(matches!(
            cert.validate(&config_origin, &mut usig_0).err().unwrap(),
            InnerError::CheckpointCertNotSufficientMsgs { receiver, origin }
            if receiver == ReplicaId::from_u64(0) && origin == ReplicaId::from_u64(0),
        ));

        assert!(cert.validate(&config_other, &mut usig_1).is_err());

        assert!(matches!(
            cert.validate(&config_other, &mut usig_1).err().unwrap(),
            InnerError::CheckpointCertNotSufficientMsgs { receiver, origin }
            if receiver == ReplicaId::from_u64(1) && origin == ReplicaId::from_u64(0),
        ));
    }

    /// Tests if the validation of a [CheckpointCertificate], in which all
    /// contained [Checkpoint]s do not have the same state hash, results in an error.
    #[test]
    fn validate_cert_not_all_same_state_hash() {
        let mut usig_0 = UsigNoOp::default();

        let mut rng = rand::thread_rng();
        let rand_counter_last_prep: u64 = rng.gen();
        let rand_total_accepted_batches: u64 = rng.gen();

        let my_checkpoint = Checkpoint::sign(
            super::CheckpointContent {
                origin: ReplicaId::from_u64(0),
                state_hash: [0; 64],
                counter_latest_prep: Count(rand_counter_last_prep),
                total_amount_accepted_batches: rand_total_accepted_batches,
            },
            &mut usig_0,
        )
        .unwrap();

        let mut usig_1 = UsigNoOp::default();

        usig_0.add_remote_party(ReplicaId::from_u64(0), ());
        usig_0.add_remote_party(ReplicaId::from_u64(1), ());
        usig_1.add_remote_party(ReplicaId::from_u64(0), ());
        usig_1.add_remote_party(ReplicaId::from_u64(1), ());

        let other_checkpoint = Checkpoint::sign(
            super::CheckpointContent {
                origin: ReplicaId::from_u64(1),
                state_hash: [1; 64],
                counter_latest_prep: Count(rand_counter_last_prep),
                total_amount_accepted_batches: rand_total_accepted_batches,
            },
            &mut usig_1,
        )
        .unwrap();

        let other_checkpoints = vec![other_checkpoint];

        let config_origin = Config {
            n: NonZeroU64::new(3).unwrap(),
            t: 1,
            id: ReplicaId::from_u64(0),
            batch_timeout: Duration::from_secs(2),
            max_batch_size: None,
            initial_timeout_duration: Duration::from_secs(2),
            checkpoint_period: NonZeroU64::new(2).unwrap(),
        };

        let config_other = Config {
            n: NonZeroU64::new(3).unwrap(),
            t: 1,
            id: ReplicaId::from_u64(1),
            batch_timeout: Duration::from_secs(2),
            max_batch_size: None,
            initial_timeout_duration: Duration::from_secs(2),
            checkpoint_period: NonZeroU64::new(2).unwrap(),
        };

        let cert = CheckpointCertificate {
            my_checkpoint,
            other_checkpoints,
        };

        assert!(cert.validate(&config_origin, &mut usig_0).is_err());

        assert!(matches!(
            cert.validate(&config_origin, &mut usig_0).err().unwrap(),
            InnerError::CheckpointCertNotAllSameStateHash { receiver, origin }
            if receiver == ReplicaId::from_u64(0) && origin == ReplicaId::from_u64(0),
        ));

        assert!(cert.validate(&config_other, &mut usig_1).is_err());

        assert!(matches!(
            cert.validate(&config_other, &mut usig_1).err().unwrap(),
            InnerError::CheckpointCertNotAllSameStateHash { receiver, origin }
            if receiver == ReplicaId::from_u64(1) && origin == ReplicaId::from_u64(0),
        ));
    }

    /// Tests if the validation of a [CheckpointCertificate], in which not all
    /// [Checkpoint]s originate from different replicas, results in an error.
    #[test]
    fn validate_cert_not_all_different_origins() {
        let mut usig_0 = UsigNoOp::default();

        let mut rng = rand::thread_rng();
        let rand_counter_last_prep: u64 = rng.gen();
        let rand_total_accepted_batches: u64 = rng.gen();

        let state_hash = create_random_state_hash();

        let my_checkpoint = Checkpoint::sign(
            super::CheckpointContent {
                origin: ReplicaId::from_u64(0),
                state_hash,
                counter_latest_prep: Count(rand_counter_last_prep),
                total_amount_accepted_batches: rand_total_accepted_batches,
            },
            &mut usig_0,
        )
        .unwrap();

        let mut usig_1 = UsigNoOp::default();

        usig_0.add_remote_party(ReplicaId::from_u64(0), ());
        usig_0.add_remote_party(ReplicaId::from_u64(1), ());
        usig_1.add_remote_party(ReplicaId::from_u64(0), ());
        usig_1.add_remote_party(ReplicaId::from_u64(1), ());

        let other_checkpoint = Checkpoint::sign(
            super::CheckpointContent {
                origin: ReplicaId::from_u64(0),
                state_hash,
                counter_latest_prep: Count(rand_counter_last_prep),
                total_amount_accepted_batches: rand_total_accepted_batches,
            },
            &mut usig_1,
        )
        .unwrap();

        let other_checkpoints = vec![other_checkpoint];

        let config = Config {
            n: NonZeroU64::new(3).unwrap(),
            t: 1,
            id: ReplicaId::from_u64(0),
            batch_timeout: Duration::from_secs(2),
            max_batch_size: None,
            initial_timeout_duration: Duration::from_secs(2),
            checkpoint_period: NonZeroU64::new(2).unwrap(),
        };

        let cert = CheckpointCertificate {
            my_checkpoint,
            other_checkpoints,
        };

        assert!(cert.validate(&config, &mut usig_0).is_err());

        assert!(matches!(
            cert.validate(&config, &mut usig_0).err().unwrap(),
            InnerError::CheckpointCertNotAllDifferentOrigin { receiver, origin }
            if receiver == ReplicaId::from_u64(0) && origin == ReplicaId::from_u64(0),
        ));
    }

    /// Tests if the validation of a [CheckpointCertificate], in which not all
    /// [Checkpoint]s originate from known replicas (previously added as remote parties), results in an error.
    #[test]
    fn validate_cert_checkpoint_invalid_usig() {
        let mut usig_0 = UsigNoOp::default();

        let mut rng = rand::thread_rng();
        let rand_counter_last_prep: u64 = rng.gen();
        let rand_total_accepted_batches: u64 = rng.gen();

        let state_hash = create_random_state_hash();

        let my_checkpoint = Checkpoint::sign(
            super::CheckpointContent {
                origin: ReplicaId::from_u64(0),
                state_hash,
                counter_latest_prep: Count(rand_counter_last_prep),
                total_amount_accepted_batches: rand_total_accepted_batches,
            },
            &mut usig_0,
        )
        .unwrap();

        let mut usig_1 = UsigNoOp::default();

        usig_0.add_remote_party(ReplicaId::from_u64(0), ());
        usig_0.add_remote_party(ReplicaId::from_u64(1), ());
        usig_1.add_remote_party(ReplicaId::from_u64(1), ());

        let other_checkpoint = Checkpoint::sign(
            super::CheckpointContent {
                origin: ReplicaId::from_u64(1),
                state_hash,
                counter_latest_prep: Count(rand_counter_last_prep),
                total_amount_accepted_batches: rand_total_accepted_batches,
            },
            &mut usig_1,
        )
        .unwrap();

        let other_checkpoints = vec![other_checkpoint];

        let config_origin = Config {
            n: NonZeroU64::new(3).unwrap(),
            t: 1,
            id: ReplicaId::from_u64(0),
            batch_timeout: Duration::from_secs(2),
            max_batch_size: None,
            initial_timeout_duration: Duration::from_secs(2),
            checkpoint_period: NonZeroU64::new(2).unwrap(),
        };

        let config_other = Config {
            n: NonZeroU64::new(3).unwrap(),
            t: 1,
            id: ReplicaId::from_u64(0),
            batch_timeout: Duration::from_secs(2),
            max_batch_size: None,
            initial_timeout_duration: Duration::from_secs(2),
            checkpoint_period: NonZeroU64::new(2).unwrap(),
        };

        let cert = CheckpointCertificate {
            my_checkpoint,
            other_checkpoints,
        };

        assert!(cert.validate(&config_origin, &mut usig_0).is_ok());

        assert!(cert.validate(&config_other, &mut usig_1).is_err());

        // TODO: Is this expected?
        // assert!(matches!(
        //     cert.validate(&config_other, &mut usig_1).err().unwrap(),
        //     InnerError::Usig { usig_error: _, replica, msg_type, origin }
        //     if replica == ReplicaId::from_u64(1) && origin == ReplicaId::from_u64(0) && msg_type == "Checkpoint",
        // ));
    }

    /// Tests if the validation of a [CheckpointCertificate], in which not all
    /// [Checkpoint]s contain the same counter of the last Prepare, results in
    /// an error.
    #[test]
    fn validate_cert_checkpoint_invalid_latest_prep() {
        let mut usig_0 = UsigNoOp::default();

        let mut rng = rand::thread_rng();
        let rand_total_accepted_batches: u64 = rng.gen();

        let state_hash = create_random_state_hash();

        let my_checkpoint = Checkpoint::sign(
            super::CheckpointContent {
                origin: ReplicaId::from_u64(0),
                state_hash,
                counter_latest_prep: Count(0),
                total_amount_accepted_batches: rand_total_accepted_batches,
            },
            &mut usig_0,
        )
        .unwrap();

        let mut usig_1 = UsigNoOp::default();

        usig_0.add_remote_party(ReplicaId::from_u64(0), ());
        usig_0.add_remote_party(ReplicaId::from_u64(1), ());
        usig_1.add_remote_party(ReplicaId::from_u64(0), ());
        usig_1.add_remote_party(ReplicaId::from_u64(1), ());

        let other_checkpoint = Checkpoint::sign(
            super::CheckpointContent {
                origin: ReplicaId::from_u64(1),
                state_hash,
                counter_latest_prep: Count(1),
                total_amount_accepted_batches: rand_total_accepted_batches,
            },
            &mut usig_1,
        )
        .unwrap();

        let other_checkpoints = vec![other_checkpoint];

        let config_origin = Config {
            n: NonZeroU64::new(3).unwrap(),
            t: 1,
            id: ReplicaId::from_u64(0),
            batch_timeout: Duration::from_secs(2),
            max_batch_size: None,
            initial_timeout_duration: Duration::from_secs(2),
            checkpoint_period: NonZeroU64::new(2).unwrap(),
        };

        let config_other = Config {
            n: NonZeroU64::new(3).unwrap(),
            t: 1,
            id: ReplicaId::from_u64(1),
            batch_timeout: Duration::from_secs(2),
            max_batch_size: None,
            initial_timeout_duration: Duration::from_secs(2),
            checkpoint_period: NonZeroU64::new(2).unwrap(),
        };

        let cert = CheckpointCertificate {
            my_checkpoint,
            other_checkpoints,
        };

        assert!(cert.validate(&config_origin, &mut usig_0).is_err());

        assert!(matches!(
            cert.validate(&config_origin, &mut usig_0).err().unwrap(),
            InnerError::CheckpointCertNotAllSameLatestPrep { receiver, origin }
            if receiver == ReplicaId::from_u64(0) && origin == ReplicaId::from_u64(0),
        ));

        assert!(cert.validate(&config_other, &mut usig_1).is_err());

        assert!(matches!(
            cert.validate(&config_other, &mut usig_1).err().unwrap(),
            InnerError::CheckpointCertNotAllSameLatestPrep { receiver, origin }
            if receiver == ReplicaId::from_u64(1) && origin == ReplicaId::from_u64(0),
        ));
    }

    /// Tests if the validation of a valid [CheckpointCertificate] succeeds.
    #[test]
    fn validate_cert_valid() {
        let mut usig_0 = UsigNoOp::default();

        let mut rng = rand::thread_rng();
        let rand_counter_last_prep: u64 = rng.gen();
        let rand_total_accepted_batches: u64 = rng.gen();

        let state_hash = create_random_state_hash();

        let my_checkpoint = Checkpoint::sign(
            super::CheckpointContent {
                origin: ReplicaId::from_u64(0),
                state_hash,
                counter_latest_prep: Count(rand_counter_last_prep),
                total_amount_accepted_batches: rand_total_accepted_batches,
            },
            &mut usig_0,
        )
        .unwrap();

        let mut usig_1 = UsigNoOp::default();

        usig_0.add_remote_party(ReplicaId::from_u64(0), ());
        usig_0.add_remote_party(ReplicaId::from_u64(1), ());
        usig_1.add_remote_party(ReplicaId::from_u64(0), ());
        usig_1.add_remote_party(ReplicaId::from_u64(1), ());

        let other_checkpoint = Checkpoint::sign(
            super::CheckpointContent {
                origin: ReplicaId::from_u64(1),
                state_hash,
                counter_latest_prep: Count(rand_counter_last_prep),
                total_amount_accepted_batches: rand_total_accepted_batches,
            },
            &mut usig_1,
        )
        .unwrap();

        let other_checkpoints = vec![other_checkpoint];

        let config = Config {
            n: NonZeroU64::new(3).unwrap(),
            t: 1,
            id: ReplicaId::from_u64(0),
            batch_timeout: Duration::from_secs(2),
            max_batch_size: None,
            initial_timeout_duration: Duration::from_secs(2),
            checkpoint_period: NonZeroU64::new(2).unwrap(),
        };

        let cert = CheckpointCertificate {
            my_checkpoint,
            other_checkpoints,
        };

        assert!(cert.validate(&config, &mut usig_0).is_ok());
        assert!(cert.validate(&config, &mut usig_1).is_ok());
    }

    /// Tests if the reference of a [ViewPeerMessage] that wraps a [crate::Prepare]
    /// corresponds to the reference of the underlying [crate::Prepare].
    #[test]
    fn ref_returns_origin_ref() {
        let origin = ReplicaId::from_u64(0);
        let cp = create_default_checkpoint(origin);
        assert_eq!(cp.origin.as_u64(), origin.as_u64());
        assert_eq!(cp.as_ref().as_u64(), origin.as_u64());
    }
}
