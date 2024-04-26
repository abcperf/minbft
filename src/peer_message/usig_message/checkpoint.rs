//! Defines a message of type [Checkpoint].\
//! A [Checkpoint] is broadcast by a replica when enough client requests have
//! been accepted.\
//! In our implementation, [Checkpoint]s are USIG signed - this seems to differ
//! from the paper "Efficient Byzantine Fault Tolerance" by Veronese et al.

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

/// The content of a message of type Checkpoint.\
/// Contains the ID of the replica to which the Checkpoint belongs to.\
/// Contains the counter of the most recently accepted Prepare.\
/// Consists of the hash of the state of the MinBft\.
/// Furthermore, it contains the amount of accepted batches until now.
#[serde_as]
#[derive(Serialize, Deserialize, Clone, Debug)]
pub(crate) struct CheckpointContent {
    /// Used for keeping track of which replica created the message of type
    /// Checkpoint.
    pub(crate) origin: ReplicaId,
    /// The counter of the most recently accepted Prepare.
    pub(crate) counter_latest_prep: Count,
    /// The hash of the MinBft's state.\
    /// All replicas must have equal state.
    #[serde_as(as = "serde_with::Bytes")]
    pub(crate) state_hash: CheckpointHash,
    /// Keeps count of the total amount of accepted batches until this
    /// Checkpoint.
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

/// The message of type [Checkpoint].\
/// [Checkpoint]s consist of their content and must be signed by a USIG.\
/// Such a message is broadcast by a replica in response to having accepted a
/// sufficient amount of client requests (for further explanation, refer to
/// [crate::Config], [crate::request_processor::RequestProcessor]).\
pub(crate) type Checkpoint<Sig> = UsigSigned<CheckpointContent, Sig>;

impl<Sig> fmt::Display for Checkpoint<Sig> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "(origin: {0}, counter of latest prepare: {1}, total amount accepted 
            batches: {2})",
            self.origin.as_u64(),
            self.counter_latest_prep.0,
            self.total_amount_accepted_batches
        )
    }
}

impl<Sig: Serialize> Checkpoint<Sig> {
    /// Validates a message of type [Checkpoint].\
    /// To validate it, its USIG signature must be valid.\
    ///
    /// # Arguments
    ///
    /// * `config` - The [Config] of the replica.
    /// * `usig` - The [USIG] signature that should be a valid one for the
    ///            [Checkpoint] message.
    ///
    /// # Return Value
    ///
    /// [Ok] if the validation succeeds, otherwise an [InnerError] is returned.
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
/// [Checkpoint].\
/// Following conditions must be met for the certificate to become stable:
/// 1. The certificate must contain at least `t + 1` [Checkpoint]s
///    (for further explanation regarding `t`, see [crate::Config]).
/// 2. They have to originate from different replicas.
/// 3. They have to share the same [CheckpointHash].
/// 4. They have to share the same counter of the latest accepted [crate::Prepare].
/// If a certificate does not (yet) meet all aforementioned conditions, it is
/// refered to as non-stable until the conditions are met.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub(crate) struct CheckpointCertificate<Sig> {
    /// The message of type [Checkpoint] created by the replica itself.\
    /// In its details, the struct differs from the paper.\
    /// Reason: We have to clear all messages from the replica's log of sent
    ///         messages that have a counter lower than the counter of its
    ///         [Checkpoint].\
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
    /// 1. The certificate must contain at least `t + 1` [Checkpoint]s
    ///    (for further explanation regarding `t`, see [crate::Config]).
    /// 2. They have to originate from different replicas.
    /// 3. They have to share the same [CheckpointHash].
    /// 4. They have to share the same counter of the latest accepted
    ///    prepare.
    /// 5. Their USIG signatures have to be valid.
    ///
    /// # Arguments
    ///
    /// * `config` - The [Config] of the algorithm.
    /// * `usig` - The [USIG] signature that should be a valid one for the
    ///            [Checkpoint] messages.
    ///
    /// # Return Value
    ///
    /// [Ok] if the validation succeeds, otherwise an [InnerError] is returned.
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
pub(crate) mod test {
    use std::{collections::HashMap, num::NonZeroU64};

    use rand::{rngs::ThreadRng, Rng};
    use rstest::rstest;
    use shared_ids::ReplicaId;
    use usig::{
        noop::{Signature, UsigNoOp},
        Count, Usig,
    };

    use crate::{
        error::InnerError,
        tests::{
            create_attested_usigs_for_replicas, create_default_configs_for_replicas,
            create_random_state_hash, get_random_included_replica_id, get_random_replica_id,
            get_shuffled_remaining_replicas, get_two_different_indexes,
        },
    };

    use super::{Checkpoint, CheckpointCertificate, CheckpointContent};

    /// Creates a Checkpoint based on the given parameters.
    ///
    /// # Arguments
    ///
    /// * `origin` - The ID of the replica that the Checkpoint originates from.
    /// * `state_hash` - The state hash to use in the Checkpoint.
    /// * `counter_latest_prep` - The counter of the latest accepted Prepare.
    /// * `usig` - The USIG signature to be used for creating the Checkpoint.
    ///
    /// # The created Checkpoint.
    pub(crate) fn create_checkpoint(
        origin: ReplicaId,
        state_hash: [u8; 64],
        counter_latest_prep: Count,
        total_amount_accepted_batches: u64,
        usig: &mut impl Usig<Signature = Signature>,
    ) -> Checkpoint<Signature> {
        Checkpoint::sign(
            CheckpointContent {
                origin,
                state_hash,
                counter_latest_prep,
                total_amount_accepted_batches,
            },
            usig,
        )
        .unwrap()
    }

    /// Create a Checkpoint certificate with random inner state.
    ///
    /// # Arguments
    ///
    /// * `n` - The number of replicas.
    /// * `t` - The maximum number of faulty replicas that the algorithm
    /// tolerates.
    /// * `origin` - The ID of the replica from which the Checkpoint originates.
    /// * `rng` - The random number generator to be used.
    /// * `usigs` - The USIG signature to be used to sign the Checkpoint.
    ///
    /// # Return Value
    ///
    /// * `The created Checkpoint certificate.`
    pub(crate) fn create_checkpoint_cert_random(
        n: NonZeroU64,
        t: u64,
        origin: ReplicaId,
        rng: &mut ThreadRng,
        usigs: &mut HashMap<ReplicaId, UsigNoOp>,
    ) -> CheckpointCertificate<Signature> {
        let counter_latest_prep = Count(rng.gen());
        let total_amount_accepted_batches: u64 = rng.gen();
        let state_hash = create_random_state_hash();
        create_checkpoint_cert_with_all_params(
            n,
            t,
            origin,
            state_hash,
            counter_latest_prep,
            total_amount_accepted_batches,
            rng,
            usigs,
        )
    }

    /// Create a Checkpoint certificate with all provided parameters.
    ///
    /// # Arguments
    ///
    /// * `n` - The number of replicas.
    /// * `t` - The maximum number of faulty replicas that the algorithm
    /// tolerates.
    /// * `origin` - The ID of the replica from which the Checkpoint originates.
    /// * `state_hash` - The state hash to be used when creating the Checkpoint.
    /// * `counter_latest_prep` - The counter of the last accepted Prepare to
    /// be used when creating the Checkpoint.
    /// * `total_amount_accepted_batches` - The total amount of accepted batches
    /// to be used when creating the Checkpoint.
    /// * `rng` - The random number generator to be used.
    /// * `usigs` - The USIG signature to be used to sign the Checkpoint.
    ///
    /// # Return Value
    ///
    /// * `The created Checkpoint certificate.`
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn create_checkpoint_cert_with_all_params(
        n: NonZeroU64,
        t: u64,
        origin: ReplicaId,
        state_hash: [u8; 64],
        counter_latest_prep: Count,
        total_amount_accepted_batches: u64,
        rng: &mut ThreadRng,
        usigs: &mut HashMap<ReplicaId, UsigNoOp>,
    ) -> CheckpointCertificate<Signature> {
        let usig_origin = usigs.get_mut(&origin).unwrap();

        let my_checkpoint = create_checkpoint(
            origin,
            state_hash,
            counter_latest_prep,
            total_amount_accepted_batches,
            usig_origin,
        );

        let mut other_checkpoints = Vec::new();
        let shuffled_remaining_reps = get_shuffled_remaining_replicas(n, Some(origin), rng);
        for other_rep_id in shuffled_remaining_reps.iter().take(t as usize) {
            let usig = usigs.get_mut(other_rep_id).unwrap();
            let other_checkpoint = create_checkpoint(
                *other_rep_id,
                state_hash,
                counter_latest_prep,
                total_amount_accepted_batches,
                usig,
            );
            other_checkpoints.push(other_checkpoint);
        }
        CheckpointCertificate {
            my_checkpoint,
            other_checkpoints,
        }
    }

    /// Create an invalid Checkpoint certificate.
    /// The Checkpoint certificate contains an unsufficient amount of messages.
    ///
    /// # Arguments
    ///
    /// * `n` - The number of replicas.
    /// * `t` - The maximum number of faulty replicas that the algorithm
    /// tolerates.
    /// * `origin` - The ID of the replica from which the Checkpoint originates.
    /// * `state_hash` - The state hash to be used when creating the Checkpoint.
    /// * `counter_latest_prep` - The counter of the last accepted Prepare to
    /// be used when creating the Checkpoint.
    /// * `total_amount_accepted_batches` - The total amount of accepted batches
    /// to be used when creating the Checkpoint.
    /// * `rng` - The random number generator to be used.
    /// * `usigs` - The USIG signature to be used to sign the Checkpoint.
    ///
    /// # Return Value
    ///
    /// * `The created invalid Checkpoint certificate.`
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn create_invalid_checkpoint_cert_unsuff_msgs(
        n: NonZeroU64,
        t: u64,
        origin: ReplicaId,
        state_hash: [u8; 64],
        counter_latest_prep: Count,
        total_amount_accepted_batches: u64,
        rng: &mut ThreadRng,
        usigs: &mut HashMap<ReplicaId, UsigNoOp>,
    ) -> CheckpointCertificate<Signature> {
        let usig_origin = usigs.get_mut(&origin).unwrap();
        let my_checkpoint = create_checkpoint(
            origin,
            state_hash,
            counter_latest_prep,
            total_amount_accepted_batches,
            usig_origin,
        );

        let mut other_checkpoints = Vec::new();
        let shuffled_remaining_reps = get_shuffled_remaining_replicas(n, Some(origin), rng);
        let amount_other_checkpoints = rng.gen_range(0..t) as usize;
        for other_rep_id in shuffled_remaining_reps
            .iter()
            .take(amount_other_checkpoints)
        {
            let usig = usigs.get_mut(other_rep_id).unwrap();
            let other_checkpoint = create_checkpoint(
                *other_rep_id,
                state_hash,
                counter_latest_prep,
                total_amount_accepted_batches,
                usig,
            );
            other_checkpoints.push(other_checkpoint);
        }
        CheckpointCertificate {
            my_checkpoint,
            other_checkpoints,
        }
    }

    /// Create an invalid Checkpoint certificate.
    /// The Checkpoint certificate consists of messages that do not share the
    /// same state hash.
    ///
    /// # Arguments
    ///
    /// * `n` - The number of replicas.
    /// * `t` - The maximum number of faulty replicas that the algorithm
    /// tolerates.
    /// * `origin` - The ID of the replica from which the Checkpoint originates.
    /// * `state_hash` - The state hash to be used when creating the Checkpoint.
    /// * `counter_latest_prep` - The counter of the last accepted Prepare to
    /// be used when creating the Checkpoint.
    /// * `total_amount_accepted_batches` - The total amount of accepted batches
    /// to be used when creating the Checkpoint.
    /// * `rng` - The random number generator to be used.
    /// * `usigs` - The USIG signature to be used to sign the Checkpoint.
    ///
    /// # Return Value
    ///
    /// * `The created invalid Checkpoint certificate.`
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn create_invalid_checkpoint_cert_not_same_hash(
        n: NonZeroU64,
        t: u64,
        origin: ReplicaId,
        state_hash: [u8; 64],
        counter_latest_prep: Count,
        total_amount_accepted_batches: u64,
        rng: &mut ThreadRng,
        usigs: &mut HashMap<ReplicaId, UsigNoOp>,
    ) -> CheckpointCertificate<Signature> {
        let usig_origin = usigs.get_mut(&origin).unwrap();
        let my_checkpoint = create_checkpoint(
            origin,
            state_hash,
            counter_latest_prep,
            total_amount_accepted_batches,
            usig_origin,
        );

        let mut other_checkpoints = Vec::new();
        let shuffled_remaining_reps = get_shuffled_remaining_replicas(n, Some(origin), rng);

        for other_rep_id in shuffled_remaining_reps.iter().take(t as usize) {
            let state_hash_diff = create_rand_state_hash_diff(state_hash, rng);

            let usig = usigs.get_mut(other_rep_id).unwrap();
            let other_checkpoint = create_checkpoint(
                *other_rep_id,
                state_hash_diff,
                counter_latest_prep,
                total_amount_accepted_batches,
                usig,
            );
            other_checkpoints.push(other_checkpoint);
        }
        CheckpointCertificate {
            my_checkpoint,
            other_checkpoints,
        }
    }

    /// Create an invalid Checkpoint certificate.
    /// The Checkpoint certificate consists of messages that do not all
    /// originate from different replicas.
    ///
    /// # Arguments
    ///
    /// * `n` - The number of replicas.
    /// * `t` - The maximum number of faulty replicas that the algorithm
    /// tolerates.
    /// * `origin` - The ID of the replica from which the Checkpoint originates.
    /// * `state_hash` - The state hash to be used when creating the Checkpoint.
    /// * `counter_latest_prep` - The counter of the last accepted Prepare to
    /// be used when creating the Checkpoint.
    /// * `total_amount_accepted_batches` - The total amount of accepted batches
    /// to be used when creating the Checkpoint.
    /// * `rng` - The random number generator to be used.
    /// * `usigs` - The USIG signature to be used to sign the Checkpoint.
    ///
    /// # Return Value
    ///
    /// * `The created invalid Checkpoint certificate.`
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn create_invalid_checkpoint_cert_not_all_diff_origin(
        n: NonZeroU64,
        t: u64,
        origin: ReplicaId,
        state_hash: [u8; 64],
        counter_latest_prep: Count,
        total_amount_accepted_batches: u64,
        rng: &mut ThreadRng,
        usigs: &mut HashMap<ReplicaId, UsigNoOp>,
    ) -> CheckpointCertificate<Signature> {
        let usig_origin = usigs.get_mut(&origin).unwrap();
        let my_checkpoint = create_checkpoint(
            origin,
            state_hash,
            counter_latest_prep,
            total_amount_accepted_batches,
            usig_origin,
        );

        let mut other_checkpoints = Vec::new();
        let shuffled_remaining_reps = get_shuffled_remaining_replicas(n, Some(origin), rng);
        let (index_to_replace_origin, index_origin_to_set_to) =
            get_two_different_indexes(t as usize, rng);

        for (rep_id_index, rep_id) in shuffled_remaining_reps.iter().enumerate().take(t as usize) {
            let mut origin_checkpoint = *rep_id;
            if rep_id_index == index_to_replace_origin {
                origin_checkpoint = shuffled_remaining_reps[index_origin_to_set_to];
            }
            let usig = usigs.get_mut(rep_id).unwrap();
            let other_checkpoint = create_checkpoint(
                origin_checkpoint,
                state_hash,
                counter_latest_prep,
                total_amount_accepted_batches,
                usig,
            );
            other_checkpoints.push(other_checkpoint);
        }

        CheckpointCertificate {
            my_checkpoint,
            other_checkpoints,
        }
    }

    /// Create an invalid Checkpoint certificate.
    /// The Checkpoint certificate consists of messages that do not share the
    /// same counter of last accepted Prepares.
    ///
    /// # Arguments
    ///
    /// * `n` - The number of replicas.
    /// * `t` - The maximum number of faulty replicas that the algorithm
    /// tolerates.
    /// * `origin` - The ID of the replica from which the Checkpoint originates.
    /// * `state_hash` - The state hash to be used when creating the Checkpoint.
    /// * `counter_latest_prep` - The counter of the last accepted Prepare to
    /// be used when creating the Checkpoint.
    /// * `total_amount_accepted_batches` - The total amount of accepted batches
    /// to be used when creating the Checkpoint.
    /// * `rng` - The random number generator to be used.
    /// * `usigs` - The USIG signature to be used to sign the Checkpoint.
    ///
    /// # Return Value
    ///
    /// * `The created invalid Checkpoint certificate.`
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn create_invalid_checkpoint_cert_not_all_same_latest_prep(
        n: NonZeroU64,
        t: u64,
        origin: ReplicaId,
        state_hash: [u8; 64],
        counter_latest_prep: Count,
        total_amount_accepted_batches: u64,
        rng: &mut ThreadRng,
        usigs: &mut HashMap<ReplicaId, UsigNoOp>,
    ) -> CheckpointCertificate<Signature> {
        let usig_origin = usigs.get_mut(&origin).unwrap();
        let my_checkpoint = create_checkpoint(
            origin,
            state_hash,
            counter_latest_prep,
            total_amount_accepted_batches,
            usig_origin,
        );

        let mut other_checkpoints = Vec::new();
        let shuffled_remaining_reps = get_shuffled_remaining_replicas(n, Some(origin), rng);

        for other_rep_id in shuffled_remaining_reps.iter().take(t as usize) {
            let mut random_counter_latest_prep = Count(rng.gen_range(0..u64::MAX));
            if random_counter_latest_prep == counter_latest_prep {
                random_counter_latest_prep = Count(random_counter_latest_prep.0.wrapping_add(1));
            }
            let usig = usigs.get_mut(other_rep_id).unwrap();
            let other_checkpoint = create_checkpoint(
                *other_rep_id,
                state_hash,
                random_counter_latest_prep,
                total_amount_accepted_batches,
                usig,
            );
            other_checkpoints.push(other_checkpoint);
        }

        CheckpointCertificate {
            my_checkpoint,
            other_checkpoints,
        }
    }

    /// Create invalid Checkpoint certificates.
    /// The Checkpoint certificate are invalid for different reasons.
    ///
    /// # Arguments
    ///
    /// * `n` - The number of replicas.
    /// * `t` - The maximum number of faulty replicas that the algorithm
    /// tolerates.
    /// * `origin` - The ID of the replica from which the Checkpoint originates.
    /// * `state_hash` - The state hash to be used when creating the Checkpoint.
    /// * `counter_latest_prep` - The counter of the last accepted Prepare to
    /// be used when creating the Checkpoint.
    /// * `total_amount_accepted_batches` - The total amount of accepted batches
    /// to be used when creating the Checkpoint.
    /// * `rng` - The random number generator to be used.
    /// * `usigs` - The USIG signature to be used to sign the Checkpoint.
    ///
    /// # Return Value
    ///
    /// * `The created invalid Checkpoint certificate.`
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn create_invalid_checkpoint_certs(
        n: NonZeroU64,
        t: u64,
        origin: ReplicaId,
        state_hash: [u8; 64],
        counter_latest_prep: Count,
        total_amount_accepted_batches: u64,
        rng: &mut ThreadRng,
        usigs: &mut HashMap<ReplicaId, UsigNoOp>,
    ) -> Vec<CheckpointCertificate<Signature>> {
        let mut certs_invalid = Vec::new();

        let cert_unsuff = create_invalid_checkpoint_cert_unsuff_msgs(
            n,
            t,
            origin,
            state_hash,
            counter_latest_prep,
            total_amount_accepted_batches,
            rng,
            usigs,
        );

        let cert_not_all_same_hash = create_invalid_checkpoint_cert_not_same_hash(
            n,
            t,
            origin,
            state_hash,
            counter_latest_prep,
            total_amount_accepted_batches,
            rng,
            usigs,
        );

        let cert_not_all_diff_origin = create_invalid_checkpoint_cert_not_all_diff_origin(
            n,
            t,
            origin,
            state_hash,
            counter_latest_prep,
            total_amount_accepted_batches,
            rng,
            usigs,
        );

        let cert_not_all_same_latest_prep = create_invalid_checkpoint_cert_not_all_same_latest_prep(
            n,
            t,
            origin,
            state_hash,
            counter_latest_prep,
            total_amount_accepted_batches,
            rng,
            usigs,
        );

        certs_invalid.push(cert_unsuff);
        certs_invalid.push(cert_not_all_same_hash);
        certs_invalid.push(cert_not_all_diff_origin);
        certs_invalid.push(cert_not_all_same_latest_prep);

        certs_invalid
    }

    /// Create a random different state hash.
    ///
    /// # Arguments
    ///
    /// * `state_hash` - The state hash from which the generated one should
    /// differ.
    /// * `rng` - The random number generator to be used.
    ///
    /// # Return Value
    ///
    /// * `The created state hash.`
    pub(crate) fn create_rand_state_hash_diff(
        state_hash: [u8; 64],
        rng: &mut ThreadRng,
    ) -> [u8; 64] {
        let mut state_hash_diff = [0u8; 64];
        let random_byte_index = rng.gen_range(0..64) as usize;
        for i in 0..64 {
            if i == random_byte_index {
                state_hash_diff[i] = state_hash[i].wrapping_add(1);
            } else {
                state_hash_diff[i] = state_hash[i];
            }
        }
        state_hash_diff
    }

    /// Tests if the validation of a [CheckpointCertificate], which does not
    /// contain a sufficient amount of [Checkpoint]s, results in an error.
    #[rstest]
    fn validate_cert_not_enough_msgs(#[values(5, 6, 7, 8, 9, 10)] n: u64) {
        let n_parsed = NonZeroU64::new(n).unwrap();

        let mut rng = rand::thread_rng();
        let origin = get_random_replica_id(n_parsed, &mut rng);
        let counter_latest_prep = Count(rng.gen());
        let total_amount_accepted_batches: u64 = rng.gen();
        let state_hash = create_random_state_hash();

        let mut usigs = create_attested_usigs_for_replicas(n_parsed, Vec::new());

        for t in 1..n / 2 {
            let configs = create_default_configs_for_replicas(n_parsed, t);

            let checkpoint_cert = create_invalid_checkpoint_cert_unsuff_msgs(
                n_parsed,
                t,
                origin,
                state_hash,
                counter_latest_prep,
                total_amount_accepted_batches,
                &mut rng,
                &mut usigs,
            );

            for i in 0..n {
                let rep_id = ReplicaId::from_u64(i);
                let config_other = configs.get(&rep_id).unwrap();
                let usig_other = usigs.get_mut(&rep_id).unwrap();

                assert!(matches!(
                    checkpoint_cert.validate(config_other, usig_other).err().unwrap(),
                    InnerError::CheckpointCertNotSufficientMsgs { receiver, origin: cert_origin }
                    if receiver == rep_id && cert_origin == origin,
                ));
            }
        }
    }

    /// Tests if the validation of a [CheckpointCertificate], in which all
    /// contained [Checkpoint]s do not have the same state hash, results in an
    /// error.
    #[rstest]
    fn validate_cert_not_all_same_state_hash(#[values(5, 6, 7, 8, 9, 10)] n: u64) {
        let n_parsed = NonZeroU64::new(n).unwrap();

        let mut rng = rand::thread_rng();
        let origin = get_random_replica_id(n_parsed, &mut rng);
        let counter_latest_prep = Count(rng.gen());
        let total_amount_accepted_batches: u64 = rng.gen();
        let state_hash = create_random_state_hash();

        let mut usigs = create_attested_usigs_for_replicas(n_parsed, Vec::new());

        for t in 1..n / 2 {
            let configs = create_default_configs_for_replicas(n_parsed, t);

            let checkpoint_cert = create_invalid_checkpoint_cert_not_same_hash(
                n_parsed,
                t,
                origin,
                state_hash,
                counter_latest_prep,
                total_amount_accepted_batches,
                &mut rng,
                &mut usigs,
            );

            for i in 0..n {
                let rep_id = ReplicaId::from_u64(i);
                let config_other = configs.get(&rep_id).unwrap();
                let usig_other = usigs.get_mut(&rep_id).unwrap();

                assert!(matches!(
                    checkpoint_cert.validate(config_other, usig_other).err().unwrap(),
                    InnerError::CheckpointCertNotAllSameStateHash { receiver, origin: cert_origin }
                    if receiver == rep_id && cert_origin == origin,
                ));
            }
        }
    }

    /// Tests if the validation of a [CheckpointCertificate], in which not all
    /// [Checkpoint]s originate from different replicas, results in an error.
    #[rstest]
    fn validate_cert_not_all_different_origins(#[values(5, 6, 7, 8, 9, 10)] n: u64) {
        let n_parsed = NonZeroU64::new(n).unwrap();

        let mut rng = rand::thread_rng();
        let origin = get_random_replica_id(n_parsed, &mut rng);
        let counter_latest_prep = Count(rng.gen());
        let total_amount_accepted_batches: u64 = rng.gen();
        let state_hash = create_random_state_hash();

        let mut usigs = create_attested_usigs_for_replicas(n_parsed, Vec::new());

        let t = n / 2;

        let configs = create_default_configs_for_replicas(n_parsed, t);

        let checkpoint_cert = create_invalid_checkpoint_cert_not_all_diff_origin(
            n_parsed,
            t,
            origin,
            state_hash,
            counter_latest_prep,
            total_amount_accepted_batches,
            &mut rng,
            &mut usigs,
        );

        for i in 0..n {
            let rep_id = ReplicaId::from_u64(i);
            let config_other = configs.get(&rep_id).unwrap();
            let usig_other = usigs.get_mut(&rep_id).unwrap();

            assert!(matches!(
                checkpoint_cert.validate(config_other, usig_other).err().unwrap(),
                InnerError::CheckpointCertNotAllDifferentOrigin { receiver, origin: cert_origin }
                if receiver == rep_id && cert_origin == origin,
            ));
        }
    }

    /// Tests if the validation of a [CheckpointCertificate], in which not all
    /// [Checkpoint]s originate from known replicas (previously added as remote
    /// parties), results in an error.
    #[rstest]
    fn validate_cert_checkpoint_invalid_usig(#[values(5, 6, 7, 8, 9, 10)] n: u64) {
        let n_parsed = NonZeroU64::new(n).unwrap();

        let mut rng = rand::thread_rng();
        let origin = get_random_replica_id(n_parsed, &mut rng);
        let counter_latest_prep = Count(rng.gen());
        let total_amount_accepted_batches: u64 = rng.gen();
        let state_hash = create_random_state_hash();

        let mut usigs = create_attested_usigs_for_replicas(n_parsed, vec![origin]);

        let t = n / 2;

        let configs = create_default_configs_for_replicas(n_parsed, t);

        let checkpoint_cert = create_checkpoint_cert_with_all_params(
            n_parsed,
            t,
            origin,
            state_hash,
            counter_latest_prep,
            total_amount_accepted_batches,
            &mut rng,
            &mut usigs,
        );

        for i in 0..n {
            let rep_id = ReplicaId::from_u64(i);
            let config_other = configs.get(&rep_id).unwrap();
            let usig_other = usigs.get_mut(&rep_id).unwrap();

            assert!(matches!(
                checkpoint_cert.validate(config_other, usig_other).err().unwrap(),
                InnerError::Usig { usig_error: _, replica, msg_type: _, origin: cert_origin }
                if replica == rep_id && cert_origin == origin,
            ));
        }
    }

    /// Tests if the validation of a [CheckpointCertificate], in which not all
    /// [Checkpoint]s contain the same counter of the last Prepare, results in
    /// an error.
    #[rstest]
    fn validate_cert_checkpoint_invalid_latest_prep(#[values(5, 6, 7, 8, 9, 10)] n: u64) {
        let n_parsed = NonZeroU64::new(n).unwrap();

        let mut rng = rand::thread_rng();
        let origin = get_random_replica_id(n_parsed, &mut rng);
        let counter_latest_prep = Count(rng.gen());
        let total_amount_accepted_batches: u64 = rng.gen();
        let state_hash = create_random_state_hash();

        let mut usigs = create_attested_usigs_for_replicas(n_parsed, Vec::new());

        for t in 1..n / 2 {
            let configs = create_default_configs_for_replicas(n_parsed, t);

            let checkpoint_cert = create_invalid_checkpoint_cert_not_all_same_latest_prep(
                n_parsed,
                t,
                origin,
                state_hash,
                counter_latest_prep,
                total_amount_accepted_batches,
                &mut rng,
                &mut usigs,
            );

            for i in 0..n {
                let rep_id = ReplicaId::from_u64(i);
                let config_other = configs.get(&rep_id).unwrap();
                let usig_other = usigs.get_mut(&rep_id).unwrap();

                assert!(matches!(
                    checkpoint_cert.validate(config_other, usig_other).err().unwrap(),
                    InnerError::CheckpointCertNotAllSameLatestPrep { receiver, origin: cert_origin }
                    if receiver == rep_id && cert_origin == origin,
                ));
            }
        }
    }

    /// Tests if the validation of a valid [CheckpointCertificate] succeeds.
    #[rstest]
    fn validate_cert_valid(#[values(5, 6, 7, 8, 9, 10)] n: u64) {
        let n_parsed = NonZeroU64::new(n).unwrap();

        let mut rng = rand::thread_rng();

        let mut usigs = create_attested_usigs_for_replicas(n_parsed, Vec::new());

        for t in 1..n / 2 {
            let configs = create_default_configs_for_replicas(n_parsed, t);

            let origin = get_random_replica_id(n_parsed, &mut rng);

            let checkpoint_cert =
                create_checkpoint_cert_random(n_parsed, t, origin, &mut rng, &mut usigs);

            for i in 0..n {
                let rep_id = ReplicaId::from_u64(i);
                let config_other = configs.get(&rep_id).unwrap();
                let usig_other = usigs.get_mut(&rep_id).unwrap();

                assert!(checkpoint_cert.validate(config_other, usig_other).is_ok());
            }
        }
    }

    /// Tests if the validation of a valid [CheckpointCertificate] succeeds.
    #[rstest]
    fn validate_valid_checkpoint_msg(#[values(5, 6, 7, 8, 9, 10)] n: u64) {
        let n_parsed = NonZeroU64::new(n).unwrap();

        let mut rng = rand::thread_rng();
        let origin = get_random_replica_id(n_parsed, &mut rng);
        let counter_latest_prep = Count(rng.gen());
        let total_amount_accepted_batches: u64 = rng.gen();
        let state_hash = create_random_state_hash();

        let mut usigs = create_attested_usigs_for_replicas(n_parsed, Vec::new());
        let usig_origin = usigs.get_mut(&origin).unwrap();

        let checkpoint = create_checkpoint(
            origin,
            state_hash,
            counter_latest_prep,
            total_amount_accepted_batches,
            usig_origin,
        );

        for t in 1..n / 2 {
            let configs = create_default_configs_for_replicas(n_parsed, t);

            for i in 0..n {
                let rep_id = ReplicaId::from_u64(i);
                let config_other = configs.get(&rep_id).unwrap();
                let usig_other = usigs.get_mut(&rep_id).unwrap();

                assert!(checkpoint.validate(config_other, usig_other).is_ok());
            }
        }
    }

    /// Tests if the validation of a valid [CheckpointCertificate] succeeds.
    #[rstest]
    fn validate_invalid_checkpoint_msg(#[values(5, 6, 7, 8, 9, 10)] n: u64) {
        let n_parsed = NonZeroU64::new(n).unwrap();

        let mut rng = rand::thread_rng();
        let origin = get_random_replica_id(n_parsed, &mut rng);
        let counter_latest_prep = Count(rng.gen());
        let total_amount_accepted_batches: u64 = rng.gen();
        let state_hash = create_random_state_hash();

        let random_rep_id_to_exclude = get_random_included_replica_id(n_parsed, origin, &mut rng);

        let mut usigs =
            create_attested_usigs_for_replicas(n_parsed, vec![random_rep_id_to_exclude]);
        let usig_origin = usigs.get_mut(&origin).unwrap();

        let checkpoint = create_checkpoint(
            origin,
            state_hash,
            counter_latest_prep,
            total_amount_accepted_batches,
            usig_origin,
        );

        for t in 1..n / 2 {
            let configs = create_default_configs_for_replicas(n_parsed, t);

            for i in 0..n {
                let rep_id = ReplicaId::from_u64(i);
                let config_other = configs.get(&rep_id).unwrap();
                let usig_other = usigs.get_mut(&rep_id).unwrap();

                if rep_id == random_rep_id_to_exclude {
                    assert!(matches!(
                        checkpoint.validate(config_other, usig_other).err().unwrap(),
                        InnerError::Usig { usig_error: _, replica, msg_type: _, origin: cert_origin }
                        if replica == rep_id && cert_origin == origin,
                    ));
                    continue;
                }
                assert!(checkpoint.validate(config_other, usig_other).is_ok());
            }
        }
    }
}
