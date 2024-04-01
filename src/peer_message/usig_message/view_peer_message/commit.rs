//! Defines a message of type [Commit].
//! A [Commit] consists of two main parts.
//! The first part is its content, the [CommitContent].
//! It contains the origin of the [Commit], i.e., the ID of the replica
//! ([ReplicaId]) which created the [Commit].
//! Moreover, it contains the [Prepare] to which the [Commit] belongs to.
//! The second part is its signature, as [Commit]s must be signed by a USIG.
//! For further explanation to why these messages (alongside other ones) must be
//! signed by a USIG,
//! refer to the paper "Efficient Byzantine Fault Tolerance" by Veronese et al.
//! A [Commit] is broadcast by a backup replica, i.e., all replicas besides the
//! current primary,
//! in response to a received [Prepare] (only sent by the current primary).

use core::fmt;

use anyhow::Result;
use blake2::digest::Update;
use serde::{Deserialize, Serialize};
use shared_ids::ReplicaId;
use tracing::{error, trace};
use usig::Usig;

use crate::{
    error::InnerError,
    peer_message::usig_message::signed::{UsigSignable, UsigSigned},
    Config, RequestPayload,
};

use super::prepare::Prepare;

/// The content of a message of type [Commit].
/// Consists of the [Prepare] to which the [Commit] belongs to.
/// Furthermore, the ID of the backup replica that created the [Commit] is
/// necessary.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub(crate) struct CommitContent<P, Sig> {
    /// The replica which the [Commit] originates from.
    pub(crate) origin: ReplicaId,
    /// The [Prepare] to which the [Commit] belongs to.
    pub(crate) prepare: Prepare<P, Sig>,
}

impl<P, Sig> AsRef<ReplicaId> for CommitContent<P, Sig> {
    /// Referencing [CommitContent] returns a reference to its origin.
    fn as_ref(&self) -> &ReplicaId {
        &self.origin
    }
}

impl<P: Serialize, Sig: Serialize> UsigSignable for CommitContent<P, Sig> {
    /// Hashes the content of a [Commit].
    /// Required for signing and verifying a message of type [Commit].
    fn hash_content<H: Update>(&self, hasher: &mut H) {
        let encoded = bincode::serialize(self).unwrap();
        hasher.update(&encoded);
    }
}

/// The message of type [Commit].
/// A [Commit] consists of, inter alia, its content and must be signed by a
/// USIG.
/// Such a message is broadcast by a backup replica in response to a received
/// [Prepare] (only sent by the current primary).
/// They can and should be validated.
pub(crate) type Commit<P, Sig> = UsigSigned<CommitContent<P, Sig>, Sig>;

impl<P: RequestPayload, Sig: Serialize> fmt::Display for Commit<P, Sig> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "(origin: {0}, prepare: {1})", self.origin, self.prepare)
    }
}

impl<P: RequestPayload, Sig: Serialize> Commit<P, Sig> {
    /// Validates a message of type [Commit].
    /// Following conditions must be met for the [Commit] to be valid:
    ///     (1) The [Commit] must originate from a backup replica, i.a. a
    ///         replica other than the current primary.
    ///     (2) The [Prepare] must be valid (for further explanation regarding
    ///         the validation of [Prepare]s see the equally named function).
    ///     (3) Additionally, the USIG signature of the [Commit] must be
    ///         valid.
    ///
    /// # Arguments
    ///
    /// * `config` - The [Config] of the algorithm.
    /// * `usig` - The USIG signature that should be a valid one for this
    ///            [Commit] message.
    pub(crate) fn validate(
        &self,
        config: &Config,
        usig: &mut impl Usig<Signature = Sig>,
    ) -> Result<(), InnerError> {
        trace!("Validating Commit ({self}) ...");

        // Check condition (1).
        if self.origin == self.prepare.origin {
            error!(
                "Failed validating Commit ({self}): Commit originates from 
            Primary. For further information see output."
            );
            return Err(InnerError::CommitFromPrimary {
                receiver: config.id,
                primary: self.origin,
            });
        }

        // Check condition (2).
        self.prepare.validate(config, usig)?;

        // Check condition (3).
        trace!("Verifying signature of Commit ({self}) ...");
        self.verify(usig).map_or_else(|usig_error| {
            error!(
                "Failed validating Commit ({self}): Signature of Commit is invalid. For further information see output.");
            Err(InnerError::parse_usig_error(usig_error, config.id, "Commit", self.origin))
        }, |v| {
            trace!("Successfully verified signature of Commit ({self}).");
            trace!("Successfully validated Commit ({self}).");
            Ok(v)
        })
    }
}

#[cfg(test)]
pub(crate) mod test {
    use std::num::NonZeroU64;

    use rand::thread_rng;
    use rstest::rstest;
    use usig::{
        noop::{Signature, UsigNoOp},
        AnyId, ReplicaId, Usig,
    };

    use crate::{
        client_request::test::create_batch,
        peer_message::usig_message::view_peer_message::{
            commit::CommitContent,
            prepare::{
                test::{create_invalid_prepares, create_prepare},
                Prepare,
            },
        },
        tests::{
            add_attestations, create_config_default, get_random_backup_replica_id,
            get_random_replica_id, DummyPayload,
        },
        View,
    };

    use super::Commit;

    pub(crate) fn create_commit(
        origin: ReplicaId,
        prepare: Prepare<DummyPayload, Signature>,
        usig: &mut impl Usig<Signature = Signature>,
    ) -> Commit<DummyPayload, Signature> {
        Commit::sign(CommitContent { origin, prepare }, usig).unwrap()
    }

    /// Tests if the validation of a valid [Commit] succeeds.
    #[rstest]
    fn validate_valid_commit(#[values(3, 4, 5, 6, 7, 8, 9, 10)] n: u64) {
        let n_parsed = NonZeroU64::new(n).unwrap();
        let mut rng = thread_rng();

        for t in 0..n / 2 {
            let primary_id = get_random_replica_id(n_parsed);
            let view = View(primary_id.as_u64());
            let mut usig_primary = UsigNoOp::default();
            let config_primary = create_config_default(n_parsed, t, primary_id);
            let request_batch = create_batch();
            let prepare = create_prepare(view, request_batch, &config_primary, &mut usig_primary);

            let backup_id = get_random_backup_replica_id(n_parsed, primary_id, &mut rng);
            let mut usig_backup = UsigNoOp::default();
            let config_backup = create_config_default(n_parsed, t, backup_id);
            let commit = create_commit(backup_id, prepare, &mut usig_backup);

            add_attestations(vec![
                (primary_id, &mut usig_primary),
                (backup_id, &mut usig_backup),
            ]);

            assert!(commit.validate(&config_primary, &mut usig_primary).is_ok());
            assert!(commit.validate(&config_backup, &mut usig_backup).is_ok());
        }
    }

    #[rstest]
    fn validate_invalid_commit_origin(#[values(3, 4, 5, 6, 7, 8, 9, 10)] n: u64) {
        let n_parsed = NonZeroU64::new(n).unwrap();
        let mut rng = thread_rng();

        for t in 0..n / 2 {
            let primary_id = get_random_replica_id(n_parsed);
            let view = View(primary_id.as_u64());
            let mut usig_primary = UsigNoOp::default();
            let config_primary = create_config_default(n_parsed, t, primary_id);
            let request_batch = create_batch();
            let prepare = create_prepare(view, request_batch, &config_primary, &mut usig_primary);

            let backup_id = get_random_backup_replica_id(n_parsed, primary_id, &mut rng);
            let mut usig_backup = UsigNoOp::default();
            let config_backup = create_config_default(n_parsed, t, backup_id);
            let commit = create_commit(primary_id, prepare, &mut usig_primary);

            add_attestations(vec![
                (primary_id, &mut usig_primary),
                (backup_id, &mut usig_backup),
            ]);

            assert!(commit.validate(&config_primary, &mut usig_primary).is_err());
            assert!(commit.validate(&config_backup, &mut usig_backup).is_err());
        }
    }

    #[rstest]
    fn validate_invalid_commit_unknown_party(#[values(3, 4, 5, 6, 7, 8, 9, 10)] n: u64) {
        let n_parsed = NonZeroU64::new(n).unwrap();
        let mut rng = thread_rng();

        for t in 0..n / 2 {
            let primary_id = get_random_replica_id(n_parsed);
            let view = View(primary_id.as_u64());
            let mut usig_primary = UsigNoOp::default();
            let config_primary = create_config_default(n_parsed, t, primary_id);
            let request_batch = create_batch();
            let prepare = create_prepare(view, request_batch, &config_primary, &mut usig_primary);

            let backup_id = get_random_backup_replica_id(n_parsed, primary_id, &mut rng);
            let mut usig_backup = UsigNoOp::default();
            let config_backup = create_config_default(n_parsed, t, backup_id);
            let commit = create_commit(backup_id, prepare, &mut usig_backup);

            usig_primary.add_remote_party(primary_id, ());
            usig_backup.add_remote_party(backup_id, ());
            usig_backup.add_remote_party(primary_id, ());

            assert!(commit.validate(&config_primary, &mut usig_primary).is_err());
            assert!(commit.validate(&config_backup, &mut usig_backup).is_ok());
        }
    }

    /// Tests if the validation of a valid [Commit] succeeds.
    #[rstest]
    fn validate_invalid_commit_invalid_prepare(#[values(3, 4, 5, 6, 7, 8, 9, 10)] n: u64) {
        let n_parsed = NonZeroU64::new(n).unwrap();
        let mut rng = thread_rng();

        for t in 0..n / 2 {
            let primary_id = get_random_replica_id(n_parsed);
            let view = View(primary_id.as_u64());
            let mut usig_primary = UsigNoOp::default();
            let config_primary = create_config_default(n_parsed, t, primary_id);
            let request_batch = create_batch();
            let prepares_invalid = create_invalid_prepares(
                view,
                request_batch.clone(),
                &config_primary,
                &mut usig_primary,
                &mut rng,
            );

            let backup_id = get_random_backup_replica_id(n_parsed, primary_id, &mut rng);
            let mut usig_backup = UsigNoOp::default();
            let config_backup = create_config_default(n_parsed, t, backup_id);

            usig_primary.add_remote_party(primary_id, ());
            usig_primary.add_remote_party(backup_id, ());
            usig_backup.add_remote_party(backup_id, ());

            let prepare_unknown_usig =
                create_prepare(view, request_batch, &config_primary, &mut usig_primary);
            let commit = create_commit(backup_id, prepare_unknown_usig, &mut usig_backup);
            assert!(commit.validate(&config_primary, &mut usig_primary).is_ok());
            assert!(commit.validate(&config_backup, &mut usig_backup).is_err());

            usig_backup.add_remote_party(primary_id, ());

            for prep_invalid in prepares_invalid {
                let commit = create_commit(backup_id, prep_invalid, &mut usig_backup);
                assert!(commit.validate(&config_primary, &mut usig_primary).is_err());
                assert!(commit.validate(&config_backup, &mut usig_backup).is_err());
            }
        }
    }
}
