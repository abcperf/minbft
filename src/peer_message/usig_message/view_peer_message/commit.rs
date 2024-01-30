//! Defines a message of type [Commit].
//! A [Commit] consists of two main parts.
//! The first part is its content, the [CommitContent].
//! It contains the origin of the [Commit], i.e., the ID of the replica
//! ([ReplicaId]) which created the [Commit].
//! Moreover, it contains the [Prepare] to which the [Commit] belongs to.
//! The second part is its signature, as [Commit]s must be signed by a USIG.
//! For further explanation to why these messages (alongside other ones) must be
//! signed by a USIG,
//! see the paper "Efficient Byzantine Fault Tolerance" by Veronese et al.
//! A [Commit] is broadcast by a backup replica, i.e., all replicas besides the
//! current primary,
//! in response to a received [Prepare] (only sent by the current primary).

use core::fmt;

use anyhow::Result;
use blake2::digest::Update;
use serde::{Deserialize, Serialize};
use shared_ids::ReplicaId;
use tracing::{debug, error, trace};
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
                "Failed validating Commit (self): Signature of Commit is invalid. For further information see output.");
            Err(InnerError::parse_usig_error(usig_error, config.id, "Commit", self.origin))
        }, |v| {
            trace!("Successfully verified signature of Commit (origin: {:?}, Prepare: [origin: {:?}, view: {:?}]).", self.origin, self.prepare.origin, self.prepare.view);
            trace!("Successfully validated Commit (origin: {:?}, Prepare: [origin: {:?}, view: {:?}]).", self.origin, self.prepare.origin, self.prepare.view);
            Ok(v)
        })
    }
}

#[cfg(test)]
mod test {
    use std::num::NonZeroU64;

    use shared_ids::{AnyId, ReplicaId};
    use usig::{noop::UsigNoOp, Usig};

    use crate::{
        error::InnerError,
        peer_message::usig_message::view_peer_message::test::{
            add_attestations, create_commit_default_usig, create_commit_with_usig,
            create_config_default, create_prepare_default_usig, create_prepare_with_usig,
        },
        View,
    };

    /// Tests if a reference to the origin of a [Commit] is returned
    /// when calling [`Commit::as_ref()`].
    #[test]
    fn obtain_origin_ref_through_as_ref() {
        let id_primary = ReplicaId::from_u64(0);
        let view = View(0);
        let prepare = create_prepare_default_usig(id_primary, view);

        let id_backup = ReplicaId::from_u64(1);
        let commit = create_commit_default_usig(id_backup, prepare);
        assert_eq!(commit.as_ref(), &id_backup);
    }

    /// Tests if the validation of a valid [Commit] succeeds.
    /// A valid [Commit] has to originate from a backup replica, and must
    /// contain a valid [Prepare].
    /// Furthermore, its [Usig] signature has to be valid, i.e., the backup
    /// replica that signed the [Commit] has to have been previously added as
    /// a known remote party.
    #[test]
    fn validate_valid_commit() {
        // Create Prepare.
        let id_primary = ReplicaId::from_u64(0);
        let view = View(0);
        let mut usig_primary = UsigNoOp::default();
        let prepare = create_prepare_with_usig(id_primary, view, &mut usig_primary);

        // Create Commit.
        let id_backup = ReplicaId::from_u64(1);
        let mut usig_backup = UsigNoOp::default();
        let commit = create_commit_with_usig(id_backup, prepare, &mut usig_backup);

        // Add attestations.
        let usigs = vec![&mut usig_primary, &mut usig_backup];
        add_attestations(usigs);

        // Create config of backup.
        let config = create_config_default(NonZeroU64::new(3).unwrap(), 1, id_backup);

        // Validate Commit on both replicas.
        assert!(commit.validate(&config, &mut usig_primary).is_ok());
        assert!(commit.validate(&config, &mut usig_backup).is_ok());
    }

    /// Tests if the validation of an invalid [Commit],
    /// in which the origin of the [Commit] is the primary, results in the
    /// expected error.
    #[test]
    fn validate_invalid_commit_primary() {
        // Create Prepare.
        let id_primary = ReplicaId::from_u64(0);
        let view = View(0);
        let mut usig_primary = UsigNoOp::default();
        let prepare = create_prepare_with_usig(id_primary, view, &mut usig_primary);

        // Create Commit.
        let id_backup = ReplicaId::from_u64(1);
        let mut usig_backup = UsigNoOp::default();
        let commit = create_commit_with_usig(id_primary, prepare, &mut usig_primary);

        // Add attestations.
        let usigs = vec![&mut usig_primary, &mut usig_backup];
        add_attestations(usigs);

        // Create config of primary.
        let config_primary = create_config_default(NonZeroU64::new(3).unwrap(), 1, id_primary);

        // Create config of backup.
        let config_backup = create_config_default(NonZeroU64::new(3).unwrap(), 1, id_backup);

        // Check (on both replicas) if the expected error is thrown when
        // validating a commit that originates from the primary.
        let res_validation_primary = commit.validate(&config_primary, &mut usig_primary);
        assert!(matches!(
            res_validation_primary,
            Err(InnerError::CommitFromPrimary { receiver, primary }) if receiver == id_primary && primary == id_primary));

        let res_validation_backup = commit.validate(&config_backup, &mut usig_backup);
        assert!(matches!(
            res_validation_backup,
                Err(InnerError::CommitFromPrimary { receiver, primary }) if receiver == id_backup && primary == id_primary));
    }

    /// Tests if the validation of an invalid [Commit],
    /// in which the origin is unknown (not previously added as remote party),
    /// results in the expected error.
    #[test]
    fn validate_invalid_commit_unknown_remote_party() {
        let id_primary = ReplicaId::from_u64(0);
        let view = View(0);
        let mut usig_primary = UsigNoOp::default();
        let prepare = create_prepare_with_usig(id_primary, view, &mut usig_primary);

        let id_backup = ReplicaId::from_u64(1);
        let mut usig_backup = UsigNoOp::default();
        let commit = create_commit_with_usig(id_backup, prepare, &mut usig_backup);

        usig_primary.add_remote_party(id_primary, ());

        let config_primary = create_config_default(NonZeroU64::new(3).unwrap(), 1, id_primary);

        // Check if the expected error is thrown when validating a commit that
        // originates from an unknown source.
        let res_validation_primary = commit.validate(&config_primary, &mut usig_primary);
        assert!(matches!(
            res_validation_primary,
            Err(InnerError::Usig {
                usig_error: _,
                replica,
                msg_type,
                origin
            }) if replica == id_primary && msg_type == "Commit" && origin == id_backup
        ));
    }
}
