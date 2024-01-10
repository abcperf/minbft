//! Defines a message of type [Commit].
//! A [Commit] consists of two main parts.
//! The first part is its content, the [CommitContent].
//! It contains the ID of the replica ([ReplicaId]) which created the [Commit].
//! Moreover, it contains the [Prepare] to which the [Commit] belongs to.
//! The second part is its signature, as [Commit]s must be signed by a USIG.
//! For further explanation to why these messages (alongside other ones) must be signed by a USIG,
//! see the paper "Efficient Byzantine Fault Tolerance" by Veronese et al.
//! A [Commit] is broadcast by a replica (other than the current primary)
//! in response to a received Prepare (only sent by the current primary).

use anyhow::Result;
use blake2::digest::Update;
use serde::{Deserialize, Serialize};
use shared_ids::ReplicaId;
use tracing::{debug, error};
use usig::Usig;

use crate::{
    error::InnerError,
    peer_message::usig_message::signed::{UsigSignable, UsigSigned},
    Config, RequestPayload,
};

use super::prepare::Prepare;

/// The content of a message of type [Commit].
/// The prepare to which the [Commit] belongs to is needed.
/// Furthermore, the ID of the Replica that created the [Commit] is necessary.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub(crate) struct CommitContent<P, Sig> {
    /// The replica which the [Commit] originates from.
    pub(crate) origin: ReplicaId,
    /// The [Prepare] to which the [Commit] belongs to.
    pub(crate) prepare: Prepare<P, Sig>,
}

impl<P, Sig> AsRef<ReplicaId> for CommitContent<P, Sig> {
    /// Referencing [CommitContent] returns a reference to the origin set in the [CommitContent].
    fn as_ref(&self) -> &ReplicaId {
        &self.origin
    }
}

impl<P: Serialize, Sig: Serialize> UsigSignable for CommitContent<P, Sig> {
    /// Hashes the content of a message of type Commit.
    /// Required for signing and verifying a message of type [Commit].
    fn hash_content<H: Update>(&self, hasher: &mut H) {
        let encoded = bincode::serialize(self).unwrap();
        hasher.update(&encoded);
    }
}

/// The message of type [Commit].
/// [Commit]s consist of their content and must be signed by a USIG.
/// Such a message is broadcast by a replica (other than the current primary)
/// in response to a received Prepare (only sent by the current primary).
/// They can and should be validated.
/// For further explanation regarding the content of the module including [Commit], see the documentation of the module itself.
/// For further explanation regarding the use of [Commit]s, see the documentation of [crate::MinBft].
pub(crate) type Commit<P, Sig> = UsigSigned<CommitContent<P, Sig>, Sig>;

impl<P: RequestPayload, Sig: Serialize> Commit<P, Sig> {
    /// Validates a message of type [Commit].
    /// Following conditions must be met for the [Commit] to be valid:
    ///     (1) The [Commit] must originate from a replica other than the current primary.
    ///     (2) The [Prepare] must be valid (for further explanation regarding
    ///         the validation of [Prepare]s see the equally named function).
    ///     (3) Additionally, the signature of the [Commit] must be verified.
    pub(crate) fn validate(
        &self,
        config: &Config,
        usig: &mut impl Usig<Signature = Sig>,
    ) -> Result<(), InnerError> {
        debug!(
            "Validating Commit (origin: {:?}, Prepare: [origin: {:?}, view: {:?}]) ...",
            self.origin, self.prepare.origin, self.prepare.view
        );
        if self.origin == self.prepare.origin {
            error!("Failed validating Commit (origin: {:?}, Prepare: [origin: {:?}, view: {:?}]): Commit originates from Primary. For further information see output", self.origin, self.prepare.origin, self.prepare.view);
            return Err(InnerError::CommitFromPrimary {
                receiver: config.id,
                primary: self.origin,
            });
        }
        self.prepare.validate(config, usig)?;

        debug!(
            "Verifying signature of Commit (origin: {:?}, Prepare: [origin: {:?}, view: {:?}]) ...",
            self.origin, self.prepare.origin, self.prepare.view
        );
        self.verify(usig).map_or_else(|usig_error| {
            error!(
                "Failed validating Commit (origin: {:?}, Prepare: [origin: {:?}, view: {:?}]): Signature of Commit is invalid. For further information see output.",
                self.origin, self.prepare.origin, self.prepare.view
            );
            Err(InnerError::parse_usig_error(usig_error, config.id, "Commit", self.origin))
        }, |v| {
            debug!("Successfully verified signature of Commit (origin: {:?}, Prepare: [origin: {:?}, view: {:?}]).", self.origin, self.prepare.origin, self.prepare.view);
            debug!("Successfully validated Commit (origin: {:?}, Prepare: [origin: {:?}, view: {:?}]).", self.origin, self.prepare.origin, self.prepare.view);
            Ok(v)
        })
    }
}

#[cfg(test)]
mod test {
    use std::{num::NonZeroU64, time::Duration};

    use shared_ids::{AnyId, ReplicaId};
    use usig::{
        noop::{Signature, UsigNoOp},
        Usig,
    };

    use crate::{
        client_request::{self, RequestBatch},
        peer_message::usig_message::view_peer_message::{
            commit::CommitContent,
            prepare::{Prepare, PrepareContent},
        },
        tests::DummyPayload,
        Config, View,
    };

    use super::Commit;

    fn create_prepare_default_usig(
        origin: ReplicaId,
        view: View,
    ) -> Prepare<DummyPayload, Signature> {
        Prepare::sign(
            PrepareContent {
                origin,
                view,
                request_batch: RequestBatch::new(Box::<
                    [client_request::ClientRequest<DummyPayload>; 0],
                >::new([])),
            },
            &mut UsigNoOp::default(),
        )
        .unwrap()
    }

    fn create_prepare_with_usig(
        origin: ReplicaId,
        view: View,
        usig: &mut impl Usig<Signature = Signature>,
    ) -> Prepare<DummyPayload, Signature> {
        Prepare::sign(
            PrepareContent {
                origin,
                view,
                request_batch: RequestBatch::new(Box::<
                    [client_request::ClientRequest<DummyPayload>; 0],
                >::new([])),
            },
            usig,
        )
        .unwrap()
    }

    fn create_commit_default_usig(
        origin: ReplicaId,
        prepare: Prepare<DummyPayload, Signature>,
    ) -> Commit<DummyPayload, Signature> {
        Commit::sign(CommitContent { origin, prepare }, &mut UsigNoOp::default()).unwrap()
    }

    fn create_commit_with_usig(
        origin: ReplicaId,
        prepare: Prepare<DummyPayload, Signature>,
        usig: &mut impl Usig<Signature = Signature>,
    ) -> Commit<DummyPayload, Signature> {
        Commit::sign(CommitContent { origin, prepare }, usig).unwrap()
    }

    fn add_attestations(mut usigs: Vec<&mut UsigNoOp>) {
        for i in 0..usigs.len() {
            for j in 0..usigs.len() {
                usigs[i].add_remote_party(ReplicaId::from_u64(j.try_into().unwrap()), ());
            }
        }
    }

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
    #[test]
    fn validate_valid_commit() {
        let id_primary = ReplicaId::from_u64(0);
        let view = View(0);
        let mut usig_primary = UsigNoOp::default();
        let prepare = create_prepare_with_usig(id_primary, view, &mut usig_primary);

        let id_backup = ReplicaId::from_u64(1);
        let mut usig_backup = UsigNoOp::default();
        let commit = create_commit_with_usig(id_backup, prepare, &mut usig_backup);

        let usigs = vec![&mut usig_primary, &mut usig_backup];
        add_attestations(usigs);

        let config = Config {
            n: NonZeroU64::new(3).unwrap(),
            t: 1,
            id: id_backup,
            batch_timeout: Duration::from_secs(2),
            max_batch_size: None,
            initial_timeout_duration: Duration::from_secs(2),
            checkpoint_period: NonZeroU64::new(2).unwrap(),
        };

        assert!(commit.validate(&config, &mut usig_primary).is_ok());
        assert!(commit.validate(&config, &mut usig_backup).is_ok());
    }

    /// Tests if the validation of an invalid [Commit],
    /// in which the origin of the [Commit] is the primary, results in an error.
    #[test]
    fn validate_invalid_commit_primary() {
        let mut usig_0 = UsigNoOp::default();

        usig_0.add_remote_party(ReplicaId::from_u64(0), ());

        let prepare = Prepare::sign(
            PrepareContent {
                origin: ReplicaId::from_u64(0),
                view: View(0),
                request_batch: RequestBatch::new(Box::<
                    [client_request::ClientRequest<DummyPayload>; 0],
                >::new([])),
            },
            &mut usig_0,
        )
        .unwrap();

        let mut usig_1 = UsigNoOp::default();

        usig_0.add_remote_party(ReplicaId::from_u64(0), ());
        usig_0.add_remote_party(ReplicaId::from_u64(1), ());
        usig_1.add_remote_party(ReplicaId::from_u64(0), ());
        usig_1.add_remote_party(ReplicaId::from_u64(1), ());

        let commit = Commit::sign(
            CommitContent {
                origin: ReplicaId::from_u64(0),
                prepare,
            },
            &mut usig_0,
        )
        .unwrap();

        let config = Config {
            n: NonZeroU64::new(3).unwrap(),
            t: 1,
            id: ReplicaId::from_u64(0),
            batch_timeout: Duration::from_secs(2),
            max_batch_size: None,
            initial_timeout_duration: Duration::from_secs(2),
            checkpoint_period: NonZeroU64::new(2).unwrap(),
        };

        assert!(commit.validate(&config, &mut usig_0).is_err());
        assert!(commit.validate(&config, &mut usig_1).is_err());
    }

    /// Tests if the validation of an invalid [Commit],
    /// in which the replica is unknown (not previously added as remote party), results in an error.
    #[test]
    fn validate_invalid_commit_unknown_remote_party() {
        let mut usig_0 = UsigNoOp::default();

        let prepare = Prepare::sign(
            PrepareContent {
                origin: ReplicaId::from_u64(0),
                view: View(0),
                request_batch: RequestBatch::new(Box::<
                    [client_request::ClientRequest<DummyPayload>; 0],
                >::new([])),
            },
            &mut usig_0,
        )
        .unwrap();

        let mut usig_1 = UsigNoOp::default();

        usig_0.add_remote_party(ReplicaId::from_u64(0), ());
        usig_1.add_remote_party(ReplicaId::from_u64(0), ());
        usig_1.add_remote_party(ReplicaId::from_u64(1), ());

        let commit = Commit::sign(
            CommitContent {
                origin: ReplicaId::from_u64(1),
                prepare,
            },
            &mut usig_1,
        )
        .unwrap();

        let config = Config {
            n: NonZeroU64::new(3).unwrap(),
            t: 1,
            id: ReplicaId::from_u64(0),
            batch_timeout: Duration::from_secs(2),
            max_batch_size: None,
            initial_timeout_duration: Duration::from_secs(2),
            checkpoint_period: NonZeroU64::new(2).unwrap(),
        };

        assert!(commit.validate(&config, &mut usig_1).is_ok());
        assert!(commit.validate(&config, &mut usig_0).is_err());
    }
}
