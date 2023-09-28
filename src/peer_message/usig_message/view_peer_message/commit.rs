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
        if self.origin == self.prepare.origin {
            return Err(InnerError::CommitFromPrimary {
                receiver: config.id,
                primary: self.origin,
            });
        }
        self.prepare.validate(config, usig)?;
        self.verify(usig).map_err(|usig_error| {
            InnerError::parse_usig_error(usig_error, config.id, "Commit", self.origin)
        })
    }
}

#[cfg(test)]
mod test {
    use std::{num::NonZeroU64, time::Duration};

    use shared_ids::{AnyId, ReplicaId};
    use usig::{noop::UsigNoOp, Usig};

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

    /// Tests if the validation of a valid [Commit] succeeds.
    #[test]
    fn validate_valid_commit() {
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
        usig_0.add_remote_party(ReplicaId::from_u64(1), ());
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
        assert!(commit.validate(&config, &mut usig_0).is_ok());
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
