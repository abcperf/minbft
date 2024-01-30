//! Defines a message of type [Prepare].
//! A [Prepare] consists of two main parts.
//! The first part is its content, the [PrepareContent].
//! It contains the ID of the replica ([ReplicaId]) which created the [Prepare].
//! Moreover, it contains the [View] to which the [Prepare] belongs to.
//! Furthermore, it contains the batch of requests ([RequestBatch]) to which it belongs to.
//! The second part is its signature, as [Prepare]s must be signed by a USIG.
//! For further explanation to why these messages (alongside other ones) must be signed by a USIG,
//! see the paper "Efficient Byzantine Fault Tolerance" by Veronese et al.
//! A [Prepare] is broadcast by the current primary (no other replicas are allowed to send a [Prepare]).
//! in response to a received client request.

use std::{cmp::Ordering, fmt};

use anyhow::Result;
use blake2::digest::Update;
use serde::{Deserialize, Serialize};
use shared_ids::ReplicaId;
use tracing::{debug, error};
use usig::{Counter, Usig};

use crate::{
    client_request::RequestBatch,
    error::InnerError,
    peer_message::usig_message::signed::{UsigSignable, UsigSigned},
    Config, RequestPayload, View,
};

/// The content of a message of type [Prepare].
/// The [View] and the batch of client requests to which the Prepare belongs to is needed.
/// Furthermore, the origin that created the [Prepare] is necessary.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub(crate) struct PrepareContent<P> {
    pub(crate) origin: ReplicaId,
    pub(crate) view: View,
    pub(crate) request_batch: RequestBatch<P>,
}

impl<P> AsRef<ReplicaId> for PrepareContent<P> {
    /// Referencing [PrepareContent] returns a reference to the origin set in the [PrepareContent].
    fn as_ref(&self) -> &ReplicaId {
        &self.origin
    }
}

impl<P: Serialize> UsigSignable for PrepareContent<P> {
    /// Hashes the content of a message of type [Prepare].
    /// Required for signing and verifying a message of type [Prepare].
    fn hash_content<H: Update>(&self, hasher: &mut H) {
        let encoded = bincode::serialize(self).unwrap();
        hasher.update(&encoded);
    }
}

/// The message of type [Prepare].
/// [Prepare]s consist of their content and must be signed by a USIG.
/// Such a message is broadcast by the current primary
/// in response to a received client request.
/// Only the primary is allowed to create a [Prepare] and broadcast it.
/// [Prepare]s can and should be validated.
/// For further explanation regarding the content of the module including [Prepare], see the documentation of the module itself.
/// For further explanation regarding the use of [Prepare]s, see the documentation of [crate::MinBft].
pub(crate) type Prepare<P, Sig> = UsigSigned<PrepareContent<P>, Sig>;

impl<P: RequestPayload, Sig: Serialize> fmt::Display for Prepare<P, Sig> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "(origin: {0}, view: {1})", self.origin, self.view)
    }
}

impl<P, Sig: Counter> PartialEq for Prepare<P, Sig> {
    /// Returns true if the counters of the [Prepare]s are equal, otherwise false.
    fn eq(&self, other: &Self) -> bool {
        self.counter().eq(&other.counter())
    }
}

impl<P, Sig: Counter> Eq for Prepare<P, Sig> {}

impl<P, Sig: Counter> PartialOrd for Prepare<P, Sig> {
    /// Partially compares the counters of the [Prepare]s.
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.counter().partial_cmp(&other.counter())
    }
}

impl<P, Sig: Counter> Ord for Prepare<P, Sig> {
    /// Compares the counters of the [Prepare]s.
    fn cmp(&self, other: &Self) -> Ordering {
        self.counter().cmp(&other.counter())
    }
}

impl<P: RequestPayload, Sig> Prepare<P, Sig> {
    /// Validates a message of type [Prepare].
    /// Following conditions must be met for the [Prepare] to be valid:
    ///     (1) The [Prepare] must originate from the current primary.
    ///     (2) The batch of requests to which the [Prepare] belongs to must be valid.
    ///         In other words, each batched request must be valid.
    ///     (3) Additionally, the signature of the [Prepare] must be verified.
    pub(crate) fn validate(
        &self,
        config: &Config,
        usig: &mut impl Usig<Signature = Sig>,
    ) -> Result<(), InnerError> {
        debug!(
            "Validating Prepare (origin: {:?}, view: {:?})...",
            self.origin, self.view
        );
        if !config.is_primary(self.view, self.origin) {
            error!(
                "Failed validating Prepare (origin: {:?}, view: {:?}): Prepare originates from a backup replica.",
                self.origin, self.view
            );
            return Err(InnerError::PrepareFromBackup {
                receiver: config.id,
                backup: self.origin,
                view: self.view,
            });
        }
        debug!(
            "Successfully validated Prepare (origin: {:?}, view: {:?}).",
            self.origin, self.view
        );
        self.request_batch
            .validate()
            .map_err(|_| InnerError::RequestInPrepare {
                receiver: config.id,
                origin: self.origin,
            })?;
        debug!(
            "Verifying signature of Prepare (origin: {:?}, view: {:?}) ...",
            self.origin, self.view
        );
        self.verify(usig).map_or_else(|usig_error| {
            error!(
                "Failed validating Prepare (origin: {:?}, view: {:?}): Signature of Prepare is invalid. For further information see output.",
                self.origin, self.view
            );
            Err(InnerError::parse_usig_error(usig_error, config.id, "Prepare", self.origin))
        }, |v| {
            debug!("Successfully verified signature of Prepare (origin: {:?}, view: {:?}).", self.origin, self.view);
            debug!("Successfully validated Prepare (origin: {:?}, view: {:?}).", self.origin, self.view);
            Ok(v)
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
        peer_message::usig_message::view_peer_message::prepare::{Prepare, PrepareContent},
        tests::{add_attestations, DummyPayload},
        Config, View,
    };

    /// Tests if the validation of a valid [Prepare] succeeds.
    #[test]
    fn validate_valid_prepare() {
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

        add_attestations(vec![&mut usig_0, &mut usig_1]);

        let config = Config {
            n: NonZeroU64::new(3).unwrap(),
            t: 1,
            id: ReplicaId::from_u64(0),
            batch_timeout: Duration::from_secs(2),
            max_batch_size: None,
            initial_timeout_duration: Duration::from_secs(2),
            checkpoint_period: NonZeroU64::new(2).unwrap(),
        };

        assert!(prepare.validate(&config, &mut usig_1).is_ok());
        assert!(prepare.validate(&config, &mut usig_0).is_ok());
    }

    /// Tests if the validation of an invalid [Prepare],
    /// in which the origin of the [Prepare] is not the primary, results in an error.
    #[test]
    fn validate_invalid_prep_not_primary() {
        let mut usig_0 = UsigNoOp::default();

        usig_0.add_remote_party(ReplicaId::from_u64(0), ());

        let prepare = Prepare::sign(
            PrepareContent {
                origin: ReplicaId::from_u64(1),
                view: View(0),
                request_batch: RequestBatch::new(Box::<
                    [client_request::ClientRequest<DummyPayload>; 0],
                >::new([])),
            },
            &mut usig_0,
        )
        .unwrap();

        let mut usig_1 = UsigNoOp::default();

        add_attestations(vec![&mut usig_0, &mut usig_1]);

        let config = Config {
            n: NonZeroU64::new(3).unwrap(),
            t: 1,
            id: ReplicaId::from_u64(0),
            batch_timeout: Duration::from_secs(2),
            max_batch_size: None,
            initial_timeout_duration: Duration::from_secs(2),
            checkpoint_period: NonZeroU64::new(2).unwrap(),
        };

        assert!(prepare.validate(&config, &mut usig_0).is_err());
        assert!(prepare.validate(&config, &mut usig_1).is_err());
    }

    /// Tests if the validation of an invalid [Prepare],
    /// in which the replica is unknown (not previously added as remote party), results in an error.
    #[test]
    fn validate_invalid_prepare_unknown_remote_party() {
        let mut usig_0 = UsigNoOp::default();

        let prepare = Prepare::sign(
            PrepareContent {
                origin: ReplicaId::from_u64(1),
                view: View(1),
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

        let config = Config {
            n: NonZeroU64::new(3).unwrap(),
            t: 1,
            id: ReplicaId::from_u64(0),
            batch_timeout: Duration::from_secs(2),
            max_batch_size: None,
            initial_timeout_duration: Duration::from_secs(2),
            checkpoint_period: NonZeroU64::new(2).unwrap(),
        };

        assert!(prepare.validate(&config, &mut usig_1).is_ok());
        assert!(prepare.validate(&config, &mut usig_0).is_err());
    }
}
