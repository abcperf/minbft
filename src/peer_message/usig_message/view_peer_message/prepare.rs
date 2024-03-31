//! Defines a message of type [Prepare].
//! A [Prepare] consists of two main parts.
//! The first part is its content, the [PrepareContent].
//! It contains the origin of the [Prepare], i.e., the ID of the replica
//! ([ReplicaId]) which created the [Prepare].
//! Moreover, it contains the [View] to which the [Prepare] belongs to.
//! Furthermore, it contains the batch of requests ([RequestBatch]) to which it
//! belongs to.
//! The second part is its signature, as [Prepare]s must be signed by a USIG.
//! For further explanation to why these messages (alongside other ones) must be
//! signed by a USIG,
//! refer to the paper "Efficient Byzantine Fault Tolerance" by Veronese et al.
//! A [Prepare] is broadcast by the current primary (no other replicas are
//! allowed to send a [Prepare]) in response to a received client request.

use std::{cmp::Ordering, fmt};

use anyhow::Result;
use blake2::digest::Update;
use serde::{Deserialize, Serialize};
use shared_ids::ReplicaId;
use tracing::{error, trace};
use usig::{Counter, Usig};

use crate::{
    client_request::RequestBatch,
    error::InnerError,
    peer_message::usig_message::signed::{UsigSignable, UsigSigned},
    Config, RequestPayload, View,
};

/// The content of a message of type [Prepare].
/// Consists of the [View] and the batch of client requests to which the
/// [Prepare] belongs to.
/// Furthermore, the origin that created the [Prepare] is necessary.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub(crate) struct PrepareContent<P> {
    /// The replica which the [Prepare] originates from.
    pub(crate) origin: ReplicaId,
    /// The [View] to which the [Prepare] belongs to.
    pub(crate) view: View,
    /// The [RequestBatch] to which the [Prepare] belongs to.
    pub(crate) request_batch: RequestBatch<P>,
}

impl<P> AsRef<ReplicaId> for PrepareContent<P> {
    /// Referencing [PrepareContent] returns a reference to the origin set in
    /// the [PrepareContent].
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
/// [Prepare]s consist of their content ([PrepareContent]) and must be signed by
/// a USIG.
/// Such a message is broadcast by the current primary in response to a received
/// client request.
/// Only the current primary is allowed to create a [Prepare] and broadcast it.
/// [Prepare]s can and should be validated.
pub(crate) type Prepare<P, Sig> = UsigSigned<PrepareContent<P>, Sig>;

impl<P: RequestPayload, Sig> fmt::Display for Prepare<P, Sig> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "(origin: {0}, view: {1})", self.origin, self.view)
    }
}

impl<P, Sig: Counter> PartialEq for Prepare<P, Sig> {
    /// Returns true if the counters of the [Prepare]s are equal, otherwise
    /// false.
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
    ///     (2) The batch of requests to which the [Prepare] belongs to must be
    ///         valid.
    ///         In other words, each batched request must be valid.
    ///     (3) Additionally, the signature of the [Prepare] must be verified.    ///
    ///
    /// # Arguments
    ///
    /// * `config` - The [Config] of the algorithm.
    /// * `usig` - The USIG signature that should be a valid one for this
    ///            [Prepare] message.
    pub(crate) fn validate(
        &self,
        config: &Config,
        usig: &mut impl Usig<Signature = Sig>,
    ) -> Result<(), InnerError> {
        trace!("Validating Prepare ({self})...");

        // Check for condition (1).
        if !config.is_primary(self.view, self.origin) {
            error!(
                "Failed validating Prepare ({self}): Prepare originates from 
            a backup replica."
            );
            return Err(InnerError::PrepareFromBackup {
                receiver: config.id,
                backup: self.origin,
                view: self.view,
            });
        }

        // Check for condition (2).
        self.request_batch
            .validate()
            .map_err(|_| InnerError::RequestInPrepare {
                receiver: config.id,
                origin: self.origin,
            })?;

        // Check for condition (3).
        trace!("Verifying signature of Prepare ({self}) ...");
        self.verify(usig).map_or_else(|usig_error| {
            error!(
                "Failed validating Prepare ({self}): Signature of Prepare is invalid. For further information see output."
            );
            Err(InnerError::parse_usig_error(usig_error, config.id, "Prepare", self.origin))
        }, |v| {
            trace!("Successfully verified signature of Prepare ({self}).");
            trace!("Successfully validated Prepare ({self}).");
            Ok(v)
        })
    }
}

#[cfg(test)]
pub(crate) mod test {
    use std::num::NonZeroU64;

    use rand::{rngs::ThreadRng, thread_rng};
    use rstest::rstest;
    use shared_ids::AnyId;
    use usig::{
        noop::{Signature, UsigNoOp},
        Usig,
    };

    use crate::{
        client_request::{test::create_batch, test::create_invalid_client_req, RequestBatch},
        tests::{
            add_attestations, create_config_default, get_random_backup_replica_id,
            get_random_replica_id, DummyPayload,
        },
        Config, View,
    };

    use super::{Prepare, PrepareContent};

    pub(crate) fn create_prepare(
        view: View,
        request_batch: RequestBatch<DummyPayload>,
        config: &Config,
        usig: &mut impl Usig<Signature = Signature>,
    ) -> Prepare<DummyPayload, Signature> {
        let origin = config.primary(view);
        Prepare::sign(
            PrepareContent {
                origin,
                view,
                request_batch,
            },
            usig,
        )
        .unwrap()
    }

    pub(crate) fn create_prepare_invalid_reqs(
        view: View,
        config: &Config,
        usig: &mut impl Usig<Signature = Signature>,
        rng: &mut ThreadRng,
    ) -> Prepare<DummyPayload, Signature> {
        let origin = config.primary(view);

        let client_req = create_invalid_client_req(rng);
        let batch = [client_req; 1];
        let batch = Box::new(batch);
        let request_batch = RequestBatch { batch };
        Prepare::sign(
            PrepareContent {
                origin,
                view,
                request_batch,
            },
            usig,
        )
        .unwrap()
    }

    pub(crate) fn create_prepare_invalid_origin(
        view: View,
        request_batch: RequestBatch<DummyPayload>,
        config: &Config,
        usig: &mut impl Usig<Signature = Signature>,
        rng: &mut ThreadRng,
    ) -> Prepare<DummyPayload, Signature> {
        let primary = config.primary(view);
        let backup_id = get_random_backup_replica_id(config.n, primary, rng);
        Prepare::sign(
            PrepareContent {
                origin: backup_id,
                view,
                request_batch,
            },
            usig,
        )
        .unwrap()
    }

    pub(crate) fn create_invalid_prepares(
        view: View,
        request_batch: RequestBatch<DummyPayload>,
        config: &Config,
        usig: &mut impl Usig<Signature = Signature>,
        rng: &mut ThreadRng,
    ) -> Vec<Prepare<DummyPayload, Signature>> {
        let prep_invalid_origin =
            create_prepare_invalid_origin(view, request_batch.clone(), config, usig, rng);
        let prep_invalid_reqs = create_prepare_invalid_reqs(view, config, usig, rng);
        let prep_invalid_usig =
            create_prepare(view, request_batch, config, &mut UsigNoOp::default());
        vec![prep_invalid_origin, prep_invalid_reqs, prep_invalid_usig]
    }

    /// Tests if the validation of a valid [Prepare](crate::peer_message::usig_message::view_peer_message::Prepare)
    /// succeeds.
    #[rstest]
    fn validate_valid_prepare(#[values(3, 4, 5, 6, 7, 8, 9, 10)] n: u64) {
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

            add_attestations(vec![
                (primary_id, &mut usig_primary),
                (backup_id, &mut usig_backup),
            ]);

            assert!(prepare.validate(&config_primary, &mut usig_primary).is_ok());
            assert!(prepare.validate(&config_backup, &mut usig_backup).is_ok());
        }
    }

    /// Tests if the validation of an invalid [Prepare](crate::peer_message::usig_message::view_peer_message::Prepare),
    /// in which the origin of the [Prepare](crate::peer_message::usig_message::view_peer_message::Prepare)
    /// is not the primary, results in an error.
    #[rstest]
    fn validate_invalid_prep_not_primary(#[values(3, 4, 5, 6, 7, 8, 9, 10)] n: u64) {
        let n_parsed = NonZeroU64::new(n).unwrap();
        let mut rng = thread_rng();

        for t in 0..n / 2 {
            let primary_id = get_random_replica_id(n_parsed);
            let view = View(primary_id.as_u64());
            let mut usig_primary = UsigNoOp::default();
            let config_primary = create_config_default(n_parsed, t, primary_id);
            let request_batch = create_batch();
            let prepare = create_prepare_invalid_origin(
                view,
                request_batch,
                &config_primary,
                &mut usig_primary,
                &mut rng,
            );

            let backup_id = get_random_backup_replica_id(n_parsed, primary_id, &mut rng);
            let mut usig_backup = UsigNoOp::default();
            let config_backup = create_config_default(n_parsed, t, backup_id);

            add_attestations(vec![
                (primary_id, &mut usig_primary),
                (backup_id, &mut usig_backup),
            ]);

            assert!(prepare
                .validate(&config_primary, &mut usig_primary)
                .is_err());
            assert!(prepare.validate(&config_backup, &mut usig_backup).is_err());
        }
    }

    #[rstest]
    fn validate_invalid_prep_reqs(#[values(3, 4, 5, 6, 7, 8, 9, 10)] n: u64) {
        let n_parsed = NonZeroU64::new(n).unwrap();
        let mut rng = thread_rng();

        for t in 0..n / 2 {
            let primary_id = get_random_replica_id(n_parsed);
            let view = View(primary_id.as_u64());
            let mut usig_primary = UsigNoOp::default();
            let config_primary = create_config_default(n_parsed, t, primary_id);
            let prepare =
                create_prepare_invalid_reqs(view, &config_primary, &mut usig_primary, &mut rng);

            let backup_id = get_random_backup_replica_id(n_parsed, primary_id, &mut rng);
            let mut usig_backup = UsigNoOp::default();
            let config_backup = create_config_default(n_parsed, t, backup_id);

            add_attestations(vec![
                (primary_id, &mut usig_primary),
                (backup_id, &mut usig_backup),
            ]);

            assert!(prepare
                .validate(&config_primary, &mut usig_primary)
                .is_err());
            assert!(prepare.validate(&config_backup, &mut usig_backup).is_err());
        }
    }

    /// Tests if the validation of an invalid [Prepare](crate::peer_message::usig_message::view_peer_message::Prepare),
    /// in which the replica is unknown (not previously added as remote party),
    /// results in an error.
    #[rstest]
    fn validate_invalid_prepare_unknown_remote_party(#[values(3, 4, 5, 6, 7, 8, 9, 10)] n: u64) {
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

            usig_primary.add_remote_party(primary_id, ());
            usig_backup.add_remote_party(backup_id, ());

            assert!(prepare.validate(&config_primary, &mut usig_primary).is_ok());
            assert!(prepare.validate(&config_backup, &mut usig_backup).is_err());
        }
    }
}
