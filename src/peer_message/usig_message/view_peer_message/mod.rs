//! Defines a message of type [ViewPeerMessage].
//! Such messages are either of inner type [Prepare] or [Commit].
//! For further explanation of the inner types, refer to the specific
//! documentation.

pub(crate) mod commit;
pub(crate) mod prepare;

use anyhow::Result;
use serde::{Deserialize, Serialize};
use usig::{Count, Counter, Usig};

use crate::{config::Config, error::InnerError, ReplicaId, RequestPayload, View};

use self::{commit::Commit, prepare::Prepare};

/// Determines the inner types of a [ViewPeerMessage],
/// namely [Prepare] and [Commit].
#[derive(Serialize, Deserialize, Clone, Debug)]
pub(crate) enum ViewPeerMessage<P, Sig> {
    /// A ViewPeerMessage of type [Prepare].
    Prepare(Prepare<P, Sig>),
    /// A ViewPeerMessage of type [Commit].
    Commit(Commit<P, Sig>),
}

impl<P, Sig> From<Prepare<P, Sig>> for ViewPeerMessage<P, Sig> {
    /// Creates a [ViewPeerMessage] based on the given [Prepare].
    fn from(prepare: Prepare<P, Sig>) -> Self {
        Self::Prepare(prepare)
    }
}

impl<P, Sig> From<Commit<P, Sig>> for ViewPeerMessage<P, Sig> {
    /// Creates a [ViewPeerMessage] based on the given [Commit].
    fn from(commit: Commit<P, Sig>) -> Self {
        Self::Commit(commit)
    }
}

impl<P, Sig: Counter> Counter for ViewPeerMessage<P, Sig> {
    /// Returns the USIG counter of the [ViewPeerMessage] (either the USIG
    /// counter of the [Prepare] or of the [Commit]).
    fn counter(&self) -> Count {
        match self {
            ViewPeerMessage::Prepare(prepare) => prepare.counter(),
            ViewPeerMessage::Commit(commit) => commit.counter(),
        }
    }
}

impl<P, Sig> AsRef<ReplicaId> for ViewPeerMessage<P, Sig> {
    /// Referencing [ViewPeerMessage] returns a reference of its inner types
    /// (either of the [Prepare] or of the [Commit]).
    fn as_ref(&self) -> &ReplicaId {
        match self {
            ViewPeerMessage::Prepare(prepare) => prepare.as_ref(),
            ViewPeerMessage::Commit(commit) => commit.as_ref(),
        }
    }
}

impl<P: RequestPayload, Sig: Serialize> ViewPeerMessage<P, Sig> {
    /// Returns the [View] to which the [ViewPeerMessage] belongs to.
    pub(crate) fn view(&self) -> View {
        match self {
            Self::Prepare(prepare) => prepare,
            Self::Commit(commit) => &commit.data.prepare,
        }
        .view
    }

    /// Validates the [ViewPeerMessage].
    /// Essentially, the inner type of the [ViewPeerMessage] is validated.
    /// An [InnerError] is returned when the validation is unsuccessful.
    ///
    /// # Arguments
    ///
    /// * `config` - The [Config] of the replica.
    /// * `usig` - The USIG signature that should be a valid one for this
    ///            [ViewPeerMessage].
    pub(crate) fn validate(
        &self,
        config: &Config,
        usig: &mut impl Usig<Signature = Sig>,
    ) -> Result<(), InnerError> {
        match self {
            ViewPeerMessage::Prepare(prepare) => prepare.validate(config, usig),
            ViewPeerMessage::Commit(commit) => commit.validate(config, usig),
        }
    }
}
impl<P, Sig> ViewPeerMessage<P, Sig> {
    /// Returns the name of the inner type of the [ViewPeerMessage] as a String
    /// slice.
    pub(crate) fn msg_type(&self) -> &'static str {
        match self {
            ViewPeerMessage::Commit(_) => "Commit",
            ViewPeerMessage::Prepare(_) => "Prepare",
        }
    }
}

#[cfg(test)]

mod test {
    use rstest::rstest;
    use std::num::NonZeroU64;
    use usig::{noop::UsigNoOp, Usig};

    use rand::thread_rng;
    use usig::AnyId;

    use crate::{
        client_request::test::create_batch,
        peer_message::usig_message::view_peer_message::{
            commit::test::create_commit,
            prepare::test::{create_invalid_prepares, create_prepare},
            ViewPeerMessage,
        },
        tests::{
            add_attestations, create_config_default, get_random_included_replica_id,
            get_random_replica_id,
        },
        View,
    };

    /// Tests if the validation of a valid ViewPeerMessage succeeds.
    /// The inner type of the ViewPeerMessage is a Prepare.
    ///
    /// # Arguments
    ///
    /// * `n` - The number of replicas.
    #[rstest]
    fn validate_valid_vp_msg_from_prep(#[values(3, 4, 5, 6, 7, 8, 9, 10)] n: u64) {
        let n_parsed = NonZeroU64::new(n).unwrap();
        let mut rng = thread_rng();

        for t in 0..n / 2 {
            let primary_id = get_random_replica_id(n_parsed, &mut rng);
            let view = View(primary_id.as_u64());
            let mut usig_primary = UsigNoOp::default();
            let config_primary = create_config_default(n_parsed, t, primary_id);
            let request_batch = create_batch();
            let prepare = create_prepare(view, request_batch, &config_primary, &mut usig_primary);

            let backup_id = get_random_included_replica_id(n_parsed, primary_id, &mut rng);
            let mut usig_backup = UsigNoOp::default();
            let config_backup = create_config_default(n_parsed, t, backup_id);

            add_attestations(&mut vec![
                (primary_id, &mut usig_primary),
                (backup_id, &mut usig_backup),
            ]);

            let vp_msg = ViewPeerMessage::from(prepare);

            assert!(vp_msg.validate(&config_primary, &mut usig_primary).is_ok());
            assert!(vp_msg.validate(&config_backup, &mut usig_backup).is_ok());
        }
    }

    /// Tests if the validation of a valid ViewPeerMessage succeeds.
    /// The inner type of the ViewPeerMessage is a Commit.
    ///
    /// # Arguments
    ///
    /// * `n` - The number of replicas.
    #[rstest]
    fn validate_valid_vp_msg_from_commit(#[values(3, 4, 5, 6, 7, 8, 9, 10)] n: u64) {
        let n_parsed = NonZeroU64::new(n).unwrap();
        let mut rng = thread_rng();

        for t in 0..n / 2 {
            let primary_id = get_random_replica_id(n_parsed, &mut rng);
            let view = View(primary_id.as_u64());
            let mut usig_primary = UsigNoOp::default();
            let config_primary = create_config_default(n_parsed, t, primary_id);
            let request_batch = create_batch();
            let prepare = create_prepare(view, request_batch, &config_primary, &mut usig_primary);

            let backup_id = get_random_included_replica_id(n_parsed, primary_id, &mut rng);
            let mut usig_backup = UsigNoOp::default();
            let config_backup = create_config_default(n_parsed, t, backup_id);
            let commit = create_commit(backup_id, prepare, &mut usig_backup);

            add_attestations(&mut vec![
                (primary_id, &mut usig_primary),
                (backup_id, &mut usig_backup),
            ]);

            let vp_msg = ViewPeerMessage::from(commit);

            assert!(vp_msg.validate(&config_primary, &mut usig_primary).is_ok());
            assert!(vp_msg.validate(&config_backup, &mut usig_backup).is_ok());
        }
    }

    /// Tests if the validation of an invalid ViewPeerMessage fails.
    /// The inner type of the ViewPeerMessage is a Prepare.
    /// All variants of an invalid Prepare are tested.
    ///
    /// # Arguments
    ///
    /// * `n` - The number of replicas.
    #[rstest]
    fn validate_invalid_vp_msg_from_prepare(#[values(3, 4, 5, 6, 7, 8, 9, 10)] n: u64) {
        let n_parsed = NonZeroU64::new(n).unwrap();
        let mut rng = thread_rng();

        for t in 0..n / 2 {
            let primary_id = get_random_replica_id(n_parsed, &mut rng);
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

            let backup_id = get_random_included_replica_id(n_parsed, primary_id, &mut rng);
            let mut usig_backup = UsigNoOp::default();
            let config_backup = create_config_default(n_parsed, t, backup_id);

            usig_primary.add_remote_party(primary_id, ());
            usig_primary.add_remote_party(backup_id, ());
            usig_backup.add_remote_party(backup_id, ());

            let prepare_unknown_usig =
                create_prepare(view, request_batch, &config_primary, &mut usig_primary);
            let vp_msg = ViewPeerMessage::from(prepare_unknown_usig);
            assert!(vp_msg.validate(&config_primary, &mut usig_primary).is_ok());
            assert!(vp_msg.validate(&config_backup, &mut usig_backup).is_err());

            usig_backup.add_remote_party(primary_id, ());

            for prep_invalid in prepares_invalid {
                let vp_msg = ViewPeerMessage::from(prep_invalid);
                assert!(vp_msg.validate(&config_primary, &mut usig_primary).is_err());
                assert!(vp_msg.validate(&config_backup, &mut usig_backup).is_err());
            }
        }
    }

    /// Tests if the validation of an invalid ViewPeerMessage fails.
    /// The inner type of the ViewPeerMessage is a Commit.
    ///
    /// # Arguments
    ///
    /// * `n` - The number of replicas.
    #[rstest]
    fn validate_invalid_vp_msg_from_commit(#[values(3, 4, 5, 6, 7, 8, 9, 10)] n: u64) {
        let n_parsed = NonZeroU64::new(n).unwrap();
        let mut rng = thread_rng();

        for t in 0..n / 2 {
            let primary_id = get_random_replica_id(n_parsed, &mut rng);
            let view = View(primary_id.as_u64());
            let mut usig_primary = UsigNoOp::default();
            let config_primary = create_config_default(n_parsed, t, primary_id);
            let request_batch = create_batch();
            let prepare = create_prepare(view, request_batch, &config_primary, &mut usig_primary);

            let backup_id = get_random_included_replica_id(n_parsed, primary_id, &mut rng);
            let mut usig_backup = UsigNoOp::default();
            let config_backup = create_config_default(n_parsed, t, backup_id);
            let commit_invalid_origin =
                create_commit(prepare.origin, prepare.clone(), &mut usig_backup);

            usig_primary.add_remote_party(primary_id, ());
            usig_backup.add_remote_party(backup_id, ());
            usig_backup.add_remote_party(primary_id, ());

            let commit_unknown_usig = create_commit(backup_id, prepare, &mut usig_backup);
            let vp_msg = ViewPeerMessage::from(commit_unknown_usig);
            assert!(vp_msg.validate(&config_primary, &mut usig_primary).is_err());
            assert!(vp_msg.validate(&config_backup, &mut usig_backup).is_ok());

            usig_primary.add_remote_party(backup_id, ());

            let vp_msg = ViewPeerMessage::from(commit_invalid_origin);
            assert!(vp_msg.validate(&config_primary, &mut usig_primary).is_err());
            assert!(vp_msg.validate(&config_backup, &mut usig_backup).is_err());
        }
    }
}
