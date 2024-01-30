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
    /// Returns the inner type of the [ViewPeerMessage] as a String slice.
    pub(crate) fn msg_type(&self) -> &'static str {
        match self {
            ViewPeerMessage::Commit(_) => "Commit",
            ViewPeerMessage::Prepare(_) => "Prepare",
        }
    }
}

#[cfg(test)]

mod test {
    use std::num::NonZeroU64;

    use rstest::rstest;
    use shared_ids::{AnyId, ReplicaId};
    use usig::{
        noop::{Signature, UsigNoOp},
        Counter, Usig,
    };

    use crate::{
        tests::{
            add_attestations, create_commit_default_usig, create_commit_with_usig,
            create_config_default, create_prepare_default_usig, create_prepare_with_usig,
            create_random_valid_commit_with_usig, create_random_valid_prepare_with_usig,
        },
        View,
    };

    use super::ViewPeerMessage;

    /// Creates a [ViewPeerMessage] from a [Prepare](crate::peer_message::usig_message::view_peer_message::Prepare)
    /// by calling [`from()`] and tests if the underlying [Prepare](crate::peer_message::usig_message::view_peer_message::Prepare)
    /// from the created [ViewPeerMessage] matches the passed [Prepare](crate::peer_message::usig_message::view_peer_message::Prepare) .
    #[rstest]
    fn from_prep_create_vp(#[values(3, 4, 5, 6, 7, 8, 9, 10)] n: u64) {
        let n_parsed = NonZeroU64::new(n).unwrap();

        let mut usig_primary = UsigNoOp::default();
        let prep = create_random_valid_prepare_with_usig(n_parsed, &mut usig_primary);

        let view_peer_msg = ViewPeerMessage::from(prep.clone());
        assert!(matches!(view_peer_msg, ViewPeerMessage::Prepare(vp_prep) if prep == vp_prep));
    }

    /// Creates a [ViewPeerMessage] from a [Commit](crate::peer_message::usig_message::view_peer_message::Commit)
    /// by calling [`from()`] and tests if the underlying
    /// [Commit](crate::peer_message::usig_message::view_peer_message::Commit)
    /// from the created [ViewPeerMessage] matches the passed
    /// [Commit](crate::peer_message::usig_message::view_peer_message::Commit).
    #[rstest]
    fn from_commit_create_vp(#[values(3, 4, 5, 6, 7, 8, 9, 10)] n: u64) {
        let n_parsed = NonZeroU64::new(n).unwrap();

        let mut usig_primary = UsigNoOp::default();
        let prep = create_random_valid_prepare_with_usig(n_parsed, &mut usig_primary);

        let mut usig_backup = UsigNoOp::default();
        let commit = create_random_valid_commit_with_usig(n_parsed, prep, &mut usig_backup);

        let view_peer_msg = ViewPeerMessage::from(commit.clone());
        assert!(
            matches!(view_peer_msg, ViewPeerMessage::Commit(vp_commit) if vp_commit.origin == commit.origin && vp_commit.prepare == commit.prepare)
        );
    }

    /// Tests if the counter of a [ViewPeerMessage] that wraps a
    /// [Prepare](crate::peer_message::usig_message::view_peer_message::Prepare)
    /// corresponds to the counter of the underlying
    /// [Prepare](crate::peer_message::usig_message::view_peer_message::Prepare).
    #[test]
    fn from_prep_create_vp_check_counter() {
        let prep_origin = ReplicaId::from_u64(0);
        let prep_view = View(prep_origin.as_u64());
        let prep = create_prepare_default_usig(prep_origin, prep_view);
        let view_peer_msg = ViewPeerMessage::Prepare(prep.clone());
        assert_eq!(view_peer_msg.counter(), prep.counter());
    }

    /// Tests if the counter of a [ViewPeerMessage] that wraps a
    /// [Commit](crate::peer_message::usig_message::view_peer_message::Commit)
    /// corresponds to the counter of the underlying
    /// [Commit](crate::peer_message::usig_message::view_peer_message::Commit).
    #[test]
    fn from_commit_create_vp_check_counter() {
        let prep_origin = ReplicaId::from_u64(0);
        let prep_view = View(prep_origin.as_u64());
        let prep = create_prepare_default_usig(prep_origin, prep_view);
        let commit_origin = ReplicaId::from_u64(1);
        let commit = create_commit_default_usig(commit_origin, prep);
        let view_peer_msg = ViewPeerMessage::Commit(commit.clone());
        assert_eq!(view_peer_msg.counter(), commit.counter());
    }

    /// Tests if the reference of a [ViewPeerMessage] that wraps a
    /// [Prepare](crate::peer_message::usig_message::view_peer_message::Prepare)
    /// corresponds to the reference of the underlying
    /// [Prepare](crate::peer_message::usig_message::view_peer_message::Prepare) .
    #[test]
    fn from_prep_create_vp_check_ref() {
        let prep_origin = ReplicaId::from_u64(0);
        let prep_view = View(prep_origin.as_u64());
        let prep = create_prepare_default_usig(prep_origin, prep_view);
        let view_peer_msg = ViewPeerMessage::Prepare(prep.clone());
        assert_eq!(view_peer_msg.as_ref(), prep.as_ref());
    }

    /// Tests if the reference of a [ViewPeerMessage] that wraps a
    /// [Commit](crate::peer_message::usig_message::view_peer_message::Commit)
    /// corresponds to the reference of the underlying
    /// [Commit](crate::peer_message::usig_message::view_peer_message::Commit) .
    #[test]
    fn from_commit_create_vp_check_ref() {
        let prep_origin = ReplicaId::from_u64(0);
        let prep_view = View(prep_origin.as_u64());
        let prep = create_prepare_default_usig(prep_origin, prep_view);
        let commit_origin = ReplicaId::from_u64(1);
        let commit = create_commit_default_usig(commit_origin, prep);
        let view_peer_msg = ViewPeerMessage::Commit(commit.clone());
        assert_eq!(view_peer_msg.as_ref(), commit.as_ref());
    }

    /// Tests if the [View] of a [ViewPeerMessage] that wraps a
    /// [Prepare](crate::peer_message::usig_message::view_peer_message::Prepare)
    /// corresponds to the [View] of the underlying
    /// [Prepare](crate::peer_message::usig_message::view_peer_message::Prepare).
    #[test]
    fn from_prep_create_vp_check_view() {
        let prep_origin = ReplicaId::from_u64(0);
        let prep_view = View(prep_origin.as_u64());
        let prep = create_prepare_default_usig(prep_origin, prep_view);
        let view_peer_msg = ViewPeerMessage::Prepare(prep.clone());
        assert_eq!(view_peer_msg.view(), prep.view);
    }

    /// Tests if the [View] of a [ViewPeerMessage] that wraps a
    /// [Commit](crate::peer_message::usig_message::view_peer_message::Commit)
    /// corresponds to the [View] of the underlying [Commit].
    #[test]
    fn from_commit_create_vp_check_view() {
        let prep_origin = ReplicaId::from_u64(0);
        let prep_view = View(prep_origin.as_u64());
        let prep = create_prepare_default_usig(prep_origin, prep_view);
        let commit_origin = ReplicaId::from_u64(1);
        let commit = create_commit_default_usig(commit_origin, prep);
        let view_peer_msg = ViewPeerMessage::Commit(commit.clone());
        assert_eq!(view_peer_msg.view(), commit.prepare.view);
    }

    /// Tests if validating a [ViewPeerMessage] that wraps a valid
    /// [Prepare](crate::peer_message::usig_message::view_peer_message::Prepare)
    /// succeeds.
    #[test]
    fn validate_valid_vp_prep_msg() {
        // Create Prepare.
        let prep_origin = ReplicaId::from_u64(0);
        let prep_view = View(prep_origin.as_u64());
        let mut usig_primary = UsigNoOp::default();
        let prep = create_prepare_with_usig(prep_origin, prep_view, &mut usig_primary);

        // Create ViewPeerMessage from Prepare.
        let view_peer_msg = ViewPeerMessage::Prepare(prep);

        // Add attestation of oneself.
        usig_primary.add_remote_party(prep_origin, ());

        // Create a default config.
        let config = create_config_default(NonZeroU64::new(3).unwrap(), 1, prep_origin);

        // Validate ViewPeerMessage using the previously created config and USIG.
        let res_vp_validation = view_peer_msg.validate(&config, &mut usig_primary);

        assert!(res_vp_validation.is_ok());
    }

    /// Tests if validating a [ViewPeerMessage] that wraps a valid
    /// [Commit](crate::peer_message::usig_message::view_peer_message::Commit)
    /// succeeds.
    #[test]
    fn validate_valid_vp_commit_msg() {
        // Create Prepare.
        let id_primary = ReplicaId::from_u64(0);
        let view = View(id_primary.as_u64());
        let mut usig_primary = UsigNoOp::default();
        let prepare = create_prepare_with_usig(id_primary, view, &mut usig_primary);

        // Create Commit.
        let id_backup = ReplicaId::from_u64(1);
        let mut usig_backup = UsigNoOp::default();
        let commit = create_commit_with_usig(id_backup, prepare, &mut usig_backup);
        let view_peer_msg: ViewPeerMessage<_, Signature> = ViewPeerMessage::Commit(commit);

        // Add attestations.
        let usigs = vec![
            (id_primary, &mut usig_primary),
            (id_backup, &mut usig_backup),
        ];
        add_attestations(usigs);

        // Create config of backup.
        let config = create_config_default(NonZeroU64::new(3).unwrap(), 1, id_backup);

        let res_vp_validation = view_peer_msg.validate(&config, &mut usig_primary);

        assert!(res_vp_validation.is_ok());
    }

    /// Tests if validating a [ViewPeerMessage] that wraps an invalid
    /// [Prepare](crate::peer_message::usig_message::view_peer_message::Prepare)
    /// (origin is not the primary) fails.
    #[test]
    fn validate_invalid_vp_prep_msg_not_primary() {
        let prep_origin = ReplicaId::from_u64(0);
        let prep_view = View(1);
        let mut usig_primary = UsigNoOp::default();
        let prep = create_prepare_with_usig(prep_origin, prep_view, &mut usig_primary);
        let view_peer_msg = ViewPeerMessage::Prepare(prep);

        usig_primary.add_remote_party(prep_origin, ());
        let config = create_config_default(NonZeroU64::new(3).unwrap(), 1, prep_origin);
        let res_vp_validation = view_peer_msg.validate(&config, &mut usig_primary);

        assert!(res_vp_validation.is_err());
    }

    /// Tests if validating a [ViewPeerMessage] that wraps an invalid [Prepare]
    /// (origin is not the primary) fails.
    #[test]
    fn validate_invalid_vp_prep_msg_unknown_party() {
        let prep_origin = ReplicaId::from_u64(0);
        let prep_view = View(0);
        let mut usig_primary = UsigNoOp::default();
        let prep = create_prepare_with_usig(prep_origin, prep_view, &mut usig_primary);

        let id_backup = ReplicaId::from_u64(1);
        let mut usig_backup = UsigNoOp::default();

        let view_peer_msg = ViewPeerMessage::Prepare(prep);

        let config_primary = create_config_default(NonZeroU64::new(3).unwrap(), 1, prep_origin);
        let config_backup = create_config_default(NonZeroU64::new(3).unwrap(), 1, id_backup);

        let res_vp_validation = view_peer_msg.validate(&config_primary, &mut usig_primary);
        assert!(res_vp_validation.is_err());

        let res_vp_validation = view_peer_msg.validate(&config_backup, &mut usig_backup);
        assert!(res_vp_validation.is_err());
    }

    /// Tests if validating a [ViewPeerMessage] that wraps an invalid [Commit]
    /// (origin is not the primary) fails.
    #[test]
    fn validate_invalid_vp_commit_msg_unknown_party() {
        let prep_origin = ReplicaId::from_u64(0);
        let prep_view = View(prep_origin.as_u64());
        let mut usig_primary = UsigNoOp::default();
        let prep = create_prepare_with_usig(prep_origin, prep_view, &mut usig_primary);

        let id_backup = ReplicaId::from_u64(1);
        let mut usig_backup = UsigNoOp::default();
        let commit = create_commit_with_usig(id_backup, prep, &mut usig_backup);

        let view_peer_msg = ViewPeerMessage::Commit(commit);

        let config_primary = create_config_default(NonZeroU64::new(3).unwrap(), 1, prep_origin);
        let config_backup = create_config_default(NonZeroU64::new(3).unwrap(), 1, id_backup);

        let res_vp_validation = view_peer_msg.validate(&config_primary, &mut usig_primary);
        assert!(res_vp_validation.is_err());
        let res_vp_validation = view_peer_msg.validate(&config_backup, &mut usig_backup);
        assert!(res_vp_validation.is_err());
    }
}
