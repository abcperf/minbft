//! Defines a message of type [ViewPeerMessage].
//! Such messages are either of inner type [Prepare] or [Commit].
//! For further explanation of the inner types, see the specific documentation.

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
    /// Returns the USIG counter of the [ViewPeerMessage] (either the USIG counter of the [Prepare] or of the [Commit]).
    fn counter(&self) -> Count {
        match self {
            ViewPeerMessage::Prepare(prepare) => prepare.counter(),
            ViewPeerMessage::Commit(commit) => commit.counter(),
        }
    }
}

impl<P, Sig> AsRef<ReplicaId> for ViewPeerMessage<P, Sig> {
    /// Referencing [ViewPeerMessage] returns a reference of its inner types (either of the [Prepare] or of the [Commit]).
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
    use std::{num::NonZeroU64, time::Duration};

    use shared_ids::{ReplicaId, AnyId};
    use usig::{noop::{Signature, UsigNoOp}, Usig, Counter};

    use crate::{View, tests::DummyPayload, client_request::{RequestBatch, self}, Config};

    use super::{prepare::{Prepare, PrepareContent}, ViewPeerMessage, commit::{Commit, CommitContent}};


    /// Returns a [Prepare] with a default [UsigNoOp] as [Usig].
    ///
    /// # Arguments
    ///
    /// * `origin` - The ID of the replica to which the [Prepare] belongs to.
    ///              It should be the ID of the primary.
    /// * `view` - The current [View].
    pub(crate) fn create_prepare_default_usig(
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

    /// Returns a [Prepare] with the provided [Usig].
    ///
    /// # Arguments
    ///
    /// * `origin` - The ID of the replica to which the [Prepare] belongs to.
    ///              It should be the ID of the primary.
    /// * `view` - The current [View].
    /// * `usig` - The [Usig] to be used for signing the [Prepare].
    pub(crate) fn create_prepare_with_usig(
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

    /// Returns a [Commit] with a default [UsigNoOp] as [Usig].
    ///
    /// # Arguments
    ///
    /// * `origin` - The ID of the replica to which the [Commit] belongs to.
    ///              It should be the ID of the primary.
    /// * `prepare` - The [Prepare] to which this [Commit] belongs to.
    pub(crate) fn create_commit_default_usig(
        origin: ReplicaId,
        prepare: Prepare<DummyPayload, Signature>,
    ) -> Commit<DummyPayload, Signature> {
        Commit::sign(CommitContent { origin, prepare }, &mut UsigNoOp::default()).unwrap()
    }
    
    /// Returns a [Commit] with the provided [Usig].
    ///
    /// # Arguments
    ///
    /// * `origin` - The ID of the backup replica to which the [Commit] belongs
    ///              to.
    /// * `prepare` - The [Prepare] to which this [Commit] belongs to.
    /// * `usig` - The [Usig] to be used for signing the [Commit].
    pub(crate) fn create_commit_with_usig(
        origin: ReplicaId,
        prepare: Prepare<DummyPayload, Signature>,
        usig: &mut impl Usig<Signature = Signature>,
    ) -> Commit<DummyPayload, Signature> {
        Commit::sign(CommitContent { origin, prepare }, usig).unwrap()
    }

    /// Returns a [Config] with default values.
    ///
    /// # Arguments
    ///
    /// * `n` - The total number of replicas.
    /// * `t` - The maximum number of faulty replicas.
    /// * `id` - The ID of the replica to which this [Config] belongs to.
    pub(crate) fn create_config_default(n: NonZeroU64, t: u64, id: ReplicaId) -> Config {
        Config {
            n,
            t,
            id,
            batch_timeout: Duration::from_secs(2),
            max_batch_size: None,
            initial_timeout_duration: Duration::from_secs(2),
            checkpoint_period: NonZeroU64::new(2).unwrap(),
        }
    }

    /// Adds each [UsigNoOp] to each [UsigNoOp] as a remote party.
    ///
    /// # Arguments
    ///
    /// * `usigs` - The [UsigNoOp]s that shall be added as a remote party to
    ///             each other.
    pub(crate) fn add_attestations(mut usigs: Vec<&mut UsigNoOp>) {
        for i in 0..usigs.len() {
            for j in 0..usigs.len() {
                usigs[i].add_remote_party(ReplicaId::from_u64(j.try_into().unwrap()), ());
            }
        }
    }

    /// Creates a [ViewPeerMessage] from a [Prepare] by calling [`from()`]
    /// and tests if the underlying [Prepare] from the created [ViewPeerMessage]
    /// matches the passed [Prepare].
    #[test]
    fn view_peer_msg_from_contains_provided_prep() {
        let prep_origin = ReplicaId::from_u64(0);
        let prep_view = View(prep_origin.as_u64());
        let prep = create_prepare_default_usig(prep_origin, prep_view);
        let view_peer_msg = ViewPeerMessage::from(prep.clone());
        assert!(matches!(view_peer_msg, ViewPeerMessage::Prepare(prepare) if prep == prepare));
    }

    /// Creates a [ViewPeerMessage] from a [Commit] by calling [`from()`]
    /// and tests if the underlying [Commit] from the created [ViewPeerMessage]
    /// matches the passed [Commit].
    #[test]
    fn view_peer_msg_from_contains_provided_commit() {
        let prep_origin = ReplicaId::from_u64(0);
        let prep_view = View(prep_origin.as_u64());
        let prep = create_prepare_default_usig(prep_origin, prep_view);
        let commit_origin = ReplicaId::from_u64(1);
        let commit = create_commit_default_usig(commit_origin, prep);
        let view_peer_msg = ViewPeerMessage::from(commit.clone());
        assert!(matches!(view_peer_msg, ViewPeerMessage::Commit(vp_commit) if vp_commit.origin == commit.origin && vp_commit.prepare == commit.prepare));
    }

    /// Tests if the counter of a [ViewPeerMessage] that wraps a [Prepare] 
    /// corresponds to the counter of the underlying [Prepare].
    #[test]
    fn counter_of_view_peer_message_from_prep() {
        let prep_origin = ReplicaId::from_u64(0);
        let prep_view = View(prep_origin.as_u64());
        let prep = create_prepare_default_usig(prep_origin, prep_view);
        let view_peer_msg = ViewPeerMessage::Prepare(prep.clone());
        assert_eq!(view_peer_msg.counter(), prep.counter());
    }

    /// Tests if the counter of a [ViewPeerMessage] that wraps a [Commit] 
    /// corresponds to the counter of the underlying [Commit].
    #[test]
    fn counter_of_view_peer_message_from_commit() {
        let prep_origin = ReplicaId::from_u64(0);
        let prep_view = View(prep_origin.as_u64());
        let prep = create_prepare_default_usig(prep_origin, prep_view);
        let commit_origin = ReplicaId::from_u64(1);
        let commit = create_commit_default_usig(commit_origin, prep);
        let view_peer_msg = ViewPeerMessage::Commit(commit.clone());
        assert_eq!(view_peer_msg.counter(), commit.counter());
    }

    /// Tests if the reference of a [ViewPeerMessage] that wraps a [Prepare] 
    /// corresponds to the reference of the underlying [Prepare].
    #[test]
    fn ref_of_view_peer_message_from_prep() {
        let prep_origin = ReplicaId::from_u64(0);
        let prep_view = View(prep_origin.as_u64());
        let prep = create_prepare_default_usig(prep_origin, prep_view);
        let view_peer_msg = ViewPeerMessage::Prepare(prep.clone());
        assert_eq!(view_peer_msg.as_ref(), prep.as_ref());
    }

    /// Tests if the reference of a [ViewPeerMessage] that wraps a [Prepare] 
    /// corresponds to the reference of the underlying [Prepare].
    #[test]
    fn ref_of_view_peer_message_from_commit() {
        let prep_origin = ReplicaId::from_u64(0);
        let prep_view = View(prep_origin.as_u64());
        let prep = create_prepare_default_usig(prep_origin, prep_view);
        let commit_origin = ReplicaId::from_u64(1);
        let commit = create_commit_default_usig(commit_origin, prep);
        let view_peer_msg = ViewPeerMessage::Commit(commit.clone());
        assert_eq!(view_peer_msg.as_ref(), commit.as_ref());
    }

    /// Tests if the [View] of a [ViewPeerMessage] that wraps a [Prepare] 
    /// corresponds to the [View] of the underlying [Prepare].
    #[test]
    fn view_of_view_peer_message_from_prep() {
        let prep_origin = ReplicaId::from_u64(0);
        let prep_view = View(prep_origin.as_u64());
        let prep = create_prepare_default_usig(prep_origin, prep_view);
        let view_peer_msg = ViewPeerMessage::Prepare(prep.clone());
        assert_eq!(view_peer_msg.view(), prep.view);
    }

    /// Tests if the [View]] of a [ViewPeerMessage] that wraps a [Prepare] 
    /// corresponds to the [View] of the underlying [Prepare].
    #[test]
    fn view_of_view_peer_message_from_commit() {
        let prep_origin = ReplicaId::from_u64(0);
        let prep_view = View(prep_origin.as_u64());
        let prep = create_prepare_default_usig(prep_origin, prep_view);
        let commit_origin = ReplicaId::from_u64(1);
        let commit = create_commit_default_usig(commit_origin, prep);
        let view_peer_msg = ViewPeerMessage::Commit(commit.clone());
        assert_eq!(view_peer_msg.view(), commit.prepare.view);
    }

    /// Tests if validating a [ViewPeerMessage] that wraps a valid [Prepare] 
    /// succeeds.
    #[test]
    fn succ_validation_of_view_peer_message_from_prep() {
        let prep_origin = ReplicaId::from_u64(0);
        let prep_view = View(prep_origin.as_u64());
        let mut usig_primary = UsigNoOp::default();
        let prep = create_prepare_with_usig(prep_origin, prep_view, &mut usig_primary);
        let view_peer_msg = ViewPeerMessage::Prepare(prep);
        
        usig_primary.add_remote_party(prep_origin, ());
        let config = create_config_default(NonZeroU64::new(3).unwrap(), 1, prep_origin);
        let res_vp_validation = view_peer_msg.validate(&config, &mut usig_primary);

        assert!(res_vp_validation.is_ok());
        
    }

    /// Tests if validating a [ViewPeerMessage] that wraps a valid [Commit] 
    /// succeeds.
    #[test]
    fn succ_validation_of_view_peer_message_from_commit() {
        // Create Prepare.
        let id_primary = ReplicaId::from_u64(0);
        let view = View(0);
        let mut usig_primary = UsigNoOp::default();
        let prepare = create_prepare_with_usig(id_primary, view, &mut usig_primary);

        // Create Commit.
        let id_backup = ReplicaId::from_u64(1);
        let mut usig_backup = UsigNoOp::default();
        let commit = create_commit_with_usig(id_backup, prepare, &mut usig_backup);
        let view_peer_msg: ViewPeerMessage<_, Signature> = ViewPeerMessage::Commit(commit);

        // Add attestations.
        let usigs = vec![&mut usig_primary, &mut usig_backup];
        add_attestations(usigs);

        // Create config of backup.
        let config = create_config_default(NonZeroU64::new(3).unwrap(), 1, id_backup);

        let res_vp_validation = view_peer_msg.validate(&config, &mut usig_primary);

        assert!(res_vp_validation.is_ok());
    }

    /// Tests if validating a [ViewPeerMessage] that wraps an invalid [Prepare] 
    /// (origin is not the primary) fails.
    #[test]
    fn fail_validation_of_view_peer_message_from_prep_not_primary() {
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
    fn fail_validation_of_view_peer_message_from_prep_unknown_party() {
        let prep_origin = ReplicaId::from_u64(0);
        let prep_view = View(0);
        let mut usig_primary = UsigNoOp::default();
        let prep = create_prepare_with_usig(prep_origin, prep_view, &mut usig_primary);
        let view_peer_msg = ViewPeerMessage::Prepare(prep);
        
        let config = create_config_default(NonZeroU64::new(3).unwrap(), 1, prep_origin);
        let res_vp_validation = view_peer_msg.validate(&config, &mut usig_primary);

        assert!(res_vp_validation.is_err());
    }

    /// Tests if validating a [ViewPeerMessage] that wraps an invalid [Commit] 
    /// (origin is not the primary) fails.
    #[test]
    fn fail_validation_of_view_peer_message_from_commit_unknown_party() {
        let prep_origin = ReplicaId::from_u64(0);
        let prep_view = View(0);
        let mut usig_primary = UsigNoOp::default();
        let prep = create_prepare_with_usig(prep_origin, prep_view, &mut usig_primary);        

        let id_backup = ReplicaId::from_u64(1);
        let mut usig_backup = UsigNoOp::default();
        let commit = create_commit_with_usig(id_backup, prep, &mut usig_backup);

        let view_peer_msg = ViewPeerMessage::Commit(commit);

        let config = create_config_default(NonZeroU64::new(3).unwrap(), 1, id_backup);
        let res_vp_validation = view_peer_msg.validate(&config, &mut usig_primary);

        assert!(res_vp_validation.is_err());
    }

}