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
    use shared_ids::{ReplicaId, AnyId};
    use usig::{noop::{Signature, UsigNoOp}, Usig};

    use crate::{View, tests::DummyPayload, client_request::{RequestBatch, self}};

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

    /// Creates a [ViewPeerMessage] from a [Prepare] by calling [`from()`]
    /// and tests if the underlying [Prepare] from the created [ViewPeerMessage]
    /// matches the passed [Prepare].
    #[test]
    fn create_view_peer_message_from_prep() {
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
    fn create_view_peer_message_from_commit() {
        let prep_origin = ReplicaId::from_u64(0);
        let prep_view = View(prep_origin.as_u64());
        let prep = create_prepare_default_usig(prep_origin, prep_view);
        let commit_origin = ReplicaId::from_u64(1);
        let commit = create_commit_default_usig(commit_origin, prep);
        let view_peer_msg = ViewPeerMessage::from(commit.clone());
        assert!(matches!(view_peer_msg, ViewPeerMessage::Commit(vp_commit) if vp_commit.origin == commit.origin && vp_commit.prepare == commit.prepare));
    }
}