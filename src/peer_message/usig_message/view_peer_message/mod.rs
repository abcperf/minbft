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
