//! Defines a message of type [PeerMessage].\
//! A [PeerMessage] is a message that is received from another peer, i.e. a
//! replica.\
//! A [PeerMessage] can be a Hello, a [ReqViewChange] or a [UsigMessage].\
//! Received [PeerMessage]s must be processed (see [crate::MinBft]).\
//! The processing depends on the inner type of the [PeerMessage].

use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use usig::{Counter, Usig};

use anyhow::Result;

use crate::{config::Config, error::InnerError, ReplicaId, RequestPayload};

use self::{req_view_change::ReqViewChange, usig_message::UsigMessage};

pub(crate) mod req_view_change;
pub(crate) mod usig_message;

/// A validated [PeerMessage] is either a message of type
/// Hello, [ReqViewChange] or [UsigMessage].
#[derive(Serialize, Deserialize, Clone, Debug)]
pub(crate) enum ValidatedPeerMessage<Att, P, Sig> {
    /// A validated [PeerMessage] of type Hello.
    Hello(Att),
    /// A validated [PeerMessage] of type [ReqViewChange].
    /// Used for requesting the change of [crate::View]s.
    ReqViewChange(ReqViewChange),
    /// A validated [PeerMessage] of type [UsigMessage].
    Usig(UsigMessage<P, Sig>),
}

impl<Att, P, Sig> ValidatedPeerMessage<Att, P, Sig> {
    /// Returns the inner type of the [ValidatedPeerMessage] as a String slice.
    pub(crate) fn msg_type(&self) -> &'static str {
        match self {
            ValidatedPeerMessage::Hello(_) => "Hello",
            ValidatedPeerMessage::ReqViewChange(_) => "ReqViewChange",
            ValidatedPeerMessage::Usig(m) => m.msg_type(),
        }
    }
}

impl<Att, P, Sig> From<ReqViewChange> for ValidatedPeerMessage<Att, P, Sig> {
    /// Create a [ValidatedPeerMessage] based on a message of type [ReqViewChange].
    fn from(req_view_change: ReqViewChange) -> Self {
        Self::ReqViewChange(req_view_change)
    }
}

impl<Att, T: Into<UsigMessage<P, Sig>>, P, Sig> From<T> for ValidatedPeerMessage<Att, P, Sig> {
    /// Create a [ValidatedPeerMessage] based on a message of type [UsigMessage].
    fn from(usig_message: T) -> Self {
        Self::Usig(usig_message.into())
    }
}

/// Defines a message that originates from a replica and is broadcasted to all replicas.\
/// All received [PeerMessage]s must be processed (see the documentation of the module).\
//
// The struct acts like a wrapper.\
// Its purpose is to make sure the peer-message is first validated.
#[derive(Serialize, Deserialize, Clone, Debug)]
#[repr(transparent)]
#[serde(transparent)]
pub struct PeerMessage<Att, P, Sig> {
    /// The message itself.
    peer_message: ValidatedPeerMessage<Att, P, Sig>,
}

impl<Att, P, Sig> PeerMessage<Att, P, Sig> {
    /// Returns the type of the [PeerMessage] as a String slice.
    pub(crate) fn msg_type(&self) -> &'static str {
        self.peer_message.msg_type()
    }
}

impl<Att, P: RequestPayload, Sig: Serialize + Counter + Debug> PeerMessage<Att, P, Sig> {
    /// Validate the [PeerMessage].\
    /// The validation is done according to the inner type of the message.
    ///
    /// # Arguments
    ///
    /// * `replica` - The ID of the replica from which the message originates.
    /// * `config` - The config of the replica.
    /// * `usig` - The USIG signature that should be a valid one for the
    /// [PeerMessage].
    ///
    /// # Return Value
    ///
    /// [Ok] if the validation succeeds, otherwise false.
    pub(crate) fn validate(
        self,
        replica: ReplicaId,
        config: &Config,
        usig: &mut impl Usig<Signature = Sig>,
    ) -> Result<ValidatedPeerMessage<Att, P, Sig>, InnerError> {
        match &self.peer_message {
            ValidatedPeerMessage::Hello(_) => {}
            ValidatedPeerMessage::Usig(usig_message) => usig_message.validate(config, usig)?,
            ValidatedPeerMessage::ReqViewChange(req_view_change_msg) => {
                req_view_change_msg.validate(replica, config)?
            }
        }
        Ok(self.peer_message)
    }
}

impl<Att, P, Sig, T: Into<ValidatedPeerMessage<Att, P, Sig>>> From<T> for PeerMessage<Att, P, Sig> {
    /// Create a [PeerMessage] based on a message of type [ValidatedPeerMessage].
    fn from(peer_message: T) -> Self {
        Self {
            peer_message: peer_message.into(),
        }
    }
}
