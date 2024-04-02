use shared_ids::{ClientId, ReplicaId};
use usig::{Count, UsigError};

use crate::View;

/// The detailed error types that may possibly occur when handling client messages, peer messages or timeouts.
#[derive(thiserror::Error, Debug)]
pub(super) enum InnerError {
    /// A replica (the given receiver) received a message of type Commit that was sent by the primary (the given primary).
    /// Primaries should not send Commits.
    #[error("replica {receiver:?} received Commit sent by the primary {primary:?}")]
    CommitFromPrimary {
        receiver: ReplicaId,
        primary: ReplicaId,
    },
    /// A replica (the given receiver) received a message of type Prepare
    /// that was sent by another replica (the given backup) other than the current view (the given view).
    #[error("replica {receiver:?} received Prepare sent by backup {backup:?} for view {view:?}")]
    PrepareFromBackup {
        receiver: ReplicaId,
        backup: ReplicaId,
        view: View,
    },
    /// A replica's (the given receiver) attempt to validate a Prepare
    /// sent by the given origin failed since it contains an invalid client request.
    #[error("replica's {receiver:?} validation of Prepare from {origin:?} failed since it contains an invalid client request")]
    RequestInPrepare {
        receiver: ReplicaId,
        origin: ReplicaId,
    },
    /// A replica's (the given receiver) attempt to validate a CheckpointCertificate
    /// from the given origin failed since it does not contain at least t + 1 messages.
    #[error("replica's ({receiver:?}) validation of the checkpoint certificate from {origin:?} failed since it does not contain at least t + 1 messages")]
    CheckpointCertNotSufficientMsgs {
        receiver: ReplicaId,
        origin: ReplicaId,
    },
    /// A replica's (the given receiver) attempt to validate a CheckpointCertificate
    /// from the given origin failed since not all checkpoint messages have the same state hash.
    #[error("replica's ({receiver:?}) validation of the checkpoint certificate from {origin:?} failed since not all checkpoint messages have the same state hash")]
    CheckpointCertNotAllSameStateHash {
        receiver: ReplicaId,
        origin: ReplicaId,
    },
    /// A replica's (the given receiver) attempt to validate a CheckpointCertificate
    /// from the given origin failed since not all checkpoint messages agree on
    /// the same counter of the latest accepted prepare.
    #[error("replica's ({receiver:?}) validation of the checkpoint certificate from {origin:?} failed since not all checkpoint messages agree on the same counter of the latest accepted Prepare")]
    CheckpointCertNotAllSameLatestPrep {
        receiver: ReplicaId,
        origin: ReplicaId,
    },
    /// A replica's (the given receiver) attempt to validate a CheckpointCertificate
    /// from the given origin failed since it not all checkpoint messages originate from a different replica.
    #[error("replica's ({receiver:?}) validation of the checkpoint certificate from {origin:?} failed since not all checkpoint messages originate from a different replica")]
    CheckpointCertNotAllDifferentOrigin {
        receiver: ReplicaId,
        origin: ReplicaId,
    },
    /// A replica's (the given receiver) attempt to validate a NewViewCheckpointCertificate
    /// from the given origin failed since it does not contain at least t + 1 messages.
    #[error("replica's ({receiver:?}) validation of the new-view's checkpoint certificate failed since it does not contain at least t + 1 view-change messages")]
    NewViewCheckpointCertNotSufficientMsgs { receiver: ReplicaId },
    /// A replica's (the given receiver) attempt to validate a NewViewCheckpointCertificate
    /// from the given origin failed since not all view-change messages are for the same next-view.
    #[error("replica's ({receiver:?}) validation of the new-view's checkpoint certificate failed since not all view-change messages are for the same next-view")]
    NewViewCheckpointCertNotAllSameNextView { receiver: ReplicaId },
    /// A replica's (the given receiver) attempt to validate a NewViewCheckpointCertificate
    /// from the given origin failed since not all view-change messages originate from a different replica.
    #[error("replica's ({receiver:?}) validation of the new-view's checkpoint certificate failed since not all view-change messages originate from a different replica")]
    NewViewCheckpointCertNotAllDifferentOrigin { receiver: ReplicaId },
    /// A replica's (the given receiver) attempt to validate a NewView message failed
    /// since the origin (origin_actual) is not the expected one (origin_expected).
    #[error("replica's {receiver:?} validation of the new-view message failed since the origin of the new-view message is {origin_actual:?} when it should be the expected next view ({origin_expected:?})")]
    NewViewContentUnexpectedNextView {
        receiver: ReplicaId,
        origin_actual: ReplicaId,
        origin_expected: ReplicaId,
    },
    /// A replica's attempt to validate a message of the given type (msg_type) from the given origin
    /// failed since the usig is invalid, resulting in the given UsigError (usig_error).
    #[error(
        "replica {replica:?} validation of the message of type {msg_type:?} from {origin:?} failed since the usig is invalid: {usig_error:?}"
    )]
    Usig {
        usig_error: UsigError,
        replica: ReplicaId,
        msg_type: &'static str,
        origin: ReplicaId,
    },
    /// A replica's (the given receiver) attempt to validate a ViewChange message
    /// from the given origin failed since there are holes in the sequence number of messages in its log.
    #[error("replica's ({receiver:?}) validation of the view-change message from {origin:?} failed since there are holes in the sequence number of messages in its log")]
    ViewChangeHolesInMessageLog {
        receiver: ReplicaId,
        origin: ReplicaId,
    },
    /// A replica's (the given receiver) attempt to validate a ViewChange message
    /// from the given origin failed since there are holes in the sequence number of messages in its log.
    #[error("replica's ({receiver:?}) validation of the view-change message from {origin:?} failed since the message log
    is empty when a certificate is passed: First message is expected to be the 
    checkpoint of the replica")]
    ViewChangeMessageLogEmptyWithCert {
        receiver: ReplicaId,
        origin: ReplicaId,
    },
    /// A replica's (the given receiver) attempt to validate a ViewChange message
    /// from the given origin failed since the counter of its last message (counter_last_msg) is not the expected one (counter_expected).
    #[error("replica's ({receiver:?}) validation of the view-change message from {origin:?} failed since the counter of its first message is {counter_first_msg:?} when it should be {counter_expected:?}")]
    ViewChangeFirstUnexpectedCounter {
        receiver: ReplicaId,
        origin: ReplicaId,
        counter_first_msg: Count,
        counter_expected: Count,
    },
    /// A replica's (the given receiver) attempt to validate a ViewChange message
    /// from the given origin failed since the counter of its last message (counter_last_msg) is not the expected one (counter_expected).
    #[error("replica's ({receiver:?}) validation of the view-change message from {origin:?} failed since the counter of its last message is {counter_last_msg:?} when it should be {counter_expected:?}")]
    ViewChangeLastUnexpectedCounter {
        receiver: ReplicaId,
        origin: ReplicaId,
        counter_last_msg: Count,
        counter_expected: Count,
    },
    /// A replica's (the given receiver) attempt to validate a ViewChange message
    /// from the given origin failed since its log is empty when it should not be the case.
    #[error("replica's ({receiver:?}) validation of the view-change message from {origin:?} failed since its log is empty when it should not be the case")]
    ViewChangeLogUnexpectedlyEmpty {
        receiver: ReplicaId,
        origin: ReplicaId,
    },
    /// A replica's (the given receiver) attempt to validate a ReqViewChange message
    /// from the given origin failed since the the previous view must be smaller than the next view.
    #[error("replica's ({receiver:?}) validation of the request for view-change from {origin:?} failed since the previous view must be smaller than the next view")]
    ReqViewChangeIncompatiblePrevNextView {
        receiver: ReplicaId,
        origin: ReplicaId,
    },
}

impl InnerError {
    /// Parses a UsigError to a OutputInnerError.
    pub(crate) fn parse_usig_error(
        usig_error: UsigError,
        replica: ReplicaId,
        msg_type: &'static str,
        origin: ReplicaId,
    ) -> Self {
        Self::Usig {
            usig_error,
            replica,
            msg_type,
            origin,
        }
    }
}

/// The error types that may possibly occur when handling client messages, peer messages or timeouts.
#[derive(thiserror::Error, Debug)]
pub enum Error {
    /// A replica's (receiver) validation of the message of type Commit from the given origin failed.
    #[error("replica's ({receiver:?}) validation of Commit from {origin:?} failed")]
    Commit {
        receiver: ReplicaId,
        origin: ReplicaId,
    },
    /// A replica's (receiver) validation of the message of type Prepare from the given origin failed.
    #[error("replica's ({receiver:?}) validation of Prepare from {origin:?} failed")]
    Prepare {
        receiver: ReplicaId,
        origin: ReplicaId,
    },
    /// A replica's (receiver) validation of the CheckpointCertificate from the given origin failed.
    #[error(
        "replica's ({receiver:?}) validation of the checkpoint certificate from {origin:?} failed"
    )]
    CheckpointCert {
        receiver: ReplicaId,
        origin: ReplicaId,
    },
    /// A replica's (receiver) validation of the NewViewCheckpointCertificate from the given origin failed.
    #[error("replica's ({receiver:?}) validation of the new-view checkpoint cert failed")]
    NewViewCheckpointCert { receiver: ReplicaId },
    /// A replica's (receiver) validation of the message of type NewView from the given origin failed.
    #[error("replica's ({receiver:?}) validation of the new-view message from {origin:?} failed")]
    NewView {
        receiver: ReplicaId,
        origin: ReplicaId,
    },
    /// A replica's (receiver) validation of the message of type ViewChange from the given origin failed.
    #[error(
        "replica's ({receiver:?}) validation of the view-change message from {origin:?} failed"
    )]
    ViewChange {
        receiver: ReplicaId,
        origin: ReplicaId,
    },
    /// A replica's (receiver) validation of the message of type ReqViewChange from the given origin failed.
    #[error("replica's ({receiver:?}) validation of the request for a view-change from {origin:?} failed")]
    ReqViewChange {
        receiver: ReplicaId,
        origin: ReplicaId,
    },
    /// A replica's (receiver) validation of the attestation from the given origin failed.
    #[error("replica's ({receiver:?}) validation of the attestation from {origin:?} failed")]
    Attestation {
        receiver: ReplicaId,
        origin: ReplicaId,
    },
    /// A replica's (receiver) validation of a client request from the given client (client_id) failed.
    #[error("replica's ({receiver:?}) validation of the request from {client_id:?} failed")]
    Request {
        receiver: ReplicaId,
        client_id: ClientId,
    },
    /// A replica's attempt to perform a usig operation for a message of the given type (msg_type)
    /// failed since the usig is invalid, resulting in the given UsigError (usig_error).
    #[error(
        "replica {replica:?} could not perform the desired usig operation for message of type {msg_type:?} as the usig is invalid: {usig_error:?}"
    )]
    Usig {
        replica: ReplicaId,
        msg_type: &'static str,
        usig_error: UsigError,
    },
}

impl From<InnerError> for Error {
    /// Create a TimeoutType based on the given InnerTimeoutType.
    fn from(inner: InnerError) -> Self {
        match inner {
            InnerError::CommitFromPrimary { receiver, primary } => Error::Commit {
                receiver,
                origin: primary,
            },
            InnerError::PrepareFromBackup {
                receiver,
                backup,
                view: _,
            } => Error::Prepare {
                receiver,
                origin: backup,
            },

            InnerError::RequestInPrepare { receiver, origin } => {
                Error::Prepare { receiver, origin }
            }
            InnerError::CheckpointCertNotSufficientMsgs { receiver, origin } => {
                Error::CheckpointCert { receiver, origin }
            }
            InnerError::CheckpointCertNotAllSameStateHash { receiver, origin } => {
                Error::CheckpointCert { receiver, origin }
            }
            InnerError::CheckpointCertNotAllSameLatestPrep { receiver, origin } => {
                Error::CheckpointCert { receiver, origin }
            }
            InnerError::CheckpointCertNotAllDifferentOrigin { receiver, origin } => {
                Error::CheckpointCert { receiver, origin }
            }
            InnerError::NewViewCheckpointCertNotSufficientMsgs { receiver } => {
                Error::NewViewCheckpointCert { receiver }
            }
            InnerError::NewViewCheckpointCertNotAllSameNextView { receiver } => {
                Error::NewViewCheckpointCert { receiver }
            }
            InnerError::NewViewCheckpointCertNotAllDifferentOrigin { receiver } => {
                Error::NewViewCheckpointCert { receiver }
            }
            InnerError::NewViewContentUnexpectedNextView {
                receiver,
                origin_actual,
                origin_expected: _,
            } => Error::NewView {
                receiver,
                origin: origin_actual,
            },
            InnerError::ViewChangeHolesInMessageLog { receiver, origin } => {
                Error::ViewChange { receiver, origin }
            }
            InnerError::ViewChangeMessageLogEmptyWithCert { receiver, origin } => {
                Error::ViewChange { receiver, origin }
            }
            InnerError::ViewChangeFirstUnexpectedCounter {
                receiver,
                origin,
                counter_first_msg: _,
                counter_expected: _,
            } => Error::ViewChange { receiver, origin },
            InnerError::ViewChangeLastUnexpectedCounter {
                receiver,
                origin,
                counter_last_msg: _,
                counter_expected: _,
            } => Error::ViewChange { receiver, origin },
            InnerError::ViewChangeLogUnexpectedlyEmpty { receiver, origin } => {
                Error::ViewChange { receiver, origin }
            }
            InnerError::ReqViewChangeIncompatiblePrevNextView { receiver, origin } => {
                Error::ReqViewChange { receiver, origin }
            }
            InnerError::Usig {
                usig_error,
                replica,
                msg_type,
                origin: _,
            } => Error::Usig {
                replica,
                msg_type,
                usig_error,
            },
        }
    }
}
