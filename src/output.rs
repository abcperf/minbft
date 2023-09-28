use std::collections::HashSet;
use std::time::Duration;

use shared_ids::{ClientId, ReplicaId};
use tracing::{debug, error_span};

use usig::{Usig, UsigError};

use crate::{Config, Error};

use crate::{
    peer_message::{usig_message::UsigMessage, ValidatedPeerMessage},
    timeout::Timeout,
    PeerMessage, RequestPayload,
};

/// The type that can only be constructed in this module.
/// This allows for trait functions that are only callable by this module.
pub(super) struct OutputRestricted(());

/// Contains the PeerMessages to be broadcasted.
type BroadcastList<Att, P, Sig> = Box<[PeerMessage<Att, P, Sig>]>;

/// Collects all the information a participant (of a system of multiple participants
/// that together form an atomic broadcast) may generate when handling
/// client-requests, peer-messages or timeouts.
///
/// A participant may generate broadcasts to other participants,
/// responses to client-requests, timeouts for messages of different kinds, or
/// various errors when handling client-requests, peer-messages or timeouts.
/// In addition, it keeps track of whether the participant is ready to receive client requests
/// and who the current primary participant is.
pub struct Output<P, U: Usig> {
    /// The messages to be broadcasted.
    pub broadcasts: BroadcastList<U::Attestation, P, U::Signature>,
    /// Contains the responses for the Clients (identified by their ClientId).
    pub responses: Box<[(ClientId, P)]>,
    /// Contains the timeout requests.
    pub timeout_requests: Box<[TimeoutRequest]>,
    /// Contains the errors possibly returned upon the receival and processing of messages.
    pub errors: Box<[Error]>,
    /// True if the participant is ready to receive client requests, otherwise false.
    pub ready_for_client_requests: bool,
    /// The current primary if the participant is in the state InView.
    pub primary: Option<ReplicaId>,
}

/// Collects all the non-reflected output, i.e. without own messages, a participant (of a system of multiple participants
/// that together form an atomic broadcast) may generate when handling
/// client-requests, peer-messages or timeouts.
pub(super) struct NotReflectedOutput<P, U: Usig> {
    /// The messages to be broadcasted.
    broadcasts: Vec<ValidatedPeerMessage<U::Attestation, P, U::Signature>>,
    /// Contains the responses for the Clients (identified by their ClientId).
    responses: Vec<(ClientId, P)>,
    /// Contains the timeout requests.
    timeout_requests: Vec<TimeoutRequest>,
    /// Contains the errors possibly returned upon the receival and processing of messages.
    errors: Vec<Error>,
    /// True if the participant is ready for client requests, otherwise false.
    ready_for_client_requests: bool,
}

/// Defines the trait of a participant being reflectable,
/// i.e. to be able to receive its own messages.
pub(super) trait Reflectable<P, U: Usig> {
    /// Processes its own reflected message of type PeerMessage.
    fn process_reflected_peer_message(
        &mut self,
        peer_message: ValidatedPeerMessage<U::Attestation, P, U::Signature>,
        output: &mut NotReflectedOutput<P, U>,
        restricted: OutputRestricted,
    );

    /// Returns the current primary participant.
    fn current_primary(&self, restricted: OutputRestricted) -> Option<ReplicaId>;
}

impl<P: RequestPayload, U: Usig> NotReflectedOutput<P, U>
where
    U::Attestation: Clone,
    U::Signature: Clone,
{
    pub(super) fn new(config: &Config, recv_hello_from: &HashSet<ReplicaId>) -> Self {
        let ready_for_client_requests = recv_hello_from.len() as u64 == config.n.get();
        NotReflectedOutput {
            broadcasts: Vec::new(),
            responses: Vec::new(),
            timeout_requests: Vec::new(),
            errors: Vec::new(),
            ready_for_client_requests,
        }
    }

    /// Broadcast the given message of type PeerMessage.
    pub(super) fn broadcast(
        &mut self,
        message: impl Into<ValidatedPeerMessage<U::Attestation, P, U::Signature>>,
        message_log: &mut Vec<UsigMessage<P, U::Signature>>,
    ) {
        let message = message.into();

        debug!("broadcasting message of type {:?}", message.msg_type(),);

        if let ValidatedPeerMessage::Usig(msg) = &message {
            message_log.push(msg.clone());
        }

        self.broadcasts.push(message);
    }

    /// Collects the given response.
    pub(super) fn response(&mut self, client_id: ClientId, output: P) {
        debug!("response to {:?} for {:?}", client_id, output.id());
        self.responses.push((client_id, output));
    }

    /// Sets the given timeout.
    pub(super) fn timeout_request(&mut self, timeout_request: TimeoutRequest) {
        match &timeout_request {
            TimeoutRequest::Start(timeout) => {
                debug!(
                    "set request for starting timeout (type: {:?}, duration: {:?})",
                    timeout.timeout_type, timeout.duration,
                );
            }
            TimeoutRequest::Stop(timeout_type) => {
                debug!(
                    "set request for stopping timeout (type: {:?})",
                    timeout_type
                );
            }
        }
        self.timeout_requests.push(timeout_request);
    }

    /// Processes the given UsigError by parsing it to
    /// an OutputError and collecting it.
    pub(super) fn process_usig_error(
        &mut self,
        usig_error: UsigError,
        replica: ReplicaId,
        msg_type: &'static str,
    ) {
        let output_error = Error::Usig {
            replica,
            msg_type,
            usig_error,
        };
        self.error(output_error);
    }

    /// Collects the given OutputError.
    pub(super) fn error(&mut self, output_error: Error) {
        self.errors.push(output_error);
    }

    /// Returns true if the participant is ready to receive client requests, otherwise false.
    pub(super) fn ready_for_client_requests(&mut self) {
        debug!("hello done");

        self.ready_for_client_requests = true;
    }

    /// Receives and processes messages that the reflectable sent to itself.
    pub(super) fn reflect<S: Reflectable<P, U>>(mut self, reflectable: &mut S) -> Output<P, U> {
        let _minbft_span = error_span!("reflecting").entered();

        let mut last_len = 0;

        loop {
            let cur_len = self.broadcasts.len();
            if last_len == cur_len {
                break;
            }
            let messages: Vec<_> = self.broadcasts.iter().skip(last_len).cloned().collect();
            for message in messages {
                reflectable.process_reflected_peer_message(
                    message,
                    &mut self,
                    OutputRestricted(()),
                );
            }
            last_len = cur_len;
        }

        let broadcast = self.broadcasts.into_iter().map(|m| m.into()).collect();

        let primary = reflectable.current_primary(OutputRestricted(()));

        Output {
            broadcasts: broadcast,
            responses: self.responses.into_boxed_slice(),
            timeout_requests: self.timeout_requests.into_boxed_slice(),
            errors: self.errors.into_boxed_slice(),
            ready_for_client_requests: self.ready_for_client_requests,
            primary,
        }
    }
}

/// A [TimeoutRequest] may be either a request to start a [Timeout] or to stop it.
/// [crate::MinBft] outputs [TimeoutRequest]s when handling messages.
/// The [TimeoutRequest]s must be handled externally.
/// For further explanation, see [crate::MinBft].
#[derive(Debug, Clone)]
pub enum TimeoutRequest {
    Start(Timeout),
    Stop(Timeout),
}

impl TimeoutRequest {
    /// Creates a new [TimeoutRequest::Start] for a [Timeout] of type Batch with the given duration.
    pub(crate) fn new_start_batch_req(duration: Duration) -> Self {
        Self::Start(Timeout::batch(duration))
    }

    /// Creates a new [TimeoutRequest::Start] for a [Timeout] of type Client with the given [ClientId] and duration.
    pub(crate) fn new_start_client_req(client_id: ClientId, duration: Duration) -> Self {
        Self::Start(Timeout::client(client_id, duration))
    }

    /// Creates a new [TimeoutRequest::Start] for a [Timeout] of type ViewChange with the given duration.
    pub(crate) fn new_start_vc_req(duration: Duration) -> Self {
        Self::Start(Timeout::view_change(duration))
    }

    /// Creates a new [TimeoutRequest::Stop] for a [Timeout] of type Batch.
    pub(crate) fn new_stop_batch_req() -> Self {
        Self::Stop(Timeout::batch(Duration::from_secs(0)))
    }

    /// Creates a new [TimeoutRequest::Stop] for a [Timeout] of type Client with the given [ClientId].
    pub(crate) fn new_stop_client_req(client_id: ClientId) -> Self {
        Self::Stop(Timeout::client(client_id, Duration::from_secs(0)))
    }

    /// Creates a new [TimeoutRequest::Stop] for a [Timeout] of type ViewChange.
    pub(crate) fn new_stop_vc_req() -> Self {
        Self::Stop(Timeout::view_change(Duration::from_secs(0)))
    }
}
