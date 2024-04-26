//! Provides Byzantine fault-tolerant consensus while reducing the amount of
//! consenting nodes (replicas) required as much as possible.
//!
//! Based on the paper ["Efficient Byzantine Fault-Tolerance" by
//! Veronese et al](doi: 10.1109/TC.2011.221), the crate provides an
//! implementation of a partially asynchronous Byzantine fault-tolerant atomic
//! broadcast (BFT) algorithm.
//! The algorithm requires n = 2t + 1 replicas in total, where t is the number
//! of faulty replicas.
//!
//! The intended way to use the library is to create an instance of the
//! struct [MinBft] for each replica, i.e. n instances.
//!
//! Instances of the struct [MinBft] may receive and handle messages from clients,
//! messages from peers (other replicas/instances), or timeouts using the
//! respective function.
//! Timeouts must be handled explicitly by calling the respective function.
//! See the dedicated function below for further explanation.
//!
//! This implementation was created as part of the [ABCperf project](doi: 10.1145/3626564.3629101).
//! An [integration in ABCperf](https://github.com/abcperf/demo) also exists.

use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::fmt;
use std::time::Duration;
use std::{cmp::Ord, collections::HashSet, fmt::Debug, ops::Add};

use anyhow::Result;
pub use config::BackoffMultiplier;
pub use config::Config;
use derivative::Derivative;
pub use error::Error;
pub use output::Output;
pub use peer_message::PeerMessage;
use peer_message_processor::collector::collector_checkpoints::CollectorCheckpoints;
use peer_message_processor::collector::collector_commits::CollectorCommits;
use peer_message_processor::collector::collector_req_view_changes::CollectorReqViewChanges;
use peer_message_processor::collector::collector_view_changes::CollectorViewChanges;
use request_processor::RequestProcessor;
use shared_ids::AnyId;
use shared_ids::{ClientId, ReplicaId, RequestId};
use timeout::TimeoutType;
use tracing::{debug, error, error_span, info, warn};
use usig::Count;
use usig::{Counter, Usig};

use serde::{Deserialize, Serialize};

use peer_message::{
    usig_message::{checkpoint::CheckpointCertificate, UsigMessage},
    ValidatedPeerMessage,
};
use usig_msg_order_enforcer::UsigMsgOrderEnforcer;

use crate::{
    client_request::ClientRequest,
    output::NotReflectedOutput,
    peer_message::{
        req_view_change::ReqViewChange,
        usig_message::view_peer_message::prepare::{Prepare, PrepareContent},
    },
};

mod config;
mod error;
mod peer_message;
mod peer_message_processor;
mod usig_msg_order_enforcer;

pub mod id;
pub mod output;
pub mod timeout;

mod client_request;
mod request_processor;
#[cfg(test)]
mod tests;

pub type MinHeap<T> = BinaryHeap<Reverse<T>>;

const BACKOFF_MULTIPLIER: u8 = 2;

/// Defines the trait the payload of a client-request must implement
/// in order to be receivable by a replica of a system of multiple replicas
/// that together form an atomic broadcast.
///
/// The payload of a client-request must have an ID.
/// It also has to define a function that verifies the payload using the ID of
/// the client.
pub trait RequestPayload: Clone + Serialize + for<'a> Deserialize<'a> + Debug {
    fn id(&self) -> RequestId;
    fn verify(&self, id: ClientId) -> Result<()>;
}

/// Defines the current view,
/// i.e. the primary replica of a system of multiple replicas
/// that together form an atomic broadcast.
///
/// The view is, therefore, in charge of generating Prepares and
/// batching them when creating a response to a client-request.
#[derive(
    Serialize, Deserialize, Debug, Clone, Copy, Ord, Eq, PartialEq, PartialOrd, Default, Hash,
)]
struct View(u64);

impl Add<u64> for View {
    type Output = Self;

    /// Defines the addition of a view with an unsigned integer.
    fn add(self, rhs: u64) -> Self::Output {
        Self(self.0 + rhs)
    }
}

impl fmt::Display for View {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "({0})", self.0)
    }
}

/// Defines the state of the view for a replica.
/// The state is to be set to this enum type when the view is non-faulty.
#[derive(Debug)]
struct InView<P, Sig> {
    /// The current non-faulty view.
    view: View,
    /// True if the replica sent a message of type ReqViewChange, otherwise false.
    has_requested_view_change: bool,
    /// Collects messages of type Commit (and the corresponding Prepare).
    collector_commits: CollectorCommits<P, Sig>,
}

/// Defines the state of the view for a replica when changing Views.
#[derive(Debug)]
struct ChangeInProgress {
    /// The previous view which turned out to be faulty.
    prev_view: View,
    /// The next view to be changed to.
    next_view: View,
    /// True if a message of type ViewChange has already been broadcast.
    /// Necessary to ensure exactly one ViewChange (from prev_view to next_view) is broadcast.
    has_broadcast_view_change: bool,
}

/// Defines the possible view states.
/// Either a replica is in the state of a functioning view or
/// in the state of changing views.
#[derive(Debug)]
enum ViewState<P, Sig> {
    /// The current view is functioning expectedly.  
    InView(InView<P, Sig>),
    /// A view-change is being performed.
    ChangeInProgress(ChangeInProgress),
}

impl<P: Clone, Sig: Counter + Clone> ViewState<P, Sig> {
    /// Creates a ViewState with the default initial values (state is InView).
    fn new() -> Self {
        Self::InView(InView {
            view: View::default(),
            has_requested_view_change: false,
            collector_commits: CollectorCommits::new(),
        })
    }
}

/// Defines the state of a replica.
#[derive(Clone, Debug, Derivative)]
#[derivative(Default(bound = "Sig: Counter"))]
struct ReplicaState<P, Sig> {
    usig_message_order_enforcer: UsigMsgOrderEnforcer<P, Sig>,
}

/// Defines a replica of a system of multiple replicas
/// that together form an atomic broadcast.
///
/// This is the main component of the crate.
/// A replica may receive client-requests, messages from other replicas
/// (peer messages) or timeouts.
/// It may send peer messages, too.
///
/// # Examples
///
/// 1. The replicas have to first initiate the communication between
/// each other by sending `Hello` messages to each other and performing
/// attestations.
/// 2. The return values of the public functions ([Output]) are to be
/// handled equally.
///
/// ```no_run
/// use anyhow::Result;
/// use std::{num::NonZeroU64, time::Duration, collections::HashMap};
/// use serde::{Deserialize, Serialize};
/// use core::fmt::Debug;
///
/// use shared_ids::{ReplicaId, ClientId, RequestId, AnyId};
/// use usig::{Usig, noop::UsigNoOp};
///
/// use minbft::{MinBft, Config, Output, RequestPayload, PeerMessage, timeout::{TimeoutType}};
///
/// #[derive(Deserialize, Serialize, Clone, Debug, Eq, PartialEq)]
/// struct SamplePayload {}
/// impl RequestPayload for SamplePayload {
///     fn id(&self) -> RequestId {
///         todo!()
///     }
///
///     fn verify(&self, id: ClientId) -> Result<()> {
///         todo!()
///     }
/// }
///
/// // Should handle the output.
/// fn handle_output<U: Usig>(output: Output<SamplePayload, U>) {
///     let Output { broadcasts, responses, timeout_requests, errors, ready_for_client_requests, primary: _, view_info: _, round: _ } = output;
///     for broadcast in broadcasts.iter() {
///         todo!();
///     }
///     for response in responses.iter() {
///         todo!();
///     }
///     for timeout_request in timeout_requests.iter() {
///         todo!();
///     }
/// }
///
/// let n = NonZeroU64::new(10).unwrap();
///
/// let mut (minbft, output) = MinBft::<SamplePayload, _>::new(
///         UsigNoOp::default(),
///         Config {
///             ..todo!() // see the crate [Config].
///         },
///     )
///     .unwrap();
///
/// // handle output to establish connection with peers, i.e., send messages
/// // contained in `broadcast` of struct Output.
/// handle_output(output);
///
/// let mut minbfts: HashMap<ReplicaId, MinBft<SamplePayload, _>> = setup_set(n);
/// let replica_id = ReplicaId::from_u64(0);
/// let minbft = minbfts.get_mut(&replica_id).unwrap();
///
/// let some_client_message: SamplePayload = todo!();
/// let output = minbft.handle_client_message(ClientId::from_u64(0), some_client_message);
/// handle_output(output);
///
/// let some_peer_message: PeerMessage<_, SamplePayload, _> = todo!();
/// let output = minbft.handle_peer_message(ReplicaId::from_u64(0), some_peer_message);
/// handle_output(output);
///
/// let some_timeout: (TimeoutType) = todo!();
/// let output = minbft.handle_timeout(some_timeout);
/// handle_output(output);
/// ```
#[derive(Derivative)]
#[derivative(Debug(bound = "U: Debug, U::Signature: Debug + Clone"))]
pub struct MinBft<P: RequestPayload, U: Usig>
where
    U::Attestation: Clone,
    U::Signature: Clone + Serialize,
    U::Signature: Debug,
{
    /// Used for USIG-signing messages of type UsigMessage.
    usig: U,

    /// Contains the configuration parameters for the algorithm.
    config: Config,

    /// Used for processing client-requests.
    request_processor: RequestProcessor<P, U>,

    /// Contains the UsigMessages that the replica created itself and broadcast.
    sent_usig_msgs: Vec<UsigMessage<P, U::Signature>>,

    /// Contains the state used to track each replica.
    replicas_state: Vec<ReplicaState<P, U::Signature>>,

    /// Either the state of the current view or the view-change state.
    view_state: ViewState<P, U::Signature>,

    /// The set containing the peers from which this replica received messages of type Hello.
    recv_hellos: HashSet<ReplicaId>,

    /// The counter of the last accepted Prepare.
    /// When the view changes, the struct field is set to the counter of the last UsigMessage sent by the new primary.
    /// This can be either the counter of the NewView or the counter of the last generated Checkpoint.
    counter_last_accepted_prep: Option<Count>,

    /// The collector of messages of type ReqViewChange.
    /// Collects ReqViewChanges filtered by their previous and next view.
    collector_rvc: CollectorReqViewChanges,

    /// The collector of messages of type ViewChange.
    /// Collects ViewChanges filtered by their next view.
    collector_vc: CollectorViewChanges<P, U::Signature>,

    /// Contains Checkpoints that together form a valid certificate.
    /// At the beginning, the struct field is set to None.
    /// Checkpoints certificates are generated periodically
    /// in order to clear the collection of sent UsigMessages (see struct field sent_usig_msgs).
    last_checkpoint_cert: Option<CheckpointCertificate<U::Signature>>,

    /// Contains currently received Checkpoints.
    /// Creates a certificate when sufficient valid Checkpoints have been collected.
    /// See the type of the struct itself for more intel.
    collector_checkpoints: CollectorCheckpoints<U::Signature>,

    /// Allows to increase the duration of timeouts exponentially.
    current_timeout_duration: Duration,
}

impl<P: RequestPayload, U: Usig> MinBft<P, U>
where
    U::Attestation: Clone,
    U::Signature: Clone + Serialize,
    U::Signature: Debug,
{
    /// Creates a new replica of a system of multiple replicas
    /// that together form an atomic broadcast.
    ///
    /// # Arguments
    ///
    /// * `usig` - The USIG signature of the [MinBft].
    /// * `config` - The configuration of the [MinBft].
    ///
    /// # Return Value
    ///
    /// A tuple consisting of the [MinBft] instance and its [Output].
    pub fn new(usig: U, config: Config) -> Result<(Self, Output<P, U>)> {
        let _minbft_span = error_span!("minbft", id = config.id.as_u64()).entered();

        config.validate();
        let mut minbft = Self {
            replicas_state: config
                .all_replicas()
                .map(|_| ReplicaState::default())
                .collect(),
            last_checkpoint_cert: None,
            sent_usig_msgs: Vec::new(),
            usig,
            request_processor: RequestProcessor::new(config.batch_timeout, config.max_batch_size),
            view_state: ViewState::new(),
            counter_last_accepted_prep: None,
            recv_hellos: HashSet::new(),
            collector_rvc: CollectorReqViewChanges::new(),
            collector_vc: CollectorViewChanges::new(),
            collector_checkpoints: CollectorCheckpoints::new(),
            current_timeout_duration: config.initial_timeout_duration,
            config,
        };
        let output = minbft.attest()?;
        let output = output.reflect(&mut minbft);
        Ok((minbft, output))
    }

    /// Returns the ID of the current primary, i.e., the one replica who creates
    /// Prepares for client requests, or [None] if the replica is in the
    /// inner state of changing views.
    pub fn primary(&self) -> Option<ReplicaId> {
        match &self.view_state {
            ViewState::InView(v) => Some(self.config.primary(v.view)),
            ViewState::ChangeInProgress(_) => None,
        }
    }

    /// Handles a message from a client.
    ///
    /// If the replica is in the state of changing views, the client-message is
    /// ignored.
    /// Otherwise, the client-message is not ignored, but undergoes several
    /// checks:
    ///
    /// 1. The request is verified regarding its validity and its age.
    ///    Should the request be too old, it is ignored.
    ///    Otherwise, and in case the replica is the current primary,
    ///    a message of type Prepare is broadcast to all replicas.
    /// 2. A timeout is set for the client-request.
    /// 3. In case batching is on, a timeout for the batch is set, too.
    ///
    /// # Arguments
    ///
    /// * `client_id` - The ID of the client from which the request originates.
    /// * `request` - The client request to be handled.
    ///
    /// # Return Value
    ///
    /// The adjusted [Output] containing relevant information regarding the
    /// handling of the client request, e.g., the response or errors.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use anyhow::Result;
    /// use serde::{Serialize, Deserialize};
    ///
    /// use minbft::{MinBft, Config, RequestPayload};
    /// use usig::noop::UsigNoOp;
    /// use shared_ids::{RequestId, ClientId, AnyId};
    ///
    /// #[derive(Deserialize, Serialize, Clone, Debug, Eq, PartialEq)]
    /// struct SamplePayload {}
    /// impl RequestPayload for SamplePayload {
    ///     fn id(&self) -> RequestId {
    ///         todo!()
    ///     }
    ///
    ///     fn verify(&self, id: ClientId) -> Result<()> {
    ///         todo!()
    ///     }
    /// }
    ///
    /// let (mut minbft, output) = MinBft::<SamplePayload, _>::new(
    ///        UsigNoOp::default(),
    ///        Config {
    ///            ..todo!() // [MinBft]
    ///        },
    ///    )
    ///    .unwrap();
    /// // handle output, see [MinBft]
    ///
    /// let some_client_message: SamplePayload = todo!();
    /// let output = minbft.handle_client_message(ClientId::from_u64(0), some_client_message);
    /// assert_eq!(output.responses[0], (ClientId::from_u64(0), some_client_message));
    /// // handle output, see [MinBft]
    /// ```
    pub fn handle_client_message(&mut self, client_id: ClientId, request: P) -> Output<P, U> {
        let _minbft_span = error_span!("minbft", id = self.config.id.as_u64()).entered();

        let req_id = request.id();

        debug!(
            "Handling client request (ID: {:?}, client ID: {:?}) ...",
            req_id, client_id
        );

        // Create output in order to return information regarding the handling of the client-message.
        let mut output = NotReflectedOutput::new(&self.config, &self.recv_hellos);

        // The payload of a client-request is forced to have a function that verifies itself.
        // It must be valid, otherwise it is not handled further.
        // Errors are stored in the output variable.
        debug!(
            "Verifying client request (ID {:?}, client ID: {:?}) ...",
            req_id, client_id
        );
        if request.verify(client_id).is_err() {
            error!(
                "Failed to handle client request (ID: {:?}, client ID: {:?}): Verification of client request failed.",
                req_id,
                client_id
            );
            output.error(Error::Request {
                receiver: self.config.id,
                client_id,
            });
            return output.reflect(self);
        }
        debug!(
            "Successfully verified client request (ID: {:?}, client ID: {:?}).",
            req_id, client_id
        );

        let client_request = ClientRequest {
            client: client_id,
            payload: request,
        };

        let (start_client_timeout, prepare_content, batch_timeout_request) =
            self.request_processor.process_client_req(
                client_request,
                &self.view_state,
                self.current_timeout_duration,
                &self.config,
            );

        if let Some(client_timeout) = start_client_timeout {
            output.timeout_request(client_timeout);
        }

        if let Some(prepare_content) = prepare_content {
            match Prepare::sign(prepare_content, &mut self.usig) {
                Ok(prepare) => {
                    debug!("Broadcast Prepare (view: {:?}, counter: {:?}) for client request (ID: {:?}, client ID: {:?}).", prepare.view, prepare.counter(), req_id, client_id);
                    output.broadcast(prepare, &mut self.sent_usig_msgs);
                }
                Err(usig_error) => {
                    error!("Failed to handle client request (ID: {:?}, client ID: {:?}): Failed to sign Prepare for client request before broadcasting it. For further information see output.", req_id, client_id);
                    output.process_usig_error(usig_error, self.config.me(), "Prepare");
                    return output.reflect(self);
                }
            }
        }

        if let Some(batch_timeout_request) = batch_timeout_request {
            output.timeout_request(batch_timeout_request);
        }

        output.reflect(self)
    }

    /// Handles a message of type PeerMessage.
    ///
    /// A message of type PeerMessage is a message from another replica.
    ///
    /// # Arguments
    ///
    /// * `from` - The ID of the replica from which the message originates.
    /// * `message` - The message of a peer (another replica) to be handled.
    ///
    /// The replica handles the message differently, depending on its concrete type.
    /// If the message is valid, it may trigger cascading events, i.e. the
    /// replica itself may broadcast a message in response to receiving this one,
    /// all depending on its inner state and the message's type. \
    /// Messages that are USIG-signed are guaranteed to be handled in correct
    /// order, i.e. messages with a lower count received from a specific replica
    /// are handled before messages with a higher count received from the same
    /// replica.
    ///
    /// # Return Value
    ///
    /// The adjusted [Output] containing relevant information regarding the
    /// handling of the peer message, e.g., response messages or errors.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use serde::{Serialize, Deserialize};
    ///
    /// use minbft::{MinBft, Config, PeerMessage, RequestPayload};
    /// use usig::noop::UsigNoOp;
    /// use shared_ids::{RequestId, ClientId};
    /// use anyhow::Result;
    ///
    /// #[derive(Deserialize, Serialize, Clone, Debug, Eq, PartialEq)]
    /// struct SamplePayload {}
    /// impl RequestPayload for SamplePayload {
    ///     fn id(&self) -> RequestId {
    ///         todo!()
    ///     }
    ///
    ///     fn verify(&self, id: ClientId) -> Result<()> {
    ///         todo!()
    ///     }
    /// }
    ///
    /// let (mut minbft_0, output) = MinBft::<SamplePayload, _>::new(
    ///        UsigNoOp::default(),
    ///        Config {
    ///            ..todo!() // [MinBft]
    ///        },
    ///    )
    ///    .unwrap();
    /// // handle output, see [MinBft]
    ///
    /// let (mut minbft_1, output) = MinBft::<SamplePayload, _>::new(
    ///        UsigNoOp::default(),
    ///        Config {
    ///            ..todo!() // [MinBft]
    ///        },
    ///    )
    ///    .unwrap();
    /// // handle output, see [MinBft]
    ///
    /// let some_peer_message: PeerMessage<_, SamplePayload, _> = todo!();
    /// // message is sent over network (it is serialized and deserialized)
    /// let output = minbft_0.handle_peer_message(todo!(), some_peer_message);
    /// // handle output, see [MinBft]
    /// ```
    pub fn handle_peer_message(
        &mut self,
        from: ReplicaId,
        message: PeerMessage<U::Attestation, P, U::Signature>,
    ) -> Output<P, U> {
        let _minbft_span = error_span!("minbft", id = self.config.id.as_u64()).entered();

        let msg_type = message.msg_type();

        debug!(
            "Handling message (origin: {:?}, type: {:?}) ...",
            from, msg_type,
        );

        assert_ne!(from, self.config.me());
        assert!(from.as_u64() < self.config.n.get());
        let mut output = NotReflectedOutput::new(&self.config, &self.recv_hellos);

        let message = match message.validate(from, &self.config, &mut self.usig) {
            Ok(message) => message,
            Err(output_inner_error) => {
                output.error(output_inner_error.into());
                return output.reflect(self);
            }
        };
        self.process_peer_message(from, message, &mut output);
        debug!(
            "Successfully handled message (origin: {:?}, from {:?}).",
            from, msg_type
        );
        output.reflect(self)
    }

    /// Handles a timeout according to their type.
    ///
    /// This function assumes no old timeouts are passed as parameters.
    ///
    /// Replicas may send timeout requests via [Output].
    /// Consequently, the timeout requests ([output::TimeoutRequest]) must be handled explicitly.
    /// This means, whenever a request to start a timeout is sent, it must be checked
    /// if there is already a timeout of the same type running, the request should be ignored.
    /// Whenever a request to stop a timeout is sent, a set timeout should only be stopped
    /// if the stop class and the type are the same.
    ///
    /// Set timeouts must be handled explicitly.
    ///
    /// They are handled differently depending on their type.
    ///
    /// A timeout for a batch is only handled if the primary is non-faulty
    /// from the standpoint of the replica.\
    /// Is this the case, then a message of type Prepare is created
    /// and broadcast for the next batch of client-requests.
    ///
    /// A timeout for a client-request is only handled if the primary is non-faulty
    /// from the standpoint of the replica.\
    /// Is this the case, then a view-change is requested.
    ///
    /// A timeout for a view-change is only handled if the replica is currently
    /// in the state of changing views.\
    /// Is this the case, then a view-change is requested.
    ///
    /// # Arguments
    ///
    /// * `timeout_type` - The type of the timeout to be handled.
    ///
    /// # Return Value
    ///
    /// The adjusted [Output] containing relevant information regarding the
    /// handling of the peer message, e.g., response messages or errors.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use serde::{Serialize, Deserialize};
    /// use anyhow::Result;
    ///
    /// use minbft::{MinBft, Config, RequestPayload, output::TimeoutRequest::{Start, Stop, StopAny}};
    /// use shared_ids::{ClientId, RequestId, AnyId};
    /// use usig::noop::UsigNoOp;
    ///
    /// #[derive(Deserialize, Serialize, Clone, Debug, Eq, PartialEq)]
    /// struct SamplePayload {}
    /// impl RequestPayload for SamplePayload {
    ///     fn id(&self) -> RequestId {
    ///         todo!()
    ///     }
    ///
    ///     fn verify(&self, id: ClientId) -> Result<()> {
    ///         todo!()
    ///     }
    /// }
    ///
    ///
    /// let (mut minbft, output) = MinBft::<SamplePayload, _>::new(
    ///        UsigNoOp::default(),
    ///        Config {
    ///            ..todo!() // [MinBft]
    ///        },
    ///    )
    ///    .unwrap();
    /// // handle output, see [MinBft]
    ///
    /// let some_client_message: SamplePayload = todo!();
    /// let output = minbft.handle_client_message(ClientId::from_u64(0), some_client_message);
    ///
    /// for timeout_request in output.timeout_requests.iter() {
    ///     match timeout_request {
    ///         Start(timeout) => {
    ///             // check if there is no timeout of the same type already set
    ///             // sleep for the duration of the timeout
    ///             minbft.handle_timeout(timeout.timeout_type);
    ///         }
    ///         Stop(timeout) => {
    ///             // if there is already a timeout set of the same type and
    ///             // stop class, stop it
    ///         }
    ///         StopAny(timeout) => {
    ///             // if there is already a timeout set of the same type and
    ///             // stop class, stop it
    ///         }
    ///     }
    /// }
    /// ```
    pub fn handle_timeout(&mut self, timeout_type: TimeoutType) -> Output<P, U> {
        let _minbft_span = error_span!("minbft", id = self.config.id.as_u64()).entered();
        debug!("Handling timeout (type: {:?}) ...", timeout_type);
        let mut output = NotReflectedOutput::new(&self.config, &self.recv_hellos);

        match timeout_type {
            TimeoutType::Batch => match &self.view_state {
                ViewState::InView(in_view) => {
                    let (maybe_batch, stop_timeout_request) =
                        self.request_processor.request_batcher.timeout();
                    output.timeout_request(stop_timeout_request);
                    let origin = self.config.me();
                    if let Some(batch) = maybe_batch {
                        debug!("Creating Prepare for timed out batch ...");
                        match Prepare::sign(
                            PrepareContent {
                                view: in_view.view,
                                origin,
                                request_batch: batch,
                            },
                            &mut self.usig,
                        ) {
                            Ok(prepare) => {
                                debug!("Successfully created Prepare for timed-out batch.");
                                debug!("Broadcast Prepare (view: {:?}, counter: {:?}) for timed-out batch.", prepare.view, prepare.counter());
                                output.broadcast(prepare, &mut self.sent_usig_msgs);
                                debug!("Successfully handled timeout (type: {:?}).", timeout_type);
                            }
                            Err(usig_error) => {
                                error!("Failed to handle timeout (type: {:?}): Failed to sign Prepare for batch before broadcasting it. For further information see output.", timeout_type);
                                output.process_usig_error(usig_error, origin, "Prepare");
                            }
                        };
                    }
                }
                ViewState::ChangeInProgress(in_progress) => {
                    warn!("Handling timeout resulted in skipping creation of Prepare for timed out batch: Replica is in progress of changing views (from: {:?}, to: {:?}).", in_progress.prev_view, in_progress.next_view);
                }
            },
            TimeoutType::Client => match &mut self.view_state {
                ViewState::InView(in_view) => {
                    warn!("Client request timed out.");
                    if !in_view.has_requested_view_change {
                        in_view.has_requested_view_change = true;
                        let msg = ReqViewChange {
                            prev_view: in_view.view,
                            next_view: in_view.view + 1,
                        };
                        info!(
                            "Broadcast ReqViewChange (previous view: {:?}, next view: {:?}).",
                            msg.prev_view, msg.next_view
                        );
                        output.broadcast(msg, &mut self.sent_usig_msgs)
                    } else {
                        debug!("Already broadcast ReqViewChange (previous view: {:?}, next view: {:?}).", in_view.view, in_view.view + 1);
                    }
                    debug!("Successfully handled timeout (type: {:?}).", timeout_type);
                }
                ViewState::ChangeInProgress(in_progress) => {
                    debug!("Handling timeout resulted in skipping creation of ReqViewChange: Replica is in progress of changing views (from: {:?}, to: {:?}).", in_progress.prev_view, in_progress.next_view);
                }
            },
            TimeoutType::ViewChange => match &mut self.view_state {
                ViewState::InView(in_view) => {
                    warn!("Handling timeout resulted in skipping creation of ReqViewChange: Replica is in view ({:?}).", in_view.view);
                }
                ViewState::ChangeInProgress(in_progress) => {
                    warn!("View-Change timed out.");
                    self.current_timeout_duration *= BACKOFF_MULTIPLIER as u32;
                    in_progress.has_broadcast_view_change = false;

                    let msg = ReqViewChange {
                        prev_view: in_progress.prev_view,
                        next_view: in_progress.next_view + 1,
                    };
                    info!(
                        "Broadcast ReqViewChange (previous view: {:?}, next view: {:?}).",
                        msg.prev_view, msg.next_view
                    );
                    output.broadcast(msg, &mut self.sent_usig_msgs);
                    debug!("Successfully handled timeout (type: {:?}).", timeout_type);
                }
            },
        }
        output.reflect(self)
    }

    /// Performs an attestation.
    fn attest(&mut self) -> Result<NotReflectedOutput<P, U>> {
        let attestation = self.usig.attest()?;
        let message = ValidatedPeerMessage::Hello(attestation);
        let mut output = NotReflectedOutput::new(&self.config, &self.recv_hellos);
        output.broadcast(message, &mut self.sent_usig_msgs);
        Ok(output)
    }
}
