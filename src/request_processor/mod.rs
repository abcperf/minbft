mod checkpoint_generator;
mod request_batcher;

use std::{
    cmp::Ordering,
    collections::{HashMap, VecDeque},
    num::NonZeroUsize,
    time::{Duration, Instant},
};

use derivative::Derivative;
use serde::{Deserialize, Serialize};
use shared_ids::{ClientId, RequestId};
use tracing::{debug, trace, warn};
use usig::Usig;

use crate::{
    client_request::ClientRequest,
    output::{NotReflectedOutput, TimeoutRequest},
    peer_message::usig_message::{
        checkpoint::CheckpointContent,
        view_peer_message::prepare::{Prepare, PrepareContent},
    },
    Config, RequestPayload, ViewState,
};

use self::{checkpoint_generator::CheckpointGenerator, request_batcher::RequestBatcher};

/// Defines the state of a Client.
/// Contains the ID of the last accepted client request and the currently
/// processing client request with its ID.
#[derive(Debug, Serialize, Deserialize, Derivative)]
#[derivative(Default(bound = ""))]
struct ClientState<P> {
    /// The last accepted client request.
    last_accepted_req: Option<RequestId>,
    /// The currently processing, not yet accepted client request with its ID.
    currently_processing_req: Option<(RequestId, ClientRequest<P>)>,
}

impl<P> ClientState<P> {
    /// Update [ClientState] when receiving a new client request.
    ///
    /// # Arguments
    ///
    /// * `request_id` - The ID of the request received.
    /// * `client_req` - The client request received.
    ///
    /// # Return Value
    ///
    /// Returns true if the [RequestId] is greater than the currently processing
    /// request or if there is no currently processing request, otherwise false.
    ///
    ///
    fn update_upon_request_receival(
        &mut self,
        request_id: RequestId,
        client_req: ClientRequest<P>,
    ) -> bool {
        if Some(request_id) <= self.last_accepted_req {
            warn!("Ignored request to update client state with an old client request with ID {:?} from client with ID {:?}: last accepted request of the same client had ID {:?}.", request_id, client_req.client, self.last_accepted_req);
            return false;
        }

        if let Some(processing) = &self.currently_processing_req {
            match request_id.cmp(&processing.0) {
                Ordering::Less => {
                    trace!("Ignored request to update client state with an old client request with ID {:?} from client with ID {:?}: currently processing request of the same client is newer, has ID {:?}.", request_id, client_req.client, processing.0);
                    false
                }

                Ordering::Equal => {
                    // It was seen before.
                    trace!("Ignored request to update client state with client request with ID {:?} from client with ID {:?} which was already previously received and is being processed.", request_id, client_req.client);
                    false
                }
                Ordering::Greater => {
                    trace!("Updated client state with client request with ID {:?} from client with ID {:?}: received request is newer than the currently processing one with ID {:?} of the same client.", request_id, client_req.client, processing.0);
                    self.currently_processing_req = Some((request_id, client_req));
                    true
                }
            }
        } else {
            trace!(
                "Updated client state with client request with ID {:?} from client with ID {:?}: no request was currently being processed of the same client.",
                request_id, client_req.client
            );
            self.currently_processing_req = Some((request_id, client_req));
            true
        }
    }

    /// Update [ClientState] when completing a client request.
    ///
    /// Ensures the ID of the request completed is higher than the ID of the
    /// last accepted request.
    /// Sets the currently processing request to [None] if its ID is lower
    /// or equal to the completed request.
    ///
    /// # Arguments
    ///
    /// * `request_id` - The ID of the request that was completed.
    ///
    /// # Return Value
    ///
    /// Returns false if the client request was already accepted, otherwise
    /// true.
    fn update_upon_request_completion(&mut self, request_id: RequestId) -> bool {
        if self.last_accepted_req >= Some(request_id) {
            warn!("Failed to update client state regarding the completion of client request (ID: {:?}): ID of last accepted request from the same client is greater than or equal to the receiving request's ID.", request_id);
            return false;
        }
        self.last_accepted_req = Some(request_id);
        if let Some(currently_processing) = &self.currently_processing_req {
            if currently_processing.0 <= request_id {
                self.currently_processing_req = None;
            }
        }
        true
    }
}

/// The purpose of the struct is to process and accept requests.
#[derive(Debug)]
pub(crate) struct RequestProcessor<P: RequestPayload, U: Usig> {
    /// Collects the ClientState of each ClientId.
    clients_state: HashMap<ClientId, ClientState<P>>,
    /// Collects the currently processing client requests.
    /// Additionally to the ID of the request and the client request itself,
    /// the time of arrival of the request is kept track of.
    currently_processing_reqs: VecDeque<(RequestId, ClientRequest<P>, Instant)>,
    /// Used for batching requests.
    pub(crate) request_batcher: RequestBatcher<P>,
    /// Used for possibly generating a Checkpoint when sufficient requests have
    /// been accepted.
    pub(crate) checkpoint_generator: CheckpointGenerator<P, U>,

    round: u64,
}

impl<P: RequestPayload, U: Usig> RequestProcessor<P, U>
where
    U::Attestation: Clone,
    U::Signature: Clone + Serialize,
{
    /// Create a new RequestProcessor with the given
    /// batch timeout and the max size for a batch.
    pub(crate) fn new(
        batch_timeout_duration: Duration,
        batch_max_size: Option<NonZeroUsize>,
    ) -> Self {
        RequestProcessor {
            clients_state: HashMap::new(),
            currently_processing_reqs: VecDeque::new(),
            request_batcher: RequestBatcher::new(batch_timeout_duration, batch_max_size),
            checkpoint_generator: CheckpointGenerator::new(),
            round: 0,
        }
    }

    /// Processes a client request.
    ///
    /// May return a client timeout, a [Prepare], and/or a batch timeout.
    ///
    /// # Arguments
    ///
    /// * `client_request` - The client request to be processed.
    /// * `view_state` - The inner state of the replica.
    /// * `client_timeout_duration` - The duration of the client timeout.
    /// * `config` - The configuration of the replica.
    ///
    /// # Return Value
    ///
    /// A client timeout is returned when there was no client timeout running
    /// upon receiving the given client request.
    ///
    /// A [Prepare] is returned when the maximum batch size of client requests
    /// has been reached.
    ///
    /// A batch timeout is returned when there was no batch timeout running upon
    /// receiving the given client request and the maximum batch size of client
    /// requests has not (yet) been reached.
    ///
    /// A tuple is returned in the aforementioned order with the respective
    /// values.
    pub(crate) fn process_client_req(
        &mut self,
        client_request: ClientRequest<P>,
        view_state: &ViewState<P, U::Signature>,
        client_timeout_duration: Duration,
        config: &Config,
    ) -> (
        Option<TimeoutRequest>,
        Option<PrepareContent<P>>,
        Option<TimeoutRequest>,
    ) {
        trace!(
            "Processing client request (ID {:?}, client ID: {:?}) ...",
            client_request.id(),
            client_request.client
        );
        let request_id = client_request.id();

        trace!(
            "Updating state of client (ID: {:?}) ...",
            client_request.client
        );
        if !self
            .clients_state
            .entry(client_request.client)
            .or_default()
            .update_upon_request_receival(request_id, client_request.clone())
        {
            return (None, None, None);
        }

        self.currently_processing_reqs.push_back((
            request_id,
            client_request.clone(),
            Instant::now(),
        ));

        let start_client_timeout = Some(TimeoutRequest::new_start_client_req(
            client_request.client,
            client_timeout_duration,
        ));

        let mut prepare_content = None;
        let mut batch_timeout_request = None;

        match view_state {
            ViewState::InView(in_view) => {
                if config.me_primary(in_view.view) {
                    let request_batch;
                    let timeout_request;
                    (request_batch, timeout_request) =
                        self.request_batcher.batch(client_request.clone());
                    batch_timeout_request = Some(timeout_request);
                    prepare_content = None;
                    if let Some(batch) = request_batch {
                        let origin = config.me();
                        prepare_content = Some(PrepareContent {
                            view: in_view.view,
                            origin,
                            request_batch: batch,
                        });
                    }
                };
            }
            // Client messages are ignored when the replica is in the state of
            // changing Views.
            ViewState::ChangeInProgress(in_progress) => {
                trace!("Ignored possible (if replica is primary) creation of Prepare as replica is in the process of changing views (from: {:?}, to: {:?}).", in_progress.prev_view, in_progress.next_view);
            }
        }
        trace!(
            "Processed client request (ID: {:?}, client ID: {:?}).",
            client_request.id(),
            client_request.client
        );
        (start_client_timeout, prepare_content, batch_timeout_request)
    }

    /// Returns the currently processing client request with its RequestId
    /// of each Client being tracked.
    pub(super) fn currently_processing_all(
        &self,
    ) -> impl Iterator<Item = (RequestId, &ClientRequest<P>)> {
        self.clients_state
            .values()
            .filter_map(|req| req.currently_processing_req.as_ref())
            .map(|(id, req)| (*id, req))
    }

    /// Accepts the provided [Prepare].
    ///
    /// # Arguments
    ///
    /// * `config` - The configuration of the replica.
    /// * `prepare` - The accepted [Prepare].
    /// * `timeout_duration` - The current set timeout duration.
    /// * `output` - The output struct to be adjusted in case of, e.g., errors
    ///              or responses.
    ///
    /// # Return Value
    ///
    /// The CheckpointContent if a Checkpoint is generated (see
    /// [CheckpointGenerator]).
    pub(crate) fn accept_prepare(
        &mut self,
        config: &Config,
        prepare: Prepare<P, U::Signature>,
        timeout_duration: Duration,
        output: &mut NotReflectedOutput<P, U>,
    ) -> Option<CheckpointContent> {
        self.round += 1;

        debug!("Accepting batch of Prepares ...");
        for request in prepare.request_batch.clone() {
            self.accept_request(request, timeout_duration, output);
        }
        trace!("Accepted batch of Prepares.");
        self.checkpoint_generator
            .generate_checkpoint(&prepare, config)
    }

    /// Returns the round, i.e. the amount of accepted [Prepare]s.
    pub(crate) fn round(&self) -> u64 {
        self.round
    }

    /// Accepts the provided request and responds to the respective client.
    /// Sets a new timeout if there are still currently processing requests.
    /// The elapsed time after the initial arrival of the client request is
    /// considered when computing the timeout duration.
    ///
    /// # Arguments
    ///
    /// * `request` - The accepted client request.
    /// * `curr_full_timeout_duration` - The current set timeout duration.
    /// * `output` - The output struct to be adjusted in case of, e.g., errors
    ///              or responses.
    fn accept_request(
        &mut self,
        request: ClientRequest<P>,
        curr_full_timeout_duration: Duration,
        output: &mut NotReflectedOutput<P, U>,
    ) {
        trace!(
            "Accepting client request (ID: {:?}, client ID: {:?}) ...",
            request,
            request.client
        );
        // Update state of the client from which the request is.
        if !self
            .clients_state
            .entry(request.client)
            .or_default()
            .update_upon_request_completion(request.payload.id())
        {
            return;
        };

        // Send request to stop timeout that may be set for this now accepted client-request.
        let stop_client_request = TimeoutRequest::new_stop_client_req(request.client);
        output.timeout_request(stop_client_request);

        // Update data structure of currently processing requests.
        // Possibly create a new timeout request with the reset duration.
        let mut start_client_timeout = None;
        while let Some(oldest_req) = self.currently_processing_reqs.front() {
            // Check if the oldest request in the data structure was already accepted.
            // Remove it if so.
            let client_state = self.clients_state.get(&oldest_req.1.client);
            if client_state.is_none() {
                // It was already accepted.
                self.currently_processing_reqs.pop_front();
                continue;
            };
            let client_state = client_state.unwrap();
            match Some(oldest_req.0).cmp(&client_state.last_accepted_req) {
                Ordering::Greater => {
                    // The oldest request in the data structure has not yet been accepted.
                    // Send a request to start a timeout for it with the reset duration.
                    start_client_timeout = Some(TimeoutRequest::new_start_client_req(
                        oldest_req.1.client,
                        curr_full_timeout_duration,
                    ));
                    break;
                }
                _ => {
                    // It was already accepted.
                    self.currently_processing_reqs.pop_front();
                    continue;
                }
            }
        }
        trace!(
            "Accepted client request (ID: {:?}, client ID: {:?})",
            request.id(),
            request.client
        );
        output.response(request.client, request.payload);
        if let Some(start_client_request) = start_client_timeout {
            output.timeout_request(start_client_request);
        }
    }
}
