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
use tracing::debug;
use usig::{Counter, Usig};

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
/// Contains the ID of the last accepted client request
/// and the currently processing client request with its ID.
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
    /// Assures the RequestId of the client request is not less or equal to
    /// the RequestId of the last accepted client request.
    fn update_upon_request_receival(
        &mut self,
        request_id: RequestId,
        client_req: ClientRequest<P>,
    ) -> bool {
        if Some(request_id) <= self.last_accepted_req {
            debug!("client request is too old");
            return false;
        }
        if let Some(processing) = &self.currently_processing_req {
            match request_id.cmp(&processing.0) {
                Ordering::Less => {
                    debug!("client request is too old");
                    false
                }

                Ordering::Equal => {
                    // It was seen before.
                    false
                }
                Ordering::Greater => {
                    self.currently_processing_req = Some((request_id, client_req));
                    true
                }
            }
        } else {
            self.currently_processing_req = Some((request_id, client_req));
            true
        }
    }

    /// Update [ClientState] when completing a client request.
    fn update_upon_request_completion(&mut self, request_id: RequestId) {
        assert!(self.last_accepted_req < Some(request_id));
        self.last_accepted_req = Some(request_id);
        if let Some(currently_processing) = &self.currently_processing_req {
            if currently_processing.0 <= request_id {
                self.currently_processing_req = None;
            }
        }
    }
}

/// The purpose of the struct is to process and accept requests.
#[derive(Debug)]
pub(crate) struct RequestProcessor<P: RequestPayload, U: Usig> {
    /// Collects the ClientState of each ClientId.
    clients_state: HashMap<ClientId, ClientState<P>>,
    /// Collects the currently processing client requests.
    /// Additionally to the ID of the request and the client request itself, the time of arrival of the request is kept track of.
    currently_processing_reqs: VecDeque<(RequestId, ClientRequest<P>, Instant)>,
    /// Used for batching requests.
    pub(crate) request_batcher: RequestBatcher<P>,
    /// Used for possibly generating a Checkpoint when sufficient requests have been accepted.
    pub(crate) checkpoint_generator: CheckpointGenerator<P, U>,
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
        }
    }

    /// Processes a client request.
    /// May return a client timeout, a [Prepare], and/or a batch timeout.
    /// A client timeout is returned when there was no client timeout running upon receiving the given client request.
    /// A [Prepare] is returned when the maximum batch size of client requests has been reached.
    /// A batch timeout is returned when there was no batch timeout running upon receiving the given client request
    /// and the maximum batch size of client requests has not (yet) been reached.
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
        let request_id = client_request.id();

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
                    (request_batch, timeout_request) = self.request_batcher.batch(client_request);
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
            // Client messages are ignored when the replica is in the state of changing Views.
            ViewState::ChangeInProgress(_) => {}
        }
        (start_client_timeout, prepare_content, batch_timeout_request)
    }

    /// Returns the currently processing client request with its RequestId
    /// of each Client being tracked.
    pub(super) fn currently_processing_all(
        &self,
    ) -> impl Iterator<Item = (RequestId, &ClientRequest<P>)> {
        self.clients_state
            .values()
            .into_iter()
            .filter_map(|req| req.currently_processing_req.as_ref())
            .map(|(id, req)| (*id, req))
    }

    /// Accepts the provided [Prepare].
    pub(crate) fn accept_prepare(
        &mut self,
        config: &Config,
        prepare: Prepare<P, U::Signature>,
        timeout_duration: Duration,
        output: &mut NotReflectedOutput<P, U>,
    ) -> Option<CheckpointContent> {
        debug!(
            "accepting batch of prepare (view: {:?}, counter: {:?}) ...",
            prepare.view,
            prepare.counter()
        );
        for request in prepare.request_batch.clone() {
            debug!("accepting request {:?}", request.clone());
            self.accept_request(request, timeout_duration, output);
            debug!("accepted request");
        }
        debug!("accepted batch");
        self.checkpoint_generator
            .generate_checkpoint(&prepare, config)
    }

    /// Accepts the provided request and responds to the respective client.
    /// Sets a new timeout if there are still currently processing requests.
    /// The elapsed time after the initial arrival of the client request is considered when computing the timeout duration.
    fn accept_request(
        &mut self,
        request: ClientRequest<P>,
        curr_full_timeout_duration: Duration,
        output: &mut NotReflectedOutput<P, U>,
    ) {
        // Update state of the client from which the request is.
        self.clients_state
            .entry(request.client)
            .or_default()
            .update_upon_request_completion(request.payload.id());

        // Send request to stop timeout that may be set for this now accepted client-request.
        let stop_client_request = TimeoutRequest::new_stop_client_req(request.client);
        output.timeout_request(stop_client_request);

        // Update data structure of currently processing requests.
        // Possibly create a new timeout request with the adjusted duration.
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
                    // Send a request to start a timeout for it with the adjusted duration.
                    assert!(request.client != oldest_req.1.client);
                    let duration = curr_full_timeout_duration
                        .checked_sub(oldest_req.2.elapsed())
                        .unwrap_or_else(|| Duration::from_secs(0));
                    start_client_timeout = Some(TimeoutRequest::new_start_client_req(
                        oldest_req.1.client,
                        duration,
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
        output.response(request.client, request.payload);
        if let Some(start_client_request) = start_client_timeout {
            output.timeout_request(start_client_request);
        }
    }
}
