use std::{
    collections::HashMap,
    num::NonZeroU64,
    time::{Duration, Instant},
};

use minbft::{
    output::TimeoutRequest,
    timeout::{StopClass, TimeoutType},
    Config, Error, MinBft, Output, PeerMessage, RequestPayload,
};
use rand::rngs::ThreadRng;
use serde::{Deserialize, Serialize};
use shared_ids::{ClientId, ReplicaId, RequestId};
use usig::{noop::UsigNoOp, Usig};

use rand::prelude::SliceRandom;

use anyhow::{anyhow, Result};

/// Defines a dummy payload for sending client-requests.
///
/// It only contains the ID of the request, and if it is a valid or invalid request.
#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
pub(super) struct DummyPayload(pub(super) u64, pub(super) bool);

impl RequestPayload for DummyPayload {
    /// Returns the ID of the Request.
    fn id(&self) -> RequestId {
        RequestId::from_u64(self.0)
    }

    /// Returns Ok(()) if it is a valid request, else Err.
    fn verify(&self, _id: ClientId) -> Result<()> {
        self.1
            .then_some(())
            .ok_or_else(|| anyhow!("invalid request"))
    }
}

type MinBftSetup = (
    (
        MinBft<DummyPayload, UsigNoOp>,
        Output<DummyPayload, UsigNoOp>,
    ),
    TimeoutHandler,
);

type SetupSet = (
    HashMap<ReplicaId, MinBft<DummyPayload, UsigNoOp>>,
    HashMap<ReplicaId, TimeoutHandler>,
);

/// Creates a minimal setup for a MinBft with the given configuration parameters.
/// n is the amount of Replicas.
/// t is the amount of faulty Replicas.
/// id is the Replica id.
/// checkpoint_period is the period for a checkpoint generation.
pub(crate) fn minimal_setup(n: u64, t: u64, id: ReplicaId, checkpoint_period: u64) -> MinBftSetup {
    let checkpoint_period = NonZeroU64::new(checkpoint_period).unwrap();
    (
        MinBft::new(
            UsigNoOp::default(),
            Config {
                n: n.try_into().unwrap(),
                t,
                id,
                max_batch_size: Some(1.try_into().expect("> 0")),
                batch_timeout: Duration::from_secs(1),
                initial_timeout_duration: Duration::from_secs(1),
                checkpoint_period,
            },
        )
        .unwrap(),
        TimeoutHandler::default(),
    )
}

/// Contains the information needed to store timeouts in the [TimeoutHandler].
#[derive(Debug, Clone, Copy)]
struct TimeoutEntry {
    /// The type of the timeout.
    timeout_type: TimeoutType,
    /// The deadline for when the timeout times out (and needs to be handled).
    timeout_deadline: Instant,
    /// The identifying class of a timeout for when a request for stopping it is received.
    stop_class: StopClass,
}

/// Handles timeout requests and timeouts.
/// See functions below for a better understanding.
#[derive(Debug, Clone, Default)]
pub(crate) struct TimeoutHandler(HashMap<TimeoutType, (TimeoutEntry, bool)>);

impl TimeoutHandler {
    /// Handles a timeout request.
    /// Sets a timeout if the timeout request itself is a start request and
    /// if there is not already a timeout of the same type set.
    /// Stops a set timeout if the timeout request it self is a stop request and
    /// if the type and the stop class of the timeout in the request is the same as the set timeout.
    pub(crate) fn handle_timeout_request(&mut self, timeout_request: TimeoutRequest) {
        if let TimeoutRequest::Start(timeout) = timeout_request {
            if self.0.contains_key(&timeout.timeout_type) {
                return;
            }
            let new_entry = TimeoutEntry {
                timeout_type: timeout.timeout_type,
                timeout_deadline: Instant::now() + timeout.duration,
                stop_class: timeout.stop_class,
            };
            self.0.insert(new_entry.timeout_type, (new_entry, false));
        }
        if let TimeoutRequest::Stop(timeout) = timeout_request {
            if !self.0.contains_key(&timeout.timeout_type) {
                return;
            }
            let (current_timeout, _) = self.0.get(&timeout.timeout_type).unwrap();
            if current_timeout.stop_class == timeout.stop_class {
                self.0.remove(&timeout.timeout_type);
            }
        }
    }

    /// Handles a collection of timeout requests.
    pub(crate) fn handle_timeout_requests(&mut self, timeout_requests: Vec<TimeoutRequest>) {
        for timeout_request in timeout_requests {
            self.handle_timeout_request(timeout_request);
        }
    }

    /// Retrieves set timeouts in the order of their deadline (from most to least urgent).
    pub(crate) fn retrieve_timeouts_ordered(&mut self) -> Vec<TimeoutType> {
        let mut timeouts: Vec<TimeoutEntry> = self
            .0
            .values()
            .filter(|(_, retrieved)| !retrieved)
            .map(|(e, _)| *e)
            .collect();

        timeouts.sort_by(|x, y| x.timeout_deadline.cmp(&y.timeout_deadline));
        let retrieved_timeouts = timeouts.iter().map(|e| e.timeout_type).collect();

        // Mark retrieved timeouts as already retrieved so that they are not retrieved once again in the future
        // (otherwise they could be handled more than once by the caller of this function).
        for retrieved_timeout in &retrieved_timeouts {
            let updated = (self.0.get(retrieved_timeout).unwrap().0, true);
            self.0.insert(*retrieved_timeout, updated);
        }

        retrieved_timeouts
    }
}

/// Setups n [MinBft]s configured with the given parameters.
/// Moreover, it returns the [TimeoutHandler]s of the [MinBft]s.
pub(crate) fn setup_set(n: u64, t: u64, checkpoint_period: u64) -> SetupSet {
    let mut minbfts = HashMap::new();
    let mut timeout_handlers = HashMap::new();

    let mut all_broadcasts = Vec::new();

    let mut hello_done_count = 0;

    for i in 0..n {
        let replica = ReplicaId::from_u64(i);
        let (
            (
                minbft,
                Output {
                    broadcasts,
                    responses,
                    timeout_requests,
                    errors,
                    ready_for_client_requests,
                    primary: _,
                    view_info: _,
                    round: _,
                },
            ),
            timeout_handler,
        ) = minimal_setup(n, t, replica, checkpoint_period);
        assert_eq!(responses.len(), 0);
        assert_eq!(errors.len(), 0);
        assert_eq!(timeout_requests.len(), 0);
        if ready_for_client_requests {
            hello_done_count += 1;
        }
        // hello should only be done when n = 1
        // otherwise, replicas still should have to attest themselves.
        assert!(!ready_for_client_requests || n == 1);
        all_broadcasts.push((replica, broadcasts));
        minbfts.insert(replica, minbft);
        timeout_handlers.insert(replica, timeout_handler);
    }

    for (id, broadcasts) in all_broadcasts.into_iter() {
        for broadcast in Vec::from(broadcasts).into_iter() {
            // remove once https://github.com/rust-lang/rust/issues/59878 is fixed
            for (_, minbft) in minbfts.iter_mut().filter(|(i, _)| **i != id) {
                let Output {
                    broadcasts,
                    responses,
                    timeout_requests,
                    errors,
                    ready_for_client_requests,
                    primary: _,
                    view_info: _,
                    round: _,
                } = minbft.handle_peer_message(id, broadcast.clone());
                assert_eq!(broadcasts.len(), 0);
                assert_eq!(responses.len(), 0);
                assert_eq!(errors.len(), 0);
                assert_eq!(timeout_requests.len(), 0);
                if ready_for_client_requests {
                    hello_done_count += 1;
                }
            }
        }
    }

    assert_eq!(hello_done_count, n);

    (minbfts, timeout_handlers)
}

/// Defines a NotValidadedPeerMessage for testing (UsigNoOp as Usig).
type PeerMessageTest =
    PeerMessage<<UsigNoOp as Usig>::Attestation, DummyPayload, <UsigNoOp as Usig>::Signature>;

/// Contains the collected [Output] (responses, errors, timeout requests) of the [MinBft]s.
#[derive(Default)]
pub(crate) struct CollectedOutput {
    pub(crate) responses: HashMap<ReplicaId, Vec<(ClientId, DummyPayload)>>,
    pub(crate) errors: HashMap<ReplicaId, Vec<Error>>,
    pub(crate) timeout_requests: HashMap<ReplicaId, Vec<TimeoutRequest>>,
}

impl CollectedOutput {
    /// Returns the relevant timeouts to handle.
    pub(crate) fn timeouts_to_handle(
        &self,
        timeout_handlers: &mut HashMap<ReplicaId, TimeoutHandler>,
        rng: &mut ThreadRng,
    ) -> HashMap<ReplicaId, Vec<TimeoutType>> {
        let mut timeouts_to_handle = HashMap::new();

        let mut replica_ids: Vec<ReplicaId> = self.timeout_requests.keys().cloned().collect();
        replica_ids.shuffle(rng);

        for rep_id in &replica_ids {
            let timeout_requests = self.timeout_requests.get(rep_id).unwrap();
            let timeout_handler = timeout_handlers.get_mut(rep_id).unwrap();
            timeout_handler.handle_timeout_requests(timeout_requests.to_vec());
            timeouts_to_handle.insert(*rep_id, timeout_handler.retrieve_timeouts_ordered());
        }
        timeouts_to_handle
    }
}

/// Handle messages to be broadcast.
pub(crate) fn handle_broadcasts(
    minbfts: &mut HashMap<ReplicaId, MinBft<DummyPayload, UsigNoOp>>,
    broadcasts_with_origin: Vec<(ReplicaId, Box<[PeerMessageTest]>)>,
    collected_output: &mut CollectedOutput,
    rng: &mut ThreadRng,
) {
    // to collect possibly new added messages to broadcast
    // see below (1)
    let mut all_broadcasts = Vec::new();
    for (from, messages_to_broadcast) in broadcasts_with_origin {
        for message_to_broadcast in Vec::from(messages_to_broadcast).into_iter() {
            // remove once https://github.com/rust-lang/rust/issues/59878 is fixed

            // all other Replicas other than the origin handle the message
            let mut replica_ids: Vec<ReplicaId> =
                minbfts.keys().filter(|id| **id != from).cloned().collect();
            replica_ids.shuffle(rng);

            for rep_id in &replica_ids {
                let minbft = minbfts.get_mut(rep_id).unwrap();

                let Output {
                    broadcasts,
                    responses,
                    timeout_requests: timeouts,
                    errors,
                    ready_for_client_requests,
                    primary: _,
                    view_info: _,
                    round: _,
                } = minbft.handle_peer_message(from, message_to_broadcast.clone());
                assert!(ready_for_client_requests);
                // collect the responses of the Replica
                collected_output
                    .responses
                    .entry(*rep_id)
                    .or_default()
                    .append(&mut Vec::from(responses));
                // collect the errors of the Replica
                collected_output
                    .errors
                    .entry(*rep_id)
                    .or_default()
                    .append(&mut Vec::from(errors));
                // collect the timeouts of the Replica
                collected_output
                    .timeout_requests
                    .entry(*rep_id)
                    .or_default()
                    .append(&mut Vec::from(timeouts));
                // (1) if the handling of the peer message triggered the creation of new messages that need to be broadcasted,
                // push these new messages to the Vec of all broadcasts to be sent
                if !broadcasts.is_empty() {
                    all_broadcasts.push((*rep_id, broadcasts));
                }
            }
        }
    }
    // handle the possibly new added messages to broadcast
    // see above (1)
    if !all_broadcasts.is_empty() {
        handle_broadcasts(minbfts, all_broadcasts, collected_output, rng);
    }
}

/// Try to send a client request.
pub(crate) fn try_client_request(
    minbfts: &mut HashMap<ReplicaId, MinBft<DummyPayload, UsigNoOp>>,
    client_id: ClientId,
    payload: DummyPayload,
    rng: &mut ThreadRng,
) -> CollectedOutput {
    // to collect the output of each Replica generated by handling the client request
    let mut collected_output = CollectedOutput::default();

    // to collect all messages to be broadcasted generated by handling the client message
    let mut all_broadcasts = Vec::new();

    // each Replica handles the client message in a randomized order
    let mut replica_ids: Vec<ReplicaId> = minbfts.keys().cloned().collect();
    replica_ids.shuffle(rng);

    for rep_id in &replica_ids {
        let minbft = minbfts.get_mut(rep_id).unwrap();
        let Output {
            broadcasts,
            responses,
            timeout_requests: timeouts,
            errors,
            ready_for_client_requests,
            primary: _,
            view_info: _,
            round: _,
        } = minbft.handle_client_message(client_id, payload);
        assert!(ready_for_client_requests);
        // collect the responses of the Replica
        collected_output
            .responses
            .entry(*rep_id)
            .or_default()
            .append(&mut Vec::from(responses));
        // collect the errors of the Replica
        collected_output
            .errors
            .entry(*rep_id)
            .or_default()
            .append(&mut Vec::from(errors));
        // collect the timeouts of the Replica
        collected_output
            .timeout_requests
            .entry(*rep_id)
            .or_default()
            .append(&mut Vec::from(timeouts));
        // (1) If the handling of the client message triggered the creation of new messages that need to be broadcasted,
        // push these new messages to the Vec of all broadcasts to be sent.
        if !broadcasts.is_empty() {
            all_broadcasts.push((*rep_id, broadcasts));
        }
    }

    // handle the new messages to be broadcasted
    // see above (1)
    handle_broadcasts(minbfts, all_broadcasts, &mut collected_output, rng);

    collected_output
}

/// Forces the provided [MinBft]s to handle the given timeouts.
pub(crate) fn force_timeout(
    minbfts: &mut HashMap<ReplicaId, MinBft<DummyPayload, UsigNoOp>>,
    timeouts: &HashMap<ReplicaId, Vec<TimeoutType>>,
    rng: &mut ThreadRng,
) -> CollectedOutput {
    // to collect the output of each Replica generated by handling the client request
    let mut collected_output = CollectedOutput::default();
    // client message is received and handled
    let mut all_broadcasts = Vec::new();

    let mut replica_ids: Vec<ReplicaId> = minbfts.keys().cloned().collect();
    replica_ids.shuffle(rng);

    for rep_id in &replica_ids {
        let minbft = minbfts.get_mut(rep_id).unwrap();
        if let Some(timeouts_to_handle) = timeouts.get(rep_id) {
            for timeout_to_handle in timeouts_to_handle {
                let timeout_type = timeout_to_handle.to_owned();
                let Output {
                    broadcasts,
                    responses,
                    timeout_requests: timeouts,
                    errors,
                    ready_for_client_requests,
                    primary: _,
                    view_info: _,
                    round: _,
                } = minbft.handle_timeout(timeout_type);
                // collect the responses of the Replica
                assert!(ready_for_client_requests);
                collected_output
                    .responses
                    .entry(*rep_id)
                    .or_default()
                    .append(&mut Vec::from(responses));
                // collect the errors of the Replica
                collected_output
                    .errors
                    .entry(*rep_id)
                    .or_default()
                    .append(&mut Vec::from(errors));
                // collect the timeouts of the Replica
                collected_output
                    .timeout_requests
                    .entry(*rep_id)
                    .or_default()
                    .append(&mut Vec::from(timeouts));
                if !broadcasts.is_empty() {
                    all_broadcasts.push((*rep_id, broadcasts));
                }
            }
        }
    }

    handle_broadcasts(minbfts, all_broadcasts, &mut collected_output, rng);
    collected_output
}

pub(crate) fn remove_random_replicas_from_hashmap(
    minbfts: &mut HashMap<ReplicaId, MinBft<DummyPayload, UsigNoOp>>,
    amount_to_keep: usize,
    explicitly_to_keep: Option<ReplicaId>,
    rng: &mut ThreadRng,
) {
    assert!(minbfts.len() >= amount_to_keep);

    let mut replica_ids: Vec<ReplicaId> = minbfts.keys().cloned().collect();
    replica_ids.shuffle(rng);
    replica_ids.truncate(amount_to_keep);

    if let Some(explicitly_to_keep) = explicitly_to_keep {
        assert!(0 <= amount_to_keep.try_into().unwrap());
        if amount_to_keep != 0 && !replica_ids.contains(&explicitly_to_keep) {
            replica_ids.pop();
            replica_ids.push(explicitly_to_keep);
        }
    }

    minbfts.retain(|i, _| replica_ids.contains(i));
}
