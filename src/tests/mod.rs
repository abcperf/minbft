use anyhow::{anyhow, Result};
use rand::{distributions::Uniform, prelude::SliceRandom, rngs::ThreadRng, Rng};
use serde::{Deserialize, Serialize};
use shared_ids::{AnyId, ClientId, RequestId};
use std::{
    collections::HashMap,
    num::NonZeroU64,
    time::{Duration, Instant},
};

use shared_ids::ReplicaId;
use usig::{noop::UsigNoOp, Usig};

use crate::{
    client_request::test::create_batch, output::TimeoutRequest,
    peer_message::{req_view_change::ReqViewChange, usig_message::checkpoint::CheckpointHash}, timeout::StopClass, Config, MinBft,
    Output, RequestPayload, ValidatedPeerMessage, View,
};

use super::{Prepare, PrepareContent, TimeoutType};

mod multi;
mod normal;
mod viewchange;

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

/// Creates a minimal setup for a MinBft with the given configuration parameters.
/// n is the amount of Replicas.
/// t is the amount of faulty Replicas.
/// id is the Replica id.
/// checkpoint_period is the period for a checkpoint generation.
fn minimal_setup(n: u64, t: u64, id: ReplicaId, checkpoint_period: u64) -> MinBftSetup {
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

/// Creates a minimal setup for a MinBft with the given configuration parameters
/// and with max_batch_size set to None.
/// n is the amount of Replicas.
/// t is the amount of faulty Replicas.
/// id is the Replica id.
fn minimal_setup_batching(n: u64, t: u64, id: ReplicaId, checkpoint_period: u64) -> MinBftSetup {
    let checkpoint_period = NonZeroU64::new(checkpoint_period).unwrap();
    (
        MinBft::new(
            UsigNoOp::default(),
            Config {
                n: n.try_into().unwrap(),
                t,
                id,
                max_batch_size: None,
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
struct TimeoutHandler(HashMap<TimeoutType, (TimeoutEntry, bool)>);

impl TimeoutHandler {
    /// Handles a timeout request.
    /// Sets a timeout if the timeout request itself is a start request and
    /// if there is not already a timeout of the same type set.
    /// Stops a set timeout if the timeout request it self is a stop request and
    /// if the type and the stop class of the timeout in the request is the same as the set timeout.
    fn handle_timeout_request(&mut self, timeout_request: TimeoutRequest) {
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
    fn handle_timeout_requests(&mut self, timeout_requests: Vec<TimeoutRequest>) {
        for timeout_request in timeout_requests {
            self.handle_timeout_request(timeout_request);
        }
    }

    /// Retrieves set timeouts in the order of their deadline (from most to least urgent).
    fn retrieve_timeouts_ordered(&mut self) -> Vec<TimeoutType> {
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

/// Test if a Replica receives a hello message from itself.
#[test]
fn hello() {
    let id = ReplicaId::from_u64(0);
    let ((mut minbft, output), _) = minimal_setup(1, 0, id, 2);

    assert_eq!(output.responses.len(), 0);
    assert_eq!(output.errors.len(), 0);
    assert_eq!(output.timeout_requests.len(), 0);
    assert_eq!(output.broadcasts.len(), 1);
    let broadcasts = Vec::from(output.broadcasts); // remove once https://github.com/rust-lang/rust/issues/59878 is fixed
    let mut iter = broadcasts.into_iter();
    let message = iter
        .next()
        .unwrap()
        .validate(id, &minbft.config, &mut minbft.usig)
        .unwrap();
    assert!(matches!(message, ValidatedPeerMessage::Hello(_)));
}

/// Returns a random [ReplicaId].
///
/// # Arguments
///
/// * `n` - The amount of peers that communicate with each other.
pub(crate) fn get_random_replica_id(n: NonZeroU64, rng: &mut ThreadRng) -> ReplicaId {
    let id: u64 = rng.gen_range(0..n.into());
    ReplicaId::from_u64(id)
}

/// Returns a random [View] smaller than the one provided.
///
/// # Arguments
///
/// * `max_view` - The [View] that should be bigger than the one returned.
pub(crate) fn get_random_view_with_max(max_view: View) -> View {
    let mut rng = rand::thread_rng();
    let view_nr: u64 = rng.gen_range(0..max_view.0);
    View(view_nr)
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

pub(crate) fn create_default_configs_for_replicas(
    n: NonZeroU64,
    t: u64,
) -> HashMap<ReplicaId, Config> {
    let mut configs = HashMap::new();
    for i in 0..n.get() {
        let rep_id = ReplicaId::from_u64(i);
        let config = create_config_default(n, t, rep_id);
        configs.insert(rep_id, config);
    }
    configs
}

pub(crate) fn create_attested_usigs_for_replicas(
    n: NonZeroU64,
    not_to_attest_with_rest: Vec<ReplicaId>,
) -> HashMap<ReplicaId, UsigNoOp> {
    let mut usigs = HashMap::new();
    for i in 0..n.get() {
        let rep_id = ReplicaId::from_u64(i);
        let usig = UsigNoOp::default();
        usigs.insert(rep_id, usig);
    }

    let mut usigs_tuple = Vec::new();
    for (rep_id, usig) in usigs.iter_mut() {
        if not_to_attest_with_rest.contains(rep_id) {
            continue;
        }
        usigs_tuple.push((*rep_id, usig));
    }

    add_attestations(&mut usigs_tuple);

    usigs
}

/// Adds each [UsigNoOp] to each [UsigNoOp] as a remote party.
///
/// # Arguments
///
/// * `usigs` - The [UsigNoOp]s that shall be added as a remote party to
///             each other.
pub(crate) fn add_attestations(usigs: &mut Vec<(ReplicaId, &mut UsigNoOp)>) {
    for i in 0..usigs.len() {
        for j in 0..usigs.len() {
            let peer_id = usigs[j].0;
            usigs[i].1.add_remote_party(peer_id, ());
        }
    }
}

/// Creates a random state hash for a [Checkpoint].
pub(crate) fn create_random_state_hash() -> CheckpointHash {
    let mut rng = rand::thread_rng();
    let range = Uniform::<u8>::new(0, 255);

    let vals: Vec<u8> = (0..64).map(|_| rng.sample(range)).collect();
    vals.try_into().unwrap()
}

pub(crate) fn get_two_different_indexes(max_range: usize, rng: &mut ThreadRng) -> (usize, usize) {
    assert!(max_range > 1);
    let index_1 = rng.gen_range(0..max_range);
    let mut index_2 = rng.gen_range(0..max_range);
    while index_2 == index_1 {
        index_2 = rng.gen_range(0..max_range);
    }
    (index_1, index_2)
}

pub(crate) fn get_shuffled_remaining_replicas(
    n: NonZeroU64,
    excluded_replica: Option<ReplicaId>,
    rng: &mut ThreadRng,
) -> Vec<ReplicaId> {
    let mut remaining_replica_ids = Vec::new();
    for i in 0..n.get() {
        let replica_id = ReplicaId::from_u64(i);
        if excluded_replica.is_none() || replica_id != excluded_replica.unwrap() {
            remaining_replica_ids.push(replica_id);
        }
    }
    remaining_replica_ids.shuffle(rng);
    remaining_replica_ids
}

/// Returns a random valid backup [ReplicaId].
///
/// # Arguments
///
/// * `n` - The amount of peers that communicate with each other.
/// * `primary_id` - The [ReplicaId] of the current primary. The generated
///                  backup [ReplicaId] should differ from it.
pub(crate) fn get_random_included_replica_id(
    n: NonZeroU64,
    excluded_rep_id: ReplicaId,
    rng: &mut ThreadRng,
) -> ReplicaId {
    let remaining_replica_ids = get_shuffled_remaining_replicas(n, Some(excluded_rep_id), rng);
    let random_index = rng.gen_range(0..remaining_replica_ids.len() as u64) as usize;
    remaining_replica_ids[random_index]
}

pub(crate) fn get_random_included_index(
    excluded_max_index: usize,
    excluded_index: Option<usize>,
    rng: &mut ThreadRng,
) -> usize {
    let mut random_index = rng.gen_range(0..excluded_max_index);
    if let Some(excluded_index) = excluded_index {
        while random_index == excluded_index {
            random_index = rng.gen_range(0..excluded_max_index);
        }
    }
    random_index
}

pub(crate) fn increase_usig_of_replica(usig: &mut UsigNoOp) {
    let _ = Prepare::sign(
        PrepareContent {
            origin: ReplicaId::from_u64(0),
            view: View(0),
            request_batch: create_batch(),
        },
        usig,
    );
}

pub(crate) fn create_random_valid_req_vc_next_dir_subsequent(
    n: NonZeroU64,
    rng: &mut ThreadRng,
) -> ReqViewChange {
    let rand_factor_0 = get_random_included_index(n.get() as usize * 10, None, rng);

    let prev_view_nr = rng.gen_range(0..=rand_factor_0 as u64 * n.get());
    let next_view_nr = prev_view_nr + 1;

    let prev_view = View(prev_view_nr);
    let next_view = View(next_view_nr);

    ReqViewChange {
        prev_view,
        next_view,
    }
}

pub(crate) fn create_random_valid_req_vc_next_jump(
    n: NonZeroU64,
    rng: &mut ThreadRng,
) -> ReqViewChange {
    let rand_factor_0 = get_random_included_index(n.get() as usize * 10, None, rng);
    let rand_summand = rng.gen_range(1..=n.get() * 10);

    let prev_view_nr = rng.gen_range(0..=rand_factor_0 as u64 * n.get());
    let next_view_nr = prev_view_nr + rand_summand;

    let prev_view = View(prev_view_nr);
    let next_view = View(next_view_nr);

    ReqViewChange {
        prev_view,
        next_view,
    }
}
