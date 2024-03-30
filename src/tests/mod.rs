use anyhow::{anyhow, Result};
use rand::{distributions::Uniform, prelude::SliceRandom, thread_rng, Rng};
use serde::{Deserialize, Serialize};
use shared_ids::{AnyId, ClientId, RequestId};
use std::{
    collections::HashMap,
    marker::PhantomData,
    num::NonZeroU64,
    time::{Duration, Instant},
};

use shared_ids::ReplicaId;
use usig::{
    noop::{Signature, UsigNoOp},
    Count, Usig,
};

use crate::{
    client_request::{self, RequestBatch},
    output::TimeoutRequest,
    peer_message::usig_message::{
        checkpoint::{Checkpoint, CheckpointCertificate, CheckpointContent, CheckpointHash},
        view_change::{
            ViewChange, ViewChangeContent, ViewChangeVariantLog, ViewChangeVariantNoLog,
        },
        view_peer_message::{
            commit::{Commit, CommitContent},
            ViewPeerMessage,
        },
        UsigMessageV,
    },
    timeout::StopClass,
    Config, MinBft, Output, RequestPayload, ValidatedPeerMessage, View,
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
            request_batch: RequestBatch::new(
                Box::<[client_request::ClientRequest<DummyPayload>; 0]>::new([]),
            ),
        },
        &mut UsigNoOp::default(),
    )
    .unwrap()
}

/// Returns a [Prepare] with the provided [Usig].
///
/// # Arguments
///
/// * `origin` - The ID of the replica to which the [Prepare] belongs to.
///              It should be the ID of the primary.
/// * `view` - The current [View].
/// * `usig` - The [Usig] to be used for signing the [Prepare].
pub(crate) fn create_prepare_with_usig(
    origin: ReplicaId,
    view: View,
    usig: &mut impl Usig<Signature = Signature>,
) -> Prepare<DummyPayload, Signature> {
    Prepare::sign(
        PrepareContent {
            origin,
            view,
            request_batch: RequestBatch::new(
                Box::<[client_request::ClientRequest<DummyPayload>; 0]>::new([]),
            ),
        },
        usig,
    )
    .unwrap()
}

/// Returns a valid [Prepare] with a random origin and with the provided USIG.
///
/// # Arguments
///
/// * `n` - The amount of peers that communicate with each other.
///         It implicitly defines the range from which a random [ReplicaId]
///         can be chosen (0 .. n - 1).
/// * `usig` - The [Usig] to be used for signing the [Prepare].
pub(crate) fn create_random_valid_prepare_with_usig(
    n: NonZeroU64,
    usig: &mut impl Usig<Signature = Signature>,
) -> Prepare<DummyPayload, Signature> {
    let mut rng = rand::thread_rng();
    let id_prim: u64 = rng.gen_range(0..n.into());

    // Create Prepare.
    let id_primary = ReplicaId::from_u64(id_prim % n);
    let view = View(id_prim);

    create_prepare_with_usig(id_primary, view, usig)
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

/// Returns a valid [Commit] with a random origin and with the provided USIG.
///
/// # Arguments
///
/// * `n` - The amount of peers that communicate with each other.
///         It implicitly defines the range from which a random [ReplicaId]
///         can be chosen (0 .. n - 1).
/// * `prepare` - The [Prepare] to which this [Commit] belongs to.
/// * `usig` - The [Usig] to be used for signing the [Commit].
pub(crate) fn create_random_valid_commit_with_usig(
    n: NonZeroU64,
    prepare: Prepare<DummyPayload, Signature>,
    usig: &mut impl Usig<Signature = Signature>,
) -> Commit<DummyPayload, Signature> {
    let id_backup = get_random_backup_replica_id(n, prepare.origin);
    create_commit_with_usig(id_backup, prepare, usig)
}

/// Returns a random [ReplicaId].
///
/// # Arguments
///
/// * `n` - The amount of peers that communicate with each other.
pub(crate) fn get_random_replica_id(n: NonZeroU64) -> ReplicaId {
    let mut rng = rand::thread_rng();
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

/// Adds each [UsigNoOp] to each [UsigNoOp] as a remote party.
///
/// # Arguments
///
/// * `usigs` - The [UsigNoOp]s that shall be added as a remote party to
///             each other.
pub(crate) fn add_attestations(mut usigs: Vec<(ReplicaId, &mut UsigNoOp)>) {
    for i in 0..usigs.len() {
        for j in 0..usigs.len() {
            let peer_id = usigs[j].0;
            usigs[i].1.add_remote_party(peer_id, ());
        }
    }
}

/// Creates a default [Checkpoint] with the provided [ReplicaId] as origin.
///
/// # Arguments
///
/// * `origin` - The ID of the replica to which the [Checkpoint] belongs to.
pub(crate) fn create_default_checkpoint(origin: ReplicaId) -> Checkpoint<Signature> {
    Checkpoint::sign(
        CheckpointContent {
            origin,
            state_hash: [0; 64],
            counter_latest_prep: Count(0),
            total_amount_accepted_batches: 0,
        },
        &mut UsigNoOp::default(),
    )
    .unwrap()
}

/// Creates a random state hash for a [Checkpoint].
pub(crate) fn create_random_state_hash() -> CheckpointHash {
    let mut rng = rand::thread_rng();
    let range = Uniform::<u8>::new(0, 255);

    let vals: Vec<u8> = (0..64).map(|_| rng.sample(range)).collect();
    vals.try_into().unwrap()
}

/// Returns a [ViewChange] with a default [Usig], no cert, and with an empty
/// message log.
///
/// # Arguments
///
/// * `origin` - The ID of the replica to which the [ViewChange] belongs to.
/// * `next_view` - The [View] to change to.
pub(crate) fn create_view_change_no_cert_empty_log_default_usig(
    origin: ReplicaId,
    next_view: View,
) -> ViewChange<DummyPayload, Signature> {
    let variant = ViewChangeVariantLog {
        message_log: vec![],
    };

    ViewChange::sign(
        ViewChangeContent {
            origin,
            next_view,
            checkpoint: None,
            variant,
            phantom_data: PhantomData,
        },
        &mut UsigNoOp::default(),
    )
    .unwrap()
}

/// Returns a [ViewChange] with the provided [Usig], no cert, and with an empty
/// message log.
///
/// # Arguments
///
/// * `origin` - The ID of the replica to which the [ViewChange] belongs to.
/// * `next_view` - The [View] to change to.
/// * `usig` - The [Usig] to be used for signing the [ViewChange].
pub(crate) fn create_view_change_no_cert_empty_log_with_usig(
    origin: ReplicaId,
    next_view: View,
    usig: &mut impl Usig<Signature = Signature>,
) -> ViewChange<DummyPayload, Signature> {
    let variant = ViewChangeVariantLog {
        message_log: vec![],
    };

    ViewChange::sign(
        ViewChangeContent {
            origin,
            next_view,
            checkpoint: None,
            variant,
            phantom_data: PhantomData,
        },
        usig,
    )
    .unwrap()
}

/// Returns a [ViewChange] with the provided [Usig], checkpoint certificate, and
/// with an empty message log.
///
/// # Arguments
///
/// * `origin` - The ID of the replica to which the [ViewChange] belongs to.
/// * `next_view` - The [View] to change to.
/// * `checkpoint_cert` - The checkpoint certificate to set for the [ViewChange].
/// * `message_log` - The message log to set for the [ViewChange].
/// * `usig` - The [Usig] to be used for signing the [ViewChange].
pub(crate) fn create_view_change_with_cert_log_and_usig(
    origin: ReplicaId,
    next_view: View,
    checkpoint_cert: Option<CheckpointCertificate<Signature>>,
    message_log: Vec<UsigMessageV<ViewChangeVariantNoLog, DummyPayload, Signature>>,
    usig: &mut impl Usig<Signature = Signature>,
) -> ViewChange<DummyPayload, Signature> {
    ViewChange::sign(
        ViewChangeContent {
            origin,
            next_view,
            checkpoint: checkpoint_cert,
            variant: ViewChangeVariantLog { message_log },
            phantom_data: PhantomData,
        },
        usig,
    )
    .unwrap()
}

pub(crate) fn create_checkpoint_cert_valid_n_3_t_1(
    rep_0: ReplicaId,
    usig_0: &mut impl Usig<Signature = Signature>,
    rep_1: ReplicaId,
    usig_1: &mut impl Usig<Signature = Signature>,
) -> CheckpointCertificate<Signature> {
    let mut rng = rand::thread_rng();
    let rand_counter_last_prep: u64 = rng.gen();
    let rand_total_accepted_batches: u64 = rng.gen();

    let state_hash = create_random_state_hash();

    let my_checkpoint = Checkpoint::sign(
        CheckpointContent {
            origin: rep_0,
            state_hash,
            counter_latest_prep: Count(rand_counter_last_prep),
            total_amount_accepted_batches: rand_total_accepted_batches,
        },
        usig_0,
    )
    .unwrap();

    let other_checkpoint = Checkpoint::sign(
        CheckpointContent {
            origin: rep_1,
            state_hash,
            counter_latest_prep: Count(rand_counter_last_prep),
            total_amount_accepted_batches: rand_total_accepted_batches,
        },
        usig_1,
    )
    .unwrap();

    let other_checkpoints = vec![other_checkpoint];

    CheckpointCertificate {
        my_checkpoint,
        other_checkpoints,
    }
}

pub(crate) fn create_message_log_valid(
    origin: ReplicaId,
    prev_view: View,
    usig: &mut impl Usig<Signature = Signature>,
) -> Vec<UsigMessageV<ViewChangeVariantNoLog, DummyPayload, Signature>> {
    let mut message_log = Vec::new();

    let prep = create_prepare_with_usig(origin, prev_view, usig);

    message_log.push(UsigMessageV::View(ViewPeerMessage::Prepare(prep)));

    message_log
}

pub(crate) fn get_shuffled_backup_replicas(n: NonZeroU64, primary_id: ReplicaId) -> Vec<ReplicaId> {
    let mut backup_replica_ids = Vec::new();
    for i in 0..n.get() {
        let replica_id = ReplicaId::from_u64(i);
        if replica_id != primary_id {
            backup_replica_ids.push(replica_id);
        }
    }
    backup_replica_ids.shuffle(&mut thread_rng());
    backup_replica_ids
}

/// Returns a random valid backup [ReplicaId].
///
/// # Arguments
///
/// * `n` - The amount of peers that communicate with each other.
/// * `primary_id` - The [ReplicaId] of the current primary. The generated
///                  backup [ReplicaId] should differ from it.
pub(crate) fn get_random_backup_replica_id(n: NonZeroU64, primary_id: ReplicaId) -> ReplicaId {
    let backup_replica_ids = get_shuffled_backup_replicas(n, primary_id);
    let mut rng = thread_rng();
    let random_index = rng.gen_range(0..backup_replica_ids.len() as u64) as usize;
    backup_replica_ids[random_index]
}
