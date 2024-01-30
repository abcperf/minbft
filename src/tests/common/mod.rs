use shared_ids::AnyId;
use std::{num::NonZeroU64, time::Duration};

use shared_ids::ReplicaId;
use usig::{
    noop::{Signature, UsigNoOp},
    Usig,
};

use crate::{
    client_request::{self, RequestBatch},
    peer_message::usig_message::view_peer_message::commit::{Commit, CommitContent},
    Config, View,
};

use super::{DummyPayload, Prepare, PrepareContent};

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
pub(crate) fn add_attestations(mut usigs: Vec<&mut UsigNoOp>) {
    for i in 0..usigs.len() {
        for j in 0..usigs.len() {
            usigs[i].add_remote_party(ReplicaId::from_u64(j.try_into().unwrap()), ());
        }
    }
}
