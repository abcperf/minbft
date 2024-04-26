//! Defines a message of type [UsigMessage].
//! Such messages are either of inner type [ViewPeerMessage], [ViewChangeV],
//! [NewView] or [Checkpoint].
//! Moreover, messages of type [UsigMessage] are signed by a USIG.

pub(crate) mod checkpoint;
pub(crate) mod new_view;
mod signed;
pub(crate) mod view_change;
pub(crate) mod view_peer_message;

use anyhow::Result;
use serde::{Deserialize, Serialize};
use shared_ids::ReplicaId;
use std::fmt::Debug;
use usig::{Counter, Usig};

use crate::{error::InnerError, Config, RequestPayload};

use self::{
    checkpoint::Checkpoint,
    new_view::NewView,
    view_change::{ViewChangeV, ViewChangeVariant, ViewChangeVariantLog, ViewChangeVariantNoLog},
    view_peer_message::ViewPeerMessage,
};

/// A [UsigMessageV] is a message that contains a USIG signature,
/// and is either a [UsigMessageV::View], [UsigMessageV::ViewChange],
/// [UsigMessageV::NewView] or a [UsigMessageV::Checkpoint].
///
/// A [UsigMessageV::View] should be created when the USIG signed message is
/// internally a [ViewPeerMessage].
/// A [UsigMessageV::ViewChange] should be created when the USIG signed message
/// is internally a [ViewChangeV] message.
/// A [UsigMessageV::NewView] should be created when the USIG signed message is
/// internally a [NewView] message.
/// A [UsigMessageV::Checkpoint] should be created when the USIG signed message
/// is internally a [Checkpoint] message.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub(crate) enum UsigMessageV<V: ViewChangeVariant<P, Sig>, P, Sig> {
    /// A [UsigMessageV] of type [ViewPeerMessage].
    View(ViewPeerMessage<P, Sig>),
    /// A [UsigMessageV] of type [ViewChangeV].
    ViewChange(ViewChangeV<V, P, Sig>),
    /// A [UsigMessageV] of type [NewView].
    NewView(NewView<P, Sig>),
    /// A [UsigMessageV] of type [Checkpoint].
    Checkpoint(Checkpoint<Sig>),
}

impl<V: ViewChangeVariant<P, Sig>, T: Into<ViewPeerMessage<P, Sig>>, P, Sig> From<T>
    for UsigMessageV<V, P, Sig>
{
    /// Create a [UsigMessageV] based on a [ViewPeerMessage].
    fn from(view_peer_message: T) -> Self {
        Self::View(view_peer_message.into())
    }
}
impl<V: ViewChangeVariant<P, Sig>, P, Sig> From<ViewChangeV<V, P, Sig>>
    for UsigMessageV<V, P, Sig>
{
    /// Create a [UsigMessageV] based on a [ViewChangeV].
    fn from(view_change: ViewChangeV<V, P, Sig>) -> Self {
        Self::ViewChange(view_change)
    }
}
impl<V: ViewChangeVariant<P, Sig>, P, Sig> From<NewView<P, Sig>> for UsigMessageV<V, P, Sig> {
    /// Create a [UsigMessageV] based on a [NewView].
    fn from(new_view: NewView<P, Sig>) -> Self {
        Self::NewView(new_view)
    }
}

/// A [UsigMessage] is a [UsigMessageV] that contains a log of messages if its
/// inner type is [ViewChangeV], i.e. a [UsigMessageV] with
/// [ViewChangeVariantLog].
pub(crate) type UsigMessage<P, Sig> = UsigMessageV<ViewChangeVariantLog<P, Sig>, P, Sig>;

impl<P: Clone + Serialize, Sig: Clone + Serialize> UsigMessage<P, Sig> {
    /// Convert a [UsigMessage] to the variant with no log
    /// (only relevant for when the internal message is of type [ViewChangeV]).
    fn to_no_log(&self) -> UsigMessageV<ViewChangeVariantNoLog, P, Sig> {
        match self {
            UsigMessageV::View(v) => v.clone().into(),
            UsigMessageV::ViewChange(v) => v.to_no_log().into(),
            UsigMessageV::NewView(n) => n.clone().into(),
            UsigMessageV::Checkpoint(c) => c.clone().into(),
        }
    }
}

impl<V: ViewChangeVariant<P, Sig>, P, Sig> From<Checkpoint<Sig>> for UsigMessageV<V, P, Sig> {
    /// Create a [UsigMessage] based on a [Checkpoint].
    fn from(checkpoint: Checkpoint<Sig>) -> Self {
        Self::Checkpoint(checkpoint)
    }
}

impl<V: ViewChangeVariant<P, Sig>, P, Sig: Counter> Counter for UsigMessageV<V, P, Sig> {
    /// Returns the counter of the [UsigMessage] by returning the counter of its
    /// inner type.
    ///
    /// # Return Value
    ///
    /// The counter of the [UsigMessage].
    fn counter(&self) -> usig::Count {
        match self {
            Self::View(view) => view.counter(),
            Self::ViewChange(view_change) => view_change.counter(),
            Self::NewView(new_view) => new_view.counter(),
            Self::Checkpoint(checkpoint) => checkpoint.counter(),
        }
    }
}

impl<V: ViewChangeVariant<P, Sig>, P, Sig> AsRef<ReplicaId> for UsigMessageV<V, P, Sig> {
    /// Referencing [UsigMessageV] returns a reference to the origin of the
    /// [UsigMessageV] (either of the [ViewPeerMessage], [ViewChangeV],
    /// [NewView] or [Checkpoint]).
    fn as_ref(&self) -> &ReplicaId {
        match self {
            Self::View(view_peer_message) => view_peer_message.as_ref(),
            Self::ViewChange(view_change) => view_change.as_ref(),
            Self::NewView(new_view) => new_view.as_ref(),
            Self::Checkpoint(checkpoint) => checkpoint.as_ref(),
        }
    }
}

impl<P: RequestPayload, Sig: Serialize + Counter + Debug> UsigMessage<P, Sig> {
    /// Validates the [UsigMessage] by validating its inner type.
    ///
    /// # Arguments
    ///
    /// * `config` - The [Config] of the algorithm.
    /// * `usig` - The USIG signature that should be a valid one for this
    ///            [UsigMessage] message.
    ///
    /// # Return Value
    ///
    /// [Ok] if the validation succeeds, otherwise [InnerError].
    pub(crate) fn validate(
        &self,
        config: &Config,
        usig: &mut impl Usig<Signature = Sig>,
    ) -> Result<(), InnerError> {
        match self {
            Self::View(view_peer_message) => view_peer_message.validate(config, usig),
            Self::ViewChange(view_change) => view_change.validate(config, usig),
            Self::NewView(new_view) => new_view.validate(config, usig),
            Self::Checkpoint(checkpoint) => checkpoint.validate(config, usig),
        }
    }
}
impl<V: ViewChangeVariant<P, Sig>, P, Sig> UsigMessageV<V, P, Sig> {
    /// Returns the type of the [UsigMessage] as a String slice.
    pub(crate) fn msg_type(&self) -> &'static str {
        match self {
            Self::NewView(_) => "NewView",
            Self::ViewChange(_) => "ViewChange",
            Self::View(m) => m.msg_type(),
            Self::Checkpoint(_) => "Checkpoint",
        }
    }
}

#[cfg(test)]

mod test {
    use rstest::rstest;

    use std::num::NonZeroU64;

    use usig::noop::UsigNoOp;

    use rand::thread_rng;
    use usig::Usig;

    use crate::{
        client_request::test::create_batch,
        peer_message::usig_message::{
            view_peer_message::{
                commit::test::create_commit, prepare::test::create_prepare, ViewPeerMessage,
            },
            UsigMessage,
        },
        tests::{
            add_attestations, create_config_default, get_random_included_replica_id,
            get_random_replica_id,
        },
        View,
    };

    /// Test if the validation of a valid UsigMessage succeeds.
    ///
    /// # Arguments
    ///
    /// * `n` - The number of replicas.
    #[rstest]
    fn validate_valid_usig_message(#[values(3, 4, 5, 6, 7, 8, 9, 10)] n: u64) {
        let n_parsed = NonZeroU64::new(n).unwrap();
        let mut rng = thread_rng();

        for t in 0..n / 2 {
            // ViewPeerMessage.
            let primary_id = get_random_replica_id(n_parsed, &mut rng);
            let view = View(primary_id.as_u64());
            let mut usig_primary = UsigNoOp::default();
            let config_primary = create_config_default(n_parsed, t, primary_id);
            let request_batch = create_batch();
            let prep = create_prepare(view, request_batch, &config_primary, &mut usig_primary);
            let view_peer_msg = ViewPeerMessage::from(prep.clone());
            let usig_message = UsigMessage::from(view_peer_msg.clone());

            // Add attestation of oneself.
            usig_primary.add_remote_party(primary_id, ());

            // Create a default config.
            let config_primary = create_config_default(n_parsed, t, primary_id);

            // Validate UsigMessage using the previously created config and USIG.
            let res_usig_msg_validation = usig_message.validate(&config_primary, &mut usig_primary);

            assert!(res_usig_msg_validation.is_ok());

            let backup_id = get_random_included_replica_id(n_parsed, primary_id, &mut rng);
            let mut usig_backup = UsigNoOp::default();
            let commit = create_commit(backup_id, prep, &mut usig_backup);
            let view_peer_msg = ViewPeerMessage::from(commit.clone());
            let usig_message = UsigMessage::from(view_peer_msg);

            // Add attestations.
            let mut usigs = vec![
                (primary_id, &mut usig_primary),
                (backup_id, &mut usig_backup),
            ];
            add_attestations(&mut usigs);

            // Create config of backup.
            let config = create_config_default(n_parsed, t, backup_id);

            let res_usig_msg_validation = usig_message.validate(&config, &mut usig_primary);

            assert!(res_usig_msg_validation.is_ok());
        }
    }
}
