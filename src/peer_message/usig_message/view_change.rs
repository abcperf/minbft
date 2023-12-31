//! Defines a message of type [ViewChange].
//! A [ViewChange] consists of two main parts.
//! The first part is its content, the [ViewChangeContent].
//! It contains the ID of the replica ([ReplicaId]) which created the [ViewChange].
//! Moreover, it contains the next [View] to which the replicas should change to.
//! Furthermore, it contains the most recent checkpoint of the replica.
//! Additionally, it may contain a log of messages or not, according to the variant of the [ViewChange].
//! The messages that are logged are the ones broadcast by the replica since the most recent checkpoint.
//! The second part is its signature, as [ViewChange]s must be signed by a USIG.
//! A [ViewChange] is broadcast by a replica when enough [crate::ReqViewChange]s have been collected.
//! For further explanation, see the documentation in [crate::MinBft] or the paper "Efficient Byzantine Fault-Tolerance" by Veronese et al.

use std::{fmt::Debug, marker::PhantomData};

use anyhow::Result;
use blake2::{digest::Update, Blake2b512, Digest};
use serde::{Deserialize, Serialize};
use shared_ids::AnyId;
use tracing::debug;
use usig::{Count, Counter, Usig};

use crate::{error::InnerError, Config, ReplicaId, RequestPayload, View};

use super::{
    checkpoint::CheckpointCertificate,
    signed::{UsigSignable, UsigSigned},
    UsigMessage, UsigMessageV,
};

/// The trait for the variants of a [ViewChange] message.
/// It can either be the variant with a message log or the variant with no log.
pub(crate) trait ViewChangeVariant<P, Sig> {
    type Hash<'s>: AsRef<[u8]>
    where
        Self: 's;

    fn hash(&self) -> Self::Hash<'_>
    where
        P: Serialize,
        Sig: Serialize;
}

/// A [ViewChange] message contains a message log.
/// However, the messages in the message log must not contain a message log itself.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub(crate) struct ViewChangeVariantLog<P, Sig> {
    /// The log containing the messages of type UsigMessage.
    pub(crate) message_log: Vec<UsigMessageV<ViewChangeVariantNoLog, P, Sig>>,
}

impl<P, Sig> ViewChangeVariant<P, Sig> for ViewChangeVariantLog<P, Sig> {
    // The hash of a ViewChange message that contains a message log is the hash of the message log.
    type Hash<'s> = [u8; 64] where Self: 's;

    // Hashes the message log of the message of type ViewChangeVariantLog.
    fn hash(&self) -> Self::Hash<'_>
    where
        P: Serialize,
        Sig: Serialize,
    {
        let mut hasher = Blake2b512::new();
        Digest::update(&mut hasher, bincode::serialize(&self.message_log).unwrap());
        hasher.finalize().into()
    }
}

/// The struct guarantees that message logs cannot contain a message log itself
/// (that is, it prevents the nesting of message logs).
#[derive(Serialize, Deserialize, Clone, Debug)]
pub(crate) struct ViewChangeVariantNoLog {
    #[serde(with = "serde_bytes")]
    hash: Box<[u8]>,
}

impl<P, Sig> ViewChangeVariant<P, Sig> for ViewChangeVariantNoLog {
    // The hash of a ViewChange message that does not contain a message log is the struct field hash.
    type Hash<'s> = &'s [u8];

    // Returns a reference of the hash.
    fn hash(&self) -> Self::Hash<'_> {
        &self.hash
    }
}

/// The struct defines the content of a [ViewChange] message.
/// For further explanation, see the documentation in the module.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub(crate) struct ViewChangeContent<V: ViewChangeVariant<P, Sig>, P, Sig> {
    /// The replica which the [ViewChange] originates from.
    pub(crate) origin: ReplicaId,
    /// The next [View] to which it should be changed to.
    pub(crate) next_view: View,
    /// The struct field checkpoint is optional since there can only exist a previous checkpoint certificate if one was created before.
    pub(crate) checkpoint: Option<CheckpointCertificate<Sig>>,
    /// The variant of the [ViewChange].
    pub(crate) variant: V,
    /// To mark P as owned.
    pub(crate) phantom_data: PhantomData<P>,
}

impl<P: Serialize + Clone, Sig: Serialize + Clone>
    ViewChangeContent<ViewChangeVariantLog<P, Sig>, P, Sig>
{
    /// Create a new ViewChangeContent.
    pub(crate) fn new<'a>(
        origin: ReplicaId,
        next_view: View,
        checkpoint: Option<CheckpointCertificate<Sig>>,
        message_log: impl Iterator<Item = &'a UsigMessage<P, Sig>>,
    ) -> Self
    where
        P: 'a,
        Sig: 'a,
    {
        let message_log = message_log.into_iter().map(|m| m.to_no_log()).collect();
        Self {
            origin,
            next_view,
            checkpoint,
            variant: ViewChangeVariantLog { message_log },
            phantom_data: PhantomData::default(),
        }
    }
}

impl<V: ViewChangeVariant<P, Sig>, P, Sig> AsRef<ReplicaId> for ViewChangeContent<V, P, Sig> {
    /// Referencing [ViewChangeContent] returns a reference to the origin in the [ViewChangeContent].
    fn as_ref(&self) -> &ReplicaId {
        &self.origin
    }
}

/// The types for defining a message of type ViewChange.
/// The type ViewChangeV can either be a ViewChange with a log or without it.
pub(crate) type ViewChangeV<V, P, Sig> = UsigSigned<ViewChangeContent<V, P, Sig>, Sig>;

/// The type [ViewChange] is always a [ViewChangeV<ViewChangeVariantLog>].
/// For further explanation see the documentation of the module.
pub(crate) type ViewChange<P, Sig> = ViewChangeV<ViewChangeVariantLog<P, Sig>, P, Sig>;

impl<P: Serialize, Sig: Clone + Serialize> ViewChange<P, Sig> {
    /// Convert a [ViewChange] to a [ViewChangeV] with the variant containing no log.
    pub(super) fn to_no_log(&self) -> ViewChangeV<ViewChangeVariantNoLog, P, Sig> {
        let ViewChangeContent {
            origin,
            next_view,
            checkpoint,
            variant,
            phantom_data,
        } = &self.data;

        self.clone_signature(ViewChangeContent {
            origin: *origin,
            next_view: *next_view,
            checkpoint: checkpoint.clone(),
            variant: ViewChangeVariantNoLog {
                hash: variant.hash().into(),
            },
            phantom_data: *phantom_data,
        })
    }
}

impl<P: Serialize, Sig: Serialize> UsigSignable
    for ViewChangeContent<ViewChangeVariantLog<P, Sig>, P, Sig>
{
    /// Hashes the content of a message of type [ViewChange].
    /// Required for signing and verifying a message of type [ViewChange].
    fn hash_content<H: Update>(&self, hasher: &mut H) {
        hasher.update(&self.origin.as_u64().to_be_bytes());
        hasher.update(&self.next_view.0.to_be_bytes());
        hasher.update(&bincode::serialize(&self.checkpoint).unwrap());
        hasher.update(&self.variant.hash());
    }
}

impl<P: RequestPayload, Sig: Counter + Serialize + Debug> ViewChange<P, Sig> {
    /// Validates the ViewChange.
    /// See below for the different steps regarding the validation.
    pub(crate) fn validate(
        &self,
        config: &Config,
        usig: &mut impl Usig<Signature = Sig>,
    ) -> Result<(), InnerError> {
        // Assure that the signature is correct.
        self.verify(usig).map_err(|usig_error| {
            InnerError::parse_usig_error(usig_error, config.id, "ViewChange", self.origin)
        })?;

        // Only consider messages consistent to the system state.

        // Assure that the CheckpointCertificate is valid.
        match &self.checkpoint {
            Some(checkpoint_cert) => checkpoint_cert.validate(config, usig)?,
            None => {}
        };

        // Assure that the counter value is correct.

        // Assure there are no holes in the sequence number of messages.
        let mut seq_num_msgs = self
            .variant
            .message_log
            .iter()
            .map(|msg| msg.counter().0)
            .collect::<Vec<u64>>();
        seq_num_msgs.sort_unstable();
        let amount_msgs = seq_num_msgs.len();
        if amount_msgs > 0 {
            let first = seq_num_msgs.first().unwrap();
            let last = seq_num_msgs.last().unwrap();
            if amount_msgs as u64 != last - first + 1 {
                return Err(InnerError::ViewChangeHolesInMessageLog {
                    receiver: config.id,
                    origin: self.origin,
                });
            }
        }

        // Assure that the last message in the field message_log of the ViewChange is one less than the counter of the ViewChange.
        match self.variant.message_log.last() {
            Some(last) => {
                if last.counter().0 != self.counter().0 - 1 {
                    return Err(InnerError::ViewChangeLastUnexpectedCounter {
                        receiver: config.id,
                        origin: self.origin,
                        counter_last_msg: last.counter(),
                        counter_expected: Count(self.counter().0 - 1),
                    });
                }
            }
            None => {
                if self.counter().0 != 0 {
                    debug!("counter of ViewChange message: {:?}", self.counter().0);
                    return Err(InnerError::ViewChangeLogUnexpectedlyEmpty {
                        receiver: config.id,
                        origin: self.origin,
                    });
                }
            }
        }
        Ok(())
    }
}
