//! Defines a message of type [NewView].
//! A [NewView] consists of two main parts.
//! The first part is its content, the [NewViewContent].
//! It contains the ID of the replica ([ReplicaId]) which created the [NewView].
//! Moreover, it contains the next [View] to which the replicas should change to.
//! Furthermore, it contains the certificate of the new [View], i.e. the [NewViewCertificate].
//! Additionally, it contains the counter to sync all replicas for the next [View].
//! In other words, it sets the counter of the last accepted [crate::Prepare]
//! to the counter of the last signed message of the new [View] right before it actually becomes the new [View].
//! The second part is its signature, as [NewView]s must be signed by a USIG.
//! A [NewView] is broadcast by the new [View] when enough [ViewChange]s have been collected.
//! For further explanation, see the documentation in [crate::MinBft] or the paper "Efficient Byzantine Fault-Tolerance" by Veronese et al.

use std::collections::HashSet;

use anyhow::Result;
use blake2::digest::Update;
use serde::{Deserialize, Serialize};
use shared_ids::ReplicaId;
use std::fmt::Debug;
use tracing::{debug, error};
use usig::{Counter, Usig};

use crate::{error::InnerError, Config, RequestPayload, View};

use super::{
    signed::{UsigSignable, UsigSigned},
    view_change::ViewChange,
};

/// The content of a message of type [NewView].
/// The content consists of the origin of the message, the next [View], the [NewViewCertificate, and the counter to sync all replicas.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub(crate) struct NewViewContent<P, Sig> {
    /// The replica which the [NewView] originates from.
    pub(crate) origin: ReplicaId,
    /// The next [View] to which the replica should changed to.
    pub(crate) next_view: View,
    /// The certificate of the [NewView] containing relevant messages for the transition to the next [View].
    pub(crate) certificate: NewViewCertificate<P, Sig>,
}

impl<P, Sig> AsRef<ReplicaId> for NewViewContent<P, Sig> {
    /// Referencing [NewViewContent] returns a reference to the origin in the [NewViewContent].
    fn as_ref(&self) -> &ReplicaId {
        &self.origin
    }
}

impl<P: RequestPayload, Sig: Serialize + Counter + Debug> NewViewContent<P, Sig> {
    /// Validates the [NewViewContent].
    /// A [NewViewContent] is valid if its origin is the next [View].
    pub(crate) fn validate(&self, config: &Config) -> Result<(), InnerError> {
        // Assure that the origin is the expected next View.
        if !config.is_primary(self.next_view, self.origin) {
            error!("Failed validating NewView (origin: {:?}, next view: {:?}): Origin does not correspond to set next view.", self.origin, self.next_view);
            return Err(InnerError::NewViewContentUnexpectedNextView {
                receiver: config.id,
                origin_actual: self.origin,
                origin_expected: ReplicaId::from_u64(self.next_view.0 % config.n),
            });
        }
        Ok(())
    }
}

/// The message of type [NewView].
/// [NewView]s consist of their content and must be signed by a USIG.
/// Such a message is broadcast by the next [View] in response to having collected a sufficient amount (t + 1) of [ViewChange]s
/// (for further explanation, see [crate::Config], [crate::peer_message_processor].
/// [NewView]s can and should be validated.
/// For further explanation regarding the content of the module including [NewView], see the documentation of the module itself.
/// For further explanation regarding the use of [NewView]s, see the documentation of [crate::MinBft].
pub(crate) type NewView<P, Sig> = UsigSigned<NewViewContent<P, Sig>, Sig>;

impl<P: Serialize, Sig: Serialize> UsigSignable for NewViewContent<P, Sig> {
    /// Hashes the content of a message of type [NewView].
    /// Required for signing and verifying a message of type [NewView].
    fn hash_content<H: Update>(&self, hasher: &mut H) {
        let encoded = bincode::serialize(self).unwrap();
        hasher.update(&encoded);
    }
}

impl<P: RequestPayload, Sig: Serialize + Counter + Debug> NewView<P, Sig> {
    /// Validates the [NewView].
    /// See below for the different steps regarding the validation.
    pub(crate) fn validate(
        &self,
        config: &Config,
        usig: &mut impl Usig<Signature = Sig>,
    ) -> Result<(), InnerError> {
        debug!(
            "Validating NewView (origin: {:?}, next view: {:?}) ...",
            self.origin, self.next_view
        );
        // The origin of the message of type NewView must be the replica that corresponds to the set next View.
        if !config.is_primary(self.next_view, self.origin) {
            error!("Failed validating NewView (origin: {:?}, next view: {:?}): The origin of the NewView does not correspond to the set next view.", self.origin, self.next_view);
        };

        // Assure that the signature is correct.
        debug!(
            "Verifying signature of NewView (origin: {:?}, next view: {:?}) ...",
            self.origin, self.next_view
        );
        self.verify(usig).map_or_else(|usig_error| {
            error!(
                "Failed validating NewView (origin: {:?}, next view: {:?}): Signature of NewView is invalid. For further information see output.",
                self.origin, self.next_view
            );
            Err(InnerError::parse_usig_error(usig_error, config.id, "NewView", self.origin))
        }, |v| {
            debug!("Successfully verified signature of NewView (origin: {:?}, next view: {:?}).", self.origin, self.next_view);
            debug!("Successfully validated NewView (origin: {:?}, next view: {:?}).", self.origin, self.next_view);
            Ok(v)
        })?;

        self.data.validate(config).map(|v| {
            debug!(
                "Successfully validated NewView (origin: {:?}, next view: {:?}).",
                self.origin, self.next_view
            );
            v
        })
    }
}

/// The certificate of the [NewView].
/// Must contain at least t + 1 valid messages of type [ViewChange].
#[derive(Serialize, Deserialize, Clone, Debug)]
pub(crate) struct NewViewCertificate<P, Sig> {
    /// The collection of messages of type [ViewChange] that together form the [NewViewCertificate].
    pub(crate) view_changes: Vec<ViewChange<P, Sig>>,
}

impl<P: RequestPayload, Sig: Serialize + Counter + Debug> NewViewCertificate<P, Sig> {
    /// Validates the [NewViewCertificate].
    /// See below for the different steps regarding the validation.
    pub(crate) fn validate(
        &self,
        config: &Config,
        usig: &mut impl Usig<Signature = Sig>,
    ) -> Result<(), InnerError> {
        debug!("Validating NewViewCertificate ...");
        // Assure that there are at least t + 1 ViewChanges.
        if (self.view_changes.len() as u64) <= config.t {
            error!("Failed validating NewViewCertificate: NewViewCertificate does not contain sufficient ViewChanges (contains: {:?}, requires: {:?}). For further information see output.", self.view_changes.len(),config.t + 1);
            return Err(InnerError::NewViewCheckpointCertNotSufficientMsgs {
                receiver: config.id,
            });
        }

        // Assure that all messages of type ViewChange have the same next View set.
        for view_change in &self.view_changes {
            if self.view_changes.first().unwrap().next_view != view_change.next_view {
                error!("Failed validating NewViewCertificate: Not all ViewChanges contained in the NewViewCertificate have all the same next view. For further information see output.");
                return Err(InnerError::NewViewCheckpointCertNotAllSameNextView {
                    receiver: config.id,
                });
            }
        }

        // Assure that all messages of type ViewChange originate from different replicas.
        let mut origins = HashSet::new();
        for msg in &self.view_changes {
            if !origins.insert(msg.origin) {
                error!("Failed validating NewViewCertificate: Not all ViewChanges contained in the NewViewCertificate originate from different replicas. For further information see output.");
                return Err(InnerError::NewViewCheckpointCertNotAllDifferentOrigin {
                    receiver: config.id,
                });
            }
        }

        // Assure that the messages of type ViewChange are valid.
        for m in &self.view_changes {
            match m.validate(config, usig) {
                Ok(_) => {}
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }
}
