//! Defines a message of type [NewView].
//! A [NewView] consists of two main parts.
//! The first part is its content, the [NewViewContent].
//! It contains the ID of the replica ([ReplicaId]) which created the [NewView].
//! Moreover, it contains the next [View] to which the replicas should change to.
//! Furthermore, it contains the certificate of the new [View], i.e. the
//! [NewViewCertificate].
//! The second part is its signature, as [NewView]s must be signed by a USIG.
//! A [NewView] is broadcast by the new [View] when enough [ViewChange]s have
//! been collected.
//! For further explanation, see the documentation in [crate::MinBft] or the
//! paper "Efficient Byzantine Fault-Tolerance" by Veronese et al.

use std::collections::HashSet;

use anyhow::Result;
use blake2::digest::Update;
use serde::{Deserialize, Serialize};
use shared_ids::{AnyId, ReplicaId};
use std::fmt::Debug;
use tracing::{debug, error};
use usig::{Counter, Usig};

use crate::{error::InnerError, Config, RequestPayload, View};

use super::{
    signed::{UsigSignable, UsigSigned},
    view_change::ViewChange,
};

/// The content of a message of type [NewView].
/// The content consists of the origin of the message, the next [View], and the
/// [NewViewCertificate].
#[derive(Serialize, Deserialize, Clone, Debug)]
pub(crate) struct NewViewContent<P, Sig> {
    /// The replica which the [NewView] originates from.
    pub(crate) origin: ReplicaId,
    /// The next [View] to which the replica should changed to.
    pub(crate) next_view: View,
    /// The certificate of the [NewView] containing relevant messages for the
    /// transition to the next [View].
    pub(crate) certificate: NewViewCertificate<P, Sig>,
}

impl<P, Sig> AsRef<ReplicaId> for NewViewContent<P, Sig> {
    /// Referencing [NewViewContent] returns a reference to the origin in the
    /// [NewViewContent].
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
            error!(
                "Failed validating NewView (origin: {:?}, next view: {:?}): 
            Origin does not correspond to set next view.",
                self.origin, self.next_view
            );
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
/// Such a message is broadcast by the next [View] in response to having
/// collected a sufficient amount (`t + 1`) of [ViewChange]s
/// (for further explanation, see [crate::Config], [crate::peer_message_processor].
/// [NewView]s can and should be validated.
/// For further explanation regarding the content of the module including
/// [NewView], see the documentation of the module itself.
/// For further explanation regarding the use of [NewView]s, see the
/// documentation of [crate::MinBft].
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
        // The origin of the message of type NewView must be the replica that
        // corresponds to the set next View.
        if !config.is_primary(self.next_view, self.origin) {
            error!(
                "Failed validating NewView (origin: {:?}, next view: {:?}): 
            The origin of the NewView does not correspond to the set next view.",
                self.origin, self.next_view
            );
        };

        // Assure that the signature is correct.
        debug!(
            "Verifying signature of NewView (origin: {:?}, next view: {:?}) ...",
            self.origin, self.next_view
        );
        self.verify(usig).map_or_else(
            |usig_error| {
                error!(
                    "Failed validating NewView (origin: {:?}, next view: {:?}): 
                Signature of NewView is invalid. For further information see output.",
                    self.origin, self.next_view
                );
                Err(InnerError::parse_usig_error(
                    usig_error,
                    config.id,
                    "NewView",
                    self.origin,
                ))
            },
            |v| {
                debug!(
                    "Successfully verified signature of NewView (origin: {:?}, 
                        next view: {:?}).",
                    self.origin, self.next_view
                );
                debug!(
                    "Successfully validated NewView (origin: {:?}, next view: {:?}).",
                    self.origin, self.next_view
                );
                Ok(v)
            },
        )?;

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
/// Must contain at least `t + 1` valid messages of type [ViewChange].
#[derive(Serialize, Deserialize, Clone, Debug)]
pub(crate) struct NewViewCertificate<P, Sig> {
    /// The collection of messages of type [ViewChange] that together form the
    /// [NewViewCertificate].
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
            error!(
                "Failed validating NewViewCertificate: NewViewCertificate 
            does not contain sufficient ViewChanges (contains: {:?}, requires: {:?}). 
            For further information see output.",
                self.view_changes.len(),
                config.t + 1
            );
            return Err(InnerError::NewViewCheckpointCertNotSufficientMsgs {
                receiver: config.id,
            });
        }

        // Assure that all messages of type ViewChange have the same next View set.
        for view_change in &self.view_changes {
            if self.view_changes.first().unwrap().next_view != view_change.next_view {
                error!(
                    "Failed validating NewViewCertificate: Not all ViewChanges 
                contained in the NewViewCertificate have all the same next view. 
                For further information see output."
                );
                return Err(InnerError::NewViewCheckpointCertNotAllSameNextView {
                    receiver: config.id,
                });
            }
        }

        // Assure that all messages of type ViewChange originate from different replicas.
        let mut origins = HashSet::new();
        for msg in &self.view_changes {
            if !origins.insert(msg.origin) {
                error!(
                    "Failed validating NewViewCertificate: Not all ViewChanges 
                contained in the NewViewCertificate originate from different 
                replicas. For further information see output."
                );
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

#[cfg(test)]
pub(crate) mod test {
    use std::num::NonZeroU64;

    use rand::{thread_rng, Rng};
    use rstest::rstest;

    use crate::{
        peer_message::usig_message::view_change::test::{
            create_message_log, create_view_change, setup_view_change_tests, ViewChangeSetup,
        },
        tests::{
            create_attested_usigs_for_replicas, create_default_configs_for_replicas,
            get_random_included_index, get_random_view_with_max, get_shuffled_remaining_replicas,
            get_two_different_indexes,
        },
        View,
    };

    use super::NewViewCertificate;

    #[rstest]
    fn validate_valid_new_view_cert(#[values(3, 4, 5, 6, 7, 8, 9, 10)] n: u64) {
        let mut view_changes = Vec::new();

        let mut vc_setup = setup_view_change_tests(n);

        let shuffled_replicas =
            get_shuffled_remaining_replicas(vc_setup.n_parsed, None, &mut vc_setup.rng);

        for shuffled_replica in &shuffled_replicas {
            let amount_messages: u64 = vc_setup.rng.gen_range(5..10);

            let message_log = create_message_log(
                *shuffled_replica,
                amount_messages,
                None,
                &mut vc_setup.rng,
                &vc_setup.configs,
                &mut vc_setup.usigs,
            );

            let usig_origin = vc_setup.usigs.get_mut(shuffled_replica).unwrap();

            let view_change = create_view_change(
                *shuffled_replica,
                vc_setup.next_view,
                None,
                message_log,
                usig_origin,
            );

            view_changes.push(view_change);
        }

        let new_view_cert = NewViewCertificate { view_changes };

        for shuffled_replica in &shuffled_replicas {
            let config = vc_setup.configs.get(shuffled_replica).unwrap();
            let usig = vc_setup.usigs.get_mut(shuffled_replica).unwrap();
            assert!(new_view_cert.validate(config, usig).is_ok());
        }
    }

    #[rstest]
    fn validate_invalid_new_view_cert_unsuff_msgs(#[values(3, 4, 5, 6, 7, 8, 9, 10)] n: u64) {
        let mut vc_setup = setup_view_change_tests(n);

        for amount_view_change_msgs in 0..vc_setup.t {
            let shuffled_replicas =
                get_shuffled_remaining_replicas(vc_setup.n_parsed, None, &mut vc_setup.rng);
            let shuffled_rep_set = shuffled_replicas
                .iter()
                .take(amount_view_change_msgs as usize);

            let mut view_changes = Vec::new();

            for shuffled_replica in shuffled_rep_set {
                let message_log = create_message_log(
                    *shuffled_replica,
                    vc_setup.amount_messages,
                    None,
                    &mut vc_setup.rng,
                    &vc_setup.configs,
                    &mut vc_setup.usigs,
                );

                let usig_origin = vc_setup.usigs.get_mut(shuffled_replica).unwrap();

                let view_change = create_view_change(
                    *shuffled_replica,
                    vc_setup.next_view,
                    None,
                    message_log,
                    usig_origin,
                );

                view_changes.push(view_change);
            }

            let new_view_cert = NewViewCertificate { view_changes };

            for shuffled_replica in &shuffled_replicas {
                let config = vc_setup.configs.get(shuffled_replica).unwrap();
                let usig = vc_setup.usigs.get_mut(shuffled_replica).unwrap();
                assert!(new_view_cert.validate(config, usig).is_err());
            }
        }
    }

    #[rstest]
    fn validate_invalid_new_view_cert_not_all_same_next_view(
        #[values(3, 4, 5, 6, 7, 8, 9, 10)] n: u64,
    ) {
        let mut view_changes = Vec::new();

        let n_parsed = NonZeroU64::new(n).unwrap();
        let mut rng = thread_rng();
        let shuffled_replicas = get_shuffled_remaining_replicas(n_parsed, None, &mut rng);

        let t = n / 2;
        let next_view_1 = get_random_view_with_max(View(2 * n + 1));
        let mut next_view_2 = get_random_view_with_max(View(2 * n + 1));
        while next_view_1 == next_view_2 {
            next_view_2 = get_random_view_with_max(View(2 * n + 1));
        }
        let random_rep = shuffled_replicas.first().unwrap();

        let shuffled_set = shuffled_replicas.iter().take(t as usize + 1);

        let configs = create_default_configs_for_replicas(n_parsed, t);
        let mut usigs = create_attested_usigs_for_replicas(n_parsed, Vec::new());

        for shuffled_replica in shuffled_set {
            let amount_messages: u64 = rng.gen_range(5..10);

            let message_log = create_message_log(
                *shuffled_replica,
                amount_messages,
                None,
                &mut rng,
                &configs,
                &mut usigs,
            );

            let usig_origin = usigs.get_mut(shuffled_replica).unwrap();

            let next_view = if *shuffled_replica == *random_rep {
                next_view_2
            } else {
                next_view_1
            };

            let view_change =
                create_view_change(*shuffled_replica, next_view, None, message_log, usig_origin);

            view_changes.push(view_change);
        }

        let new_view_cert = NewViewCertificate { view_changes };

        for shuffled_replica in &shuffled_replicas {
            let config = configs.get(shuffled_replica).unwrap();
            let usig = usigs.get_mut(shuffled_replica).unwrap();
            assert!(new_view_cert.validate(config, usig).is_err());
        }
    }

    #[rstest]
    fn validate_invalid_new_view_cert_not_all_diff_origin(
        #[values(3, 4, 5, 6, 7, 8, 9, 10)] n: u64,
    ) {
        let mut view_changes = Vec::new();

        let mut vc_setup = setup_view_change_tests(n);

        let shuffled_replicas =
            get_shuffled_remaining_replicas(vc_setup.n_parsed, None, &mut vc_setup.rng);

        let (index_to_replace_origin, index_origin_to_set_to) =
            get_two_different_indexes(vc_setup.t as usize + 1, &mut vc_setup.rng);
        let (rep_id_to_replace_origin, rep_id_origin_to_set_to) = (
            shuffled_replicas[index_to_replace_origin],
            shuffled_replicas[index_origin_to_set_to],
        );

        let shuffled_set = shuffled_replicas.iter().take(vc_setup.t as usize + 1);

        for shuffled_replica in shuffled_set {
            let amount_messages: u64 = vc_setup.rng.gen_range(5..10);

            let origin = if *shuffled_replica == rep_id_to_replace_origin {
                rep_id_origin_to_set_to
            } else {
                *shuffled_replica
            };

            let message_log = create_message_log(
                origin,
                amount_messages,
                None,
                &mut vc_setup.rng,
                &vc_setup.configs,
                &mut vc_setup.usigs,
            );

            let usig_origin = vc_setup.usigs.get_mut(&origin).unwrap();

            let view_change =
                create_view_change(origin, vc_setup.next_view, None, message_log, usig_origin);

            view_changes.push(view_change);
        }

        let new_view_cert = NewViewCertificate { view_changes };

        for shuffled_replica in &shuffled_replicas {
            let config = vc_setup.configs.get(shuffled_replica).unwrap();
            let usig = vc_setup.usigs.get_mut(shuffled_replica).unwrap();
            assert!(new_view_cert.validate(config, usig).is_err());
        }
    }

    #[rstest]
    fn validate_invalid_new_view_cert_invalid_vchange_msgs(
        #[values(3, 4, 5, 6, 7, 8, 9, 10)] n: u64,
    ) {
        use usig::noop::Signature;

        use crate::{
            peer_message::usig_message::view_change::{
                test::{
                    create_invalid_vchange_cert_empty_log,
                    create_invalid_vchange_cert_log_first_not_cp,
                    create_invalid_vchange_counter_greater_0_empty_msg_log_no_cert,
                    create_invalid_vchange_msg_log_first_missing,
                    create_invalid_vchange_msg_log_hole,
                    create_invalid_vchange_msg_log_last_missing,
                },
                ViewChange,
            },
            tests::DummyPayload,
        };

        let fns_create_invalid_vchange: Vec<
            fn(
                u64,
                Option<&mut ViewChangeSetup>,
            ) -> (ViewChange<DummyPayload, Signature>, Option<ViewChangeSetup>),
        > = vec![
            create_invalid_vchange_cert_empty_log,
            create_invalid_vchange_cert_log_first_not_cp,
            create_invalid_vchange_counter_greater_0_empty_msg_log_no_cert,
            create_invalid_vchange_msg_log_hole,
            create_invalid_vchange_msg_log_first_missing,
            create_invalid_vchange_msg_log_last_missing,
        ];

        for fn_create_invalid_vchange in fns_create_invalid_vchange {
            let mut view_changes = Vec::new();

            let mut vc_setup = setup_view_change_tests(n);

            let shuffled_replicas =
                get_shuffled_remaining_replicas(vc_setup.n_parsed, None, &mut vc_setup.rng);

            let shuffled_set = shuffled_replicas.iter().take(vc_setup.t as usize + 1);

            let random_rep =
                get_random_included_index(vc_setup.t as usize + 1, None, &mut vc_setup.rng);

            for (index, shuffled_rep) in shuffled_set.enumerate() {
                let amount_messages: u64 = vc_setup.rng.gen_range(5..10);

                let message_log = create_message_log(
                    *shuffled_rep,
                    amount_messages,
                    None,
                    &mut vc_setup.rng,
                    &vc_setup.configs,
                    &mut vc_setup.usigs,
                );

                let usig_origin = vc_setup.usigs.get_mut(shuffled_rep).unwrap();

                let view_change = if index == random_rep {
                    fn_create_invalid_vchange(n, Some(&mut vc_setup)).0
                } else {
                    create_view_change(
                        *shuffled_rep,
                        vc_setup.next_view,
                        None,
                        message_log,
                        usig_origin,
                    )
                };

                view_changes.push(view_change);
            }

            let new_view_cert = NewViewCertificate { view_changes };

            for shuffled_replica in &shuffled_replicas {
                let config = vc_setup.configs.get(shuffled_replica).unwrap();
                let usig = vc_setup.usigs.get_mut(shuffled_replica).unwrap();
                assert!(new_view_cert.validate(config, usig).is_err());
            }
        }
    }

    #[ignore]
    fn validate_valid_new_view() {
        todo!();
    }

    #[ignore]
    fn validate_invalid_new_view_origin() {
        todo!();
    }

    #[ignore]
    fn validate_invalid_new_view_signature() {
        todo!();
    }
}
