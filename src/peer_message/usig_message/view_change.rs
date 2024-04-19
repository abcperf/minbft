//! Defines a message of type [ViewChange].\
//! A [ViewChange] is broadcast by a replica when enough [crate::ReqViewChange]s
//! have been collected.\
//! For further explanation, see the documentation in [crate::MinBft] or the
//! paper "Efficient Byzantine Fault-Tolerance" by Veronese et al.

use std::{fmt::Debug, marker::PhantomData};

use anyhow::Result;
use blake2::{digest::Update, Blake2b512, Digest};
use serde::{Deserialize, Serialize};
use shared_ids::AnyId;
use tracing::{error, trace};
use usig::{Count, Counter, Usig};

use crate::{error::InnerError, Config, ReplicaId, RequestPayload, View};

use super::{
    checkpoint::CheckpointCertificate,
    signed::{UsigSignable, UsigSigned},
    UsigMessage, UsigMessageV,
};

/// The trait for the variants of a [ViewChange] message.\
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

/// A [ViewChange] message contains a message log.\
/// However, the messages in the message log must not contain a message log
/// themselves.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub(crate) struct ViewChangeVariantLog<P, Sig> {
    /// The log containing the messages of type UsigMessage.
    pub(crate) message_log: Vec<UsigMessageV<ViewChangeVariantNoLog, P, Sig>>,
}

impl<P, Sig> ViewChangeVariant<P, Sig> for ViewChangeVariantLog<P, Sig> {
    // The hash of a ViewChange message that contains a message log is the hash
    // of the message log.
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

/// The struct guarantees that message logs cannot contain a message log
/// themselves (that is, it prevents the nesting of message logs).
#[derive(Serialize, Deserialize, Clone, Debug)]
pub(crate) struct ViewChangeVariantNoLog {
    #[serde(with = "serde_bytes")]
    hash: Box<[u8]>,
}

impl<P, Sig> ViewChangeVariant<P, Sig> for ViewChangeVariantNoLog {
    // The hash of a ViewChange message that does not contain a message log is
    // simply the struct field `hash`.
    type Hash<'s> = &'s [u8];

    // Returns a reference of the hash.
    fn hash(&self) -> Self::Hash<'_> {
        &self.hash
    }
}

/// The struct defines the content of a [ViewChange] message.\
/// For further explanation, see the documentation in the module.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub(crate) struct ViewChangeContent<V: ViewChangeVariant<P, Sig>, P, Sig> {
    /// The replica which the [ViewChange] originates from.
    pub(crate) origin: ReplicaId,
    /// The next [View] to which it should be changed to.
    pub(crate) next_view: View,
    /// The struct field checkpoint is optional since there can only exist a
    /// previous checkpoint certificate if one was created before.
    pub(crate) checkpoint_cert: Option<CheckpointCertificate<Sig>>,
    /// The variant of the [ViewChange].
    pub(crate) variant: V,
    /// To mark P as owned.
    pub(crate) phantom_data: PhantomData<P>,
}

impl<P: Serialize + Clone, Sig: Serialize + Clone>
    ViewChangeContent<ViewChangeVariantLog<P, Sig>, P, Sig>
{
    /// Create a new ViewChangeContent.
    ///
    /// # Arguments
    ///
    /// * `origin` - The ID of the replica from which the content of the
    /// [ViewChange] originates.
    /// * `next_view` - The next [View] to change to.
    /// * `checkpoint` - The last [CheckpointCertificate] generated.
    /// * `message_log` - The message log containing the messages broadcast
    /// by the replica since the last [CheckpointCertificate].
    ///
    /// # Return Value
    ///
    /// The created [ViewChangeContent].
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
            checkpoint_cert: checkpoint,
            variant: ViewChangeVariantLog { message_log },
            phantom_data: PhantomData::default(),
        }
    }
}

impl<V: ViewChangeVariant<P, Sig>, P, Sig> AsRef<ReplicaId> for ViewChangeContent<V, P, Sig> {
    /// Referencing [ViewChangeContent] returns a reference to the origin in the
    /// [ViewChangeContent].
    fn as_ref(&self) -> &ReplicaId {
        &self.origin
    }
}

/// The types for defining a message of type ViewChange.\
/// The type ViewChangeV can either be a ViewChange with a log or without it.
pub(crate) type ViewChangeV<V, P, Sig> = UsigSigned<ViewChangeContent<V, P, Sig>, Sig>;

/// The type [ViewChange] is always a [ViewChangeV<ViewChangeVariantLog>].\
/// For further explanation see the documentation of the module.
pub(crate) type ViewChange<P, Sig> = ViewChangeV<ViewChangeVariantLog<P, Sig>, P, Sig>;

impl<P: Serialize, Sig: Clone + Serialize> ViewChange<P, Sig> {
    /// Convert a [ViewChange] to a [ViewChangeV] with the variant containing no
    /// log.
    pub(super) fn to_no_log(&self) -> ViewChangeV<ViewChangeVariantNoLog, P, Sig> {
        let ViewChangeContent {
            origin,
            next_view,
            checkpoint_cert: checkpoint,
            variant,
            phantom_data,
        } = &self.data;

        self.clone_signature(ViewChangeContent {
            origin: *origin,
            next_view: *next_view,
            checkpoint_cert: checkpoint.clone(),
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
    /// Hashes the content of a message of type [ViewChange].\
    /// Required for signing and verifying a message of type [ViewChange].
    fn hash_content<H: Update>(&self, hasher: &mut H) {
        hasher.update(&self.origin.as_u64().to_be_bytes());
        hasher.update(&self.next_view.0.to_be_bytes());
        hasher.update(&bincode::serialize(&self.checkpoint_cert).unwrap());
        hasher.update(&self.variant.hash());
    }
}

impl<P: RequestPayload, Sig: Counter + Serialize + Debug> ViewChange<P, Sig> {
    /// Validates the ViewChange.\
    /// See below for the different steps regarding the validation.
    ///
    /// # Arguments
    ///
    /// * `config` - The config of the replica.
    /// * `usig` - The USIG signature that should be valid for the [ViewChange].
    pub(crate) fn validate(
        &self,
        config: &Config,
        usig: &mut impl Usig<Signature = Sig>,
    ) -> Result<(), InnerError> {
        trace!(
            "Validating ViewChange (origin: {:?}, next view: {:?}) ...",
            self.origin,
            self.next_view
        );
        // Assure that the signature is correct.
        trace!(
            "Verifying signature of ViewChange (origin: {:?}, next view: {:?}) ...",
            self.origin,
            self.next_view
        );
        self.verify(usig).map_or_else(|usig_error| {
            error!("Failed validating ViewChange (origin: {:?}, next view: {:?}): Verification of the signature failed.", self.origin, self.next_view);
            Err(InnerError::parse_usig_error(usig_error, config.id, "Commit", self.origin))
        }, |v| {
            trace!("Successfully verified signature of ViewChange (origin: {:?}, next view: {:?}).", self.origin, self.next_view);
            Ok(v)
        })?;

        // Only consider messages consistent to the system state.

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

        // Assure that the CheckpointCertificate is valid.
        match &self.checkpoint_cert {
            Some(checkpoint_cert) => {
                checkpoint_cert.validate(config, usig)?;
                if amount_msgs == 0 {
                    error!(
                        "Failed validating ViewChange (origin: {:?}, next 
                        view: {:?}): Log of ViewChange is empty when a 
                        certificate was provided. The first message should be
                        the checkpoint of the replica's certificate. For further 
                        information see output.",
                        self.origin, self.next_view
                    );
                    return Err(InnerError::ViewChangeMessageLogEmptyWithCert {
                        receiver: config.id,
                        origin: self.origin,
                    });
                }
                if *seq_num_msgs.first().unwrap() != checkpoint_cert.my_checkpoint.counter().0 {
                    error!(
                        "Failed validating ViewChange (origin: {:?}, next 
                        view: {:?}): First message in log of ViewChange does 
                        not have the expected counter. The first message should
                        correspond to the checkpoint of the replica's 
                        certificate. For further information see output.",
                        self.origin, self.next_view
                    );
                    return Err(InnerError::ViewChangeFirstUnexpectedCounter {
                        receiver: config.id,
                        origin: self.origin,
                        counter_first_msg: Count(*seq_num_msgs.first().unwrap()),
                        counter_expected: checkpoint_cert.my_checkpoint.counter(),
                    });
                }
            }
            None => {
                if amount_msgs > 0 && *seq_num_msgs.first().unwrap() != 0 {
                    error!(
                        "Failed validating ViewChange (origin: {:?}, next 
                        view: {:?}): First message in log of ViewChange does 
                        not have the expected counter when no certificate is 
                        provided. For further information see output.",
                        self.origin, self.next_view
                    );
                    return Err(InnerError::ViewChangeFirstUnexpectedCounter {
                        receiver: config.id,
                        origin: self.origin,
                        counter_first_msg: Count(*seq_num_msgs.first().unwrap()),
                        counter_expected: Count(0),
                    });
                }
            }
        };

        if amount_msgs > 0 {
            let first = seq_num_msgs.first().unwrap();
            let last = seq_num_msgs.last().unwrap();
            if amount_msgs as u64 != last - first + 1 {
                error!("Failed validating ViewChange (origin: {:?}, next view: {:?}): ViewChange contains holes in its message log. For further information see output.", self.origin, self.next_view);
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
                    error!("Failed validating ViewChange (origin: {:?}, next view: {:?}): Counter of last message is not one less than the counter of the ViewChange. For further information see output.", self.origin, self.next_view);
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
                    error!("Failed validating ViewChange (origin: {:?}, next view: {:?}): The message log of the ViewChange is unexpectedly empty. For further information see output.", self.origin, self.next_view);
                    return Err(InnerError::ViewChangeLogUnexpectedlyEmpty {
                        receiver: config.id,
                        origin: self.origin,
                    });
                }
            }
        }
        trace!(
            "Successfully validated ViewChange (origin: {:?}, next view: {:?}).",
            self.origin,
            self.next_view
        );
        Ok(())
    }
}

#[cfg(test)]
pub(crate) mod test {
    use rand::rngs::ThreadRng;
    use rand::{thread_rng, Rng};
    use rstest::rstest;
    use usig::{noop::Signature, AnyId};

    use std::num::NonZeroU64;
    use std::{collections::HashMap, marker::PhantomData};

    use super::{ViewChange, ViewChangeVariantNoLog};

    use usig::{noop::UsigNoOp, Count, ReplicaId};

    use crate::tests::{create_attested_usigs_for_replicas, create_default_configs_for_replicas};
    use crate::{
        client_request::test::create_batch,
        peer_message::usig_message::{
            checkpoint::{
                test::{create_checkpoint_cert_random, create_invalid_checkpoint_certs},
                CheckpointCertificate,
            },
            view_change::{ViewChangeContent, ViewChangeVariantLog},
            view_peer_message::{prepare::test::create_prepare, ViewPeerMessage},
            UsigMessageV,
        },
        tests::{
            create_random_state_hash, get_random_replica_id, get_random_view_with_max, DummyPayload,
        },
        Config, View,
    };

    /// Defines the type for the creation of a [ViewChange] for the tests.
    pub(crate) type ViewChangeCreation =
        fn(
            u64,
            Option<&mut ViewChangeSetup>,
        ) -> (ViewChange<DummyPayload, Signature>, Option<ViewChangeSetup>);

    /// Enumerates the different types of message log manipulation.
    pub(crate) enum MessageLogManipulation {
        PopFirst,
        PopLast,
        RemoveRandomInBetween,
    }

    /// Creates a [ViewChange] with the given parameters.
    ///
    /// # Arguments
    ///
    /// * `origin` - The ID of the replica from which the [ViewChange]
    /// originates.
    /// * `next_view` - The next [View] to which it should be changed to.
    /// * `checkpoint_cert` - The last [CheckpointCertificate] generated, if
    /// there is one, else [None].
    /// * `message_log` - The message log containing the messages broadcast by
    /// the replica itself since the last [CheckpointCertificate].
    /// * `usig` - The USIG signature to be used to sign the [ViewChange].
    ///
    /// # Return Value
    ///
    /// The created [ViewChange].
    pub(crate) fn create_view_change(
        origin: ReplicaId,
        next_view: View,
        checkpoint_cert: Option<CheckpointCertificate<Signature>>,
        message_log: Vec<UsigMessageV<ViewChangeVariantNoLog, DummyPayload, Signature>>,
        usig: &mut UsigNoOp,
    ) -> ViewChange<DummyPayload, Signature> {
        ViewChange::sign(
            ViewChangeContent {
                origin,
                next_view,
                checkpoint_cert,
                variant: ViewChangeVariantLog { message_log },
                phantom_data: PhantomData,
            },
            usig,
        )
        .unwrap()
    }

    /// Creates an invalid [ViewChange].
    /// The message log of the [ViewChange] is manipulated with the given
    /// variant.
    ///
    /// # Arguments
    ///
    /// * `manipulation` - The message log manipulation variant, if any.
    /// * `vc_setup` - The [ViewChangeSetup] to be used to create the invalid
    /// [ViewChange].
    ///
    /// # Return Value
    ///
    /// The created invalid [ViewChange].
    pub(crate) fn create_invalid_view_change_log_manipulated(
        manipulation: Option<MessageLogManipulation>,
        vc_setup: &mut ViewChangeSetup,
    ) -> ViewChange<DummyPayload, Signature> {
        let message_log = create_message_log(
            vc_setup.origin,
            vc_setup.amount_messages,
            manipulation,
            &mut vc_setup.rng,
            &vc_setup.configs,
            &mut vc_setup.usigs,
        );

        let usig_origin = vc_setup.usigs.get_mut(&vc_setup.origin).unwrap();

        create_view_change(
            vc_setup.origin,
            vc_setup.next_view,
            None,
            message_log,
            usig_origin,
        )
    }

    /// Create a message log.
    ///
    /// # Arguments
    ///
    /// * `origin` - The ID of the replica from which the log originates.
    /// * `amount_messages` - The amount of messages to be contained in the log.
    /// * `manipulation` - The message log manipulation variant, if any.
    /// * `rng` - The random number generator.
    /// * `configs` - The configs of the replicas.
    /// * `usigs` - The USIGs of the replicas.
    ///
    /// # Return Value
    ///
    /// The created message log.
    pub(crate) fn create_message_log(
        origin: ReplicaId,
        amount_messages: u64,
        manipulation: Option<MessageLogManipulation>,
        rng: &mut ThreadRng,
        configs: &HashMap<ReplicaId, Config>,
        usigs: &mut HashMap<ReplicaId, UsigNoOp>,
    ) -> Vec<UsigMessageV<ViewChangeVariantNoLog, DummyPayload, Signature>> {
        let mut message_log = Vec::new();

        let config_origin = configs.get(&origin).unwrap();
        let usig_origin = usigs.get_mut(&origin).unwrap();

        let view = View(origin.as_u64());
        for _ in 0..amount_messages {
            let request_batch = create_batch();
            let prepare = create_prepare(view, request_batch, config_origin, usig_origin);
            message_log.push(UsigMessageV::View(ViewPeerMessage::Prepare(prepare)));
        }

        match manipulation {
            Some(MessageLogManipulation::PopFirst) if !message_log.is_empty() => {
                message_log.remove(0);
            }
            Some(MessageLogManipulation::PopLast) if !message_log.is_empty() => {
                message_log.pop();
            }
            Some(MessageLogManipulation::RemoveRandomInBetween) if message_log.len() > 2 => {
                let rand_i = rng.gen_range(1..message_log.len() - 1);
                message_log.remove(rand_i);
            }
            _ => {}
        }

        message_log
    }

    /// Sets up the [ViewChange] tests.
    ///
    /// # Arguments
    ///
    /// * `n` - The number of replicas.
    ///
    /// # Return Value
    ///
    /// The created [ViewChangeSetup].
    pub(crate) fn setup_view_change_tests(n: u64) -> ViewChangeSetup {
        let n_parsed = NonZeroU64::new(n).unwrap();
        let mut rng = thread_rng();
        let origin = get_random_replica_id(n_parsed, &mut rng);

        let t = n / 2;
        let amount_messages: u64 = rng.gen_range(5..10);
        let next_view = get_random_view_with_max(View(2 * n + 1)) + 1;

        let configs = create_default_configs_for_replicas(n_parsed, t);
        let usigs = create_attested_usigs_for_replicas(n_parsed, Vec::new());

        ViewChangeSetup {
            n_parsed,
            t,
            origin,
            next_view,
            amount_messages,
            rng,
            configs,
            usigs,
        }
    }

    /// Defines the setup state of [ViewChange] tests.
    pub(crate) struct ViewChangeSetup {
        /// The parsed number of replicas.
        pub(crate) n_parsed: NonZeroU64,
        /// The maximum number of replicas that can be faulty, but still
        /// tolerated by the algorithm.
        pub(crate) t: u64,
        /// The ID of the replica from which the [ViewChange] originates.
        pub(crate) origin: ReplicaId,
        /// The next [View] to be changed to.
        pub(crate) next_view: View,
        /// The amount of messages that the message log should contain.
        pub(crate) amount_messages: u64,
        /// The random number generator.
        pub(crate) rng: ThreadRng,
        /// The configs of the replicas.
        pub(crate) configs: HashMap<ReplicaId, Config>,
        /// The USIGs of the replicas.
        pub(crate) usigs: HashMap<ReplicaId, UsigNoOp>,
    }

    /// Tests if the validation of a valid [ViewChange] with no certificate
    /// succeeds.
    ///
    /// # Arguments
    ///
    /// * `n` - The number of the replicas.
    #[rstest]
    fn validate_valid_view_change_no_cert(#[values(3, 4, 5, 6, 7, 8, 9, 10)] n: u64) {
        let mut vc_setup = setup_view_change_tests(n);
        let message_log = create_message_log(
            vc_setup.origin,
            vc_setup.amount_messages,
            None,
            &mut vc_setup.rng,
            &vc_setup.configs,
            &mut vc_setup.usigs,
        );

        let usig_origin = vc_setup.usigs.get_mut(&vc_setup.origin).unwrap();

        let view_change = create_view_change(
            vc_setup.origin,
            vc_setup.next_view,
            None,
            message_log,
            usig_origin,
        );

        for i in 0..n {
            let rep_id = ReplicaId::from_u64(i);
            let config = vc_setup.configs.get(&rep_id).unwrap();
            let usig = vc_setup.usigs.get_mut(&rep_id).unwrap();
            assert!((view_change.validate(config, usig)).is_ok());
        }
    }

    /// Tests if the validation of a valid [ViewChange] with no certificate and
    /// empty message log succeeds.
    ///
    /// # Arguments
    ///
    /// * `n` - The number of the replicas.
    #[rstest]
    fn validate_valid_view_change_counter_eq_0_empty_msg_log_no_cert(
        #[values(3, 4, 5, 6, 7, 8, 9, 10)] n: u64,
    ) {
        let mut vc_setup = setup_view_change_tests(n);

        let message_log = Vec::new();

        let usig_origin = vc_setup.usigs.get_mut(&vc_setup.origin).unwrap();

        let view_change = create_view_change(
            vc_setup.origin,
            vc_setup.next_view,
            None,
            message_log,
            usig_origin,
        );

        for i in 0..n {
            let rep_id = ReplicaId::from_u64(i);
            let config = vc_setup.configs.get(&rep_id).unwrap();
            let usig = vc_setup.usigs.get_mut(&rep_id).unwrap();
            assert!((view_change.validate(config, usig)).is_ok());
        }
    }

    /// Create a [ViewChange] with no certificate, but with counter greater than
    /// zero, and empty message log.
    ///
    /// # Arguments
    ///
    /// * `n` - The number of the replicas.
    ///
    /// # Return Value
    ///
    /// The created invalid [ViewChange].
    pub(crate) fn create_invalid_vchange_counter_greater_0_empty_msg_log_no_cert(
        n: u64,
        vc_setup: Option<&mut ViewChangeSetup>,
    ) -> (ViewChange<DummyPayload, Signature>, Option<ViewChangeSetup>) {
        if let Some(vc_setup) = vc_setup {
            let mut message_log = create_message_log(
                vc_setup.origin,
                vc_setup.amount_messages,
                None,
                &mut vc_setup.rng,
                &vc_setup.configs,
                &mut vc_setup.usigs,
            );

            message_log.drain(..);

            let usig_origin = vc_setup.usigs.get_mut(&vc_setup.origin).unwrap();

            let view_change = create_view_change(
                vc_setup.origin,
                vc_setup.next_view,
                None,
                message_log,
                usig_origin,
            );

            (view_change, None)
        } else {
            let mut vc_setup = setup_view_change_tests(n);

            let mut message_log = create_message_log(
                vc_setup.origin,
                vc_setup.amount_messages,
                None,
                &mut vc_setup.rng,
                &vc_setup.configs,
                &mut vc_setup.usigs,
            );

            message_log.drain(..);

            let usig_origin = vc_setup.usigs.get_mut(&vc_setup.origin).unwrap();

            let view_change = create_view_change(
                vc_setup.origin,
                vc_setup.next_view,
                None,
                message_log,
                usig_origin,
            );

            (view_change, Some(vc_setup))
        }
    }

    /// Tests if the validation of an invalid [ViewChange] with no certificate
    /// fails.
    /// The [ViewChange] counter is greater zero, though the message log is
    /// empty.
    ///
    /// # Arguments
    ///
    /// * `n` - The number of the replicas.
    #[rstest]
    fn validate_invalid_view_change_counter_greater_0_empty_msg_log_no_cert(
        #[values(3, 4, 5, 6, 7, 8, 9, 10)] n: u64,
    ) {
        let (view_change, vc_setup) =
            create_invalid_vchange_counter_greater_0_empty_msg_log_no_cert(n, None);
        let mut vc_setup = vc_setup.unwrap();

        for i in 0..n {
            let rep_id = ReplicaId::from_u64(i);
            let config = vc_setup.configs.get(&rep_id).unwrap();
            let usig = vc_setup.usigs.get_mut(&rep_id).unwrap();
            assert!((view_change.validate(config, usig)).is_err());
        }
    }

    /// Creates an invalid [ViewChange] with no certificate.
    /// The message log of the [ViewChange] contains a hole, i.e., a message
    /// in-between is missing.
    ///
    /// # Arguments
    ///
    /// * `n` - The number of the replicas.
    ///
    /// # Return Value
    ///
    /// The created invalid [ViewChange].
    pub(crate) fn create_invalid_vchange_msg_log_hole(
        n: u64,
        vc_setup: Option<&mut ViewChangeSetup>,
    ) -> (ViewChange<DummyPayload, Signature>, Option<ViewChangeSetup>) {
        if let Some(vc_setup) = vc_setup {
            let view_change = create_invalid_view_change_log_manipulated(
                Some(MessageLogManipulation::RemoveRandomInBetween),
                vc_setup,
            );
            (view_change, None)
        } else {
            let mut vc_setup = setup_view_change_tests(n);

            let view_change = create_invalid_view_change_log_manipulated(
                Some(MessageLogManipulation::RemoveRandomInBetween),
                &mut vc_setup,
            );

            (view_change, Some(vc_setup))
        }
    }

    /// Tests if the validation of an invalid [ViewChange] with no certificate
    /// fails.
    /// The message log of the [ViewChange] contains a hole, i.e., a message
    /// in-between is missing.
    ///
    /// # Arguments
    ///
    /// * `n` - The number of the replicas.
    #[rstest]
    fn validate_invalid_view_change_msg_log_hole(#[values(3, 4, 5, 6, 7, 8, 9, 10)] n: u64) {
        let (view_change, vc_setup) = create_invalid_vchange_msg_log_hole(n, None);
        let mut vc_setup = vc_setup.unwrap();

        for i in 0..n {
            let rep_id = ReplicaId::from_u64(i);
            let config = vc_setup.configs.get(&rep_id).unwrap();
            let usig = vc_setup.usigs.get_mut(&rep_id).unwrap();
            assert!((view_change.validate(config, usig)).is_err());
        }
    }

    /// Creates an invalid [ViewChange] with no certificate.
    /// The first message of the message log of the [ViewChange] is missing.
    ///
    /// # Arguments
    ///
    /// * `n` - The number of the replicas.
    ///
    /// # Return Value
    ///
    /// The created invalid [ViewChange].
    pub(crate) fn create_invalid_vchange_msg_log_first_missing(
        n: u64,
        vc_setup: Option<&mut ViewChangeSetup>,
    ) -> (ViewChange<DummyPayload, Signature>, Option<ViewChangeSetup>) {
        if let Some(vc_setup) = vc_setup {
            let view_change = create_invalid_view_change_log_manipulated(
                Some(MessageLogManipulation::PopFirst),
                vc_setup,
            );
            (view_change, None)
        } else {
            let mut vc_setup = setup_view_change_tests(n);
            let view_change = create_invalid_view_change_log_manipulated(
                Some(MessageLogManipulation::PopFirst),
                &mut vc_setup,
            );
            (view_change, Some(vc_setup))
        }
    }

    /// Tests if the validation of an invalid [ViewChange] with no certificate
    /// fails.
    /// The first message of the message log of the [ViewChange] is missing.
    ///
    /// # Arguments
    ///
    /// * `n` - The number of the replicas.
    #[rstest]
    fn validate_invalid_view_change_msg_log_first_missing(
        #[values(3, 4, 5, 6, 7, 8, 9, 10)] n: u64,
    ) {
        let (view_change, vc_setup) = create_invalid_vchange_msg_log_first_missing(n, None);
        let mut vc_setup = vc_setup.unwrap();

        for i in 0..n {
            let rep_id = ReplicaId::from_u64(i);
            let config = vc_setup.configs.get(&rep_id).unwrap();
            let usig = vc_setup.usigs.get_mut(&rep_id).unwrap();
            assert!((view_change.validate(config, usig)).is_err());
        }
    }

    /// Creates an invalid [ViewChange] with no certificate.
    /// The last message of the message log of the [ViewChange] is missing.
    ///
    /// # Arguments
    ///
    /// * `n` - The number of the replicas.
    ///
    /// # Return Value
    ///
    /// The created invalid [ViewChange].
    pub(crate) fn create_invalid_vchange_msg_log_last_missing(
        n: u64,
        vc_setup: Option<&mut ViewChangeSetup>,
    ) -> (ViewChange<DummyPayload, Signature>, Option<ViewChangeSetup>) {
        if let Some(vc_setup) = vc_setup {
            let view_change = create_invalid_view_change_log_manipulated(
                Some(MessageLogManipulation::PopLast),
                vc_setup,
            );
            (view_change, None)
        } else {
            let mut vc_setup = setup_view_change_tests(n);
            let view_change = create_invalid_view_change_log_manipulated(
                Some(MessageLogManipulation::PopLast),
                &mut vc_setup,
            );
            (view_change, Some(vc_setup))
        }
    }

    /// Tests if the validation of an invalid [ViewChange] with no certificate
    /// fails.
    /// The last message of the message log of the [ViewChange] is missing.
    ///
    /// # Arguments
    ///
    /// * `n` - The number of the replicas.
    #[rstest]
    fn validate_invalid_view_change_msg_log_last_missing(
        #[values(3, 4, 5, 6, 7, 8, 9, 10)] n: u64,
    ) {
        let (view_change, vc_setup) = create_invalid_vchange_msg_log_last_missing(n, None);
        let mut vc_setup = vc_setup.unwrap();

        for i in 0..n {
            let rep_id = ReplicaId::from_u64(i);
            let config = vc_setup.configs.get(&rep_id).unwrap();
            let usig = vc_setup.usigs.get_mut(&rep_id).unwrap();
            assert!((view_change.validate(config, usig)).is_err());
        }
    }

    /// Tests if the validation of an invalid [ViewChange] with no certificate
    /// fails.
    /// The USIG signature of the [ViewChange] is unknown.
    ///
    /// # Arguments
    ///
    /// * `n` - The number of the replicas.
    #[rstest]
    fn validate_invalid_view_change_signature(#[values(3, 4, 5, 6, 7, 8, 9, 10)] n: u64) {
        let n_parsed = NonZeroU64::new(n).unwrap();
        let mut rng = thread_rng();
        let origin = get_random_replica_id(n_parsed, &mut rng);

        let t = n / 2;
        let amount_messages: u64 = rng.gen_range(5..10);
        let next_view = get_random_view_with_max(View(2 * n + 1)) + 1;

        let configs = create_default_configs_for_replicas(n_parsed, t);
        let mut usigs = create_attested_usigs_for_replicas(n_parsed, vec![origin]);

        let message_log = create_message_log(
            origin,
            amount_messages,
            None,
            &mut rng,
            &configs,
            &mut usigs,
        );

        let usig_origin = usigs.get_mut(&origin).unwrap();

        let view_change = create_view_change(origin, next_view, None, message_log, usig_origin);

        for i in 0..n {
            let rep_id = ReplicaId::from_u64(i);
            let config = configs.get(&rep_id).unwrap();
            let usig = usigs.get_mut(&rep_id).unwrap();
            assert!((view_change.validate(config, usig)).is_err());
        }
    }

    /// Tests if the validation of a valid [ViewChange] with a certificate
    /// succeeds.
    ///
    /// # Arguments
    ///
    /// * `n` - The number of the replicas.
    #[rstest]
    fn validate_valid_view_change_with_cert(#[values(3, 4, 5, 6, 7, 8, 9, 10)] n: u64) {
        let mut vc_setup = setup_view_change_tests(n);

        let _ = create_message_log(
            vc_setup.origin,
            vc_setup.amount_messages,
            None,
            &mut vc_setup.rng,
            &vc_setup.configs,
            &mut vc_setup.usigs,
        );

        let checkpoint_cert = create_checkpoint_cert_random(
            vc_setup.n_parsed,
            vc_setup.t,
            vc_setup.origin,
            &mut vc_setup.rng,
            &mut vc_setup.usigs,
        );

        let mut message_log = Vec::new();
        message_log.push(UsigMessageV::Checkpoint(
            checkpoint_cert.my_checkpoint.clone(),
        ));
        let mut next_messages = create_message_log(
            vc_setup.origin,
            vc_setup.amount_messages,
            None,
            &mut vc_setup.rng,
            &vc_setup.configs,
            &mut vc_setup.usigs,
        );
        message_log.append(&mut next_messages);

        let usig_origin = vc_setup.usigs.get_mut(&vc_setup.origin).unwrap();

        let view_change = create_view_change(
            vc_setup.origin,
            vc_setup.next_view,
            Some(checkpoint_cert),
            message_log,
            usig_origin,
        );

        for i in 0..n {
            let rep_id = ReplicaId::from_u64(i);
            let config = vc_setup.configs.get(&rep_id).unwrap();
            let usig = vc_setup.usigs.get_mut(&rep_id).unwrap();
            assert!((view_change.validate(config, usig)).is_ok());
        }
    }

    /// Tests if the validation of an invalid [ViewChange] with a certificate
    /// fails.
    ///
    /// # Arguments
    ///
    /// * `n` - The number of the replicas.
    #[rstest]
    fn validate_invalid_view_change_with_invalid_cert(#[values(5, 6, 7, 8, 9, 10)] n: u64) {
        let mut vc_setup = setup_view_change_tests(n);

        let _ = create_message_log(
            vc_setup.origin,
            vc_setup.amount_messages,
            None,
            &mut vc_setup.rng,
            &vc_setup.configs,
            &mut vc_setup.usigs,
        );

        let counter_latest_prep = Count(vc_setup.rng.gen());
        let total_amount_accepted_batches: u64 = vc_setup.rng.gen();
        let state_hash = create_random_state_hash();
        let certs_invalid = create_invalid_checkpoint_certs(
            vc_setup.n_parsed,
            vc_setup.t,
            vc_setup.origin,
            state_hash,
            counter_latest_prep,
            total_amount_accepted_batches,
            &mut vc_setup.rng,
            &mut vc_setup.usigs,
        );

        for cert_invalid in certs_invalid {
            let mut message_log = Vec::new();
            message_log.push(UsigMessageV::Checkpoint(cert_invalid.my_checkpoint.clone()));
            let mut next_messages = create_message_log(
                vc_setup.origin,
                vc_setup.amount_messages,
                None,
                &mut vc_setup.rng,
                &vc_setup.configs,
                &mut vc_setup.usigs,
            );
            message_log.append(&mut next_messages);

            let usig_origin = vc_setup.usigs.get_mut(&vc_setup.origin).unwrap();

            let view_change = create_view_change(
                vc_setup.origin,
                vc_setup.next_view,
                Some(cert_invalid),
                message_log,
                usig_origin,
            );

            for i in 0..n {
                let rep_id = ReplicaId::from_u64(i);
                let config = vc_setup.configs.get(&rep_id).unwrap();
                let usig = vc_setup.usigs.get_mut(&rep_id).unwrap();
                assert!((view_change.validate(config, usig)).is_err());
            }
        }
    }

    /// Creates an invalid [ViewChange] with a certificate, but with an empty
    /// log.
    ///
    /// # Arguments
    ///
    /// * `n` - The number of the replicas.
    /// * `vc_setup` - The [ViewChangeSetup].
    pub(crate) fn create_invalid_vchange_cert_empty_log(
        n: u64,
        vc_setup: Option<&mut ViewChangeSetup>,
    ) -> (ViewChange<DummyPayload, Signature>, Option<ViewChangeSetup>) {
        if let Some(vc_setup) = vc_setup {
            let _ = create_message_log(
                vc_setup.origin,
                vc_setup.amount_messages,
                None,
                &mut vc_setup.rng,
                &vc_setup.configs,
                &mut vc_setup.usigs,
            );

            let checkpoint_cert = create_checkpoint_cert_random(
                vc_setup.n_parsed,
                vc_setup.t,
                vc_setup.origin,
                &mut vc_setup.rng,
                &mut vc_setup.usigs,
            );

            let message_log = Vec::new();

            let usig_origin = vc_setup.usigs.get_mut(&vc_setup.origin).unwrap();

            let view_change = create_view_change(
                vc_setup.origin,
                vc_setup.next_view,
                Some(checkpoint_cert),
                message_log,
                usig_origin,
            );

            (view_change, None)
        } else {
            let mut vc_setup = setup_view_change_tests(n);

            let _ = create_message_log(
                vc_setup.origin,
                vc_setup.amount_messages,
                None,
                &mut vc_setup.rng,
                &vc_setup.configs,
                &mut vc_setup.usigs,
            );

            let checkpoint_cert = create_checkpoint_cert_random(
                vc_setup.n_parsed,
                vc_setup.t,
                vc_setup.origin,
                &mut vc_setup.rng,
                &mut vc_setup.usigs,
            );

            let message_log = Vec::new();

            let usig_origin = vc_setup.usigs.get_mut(&vc_setup.origin).unwrap();

            let view_change = create_view_change(
                vc_setup.origin,
                vc_setup.next_view,
                Some(checkpoint_cert),
                message_log,
                usig_origin,
            );

            (view_change, Some(vc_setup))
        }
    }

    /// Tests if the validation of an invalid [ViewChange] with a certificate,
    /// but with an empty log, fails.
    ///
    /// # Arguments
    ///
    /// * `n` - The number of the replicas.
    #[rstest]
    fn validate_invalid_view_change_with_cert_empty_log(#[values(3, 4, 5, 6, 7, 8, 9, 10)] n: u64) {
        let (view_change, vc_setup) = create_invalid_vchange_cert_empty_log(n, None);
        let mut vc_setup = vc_setup.unwrap();

        for i in 0..n {
            let rep_id = ReplicaId::from_u64(i);
            let config = vc_setup.configs.get(&rep_id).unwrap();
            let usig = vc_setup.usigs.get_mut(&rep_id).unwrap();
            assert!((view_change.validate(config, usig)).is_err());
        }
    }

    /// Creates an invalid [ViewChange] with a certificate, but the first
    /// message in the log is not a Checkpoint.
    ///
    /// # Arguments
    ///
    /// * `n` - The number of the replicas.
    /// * `vc_setup` - The [ViewChangeSetup].
    pub(crate) fn create_invalid_vchange_cert_log_first_not_cp(
        n: u64,
        vc_setup: Option<&mut ViewChangeSetup>,
    ) -> (ViewChange<DummyPayload, Signature>, Option<ViewChangeSetup>) {
        if let Some(vc_setup) = vc_setup {
            let _ = create_message_log(
                vc_setup.origin,
                vc_setup.amount_messages,
                None,
                &mut vc_setup.rng,
                &vc_setup.configs,
                &mut vc_setup.usigs,
            );

            let checkpoint_cert = create_checkpoint_cert_random(
                vc_setup.n_parsed,
                vc_setup.t,
                vc_setup.origin,
                &mut vc_setup.rng,
                &mut vc_setup.usigs,
            );

            let mut message_log = Vec::new();
            let mut next_messages = create_message_log(
                vc_setup.origin,
                vc_setup.amount_messages,
                None,
                &mut vc_setup.rng,
                &vc_setup.configs,
                &mut vc_setup.usigs,
            );
            message_log.append(&mut next_messages);

            let usig_origin = vc_setup.usigs.get_mut(&vc_setup.origin).unwrap();

            let view_change = create_view_change(
                vc_setup.origin,
                vc_setup.next_view,
                Some(checkpoint_cert),
                message_log,
                usig_origin,
            );
            (view_change, None)
        } else {
            let mut vc_setup = setup_view_change_tests(n);

            let _ = create_message_log(
                vc_setup.origin,
                vc_setup.amount_messages,
                None,
                &mut vc_setup.rng,
                &vc_setup.configs,
                &mut vc_setup.usigs,
            );

            let checkpoint_cert = create_checkpoint_cert_random(
                vc_setup.n_parsed,
                vc_setup.t,
                vc_setup.origin,
                &mut vc_setup.rng,
                &mut vc_setup.usigs,
            );

            let mut message_log = Vec::new();
            let mut next_messages = create_message_log(
                vc_setup.origin,
                vc_setup.amount_messages,
                None,
                &mut vc_setup.rng,
                &vc_setup.configs,
                &mut vc_setup.usigs,
            );
            message_log.append(&mut next_messages);

            let usig_origin = vc_setup.usigs.get_mut(&vc_setup.origin).unwrap();

            let view_change = create_view_change(
                vc_setup.origin,
                vc_setup.next_view,
                Some(checkpoint_cert),
                message_log,
                usig_origin,
            );

            (view_change, Some(vc_setup))
        }
    }

    /// Tests if the validation of an invalid [ViewChange] with a certificate,
    /// but the first message of the message log is not a Checkpoint.
    ///
    /// # Arguments
    ///
    /// * `n` - The number of the replicas.
    #[rstest]
    fn validate_invalid_view_change_with_cert_log_first_not_cp(
        #[values(3, 4, 5, 6, 7, 8, 9, 10)] n: u64,
    ) {
        let (view_change, vc_setup) = create_invalid_vchange_cert_log_first_not_cp(n, None);
        let mut vc_setup = vc_setup.unwrap();

        for i in 0..n {
            let rep_id = ReplicaId::from_u64(i);
            let config = vc_setup.configs.get(&rep_id).unwrap();
            let usig = vc_setup.usigs.get_mut(&rep_id).unwrap();
            assert!((view_change.validate(config, usig)).is_err());
        }
    }
}
