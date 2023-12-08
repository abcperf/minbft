use std::collections::HashMap;

use crate::{
    client_request::RequestBatch,
    peer_message::{
        usig_message::new_view::NewView, usig_message::new_view::NewViewCertificate,
        usig_message::new_view::NewViewContent,
    },
};
use rstest::rstest;

use super::*;

/// Setups n [MinBft]s configured with the given parameters.
/// Moreover, it returns the [TimeoutHandler]s of the [MinBft]s.
fn setup_set(
    n: u64,
    t: u64,
    checkpoint_period: u64,
) -> (
    Vec<MinBft<DummyPayload, UsigNoOp>>,
    HashMap<ReplicaId, TimeoutHandler>,
) {
    let mut minbfts = Vec::new();
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
        minbfts.push(minbft);
        timeout_handlers.insert(replica, timeout_handler);
    }

    for (id, broadcasts) in all_broadcasts.into_iter() {
        for broadcast in Vec::from(broadcasts).into_iter() {
            // remove once https://github.com/rust-lang/rust/issues/59878 is fixed
            for (_, minbft) in minbfts
                .iter_mut()
                .enumerate()
                .filter(|&(i, _)| ReplicaId::from_u64(i as u64) != id)
            {
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
struct CollectedOutput {
    responses: HashMap<ReplicaId, Vec<(ClientId, DummyPayload)>>,
    errors: HashMap<ReplicaId, Vec<Error>>,
    timeout_requests: HashMap<ReplicaId, Vec<TimeoutRequest>>,
}

impl CollectedOutput {
    /// Returns the relevant timeouts to handle.
    fn timeouts_to_handle(
        &self,
        timeout_handlers: &mut HashMap<ReplicaId, TimeoutHandler>,
    ) -> HashMap<ReplicaId, Vec<TimeoutType>> {
        let mut timeouts_to_handle = HashMap::new();
        for (replica, timeout_requests) in self.timeout_requests.iter() {
            let timeout_handler = timeout_handlers.get_mut(replica).unwrap();
            timeout_handler.handle_timeout_requests(timeout_requests.to_vec());
            timeouts_to_handle.insert(*replica, timeout_handler.retrieve_timeouts_ordered());
        }
        timeouts_to_handle
    }
}

/// Handle messages to be broadcast.
fn handle_broadcasts(
    minbfts: &mut [MinBft<DummyPayload, UsigNoOp>],
    broadcasts_with_origin: Vec<(ReplicaId, Box<[PeerMessageTest]>)>,
    collected_output: &mut CollectedOutput,
) {
    // to collect possibly new added messages to broadcast
    // see below (1)
    let mut all_broadcasts = Vec::new();
    for (from, messages_to_broadcast) in broadcasts_with_origin {
        for message_to_broadcast in Vec::from(messages_to_broadcast).into_iter() {
            // remove once https://github.com/rust-lang/rust/issues/59878 is fixed
            // all other Replicas other than the origin handle the message
            for minbft in minbfts.iter_mut().filter(|m| m.config.id != from) {
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
                    .entry(minbft.config.id)
                    .or_default()
                    .append(&mut Vec::from(responses));
                // collect the errors of the Replica
                collected_output
                    .errors
                    .entry(minbft.config.id)
                    .or_default()
                    .append(&mut Vec::from(errors));
                // collect the timeouts of the Replica
                collected_output
                    .timeout_requests
                    .entry(minbft.config.id)
                    .or_default()
                    .append(&mut Vec::from(timeouts));
                // (1) if the handling of the peer message triggered the creation of new messages that need to be broadcasted,
                // push these new messages to the Vec of all broadcasts to be sent
                if !broadcasts.is_empty() {
                    all_broadcasts.push((minbft.config.id, broadcasts));
                }
            }
        }
    }
    // handle the possibly new added messages to broadcast
    // see above (1)
    if !all_broadcasts.is_empty() {
        handle_broadcasts(minbfts, all_broadcasts, collected_output);
    }
}

/// Try to send a client request.
fn try_client_request(
    minbfts: &mut [MinBft<DummyPayload, UsigNoOp>],
    client_id: ClientId,
    payload: DummyPayload,
) -> CollectedOutput {
    // to collect the output of each Replica generated by handling the client request
    let mut collected_output = CollectedOutput::default();

    // to collect all messages to be broadcasted generated by handling the client message
    let mut all_broadcasts = Vec::new();
    // each Replica handles the client message
    for minbft in minbfts.iter_mut() {
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
            .entry(minbft.config.id)
            .or_default()
            .append(&mut Vec::from(responses));
        // collect the errors of the Replica
        collected_output
            .errors
            .entry(minbft.config.id)
            .or_default()
            .append(&mut Vec::from(errors));
        // collect the timeouts of the Replica
        collected_output
            .timeout_requests
            .entry(minbft.config.id)
            .or_default()
            .append(&mut Vec::from(timeouts));
        // (1) If the handling of the client message triggered the creation of new messages that need to be broadcasted,
        // push these new messages to the Vec of all broadcasts to be sent.
        if !broadcasts.is_empty() {
            all_broadcasts.push((minbft.config.id, broadcasts));
        }
    }

    // handle the new messages to be broadcasted
    // see above (1)
    handle_broadcasts(minbfts, all_broadcasts, &mut collected_output);

    collected_output
}

/// Forces the provided [MinBft]s to handle the given timeouts.
fn force_timeout(
    minbfts: &mut [MinBft<DummyPayload, UsigNoOp>],
    timeouts: &HashMap<ReplicaId, Vec<TimeoutType>>,
) -> CollectedOutput {
    // to collect the output of each Replica generated by handling the client request
    let mut collected_output = CollectedOutput::default();
    // client message is received and handled
    let mut all_broadcasts = Vec::new();

    for minbft in minbfts.iter_mut() {
        if let Some(timeouts_to_handle) = timeouts.get(&minbft.config.id) {
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
                    .entry(minbft.config.id)
                    .or_default()
                    .append(&mut Vec::from(responses));
                // collect the errors of the Replica
                collected_output
                    .errors
                    .entry(minbft.config.id)
                    .or_default()
                    .append(&mut Vec::from(errors));
                // collect the timeouts of the Replica
                collected_output
                    .timeout_requests
                    .entry(minbft.config.id)
                    .or_default()
                    .append(&mut Vec::from(timeouts));
                if !broadcasts.is_empty() {
                    all_broadcasts.push((minbft.config.id, broadcasts));
                }
            }
        }
    }

    handle_broadcasts(minbfts, all_broadcasts, &mut collected_output);
    collected_output
}

/// Forces the provided [MinBft]s to handle the given timeouts.
/// An error is expected (for testing purposes).
fn force_timeout_expect_error(
    minbfts: &mut [MinBft<DummyPayload, UsigNoOp>],
    timeouts: &HashMap<ReplicaId, Vec<TimeoutType>>,
) {
    // to collect the output of each Replica generated by handling the client request
    let mut collected_output = CollectedOutput::default();

    // client message is received and handled
    let mut all_broadcasts = Vec::new();
    for timeouts_to_handle in timeouts.values() {
        for timeout_to_handle in timeouts_to_handle {
            for minbft in minbfts.iter_mut() {
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

                assert!(ready_for_client_requests);
                collected_output
                    .responses
                    .entry(minbft.config.id)
                    .or_default()
                    .append(&mut Vec::from(responses));
                // collect the errors of the Replica
                collected_output
                    .errors
                    .entry(minbft.config.id)
                    .or_default()
                    .append(&mut Vec::from(errors));
                // collect the timeouts of the Replica
                collected_output
                    .timeout_requests
                    .entry(minbft.config.id)
                    .or_default()
                    .append(&mut Vec::from(timeouts));
                if !broadcasts.is_empty() {
                    all_broadcasts.push((minbft.config.id, broadcasts));
                }
            }
        }
    }

    handle_broadcasts(minbfts, all_broadcasts, &mut collected_output);
}

/// Test if sending a single request to a setup of ten Replicas
/// behaves as expected for all valid configurations of t (number of faulty Replicas),
/// i.e. output should be generated as there are sufficient non-faulty Replicas.
#[rstest]
fn single_request_to_ten(#[values(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)] n: u64) {
    for t in 0..n / 2 {
        let (mut minbfts, _timeout_handlers) = setup_set(n, t, 2);
        let collected_output =
            try_client_request(&mut minbfts, ClientId::from_u64(0), DummyPayload(0, true));
        for collected_output_responses in collected_output.responses {
            assert_eq!(
                collected_output_responses.1,
                [(ClientId::from_u64(0), DummyPayload(0, true))]
            )
        }
    }
}

/// Test if sending a single request to varied setups of Replicas (n is at minimum 1 and at maximum 99)
/// behaves as expected for all valid configurations of t (number of faulty Replicas),
/// i.e. output should be generated as there are sufficient non-faulty Replicas.
#[rstest]
fn single_request_to_hundred(
    #[values(1, 3, 5, 7, 9, 11, 13, 15, 17, 19, 29, 39, 49, 59, 69, 79, 89, 99)] n: u64,
) {
    for t in 0..n / 2 {
        let (mut minbfts, _timeout_handlers) = setup_set(n, t, 2);
        let collected_output =
            try_client_request(&mut minbfts, ClientId::from_u64(0), DummyPayload(0, true));
        for collected_output_responses in collected_output.responses {
            assert_eq!(
                collected_output_responses.1,
                [(ClientId::from_u64(0), DummyPayload(0, true))]
            )
        }
    }
}

/// Test if sending a single request to a setup of t + 1 Replicas
/// behaves as expected for all valid configurations of t (number of faulty Replicas),
/// i.e. output should be generated as there are sufficient non-faulty Replicas.
/// n ranges from 1 to 10.
#[rstest]
fn single_request_to_ten_only_t_plus_one(#[values(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)] n: u64) {
    for t in 0..n / 2 {
        let (mut minbfts, _timeout_handlers) = setup_set(n, t, 2);
        minbfts.truncate(t as usize + 1);
        let collected_output =
            try_client_request(&mut minbfts, ClientId::from_u64(0), DummyPayload(0, true));
        for collected_output_responses in collected_output.responses {
            assert_eq!(
                collected_output_responses.1,
                [(ClientId::from_u64(0), DummyPayload(0, true))]
            );
        }
        for collected_output_errors in collected_output.errors {
            assert!(collected_output_errors.1.is_empty());
        }
    }
}

/// Test if sending a single request to t + 1 of Replicas
/// behaves as expected for all valid configurations of t (number of faulty Replicas),
/// i.e. output should be generated as there are sufficient non-faulty Replicas.
/// n is at minimum 1 and at maximum 99.
#[rstest]
fn single_request_to_hundred_only_t_plus_one(
    #[values(1, 3, 5, 7, 9, 11, 13, 15, 17, 19, 29, 39, 49, 59, 69, 79, 89, 99)] n: u64,
) {
    for t in 0..n / 2 {
        let (mut minbfts, _timeout_handlers) = setup_set(n, t, 2);
        minbfts.truncate(t as usize + 1);
        let collected_output =
            try_client_request(&mut minbfts, ClientId::from_u64(0), DummyPayload(0, true));
        for collected_output_responses in collected_output.responses {
            assert_eq!(
                collected_output_responses.1,
                [(ClientId::from_u64(0), DummyPayload(0, true))]
            )
        }
        for collected_output_errors in collected_output.errors {
            assert!(collected_output_errors.1.is_empty());
        }
    }
}

/// Test if sending a single request to a setup of t Replicas
/// behaves as expected for all valid configurations of t (number of faulty Replicas),
/// i.e. no output should be generated as there are insufficient non-faulty Replicas.
/// n ranges from 1 to 10.
#[rstest]
fn single_request_to_ten_only_t(#[values(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)] n: u64) {
    for t in 0..n / 2 {
        let (mut minbfts, _timeout_handlers) = setup_set(n, t, 2);
        minbfts.truncate(t as usize);
        let collected_output =
            try_client_request(&mut minbfts, ClientId::from_u64(0), DummyPayload(0, true));
        for collected_output_responses in collected_output.responses {
            assert_eq!(collected_output_responses.1, [])
        }
        for collected_output_errors in collected_output.errors {
            assert!(collected_output_errors.1.is_empty());
        }
    }
}

/// Test if sending a single request to varied setups of Replicas (n is at minimum 1 and at maximum 99)
/// behaves as expected for all valid configurations of t (number of faulty Replicas),
/// i.e. no output should be generated as there are insufficient non-faulty Replicas.
#[rstest]
fn single_request_to_hundred_only_t(
    #[values(1, 3, 5, 7, 9, 11, 13, 15, 17, 19, 29, 39, 49, 59, 69, 79, 89, 99)] n: u64,
) {
    for t in 0..n / 2 {
        let (mut minbfts, _timeout_handlers) = setup_set(n, t, 2);
        minbfts.truncate(t as usize);
        let collected_output =
            try_client_request(&mut minbfts, ClientId::from_u64(0), DummyPayload(0, true));
        for collected_output_responses in collected_output.responses {
            assert_eq!(collected_output_responses.1, [])
        }
        for collected_output_errors in collected_output.errors {
            assert!(collected_output_errors.1.is_empty());
        }
    }
}

/// Tests for different setups if after a successfull view-change, the new View receives messages.
/// Furthermore, it tests if the new View handles right away a client request previously not handled by the previous View.
#[rstest]
fn view_change_new_view_receives_msgs(
    #[values(3, 4, 5, 6, 7, 8, 9, 10)] n: u64,
    #[values(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)] checkpoint_period: u64,
) {
    for t in 1..n / 2 {
        let (mut minbfts, mut timeout_handlers) = setup_set(n, t, checkpoint_period);

        // remove primary, view-change is expected to be forced when a timeout is triggered
        minbfts.remove(0);

        // send client message
        // the primary got deleted, therefore it is expected that the message is not handled
        let collected_output =
            try_client_request(&mut minbfts, ClientId::from_u64(0), DummyPayload(0, true));

        // output should be empty
        for o in collected_output.responses.values() {
            assert_eq!(o.len(), 0);
        }

        // force timeout in order for view-change to take place
        let timeouts_to_handle = collected_output.timeouts_to_handle(&mut timeout_handlers);
        let collected_output = force_timeout(&mut minbfts, &timeouts_to_handle);

        // (ClientId::from_u64(0), DummyPayload(0, true)) should have been handled by the new View now
        for responses in collected_output.responses.values() {
            assert_eq!(responses.len(), 1);
            assert_eq!(
                *responses.get(0).unwrap(),
                (ClientId::from_u64(0), DummyPayload(0, true))
            );
        }

        // no errors should have been collected
        for errors in collected_output.errors.values() {
            assert!(errors.is_empty());
        }

        // the view-change should have taken place
        // the new View should be Replica(1)
        match &minbfts.get(0).unwrap().view_state {
            ViewState::InView(in_view) => {
                assert!(minbfts.get(0).unwrap().config.me_primary(in_view.view));
            }
            ViewState::ChangeInProgress(_) => {
                panic!("view-change is still in progress, possibly stuck")
            }
        }

        // receive and handle new client message
        let collected_output =
            try_client_request(&mut minbfts, ClientId::from_u64(0), DummyPayload(1, true));

        // (ClientId::from_u64(0), DummyPayload(1, true)) should have been handled by the new View now
        for o in collected_output.responses.values() {
            assert_eq!(o.len(), 1);
            assert_eq!(
                *o.get(0).unwrap(),
                (ClientId::from_u64(0), DummyPayload(1, true))
            );
        }
        // no errors should have been collected
        for errors in collected_output.errors.values() {
            assert!(errors.is_empty());
        }
    }
}

/// Tests for different setups if NewView messages sent by
/// Replicas that are not the new View are ignored.
#[rstest]
fn validation_other_replicas_cannot_send_new_view(
    #[values(2, 3, 4, 5, 6, 7, 8, 9, 10)] n: u64,
    #[values(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)] checkpoint_period: u64,
) {
    let t = n / 2 - 1;
    let (mut minbfts, _timeout_handlers) = setup_set(n, t, checkpoint_period);
    // check if NewView messages sent by Replicas that are not the new View are ignored
    for index_other_non_primary in 1..t {
        let minbft_origin = minbfts.get_mut(index_other_non_primary as usize).unwrap();
        let id_origin = minbft_origin.config.id;
        let next_view = View(index_other_non_primary + 1);

        let msg: NewView<DummyPayload, _> = NewView::sign(
            NewViewContent {
                origin: minbft_origin.config.id,
                next_view,
                certificate: NewViewCertificate {
                    view_changes: vec![],
                },
            },
            &mut minbft_origin.usig,
        )
        .unwrap();

        minbfts
            .get_mut(index_other_non_primary as usize)
            .unwrap()
            .sent_usig_msgs
            .push(peer_message::usig_message::UsigMessageV::NewView(
                msg.clone(),
            ));

        let new_view_msg = ValidatedPeerMessage::from(msg).into();

        let output = minbfts
            .get_mut(0)
            .unwrap()
            .handle_peer_message(id_origin, new_view_msg);

        assert_eq!(output.errors.len(), 1);
    }
}

/// Tests for different setups if the new View is and behaves as expected
/// when forcing a second view-change after the first one succeeded.
#[rstest]
fn view_change_two_view_changes(
    #[values(6, 7, 8, 9, 10)] n: u64,
    #[values(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)] checkpoint_period: u64,
) {
    for t in 1..n / 2 - 1 {
        let (mut minbfts, mut timeout_handlers) = setup_set(n, t, checkpoint_period);

        let collected_output =
            try_client_request(&mut minbfts, ClientId::from_u64(0), DummyPayload(0, true));

        for responses in collected_output.responses.values() {
            assert_eq!(responses.len(), 1);
            assert_eq!(
                *responses.get(0).unwrap(),
                (ClientId::from_u64(0), DummyPayload(0, true))
            );
        }
        // no errors should have been collected
        for errors in collected_output.errors.values() {
            assert!(errors.is_empty());
        }

        let collected_output =
            try_client_request(&mut minbfts, ClientId::from_u64(0), DummyPayload(1, true));

        // DummyPayload(1, true) should have been handled
        for responses in collected_output.responses.values() {
            assert_eq!(responses.len(), 1);
            assert_eq!(
                *responses.get(0).unwrap(),
                (ClientId::from_u64(0), DummyPayload(1, true))
            );
        }
        // no errors should have been collected
        for errors in collected_output.errors.values() {
            assert!(errors.is_empty());
        }

        // the View should still be Replica(0)
        for minbft in &minbfts {
            match &minbft.view_state {
                ViewState::InView(in_view) => {
                    assert!(minbft
                        .config
                        .is_primary(in_view.view, minbfts.get(0).unwrap().config.id));
                }
                ViewState::ChangeInProgress(_) => panic!(
                    "view-change is still in progress for {:?}, possibly stuck",
                    minbft.config.id
                ),
            }
        }

        minbfts.remove(0);

        let collected_output =
            try_client_request(&mut minbfts, ClientId::from_u64(0), DummyPayload(2, true));

        for responses in collected_output.responses.values() {
            assert_eq!(responses.len(), 0);
        }

        let timeouts_to_handle = collected_output.timeouts_to_handle(&mut timeout_handlers);
        let collected_output = force_timeout(&mut minbfts, &timeouts_to_handle);

        let timeouts_to_handle = collected_output.timeouts_to_handle(&mut timeout_handlers);
        let collected_output = force_timeout(&mut minbfts, &timeouts_to_handle);

        // the View should be Replica(1)
        for minbft in &minbfts {
            match &minbft.view_state {
                ViewState::InView(in_view) => {
                    assert!(minbft
                        .config
                        .is_primary(in_view.view, minbfts.get(0).unwrap().config.id));
                }
                ViewState::ChangeInProgress(_) => panic!(
                    "view-change is still in progress for {:?}, possibly stuck",
                    minbft.config.id
                ),
            }
        }

        // DummyPayload(2, true) should have been handled by the new View now
        for responses in collected_output.responses.values() {
            assert_eq!(responses.len(), 1);
            assert_eq!(
                *responses.get(0).unwrap(),
                (ClientId::from_u64(0), DummyPayload(2, true))
            );
        }
        // no errors should have been collected
        for errors in collected_output.errors.values() {
            assert!(errors.is_empty());
        }

        minbfts.remove(0);

        let collected_output =
            try_client_request(&mut minbfts, ClientId::from_u64(0), DummyPayload(3, true));

        for responses in collected_output.responses.values() {
            assert_eq!(responses.len(), 0);
        }

        // force handling timeout, view-change is expected to be forced as well
        let timeouts_to_handle = collected_output.timeouts_to_handle(&mut timeout_handlers);
        let collected_output = force_timeout(&mut minbfts, &timeouts_to_handle);

        // the View should still be Replica(2)
        for minbft in &minbfts {
            match &minbft.view_state {
                ViewState::InView(in_view) => {
                    assert!(minbft
                        .config
                        .is_primary(in_view.view, minbfts.get(0).unwrap().config.id));
                }
                ViewState::ChangeInProgress(_) => panic!(
                    "view-change is still in progress for {:?}, possibly stuck",
                    minbft.config.id
                ),
            }
        }

        // DummyPayload(3, true) should have been handled by the new View now
        for (_, responses) in collected_output.responses.iter() {
            assert_eq!(responses.len(), 1);
            assert_eq!(
                *responses.get(0).unwrap(),
                (ClientId::from_u64(0), DummyPayload(3, true))
            );
        }
        // no errors should have been collected
        for errors in collected_output.errors.values() {
            assert!(errors.is_empty());
        }
    }
}

/// Tests for different setups if the new View accepts client requests that the previous view received
/// and had not yet accepted before a view-change took place.
#[rstest]
fn view_change_request_accepted_prev_view(
    #[values(4, 5, 6, 7, 8, 9, 10)] n: u64,
    #[values(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)] checkpoint_period: u64,
) {
    for t in 1..n / 2 {
        let (mut minbfts, mut timeout_handlers) = setup_set(n, t, checkpoint_period);

        for client_request_i in (0..t).step_by(2) {
            let collected_output = try_client_request(
                &mut minbfts,
                ClientId::from_u64(0),
                DummyPayload(client_request_i, true),
            );
            for responses in collected_output.responses.values() {
                assert_eq!(responses.len(), 1);
            }
            // no errors should have been collected
            for errors in collected_output.errors.values() {
                assert!(errors.is_empty());
            }

            // primary is deleted, a client message should therefore be ignored
            minbfts.remove(0);
            let collected_output = try_client_request(
                &mut minbfts,
                ClientId::from_u64(0),
                DummyPayload(client_request_i + 1, true),
            );

            // output is expected to remain unchanged
            for responses in collected_output.responses.values() {
                assert_eq!(responses.len(), 0);
            }

            // force handling timeout, view-change is expected to be forced as well
            let timeouts_to_handle = collected_output.timeouts_to_handle(&mut timeout_handlers);
            let collected_output = force_timeout(&mut minbfts, &timeouts_to_handle);

            let timeouts_to_handle = collected_output.timeouts_to_handle(&mut timeout_handlers);
            let collected_output = force_timeout(&mut minbfts, &timeouts_to_handle);

            // DummyPayload(client_request_i + 1, true) should have been handled by the new View now
            for responses in collected_output.responses.values() {
                assert_eq!(responses.len(), 1);
                assert_eq!(
                    *responses.get(0).unwrap(),
                    (
                        ClientId::from_u64(0),
                        DummyPayload(client_request_i + 1, true)
                    )
                );
                // no errors should have been collected
                for errors in collected_output.errors.values() {
                    assert!(errors.is_empty());
                }
            }

            // view-change should have taken place
            for minbft in &minbfts {
                match &minbft.view_state {
                    ViewState::InView(in_view) => {
                        assert!(minbft
                            .config
                            .is_primary(in_view.view, minbfts.get(0).unwrap().config.id));
                    }
                    ViewState::ChangeInProgress(_) => panic!(
                        "view-change is still in progress for {:?}, possibly stuck",
                        minbft.config.id
                    ),
                }
            }

            // request that was already accepted by previous view should not be accepted by the new view
            for minbft in minbfts.iter_mut() {
                assert!(minbft
                    .handle_client_message(
                        ClientId::from_u64(0),
                        DummyPayload(client_request_i, true)
                    )
                    .responses
                    .is_empty());
            }
        }
    }
}

/// Test if after performing a view-change, only the new View may send a Prepare message.
/// In other words, test if no Replica is able to impersonate the current View.
#[rstest]
fn view_change_primary_impersonation(
    #[values(4, 5, 6, 7)] n: u64,
    #[values(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)] checkpoint_period: u64,
) {
    for t in 1..n / 2 {
        let (mut minbfts, mut timeout_handlers) = setup_set(n, t, checkpoint_period);

        let collected_output =
            try_client_request(&mut minbfts, ClientId::from_u64(0), DummyPayload(0, true));
        for responses in collected_output.responses.values() {
            assert_eq!(responses.len(), 1);
            assert_eq!(
                *responses.get(0).unwrap(),
                (ClientId::from_u64(0), DummyPayload(0, true))
            );
        }
        // no errors should have been collected
        for errors in collected_output.errors.values() {
            assert!(errors.is_empty());
        }

        // primary is deleted, a client message should therefore be ignored
        let deleted_primary = minbfts.remove(0);
        let collected_output =
            try_client_request(&mut minbfts, ClientId::from_u64(0), DummyPayload(1, true));

        // output is expected to remain unchanged
        for responses in collected_output.responses.values() {
            assert_eq!(responses.len(), 0);
        }

        // force handling timeout, view-change is expected to be forced as well
        let timeouts_to_handle = collected_output.timeouts_to_handle(&mut timeout_handlers);
        let collected_output = force_timeout(&mut minbfts, &timeouts_to_handle);

        // view-change should have taken place
        // new View should be Replica(1)
        for minbft in &minbfts {
            match &minbft.view_state {
                ViewState::InView(in_view) => {
                    assert!(minbft
                        .config
                        .is_primary(in_view.view, minbfts.get(0).unwrap().config.id));
                }
                ViewState::ChangeInProgress(_) => panic!(
                    "view-change is still in progress for {:?}, possibly stuck",
                    minbft.config.id
                ),
            }
        }

        // DummyPayload(1, true) should have been handled by the new View now
        for responses in collected_output.responses.values() {
            assert_eq!(responses.len(), 1);
            assert_eq!(
                *responses.get(0).unwrap(),
                (ClientId::from_u64(0), DummyPayload(1, true))
            );
        }
        // no errors should have been collected
        for errors in collected_output.errors.values() {
            assert!(errors.is_empty());
        }

        assert_eq!(
            minbfts.get(0).unwrap().config.me(),
            ReplicaId::from_u64(deleted_primary.config.id.as_u64() + 1)
        );

        for i in 1..n - 1 {
            let minbft_origin = minbfts.get_mut(i as usize).unwrap();
            let id_origin = minbft_origin.config.id;

            let msg = Prepare::sign(
                PrepareContent {
                    view: View(deleted_primary.config.id.as_u64() + 1),
                    origin: id_origin,
                    request_batch: RequestBatch::new(Box::new([])),
                },
                &mut minbft_origin.usig,
            )
            .unwrap();

            let prepare = ValidatedPeerMessage::from(msg).into();

            let output = minbfts
                .get_mut(0)
                .unwrap()
                .handle_peer_message(id_origin, prepare);

            assert_eq!(output.errors.len(), 1);
        }
    }
}

/// Tests for different setups if when deleting the last message of the message log results in an error.
#[rstest]
fn view_change_manipulate_message_log_pop(
    #[values(3, 4, 5, 6, 7, 8, 9, 10)] n: u64,
    #[values(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)] checkpoint_period: u64,
) {
    for t in 1..n / 2 {
        let (mut minbfts, mut timeout_handlers) = setup_set(n, t, checkpoint_period);

        // client message is received and handled
        let collected_output =
            try_client_request(&mut minbfts, ClientId::from_u64(0), DummyPayload(0, true));
        for o in collected_output.responses.values() {
            assert_eq!(o.len(), 1);
            assert_eq!(
                *o.get(0).unwrap(),
                (ClientId::from_u64(0), DummyPayload(0, true))
            );
        }
        // no errors should have been collected
        for errors in collected_output.errors.values() {
            assert!(errors.is_empty());
        }

        // primary is deleted, a client message should therefore be ignored
        minbfts.remove(0);
        let collected_output =
            try_client_request(&mut minbfts, ClientId::from_u64(0), DummyPayload(1, true));

        // output is expected to remain unchanged
        for responses in collected_output.responses.values() {
            assert_eq!(responses.len(), 0);
        }

        assert_eq!(minbfts.get(1).unwrap().config.me(), ReplicaId::from_u64(2));
        let minbft = minbfts.get_mut(1).unwrap();
        minbft.sent_usig_msgs.pop().unwrap();

        // force handling timeout, view-change is expected to be forced as well
        let timeouts_to_handle = collected_output.timeouts_to_handle(&mut timeout_handlers);
        let collected_output = force_timeout(&mut minbfts, &timeouts_to_handle);
        assert_eq!(collected_output.errors.len(), minbfts.len());
    }
}

/// Tests for different setups if when deleting the first message of the message log results in an error.
#[rstest]
fn view_change_manipulate_message_log_pop_front(
    #[values(3, 4, 5, 6, 7, 8, 9, 10)] n: u64,
    #[values(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)] checkpoint_period: u64,
) {
    for t in 1..n / 2 {
        let (mut minbfts, mut timeout_handlers) = setup_set(n, t, checkpoint_period);

        // client message is received and handled
        let collected_output =
            try_client_request(&mut minbfts, ClientId::from_u64(0), DummyPayload(0, true));
        for responses in collected_output.responses.values() {
            assert_eq!(responses.len(), 1);
            assert_eq!(
                *responses.get(0).unwrap(),
                (ClientId::from_u64(0), DummyPayload(0, true))
            );
        }
        // no errors should have been collected
        for errors in collected_output.errors.values() {
            assert!(errors.is_empty());
        }

        // primary is deleted, a client message should therefore be ignored
        minbfts.remove(0);
        let collected_output =
            try_client_request(&mut minbfts, ClientId::from_u64(0), DummyPayload(1, true));

        // output is expected to remain unchanged
        for o in collected_output.responses.values() {
            assert_eq!(o.len(), 0);
        }

        assert_eq!(minbfts.get(1).unwrap().config.me(), ReplicaId::from_u64(2));
        let minbft = minbfts.get_mut(1).unwrap();
        minbft.sent_usig_msgs.pop().unwrap();

        // force handling timeout, view-change is expected to be forced as well
        let timeouts_to_handle = collected_output.timeouts_to_handle(&mut timeout_handlers);
        force_timeout_expect_error(&mut minbfts, &timeouts_to_handle);
        assert_eq!(collected_output.errors.len(), minbfts.len());
    }
}

/// Tests for different setups if when deleting a message somewhere in the middle
/// (not first nor last) of the message log results in an error.
#[rstest]
fn view_change_manipulate_message_log_hole_middle(
    #[values(3, 4, 5, 6, 7, 8, 9, 10)] n: u64,
    #[values(4, 5, 6, 7, 8, 9, 10)] checkpoint_period: u64,
) {
    for t in 1..n / 2 {
        let (mut minbfts, mut timeout_handlers) = setup_set(n, t, checkpoint_period);

        let collected_output =
            try_client_request(&mut minbfts, ClientId::from_u64(0), DummyPayload(0, true));

        for responses in collected_output.responses.values() {
            assert_eq!(responses.len(), 1);
            assert_eq!(
                *responses.get(0).unwrap(),
                (ClientId::from_u64(0), DummyPayload(0, true))
            );
        }
        // no errors should have been collected
        for errors in collected_output.errors.values() {
            assert!(errors.is_empty());
        }

        let collected_output =
            try_client_request(&mut minbfts, ClientId::from_u64(0), DummyPayload(1, true));

        // DummyPayload(1, true) should have been handled
        for o in collected_output.responses.values() {
            assert_eq!(o.len(), 1);
            assert_eq!(
                *o.get(0).unwrap(),
                (ClientId::from_u64(0), DummyPayload(1, true))
            );
        }
        // no errors should have been collected
        for errors in collected_output.errors.values() {
            assert!(errors.is_empty());
        }

        let collected_output =
            try_client_request(&mut minbfts, ClientId::from_u64(0), DummyPayload(2, true));

        // DummyPayload(2, true) should have been handled
        for o in collected_output.responses.values() {
            assert_eq!(o.len(), 1);
            assert_eq!(
                *o.get(0).unwrap(),
                (ClientId::from_u64(0), DummyPayload(2, true))
            );
        }
        // no errors should have been collected
        for errors in collected_output.errors.values() {
            assert!(errors.is_empty());
        }

        // primary is deleted, a client message should therefore be ignored
        minbfts.remove(0);
        let collected_output =
            try_client_request(&mut minbfts, ClientId::from_u64(0), DummyPayload(3, true));

        // output is expected to remain unchanged
        for o in collected_output.responses.values() {
            assert_eq!(o.len(), 0);
        }

        assert_eq!(minbfts.get(1).unwrap().config.me(), ReplicaId::from_u64(2));
        let minbft = minbfts.get_mut(1).unwrap();
        minbft.sent_usig_msgs.remove(1);

        // force handling timeout, view-change is expected to be forced as well
        let timeouts_to_handle = collected_output.timeouts_to_handle(&mut timeout_handlers);
        force_timeout_expect_error(&mut minbfts, &timeouts_to_handle);
        assert_eq!(collected_output.errors.len(), minbfts.len());
    }
}

/// Tests for different setups if every Replica generates a checkpoint when expected
/// (= when enough batches have been accepted so that the checkpoint period is met).
/// Validates if the checkpoint certificates are valid.
/// Implicitly, it tests if the order of the checkpoints generated is as expected.
#[rstest]
fn checkpoint_is_generated_after_period_set(
    #[values(3, 4, 5, 6, 7, 8, 9, 10)] n: u64,
    #[values(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 21, 42)] checkpoint_period: u64,
) {
    for t in 1..n / 2 {
        let (mut minbfts, _timeout_handlers) = setup_set(n, t, checkpoint_period);

        for minbft in &minbfts {
            assert_eq!(
                minbft.config.checkpoint_period,
                NonZeroU64::new(checkpoint_period).unwrap()
            )
        }

        // checkpoint cert does not contain any messages at the beginning
        for minbft in &minbfts {
            assert!(minbft.last_checkpoint_cert.is_none());
        }

        for client_request_i in 1..(checkpoint_period * 2 + 1) {
            // client message is received and handled
            let collected_output = try_client_request(
                &mut minbfts,
                ClientId::from_u64(0),
                DummyPayload(client_request_i, true),
            );

            for responses in collected_output.responses.values() {
                assert_eq!(responses.len(), 1);
                assert_eq!(
                    *responses.get(0).unwrap(),
                    (ClientId::from_u64(0), DummyPayload(client_request_i, true))
                );
            }
            // no errors should have been collected
            for errors in collected_output.errors.values() {
                assert!(errors.is_empty());
            }

            let mut last_hash_expected = [0; 64];
            let mut amount_accepted_batches_expected = 0;
            for minbft in &mut minbfts {
                if minbft.config.id == ReplicaId::from_u64(0) {
                    // Set expected last hash and amount accept batches according to primary.
                    last_hash_expected = minbft.request_processor.checkpoint_generator.last_hash;
                    amount_accepted_batches_expected = minbft
                        .request_processor
                        .checkpoint_generator
                        .total_amount_accepted_batches;
                } else {
                    // Check if all others agree on the expected last hash and amount of accepted batches.
                    assert_eq!(
                        minbft.request_processor.checkpoint_generator.last_hash,
                        last_hash_expected
                    );
                    assert_eq!(
                        minbft
                            .request_processor
                            .checkpoint_generator
                            .total_amount_accepted_batches,
                        amount_accepted_batches_expected
                    );
                }
                // If a CheckpointCertificate was generated, see if everyone agrees on
                // the expected last hash and amount of accept batches.
                if amount_accepted_batches_expected % checkpoint_period == 0 {
                    validate_checkpoint_cert(minbft);
                    assert_eq!(
                        minbft
                            .last_checkpoint_cert
                            .clone()
                            .unwrap()
                            .my_checkpoint
                            .state_hash,
                        last_hash_expected
                    );
                    assert_eq!(
                        minbft
                            .last_checkpoint_cert
                            .clone()
                            .unwrap()
                            .my_checkpoint
                            .total_amount_accepted_batches,
                        amount_accepted_batches_expected
                    );
                }
            }
        }
    }
}

/// Tests if all Replicas change to each expected view in a total of 2n - 1 view-changes
/// when receiving t + 1 ViewChange messages each time.
#[rstest]
fn view_change_multi_view_changes(#[values(4)] n: u64, #[values(1)] checkpoint_period: u64) {
    for t in 1..n / 2 {
        let (mut minbfts, mut timeout_handlers) = setup_set(n, t, checkpoint_period);
        let mut prev_view = View(0);
        let mut next_view = View(1);
        let mut view_change_number = 0;
        while view_change_number < 2 * n + 1 {
            let mut all_broadcasts = Vec::new();
            // to collect the output of each Replica generated by handling client requests
            let mut collected_output = CollectedOutput::default();

            let expected_view = View(prev_view.0);
            let mut replica_number = 0;
            let mut id = (prev_view.0 % n) as usize;
            while replica_number < minbfts.len() {
                match &minbfts[id].view_state {
                    ViewState::InView(in_view) => {
                        assert_eq!(in_view.view.0, expected_view.0);
                        let Output {
                            broadcasts: _,
                            responses: _,
                            timeout_requests,
                            errors: _,
                            ready_for_client_requests: _,
                            primary: _,
                            view_info: _,
                            round: _,
                        } = minbfts[id].handle_client_message(
                            ClientId::from_u64(0),
                            DummyPayload(view_change_number, true),
                        );

                        collected_output
                            .timeout_requests
                            .entry(minbfts[id].config.id)
                            .or_default()
                            .append(&mut Vec::from(timeout_requests));

                        let timeout_types =
                            collected_output.timeouts_to_handle(&mut timeout_handlers);

                        let replica_timeout_types = timeout_types.get(&minbfts[id].config.id);

                        if replica_timeout_types.is_none() {
                            replica_number += 1;
                            id = (id + 1) % n as usize;
                            continue;
                        }

                        for timeout_type in replica_timeout_types.unwrap() {
                            let Output {
                                broadcasts,
                                responses,
                                timeout_requests,
                                errors,
                                ready_for_client_requests,
                                primary: _,
                                view_info: _,
                                round: _,
                            } = minbfts[id].handle_timeout(*timeout_type);

                            assert!(ready_for_client_requests);
                            collected_output
                                .responses
                                .entry(minbfts[id].config.id)
                                .or_default()
                                .append(&mut Vec::from(responses));
                            collected_output
                                .errors
                                .entry(minbfts[id].config.id)
                                .or_default()
                                .append(&mut Vec::from(errors));
                            collected_output
                                .timeout_requests
                                .entry(minbfts[id].config.id)
                                .or_default()
                                .append(&mut Vec::from(timeout_requests));
                            if !broadcasts.is_empty() {
                                all_broadcasts.push((minbfts[id].config.id, broadcasts));
                            }
                        }
                    }
                    ViewState::ChangeInProgress(_) => {
                        panic!("before handle_client_message(): view-change is still in progress, possibly stuck")
                    }
                }
                replica_number += 1;
                id = (id + 1) % n as usize;
            }

            handle_broadcasts(&mut minbfts, all_broadcasts, &mut collected_output);

            let timeouts_to_handle: HashMap<ReplicaId, Vec<TimeoutType>> =
                collected_output.timeouts_to_handle(&mut timeout_handlers);
            let collected_output = force_timeout(&mut minbfts, &timeouts_to_handle);

            // view-change should have taken place
            for minbft in &minbfts {
                let expected_view = View(next_view.0);
                match &minbft.view_state {
                    ViewState::InView(in_view) => {
                        assert_eq!(
                            in_view.view.0, expected_view.0,
                            "for {:?} view is {:?} rather than the expected view {:?}",
                            minbft.config.id, in_view.view, expected_view
                        );
                    }
                    ViewState::ChangeInProgress(_) => {
                        panic!("{:?}: after handle_broadcasts_new(): view-change is still in progress, possibly stuck", minbft.config.id);
                    }
                }
            }

            for (id, responses) in collected_output.responses.iter() {
                assert_eq!(responses.len(), 1, "failed for ReplicaId {:?}", id);
                assert_eq!(
                    *responses.get(0).unwrap(),
                    (
                        ClientId::from_u64(0),
                        DummyPayload(view_change_number, true)
                    ),
                    "failed for ReplicaId {:?}",
                    id
                );
            }

            prev_view = prev_view + 1;
            next_view = next_view + 1;
            view_change_number += 1;
        }
    }
}

/// Tests if a backup does not broadcast two Commits when the primary sends its Prepare twice.
#[test]
fn send_prepare_twice_to_backup() {
    let (mut minbfts, _timeout_handlers) = setup_set(5, 2, 2);

    // send client-request to primary
    let output = minbfts[0].handle_client_message(ClientId::from_u64(23), DummyPayload(56, true));

    // should broadcast Prepare
    assert_eq!(output.broadcasts.len(), 1);
    let broadcasts = Vec::from(output.broadcasts); // remove once https://github.com/rust-lang/rust/issues/59878 is fixed
    let mut iter = broadcasts.into_iter();
    let prepare = iter.next().unwrap();

    // send client-request to backup once
    let output = minbfts[1].handle_peer_message(ReplicaId::from_u64(0), prepare.clone());

    // backup should broadcast Commit
    assert_eq!(output.broadcasts.len(), 1);

    // send client-request to backup twice
    let output = minbfts[1].handle_peer_message(ReplicaId::from_u64(0), prepare);

    // backup should not broadcast Commit
    assert_eq!(output.broadcasts.len(), 0);
}

/// Tests if performing two view-changes subsequently changes to the respective view and
/// if a pending client-request is accepted in the end.
#[test]
fn two_view_changes_subsequently() {
    let (mut minbfts, mut timeout_handlers) = setup_set(5, 1, 2);

    // Remove first two replicas that would be the first two views.
    minbfts.remove(0);
    minbfts.remove(0);

    // Send a client-request.
    let collected_output =
        try_client_request(&mut minbfts, ClientId::from_u64(0), DummyPayload(0, true));

    for responses in collected_output.responses.values() {
        assert_eq!(responses.len(), 0);
    }

    let timeouts_to_handle = collected_output.timeouts_to_handle(&mut timeout_handlers);
    let collected_output = force_timeout(&mut minbfts, &timeouts_to_handle);

    let timeouts_to_handle = collected_output.timeouts_to_handle(&mut timeout_handlers);
    let collected_output = force_timeout(&mut minbfts, &timeouts_to_handle);

    // the View should be Replica(2)
    for minbft in &minbfts {
        match &minbft.view_state {
            ViewState::InView(in_view) => {
                assert!(minbft
                    .config
                    .is_primary(in_view.view, minbfts.get(0).unwrap().config.id));
            }
            ViewState::ChangeInProgress(_) => panic!(
                "view-change is still in progress for {:?}, possibly stuck",
                minbft.config.id
            ),
        }
    }

    // DummyPayload(0, true) should have been handled by the new View now
    for responses in collected_output.responses.values() {
        assert_eq!(responses.len(), 1);
        assert_eq!(
            *responses.get(0).unwrap(),
            (ClientId::from_u64(0), DummyPayload(0, true))
        );
    }
    // no errors should have been collected
    for errors in collected_output.errors.values() {
        assert!(errors.is_empty());
    }
}

/// Tests if performing two view-changes subsequently changes to the respective view and
/// if a pending client-request is accepted in the end.
#[rstest]
fn multi_view_changes_subsequently(
    #[values(5, 6, 7, 8, 9, 11, 21, 31)] n: u64,
    #[values(1, 2, 3)] checkpoint_period: u64,
) {
    let mut t = 1;
    let mut amount_replicas_removable = n - (2 * t + 1);
    while amount_replicas_removable > 1 {
        // At least two view-changes can be forced.
        let (mut minbfts, mut timeout_handlers) = setup_set(n, t, checkpoint_period);
        // Remove first n - (2 * t + 1) replicas to force the same amount of view-changes.
        let mut amount_replicas_removed = 0;
        while amount_replicas_removed < amount_replicas_removable {
            minbfts.remove(0);
            amount_replicas_removed += 1;
        }
        // Send a client-request.
        let collected_output =
            try_client_request(&mut minbfts, ClientId::from_u64(0), DummyPayload(0, true));

        for responses in collected_output.responses.values() {
            assert_eq!(responses.len(), 0);
        }

        let timeouts_to_handle: HashMap<ReplicaId, Vec<TimeoutType>> =
            collected_output.timeouts_to_handle(&mut timeout_handlers);
        let mut collected_output = force_timeout(&mut minbfts, &timeouts_to_handle);

        let mut i = 1;
        while i < amount_replicas_removed {
            let timeouts_to_handle: HashMap<ReplicaId, Vec<TimeoutType>> =
                collected_output.timeouts_to_handle(&mut timeout_handlers);
            collected_output = force_timeout(&mut minbfts, &timeouts_to_handle);
            i += 1;
        }

        // the View should be Replica(2 * t)
        for minbft in &minbfts {
            match &minbft.view_state {
                ViewState::InView(in_view) => {
                    assert!(minbft
                        .config
                        .is_primary(in_view.view, minbfts.get(0).unwrap().config.id));
                }
                ViewState::ChangeInProgress(_) => panic!(
                    "view-change is still in progress for {:?}, possibly stuck",
                    minbft.config.id
                ),
            }
        }

        // DummyPayload(0, true) should have been handled by the new View now
        for responses in collected_output.responses.values() {
            assert_eq!(responses.len(), 1);
            assert_eq!(
                *responses.get(0).unwrap(),
                (ClientId::from_u64(0), DummyPayload(0, true))
            );
        }
        // no errors should have been collected
        for errors in collected_output.errors.values() {
            assert!(errors.is_empty());
        }
        t += 1;
        amount_replicas_removable = n - (2 * t + 1);
    }
}

/// Test if a replica that was previously the primary and later becomes the primary again
/// still receives and accepts client requests after a full view change cycle.
#[rstest]
fn replica_becomes_primary_after_view_change_cycle(
    #[values(5, 6, 7, 8, 9, 11, 21, 31)] n: u64,
    #[values(1, 2, 3)] checkpoint_period: u64,
) {
    let (mut minbfts, mut timeout_handlers) = setup_set(n, 1, checkpoint_period);

    let mut current_req_id = 0;
    let mut current_view = 0;

    while current_view < n {
        let current_primary = minbfts.remove((current_view % n).try_into().unwrap());

        // send client message
        // the primary got deleted, therefore it is expected that the message is not handled
        let collected_output = try_client_request(
            &mut minbfts,
            ClientId::from_u64(0),
            DummyPayload(current_req_id, true),
        );
        // output should be empty
        for o in collected_output.responses.values() {
            assert_eq!(o.len(), 0);
        }
        minbfts.insert((current_view % n).try_into().unwrap(), current_primary);

        // force timeout in order for view-change to take place
        let timeouts_to_handle = collected_output.timeouts_to_handle(&mut timeout_handlers);
        let collected_output = force_timeout(&mut minbfts, &timeouts_to_handle);

        let timeouts_to_handle = collected_output.timeouts_to_handle(&mut timeout_handlers);
        let collected_output = force_timeout(&mut minbfts, &timeouts_to_handle);

        // client request should have been handled by the new View now
        for responses in collected_output.responses.values() {
            assert_eq!(responses.len(), 1);
            assert_eq!(
                *responses.get(0).unwrap(),
                (ClientId::from_u64(0), DummyPayload(current_req_id, true))
            );
        }

        // no errors should have been collected
        for errors in collected_output.errors.values() {
            assert!(errors.is_empty());
        }
        // the view-change should have taken place
        // the new View should be Replica(1)
        for minbft in &minbfts {
            match &minbft.view_state {
                ViewState::InView(in_view) => {
                    assert!(minbft
                        .config
                        .is_primary(in_view.view, ReplicaId::from_u64((current_view + 1) % n)));
                }
                ViewState::ChangeInProgress(in_progress) => {
                    panic!(
                        "view-change is still in progress, possibly stuck (from {:?}, to {:?}",
                        in_progress.prev_view, in_progress.next_view
                    );
                }
            }
        }
        current_view += 1;
        current_req_id += 1;
    }
    // receive and handle new client message
    let collected_output = try_client_request(
        &mut minbfts,
        ClientId::from_u64(0),
        DummyPayload(current_req_id, true),
    );

    // (ClientId::from_u64(0), DummyPayload(1, true)) should have been handled by the new View now
    for o in collected_output.responses.values() {
        assert_eq!(o.len(), 1);
        assert_eq!(
            *o.get(0).unwrap(),
            (ClientId::from_u64(0), DummyPayload(current_req_id, true))
        );
    }
    // no errors should have been collected
    for errors in collected_output.errors.values() {
        assert!(errors.is_empty());
    }
}

/// Validates the checkpoint certificate of the provided Replica.
fn validate_checkpoint_cert(minbft: &mut MinBft<DummyPayload, UsigNoOp>) {
    assert!(minbft.last_checkpoint_cert.is_some());
    // assure checkpoint cert contains at least t + 1 checkpoint messages,
    // in which 1 is own message
    assert!(
        (minbft
            .last_checkpoint_cert
            .clone()
            .unwrap()
            .other_checkpoints
            .len() as u64)
            >= minbft.config.t
    );

    // assure all checkpoint messages have the state hash of own message
    for other in &minbft
        .last_checkpoint_cert
        .clone()
        .unwrap()
        .other_checkpoints
    {
        assert!(
            minbft
                .last_checkpoint_cert
                .clone()
                .unwrap()
                .my_checkpoint
                .state_hash
                == other.state_hash
        );
    }

    // assure all checkpoint messages are from different replicas
    let mut origins = HashSet::new();
    origins.insert(
        minbft
            .last_checkpoint_cert
            .clone()
            .unwrap()
            .my_checkpoint
            .origin,
    );
    for msg in &minbft
        .last_checkpoint_cert
        .as_ref()
        .unwrap()
        .other_checkpoints
    {
        assert!(origins.insert(msg.origin));
    }

    // assure checkpoint messages are valid
    minbft
        .last_checkpoint_cert
        .clone()
        .unwrap()
        .my_checkpoint
        .validate(&minbft.config, &mut minbft.usig)
        .unwrap();
    for msg in &minbft
        .last_checkpoint_cert
        .clone()
        .unwrap()
        .other_checkpoints
    {
        assert!(msg.validate(&minbft.config, &mut minbft.usig).is_ok());
    }
}
