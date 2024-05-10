use std::collections::HashSet;

use crate::{
    client_request::RequestBatch,
    peer_message::usig_message::UsigMessageV,
    peer_message::{
        usig_message::new_view::NewView, usig_message::new_view::NewViewCertificate,
        usig_message::new_view::NewViewContent,
    },
    ViewState,
};
use rstest::rstest;

use rand::thread_rng;

use super::*;

/// Tests for different setups if after a successfull view-change, the new View receives messages.
/// Furthermore, it tests if the new View handles right away a client request previously not handled by the previous View.
#[rstest]
fn view_change_new_view_receives_msgs(
    #[values(3, 4, 5, 6, 7, 8, 9, 10)] n: u64,
    #[values(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)] checkpoint_period: u64,
) {
    let mut rng = thread_rng();
    for t in 1..n / 2 {
        let (mut minbfts, mut timeout_handlers) = setup_set(n, t, checkpoint_period);

        // remove primary, view-change is expected to be forced when a timeout is triggered
        minbfts.remove(&ReplicaId::from_u64(0));

        // send client message
        // the primary got deleted, therefore it is expected that the message is not handled
        let mut collected_output = try_client_request(
            &mut minbfts,
            ClientId::from_u64(0),
            DummyPayload(0, true),
            &mut rng,
        );

        // output should be empty
        for o in collected_output.responses.values() {
            assert_eq!(o.len(), 0);
        }

        // force timeout in order for view-change to take place
        for (r, _) in minbfts.iter() {
            timeout_handlers
                .get_mut(r)
                .unwrap()
                .handle_timeout_requests(
                    collected_output.timeout_requests.get(r).unwrap().to_owned(),
                );
        }
        collected_output = force_timeout(&mut minbfts, &mut timeout_handlers, &mut rng);

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
        let new_view = ReplicaId::from_u64(1);
        match &minbfts.get(&new_view).unwrap().view_state {
            ViewState::InView(in_view) => {
                assert!(minbfts
                    .get(&new_view)
                    .unwrap()
                    .config
                    .me_primary(in_view.view));
            }
            ViewState::ChangeInProgress(_) => {
                panic!("view-change is still in progress, possibly stuck")
            }
        }

        // receive and handle new client message
        let collected_output = try_client_request(
            &mut minbfts,
            ClientId::from_u64(0),
            DummyPayload(1, true),
            &mut rng,
        );

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
    #[values(3, 4, 5, 6, 7, 8, 9, 10)] n: u64,
    #[values(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)] checkpoint_period: u64,
) {
    let t = n / 2 - 1;
    let (mut minbfts, _timeout_handlers) = setup_set(n, t, checkpoint_period);
    // check if NewView messages sent by Replicas that are not the new View are ignored
    for index_other_non_primary in 1..t {
        let rep_id = ReplicaId::from_u64(index_other_non_primary);
        let minbft_origin = minbfts.get_mut(&rep_id).unwrap();
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
            .get_mut(&rep_id)
            .unwrap()
            .sent_usig_msgs
            .push(UsigMessageV::NewView(msg.clone()));

        let new_view_msg = ValidatedPeerMessage::from(msg).into();

        let output = minbfts
            .get_mut(&ReplicaId::from_u64(0))
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
    let mut rng = thread_rng();
    for t in 1..(n - 1) / 2 {
        let (mut minbfts, mut timeout_handlers) = setup_set(n, t, checkpoint_period);

        let collected_output = try_client_request(
            &mut minbfts,
            ClientId::from_u64(0),
            DummyPayload(0, true),
            &mut rng,
        );

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

        let collected_output = try_client_request(
            &mut minbfts,
            ClientId::from_u64(0),
            DummyPayload(1, true),
            &mut rng,
        );

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
        let id_primary = ReplicaId::from_u64(0);
        for minbft in minbfts.values() {
            match &minbft.view_state {
                ViewState::InView(in_view) => {
                    assert!(minbft
                        .config
                        .is_primary(in_view.view, minbfts.get(&id_primary).unwrap().config.id));
                }
                ViewState::ChangeInProgress(_) => panic!(
                    "view-change is still in progress for {:?}, possibly stuck",
                    minbft.config.id
                ),
            }
        }

        minbfts.remove(&id_primary);

        let mut collected_output = try_client_request(
            &mut minbfts,
            ClientId::from_u64(0),
            DummyPayload(2, true),
            &mut rng,
        );

        for responses in collected_output.responses.values() {
            assert_eq!(responses.len(), 0);
        }

        for (r, _) in minbfts.iter() {
            timeout_handlers
                .get_mut(r)
                .unwrap()
                .handle_timeout_requests(
                    collected_output.timeout_requests.get(r).unwrap().to_owned(),
                );
        }

        for (r, _) in minbfts.iter() {
            timeout_handlers
                .get_mut(r)
                .unwrap()
                .handle_timeout_requests(
                    collected_output.timeout_requests.get(r).unwrap().to_owned(),
                );
        }
        collected_output = force_timeout(&mut minbfts, &mut timeout_handlers, &mut rng);

        for (r, _) in minbfts.iter() {
            timeout_handlers
                .get_mut(r)
                .unwrap()
                .handle_timeout_requests(
                    collected_output.timeout_requests.get(r).unwrap().to_owned(),
                );
        }

        collected_output = force_timeout(&mut minbfts, &mut timeout_handlers, &mut rng);

        // The View should be Replica(1), and the counter of the latest accepted
        // Prepare should be synchronized.
        let mut counters_last_accepted_prep = HashSet::new();

        let primary_id = ReplicaId::from_u64(1);
        for minbft in minbfts.values() {
            match &minbft.view_state {
                ViewState::InView(in_view) => {
                    assert!(minbft
                        .config
                        .is_primary(in_view.view, minbfts.get(&primary_id).unwrap().config.id));
                    assert!(minbft.counter_last_accepted_prep.is_some());
                    counters_last_accepted_prep.insert(minbft.counter_last_accepted_prep.unwrap());
                }
                ViewState::ChangeInProgress(_) => panic!(
                    "view-change is still in progress for {:?}, possibly stuck",
                    minbft.config.id
                ),
            }
        }
        assert_eq!(counters_last_accepted_prep.len(), 1);

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

        minbfts.remove(&primary_id);

        let mut collected_output = try_client_request(
            &mut minbfts,
            ClientId::from_u64(0),
            DummyPayload(3, true),
            &mut rng,
        );

        for responses in collected_output.responses.values() {
            assert_eq!(responses.len(), 0);
        }

        // force handling timeout, view-change is expected to be forced as well
        for (r, _) in minbfts.iter() {
            timeout_handlers
                .get_mut(r)
                .unwrap()
                .handle_timeout_requests(
                    collected_output.timeout_requests.get(r).unwrap().to_owned(),
                );
        }
        collected_output = force_timeout(&mut minbfts, &mut timeout_handlers, &mut rng);

        // the View should be Replica(2), and the counters of the last accepted
        // Prepare should be synced.
        let mut counters_last_accepted_prep = HashSet::new();

        let primary_id = ReplicaId::from_u64(2);
        for minbft in minbfts.values() {
            match &minbft.view_state {
                ViewState::InView(in_view) => {
                    assert!(minbft
                        .config
                        .is_primary(in_view.view, minbfts.get(&primary_id).unwrap().config.id));
                    assert!(minbft.counter_last_accepted_prep.is_some());
                    counters_last_accepted_prep.insert(minbft.counter_last_accepted_prep.unwrap());
                }
                ViewState::ChangeInProgress(_) => panic!(
                    "view-change is still in progress for {:?}, possibly stuck",
                    minbft.config.id
                ),
            }
        }
        assert_eq!(counters_last_accepted_prep.len(), 1);

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
    let mut rng = thread_rng();
    for t in 1..n / 2 {
        let (mut minbfts, mut timeout_handlers) = setup_set(n, t, checkpoint_period);

        for (i, client_request_i) in (0..t).step_by(2).enumerate() {
            let collected_output = try_client_request(
                &mut minbfts,
                ClientId::from_u64(0),
                DummyPayload(client_request_i, true),
                &mut rng,
            );
            for responses in collected_output.responses.values() {
                assert_eq!(responses.len(), 1);
            }
            // no errors should have been collected
            for errors in collected_output.errors.values() {
                assert!(errors.is_empty());
            }

            // primary is deleted, a client message should therefore be ignored
            let deleted_primary = ReplicaId::from_u64(i.try_into().unwrap());
            minbfts.remove(&deleted_primary);
            let mut collected_output = try_client_request(
                &mut minbfts,
                ClientId::from_u64(0),
                DummyPayload(client_request_i + 1, true),
                &mut rng,
            );

            // output is expected to remain unchanged
            for responses in collected_output.responses.values() {
                assert_eq!(responses.len(), 0);
            }

            // force handling timeout, view-change is expected to be forced as well
            for (r, _) in minbfts.iter() {
                timeout_handlers
                    .get_mut(r)
                    .unwrap()
                    .handle_timeout_requests(
                        collected_output.timeout_requests.get(r).unwrap().to_owned(),
                    );
            }
            collected_output = force_timeout(&mut minbfts, &mut timeout_handlers, &mut rng);

            for (r, _) in minbfts.iter() {
                timeout_handlers
                    .get_mut(r)
                    .unwrap()
                    .handle_timeout_requests(
                        collected_output.timeout_requests.get(r).unwrap().to_owned(),
                    );
            }
            collected_output = force_timeout(&mut minbfts, &mut timeout_handlers, &mut rng);

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
            let primary_id = ReplicaId::from_u64((i + 1).try_into().unwrap());
            let mut counters_last_accepted_prep = HashSet::new();
            for minbft in minbfts.values() {
                match &minbft.view_state {
                    ViewState::InView(in_view) => {
                        assert!(minbft
                            .config
                            .is_primary(in_view.view, minbfts.get(&primary_id).unwrap().config.id));
                        assert!(minbft.counter_last_accepted_prep.is_some());
                        counters_last_accepted_prep
                            .insert(minbft.counter_last_accepted_prep.unwrap());
                    }
                    ViewState::ChangeInProgress(_) => panic!(
                        "view-change is still in progress for {:?}, possibly stuck",
                        minbft.config.id
                    ),
                }
            }
            assert_eq!(counters_last_accepted_prep.len(), 1);

            // request that was already accepted by previous view should not be accepted by the new view
            for (_, minbft) in minbfts.iter_mut() {
                assert!(minbft
                    .handle_client_message(
                        ClientId::from_u64(0),
                        DummyPayload(client_request_i, true),
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
    let mut rng = thread_rng();
    for t in 1..n / 2 {
        let (mut minbfts, mut timeout_handlers) = setup_set(n, t, checkpoint_period);

        let collected_output = try_client_request(
            &mut minbfts,
            ClientId::from_u64(0),
            DummyPayload(0, true),
            &mut rng,
        );
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
        let primary_id = ReplicaId::from_u64(0);
        let deleted_primary = minbfts.remove(&primary_id).unwrap();
        let mut collected_output = try_client_request(
            &mut minbfts,
            ClientId::from_u64(0),
            DummyPayload(1, true),
            &mut rng,
        );

        // output is expected to remain unchanged
        for responses in collected_output.responses.values() {
            assert_eq!(responses.len(), 0);
        }

        // force handling timeout, view-change is expected to be forced as well
        for (r, _) in minbfts.iter() {
            timeout_handlers
                .get_mut(r)
                .unwrap()
                .handle_timeout_requests(
                    collected_output.timeout_requests.get(r).unwrap().to_owned(),
                );
        }
        collected_output = force_timeout(&mut minbfts, &mut timeout_handlers, &mut rng);

        // view-change should have taken place
        // new View should be Replica(1)
        let mut counters_last_accepted_prep = HashSet::new();
        let primary_id = ReplicaId::from_u64(1);
        for minbft in minbfts.values() {
            match &minbft.view_state {
                ViewState::InView(in_view) => {
                    assert!(minbft
                        .config
                        .is_primary(in_view.view, minbfts.get(&primary_id).unwrap().config.id));
                    assert!(minbft.counter_last_accepted_prep.is_some());
                    counters_last_accepted_prep.insert(minbft.counter_last_accepted_prep.unwrap());
                }
                ViewState::ChangeInProgress(_) => panic!(
                    "view-change is still in progress for {:?}, possibly stuck",
                    minbft.config.id
                ),
            }
        }
        assert_eq!(counters_last_accepted_prep.len(), 1);

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
            minbfts.get(&primary_id).unwrap().config.me(),
            ReplicaId::from_u64(deleted_primary.config.id.as_u64() + 1)
        );

        for i in 2..n - 1 {
            let rep_id = ReplicaId::from_u64(i);
            let minbft_origin = minbfts.get_mut(&rep_id).unwrap();
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
                .get_mut(&primary_id)
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
    let mut rng = thread_rng();
    for t in 1..n / 2 {
        let (mut minbfts, mut timeout_handlers) = setup_set(n, t, checkpoint_period);

        // client message is received and handled
        let collected_output = try_client_request(
            &mut minbfts,
            ClientId::from_u64(0),
            DummyPayload(0, true),
            &mut rng,
        );
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
        let primary_id = ReplicaId::from_u64(0);
        minbfts.remove(&primary_id);
        let mut collected_output = try_client_request(
            &mut minbfts,
            ClientId::from_u64(0),
            DummyPayload(1, true),
            &mut rng,
        );

        // output is expected to remain unchanged
        for responses in collected_output.responses.values() {
            assert_eq!(responses.len(), 0);
        }

        let minbft = minbfts.get_mut(&ReplicaId::from_u64(2)).unwrap();
        minbft.sent_usig_msgs.pop().unwrap();

        // force handling timeout, view-change is expected to be forced as well
        for (r, _) in minbfts.iter() {
            timeout_handlers
                .get_mut(r)
                .unwrap()
                .handle_timeout_requests(
                    collected_output.timeout_requests.get(r).unwrap().to_owned(),
                );
        }
        collected_output = force_timeout(&mut minbfts, &mut timeout_handlers, &mut rng);
        assert_eq!(collected_output.errors.len(), minbfts.len());
    }
}

/// Tests for different setups if when deleting the first message of the message log results in an error.
#[rstest]
fn view_change_manipulate_message_log_pop_front(
    #[values(3, 4, 5, 6, 7, 8, 9, 10)] n: u64,
    #[values(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)] checkpoint_period: u64,
) {
    let mut rng = thread_rng();
    for t in 1..n / 2 {
        let (mut minbfts, mut timeout_handlers) = setup_set(n, t, checkpoint_period);

        // client message is received and handled
        let collected_output = try_client_request(
            &mut minbfts,
            ClientId::from_u64(0),
            DummyPayload(0, true),
            &mut rng,
        );
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
        let primary_id = ReplicaId::from_u64(0);
        minbfts.remove(&primary_id);
        let collected_output = try_client_request(
            &mut minbfts,
            ClientId::from_u64(0),
            DummyPayload(1, true),
            &mut rng,
        );

        // output is expected to remain unchanged
        for o in collected_output.responses.values() {
            assert_eq!(o.len(), 0);
        }

        let minbft = minbfts.get_mut(&ReplicaId::from_u64(2)).unwrap();
        minbft.sent_usig_msgs.pop().unwrap();

        // force handling timeout, view-change is expected to be forced as well
        let timeouts_to_handle =
            collected_output.timeouts_to_handle(&mut timeout_handlers, &mut rng);
        force_timeout_expect_error(&mut minbfts, &timeouts_to_handle, &mut rng);
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
    let mut rng = thread_rng();
    for t in 1..n / 2 {
        let (mut minbfts, mut timeout_handlers) = setup_set(n, t, checkpoint_period);

        let collected_output = try_client_request(
            &mut minbfts,
            ClientId::from_u64(0),
            DummyPayload(0, true),
            &mut rng,
        );

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

        let collected_output = try_client_request(
            &mut minbfts,
            ClientId::from_u64(0),
            DummyPayload(1, true),
            &mut rng,
        );

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

        let collected_output = try_client_request(
            &mut minbfts,
            ClientId::from_u64(0),
            DummyPayload(2, true),
            &mut rng,
        );

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
        let primary_id = ReplicaId::from_u64(0);
        minbfts.remove(&primary_id);
        let collected_output = try_client_request(
            &mut minbfts,
            ClientId::from_u64(0),
            DummyPayload(3, true),
            &mut rng,
        );

        // output is expected to remain unchanged
        for o in collected_output.responses.values() {
            assert_eq!(o.len(), 0);
        }

        let minbft = minbfts.get_mut(&ReplicaId::from_u64(2)).unwrap();
        minbft.sent_usig_msgs.remove(1);

        // force handling timeout, view-change is expected to be forced as well
        let timeouts_to_handle =
            collected_output.timeouts_to_handle(&mut timeout_handlers, &mut rng);
        force_timeout_expect_error(&mut minbfts, &timeouts_to_handle, &mut rng);
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
    let mut rng = thread_rng();
    for t in 1..n / 2 {
        let (mut minbfts, _timeout_handlers) = setup_set(n, t, checkpoint_period);

        for minbft in minbfts.values() {
            assert_eq!(
                minbft.config.checkpoint_period,
                NonZeroU64::new(checkpoint_period).unwrap()
            )
        }

        // checkpoint cert does not contain any messages at the beginning
        for minbft in minbfts.values() {
            assert!(minbft.last_checkpoint_cert.is_none());
        }

        for client_request_i in 1..(checkpoint_period * 2 + 1) {
            // client message is received and handled
            let collected_output = try_client_request(
                &mut minbfts,
                ClientId::from_u64(0),
                DummyPayload(client_request_i, true),
                &mut rng,
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

            let primary_id = ReplicaId::from_u64(0);
            let primary = minbfts.get(&primary_id).unwrap();
            let last_hash_expected = primary.request_processor.checkpoint_generator.last_hash;
            let amount_accepted_batches_expected = primary
                .request_processor
                .checkpoint_generator
                .total_amount_accepted_batches;
            for minbft in minbfts.values_mut() {
                if minbft.config.id != ReplicaId::from_u64(0) {
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
    let mut rng = thread_rng();
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
                let rep_id = ReplicaId::from_u64(id.try_into().unwrap());
                match &minbfts.get(&rep_id).unwrap().view_state {
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
                        } = minbfts.get_mut(&rep_id).unwrap().handle_client_message(
                            ClientId::from_u64(0),
                            DummyPayload(view_change_number, true),
                        );

                        collected_output
                            .timeout_requests
                            .entry(minbfts.get(&rep_id).unwrap().config.id)
                            .or_default()
                            .append(&mut Vec::from(timeout_requests));

                        let timeout_types =
                            collected_output.timeouts_to_handle(&mut timeout_handlers, &mut rng);

                        let replica_timeout_types =
                            timeout_types.get(&minbfts.get(&rep_id).unwrap().config.id);

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
                            } = minbfts
                                .get_mut(&rep_id)
                                .unwrap()
                                .handle_timeout(*timeout_type);

                            assert!(ready_for_client_requests);
                            collected_output
                                .responses
                                .entry(minbfts.get(&rep_id).unwrap().config.id)
                                .or_default()
                                .append(&mut Vec::from(responses));
                            collected_output
                                .errors
                                .entry(minbfts.get(&rep_id).unwrap().config.id)
                                .or_default()
                                .append(&mut Vec::from(errors));
                            collected_output
                                .timeout_requests
                                .entry(minbfts.get(&rep_id).unwrap().config.id)
                                .or_default()
                                .append(&mut Vec::from(timeout_requests));
                            if !broadcasts.is_empty() {
                                all_broadcasts
                                    .push((minbfts.get(&rep_id).unwrap().config.id, broadcasts));
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

            handle_broadcasts(
                &mut minbfts,
                all_broadcasts,
                &mut collected_output,
                &mut rng,
            );

            for (r, _) in minbfts.iter() {
                timeout_handlers
                    .get_mut(r)
                    .unwrap()
                    .handle_timeout_requests(
                        collected_output.timeout_requests.get(r).unwrap().to_owned(),
                    );
            }
            collected_output = force_timeout(&mut minbfts, &mut timeout_handlers, &mut rng);

            // view-change should have taken place

            let mut counters_last_accepted_prep = HashSet::new();

            for minbft in minbfts.values() {
                let expected_view = View(next_view.0);
                match &minbft.view_state {
                    ViewState::InView(in_view) => {
                        assert_eq!(
                            in_view.view.0, expected_view.0,
                            "for {:?} view is {:?} rather than the expected view {:?}",
                            minbft.config.id, in_view.view, expected_view
                        );
                        assert!(minbft.counter_last_accepted_prep.is_some());
                        counters_last_accepted_prep
                            .insert(minbft.counter_last_accepted_prep.unwrap());
                    }
                    ViewState::ChangeInProgress(_) => {
                        panic!("{:?}: after handle_broadcasts_new(): view-change is still in progress, possibly stuck", minbft.config.id);
                    }
                }
            }
            assert_eq!(counters_last_accepted_prep.len(), 1);

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
    let primary_id = ReplicaId::from_u64(0);
    let output = minbfts
        .get_mut(&primary_id)
        .unwrap()
        .handle_client_message(ClientId::from_u64(23), DummyPayload(56, true));

    // should broadcast Prepare
    assert_eq!(output.broadcasts.len(), 1);
    let broadcasts = Vec::from(output.broadcasts); // remove once https://github.com/rust-lang/rust/issues/59878 is fixed
    let mut iter = broadcasts.into_iter();
    let prepare = iter.next().unwrap();

    // send client-request to backup once
    let backup_id = ReplicaId::from_u64(1);
    let output = minbfts
        .get_mut(&backup_id)
        .unwrap()
        .handle_peer_message(ReplicaId::from_u64(0), prepare.clone());

    // backup should broadcast Commit
    assert_eq!(output.broadcasts.len(), 1);

    // send client-request to backup twice
    let output = minbfts
        .get_mut(&backup_id)
        .unwrap()
        .handle_peer_message(ReplicaId::from_u64(0), prepare);

    // backup should not broadcast Commit
    assert_eq!(output.broadcasts.len(), 0);
}

/// Tests if performing two view-changes subsequently changes to the respective view and
/// if a pending client-request is accepted in the end.
#[test]
fn two_view_changes_subsequently() {
    let mut rng = thread_rng();

    let (mut minbfts, mut timeout_handlers) = setup_set(5, 1, 2);

    // Remove first two replicas that would be the first two views.
    let primary_id = ReplicaId::from_u64(0);
    minbfts.remove(&primary_id);
    let primary_id = ReplicaId::from_u64(1);
    minbfts.remove(&primary_id);

    // Send a client-request.
    let mut collected_output = try_client_request(
        &mut minbfts,
        ClientId::from_u64(0),
        DummyPayload(0, true),
        &mut rng,
    );

    for responses in collected_output.responses.values() {
        assert_eq!(responses.len(), 0);
    }

    for (r, _) in minbfts.iter() {
        timeout_handlers
            .get_mut(r)
            .unwrap()
            .handle_timeout_requests(collected_output.timeout_requests.get(r).unwrap().to_owned());
    }
    collected_output = force_timeout(&mut minbfts, &mut timeout_handlers, &mut rng);
    for (r, _) in minbfts.iter() {
        timeout_handlers
            .get_mut(r)
            .unwrap()
            .handle_timeout_requests(collected_output.timeout_requests.get(r).unwrap().to_owned());
    }
    collected_output = force_timeout(&mut minbfts, &mut timeout_handlers, &mut rng);

    // the View should be Replica(2)
    let mut counters_last_accepted_prep = HashSet::new();
    let primary_id = ReplicaId::from_u64(2);
    for minbft in minbfts.values() {
        match &minbft.view_state {
            ViewState::InView(in_view) => {
                assert!(minbft
                    .config
                    .is_primary(in_view.view, minbfts.get(&primary_id).unwrap().config.id));
                assert!(minbft.counter_last_accepted_prep.is_some());
                counters_last_accepted_prep.insert(minbft.counter_last_accepted_prep.unwrap());
            }
            ViewState::ChangeInProgress(_) => panic!(
                "view-change is still in progress for {:?}, possibly stuck",
                minbft.config.id
            ),
        }
    }
    assert_eq!(counters_last_accepted_prep.len(), 1);

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
// #[traced_test]
#[rstest]
fn multi_view_changes_subsequently(
    #[values(5, 6, 7, 8, 9, 11, 21, 31)] n: u64,
    #[values(1, 2, 3)] checkpoint_period: u64,
) {
    let mut rng = thread_rng();
    let mut f = 1;
    let t = (n - 1) / 2;
    while f <= t {
        // At least two view-changes can be forced.
        let (mut minbfts, mut timeout_handlers) = setup_set(n, t, checkpoint_period);
        // Remove first n - (2 * t + 1) replicas to force the same amount of view-changes.
        let mut amount_replicas_removed = 0;
        while amount_replicas_removed < f {
            minbfts.remove(&ReplicaId::from_u64(amount_replicas_removed));
            amount_replicas_removed += 1;
        }
        // Send a client-request.
        let mut collected_output = try_client_request(
            &mut minbfts,
            ClientId::from_u64(0),
            DummyPayload(0, true),
            &mut rng,
        );

        for responses in collected_output.responses.values() {
            assert_eq!(responses.len(), 0);
        }

        let mut i = 0;
        while i < f {
            for (r, _) in minbfts.iter() {
                timeout_handlers
                    .get_mut(r)
                    .unwrap()
                    .handle_timeout_requests(
                        collected_output.timeout_requests.get(r).unwrap().to_owned(),
                    );
            }
            collected_output = force_timeout(&mut minbfts, &mut timeout_handlers, &mut rng);
            i += 1;
        }

        // the View should be Replica with ID (amount_replicas_removed)
        let mut counters_last_accepted_prep = HashSet::new();
        let primary_id = ReplicaId::from_u64(amount_replicas_removed);
        for minbft in minbfts.values() {
            match &minbft.view_state {
                ViewState::InView(in_view) => {
                    assert!(minbft
                        .config
                        .is_primary(in_view.view, primary_id));
                    assert!(minbft.counter_last_accepted_prep.is_some());
                    counters_last_accepted_prep.insert(minbft.counter_last_accepted_prep.unwrap());
                }
                ViewState::ChangeInProgress(_) => panic!(
                    "view-change is still in progress for {:?}, possibly stuck (primary should be {:?}",
                    minbft.config.id,
                    primary_id
                ),
            }
        }
        assert_eq!(counters_last_accepted_prep.len(), 1);

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
        f += 1;
    }
}

/// Test if a replica that was previously the primary and later becomes the primary again
/// still receives and accepts client requests after a full view change cycle.
#[rstest]
fn replica_becomes_primary_after_view_change_cycle(
    #[values(5, 6, 7, 8, 9, 11, 21, 31)] n: u64,
    #[values(1, 2, 3)] checkpoint_period: u64,
) {
    let mut rng = thread_rng();
    let (mut minbfts, mut timeout_handlers) = setup_set(n, 1, checkpoint_period);

    let mut current_req_id = 0;
    let mut current_view = 0;

    while current_view < n {
        let primary_id = ReplicaId::from_u64(current_view % n);
        let current_primary = minbfts.remove(&primary_id).unwrap();

        // send client message
        // the primary got deleted, therefore it is expected that the message is not handled
        let mut collected_output = try_client_request(
            &mut minbfts,
            ClientId::from_u64(0),
            DummyPayload(current_req_id, true),
            &mut rng,
        );
        // output should be empty
        for o in collected_output.responses.values() {
            assert_eq!(o.len(), 0);
        }
        minbfts.insert(primary_id, current_primary);

        // force timeout in order for view-change to take place
        for (r, _) in minbfts.iter() {
            if *r == primary_id {
                continue;
            }
            timeout_handlers
                .get_mut(r)
                .unwrap()
                .handle_timeout_requests(
                    collected_output.timeout_requests.get(r).unwrap().to_owned(),
                );
        }
        collected_output = force_timeout(&mut minbfts, &mut timeout_handlers, &mut rng);

        for (r, _) in minbfts.iter() {
            timeout_handlers
                .get_mut(r)
                .unwrap()
                .handle_timeout_requests(
                    collected_output.timeout_requests.get(r).unwrap().to_owned(),
                );
        }
        collected_output = force_timeout(&mut minbfts, &mut timeout_handlers, &mut rng);

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
        let mut counters_last_accepted_prep = HashSet::new();
        let primary_id = ReplicaId::from_u64((current_view + 1) % n);
        for minbft in minbfts.values() {
            match &minbft.view_state {
                ViewState::InView(in_view) => {
                    assert!(minbft.config.is_primary(in_view.view, primary_id));
                    assert!(minbft.counter_last_accepted_prep.is_some());
                    counters_last_accepted_prep.insert(minbft.counter_last_accepted_prep.unwrap());
                }
                ViewState::ChangeInProgress(in_progress) => {
                    panic!(
                        "view-change is still in progress, possibly stuck (from {:?}, to {:?}",
                        in_progress.prev_view, in_progress.next_view
                    );
                }
            }
        }
        assert_eq!(counters_last_accepted_prep.len(), 1);

        current_view += 1;
        current_req_id += 1;
    }
    // receive and handle new client message
    let collected_output = try_client_request(
        &mut minbfts,
        ClientId::from_u64(0),
        DummyPayload(current_req_id, true),
        &mut rng,
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
