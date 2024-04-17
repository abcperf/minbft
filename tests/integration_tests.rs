use rstest::rstest;

mod common;

use common::force_timeout;
use common::setup_set;

use common::try_client_request;
use shared_ids::AnyId;
use shared_ids::ClientId;
use shared_ids::ReplicaId;
use std::collections::HashMap;

use minbft::timeout::TimeoutType;

use crate::common::DummyPayload;

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
    use common::force_timeout;

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

            // request that was already accepted by previous view should not be accepted by the new view
            for (_, minbft) in minbfts.iter_mut() {
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

/// Tests if a backup does not broadcast two Commits when the primary sends its Prepare twice.
#[test]
fn send_prepare_twice_to_backup() {
    let (mut minbfts, _timeout_handlers) = setup_set(5, 2, 2);

    // send client-request to primary
    let output = minbfts[0]
        .1
        .handle_client_message(ClientId::from_u64(23), DummyPayload(56, true));

    // should broadcast Prepare
    assert_eq!(output.broadcasts.len(), 1);
    let broadcasts = Vec::from(output.broadcasts); // remove once https://github.com/rust-lang/rust/issues/59878 is fixed
    let mut iter = broadcasts.into_iter();
    let prepare = iter.next().unwrap();

    // send client-request to backup once
    let output = minbfts[1]
        .1
        .handle_peer_message(ReplicaId::from_u64(0), prepare.clone());

    // backup should broadcast Commit
    assert_eq!(output.broadcasts.len(), 1);

    // send client-request to backup twice
    let output = minbfts[1]
        .1
        .handle_peer_message(ReplicaId::from_u64(0), prepare);

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
