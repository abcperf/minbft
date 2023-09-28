use crate::peer_message::usig_message::view_peer_message::ViewPeerMessage;

use super::*;

/// Test if handling a timeout for a ClientRequest which has not been accepted yet
/// results in the broadcast of a message of type ReqViewChange for the subsequent View.
/// More precisely, tests if the Replica with id = 0 broadcasts a ReqViewChange when
/// the timeout for a ClientRequest is triggered.
/// The configuration used is n = 3, t = 1, checkpoint_period = 2.
#[test]
fn req_view_change() {
    let id = ReplicaId::from_u64(0);
    let ((mut minbft, _), mut timeout_handler) = minimal_setup(3, 1, id, 2);

    let output = minbft.handle_client_message(ClientId::from_u64(23), DummyPayload(56, true));

    assert_eq!(output.broadcasts.len(), 1);
    let broadcasts = Vec::from(output.broadcasts); // remove once https://github.com/rust-lang/rust/issues/59878 is fixed
    let mut iter = broadcasts.into_iter();
    let message = iter
        .next()
        .unwrap()
        .validate(id, &minbft.config, &mut minbft.usig)
        .unwrap();
    assert!(matches!(
        message,
        ValidatedPeerMessage::Usig(UsigMessage::View(ViewPeerMessage::Prepare(_)))
    ));

    assert_eq!(output.responses.len(), 0);
    assert_eq!(output.errors.len(), 0);

    let timeout_requests = Vec::from(output.timeout_requests); // remove once https://github.com/rust-lang/rust/issues/59878 is fixed
    assert_eq!(timeout_requests.len(), 2);
    let mut iter = timeout_requests.clone().into_iter();
    let timeout_request = iter.next().unwrap();
    assert!(matches!(timeout_request, TimeoutRequest::Start(_)));
    if let TimeoutRequest::Start(timeout) = timeout_request {
        assert!(matches!(timeout.timeout_type, TimeoutType::Client));
        assert!(matches!(timeout.stop_class, StopClass(23)));
    } else {
        unreachable!();
    }
    let timeout_request = iter.next().unwrap();
    assert!(matches!(timeout_request, TimeoutRequest::Stop(_)));
    if let TimeoutRequest::Stop(timeout) = timeout_request {
        assert!(matches!(timeout.timeout_type, TimeoutType::Batch));
        assert_eq!(timeout.stop_class, StopClass::default());
    } else {
        unreachable!();
    }

    timeout_handler.handle_timeout_requests(timeout_requests);
    let timeout_types = timeout_handler.retrieve_timeouts_ordered();
    assert_eq!(timeout_types.len(), 1);
    let mut iter = timeout_types.into_iter();
    let timeout_type = iter.next().unwrap();
    assert!(matches!(timeout_type, TimeoutType::Client));

    let output = minbft.handle_timeout(timeout_type);

    assert_eq!(output.broadcasts.len(), 1);
    let broadcasts = Vec::from(output.broadcasts); // remove once https://github.com/rust-lang/rust/issues/59878 is fixed
    let mut iter = broadcasts.into_iter();
    let message = iter
        .next()
        .unwrap()
        .validate(id, &minbft.config, &mut minbft.usig)
        .unwrap();
    assert!(matches!(
        message,
        ValidatedPeerMessage::ReqViewChange(ReqViewChange {
            next_view: View(1),
            prev_view: View(0),
        })
    ));

    assert_eq!(output.responses.len(), 0);
    assert_eq!(output.errors.len(), 0);
    assert_eq!(output.timeout_requests.len(), 0);
}

/// Tests if a message of type ViewChange is broadcasted when sufficient Replicas broadcasted messages of type ReqViewChange.
/// More precisely, tests if the Replica with id = 0 broadcasts a ViewChange when both the Replica with id = 0 and the Replica
/// with id = 1 each broadcasted a ReqViewChange message previously.
/// The configuration used is n = 3, t = 1, checkpoint_period = 2.
#[test]
fn view_change() {
    let ((mut minbft_0, _), mut timeout_handler_0) = minimal_setup(3, 1, ReplicaId::from_u64(0), 2);
    let ((mut minbft_1, _), mut timeout_handler_1) = minimal_setup(3, 1, ReplicaId::from_u64(1), 2);

    let output = minbft_1.handle_client_message(ClientId::from_u64(23), DummyPayload(56, true));

    assert_eq!(output.broadcasts.len(), 0);
    assert_eq!(output.responses.len(), 0);
    assert_eq!(output.errors.len(), 0);

    let timeout_requests = Vec::from(output.timeout_requests); // remove once https://github.com/rust-lang/rust/issues/59878 is fixed
    assert_eq!(timeout_requests.len(), 1);
    let mut iter = timeout_requests.clone().into_iter();
    let timeout_request = iter.next().unwrap();
    assert!(matches!(timeout_request, TimeoutRequest::Start(_)));
    if let TimeoutRequest::Start(timeout) = timeout_request {
        assert!(matches!(timeout.timeout_type, TimeoutType::Client));
        assert!(matches!(timeout.stop_class, StopClass(23)));
    } else {
        unreachable!();
    }

    timeout_handler_1.handle_timeout_requests(timeout_requests);
    let timeout_types = timeout_handler_1.retrieve_timeouts_ordered();
    assert_eq!(timeout_types.len(), 1);
    let mut iter = timeout_types.into_iter();
    let timeout_type = iter.next().unwrap();
    assert!(matches!(timeout_type, TimeoutType::Client));

    let output = minbft_1.handle_timeout(timeout_type);
    assert_eq!(output.broadcasts.len(), 1);
    let broadcasts = Vec::from(output.broadcasts); // remove once https://github.com/rust-lang/rust/issues/59878 is fixed
    let mut iter = broadcasts.into_iter();
    let message = iter.next().unwrap();
    assert_eq!(output.responses.len(), 0);
    assert_eq!(output.errors.len(), 0);
    assert_eq!(output.timeout_requests.len(), 0);

    let output = minbft_0.handle_client_message(ClientId::from_u64(23), DummyPayload(56, true));

    assert_eq!(output.broadcasts.len(), 1);
    assert_eq!(output.responses.len(), 0);
    assert_eq!(output.errors.len(), 0);

    let timeout_requests = Vec::from(output.timeout_requests); // remove once https://github.com/rust-lang/rust/issues/59878 is fixed
    assert_eq!(timeout_requests.len(), 2);
    let mut iter = timeout_requests.clone().into_iter();
    let timeout_request = iter.next().unwrap();
    assert!(matches!(timeout_request, TimeoutRequest::Start(_)));
    if let TimeoutRequest::Start(timeout) = timeout_request {
        assert!(matches!(timeout.timeout_type, TimeoutType::Client));
        assert!(matches!(timeout.stop_class, StopClass(23)));
    } else {
        unreachable!();
    }
    let timeout_request = iter.next().unwrap();
    assert!(matches!(timeout_request, TimeoutRequest::Stop(_)));
    if let TimeoutRequest::Stop(timeout) = timeout_request {
        assert!(matches!(timeout.timeout_type, TimeoutType::Batch));
        assert_eq!(timeout.stop_class, StopClass::default());
    } else {
        unreachable!();
    }

    timeout_handler_0.handle_timeout_requests(timeout_requests);
    let timeout_types = timeout_handler_0.retrieve_timeouts_ordered();
    assert_eq!(timeout_types.len(), 1);
    let mut iter = timeout_types.into_iter();
    let timeout_type = iter.next().unwrap();
    assert!(matches!(timeout_type, TimeoutType::Client));

    let output = minbft_0.handle_timeout(timeout_type);
    assert_eq!(output.broadcasts.len(), 1);
    assert_eq!(output.responses.len(), 0);
    assert_eq!(output.errors.len(), 0);
    assert_eq!(output.timeout_requests.len(), 0);

    let output = minbft_0.handle_peer_message(ReplicaId::from_u64(1), message);
    assert_eq!(output.broadcasts.len(), 1);
    let broadcasts = Vec::from(output.broadcasts); // remove once https://github.com/rust-lang/rust/issues/59878 is fixed
    let mut iter = broadcasts.into_iter();
    let message = iter
        .next()
        .unwrap()
        .validate(ReplicaId::from_u64(0), &minbft_0.config, &mut minbft_0.usig)
        .unwrap();
    assert!(matches!(
        message,
        ValidatedPeerMessage::Usig(UsigMessage::ViewChange(_))
    ));

    assert_eq!(output.responses.len(), 0);
    assert_eq!(output.errors.len(), 0);

    let timeout_requests = Vec::from(output.timeout_requests); // remove once https://github.com/rust-lang/rust/issues/59878 is fixed
    assert_eq!(timeout_requests.len(), 2);
    let mut iter = timeout_requests.clone().into_iter();
    let timeout_request = iter.next().unwrap();
    assert!(matches!(timeout_request, TimeoutRequest::Stop(_)));
    if let TimeoutRequest::Stop(timeout) = timeout_request {
        assert!(matches!(timeout.timeout_type, TimeoutType::ViewChange));
        assert_eq!(timeout.stop_class, StopClass::default());
    } else {
        unreachable!();
    }
    let timeout_request = iter.next().unwrap();
    assert!(matches!(timeout_request, TimeoutRequest::Start(_)));
    if let TimeoutRequest::Start(timeout) = timeout_request {
        assert!(matches!(timeout.timeout_type, TimeoutType::ViewChange));
        assert_eq!(timeout.stop_class, StopClass::default());
    } else {
        unreachable!();
    }

    timeout_handler_0.handle_timeout_requests(timeout_requests);
    let timeout_types = timeout_handler_0.retrieve_timeouts_ordered();
    assert_eq!(timeout_types.len(), 1);
    let mut iter = timeout_types.into_iter();
    let timeout_type = iter.next().unwrap();
    assert!(matches!(timeout_type, TimeoutType::ViewChange));
}

/// Tests if handling a timeout of a message of type ViewChange results in the broadcast of a message of type ReqViewChange.
/// More precisely, tests if the Replica with id = 0 broadcasts a ReqViewChange
/// the moment the timeout for its ViewChange message is triggered.
/// Furthermore, it tests if the ReqViewChange message is for the next View, i.e. View(2).
/// The configuration used is n = 3, t = 1, checkpoint_period = 2.
#[test]
fn view_change_timeout() {
    let ((mut minbft_0, _), mut timeout_handler_0) = minimal_setup(3, 1, ReplicaId::from_u64(0), 2);
    let ((mut minbft_1, _), mut timeout_handler_1) = minimal_setup(3, 1, ReplicaId::from_u64(1), 2);

    let output = minbft_1.handle_client_message(ClientId::from_u64(23), DummyPayload(56, true));

    assert_eq!(output.broadcasts.len(), 0);
    assert_eq!(output.responses.len(), 0);
    assert_eq!(output.errors.len(), 0);

    let timeout_requests = Vec::from(output.timeout_requests); // remove once https://github.com/rust-lang/rust/issues/59878 is fixed
    assert_eq!(timeout_requests.len(), 1);
    let mut iter = timeout_requests.clone().into_iter();
    let timeout_request = iter.next().unwrap();
    assert!(matches!(timeout_request, TimeoutRequest::Start(_)));
    if let TimeoutRequest::Start(timeout) = timeout_request {
        assert!(matches!(timeout.timeout_type, TimeoutType::Client));
        assert!(matches!(timeout.stop_class, StopClass(23)));
    } else {
        unreachable!();
    }

    timeout_handler_1.handle_timeout_requests(timeout_requests);
    let timeout_types = timeout_handler_1.retrieve_timeouts_ordered();
    assert_eq!(timeout_types.len(), 1);
    let mut iter = timeout_types.into_iter();
    let timeout_type = iter.next().unwrap();
    assert!(matches!(timeout_type, TimeoutType::Client));

    let output = minbft_1.handle_timeout(timeout_type);
    assert_eq!(output.broadcasts.len(), 1);
    let broadcasts = Vec::from(output.broadcasts); // remove once https://github.com/rust-lang/rust/issues/59878 is fixed
    let mut iter = broadcasts.into_iter();
    let message = iter.next().unwrap();
    assert_eq!(output.responses.len(), 0);
    assert_eq!(output.errors.len(), 0);
    assert_eq!(output.timeout_requests.len(), 0);

    let output = minbft_0.handle_client_message(ClientId::from_u64(23), DummyPayload(56, true));

    assert_eq!(output.broadcasts.len(), 1);
    assert_eq!(output.responses.len(), 0);
    assert_eq!(output.errors.len(), 0);

    let timeout_requests = Vec::from(output.timeout_requests); // remove once https://github.com/rust-lang/rust/issues/59878 is fixed
    assert_eq!(timeout_requests.len(), 2);
    let mut iter = timeout_requests.into_iter();
    let timeout_request = iter.next().unwrap();
    assert!(matches!(timeout_request, TimeoutRequest::Start(_)));
    if let TimeoutRequest::Start(timeout) = timeout_request {
        assert!(matches!(timeout.timeout_type, TimeoutType::Client));
        assert!(matches!(timeout.stop_class, StopClass(23)));
    } else {
        unreachable!();
    }
    let timeout_request = iter.next().unwrap();
    assert!(matches!(timeout_request, TimeoutRequest::Stop(_)));
    if let TimeoutRequest::Stop(timeout) = timeout_request {
        assert!(matches!(timeout.timeout_type, TimeoutType::Batch));
        assert_eq!(timeout.stop_class, StopClass::default());
    } else {
        unreachable!();
    }

    let output = minbft_0.handle_timeout(timeout_type);
    assert_eq!(output.broadcasts.len(), 1);
    assert_eq!(output.responses.len(), 0);
    assert_eq!(output.errors.len(), 0);
    assert_eq!(output.timeout_requests.len(), 0);

    let output = minbft_0.handle_peer_message(ReplicaId::from_u64(1), message);
    assert_eq!(output.broadcasts.len(), 1);
    assert_eq!(output.responses.len(), 0);
    assert_eq!(output.errors.len(), 0);

    let timeout_requests = Vec::from(output.timeout_requests); // remove once https://github.com/rust-lang/rust/issues/59878 is fixed
    assert_eq!(timeout_requests.len(), 2);
    let mut iter = timeout_requests.clone().into_iter();
    let timeout_request = iter.next().unwrap();
    assert!(matches!(timeout_request, TimeoutRequest::Stop(_)));
    if let TimeoutRequest::Stop(timeout) = timeout_request {
        assert!(matches!(timeout.timeout_type, TimeoutType::ViewChange));
        assert_eq!(timeout.stop_class, StopClass::default());
    } else {
        unreachable!();
    }
    let timeout_request = iter.next().unwrap();
    assert!(matches!(timeout_request, TimeoutRequest::Start(_)));
    if let TimeoutRequest::Start(timeout) = timeout_request {
        assert!(matches!(timeout.timeout_type, TimeoutType::ViewChange));
        assert_eq!(timeout.stop_class, StopClass::default());
    } else {
        unreachable!();
    }

    timeout_handler_0.handle_timeout_requests(timeout_requests);
    let timeout_types = timeout_handler_0.retrieve_timeouts_ordered();
    assert_eq!(timeout_types.len(), 1);
    let mut iter = timeout_types.into_iter();
    let timeout_type = iter.next().unwrap();
    assert!(matches!(timeout_type, TimeoutType::ViewChange));

    let output = minbft_0.handle_timeout(timeout_type);
    assert_eq!(output.broadcasts.len(), 1);
    let broadcasts = Vec::from(output.broadcasts); // remove once https://github.com/rust-lang/rust/issues/59878 is fixed
    let mut iter = broadcasts.into_iter();
    let message = iter
        .next()
        .unwrap()
        .validate(ReplicaId::from_u64(0), &minbft_0.config, &mut minbft_0.usig)
        .unwrap();
    assert!(matches!(
        message,
        ValidatedPeerMessage::ReqViewChange(ReqViewChange {
            next_view: View(2),
            prev_view: View(0),
        })
    ));

    assert_eq!(output.responses.len(), 0);
    assert_eq!(output.errors.len(), 0);
    assert_eq!(output.timeout_requests.len(), 0);
}
